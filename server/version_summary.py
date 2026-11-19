"""Resumo por versão: nº de arquivos, bytes lógicos e diff contra a anterior.

Por que existe: as stats agregavam version_files inteira (milhões de linhas) a
cada recálculo, para obter números que, numa versão finalizada, nunca mudam.
Uma versão done/failed é imutável (finish_version recusa mudar o status, upload
e absorb só aceitam running/incomplete), então o agregado dela é calculado uma
vez e guardado nas colunas sum_* de backup_versions.

O resumo é um cache, não a verdade: version_files continua sendo a fonte, e
uma versão é dada como "suja" (recalculada) sempre que o resumo pode não valer
mais, sem depender de nenhum ponto de escrita avisar:

- sum_status é NULL ou diferente do status atual (versão nova, restaurada da
  lixeira, re-finalizada);
- é done e sum_prev_id não é a done anterior ATUAL dela (a anterior foi
  apagada ou foi para a lixeira — o diff muda de base).

Ficam de fora, e são sempre calculadas ao vivo por quem lê, as versões que
podem receber arquivos: running, incomplete (upload e absorb aceitam) e as dos
labels do Cloud (kind=server), cuja versão "live" fica done e muda o tempo todo.
"""
import logging
import threading
import time
from typing import Callable, Iterable, Optional

from sqlalchemy import and_, case, func, select
from sqlalchemy.orm import Session, aliased

import version_diff
from database import (
    BackupID, BackupVersion, FileContent, VersionFile,
    SERVER_KIND, TRASHED_STATUS,
)

log = logging.getLogger("backup-server")

# Status cujo conteúdo não muda mais. TRASHED_STATUS entra porque a lixeira não
# mexe em version_files; a restauração troca o status e o resumo é refeito.
SUMMARIZABLE = ("done", "failed", TRASHED_STATUS)

# Quantas versões pendentes quem lê (as stats) resume na hora, antes de responder.
# No dia a dia são 1 ou 2 (as que acabaram de fechar); no backfill do primeiro
# boot são centenas e ficam para o thread, com a página avisando quantas faltam.
INLINE_MAX = 10

AUTOSTART = True  # tests/conftest desliga: o thread usaria o banco do teste em paralelo


def _server_labels_sq():
    return select(BackupID.label).where(BackupID.kind == SERVER_KIND)


def current_prev_map(db: Session) -> dict[int, Optional[int]]:
    """vid → done anterior do mesmo label (ordem de version_key), para as done."""
    sq = version_diff.done_version_lag_sq(db)
    return {r.vid: r.prev_id for r in db.query(sq.c.vid, sq.c.prev_id).all()}


def pending_ids(db: Session, prev_map: Optional[dict] = None) -> list[int]:
    """Versões resumíveis cujo resumo falta ou não vale mais (ver docstring do módulo)."""
    if prev_map is None:
        prev_map = current_prev_map(db)
    rows = (
        db.query(BackupVersion.id, BackupVersion.status,
                 BackupVersion.sum_status, BackupVersion.sum_prev_id)
        .filter(BackupVersion.status.in_(SUMMARIZABLE),
                BackupVersion.backup_label.notin_(_server_labels_sq()))
        .all()
    )
    out = []
    for r in rows:
        if r.sum_status is None or r.sum_status != r.status:
            out.append(r.id)
        elif r.status == "done" and r.sum_prev_id != prev_map.get(r.id):
            out.append(r.id)
    return out


def _totals(db: Session, version_ids: Iterable[int]) -> dict[int, tuple[int, int]]:
    """vid → (nº de arquivos, bytes lógicos), numa query agrupada."""
    ids = list(version_ids)
    if not ids:
        return {}
    rows = (
        db.query(VersionFile.version_id,
                 func.count(VersionFile.id).label("fc"),
                 func.coalesce(func.sum(FileContent.size), 0).label("lb"))
        .join(FileContent, FileContent.sha256 == VersionFile.sha256)
        .filter(VersionFile.version_id.in_(ids))
        .group_by(VersionFile.version_id)
        .all()
    )
    return {r.version_id: (int(r.fc), int(r.lb)) for r in rows}


def _diff(db: Session, vid: int, prev_id: Optional[int], total: int) -> tuple[int, int, int]:
    """(added, modified, removed) de `vid` contra `prev_id` — mesmas identidades
    de version_diff.diff_counts_by_version, para uma versão só."""
    if prev_id is None:
        return total, 0, 0
    prev_total = (db.query(func.count(VersionFile.id))
                  .filter(VersionFile.version_id == prev_id).scalar() or 0)
    cf, pf = aliased(VersionFile), aliased(VersionFile)
    row = (
        db.query(func.count().label("same_path"),
                 func.coalesce(func.sum(case((cf.sha256 == pf.sha256, 1), else_=0)), 0)
                 .label("same_both"))
        .select_from(cf)
        .join(pf, and_(pf.version_id == prev_id, pf.original_path == cf.original_path))
        .filter(cf.version_id == vid)
        .one()
    )
    same_path, same_both = int(row.same_path or 0), int(row.same_both or 0)
    return total - same_path, same_path - same_both, int(prev_total) - same_path


def summarize(db: Session, vid: int, prev_map: dict) -> bool:
    """Calcula e grava o resumo de uma versão. Devolve False se ela mudou de
    status no meio (o UPDATE é condicionado ao status lido) ou sumiu."""
    v = db.get(BackupVersion, vid)
    if v is None or v.status not in SUMMARIZABLE:
        return False
    status = v.status
    fc, lb = _totals(db, [vid]).get(vid, (0, 0))
    prev_id = prev_map.get(vid) if status == "done" else None
    if status == "done":
        added, modified, removed = _diff(db, vid, prev_id, fc)
    else:
        added = modified = removed = None
    n = (
        db.query(BackupVersion)
        .filter(BackupVersion.id == vid, BackupVersion.status == status)
        .update({
            "sum_file_count": fc, "sum_logical_bytes": lb,
            "sum_added": added, "sum_modified": modified, "sum_removed": removed,
            "sum_prev_id": prev_id, "sum_status": status,
        }, synchronize_session=False)
    )
    db.commit()
    return n == 1


def summarize_pending(db: Session, limit: Optional[int] = None,
                      should_continue: Callable[[], bool] = lambda: True) -> tuple[int, int]:
    """Resume as pendentes (até `limit`). Devolve (resumidas, ainda pendentes)."""
    prev_map = current_prev_map(db)
    ids = pending_ids(db, prev_map)
    todo = ids if limit is None else ids[:limit]
    done = 0
    for vid in todo:
        if not should_continue():
            break
        try:
            if summarize(db, vid, prev_map):
                done += 1
        except Exception:
            db.rollback()
            log.exception(f"[version-summary] falha ao resumir a versão {vid}")
    return done, len(ids) - done


class VersionTotals:
    """O que as stats precisam de cada versão: resumo gravado quando válido,
    cálculo ao vivo para as mutáveis, e a contagem das que ainda faltam."""

    def __init__(self, db: Session):
        prev_map = current_prev_map(db)
        pending = pending_ids(db, prev_map)
        if pending and len(pending) <= INLINE_MAX:
            for vid in pending:
                try:
                    summarize(db, vid, prev_map)
                except Exception:
                    db.rollback()
                    log.exception(f"[version-summary] falha ao resumir a versão {vid}")
            pending = pending_ids(db, prev_map)
        self.pending = set(pending)

        server_labels = _server_labels_sq()
        rows = db.query(
            BackupVersion.id, BackupVersion.status, BackupVersion.backup_label,
            BackupVersion.sum_file_count, BackupVersion.sum_logical_bytes,
            BackupVersion.sum_added, BackupVersion.sum_modified, BackupVersion.sum_removed,
            BackupVersion.backup_label.in_(server_labels).label("is_server"),
        ).all()

        # vid → (file_count, logical_bytes); vid → {"added","modified","removed"}
        self.totals: dict[int, tuple[int, int]] = {}
        self.diffs: dict[int, dict[str, int]] = {}
        live_ids: list[int] = []
        live_done_labels: set[str] = set()
        for r in rows:
            if r.id in self.pending:
                continue
            if r.status in SUMMARIZABLE and not r.is_server:
                self.totals[r.id] = (int(r.sum_file_count or 0), int(r.sum_logical_bytes or 0))
                if r.status == "done":
                    self.diffs[r.id] = {"added": int(r.sum_added or 0),
                                        "modified": int(r.sum_modified or 0),
                                        "removed": int(r.sum_removed or 0)}
            else:
                live_ids.append(r.id)
                if r.status == "done":
                    live_done_labels.add(r.backup_label)

        # Mutáveis: ao vivo. São poucas (as em andamento e as do Cloud).
        live_totals = _totals(db, live_ids)
        for vid in live_ids:
            self.totals[vid] = live_totals.get(vid, (0, 0))
        if live_done_labels:
            lag = version_diff.done_version_lag_sq(db, labels=sorted(live_done_labels))
            win = db.query(lag).filter(lag.c.vid.in_(live_ids)).subquery()
            self.diffs.update(version_diff.diff_counts_by_version(db, win))


# ── Thread de background ────────────────────────────────────────────────────

class Summarizer:
    """Um thread que resume as versões pendentes: no boot (backfill), quando
    acordado (versão fechada, versões apagadas) e a cada INTERVAL, que é a rede
    de segurança — sem nenhum wake o resultado é o mesmo, só mais tarde."""

    INTERVAL = 600
    STARTUP_DELAY = 15  # deixa o boot terminar antes de disputar o banco

    def __init__(self):
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self, session_factory) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, args=(session_factory,),
                                        name="version-summary", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def wake(self) -> None:
        self._wake.set()

    def _loop(self, session_factory) -> None:
        self._stop.wait(self.STARTUP_DELAY)
        while not self._stop.is_set():
            try:
                db = session_factory()
                try:
                    t0 = time.monotonic()
                    done, left = summarize_pending(
                        db, should_continue=lambda: not self._stop.is_set())
                    if done:
                        log.info(f"[version-summary] {done} versão(ões) resumida(s) em "
                                 f"{time.monotonic() - t0:.0f}s"
                                 + (f" — {left} ainda pendente(s)" if left else ""))
                finally:
                    db.close()
            except Exception:  # o loop não pode morrer
                log.exception("[version-summary] ciclo falhou")
            self._wake.wait(self.INTERVAL)
            self._wake.clear()


summarizer = Summarizer()


def wake() -> None:
    summarizer.wake()
