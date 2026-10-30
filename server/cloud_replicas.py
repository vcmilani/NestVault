"""Replicação dedicada do Cloud (v10.3.0).

Os arquivos das pastas do servidor do /cloud são editados só pelo navegador —
não têm outra cópia numa máquina cliente, como um backup tem. Por isso podem ter
mais réplicas que o resto: cloud.replication_factor (0 = o do storage; nunca
abaixo dele, ver storage._factor).

Regra de um sha256 (cloud_sha_set): exige o fator do Cloud se
- está na versão live de alguma pasta do servidor fora da lixeira; ou
- só existe em pastas do servidor (lixeira dela, ou a pasta inteira na lixeira
  do admin) — enquanto puder ser restaurado continua tão protegido quanto antes.
Saiu do Cloud e existe em algum backup: volta ao fator do storage, e as cópias
extras são removidas (o backup já é a outra proteção dele).

Quem muda o que está no Cloud (upload, cópia, lixeira, restauração, purge) chama
apply() com os sha256 afetados; reconcile_all() é a rede de segurança da limpeza
noturna e da troca do fator em /settings.
"""
import logging
import os
import threading
import time
from pathlib import Path
from typing import Callable, Iterable

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

import storage
from database import (BackupID, BackupVersion, FileContent, FileContentCopy, VersionFile,
                      SERVER_KIND, SERVER_LIVE_KEY, TRASHED_STATUS)

log = logging.getLogger("backup-server")

_CHUNK = 500

# Sessão das tarefas em background. main.py aponta para o SessionLocal dele (que
# os testes trocam pelo banco do teste); sem isso, usa o do database.
session_factory: Callable[[], Session] | None = None


def _session() -> Session:
    if session_factory is not None:
        return session_factory()
    from database import SessionLocal
    return SessionLocal()


def _chunks(items: list[str]):
    for i in range(0, len(items), _CHUNK):
        yield items[i:i + _CHUNK]


def _live_label():
    return or_(BackupID.status.is_(None), BackupID.status != TRASHED_STATUS)


def _shas(db: Session, *filters, shas: list[str] | None = None) -> set[str]:
    base = (db.query(VersionFile.sha256).distinct()
            .join(BackupVersion, BackupVersion.id == VersionFile.version_id)
            .join(BackupID, BackupID.label == BackupVersion.backup_label)
            .filter(*filters))
    if shas is None:
        return {r[0] for r in base.all()}
    out: set[str] = set()
    for chunk in _chunks(shas):
        out.update(r[0] for r in base.filter(VersionFile.sha256.in_(chunk)).all())
    return out


def cloud_sha_set(db: Session, shas: Iterable[str] | None = None) -> set[str]:
    """Os sha256 (entre `shas`, ou todos) que exigem o fator do Cloud."""
    shas = None if shas is None else list(set(shas))
    if shas == []:
        return set()
    live = _shas(db, BackupID.kind == SERVER_KIND, _live_label(),
                 BackupVersion.version_key == SERVER_LIVE_KEY, BackupVersion.status == "done",
                 shas=shas)
    server = _shas(db, BackupID.kind == SERVER_KIND, shas=shas)
    candidates = sorted(server - live)
    if not candidates:
        return live
    elsewhere = _shas(db, BackupID.kind != SERVER_KIND, shas=candidates)
    return live | (set(candidates) - elsewhere)


def _readable(copies: list[FileContentCopy], expected: int) -> list[FileContentCopy]:
    """Cópias no disco com o tamanho esperado, fora de volume degraded."""
    degraded = {str(v) for v in storage._degraded_volumes}
    out = []
    for c in copies:
        if c.volume_path in degraded:
            continue
        try:
            if os.stat(c.stored_at).st_size == expected:
                out.append(c)
        except OSError:
            pass
    return out


def _plan(db: Session, shas: list[str]) -> list[tuple[str, int, int, bool]]:
    """(sha256, tamanho, nº de cópias, é_cloud) dos que estão fora do alvo."""
    cloud = cloud_sha_set(db, shas)
    create = {False: storage.target_replicas(), True: storage.target_replicas(cloud=True)}
    keep = {False: storage.configured_replicas(), True: storage.configured_replicas(cloud=True)}
    out = []
    for chunk in _chunks(shas):
        rows = (db.query(FileContent.sha256, FileContent.size, func.count(FileContentCopy.id))
                .outerjoin(FileContentCopy, FileContentCopy.sha256 == FileContent.sha256)
                .filter(FileContent.sha256.in_(chunk))
                .group_by(FileContent.sha256, FileContent.size).all())
        for sha, size, cnt in rows:
            is_cloud = sha in cloud
            if cnt < create[is_cloud] or cnt > keep[is_cloud]:
                out.append((sha, size or 0, cnt, is_cloud))
    return out


def _fix_one(db: Session, sha: str, is_cloud: bool) -> tuple[int, int]:
    """Leva um sha256 ao alvo dele. Devolve (cópias criadas, cópias removidas)."""
    fc = db.get(FileContent, sha)
    if fc is None or fc.quarantined_at is not None:
        return 0, 0
    copies = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha).all()
    keep = storage.configured_replicas(cloud=is_cloud)
    if len(copies) > keep:
        if storage._degraded_volumes:
            return 0, 0  # mesma trava do cleanup_excess_copies: só apaga com todos os discos no ar
        return 0, storage.trim_copies(db, fc, copies, keep)
    if len(copies) < storage.target_replicas(cloud=is_cloud):
        expected = storage.expected_stored_size(fc.size, bool(fc.encrypted))
        source = next(iter(_readable(copies, expected)), None)
        if source is None:
            log.warning(f"[cloud-replicas] {sha[:8]}… sem cópia legível — não dá para replicar")
            return 0, 0
        return storage.ensure_replicas(sha, Path(source.stored_at), db, cloud=is_cloud), 0
    return 0, 0


def apply(db: Session, shas: Iterable[str], on_progress=None, prefix: str = "") -> tuple[int, int]:
    """Cria ou remove cópias de `shas` até o alvo de cada um. Faz commit.
    Devolve (cópias criadas, cópias removidas)."""
    work = _plan(db, sorted(set(shas)))
    if not work:
        return 0, 0
    total_bytes = sum(size * max(0, storage.target_replicas(cloud=c) - cnt) for _, size, cnt, c in work)
    progress = storage.ReplicationProgress("cloud-replicas", len(work), total_bytes, db,
                                           on_progress, prefix) if on_progress else None
    added = removed = 0
    for sha, size, _, is_cloud in work:
        a, r = _fix_one(db, sha, is_cloud)
        added += a
        removed += r
        if progress:
            progress.tick(copies=a, nbytes=size * a)
    db.commit()
    if progress:
        progress.emit(force_log=True)
    if added or removed:
        log.info(f"[cloud-replicas] {len(work)} conteúdo(s) fora do alvo — "
                 f"{added} cópia(s) criada(s), {removed} removida(s)")
    return added, removed


def apply_bg(shas: Iterable[str]) -> None:
    """apply() numa sessão própria — para BackgroundTasks, depois da resposta."""
    shas = list(shas)
    if not shas:
        return
    db = _session()
    try:
        apply(db, shas)
    except Exception:
        db.rollback()
        log.exception(f"[cloud-replicas] falha ao ajustar as réplicas de {len(shas)} conteúdo(s)")
    finally:
        db.close()


def label_shas(db: Session, label: str) -> set[str]:
    """Todos os sha256 de um label (para quando uma pasta do servidor inteira muda)."""
    return _shas(db, BackupID.label == label)


def reconcile_all(db: Session, on_progress=None) -> tuple[int, int]:
    """Confere tudo: os conteúdos do Cloud e os que têm cópias além do fator do
    storage (que podem ser sobras de um arquivo que saiu do Cloud)."""
    over = {r[0] for r in (db.query(FileContentCopy.sha256)
                           .group_by(FileContentCopy.sha256)
                           .having(func.count(FileContentCopy.id) > storage.configured_replicas())
                           .all())}
    return apply(db, over | cloud_sha_set(db), on_progress=on_progress, prefix="Cloud — ")


def reconcile_job() -> None:
    """reconcile_all() como MaintenanceJob "cloud-replication" (Atividade, com %)."""
    from cache_state import invalidate_activity
    from database import MaintenanceJob
    from datetime import datetime
    db = _session()
    mj_id = None
    try:
        mj = MaintenanceJob(job_type="cloud-replication", status="running",
                            summary="Ajustando as réplicas do Cloud...")
        db.add(mj)
        db.commit()
        mj_id = mj.id
        invalidate_activity()

        def _progress(msg: str) -> None:
            job = db.get(MaintenanceJob, mj_id)
            if job:
                job.summary = msg
                db.commit()
                invalidate_activity()

        added, removed = reconcile_all(db, on_progress=_progress)
        mj = db.get(MaintenanceJob, mj_id)
        if mj:
            mj.status = "done"
            mj.finished_at = datetime.now()
            mj.summary = (f"{added} cópia(s) criada(s), {removed} removida(s) — fator do Cloud: "
                          f"{storage.target_replicas(cloud=True)}, do storage: "
                          f"{storage.target_replicas()}")
            db.commit()
    except Exception as e:
        log.exception("[cloud-replicas] erro ao reconciliar as réplicas do Cloud")
        db.rollback()
        mj = db.get(MaintenanceJob, mj_id) if mj_id is not None else None
        if mj:
            mj.status = "error"
            mj.finished_at = datetime.now()
            mj.summary = f"erro — {e}"
            db.commit()
    finally:
        invalidate_activity()
        db.close()


def start_reconcile() -> None:
    threading.Thread(target=reconcile_job, name="cloud-replication", daemon=True).start()


# -- Espaço disponível (barra do /cloud) ---------------------------------------

_FREE_TTL = 60.0
_free_lock = threading.Lock()
_free_cache: dict = {"value": 0, "ts": 0.0}


def reset() -> None:
    with _free_lock:
        _free_cache.update({"value": 0, "ts": 0.0})


def storage_free_bytes() -> int:
    """Quanto ainda cabe no Cloud: o livre acima do limiar de cada disco saudável,
    dividido pelo nº de cópias que um arquivo novo do Cloud ganha."""
    with _free_lock:
        if time.monotonic() - _free_cache["ts"] < _FREE_TTL:
            return _free_cache["value"]
    threshold = storage.STORAGE_FALLBACK_THRESHOLD_GB * 1024 ** 3
    free = 0
    for vol in storage.healthy_volumes():
        usage = storage.safe_disk_usage(vol)
        if usage:
            free += max(0, usage.free - threshold)
    value = int(free // max(1, storage.target_replicas(cloud=True)))
    with _free_lock:
        _free_cache.update({"value": value, "ts": time.monotonic()})
    return value
