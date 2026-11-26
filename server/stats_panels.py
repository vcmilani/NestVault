"""Painéis globais das stats, recalculados só quando os dados de que dependem mudam.

Três números dependem do conjunto inteiro de versões ("qual é a mais recente de
cada label") e não saem do resumo por versão (version_summary.py):

- kept          — bytes retidos pelas versões mais recentes (o "liberável" total
                  é o store inteiro menos isto; ver main._get_reclaimable_bytes)
- reclaimable   — espaço liberável por label (versões done antigas)
- exclusive     — arquivos exclusivos da versão mais recente de cada label

Eles só mudam quando muda o conjunto de versões done, ou o conteúdo das versões
do Cloud (a "live" de uma pasta do servidor fica done e recebe arquivos). Cada
painel guarda a impressão digital desses dados junto do resultado: abrir a página
mil vezes sem nada mudar não recalcula nada. Mudou, o recálculo vai para um
thread, respeitando um intervalo mínimo proporcional ao custo do último cálculo,
para uma madrugada de backups não emendar um recálculo no outro.
"""
import logging
import threading
import time
from typing import Callable, Optional

from sqlalchemy import and_, case, exists, func, select
from sqlalchemy.orm import Session, aliased

from database import BackupID, BackupVersion, FileContent, VersionFile, SERVER_KIND

log = logging.getLogger("backup-server")

# tests/conftest liga: calcula no próprio request, sem thread (resultado determinístico).
SYNC = False

MIN_INTERVAL = 60.0     # piso do intervalo entre recálculos disparados por mudança
BUILD_FACTOR = 3.0      # intervalo = max(MIN_INTERVAL, BUILD_FACTOR × duração do último)
MAX_AGE = 6 * 3600.0    # rede de segurança: recalcula mesmo sem mudança detectada


def fingerprint(db: Session) -> tuple:
    """Barata: um agregado de backup_versions (centenas de linhas) e um do conteúdo
    das versões do Cloud. count + max + soma dos ids detecta versão criada,
    apagada ou que entrou/saiu de done; nas do Cloud, count + max(id) + soma dos
    mtime detecta arquivo novo, apagado ou substituído (o upsert troca o mtime)."""
    done = db.query(func.count(BackupVersion.id), func.max(BackupVersion.id),
                    func.coalesce(func.sum(BackupVersion.id), 0)
                    ).filter(BackupVersion.status == "done").one()
    server_vids = (select(BackupVersion.id)
                   .join(BackupID, BackupID.label == BackupVersion.backup_label)
                   .where(BackupID.kind == SERVER_KIND, BackupVersion.status == "done"))
    srv = db.query(func.count(VersionFile.id), func.max(VersionFile.id),
                   func.coalesce(func.sum(VersionFile.mtime), 0.0)
                   ).filter(VersionFile.version_id.in_(server_vids)).one()
    return (tuple(done), (srv[0], srv[1], round(float(srv[2]), 3)))


# ── Cálculos ────────────────────────────────────────────────────────────────

def _latest_by_id(db: Session) -> list:
    return (db.query(BackupVersion.backup_label.label("label"),
                     func.max(BackupVersion.id).label("latest_id"))
            .filter(BackupVersion.status == "done")
            .group_by(BackupVersion.backup_label).all())


def _names(db: Session, labels) -> dict:
    labels = list(labels)
    if not labels:
        return {}
    return {r.label: r.client_name for r in
            db.query(BackupID.label, BackupID.client_name).filter(BackupID.label.in_(labels)).all()}


def compute_kept(db: Session) -> int:
    """Bytes que as versões mantidas (a done mais recente por created_at de cada
    label) ainda usam. A varredura parte do conjunto pequeno (shas retidos) e entra
    em file_contents pela PK — o LEFT JOIN de file_contents inteiro contra o
    subquery levava o SQLite a minutos de CPU."""
    latest_ts_sq = (
        db.query(BackupVersion.backup_label, func.max(BackupVersion.created_at).label("latest_ts"))
        .filter(BackupVersion.status == "done")
        .group_by(BackupVersion.backup_label)
        .subquery()
    )
    keeper_ids = [r.id for r in
                  db.query(BackupVersion.id)
                  .join(latest_ts_sq,
                        (BackupVersion.backup_label == latest_ts_sq.c.backup_label) &
                        (BackupVersion.created_at == latest_ts_sq.c.latest_ts))
                  .all()]
    if not keeper_ids:
        return 0
    kept_sq = (db.query(VersionFile.sha256.label("sha256"))
               .filter(VersionFile.version_id.in_(keeper_ids))
               .distinct().subquery())
    return int(db.query(func.coalesce(func.sum(FileContent.size), 0))
               .select_from(kept_sq)
               .join(FileContent, FileContent.sha256 == kept_sq.c.sha256)
               .scalar() or 0)


def compute_reclaimable_by_label(db: Session) -> list[dict]:
    """Espaço liberável por label: conteúdo das versões done antigas que nenhuma
    versão mais recente (de label nenhum) usa.

    Duas etapas: primeiro os pares DISTINCT (label, sha256) das versões antigas,
    depois o NOT EXISTS contra as mais recentes (coberto por idx_vf_sha_version)
    sobre esses pares. Um subquery com DISTINCT não é achatado pelo planejador,
    então a sondagem roda uma vez por conteúdo distinto de cada label, e não por
    linha de version_files."""
    latest = _latest_by_id(db)
    latest_ids = [r.latest_id for r in latest]
    if not latest_ids:
        return []
    old_pairs_sq = (
        db.query(BackupVersion.backup_label.label("label"), VersionFile.sha256.label("sha256"))
        .join(VersionFile, VersionFile.version_id == BackupVersion.id)
        .filter(BackupVersion.status == "done", BackupVersion.id.notin_(latest_ids))
        .distinct().subquery()
    )
    kf = aliased(VersionFile)
    size_rows = (
        db.query(old_pairs_sq.c.label.label("label"),
                 func.coalesce(func.sum(FileContent.size), 0).label("size"))
        .join(FileContent, FileContent.sha256 == old_pairs_sq.c.sha256)
        .filter(~exists().where(and_(kf.sha256 == old_pairs_sq.c.sha256,
                                     kf.version_id.in_(latest_ids))))
        .group_by(old_pairs_sq.c.label)
        .order_by(func.coalesce(func.sum(FileContent.size), 0).desc())
        .all()
    )
    cnt = {r.label: int(r.cnt) for r in
           db.query(BackupVersion.backup_label.label("label"),
                    func.count(BackupVersion.id.distinct()).label("cnt"))
           .filter(BackupVersion.status == "done", BackupVersion.id.notin_(latest_ids))
           .group_by(BackupVersion.backup_label).all()}
    names = _names(db, [r.label for r in latest])
    return [
        {"label": r.label, "client_name": names.get(r.label),
         "old_version_count": cnt.get(r.label, 0),
         "old_versions_size_bytes": int(r.size)}
        for r in size_rows if int(r.size) > 0
    ]


def compute_exclusive_by_label(db: Session) -> list[dict]:
    """Arquivo exclusivo = conteúdo da versão mais recente de um label que não
    aparece na mais recente de nenhum outro. NOT EXISTS coberto por idx_vf_sha_version."""
    latest = _latest_by_id(db)
    latest_ids = [r.latest_id for r in latest]
    if not latest_ids:
        return []
    of = aliased(VersionFile)
    excl_flag = case(
        (exists().where(and_(of.sha256 == VersionFile.sha256,
                             of.version_id.in_(latest_ids),
                             of.version_id != VersionFile.version_id)), 0),
        else_=1,
    )
    sq = (db.query(VersionFile.version_id.label("vid"), FileContent.size.label("size"),
                   excl_flag.label("excl"))
          .join(FileContent, FileContent.sha256 == VersionFile.sha256)
          .filter(VersionFile.version_id.in_(latest_ids))
          .subquery())
    rows = (db.query(sq.c.vid, func.count().label("fc"),
                     func.coalesce(func.sum(sq.c.excl), 0).label("ec"),
                     func.coalesce(func.sum(sq.c.excl * sq.c.size), 0).label("es"))
            .group_by(sq.c.vid).all())
    by_vid = {r.vid: (int(r.fc), int(r.ec), int(r.es)) for r in rows}
    names = _names(db, [r.label for r in latest])
    out = []
    for r in latest:
        fc, ec, es = by_vid.get(r.latest_id, (0, 0, 0))
        if not fc:
            continue
        out.append({"label": r.label, "client_name": names.get(r.label),
                    "file_count": fc, "exclusive_file_count": ec,
                    "exclusive_pct": round(ec / fc * 100, 1),
                    "exclusive_size_bytes": es})
    out.sort(key=lambda e: e["exclusive_pct"], reverse=True)
    return out


# ── Cache por painel ────────────────────────────────────────────────────────

class Panel:
    def __init__(self, name: str, compute: Callable[[Session], object],
                 inline_first: bool, inline_max_s: float = 0.0):
        self.name = name
        self._compute = compute
        # Sem valor nenhum ainda: calcula no request (kept: /storage/info e a
        # Atividade sempre mostraram o número calculado na hora) em vez de
        # responder "calculando".
        self._inline_first = inline_first
        # Recalcula no próprio request, a cada mudança, enquanto o último cálculo
        # levou até isto: num acervo pequeno o número reflete uma exclusão na hora.
        self._inline_max_s = inline_max_s
        self._lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        self.data = None
        self.fp = None
        self.ts = 0.0
        self.build_s = 0.0
        self.computing = False
        self.error: Optional[str] = None

    def _min_interval(self) -> float:
        return max(MIN_INTERVAL, BUILD_FACTOR * self.build_s)

    def _run(self, db: Session, fp: tuple) -> None:
        """Calcula e grava. Quem chama segura self._lock."""
        t0 = time.monotonic()
        try:
            data = self._compute(db)
        except Exception as e:
            db.rollback()
            self.error = f"{e.__class__.__name__}: {e}"
            log.exception(f"[stats] painel {self.name}: falha no cálculo")
            return
        now = time.monotonic()
        self.data, self.fp, self.ts, self.build_s, self.error = data, fp, now, now - t0, None
        if self.build_s > 30:
            log.info(f"[stats] painel {self.name}: recálculo levou {self.build_s:.0f}s")

    def _start_background(self, session_factory) -> None:
        if not self._lock.acquire(blocking=False):
            return  # já há um cálculo em andamento
        self.computing = True

        def work():
            try:
                db = session_factory()
                try:
                    self._run(db, fingerprint(db))
                finally:
                    db.close()
            except Exception:
                log.exception(f"[stats] painel {self.name}: thread falhou")
            finally:
                self.computing = False
                self._lock.release()

        threading.Thread(target=work, name=f"stats-{self.name}", daemon=True).start()

    def get(self, db: Session, session_factory, fp: Optional[tuple] = None):
        """Valor atual do painel, disparando recálculo se os dados mudaram."""
        if fp is None:
            fp = fingerprint(db)
        now = time.monotonic()
        stale = self.data is None or fp != self.fp or now - self.ts > MAX_AGE
        if stale:
            inline = (SYNC
                      or (self.data is None and self._inline_first)
                      or (self.data is not None and self.build_s <= self._inline_max_s))
            if inline:
                with self._lock:
                    if self.data is None or fp != self.fp or time.monotonic() - self.ts > MAX_AGE:
                        self._run(db, fp)
            elif self.data is None or now - self.ts >= self._min_interval():
                self._start_background(session_factory)
        return self.data

    def refresh(self, db: Session, session_factory) -> None:
        """Botão ↻ do painel: recalcula já, ignorando o intervalo mínimo."""
        if SYNC:
            with self._lock:
                self._run(db, fingerprint(db))
        else:
            self._start_background(session_factory)

    def state(self) -> dict:
        age = max(0.0, time.monotonic() - self.ts) if self.data is not None else None
        return {"age_seconds": round(age, 1) if age is not None else None,
                "computing": self.computing, "error": self.error}


PANELS: dict[str, Panel] = {
    "kept":        Panel("kept", compute_kept, inline_first=True, inline_max_s=2.0),
    "reclaimable": Panel("reclaimable", compute_reclaimable_by_label, inline_first=False),
    "exclusive":   Panel("exclusive", compute_exclusive_by_label, inline_first=False),
}


def reset_all() -> None:
    for p in PANELS.values():
        p.reset()
