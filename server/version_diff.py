"""Contagem de diffs entre versões (added/modified/removed), calculada no banco.

Compartilhado por /api/stats (changes_days), /api/activity (recent_versions) e o
digest diário. Cada um desses montava, por conta própria, dicts
{original_path: sha256} com as versões INTEIRAS em memória só para comparar —
no banco descrito no CHANGELOG v7.12 (525k version_files) isso são centenas de
MB por recálculo, num processo que também serve upload.

As contagens saem destas identidades, sem trazer nenhuma linha de version_files
para o Python:

    added    = total_atual    - same_path
    removed  = total_anterior - same_path
    modified = same_path      - same_both

onde same_path = arquivos presentes nas duas versões (mesmo original_path) e
same_both = presentes nas duas E com o mesmo sha256. Elas valem porque
(version_id, original_path) é único (uq_version_path) e sha256 é NOT NULL —
sem isso um caminho duplicado ou um sha nulo quebraria a aritmética.
"""
from typing import Iterable, Optional, Sequence

from sqlalchemy import and_, case, func
from sqlalchemy.orm import Session, aliased

from database import BackupVersion, VersionFile


def done_version_lag_sq(db: Session, labels: Optional[Sequence[str]] = None):
    """Subquery (vid, ts, prev_id): para cada versão done, a done imediatamente
    anterior do mesmo label, na ordem de version_key.

    LAG exige SQLite >= 3.25. `labels`, quando informado, restringe a varredura
    aos labels que o chamador já sabe que interessam.
    """
    q = db.query(
        BackupVersion.id.label("vid"),
        BackupVersion.created_at.label("ts"),
        func.lag(BackupVersion.id).over(
            partition_by=BackupVersion.backup_label,
            order_by=BackupVersion.version_key,
        ).label("prev_id"),
    ).filter(BackupVersion.status == "done")
    if labels is not None:
        q = q.filter(BackupVersion.backup_label.in_(list(labels)))
    return q.subquery()


def file_counts_by_version(db: Session, version_ids: Iterable[int]) -> dict[int, int]:
    """Número de arquivos de cada versão, numa query só."""
    ids = list(version_ids)
    if not ids:
        return {}
    return {
        r.version_id: int(r.n)
        for r in db.query(VersionFile.version_id, func.count().label("n"))
        .filter(VersionFile.version_id.in_(ids))
        .group_by(VersionFile.version_id)
        .all()
    }


def diff_counts_by_version(db: Session, win_sq) -> dict[int, dict[str, int]]:
    """added/modified/removed de cada versão de `win_sq` contra sua prev_id.

    `win_sq` precisa expor as colunas `vid` e `prev_id` — tipicamente vinda de
    done_version_lag_sq(), possivelmente já filtrada pelo chamador. Versões sem
    predecessora (prev_id NULL) contam todos os arquivos como adicionados.
    """
    rows = db.query(win_sq.c.vid, win_sq.c.prev_id).all()
    if not rows:
        return {}

    ids = {r.vid for r in rows} | {r.prev_id for r in rows if r.prev_id is not None}
    total_by_vid = file_counts_by_version(db, ids)

    # Interseção por caminho entre cada versão e sua predecessora, num único
    # join (coberto pelo índice único uq_version_path).
    cf, pf = aliased(VersionFile), aliased(VersionFile)
    match_by_vid = {
        r.vid: (int(r.same_path), int(r.same_both))
        for r in db.query(
            win_sq.c.vid.label("vid"),
            func.count().label("same_path"),
            func.coalesce(
                func.sum(case((cf.sha256 == pf.sha256, 1), else_=0)), 0
            ).label("same_both"),
        )
        .select_from(win_sq)
        .join(cf, cf.version_id == win_sq.c.vid)
        .join(pf, and_(pf.version_id == win_sq.c.prev_id,
                       pf.original_path == cf.original_path))
        .group_by(win_sq.c.vid)
        .all()
    }

    out: dict[int, dict[str, int]] = {}
    for r in rows:
        cur_total = total_by_vid.get(r.vid, 0)
        if r.prev_id is None:
            out[r.vid] = {"added": cur_total, "modified": 0, "removed": 0}
        else:
            same_path, same_both = match_by_vid.get(r.vid, (0, 0))
            out[r.vid] = {
                "added":    cur_total - same_path,
                "removed":  total_by_vid.get(r.prev_id, 0) - same_path,
                "modified": same_path - same_both,
            }
    return out
