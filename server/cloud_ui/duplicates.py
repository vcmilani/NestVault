"""Localizador de duplicados do /cloud (v10.2.0).

Arquivos com o mesmo sha256 em mais de um lugar entre os labels do PRÓPRIO
usuário (mesma regra da raiz do drive, tree.list_labels), olhando só a versão
done mais recente de cada label — a pasta do servidor tem uma só (a live), e
versões antigas de backup não são "onde o arquivo está" hoje.

O storage já guarda cada sha256 uma única vez, então apagar uma cópia NÃO libera
disco: o número exibido é espaço lógico repetido. O ganho é organização. Apagar
só vale onde é editável (pastas do servidor, via drive.delete → lixeira); em
backup de cliente a cópia volta no próximo backup, então lá é somente leitura.
"""
from sqlalchemy import func
from sqlalchemy.orm import Session

from database import (BackupID, BackupVersion, FileContent, MediaInfo, User, VersionFile,
                      SERVER_KIND)
from . import tree

DEFAULT_MIN_SIZE = 1024 * 1024
MAX_GROUPS = 500


def _latest_versions(db: Session, labels: list[BackupID]) -> dict[int, BackupID]:
    """{version_id da versão done mais recente: label} — 1 query."""
    by_label = {b.label: b for b in labels}
    latest_sq = (
        db.query(BackupVersion.backup_label, func.max(BackupVersion.created_at).label("ts"))
        .filter(BackupVersion.backup_label.in_(list(by_label)), BackupVersion.status == "done")
        .group_by(BackupVersion.backup_label)
        .subquery()
    )
    rows = (db.query(BackupVersion.backup_label, func.max(BackupVersion.id))
            .join(latest_sq, (BackupVersion.backup_label == latest_sq.c.backup_label)
                  & (BackupVersion.created_at == latest_sq.c.ts))
            .filter(BackupVersion.status == "done")
            .group_by(BackupVersion.backup_label)
            .all())
    return {vid: by_label[label] for label, vid in rows}


def find(db: Session, user: User, min_size: int = DEFAULT_MIN_SIZE,
         limit: int = 100) -> dict:
    labels = (db.query(BackupID)
              .filter(BackupID.owner_user_id == user.id, tree.live_label_filter())
              .all())
    empty = {"groups": [], "group_count": 0, "repeated_bytes": 0, "min_size": min_size}
    if not labels:
        return empty
    versions = _latest_versions(db, labels)
    if not versions:
        return empty
    vids = list(versions)

    cnt = func.count(VersionFile.id)
    agg = (db.query(VersionFile.sha256, FileContent.size, cnt.label("n"))
           .join(FileContent, FileContent.sha256 == VersionFile.sha256)
           .filter(VersionFile.version_id.in_(vids), FileContent.size >= max(1, min_size))
           .group_by(VersionFile.sha256, FileContent.size)
           .having(cnt > 1)
           .all())
    if not agg:
        return empty
    agg.sort(key=lambda r: (r.size * (r.n - 1), r.sha256), reverse=True)
    repeated = sum(r.size * (r.n - 1) for r in agg)
    top = agg[:limit]
    shas = [r.sha256 for r in top]

    members: dict[str, list] = {s: [] for s in shas}
    for r in (db.query(VersionFile.id, VersionFile.version_id, VersionFile.original_path,
                       VersionFile.sha256, VersionFile.mtime)
              .filter(VersionFile.version_id.in_(vids), VersionFile.sha256.in_(shas))
              .all()):
        members[r.sha256].append(r)
    media = {m.sha256: m.kind for m in
             db.query(MediaInfo.sha256, MediaInfo.kind)
             .filter(MediaInfo.sha256.in_(shas), MediaInfo.status == "done",
                     MediaInfo.thumb_sm.isnot(None))}

    # Mesma base da árvore (tree.label_base sobre todas as versões done), para o
    # caminho devolvido abrir direto em /cloud/tree.
    bases: dict[str, str] = {}
    groups = []
    for r in top:
        files = []
        for f in members[r.sha256]:
            b = versions[f.version_id]
            if b.label not in bases:
                bases[b.label] = tree.label_base(db, b.label, tree.done_versions(db, b.label))
            base = bases[b.label]
            rel = f.original_path[len(base):] if f.original_path.startswith(base) else f.original_path
            files.append({
                "file_id": f.id, "label": b.label,
                "drive": b.client_name if b.kind == SERVER_KIND and b.client_name else b.label,
                "kind": b.kind or "backup", "path": rel, "mtime": f.mtime,
                "editable": b.kind == SERVER_KIND,
            })
        files.sort(key=lambda x: (not x["editable"], x["drive"], x["path"]))
        groups.append({"sha256": r.sha256, "size": r.size, "count": r.n,
                       "repeated_bytes": r.size * (r.n - 1),
                       "media": media.get(r.sha256), "files": files})
    return {"groups": groups, "group_count": len(agg), "repeated_bytes": repeated,
            "min_size": min_size}
