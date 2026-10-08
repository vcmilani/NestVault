"""Consultas do drive: labels do usuário, filhos de uma pasta, busca e histórico.

Navegação por caminho RELATIVO à "raiz" do label — o prefixo de diretório comum a
todos os arquivos das versões done (ex.: /Users/victor/Pictures/). Sem isso, abrir um
label mostraria a cadeia /Users/victor/... inteira até chegar ao conteúdo. A raiz é
calculada sobre todas as versões done (e não só a última) para que o mesmo caminho
relativo aponte para o mesmo arquivo em qualquer versão — o histórico depende disso.
"""
import os
import threading

from fastapi import HTTPException
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from auth import require_owner_or_admin
from database import (BackupID, BackupVersion, VersionFile, FileContent, User,
                      TRASHED_STATUS)

_base_cache: dict[tuple, str] = {}
_base_lock = threading.Lock()


def live_label_filter():
    """BackupID fora da lixeira (status NULL em linhas antigas conta como vivo)."""
    return or_(BackupID.status.is_(None), BackupID.status != TRASHED_STATUS)


def norm_rel(path: str | None) -> str:
    """'/a//b/' → 'a/b'. Caminho relativo à raiz do label, sem barras nas pontas."""
    return "/".join(p for p in (path or "").split("/") if p)


def get_label_or_404(db: Session, label: str, user: User) -> BackupID:
    b = db.query(BackupID).filter(BackupID.label == label, live_label_filter()).first()
    if not b:
        raise HTTPException(404, f"Backup '{label}' nao encontrado")
    require_owner_or_admin(b.owner_user_id, user)
    return b


def done_versions(db: Session, label: str) -> list[BackupVersion]:
    """Versões done do label, mais recente primeiro (mesmo critério de /backups)."""
    return (db.query(BackupVersion)
            .filter(BackupVersion.backup_label == label, BackupVersion.status == "done")
            .order_by(BackupVersion.created_at.desc(), BackupVersion.id.desc())
            .all())


def pick_version(versions: list[BackupVersion], version_key: str | None) -> BackupVersion:
    if not versions:
        raise HTTPException(404, "Backup ainda nao tem versao concluida")
    if not version_key:
        return versions[0]
    for v in versions:
        if v.version_key == version_key:
            return v
    raise HTTPException(404, f"Versao '{version_key}' nao encontrada")


def label_base(db: Session, label: str, versions: list[BackupVersion]) -> str:
    """Prefixo de diretório comum (com '/' final, ou '') a todas as versões done.

    Todo caminho s entre min e max (ordem lexicográfica) compartilha o prefixo comum
    de min e max — então bastam MIN/MAX por versão, que o índice único
    (version_id, original_path) resolve sem varrer as linhas. Cacheado pelo conjunto
    de versões done: muda quando entra versão nova ou a limpeza apaga uma."""
    key = (label, tuple(sorted(v.id for v in versions)))
    with _base_lock:
        if key in _base_cache:
            return _base_cache[key]

    lows, highs = [], []
    for v in versions:
        # Uma agregação por query: o SQLite só usa o atalho de índice para MIN/MAX
        # quando ele é o único agregado do SELECT.
        lo = db.query(func.min(VersionFile.original_path)).filter(VersionFile.version_id == v.id).scalar()
        if lo is None:
            continue
        hi = db.query(func.max(VersionFile.original_path)).filter(VersionFile.version_id == v.id).scalar()
        lows.append(lo)
        highs.append(hi)
    if not lows:
        base = ""
    else:
        common = os.path.commonprefix([min(lows), max(highs)])
        base = common[:common.rfind("/") + 1]

    with _base_lock:
        if len(_base_cache) > 512:
            _base_cache.clear()
        _base_cache[key] = base
    return base


def _instr(db: Session, haystack, needle: str):
    # instr() é SQLite; no PostgreSQL a mesma função se chama strpos().
    if db.bind.dialect.name == "postgresql":
        return func.strpos(haystack, needle)
    return func.instr(haystack, needle)


def list_labels(db: Session, user: User) -> list[dict]:
    """Raiz do drive: os labels do PRÓPRIO usuário (inclusive admin — o drive é
    pessoal; o explorer continua sendo a visão administrativa)."""
    labels = (db.query(BackupID)
              .filter(BackupID.owner_user_id == user.id, live_label_filter())
              .order_by(BackupID.label)
              .all())
    if not labels:
        return []
    latest = dict(
        db.query(BackupVersion.backup_label, func.max(BackupVersion.created_at))
        .filter(BackupVersion.backup_label.in_([b.label for b in labels]),
                BackupVersion.status == "done")
        .group_by(BackupVersion.backup_label)
        .all()
    )
    return [
        {"label": b.label, "client_name": b.client_name,
         "last_backup_at": str(latest[b.label]) if latest.get(b.label) else None}
        for b in labels
    ]


_SORTS = {
    "name":  lambda: [VersionFile.original_path.asc()],
    "mtime": lambda: [VersionFile.mtime.desc(), VersionFile.original_path.asc()],
    "size":  lambda: [FileContent.size.desc(), VersionFile.original_path.asc()],
}


def list_children(db: Session, version: BackupVersion, base: str, rel: str,
                  offset: int, limit: int, sort: str) -> dict:
    """Subpastas (agregadas no SQL) + uma página de arquivos diretos de `rel`."""
    prefix = base + (rel + "/" if rel else "")
    rest   = func.substr(VersionFile.original_path, len(prefix) + 1)
    slash  = _instr(db, rest, "/")
    in_dir = (VersionFile.version_id == version.id,
              VersionFile.original_path.startswith(prefix, autoescape=True))

    name = func.substr(rest, 1, slash - 1)
    folders = (
        db.query(name.label("name"),
                 func.count(VersionFile.id).label("count"),
                 func.coalesce(func.sum(FileContent.size), 0).label("size"),
                 func.max(VersionFile.mtime).label("mtime"))
        .outerjoin(FileContent, FileContent.sha256 == VersionFile.sha256)
        .filter(*in_dir, slash > 0)
        .group_by(name)
        .order_by(name)
        .all()
    )

    direct = (*in_dir, slash == 0)
    total = db.query(func.count(VersionFile.id)).filter(*direct).scalar() or 0
    rows = (
        db.query(VersionFile.id, VersionFile.original_path, VersionFile.sha256,
                 VersionFile.mtime, FileContent.size)
        .outerjoin(FileContent, FileContent.sha256 == VersionFile.sha256)
        .filter(*direct)
        .order_by(*_SORTS.get(sort, _SORTS["name"])())
        .offset(offset).limit(limit)
        .all()
    )
    return {
        "folders": [{"name": f.name, "count": f.count, "size": int(f.size), "mtime": f.mtime}
                    for f in folders if f.name],
        "files": [file_entry(r, prefix) for r in rows],
        "total_files": total,
    }


def file_entry(r, strip: str) -> dict:
    path = r.original_path[len(strip):] if r.original_path.startswith(strip) else r.original_path
    return {"id": r.id, "name": path.rsplit("/", 1)[-1], "path": path,
            "sha256": r.sha256, "size": r.size or 0, "mtime": r.mtime}


def search(db: Session, version: BackupVersion, base: str, q: str, limit: int) -> list[dict]:
    """Arquivos cujo caminho (relativo à raiz do label) contém `q`, sem diferenciar caixa."""
    rows = (
        db.query(VersionFile.id, VersionFile.original_path, VersionFile.sha256,
                 VersionFile.mtime, FileContent.size)
        .outerjoin(FileContent, FileContent.sha256 == VersionFile.sha256)
        .filter(VersionFile.version_id == version.id,
                VersionFile.original_path.startswith(base, autoescape=True),
                func.lower(func.substr(VersionFile.original_path, len(base) + 1))
                    .contains(q.lower(), autoescape=True))
        .order_by(VersionFile.original_path)
        .limit(limit)
        .all()
    )
    return [file_entry(r, base) for r in rows]


def history(db: Session, label: str, full_path: str) -> list[dict]:
    """Uma linha por versão done que contém o arquivo, mais recente primeiro.
    `changed` marca onde o conteúdo difere da versão anterior que também o tinha."""
    rows = (
        db.query(BackupVersion.version_key, BackupVersion.created_at,
                 VersionFile.id, VersionFile.sha256, VersionFile.mtime, FileContent.size)
        .join(VersionFile, VersionFile.version_id == BackupVersion.id)
        .outerjoin(FileContent, FileContent.sha256 == VersionFile.sha256)
        .filter(BackupVersion.backup_label == label,
                BackupVersion.status == "done",
                VersionFile.original_path == full_path)
        .order_by(BackupVersion.created_at.desc(), BackupVersion.id.desc())
        .all()
    )
    out = []
    for i, r in enumerate(rows):
        older = rows[i + 1] if i + 1 < len(rows) else None
        out.append({
            "version_key": r.version_key, "created_at": str(r.created_at),
            "file_id": r.id, "sha256": r.sha256, "size": r.size or 0, "mtime": r.mtime,
            "changed": older is None or older.sha256 != r.sha256,
        })
    return out
