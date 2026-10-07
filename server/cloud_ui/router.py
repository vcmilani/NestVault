"""Front "cloud" — API do drive (estilo OneDrive), montada em /cloud.

Somente leitura. Autentica pelo header X-API-Key ou pelo cookie de sessão emitido
em POST /cloud/session (ver auth.get_user_header_or_cookie).
"""
from pathlib import Path
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from sqlalchemy.orm import Session

from auth import (SESSION_COOKIE, SESSION_TTL, get_current_user, get_user_header_or_cookie,
                  make_session_token, require_owner_or_admin)
from database import (BackupID, BackupVersion, FileContent, User, VersionFile, get_db,
                      TRASHED_STATUS)
from . import content, tree

router = APIRouter()


# -- Sessão -------------------------------------------------------------------

def _is_https(request: Request) -> bool:
    return (request.url.scheme == "https"
            or request.headers.get("x-forwarded-proto", "").lower() == "https")


@router.post("/session")
def create_session(request: Request, response: Response, user: User = Depends(get_current_user)):
    """Troca a API key (header) por um cookie HttpOnly — usado por <img>/<video>."""
    response.set_cookie(SESSION_COOKIE, make_session_token(user), max_age=SESSION_TTL,
                        httponly=True, samesite="strict", secure=_is_https(request), path="/")
    return {"username": user.username, "role": user.role}


@router.delete("/session")
def delete_session(response: Response):
    response.delete_cookie(SESSION_COOKIE, path="/")
    return {"ok": True}


# -- Drive --------------------------------------------------------------------

@router.get("/tree")
def get_tree(label: Optional[str] = None,
             path: str = "",
             version: Optional[str] = None,
             offset: int = Query(0, ge=0),
             limit: int = Query(500, ge=1, le=2000),
             sort: Literal["name", "mtime", "size"] = "name",
             db: Session = Depends(get_db),
             user: User = Depends(get_user_header_or_cookie)):
    """Sem label: a raiz do drive (labels do usuário como pastas). Com label: as
    subpastas e uma página dos arquivos diretos de `path` na versão pedida (padrão:
    a última done)."""
    if not label:
        return {"label": None, "labels": tree.list_labels(db, user)}

    tree.get_label_or_404(db, label, user)
    versions = tree.done_versions(db, label)
    v = tree.pick_version(versions, version)
    base = tree.label_base(db, label, versions)
    rel = tree.norm_rel(path)
    children = tree.list_children(db, v, base, rel, offset, limit, sort)
    return {
        "label": label, "path": rel,
        "version_key": v.version_key, "version_created_at": str(v.created_at),
        "is_latest": v.id == versions[0].id,
        "offset": offset, "limit": limit,
        **children,
    }


@router.get("/versions")
def get_versions(label: str, db: Session = Depends(get_db),
                 user: User = Depends(get_user_header_or_cookie)):
    tree.get_label_or_404(db, label, user)
    return [{"version_key": v.version_key, "created_at": str(v.created_at)}
            for v in tree.done_versions(db, label)]


@router.get("/search")
def search(label: str, q: str = Query(..., min_length=1, max_length=200),
           version: Optional[str] = None,
           limit: int = Query(200, ge=1, le=1000),
           db: Session = Depends(get_db),
           user: User = Depends(get_user_header_or_cookie)):
    tree.get_label_or_404(db, label, user)
    versions = tree.done_versions(db, label)
    v = tree.pick_version(versions, version)
    base = tree.label_base(db, label, versions)
    return {"version_key": v.version_key, "results": tree.search(db, v, base, q, limit)}


@router.get("/history")
def get_history(label: str, path: str, db: Session = Depends(get_db),
                user: User = Depends(get_user_header_or_cookie)):
    """Versões em que o arquivo `path` (relativo à raiz do label) existe."""
    tree.get_label_or_404(db, label, user)
    versions = tree.done_versions(db, label)
    base = tree.label_base(db, label, versions)
    return tree.history(db, label, base + tree.norm_rel(path))


@router.get("/content/{file_id}")
def get_content(file_id: int, request: Request, download: bool = False,
                db: Session = Depends(get_db),
                user: User = Depends(get_user_header_or_cookie)):
    """Conteúdo do arquivo inline (preview) ou como download, com suporte a Range."""
    row = (db.query(VersionFile.original_path, VersionFile.sha256, BackupID.owner_user_id,
                    FileContent.size, FileContent.encrypted)
           .join(BackupVersion, BackupVersion.id == VersionFile.version_id)
           .join(BackupID, BackupID.label == BackupVersion.backup_label)
           .join(FileContent, FileContent.sha256 == VersionFile.sha256)
           .filter(VersionFile.id == file_id,
                   BackupVersion.status != TRASHED_STATUS,
                   tree.live_label_filter())
           .first())
    if not row:
        raise HTTPException(404, "Arquivo nao encontrado")
    require_owner_or_admin(row.owner_user_id, user)
    return content.stream(request, db, sha256=row.sha256, name=Path(row.original_path).name,
                          size=row.size, encrypted=bool(row.encrypted), download=download)
