"""Front "cloud" — API do drive (estilo OneDrive), montada em /cloud.

Somente leitura. Autentica pelo header X-API-Key ou pelo cookie de sessão emitido
em POST /cloud/session (ver auth.get_user_header_or_cookie).
"""
import logging
from pathlib import Path
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from auth import (SESSION_COOKIE, SESSION_TTL, get_current_user, get_user_header_or_cookie,
                  make_session_token, require_owner_or_admin)
import crypto
import storage
from database import (BackupID, BackupVersion, FileContent, MediaInfo, User, VersionFile, get_db,
                      TRASHED_STATUS)
from . import content, media, tree

router = APIRouter()
log = logging.getLogger("backup-server")


# -- Sessão -------------------------------------------------------------------

def _is_https(request: Request) -> bool:
    return (request.url.scheme == "https"
            or request.headers.get("x-forwarded-proto", "").lower() == "https")


@router.post("/session")
def create_session(request: Request, response: Response, user: User = Depends(get_current_user)):
    """Troca a API key (header) por um cookie HttpOnly — usado por <img>/<video>."""
    response.set_cookie(SESSION_COOKIE, make_session_token(user), max_age=SESSION_TTL,
                        httponly=True, samesite="strict", secure=_is_https(request), path="/")
    log.info(f"[cloud] sessao aberta para {user.username}")
    return {"username": user.username, "role": user.role}


@router.get("/me")
def get_me(user: User = Depends(get_user_header_or_cookie)):
    """Quem é o dono da chave/sessão — as páginas usam o role para decidir o que
    mostrar (usuário comum só navega entre Fotos e Cloud)."""
    return {"username": user.username, "role": user.role}


@router.delete("/session")
def delete_session(response: Response):
    response.delete_cookie(SESSION_COOKIE, path="/")
    log.info("[cloud] sessao encerrada (logout)")
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
                          size=row.size, encrypted=bool(row.encrypted), download=download,
                          who=f"{user.username} file_id={file_id}")


# -- Fotos --------------------------------------------------------------------

@router.get("/photos")
def get_photos(before_ts: Optional[float] = None, before_id: Optional[int] = None,
               limit: int = Query(200, ge=1, le=1000),
               db: Session = Depends(get_db),
               user: User = Depends(get_user_header_or_cookie)):
    """Timeline de fotos e vídeos de todos os backups do usuário (última versão de
    cada um), mais recentes primeiro. Paginação keyset por (before_ts, before_id)."""
    items = media.timeline(db, user)
    chunk = media.page(items, before_ts, before_id, limit)
    nxt = None
    if chunk and len(chunk) == limit:
        nxt = {"before_ts": chunk[-1]["ts"], "before_id": chunk[-1]["id"]}
    return {"items": chunk, "next": nxt, "total": len(items)}


@router.get("/photos/months")
def get_photo_months(db: Session = Depends(get_db),
                     user: User = Depends(get_user_header_or_cookie)):
    return media.months(media.timeline(db, user))


@router.get("/photos/indexing")
def get_indexing(db: Session = Depends(get_db),
                 user: User = Depends(get_user_header_or_cookie)):
    return media.indexing_status(media.timeline(db, user))


@router.get("/photos/labels")
def get_photo_labels(db: Session = Depends(get_db),
                     user: User = Depends(get_user_header_or_cookie)):
    """Backups do usuário e se cada um entra na galeria."""
    return media.photo_labels(db, user)


class PhotoLabelUpdate(BaseModel):
    enabled: bool


@router.put("/photos/labels/{label}")
def set_photo_label(label: str, req: PhotoLabelUpdate, db: Session = Depends(get_db),
                    user: User = Depends(get_current_user)):
    """Liga/desliga um backup na galeria. Escrita: exige o header X-API-Key — o
    cookie de sessão só vale para leitura. A galeria é pessoal, então só o dono
    escolhe (nem o admin mexe na de outro usuário)."""
    b = db.query(BackupID).filter(BackupID.label == label, tree.live_label_filter()).first()
    if not b:
        raise HTTPException(404, f"Backup '{label}' nao encontrado")
    if b.owner_user_id != user.id:
        raise HTTPException(403, "Voce nao tem permissao sobre este backup")
    b.photos_enabled = req.enabled
    db.commit()
    log.info(f"[photos] {user.username}: backup '{label}' "
             f"{'incluido na' if req.enabled else 'removido da'} galeria")
    if req.enabled:
        media.indexer.wake()  # fotos que nunca foram indexadas entram agora
    return {"label": label, "enabled": req.enabled}


@router.get("/thumb/{sha256}")
def get_thumb(sha256: str, request: Request, size: Literal["sm", "lg"] = "sm",
              db: Session = Depends(get_db),
              user: User = Depends(get_user_header_or_cookie)):
    # 404 (e não 403) para conteúdo alheio: não confirma que o sha256 existe.
    mi = db.get(MediaInfo, sha256)
    path = (mi.thumb_sm if size == "sm" else mi.thumb_lg) if mi else None
    if not path or not media.thumb_visible_to(db, user, sha256):
        raise HTTPException(404, "Miniatura nao encontrada")
    p = Path(path)
    if not p.exists():
        # Volume trocado/limpo: descarta o registro para o indexador refazer.
        log.warning(f"[photos] miniatura de {sha256[:8]}… sumiu do disco ({p}) — "
                    f"registro descartado para ser refeito pelo indexador")
        db.delete(mi)
        db.commit()
        media.bump_generation()
        media.indexer.wake()
        raise HTTPException(404, "Miniatura nao encontrada")
    headers = {"Cache-Control": "private, max-age=604800", "ETag": f'"{sha256}-{size}"'}
    if request.headers.get("if-none-match") == headers["ETag"]:
        return Response(status_code=304, headers=headers)
    if mi.thumb_encrypted:
        return StreamingResponse(crypto.decrypt_chunks(p, storage.encryption_key),
                                 media_type="image/jpeg", headers=headers)
    return FileResponse(p, media_type="image/jpeg", headers=headers)
