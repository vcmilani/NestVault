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

from auth import (HIDDEN_COOKIE, HIDDEN_TTL, SESSION_COOKIE, SESSION_TTL, effective_role,
                  get_current_user, get_user_header_or_cookie, hidden_unlocked,
                  make_hidden_token, make_session_token, pin_locked_for,
                  register_pin_attempt, require_owner_or_admin)
import crypto
import storage
from database import (BackupID, BackupVersion, FileContent, MediaInfo, User, VersionFile, get_db,
                      TRASHED_STATUS, check_pin)
from . import content, media, tree

router = APIRouter()
log = logging.getLogger("backup-server")


# -- Sessão -------------------------------------------------------------------

def _is_https(request: Request) -> bool:
    return (request.url.scheme == "https"
            or request.headers.get("x-forwarded-proto", "").lower() == "https")


@router.post("/session")
def create_session(request: Request, response: Response, db: Session = Depends(get_db),
                   user: User = Depends(get_current_user)):
    """Troca a API key (header) por um cookie HttpOnly — usado por <img>/<video>.
    O cookie herda o escopo da chave: aberto com chave de cliente, não é admin."""
    response.set_cookie(SESSION_COOKIE, make_session_token(db, user), max_age=SESSION_TTL,
                        httponly=True, samesite="strict", secure=_is_https(request), path="/")
    log.info(f"[cloud] sessao aberta para {user.username} (chave {user.key_scope})")
    return {"username": user.username, "role": effective_role(user), "scope": user.key_scope}


@router.get("/me")
def get_me(user: User = Depends(get_user_header_or_cookie)):
    """Quem é o dono da chave/sessão — as páginas usam o role para decidir o que
    mostrar. É o papel EFETIVO: a chave de cliente de um admin responde "user"."""
    return {"username": user.username, "role": effective_role(user), "scope": user.key_scope}


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


def _file_row(db: Session, file_id: int, user: User):
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
    return row


@router.get("/content/{file_id}")
def get_content(file_id: int, request: Request, download: bool = False,
                db: Session = Depends(get_db),
                user: User = Depends(get_user_header_or_cookie)):
    """Conteúdo do arquivo inline (preview) ou como download, com suporte a Range."""
    row = _file_row(db, file_id, user)
    return content.stream(request, db, sha256=row.sha256, name=Path(row.original_path).name,
                          size=row.size, encrypted=bool(row.encrypted), download=download,
                          who=f"{user.username} file_id={file_id}")


# -- Fotos --------------------------------------------------------------------

# Detail fixo: o front reconhece este 403 e volta a esconder as ocultas.
PIN_REQUIRED = "PIN necessario"


def _check_hidden(request: Request, user: User, show_hidden: bool) -> None:
    """show_hidden de quem tem PIN exige o cookie de desbloqueio (POST /photos/unlock)."""
    if show_hidden and not hidden_unlocked(request, user):
        raise HTTPException(403, PIN_REQUIRED)


@router.get("/photos")
def get_photos(request: Request,
               before_ts: Optional[float] = None, before_id: Optional[int] = None,
               limit: int = Query(200, ge=1, le=1000),
               show_hidden: bool = False,
               db: Session = Depends(get_db),
               user: User = Depends(get_user_header_or_cookie)):
    """Timeline de fotos e vídeos de todos os backups do usuário (última versão de
    cada um), mais recentes primeiro. Paginação keyset por (before_ts, before_id).
    Fotos em pastas ocultas (álbum Hidden do iCloud etc.) só com show_hidden — e,
    se o usuário tem PIN, só depois de desbloquear."""
    _check_hidden(request, user, show_hidden)
    items = media.visible(media.timeline(db, user), show_hidden)
    chunk = media.page(items, before_ts, before_id, limit)
    nxt = None
    if chunk and len(chunk) == limit:
        nxt = {"before_ts": chunk[-1]["ts"], "before_id": chunk[-1]["id"]}
    return {"items": chunk, "next": nxt, "total": len(items)}


@router.get("/photos/months")
def get_photo_months(request: Request, show_hidden: bool = False,
                     db: Session = Depends(get_db),
                     user: User = Depends(get_user_header_or_cookie)):
    _check_hidden(request, user, show_hidden)
    return media.months(media.visible(media.timeline(db, user), show_hidden))


@router.get("/photos/indexing")
def get_indexing(request: Request, show_hidden: bool = False,
                 db: Session = Depends(get_db),
                 user: User = Depends(get_user_header_or_cookie)):
    _check_hidden(request, user, show_hidden)
    items = media.timeline(db, user)
    status = media.indexing_status(media.visible(items, show_hidden))
    status["hidden"] = sum(1 for i in items if i["hidden"])
    status["pin_required"] = bool(user.hidden_pin_hash)
    status["unlocked"] = hidden_unlocked(request, user)
    return status


class PinUnlock(BaseModel):
    pin: str


@router.post("/photos/unlock")
def unlock_hidden(req: PinUnlock, request: Request, response: Response,
                  user: User = Depends(get_current_user)):
    """Confere o PIN das ocultas e emite o cookie de desbloqueio (HIDDEN_TTL).
    Escrita: exige o header X-API-Key, como as outras. Erros seguidos bloqueiam
    por alguns minutos (429)."""
    if not user.hidden_pin_hash:
        return {"unlocked_until": None}
    wait = pin_locked_for(user)
    if wait:
        raise HTTPException(429, f"Muitas tentativas — tente de novo em {wait}s")
    ok = check_pin(req.pin, user.hidden_pin_hash)
    register_pin_attempt(user, ok)
    if not ok:
        log.info(f"[photos] {user.username}: PIN das ocultas incorreto")
        raise HTTPException(403, "PIN incorreto")
    token, exp = make_hidden_token(user)
    response.set_cookie(HIDDEN_COOKIE, token, max_age=HIDDEN_TTL, httponly=True,
                        samesite="strict", secure=_is_https(request), path="/")
    log.info(f"[photos] {user.username}: ocultas desbloqueadas por {HIDDEN_TTL // 60} min")
    return {"unlocked_until": exp}


@router.delete("/photos/unlock")
def lock_hidden(response: Response):
    response.delete_cookie(HIDDEN_COOKIE, path="/")
    return {"ok": True}


@router.get("/photos/labels")
def get_photo_labels(db: Session = Depends(get_db),
                     user: User = Depends(get_user_header_or_cookie)):
    """Backups do usuário e se cada um entra na galeria."""
    return media.photo_labels(db, user)


class PhotoLabelUpdate(BaseModel):
    enabled: bool


@router.put("/photos/labels")
def set_all_photo_labels(req: PhotoLabelUpdate, db: Session = Depends(get_db),
                         user: User = Depends(get_current_user)):
    """Marca/desmarca de uma vez todos os backups do próprio usuário na galeria."""
    labels = (db.query(BackupID)
              .filter(BackupID.owner_user_id == user.id, tree.live_label_filter()).all())
    for b in labels:
        b.photos_enabled = req.enabled
    db.commit()
    log.info(f"[photos] {user.username}: todos os {len(labels)} backup(s) "
             f"{'incluidos na' if req.enabled else 'removidos da'} galeria")
    if req.enabled:
        media.indexer.wake()
    return {"enabled": req.enabled, "count": len(labels)}


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


@router.get("/photo/{file_id}/full")
def get_photo_full(file_id: int, request: Request, db: Session = Depends(get_db),
                   user: User = Depends(get_user_header_or_cookie)):
    """A foto em resolução total para o visualizador de /photos: o próprio original
    quando o navegador abre o formato, senão (HEIC, TIFF) convertido para JPEG."""
    row = _file_row(db, file_id, user)
    name = Path(row.original_path).name
    if media.kind_of(name) != "image":
        raise HTTPException(404, "Nao e uma foto")
    if name.rsplit(".", 1)[-1].lower() in media.BROWSER_IMG_EXT:
        return content.stream(request, db, sha256=row.sha256, name=name, size=row.size,
                              encrypted=bool(row.encrypted), download=False,
                              who=f"{user.username} file_id={file_id}")
    headers = {"Cache-Control": "private, max-age=86400", "ETag": f'"{row.sha256}-full"'}
    if request.headers.get("if-none-match") == headers["ETag"]:
        return Response(status_code=304, headers=headers)
    try:
        data = media.full_jpeg(db, row.sha256, name, bool(row.encrypted))
    except FileNotFoundError as exc:
        log.error(f"[photos] {user.username} file_id={file_id} {name!r}: {exc}")
        raise HTTPException(410, "Conteudo fisico nao encontrado")
    if data is None:
        raise HTTPException(415, "Formato sem suporte no servidor")
    log.info(f"[photos] {user.username} abriu {name!r} em resolucao total "
             f"(convertido, {len(data) // 1024} KB)")
    return Response(data, media_type="image/jpeg", headers=headers)


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
    headers = {"Cache-Control": "private, max-age=604800",
               "ETag": f'"{sha256}-{size}-{mi.thumb_rev or 0}"'}
    if request.headers.get("if-none-match") == headers["ETag"]:
        return Response(status_code=304, headers=headers)
    if mi.thumb_encrypted:
        return StreamingResponse(crypto.decrypt_chunks(p, storage.encryption_key),
                                 media_type="image/jpeg", headers=headers)
    return FileResponse(p, media_type="image/jpeg", headers=headers)
