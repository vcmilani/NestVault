"""Front "cloud" — API do drive (estilo OneDrive), montada em /cloud.

Leitura autentica pelo header X-API-Key ou pelo cookie de sessão emitido em
POST /cloud/session (ver auth.get_user_header_or_cookie). Escrita — só nas pastas
do servidor (ver drive.py), backups são somente leitura — exige o header.
"""
import logging
from pathlib import Path
from typing import Literal, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from auth import (HIDDEN_COOKIE, HIDDEN_TTL, SESSION_COOKIE, SESSION_TTL, effective_role,
                  get_current_user, get_user_header_or_cookie, hidden_unlocked,
                  make_hidden_token, make_session_token, pin_locked_for,
                  register_pin_attempt, require_owner_or_admin)
import cloud_replicas
import crypto
import quota
import storage
from database import (BackupID, BackupVersion, FileContent, MediaInfo, User, VersionFile, get_db,
                      SERVER_KIND, TRASHED_STATUS, check_pin)
from urllib.parse import quote

from . import archive, content, drive, duplicates, media, tree

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
def get_me(db: Session = Depends(get_db), user: User = Depends(get_user_header_or_cookie)):
    """Quem é o dono da chave/sessão — as páginas usam o role para decidir o que
    mostrar. É o papel EFETIVO: a chave de cliente de um admin responde "user".
    Traz também o espaço (barra "usados · disponíveis" do Cloud): o disponível é o
    que sobra da cota, limitado ao que ainda cabe nos discos com as réplicas do Cloud."""
    me = {"username": user.username, "role": effective_role(user), "scope": user.key_scope}
    used = quota.usage(db, user.id)
    free = cloud_replicas.storage_free_bytes()
    available = free if user.quota_bytes is None else min(max(0, user.quota_bytes - used), free)
    me.update(quota_bytes=user.quota_bytes, usage_bytes=used,
              storage_free_bytes=free, available_bytes=available)
    return me


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

    b = tree.get_label_or_404(db, label, user)
    versions = tree.done_versions(db, label)
    v = tree.pick_version(versions, version)
    base = tree.label_base(db, label, versions)
    rel = tree.norm_rel(path)
    editable = b.kind == SERVER_KIND
    extra = tree.empty_dirs(db, label, rel) if editable else []
    children = tree.list_children(db, v, base, rel, offset, limit, sort, extra)
    return {
        "label": label, "path": rel,
        "kind": b.kind or "backup", "editable": editable,
        "name": b.client_name if editable and b.client_name else label,
        "version_key": v.version_key, "version_created_at": str(v.created_at),
        "is_latest": v.id == versions[0].id,
        "offset": offset, "limit": limit,
        **children,
    }


@router.get("/duplicates")
def get_duplicates(min_size: int = Query(duplicates.DEFAULT_MIN_SIZE, ge=1),
                   limit: int = Query(100, ge=1, le=duplicates.MAX_GROUPS),
                   db: Session = Depends(get_db),
                   user: User = Depends(get_user_header_or_cookie)):
    """Grupos de arquivos idênticos (mesmo sha256) entre os labels do usuário, na
    versão mais recente de cada um. Apagar usa POST /drive/delete (só pastas do servidor)."""
    return duplicates.find(db, user, min_size, limit)


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


@router.post("/zip")
def download_zip(label: str = Form(...), items: list[str] = Form(...),
                 version: Optional[str] = Form(None),
                 dry_run: bool = Form(False),
                 db: Session = Depends(get_db),
                 user: User = Depends(get_user_header_or_cookie)):
    """Vários arquivos/pastas de um label (backup ou pasta do servidor) num .zip,
    gerado em streaming. Formulário (e não JSON) para o navegador poder baixar
    direto, com o cookie de sessão — é leitura, como /content. dry_run só valida
    e conta: a página confere antes, para um erro não virar uma página de JSON."""
    entries, name = archive.collect(db, label, version or None, items, user)
    if dry_run:
        return {"name": name, "files": len(entries), "size": sum(e.size for e in entries),
                "unreadable": sum(1 for e in entries if e.path is None)}
    who = f"{user.username} {label}"
    log.info(f"[cloud] {who}: zip de {len(items)} item(ns), {len(entries)} arquivo(s) "
             f"({sum(e.size for e in entries) // (1024 * 1024)} MB) → {name!r}")
    return StreamingResponse(
        archive.stream(entries, who), media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(name)}",
                 "Cache-Control": "no-store"})


# -- Pastas do servidor (edição) ----------------------------------------------
# Todas exigem o header X-API-Key (get_current_user): o cookie de sessão só lê.

def _changed(d: drive.Drive | None = None, background_tasks: BackgroundTasks | None = None) -> None:
    # A timeline de fotos é cacheada pelo id das versões, que não muda numa
    # edição da versão live: força o recálculo.
    media.bump_generation()
    # Conteúdo que entrou ou saiu do Cloud: réplicas dedicadas (cloud_replicas)
    # ajustadas depois da resposta — criar cópias de uma pasta grande demora.
    if d is not None and d.touched and background_tasks is not None:
        background_tasks.add_task(cloud_replicas.apply_bg, set(d.touched))


class DriveCreate(BaseModel):
    name: str


@router.post("/drives")
def create_drive(req: DriveCreate, db: Session = Depends(get_db),
                 user: User = Depends(get_current_user)):
    b = drive.create(db, user, req.name)
    return {"label": b.label, "name": b.client_name, "kind": SERVER_KIND}


@router.patch("/drives/{label}")
def rename_drive(label: str, req: DriveCreate, db: Session = Depends(get_db),
                 user: User = Depends(get_current_user)):
    d = drive.get(db, label, user)
    drive.rename_drive(db, d, req.name, user)
    return {"label": label, "name": d.b.client_name}


class MkdirReq(BaseModel):
    label: str
    path: str


@router.post("/drive/mkdir")
def drive_mkdir(req: MkdirReq, db: Session = Depends(get_db),
                user: User = Depends(get_current_user)):
    d = drive.get(db, req.label, user)
    return {"path": drive.mkdir(db, d, req.path, user)}


class MoveReq(BaseModel):
    label: str
    items: list[str]
    dest_dir: str = ""


@router.post("/drive/move")
def drive_move(req: MoveReq, db: Session = Depends(get_db),
               user: User = Depends(get_current_user)):
    d = drive.get(db, req.label, user)
    n = drive.move(db, d, req.items, req.dest_dir, user)
    _changed()
    return {"moved": n}


class RenameReq(BaseModel):
    label: str
    path: str
    new_name: str


@router.post("/drive/rename")
def drive_rename(req: RenameReq, db: Session = Depends(get_db),
                 user: User = Depends(get_current_user)):
    d = drive.get(db, req.label, user)
    path = drive.rename(db, d, req.path, req.new_name, user)
    _changed()
    return {"path": path}


class ItemsReq(BaseModel):
    label: str
    items: list[str]


@router.post("/drive/delete")
def drive_delete(req: ItemsReq, background_tasks: BackgroundTasks, db: Session = Depends(get_db),
                 user: User = Depends(get_current_user)):
    d = drive.get(db, req.label, user)
    n = drive.delete(db, d, req.items, user)
    _changed(d, background_tasks)
    return {"trashed": n}


@router.get("/drive/trash")
def drive_trash(label: str, db: Session = Depends(get_db),
                user: User = Depends(get_user_header_or_cookie)):
    return drive.trash_list(db, drive.get(db, label, user))


class TrashIdsReq(BaseModel):
    label: str
    ids: list[int]


@router.post("/drive/restore")
def drive_restore(req: TrashIdsReq, background_tasks: BackgroundTasks, db: Session = Depends(get_db),
                  user: User = Depends(get_current_user)):
    d = drive.get(db, req.label, user)
    n = drive.restore(db, d, req.ids, user)
    _changed(d, background_tasks)
    return {"restored": n}


@router.post("/drive/purge")
def drive_purge(req: TrashIdsReq, background_tasks: BackgroundTasks, db: Session = Depends(get_db),
                user: User = Depends(get_current_user)):
    d = drive.get(db, req.label, user)
    n = drive.purge(db, d, req.ids, user)
    _changed(d, background_tasks)
    return {"purged": n}


class CopyReq(BaseModel):
    src_label: str
    src_version: Optional[str] = None
    items: list[str]
    dest_label: str
    dest_dir: str = ""


@router.post("/drive/copy")
def drive_copy(req: CopyReq, background_tasks: BackgroundTasks, db: Session = Depends(get_db),
               user: User = Depends(get_current_user)):
    d = drive.get(db, req.dest_label, user)
    n = drive.copy(db, req.src_label, req.src_version, req.items, d, req.dest_dir, user)
    _changed(d, background_tasks)
    if n:
        media.indexer.wake()
    return {"copied": n}


# PUT /cloud/drive/upload fica em main.py: reutiliza o pipeline de gravação do
# /upload (staging no SSD, dedup, réplicas), que mora lá.


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
