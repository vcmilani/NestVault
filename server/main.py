"""
Backup Files — Raspberry Pi  v2.0
Suporte a versoes dentro do mesmo label.
Deduplicacao por conteudo (sha256) — cada arquivo fisico e armazenado uma unica vez.
Arquivos deletados sao marcados, nao removidos do storage.
"""

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Header
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional
import os, hashlib, secrets, base64
from pathlib import Path
from datetime import datetime

from database import init_db, get_db, BackupID, BackupVersion, FileContent, VersionFile
from sqlalchemy.orm import Session

# -- Config -------------------------------------------------------------------
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "./storage"))
API_KEY     = os.getenv("BACKUP_API_KEY", "")
STATIC_DIR  = Path(__file__).parent / "static"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)
AUTH_ENABLED = bool(API_KEY)

app = FastAPI(title="Backup Files — Raspberry Pi", version="2.0.0")

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# -- Startup ------------------------------------------------------------------
@app.on_event("startup")
def startup():
    init_db()


# -- Auth ---------------------------------------------------------------------
def require_api_key(x_api_key: Optional[str] = Header(None)):
    if not AUTH_ENABLED:
        return
    if not x_api_key or not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="API key invalida")


# -- Schemas ------------------------------------------------------------------
class BackupCreate(BaseModel):
    label: str
    client_name: Optional[str] = None
    prefix: Optional[str] = None

class VersionCreate(BaseModel):
    version_key: str   # datetime ISO: "2026-04-25T10:42:31"

class VersionFinish(BaseModel):
    status: str = "done"  # done | failed

class CheckRequest(BaseModel):
    backup_label: str
    version_key: str
    original_path: str
    sha256: str
    size: int
    mtime: float

class CheckResponse(BaseModel):
    needs_upload: bool
    content_exists: bool   # True = conteudo ja no storage, so precisa registrar
    reason: str
    file_id: Optional[int] = None

class SyncRequest(BaseModel):
    backup_label: str
    version_key: str
    existing_paths: list[str]

class SyncResponse(BaseModel):
    marked_deleted: list[str]
    deleted_count: int

class CleanupRequest(BaseModel):
    backup_label: str
    keep: int = 5   # quantas versoes manter


# -- Helpers ------------------------------------------------------------------
def _content_path(sha256: str) -> Path:
    """Storage fisico unico por sha256 — compartilhado entre todas as versoes e labels."""
    dest = STORAGE_DIR / "_content" / sha256[:2] / sha256
    dest.parent.mkdir(parents=True, exist_ok=True)
    return dest

def _read_upload(upload: UploadFile) -> tuple[str, bytes]:
    h = hashlib.sha256()
    chunks = []
    while True:
        chunk = upload.file.read(65536)
        if not chunk:
            break
        h.update(chunk)
        chunks.append(chunk)
    return h.hexdigest(), b"".join(chunks)

def _get_backup_or_404(label: str, db: Session) -> BackupID:
    b = db.query(BackupID).filter(BackupID.label == label).first()
    if not b:
        raise HTTPException(404, f"Backup '{label}' nao encontrado")
    return b

def _get_version_or_404(label: str, version_key: str, db: Session) -> BackupVersion:
    v = (db.query(BackupVersion)
         .filter(BackupVersion.backup_label == label,
                 BackupVersion.version_key  == version_key)
         .first())
    if not v:
        raise HTTPException(404, f"Versao '{version_key}' nao encontrada em '{label}'")
    return v

def _version_stats(v: BackupVersion, db: Session) -> dict:
    files = db.query(VersionFile).filter(VersionFile.version_id == v.id).all()
    active  = [f for f in files if f.status == "active"]
    deleted = [f for f in files if f.status == "deleted"]
    size    = sum(f.content.size for f in active if f.content)
    return {
        "id": v.id,
        "version_key": v.version_key,
        "backup_label": v.backup_label,
        "status": v.status,
        "created_at": str(v.created_at),
        "finished_at": str(v.finished_at) if v.finished_at else None,
        "file_count": len(active),
        "deleted_count": len(deleted),
        "total_size_bytes": size,
    }

def _backup_info(b: BackupID, db: Session) -> dict:
    versions = (db.query(BackupVersion)
                .filter(BackupVersion.backup_label == b.label,
                        BackupVersion.status == "done")
                .order_by(BackupVersion.version_key.desc())
                .all())
    latest = versions[0] if versions else None
    total_files = sum(_version_stats(v, db)["file_count"] for v in versions[:1])
    total_size  = sum(_version_stats(v, db)["total_size_bytes"] for v in versions[:1])
    return {
        "id": b.id,
        "label": b.label,
        "client_name": b.client_name,
        "prefix": b.prefix,
        "status": b.status,
        "created_at": str(b.created_at),
        "last_version": latest.version_key if latest else None,
        "version_count": len(versions),
        "file_count": total_files,
        "total_size_bytes": total_size,
    }


# -- Dashboard ----------------------------------------------------------------
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    index = STATIC_DIR / "index.html"
    if not index.exists():
        return HTMLResponse("<h1>Dashboard nao encontrado</h1>", status_code=404)
    return HTMLResponse(index.read_text(encoding="utf-8"))


# -- Health -------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "version": "2.0.0", "time": datetime.utcnow().isoformat()}


# -- Backups ------------------------------------------------------------------
@app.post("/backups", dependencies=[Depends(require_api_key)])
def create_backup(req: BackupCreate, db: Session = Depends(get_db)):
    existing = db.query(BackupID).filter(BackupID.label == req.label).first()
    if existing:
        return {"created": False, "backup": _backup_info(existing, db)}
    b = BackupID(label=req.label, client_name=req.client_name, prefix=req.prefix)
    db.add(b); db.commit(); db.refresh(b)
    return {"created": True, "backup": _backup_info(b, db)}

@app.get("/backups", dependencies=[Depends(require_api_key)])
def list_backups(client_name: Optional[str] = None, db: Session = Depends(get_db)):
    q = db.query(BackupID).order_by(BackupID.created_at.desc())
    if client_name:
        q = q.filter(BackupID.client_name == client_name)
    return [_backup_info(b, db) for b in q.all()]

@app.get("/backups/{label}", dependencies=[Depends(require_api_key)])
def get_backup(label: str, db: Session = Depends(get_db)):
    return _backup_info(_get_backup_or_404(label, db), db)

@app.delete("/backups/{label}", dependencies=[Depends(require_api_key)])
def delete_backup(label: str, db: Session = Depends(get_db)):
    b = _get_backup_or_404(label, db)
    versions = db.query(BackupVersion).filter(BackupVersion.backup_label == label).all()
    for v in versions:
        db.query(VersionFile).filter(VersionFile.version_id == v.id).delete()
        db.delete(v)
    db.delete(b)
    db.commit()
    _cleanup_orphan_contents(db)
    return {"status": "deleted", "label": label}


# -- Versions -----------------------------------------------------------------
@app.post("/backups/{label}/versions", dependencies=[Depends(require_api_key)])
def create_version(label: str, req: VersionCreate, db: Session = Depends(get_db)):
    """Cria uma nova versao. version_key deve ser unico dentro do label."""
    _get_backup_or_404(label, db)
    existing = (db.query(BackupVersion)
                .filter(BackupVersion.backup_label == label,
                        BackupVersion.version_key  == req.version_key)
                .first())
    if existing:
        return {"created": False, "version": _version_stats(existing, db)}
    v = BackupVersion(backup_label=label, version_key=req.version_key)
    db.add(v); db.commit(); db.refresh(v)
    return {"created": True, "version": _version_stats(v, db)}

@app.get("/backups/{label}/versions", dependencies=[Depends(require_api_key)])
def list_versions(label: str, db: Session = Depends(get_db)):
    _get_backup_or_404(label, db)
    versions = (db.query(BackupVersion)
                .filter(BackupVersion.backup_label == label)
                .order_by(BackupVersion.version_key.desc())
                .all())
    return [_version_stats(v, db) for v in versions]

@app.get("/backups/{label}/versions/{version_key}", dependencies=[Depends(require_api_key)])
def get_version(label: str, version_key: str, db: Session = Depends(get_db)):
    return _version_stats(_get_version_or_404(label, version_key, db), db)

@app.patch("/backups/{label}/versions/{version_key}", dependencies=[Depends(require_api_key)])
def finish_version(label: str, version_key: str, req: VersionFinish, db: Session = Depends(get_db)):
    v = _get_version_or_404(label, version_key, db)
    v.status = req.status
    v.finished_at = datetime.utcnow()
    db.commit()
    return _version_stats(v, db)

@app.delete("/backups/{label}/versions/{version_key}", dependencies=[Depends(require_api_key)])
def delete_version(label: str, version_key: str, db: Session = Depends(get_db)):
    """Remove uma versao especifica. Limpa FileContents orfaos automaticamente."""
    v = _get_version_or_404(label, version_key, db)
    db.query(VersionFile).filter(VersionFile.version_id == v.id).delete()
    db.delete(v); db.commit()
    removed = _cleanup_orphan_contents(db)
    return {"status": "deleted", "version_key": version_key, "files_removed_from_storage": removed}


# -- Check --------------------------------------------------------------------
@app.post("/check", response_model=CheckResponse, dependencies=[Depends(require_api_key)])
def check_file(req: CheckRequest, db: Session = Depends(get_db)):
    """
    Verifica se o arquivo precisa ser enviado na versao atual.
    Retorna content_exists=True se o conteudo ja existe no storage
    (cliente nao precisa fazer upload, so registrar).
    """
    v = _get_version_or_404(req.backup_label, req.version_key, db)

    # Ja registrado nesta versao com mesmo conteudo?
    vf = (db.query(VersionFile)
          .filter(VersionFile.version_id    == v.id,
                  VersionFile.original_path == req.original_path,
                  VersionFile.sha256        == req.sha256)
          .first())
    if vf:
        return CheckResponse(needs_upload=False, content_exists=True,
                             reason="Ja registrado nesta versao", file_id=vf.id)

    # Conteudo ja existe no storage (outra versao ou label)?
    content_exists = db.query(FileContent).filter(FileContent.sha256 == req.sha256).first() is not None

    return CheckResponse(
        needs_upload=True,
        content_exists=content_exists,
        reason="Conteudo ja no storage — apenas registrar" if content_exists else "Upload necessario",
    )


# -- Upload -------------------------------------------------------------------
@app.post("/upload", dependencies=[Depends(require_api_key)])
async def upload_file(
    file: UploadFile = File(None),
    backup_label:  str           = Header(..., alias="X-Backup-Label"),
    version_key:   str           = Header(..., alias="X-Version-Key"),
    original_path: str           = Header(..., alias="X-Original-Path"),
    mtime:         float         = Header(..., alias="X-Mtime"),
    content_sha256: Optional[str] = Header(None, alias="X-Content-Sha256"),
    db: Session = Depends(get_db),
):
    """
    Registra um arquivo na versao atual.
    Se X-Content-Sha256 for enviado e o conteudo ja existir no storage,
    o cliente pode omitir o body (evita re-upload do mesmo conteudo).
    Headers:
      X-Backup-Label   : label do backup
      X-Version-Key    : chave da versao (datetime ISO)
      X-Original-Path  : path original no cliente (base64)
      X-Mtime          : modification time (epoch float)
      X-Content-Sha256 : sha256 do arquivo (quando content_exists=True no /check)
    """
    v = _get_version_or_404(backup_label, version_key, db)

    try:
        original_path = base64.b64decode(original_path.encode("ascii")).decode("utf-8")
    except Exception:
        pass

    # Modo "so registrar" — conteudo ja existe no storage
    if content_sha256 and not file:
        fc = db.query(FileContent).filter(FileContent.sha256 == content_sha256).first()
        if not fc:
            raise HTTPException(400, f"Conteudo sha256={content_sha256} nao encontrado no storage")
        sha256 = content_sha256
        size   = fc.size
    else:
        if not file:
            raise HTTPException(400, "Body do arquivo obrigatorio quando conteudo nao existe no storage")
        sha256, content = _read_upload(file)
        size = len(content)
        fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
        if not fc:
            dest = _content_path(sha256)
            dest.write_bytes(content)
            fc = FileContent(sha256=sha256, stored_at=str(dest), size=size)
            db.add(fc)

    # Upsert VersionFile
    vf = (db.query(VersionFile)
          .filter(VersionFile.version_id    == v.id,
                  VersionFile.original_path == original_path)
          .first())
    if vf:
        vf.sha256  = sha256
        vf.mtime   = mtime
        vf.status  = "active"
    else:
        vf = VersionFile(version_id=v.id, original_path=original_path,
                         sha256=sha256, mtime=mtime, status="active")
        db.add(vf)

    db.commit(); db.refresh(vf)
    return {"status": "registered", "file_id": vf.id, "sha256": sha256,
            "uploaded": not bool(content_sha256)}


# -- Sync (fecha versao) ------------------------------------------------------
@app.post("/sync", response_model=SyncResponse, dependencies=[Depends(require_api_key)])
def sync(req: SyncRequest, db: Session = Depends(get_db)):
    """
    Fecha a versao: arquivos que nao estao em existing_paths sao marcados como deleted.
    O conteudo fisico NAO e removido.
    """
    v = _get_version_or_404(req.backup_label, req.version_key, db)
    client_set = set(req.existing_paths)
    active_files = (db.query(VersionFile)
                    .filter(VersionFile.version_id == v.id,
                            VersionFile.status     == "active")
                    .all())
    marked = []
    for vf in active_files:
        if vf.original_path not in client_set:
            vf.status = "deleted"
            marked.append(vf.original_path)
    db.commit()
    return SyncResponse(marked_deleted=marked, deleted_count=len(marked))


# -- Files --------------------------------------------------------------------
@app.get("/files", dependencies=[Depends(require_api_key)])
def list_files(
    backup_label: str,
    version_key: str,
    include_deleted: bool = False,
    db: Session = Depends(get_db),
):
    """Lista arquivos de uma versao especifica."""
    v = _get_version_or_404(backup_label, version_key, db)
    q = db.query(VersionFile).filter(VersionFile.version_id == v.id)
    if not include_deleted:
        q = q.filter(VersionFile.status == "active")
    files = q.order_by(VersionFile.original_path).all()
    return [
        {
            "id": f.id,
            "original_path": f.original_path,
            "sha256": f.sha256,
            "size": f.content.size if f.content else 0,
            "mtime": f.mtime,
            "status": f.status,
            "created_at": str(f.created_at),
        }
        for f in files
    ]

@app.get("/files/{file_id}/download", dependencies=[Depends(require_api_key)])
def download_file(file_id: int, db: Session = Depends(get_db)):
    vf = db.query(VersionFile).filter(VersionFile.id == file_id).first()
    if not vf:
        raise HTTPException(404, "Arquivo nao encontrado")
    if vf.status == "deleted":
        raise HTTPException(410, "Arquivo marcado como deletado nesta versao")
    fc = db.query(FileContent).filter(FileContent.sha256 == vf.sha256).first()
    if not fc or not Path(fc.stored_at).exists():
        raise HTTPException(410, "Conteudo fisico nao encontrado")
    return FileResponse(Path(fc.stored_at), filename=Path(vf.original_path).name)


# -- Cleanup ------------------------------------------------------------------
@app.post("/backups/{label}/cleanup", dependencies=[Depends(require_api_key)])
def cleanup_versions(label: str, req: CleanupRequest, db: Session = Depends(get_db)):
    """
    Mantem apenas as `keep` versoes mais recentes (status=done).
    Versoes mais antigas sao removidas. FileContents orfaos sao apagados do storage.
    """
    _get_backup_or_404(label, db)
    done_versions = (db.query(BackupVersion)
                     .filter(BackupVersion.backup_label == label,
                             BackupVersion.status       == "done")
                     .order_by(BackupVersion.version_key.desc())
                     .all())
    to_delete = done_versions[req.keep:]
    removed_versions = []
    for v in to_delete:
        db.query(VersionFile).filter(VersionFile.version_id == v.id).delete()
        db.delete(v)
        removed_versions.append(v.version_key)
    db.commit()
    orphans_removed = _cleanup_orphan_contents(db)
    return {
        "kept": req.keep,
        "versions_removed": removed_versions,
        "storage_files_removed": orphans_removed,
    }


def _cleanup_orphan_contents(db: Session) -> int:
    """Remove FileContents que nao sao referenciados por nenhum VersionFile."""
    all_contents = db.query(FileContent).all()
    removed = 0
    for fc in all_contents:
        refs = db.query(VersionFile).filter(VersionFile.sha256 == fc.sha256).count()
        if refs == 0:
            p = Path(fc.stored_at)
            if p.exists():
                p.unlink()
            db.delete(fc)
            removed += 1
    db.commit()
    return removed