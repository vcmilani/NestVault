"""
Backup Server - FastAPI
Cada backup e identificado por um label unico. Arquivos de backups diferentes
sao completamente isolados — check, upload, sync e storage fisico sao todos
escopados ao label. Um backup nunca interfere nos arquivos de outro.
"""

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Header
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import os
import hashlib
import secrets
import base64
from pathlib import Path
from datetime import datetime

from database import init_db, get_db, FileRecord, BackupID, SessionLocal
from sqlalchemy.orm import Session

# -- Config -------------------------------------------------------------------
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "./storage"))
API_KEY     = os.getenv("BACKUP_API_KEY", "change-me-in-production")
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Backup Server", version="3.0.0")


# -- Startup ------------------------------------------------------------------
@app.on_event("startup")
def startup():
    init_db()


# -- Auth ---------------------------------------------------------------------
def require_api_key(x_api_key: str = Header(...)):
    if not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="API key invalida")


# -- Schemas ------------------------------------------------------------------
class BackupCreate(BaseModel):
    label: str                        # identificador unico — ex: "producao", "notebook-joao"
    client_name: Optional[str] = None
    prefix: Optional[str] = None

class BackupInfo(BaseModel):
    id: int
    label: str
    client_name: Optional[str]
    prefix: Optional[str]
    created_at: str
    last_run_at: Optional[str]
    status: str
    file_count: int
    total_size_bytes: int

class CheckRequest(BaseModel):
    backup_label: str
    original_path: str
    sha256: str
    size: int
    mtime: float

class CheckResponse(BaseModel):
    needs_upload: bool
    reason: str
    file_id: Optional[int] = None

class FileInfo(BaseModel):
    id: int
    backup_label: str
    original_path: str
    sha256: str
    size: int
    mtime: float
    stored_at: str
    created_at: str

class SyncRequest(BaseModel):
    backup_label: str
    existing_paths: list[str]

class SyncResponse(BaseModel):
    deleted: list[str]
    deleted_count: int


# -- Helpers ------------------------------------------------------------------
def _storage_path(backup_label: str, sha256: str, original_path: str) -> Path:
    """
    Cada backup tem sua propria subpasta em storage/.
    storage/<backup_label>/<2 chars sha256>/<hash>_<filename>
    Isso garante isolamento fisico total entre backups diferentes.
    """
    dest = STORAGE_DIR / backup_label / sha256[:2] / f"{sha256[:16]}_{Path(original_path).name}"
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
    backup = db.query(BackupID).filter(BackupID.label == label).first()
    if not backup:
        raise HTTPException(
            status_code=404,
            detail=f"Backup '{label}' nao encontrado. Crie-o primeiro via POST /backups"
        )
    return backup

def _backup_info(b: BackupID, db: Session) -> BackupInfo:
    files = db.query(FileRecord).filter(FileRecord.backup_label == b.label).all()
    return BackupInfo(
        id=b.id,
        label=b.label,
        client_name=b.client_name,
        prefix=b.prefix,
        created_at=str(b.created_at),
        last_run_at=str(b.last_run_at) if b.last_run_at else None,
        status=b.status,
        file_count=len(files),
        total_size_bytes=sum(f.size for f in files),
    )


# -- Health -------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


# -- Backup IDs ---------------------------------------------------------------
@app.post("/backups", dependencies=[Depends(require_api_key)])
def create_backup(req: BackupCreate, db: Session = Depends(get_db)):
    """
    Cria um novo identificador de backup. O label deve ser unico.
    Se ja existir um backup com esse label, retorna o existente (idempotente).
    """
    existing = db.query(BackupID).filter(BackupID.label == req.label).first()
    if existing:
        return {"created": False, "backup": _backup_info(existing, db)}

    backup = BackupID(
        label=req.label,
        client_name=req.client_name,
        prefix=req.prefix,
    )
    db.add(backup)
    db.commit()
    db.refresh(backup)
    return {"created": True, "backup": _backup_info(backup, db)}


@app.get("/backups", dependencies=[Depends(require_api_key)])
def list_backups(client_name: Optional[str] = None, db: Session = Depends(get_db)):
    """Lista todos os identificadores de backup com contagem de arquivos e tamanho total."""
    query = db.query(BackupID).order_by(BackupID.last_run_at.desc().nullslast())
    if client_name:
        query = query.filter(BackupID.client_name == client_name)
    return [_backup_info(b, db) for b in query.all()]


@app.get("/backups/{label}", dependencies=[Depends(require_api_key)])
def get_backup(label: str, db: Session = Depends(get_db)):
    """Detalhes de um backup especifico."""
    return _backup_info(_get_backup_or_404(label, db), db)


@app.delete("/backups/{label}", dependencies=[Depends(require_api_key)])
def delete_backup(label: str, db: Session = Depends(get_db)):
    """Remove um backup e TODOS os seus arquivos. Nao afeta outros backups."""
    backup = _get_backup_or_404(label, db)
    files = db.query(FileRecord).filter(FileRecord.backup_label == label).all()
    for f in files:
        path = Path(f.stored_at)
        if path.exists():
            path.unlink()
        db.delete(f)
    # Remove a pasta fisica do backup se estiver vazia
    backup_dir = STORAGE_DIR / label
    if backup_dir.exists():
        try:
            backup_dir.rmdir()
        except OSError:
            pass  # pasta nao esta vazia, ignora
    db.delete(backup)
    db.commit()
    return {"status": "deleted", "label": label, "files_removed": len(files)}


# -- Check --------------------------------------------------------------------
@app.post("/check", response_model=CheckResponse, dependencies=[Depends(require_api_key)])
def check_file(req: CheckRequest, db: Session = Depends(get_db)):
    """
    Verifica se o arquivo precisa ser enviado para um backup especifico.
    A verificacao e ISOLADA ao backup_label — nunca consulta outros backups.
    """
    _get_backup_or_404(req.backup_label, db)

    record = (
        db.query(FileRecord)
        .filter(
            FileRecord.backup_label  == req.backup_label,
            FileRecord.original_path == req.original_path,
            FileRecord.sha256        == req.sha256,
            FileRecord.size          == req.size,
            FileRecord.mtime         == req.mtime,
        )
        .first()
    )
    if record:
        return CheckResponse(
            needs_upload=False,
            reason="Metadados identicos — arquivo ja esta neste backup",
            file_id=record.id,
        )

    existing = (
        db.query(FileRecord)
        .filter(
            FileRecord.backup_label  == req.backup_label,
            FileRecord.original_path == req.original_path,
        )
        .first()
    )
    if existing:
        return CheckResponse(needs_upload=True, reason="Arquivo modificado — sera atualizado neste backup")

    return CheckResponse(needs_upload=True, reason="Arquivo novo neste backup")


# -- Upload -------------------------------------------------------------------
@app.post("/upload", dependencies=[Depends(require_api_key)])
async def upload_file(
    file: UploadFile = File(...),
    backup_label: str  = Header(..., alias="X-Backup-Label"),
    original_path: str = Header(..., alias="X-Original-Path"),
    mtime: float       = Header(..., alias="X-Mtime"),
    db: Session        = Depends(get_db),
):
    """
    Recebe e armazena um arquivo dentro do backup identificado por X-Backup-Label.
    O storage fisico e isolado por label — nunca ha colisao entre backups.

    Headers:
      X-Backup-Label  : identificador do backup (deve existir via POST /backups)
      X-Original-Path : caminho original no cliente (base64)
      X-Mtime         : modification time (epoch float)
    """
    _get_backup_or_404(backup_label, db)

    try:
        original_path = base64.b64decode(original_path.encode("ascii")).decode("utf-8")
    except Exception:
        pass

    sha256, content = _read_upload(file)
    size = len(content)

    # Upsert: atualiza se o arquivo ja existe neste backup
    record = (
        db.query(FileRecord)
        .filter(
            FileRecord.backup_label  == backup_label,
            FileRecord.original_path == original_path,
        )
        .first()
    )

    if record and record.sha256 == sha256 and record.size == size and record.mtime == mtime:
        return {"status": "skipped", "reason": "Identico ao registrado neste backup", "file_id": record.id}

    dest = _storage_path(backup_label, sha256, original_path)
    dest.write_bytes(content)

    if record:
        # Remove arquivo fisico antigo se o path mudou
        old_path = Path(record.stored_at)
        if old_path.exists() and old_path != dest:
            old_path.unlink()
        record.sha256     = sha256
        record.size       = size
        record.mtime      = mtime
        record.stored_at  = str(dest)
        record.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(record)
        return {"status": "updated", "file_id": record.id, "backup_label": backup_label, "sha256": sha256}
    else:
        new_record = FileRecord(
            backup_label=backup_label,
            original_path=original_path,
            sha256=sha256,
            size=size,
            mtime=mtime,
            stored_at=str(dest),
        )
        db.add(new_record)
        db.commit()
        db.refresh(new_record)
        return {"status": "stored", "file_id": new_record.id, "backup_label": backup_label, "sha256": sha256}


# -- Files --------------------------------------------------------------------
@app.get("/files", dependencies=[Depends(require_api_key)])
def list_files(
    backup_label: str,
    path_prefix: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Lista arquivos de um backup especifico. backup_label e obrigatorio."""
    _get_backup_or_404(backup_label, db)
    query = db.query(FileRecord).filter(FileRecord.backup_label == backup_label)
    if path_prefix:
        query = query.filter(FileRecord.original_path.startswith(path_prefix))
    return [
        FileInfo(
            id=r.id,
            backup_label=r.backup_label,
            original_path=r.original_path,
            sha256=r.sha256,
            size=r.size,
            mtime=r.mtime,
            stored_at=r.stored_at,
            created_at=str(r.created_at),
        )
        for r in query.order_by(FileRecord.original_path).all()
    ]


@app.get("/files/{file_id}/download", dependencies=[Depends(require_api_key)])
def download_file(file_id: int, db: Session = Depends(get_db)):
    record = db.query(FileRecord).filter(FileRecord.id == file_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Arquivo nao encontrado")
    path = Path(record.stored_at)
    if not path.exists():
        raise HTTPException(status_code=410, detail="Arquivo removido do storage")
    return FileResponse(path, filename=Path(record.original_path).name)


@app.delete("/files/{file_id}", dependencies=[Depends(require_api_key)])
def delete_file(file_id: int, db: Session = Depends(get_db)):
    record = db.query(FileRecord).filter(FileRecord.id == file_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Arquivo nao encontrado")
    path = Path(record.stored_at)
    if path.exists():
        path.unlink()
    db.delete(record)
    db.commit()
    return {"status": "deleted", "file_id": file_id}


# -- Sync ---------------------------------------------------------------------
@app.post("/sync", response_model=SyncResponse, dependencies=[Depends(require_api_key)])
def sync(req: SyncRequest, db: Session = Depends(get_db)):
    """
    Remove do backup apenas os arquivos que nao existem mais no cliente.
    Completamente isolado ao backup_label — nunca toca arquivos de outros backups.
    """
    _get_backup_or_404(req.backup_label, db)

    records = db.query(FileRecord).filter(FileRecord.backup_label == req.backup_label).all()
    client_set = set(req.existing_paths)
    deleted = []

    for record in records:
        if record.original_path not in client_set:
            path = Path(record.stored_at)
            if path.exists():
                path.unlink()
            db.delete(record)
            deleted.append(record.original_path)

    # Atualiza last_run_at do backup
    backup = db.query(BackupID).filter(BackupID.label == req.backup_label).first()
    if backup:
        backup.last_run_at = datetime.utcnow()

    db.commit()
    return SyncResponse(deleted=deleted, deleted_count=len(deleted))