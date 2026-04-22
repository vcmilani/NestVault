"""
Backup Server - FastAPI
Roda na Raspberry Pi. Recebe, armazena e controla arquivos com deduplicacao por metadados.
Suporta sessoes de backup para permitir restore seletivo.
"""

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Header
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import os
import hashlib
import secrets
import base64
import uuid
from pathlib import Path
from datetime import datetime

from database import init_db, get_db, FileRecord, BackupSession, SessionLocal
from sqlalchemy.orm import Session

# -- Config -------------------------------------------------------------------
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "./storage"))
API_KEY     = os.getenv("BACKUP_API_KEY", "change-me-in-production")
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Backup Server", version="2.0.0")


# -- Startup ------------------------------------------------------------------
@app.on_event("startup")
def startup():
    init_db()


# -- Auth ---------------------------------------------------------------------
def require_api_key(x_api_key: str = Header(...)):
    if not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="API key invalida")


# -- Schemas ------------------------------------------------------------------
class CheckRequest(BaseModel):
    original_path: str
    sha256: str
    size: int
    mtime: float

class CheckResponse(BaseModel):
    needs_upload: bool
    reason: str
    file_id: Optional[int] = None

class SessionCreate(BaseModel):
    label: Optional[str] = None       # ex: "backup-semanal", "pre-atualizacao"
    client_name: Optional[str] = None # ex: "notebook-joao", "servidor-web"
    prefix: Optional[str] = None

class SessionInfo(BaseModel):
    id: str
    label: Optional[str]
    client_name: Optional[str]
    prefix: Optional[str]
    started_at: str
    finished_at: Optional[str]
    status: str
    file_count: int
    total_size_bytes: int

class SessionFinish(BaseModel):
    status: str = "done"  # done | failed

class FileInfo(BaseModel):
    id: int
    session_id: Optional[str]
    original_path: str
    sha256: str
    size: int
    mtime: float
    stored_at: str
    created_at: str

class SyncRequest(BaseModel):
    existing_paths: list[str]
    path_prefix: Optional[str] = None

class SyncResponse(BaseModel):
    deleted: list[str]
    deleted_count: int


# -- Helpers ------------------------------------------------------------------
def _storage_path(sha256: str, original_path: str) -> Path:
    prefix = sha256[:2]
    filename = Path(original_path).name
    dest = STORAGE_DIR / prefix / f"{sha256[:16]}_{filename}"
    dest.parent.mkdir(parents=True, exist_ok=True)
    return dest

def _sha256_of_upload(upload: UploadFile) -> tuple[str, bytes]:
    h = hashlib.sha256()
    chunks = []
    while True:
        chunk = upload.file.read(65536)
        if not chunk:
            break
        h.update(chunk)
        chunks.append(chunk)
    upload.file.seek(0)
    return h.hexdigest(), b"".join(chunks)

def _session_info(record: BackupSession, db: Session) -> SessionInfo:
    files = db.query(FileRecord).filter(FileRecord.session_id == record.id).all()
    return SessionInfo(
        id=record.id,
        label=record.label,
        client_name=record.client_name,
        prefix=record.prefix,
        started_at=str(record.started_at),
        finished_at=str(record.finished_at) if record.finished_at else None,
        status=record.status,
        file_count=len(files),
        total_size_bytes=sum(f.size for f in files),
    )


# -- Health -------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


# -- Sessions -----------------------------------------------------------------
@app.post("/sessions", dependencies=[Depends(require_api_key)])
def create_session(req: SessionCreate, db: Session = Depends(get_db)):
    """Cria uma nova sessao de backup. Retorna o session_id a ser usado nos uploads."""
    session = BackupSession(
        id=str(uuid.uuid4()),
        label=req.label,
        client_name=req.client_name,
        prefix=req.prefix,
    )
    db.add(session)
    db.commit()
    db.refresh(session)
    return {"session_id": session.id, "started_at": str(session.started_at)}


@app.get("/sessions", dependencies=[Depends(require_api_key)])
def list_sessions(
    client_name: Optional[str] = None,
    status: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Lista todas as sessoes de backup com contagem de arquivos e tamanho total."""
    query = db.query(BackupSession).order_by(BackupSession.started_at.desc())
    if client_name:
        query = query.filter(BackupSession.client_name == client_name)
    if status:
        query = query.filter(BackupSession.status == status)
    return [_session_info(s, db) for s in query.all()]


@app.get("/sessions/{session_id}", dependencies=[Depends(require_api_key)])
def get_session(session_id: str, db: Session = Depends(get_db)):
    """Detalhes de uma sessao especifica."""
    record = db.query(BackupSession).filter(BackupSession.id == session_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Sessao nao encontrada")
    return _session_info(record, db)


@app.patch("/sessions/{session_id}", dependencies=[Depends(require_api_key)])
def finish_session(session_id: str, req: SessionFinish, db: Session = Depends(get_db)):
    """Marca a sessao como concluida (done) ou falha (failed)."""
    record = db.query(BackupSession).filter(BackupSession.id == session_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Sessao nao encontrada")
    record.status = req.status
    record.finished_at = datetime.utcnow()
    db.commit()
    return _session_info(record, db)


@app.delete("/sessions/{session_id}", dependencies=[Depends(require_api_key)])
def delete_session(session_id: str, db: Session = Depends(get_db)):
    """Remove uma sessao e todos os seus arquivos do backup."""
    record = db.query(BackupSession).filter(BackupSession.id == session_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Sessao nao encontrada")
    files = db.query(FileRecord).filter(FileRecord.session_id == session_id).all()
    deleted_files = []
    for f in files:
        path = Path(f.stored_at)
        if path.exists():
            path.unlink()
        db.delete(f)
        deleted_files.append(f.original_path)
    db.delete(record)
    db.commit()
    return {"status": "deleted", "session_id": session_id, "files_removed": len(deleted_files)}


# -- Check & Upload -----------------------------------------------------------
@app.post("/check", response_model=CheckResponse, dependencies=[Depends(require_api_key)])
def check_file(req: CheckRequest, db: Session = Depends(get_db)):
    """
    Verifica se o arquivo precisa ser enviado comparando metadados.
    Nao considera session_id — verifica se o conteudo ja existe no servidor.
    """
    record = (
        db.query(FileRecord)
        .filter(
            FileRecord.original_path == req.original_path,
            FileRecord.sha256 == req.sha256,
            FileRecord.size == req.size,
            FileRecord.mtime == req.mtime,
        )
        .first()
    )
    if record:
        return CheckResponse(
            needs_upload=False,
            reason="Metadados identicos — arquivo ja esta no backup",
            file_id=record.id,
        )
    existing = db.query(FileRecord).filter(FileRecord.original_path == req.original_path).first()
    if existing:
        return CheckResponse(needs_upload=True, reason="Arquivo modificado — nova versao sera armazenada")
    return CheckResponse(needs_upload=True, reason="Arquivo novo — upload necessario")


@app.post("/upload", dependencies=[Depends(require_api_key)])
async def upload_file(
    file: UploadFile = File(...),
    original_path: str = Header(..., alias="X-Original-Path"),
    mtime: float = Header(..., alias="X-Mtime"),
    session_id: Optional[str] = Header(None, alias="X-Session-Id"),
    db: Session = Depends(get_db),
):
    """
    Recebe o arquivo e armazena. Associa ao session_id se informado.
    Headers:
      X-Original-Path : caminho original no cliente (base64)
      X-Mtime         : modification time (epoch float)
      X-Session-Id    : ID da sessao de backup (opcional)
    """
    # Decodifica path (base64 para suportar caracteres especiais)
    try:
        original_path = base64.b64decode(original_path.encode("ascii")).decode("utf-8")
    except Exception:
        pass

    # Valida sessao se informada
    if session_id:
        session = db.query(BackupSession).filter(BackupSession.id == session_id).first()
        if not session:
            raise HTTPException(status_code=404, detail=f"Sessao '{session_id}' nao encontrada")

    sha256, content = _sha256_of_upload(file)
    size = len(content)

    # Dupla verificacao
    record = (
        db.query(FileRecord)
        .filter(
            FileRecord.session_id == session_id,
            FileRecord.original_path == original_path,
            FileRecord.sha256 == sha256,
            FileRecord.size == size,
            FileRecord.mtime == mtime,
        )
        .first()
    )
    if record:
        return {"status": "skipped", "reason": "Ja existe registro identico nesta sessao", "file_id": record.id}

    dest = _storage_path(sha256, original_path)
    dest.write_bytes(content)

    new_record = FileRecord(
        session_id=session_id,
        original_path=original_path,
        sha256=sha256,
        size=size,
        mtime=mtime,
        stored_at=str(dest),
    )
    db.add(new_record)
    db.commit()
    db.refresh(new_record)

    return {
        "status": "stored",
        "file_id": new_record.id,
        "session_id": session_id,
        "sha256": sha256,
        "stored_at": str(dest),
    }


# -- Files --------------------------------------------------------------------
@app.get("/files", dependencies=[Depends(require_api_key)])
def list_files(
    session_id: Optional[str] = None,
    path_prefix: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Lista arquivos. Filtra por session_id e/ou path_prefix."""
    query = db.query(FileRecord)
    if session_id:
        query = query.filter(FileRecord.session_id == session_id)
    if path_prefix:
        query = query.filter(FileRecord.original_path.startswith(path_prefix))
    records = query.order_by(FileRecord.created_at.desc()).all()
    return [
        FileInfo(
            id=r.id,
            session_id=r.session_id,
            original_path=r.original_path,
            sha256=r.sha256,
            size=r.size,
            mtime=r.mtime,
            stored_at=r.stored_at,
            created_at=str(r.created_at),
        )
        for r in records
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
    """Remove do backup arquivos que nao existem mais no cliente."""
    query = db.query(FileRecord)
    if req.path_prefix:
        query = query.filter(FileRecord.original_path.startswith(req.path_prefix))
    server_records = query.all()
    client_set = set(req.existing_paths)
    deleted = []
    for record in server_records:
        if record.original_path not in client_set:
            path = Path(record.stored_at)
            if path.exists():
                path.unlink()
            db.delete(record)
            deleted.append(record.original_path)
    db.commit()
    return SyncResponse(deleted=deleted, deleted_count=len(deleted))