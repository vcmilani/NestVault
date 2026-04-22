"""
Backup Server - FastAPI
Roda na Raspberry Pi. Recebe, armazena e controla arquivos com deduplicação por metadados.
"""

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Header
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional
import os
import shutil
import hashlib
import secrets
import base64
from pathlib import Path
from datetime import datetime

from database import init_db, get_db, FileRecord, SessionLocal
from sqlalchemy.orm import Session

# ── Config ──────────────────────────────────────────────────────────────────
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "./storage"))
API_KEY = os.getenv("BACKUP_API_KEY", "change-me-in-production")
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Backup Server", version="1.0.0")


# ── Startup ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
def startup():
    init_db()


# ── Auth ─────────────────────────────────────────────────────────────────────
def require_api_key(x_api_key: str = Header(...)):
    if not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="API key inválida")


# ── Schemas ───────────────────────────────────────────────────────────────────
class CheckRequest(BaseModel):
    original_path: str   # caminho original no cliente  ex: /home/pi/docs/relatorio.pdf
    sha256: str          # hash do conteúdo do arquivo
    size: int            # tamanho em bytes
    mtime: float         # modification time (epoch)


class CheckResponse(BaseModel):
    needs_upload: bool
    reason: str
    file_id: Optional[int] = None


class FileInfo(BaseModel):
    id: int
    original_path: str
    sha256: str
    size: int
    mtime: float
    stored_at: str
    created_at: str


# ── Helpers ───────────────────────────────────────────────────────────────────
def _storage_path(sha256: str, original_path: str) -> Path:
    """
    Armazena em: storage/<2 primeiros chars do hash>/<hash>_<nome_do_arquivo>
    Isso evita colisão de nomes e distribui os arquivos em subpastas.
    """
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


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.post("/check", response_model=CheckResponse, dependencies=[Depends(require_api_key)])
def check_file(req: CheckRequest, db: Session = Depends(get_db)):
    """
    Verifica se o arquivo precisa ser enviado.
    O cliente manda os metadados; o servidor responde se precisa de upload.

    Lógica:
    - Se existe um registro com o mesmo original_path, sha256, size e mtime → não precisa
    - Caso contrário → precisa fazer upload
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
            reason="Metadados idênticos — arquivo já está no backup",
            file_id=record.id,
        )

    # Verifica se é um arquivo diferente no mesmo path (modificado)
    existing = db.query(FileRecord).filter(FileRecord.original_path == req.original_path).first()
    if existing:
        return CheckResponse(
            needs_upload=True,
            reason="Arquivo modificado — nova versão será armazenada",
        )

    return CheckResponse(
        needs_upload=True,
        reason="Arquivo novo — upload necessário",
    )


@app.post("/upload", dependencies=[Depends(require_api_key)])
async def upload_file(
    file: UploadFile = File(...),
    original_path: str = Header(..., alias="X-Original-Path"),
    mtime: float = Header(..., alias="X-Mtime"),
    db: Session = Depends(get_db),
):
    """
    Recebe o arquivo e armazena. Registra metadados no banco.
    Cabeçalhos obrigatórios:
      X-Original-Path : caminho original no cliente
      X-Mtime         : modification time (epoch float)
    """
    # Decodifica o path (cliente envia em base64 para evitar UnicodeEncodeError nos headers)
    try:
        original_path = base64.b64decode(original_path.encode("ascii")).decode("utf-8")
    except Exception:
        pass  # se nao for base64, usa o valor bruto

    sha256, content = _sha256_of_upload(file)
    size = len(content)

    # Dupla verificação: o arquivo pode já ter chegado por outra instância
    record = (
        db.query(FileRecord)
        .filter(
            FileRecord.original_path == original_path,
            FileRecord.sha256 == sha256,
            FileRecord.size == size,
            FileRecord.mtime == mtime,
        )
        .first()
    )
    if record:
        return {"status": "skipped", "reason": "Já existe registro idêntico", "file_id": record.id}

    # Persiste o arquivo
    dest = _storage_path(sha256, original_path)
    dest.write_bytes(content)

    # Registra no banco (nova versão substitui a anterior no mesmo path)
    new_record = FileRecord(
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
        "sha256": sha256,
        "stored_at": str(dest),
    }


@app.get("/files", dependencies=[Depends(require_api_key)])
def list_files(
    path_prefix: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Lista todos os arquivos no backup. Filtra por prefixo de path se informado."""
    query = db.query(FileRecord)
    if path_prefix:
        query = query.filter(FileRecord.original_path.startswith(path_prefix))
    records = query.order_by(FileRecord.created_at.desc()).all()
    return [
        FileInfo(
            id=r.id,
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
    """Faz download de um arquivo pelo ID."""
    record = db.query(FileRecord).filter(FileRecord.id == file_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")
    path = Path(record.stored_at)
    if not path.exists():
        raise HTTPException(status_code=410, detail="Arquivo removido do storage")
    return FileResponse(path, filename=Path(record.original_path).name)


@app.delete("/files/{file_id}", dependencies=[Depends(require_api_key)])
def delete_file(file_id: int, db: Session = Depends(get_db)):
    """Remove um arquivo do backup (registro + arquivo físico)."""
    record = db.query(FileRecord).filter(FileRecord.id == file_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")
    path = Path(record.stored_at)
    if path.exists():
        path.unlink()
    db.delete(record)
    db.commit()
    return {"status": "deleted", "file_id": file_id}


class SyncRequest(BaseModel):
    existing_paths: list[str]   # todos os paths que EXISTEM no cliente agora
    path_prefix: Optional[str] = None  # se informado, limpa apenas dentro desse prefixo


class SyncResponse(BaseModel):
    deleted: list[str]
    deleted_count: int


@app.post("/sync", response_model=SyncResponse, dependencies=[Depends(require_api_key)])
def sync(req: SyncRequest, db: Session = Depends(get_db)):
    """
    Recebe a lista completa de paths que existem no cliente.
    Remove do backup tudo que não está mais lá.

    Se path_prefix for informado, a limpeza é restrita a arquivos
    que comecem com esse prefixo (útil quando vários clientes
    compartilham o mesmo servidor).
    """
    query = db.query(FileRecord)
    if req.path_prefix:
        query = query.filter(FileRecord.original_path.startswith(req.path_prefix))

    server_records = query.all()
    client_set = set(req.existing_paths)

    deleted = []
    for record in server_records:
        if record.original_path not in client_set:
            # Remove arquivo físico
            path = Path(record.stored_at)
            if path.exists():
                path.unlink()
            db.delete(record)
            deleted.append(record.original_path)

    db.commit()
    return SyncResponse(deleted=deleted, deleted_count=len(deleted))
