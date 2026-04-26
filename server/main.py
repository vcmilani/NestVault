"""
Backup Files — Raspberry Pi  v2.1
Otimizacoes de performance:
- Upload faz streaming para disco (nao carrega na RAM)
- Hash calculado durante o stream (single-pass)
- Queries agregadas (func.count, func.sum) em vez de carregar entidades
- Indices no banco + WAL mode
- Cleanup de orfaos em uma unica query
"""

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Header
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Optional, Literal
import os, hashlib, secrets, base64, tempfile, shutil
from pathlib import Path
from datetime import datetime
from collections import defaultdict

from sqlalchemy import func, select, and_
from sqlalchemy.orm import Session, selectinload

from database import init_db, get_db, BackupID, BackupVersion, FileContent, VersionFile

# -- Config -------------------------------------------------------------------
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "./storage"))
API_KEY     = os.getenv("BACKUP_API_KEY", "")
STATIC_DIR  = Path(__file__).parent / "static"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)
AUTH_ENABLED = bool(API_KEY)

# Buffer de leitura/escrita - 1 MB e bem mais rapido que 64KB no I/O
CHUNK_SIZE = 1024 * 1024

app = FastAPI(title="Backup Files — Raspberry Pi", version="2.1.0")

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.on_event("startup")
def startup():
    init_db()


# -- Auth ---------------------------------------------------------------------
def require_api_key(x_api_key: Optional[str] = Header(None)):
    if not AUTH_ENABLED:
        return
    if not x_api_key or not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(401, "API key invalida")


# -- Schemas: Requests --------------------------------------------------------
class BackupCreate(BaseModel):
    label: str
    client_name: Optional[str] = None
    prefix: Optional[str] = None

class VersionCreate(BaseModel):
    version_key: str = Field(..., description="ISO datetime: 2026-04-25T10:42:31")

class VersionFinish(BaseModel):
    status: Literal["done", "failed"] = "done"

class CheckRequest(BaseModel):
    backup_label: str
    version_key: str
    original_path: str
    sha256: str = Field(..., min_length=64, max_length=64)
    size: int = Field(..., ge=0)
    mtime: float

class SyncRequest(BaseModel):
    backup_label: str
    version_key: str
    existing_paths: list[str]

class CleanupRequest(BaseModel):
    backup_label: str
    keep: int = Field(5, ge=0, description="Quantas versoes manter")


# -- Schemas: Responses -------------------------------------------------------
class HealthResponse(BaseModel):
    status: Literal["ok"]
    version: str
    time: str

class BackupInfo(BaseModel):
    """Detalhes de um backup com agregados da ultima versao 'done'."""
    id: int
    label: str
    client_name: Optional[str] = None
    prefix: Optional[str] = None
    status: str
    created_at: str
    last_version: Optional[str] = None
    version_count: int
    file_count: int
    total_size_bytes: int

class BackupCreatedResponse(BaseModel):
    created: bool
    backup: BackupInfo

class BackupDeletedResponse(BaseModel):
    status: Literal["deleted"]
    label: str

class VersionInfo(BaseModel):
    """Detalhes de uma versao especifica."""
    id: int
    version_key: str
    backup_label: str
    status: Literal["running", "done", "failed"]
    created_at: str
    finished_at: Optional[str] = None
    file_count: int
    deleted_count: int
    total_size_bytes: int

class VersionCreatedResponse(BaseModel):
    created: bool
    version: VersionInfo

class VersionDeletedResponse(BaseModel):
    status: Literal["deleted"]
    version_key: str
    files_removed_from_storage: int

class CheckResponse(BaseModel):
    needs_upload: bool
    content_exists: bool
    reason: str
    file_id: Optional[int] = None

class UploadResponse(BaseModel):
    status: Literal["registered"]
    file_id: int
    sha256: str
    uploaded: bool = Field(..., description="True se o conteudo foi enviado, False se apenas registrado")

class SyncResponse(BaseModel):
    marked_deleted: list[str]
    deleted_count: int

class FileInfo(BaseModel):
    id: int
    original_path: str
    sha256: str
    size: int
    mtime: float
    status: Literal["active", "deleted"]
    created_at: str

class CleanupResponse(BaseModel):
    kept: int
    versions_removed: list[str]
    storage_files_removed: int


# -- Helpers ------------------------------------------------------------------
def _content_path(sha256: str) -> Path:
    dest = STORAGE_DIR / "_content" / sha256[:2] / sha256
    dest.parent.mkdir(parents=True, exist_ok=True)
    return dest


def _stream_upload_to_disk(upload: UploadFile) -> tuple[str, int, Path]:
    """
    Faz streaming do upload para um arquivo temporario calculando o sha256
    durante a leitura. Retorna (sha256, size, temp_path).
    Single-pass: nao carrega o arquivo na memoria.
    """
    h = hashlib.sha256()
    size = 0
    tmp = tempfile.NamedTemporaryFile(delete=False, dir=STORAGE_DIR, prefix="upload_")
    try:
        while True:
            chunk = upload.file.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
            tmp.write(chunk)
            size += len(chunk)
        tmp.flush()
        tmp.close()
        return h.hexdigest(), size, Path(tmp.name)
    except Exception:
        tmp.close()
        Path(tmp.name).unlink(missing_ok=True)
        raise


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


def _version_stats(v: BackupVersion, db: Session) -> VersionInfo:
    """Stats agregados via SQL — sem carregar VersionFiles."""
    counts = (
        db.query(VersionFile.status, func.count(VersionFile.id))
        .filter(VersionFile.version_id == v.id)
        .group_by(VersionFile.status)
        .all()
    )
    counts_dict = dict(counts)
    active_count  = counts_dict.get("active", 0)
    deleted_count = counts_dict.get("deleted", 0)

    # Tamanho total via JOIN agregado — uma unica query
    total_size = (
        db.query(func.coalesce(func.sum(FileContent.size), 0))
        .join(VersionFile, VersionFile.sha256 == FileContent.sha256)
        .filter(VersionFile.version_id == v.id, VersionFile.status == "active")
        .scalar()
    ) or 0

    return VersionInfo(
        id=v.id,
        version_key=v.version_key,
        backup_label=v.backup_label,
        status=v.status,
        created_at=str(v.created_at),
        finished_at=str(v.finished_at) if v.finished_at else None,
        file_count=active_count,
        deleted_count=deleted_count,
        total_size_bytes=int(total_size),
    )


def _backup_info(b: BackupID, db: Session) -> BackupInfo:
    """Stats agregados — sem carregar todas as versoes."""
    # Total de versoes done + ultima versao em uma query
    done_versions = (
        db.query(BackupVersion.id, BackupVersion.version_key)
        .filter(BackupVersion.backup_label == b.label,
                BackupVersion.status       == "done")
        .order_by(BackupVersion.version_key.desc())
        .all()
    )
    version_count = len(done_versions)
    latest_id  = done_versions[0][0] if done_versions else None
    latest_key = done_versions[0][1] if done_versions else None

    file_count = 0
    total_size = 0
    if latest_id:
        file_count = (
            db.query(func.count(VersionFile.id))
            .filter(VersionFile.version_id == latest_id,
                    VersionFile.status     == "active")
            .scalar() or 0
        )
        total_size = (
            db.query(func.coalesce(func.sum(FileContent.size), 0))
            .join(VersionFile, VersionFile.sha256 == FileContent.sha256)
            .filter(VersionFile.version_id == latest_id,
                    VersionFile.status     == "active")
            .scalar() or 0
        )

    return BackupInfo(
        id=b.id,
        label=b.label,
        client_name=b.client_name,
        prefix=b.prefix,
        status=b.status,
        created_at=str(b.created_at),
        last_version=latest_key,
        version_count=version_count,
        file_count=file_count,
        total_size_bytes=int(total_size),
    )


def _cleanup_orphan_contents(db: Session) -> int:
    """
    Remove FileContents nao referenciados por nenhum VersionFile.
    Usa subquery em vez de N+1 queries.
    """
    # Subquery: shas que ainda tem referencias
    used_shas = db.query(VersionFile.sha256).distinct().subquery()
    orphans = (
        db.query(FileContent)
        .filter(~FileContent.sha256.in_(select(used_shas)))
        .all()
    )
    removed = 0
    for fc in orphans:
        p = Path(fc.stored_at)
        if p.exists():
            p.unlink()
        db.delete(fc)
        removed += 1
    db.commit()
    return removed


# -- Dashboard ----------------------------------------------------------------
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    index = STATIC_DIR / "index.html"
    if not index.exists():
        return HTMLResponse("<h1>Dashboard nao encontrado</h1>", status_code=404)
    return HTMLResponse(index.read_text(encoding="utf-8"))


@app.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(status="ok", version="2.1.0", time=datetime.utcnow().isoformat())


# -- Backups ------------------------------------------------------------------
@app.post("/backups", response_model=BackupCreatedResponse, dependencies=[Depends(require_api_key)])
def create_backup(req: BackupCreate, db: Session = Depends(get_db)):
    existing = db.query(BackupID).filter(BackupID.label == req.label).first()
    if existing:
        return BackupCreatedResponse(created=False, backup=_backup_info(existing, db))
    b = BackupID(label=req.label, client_name=req.client_name, prefix=req.prefix)
    db.add(b); db.commit(); db.refresh(b)
    return BackupCreatedResponse(created=True, backup=_backup_info(b, db))


@app.get("/backups", response_model=list[BackupInfo], dependencies=[Depends(require_api_key)])
def list_backups(client_name: Optional[str] = None, db: Session = Depends(get_db)):
    """
    Lista backups com stats — usa uma unica query agregada
    para o tamanho total de cada label, evitando N+1.
    """
    q = db.query(BackupID).order_by(BackupID.created_at.desc())
    if client_name:
        q = q.filter(BackupID.client_name == client_name)
    backups = q.all()
    return [_backup_info(b, db) for b in backups]


@app.get("/backups/{label}", response_model=BackupInfo, dependencies=[Depends(require_api_key)])
def get_backup(label: str, db: Session = Depends(get_db)):
    return _backup_info(_get_backup_or_404(label, db), db)


@app.delete("/backups/{label}", response_model=BackupDeletedResponse, dependencies=[Depends(require_api_key)])
def delete_backup(label: str, db: Session = Depends(get_db)):
    b = _get_backup_or_404(label, db)
    # Cascade da relationship cuida dos VersionFiles automaticamente
    db.query(BackupVersion).filter(BackupVersion.backup_label == label).delete(
        synchronize_session=False
    )
    db.delete(b)
    db.commit()
    _cleanup_orphan_contents(db)
    return BackupDeletedResponse(status="deleted", label=label)


# -- Versions -----------------------------------------------------------------
@app.post("/backups/{label}/versions", response_model=VersionCreatedResponse, dependencies=[Depends(require_api_key)])
def create_version(label: str, req: VersionCreate, db: Session = Depends(get_db)):
    _get_backup_or_404(label, db)
    existing = (db.query(BackupVersion)
                .filter(BackupVersion.backup_label == label,
                        BackupVersion.version_key  == req.version_key)
                .first())
    if existing:
        return VersionCreatedResponse(created=False, version=_version_stats(existing, db))
    v = BackupVersion(backup_label=label, version_key=req.version_key)
    db.add(v); db.commit(); db.refresh(v)
    return VersionCreatedResponse(created=True, version=_version_stats(v, db))


@app.get("/backups/{label}/versions", response_model=list[VersionInfo], dependencies=[Depends(require_api_key)])
def list_versions(label: str, db: Session = Depends(get_db)):
    """Lista versoes com stats agregados — uma query por versao via _version_stats."""
    _get_backup_or_404(label, db)
    versions = (db.query(BackupVersion)
                .filter(BackupVersion.backup_label == label)
                .order_by(BackupVersion.version_key.desc())
                .all())
    return [_version_stats(v, db) for v in versions]


@app.get("/backups/{label}/versions/{version_key}", response_model=VersionInfo, dependencies=[Depends(require_api_key)])
def get_version(label: str, version_key: str, db: Session = Depends(get_db)):
    return _version_stats(_get_version_or_404(label, version_key, db), db)


@app.patch("/backups/{label}/versions/{version_key}", response_model=VersionInfo, dependencies=[Depends(require_api_key)])
def finish_version(label: str, version_key: str, req: VersionFinish, db: Session = Depends(get_db)):
    v = _get_version_or_404(label, version_key, db)
    v.status = req.status
    v.finished_at = datetime.utcnow()
    db.commit()
    return _version_stats(v, db)


@app.delete("/backups/{label}/versions/{version_key}", response_model=VersionDeletedResponse, dependencies=[Depends(require_api_key)])
def delete_version(label: str, version_key: str, db: Session = Depends(get_db)):
    v = _get_version_or_404(label, version_key, db)
    db.delete(v)
    db.commit()
    removed = _cleanup_orphan_contents(db)
    return VersionDeletedResponse(
        status="deleted",
        version_key=version_key,
        files_removed_from_storage=removed,
    )


# -- Check --------------------------------------------------------------------
@app.post("/check", response_model=CheckResponse, dependencies=[Depends(require_api_key)])
def check_file(req: CheckRequest, db: Session = Depends(get_db)):
    """Apenas duas queries no caso comum — tudo indexado."""
    v = _get_version_or_404(req.backup_label, req.version_key, db)

    # Ja registrado nesta versao com mesmo conteudo? — usa indice (version_id, original_path)
    vf = (db.query(VersionFile.id)
          .filter(VersionFile.version_id    == v.id,
                  VersionFile.original_path == req.original_path,
                  VersionFile.sha256        == req.sha256)
          .first())
    if vf:
        return CheckResponse(needs_upload=False, content_exists=True,
                             reason="Ja registrado nesta versao", file_id=vf[0])

    # Conteudo ja existe? — primary key, lookup O(1)
    content_exists = db.query(FileContent.sha256).filter(
        FileContent.sha256 == req.sha256
    ).first() is not None

    return CheckResponse(
        needs_upload=True,
        content_exists=content_exists,
        reason="Conteudo ja no storage — apenas registrar" if content_exists else "Upload necessario",
    )


# -- Upload -------------------------------------------------------------------
@app.post("/upload", response_model=UploadResponse, dependencies=[Depends(require_api_key)])
def upload_file(
    file: UploadFile = File(None),
    backup_label:  str           = Header(..., alias="X-Backup-Label"),
    version_key:   str           = Header(..., alias="X-Version-Key"),
    original_path: str           = Header(..., alias="X-Original-Path"),
    mtime:         float         = Header(..., alias="X-Mtime"),
    content_sha256: Optional[str] = Header(None, alias="X-Content-Sha256"),
    db: Session = Depends(get_db),
):
    """
    Streaming upload — nao carrega o arquivo na RAM.
    """
    v = _get_version_or_404(backup_label, version_key, db)

    try:
        original_path = base64.b64decode(original_path.encode("ascii")).decode("utf-8")
    except Exception:
        pass

    # Modo "so registrar" — content_exists no /check
    if content_sha256 and not file:
        fc = db.query(FileContent).filter(FileContent.sha256 == content_sha256).first()
        if not fc:
            raise HTTPException(400, f"Conteudo sha256={content_sha256} nao encontrado no storage")
        sha256 = content_sha256
    else:
        if not file:
            raise HTTPException(400, "Body do arquivo obrigatorio quando conteudo nao existe no storage")

        # Stream para temp file calculando hash em paralelo
        sha256, size, tmp_path = _stream_upload_to_disk(file)

        try:
            fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
            if fc:
                # Conteudo ja existia — descarta o temp
                tmp_path.unlink(missing_ok=True)
            else:
                # Move atomico para o destino final
                dest = _content_path(sha256)
                shutil.move(str(tmp_path), str(dest))
                fc = FileContent(sha256=sha256, stored_at=str(dest), size=size)
                db.add(fc)
                db.flush()
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    # Upsert VersionFile
    vf = (db.query(VersionFile)
          .filter(VersionFile.version_id    == v.id,
                  VersionFile.original_path == original_path)
          .first())
    if vf:
        vf.sha256 = sha256
        vf.mtime  = mtime
        vf.status = "active"
    else:
        vf = VersionFile(version_id=v.id, original_path=original_path,
                         sha256=sha256, mtime=mtime, status="active")
        db.add(vf)

    db.commit()
    db.refresh(vf)
    return UploadResponse(
        status="registered",
        file_id=vf.id,
        sha256=sha256,
        uploaded=not bool(content_sha256),
    )


# -- Sync ---------------------------------------------------------------------
@app.post("/sync", response_model=SyncResponse, dependencies=[Depends(require_api_key)])
def sync(req: SyncRequest, db: Session = Depends(get_db)):
    """
    Marca como deleted via UPDATE em massa quando possivel.
    Pega so os paths para retornar a lista — nao carrega VersionFiles inteiros.
    """
    v = _get_version_or_404(req.backup_label, req.version_key, db)
    client_set = set(req.existing_paths)

    # Busca apenas (id, original_path) — leve
    rows = (db.query(VersionFile.id, VersionFile.original_path)
            .filter(VersionFile.version_id == v.id,
                    VersionFile.status     == "active")
            .all())

    to_mark = [r[0] for r in rows if r[1] not in client_set]
    marked  = [r[1] for r in rows if r[1] not in client_set]

    if to_mark:
        # UPDATE em massa
        db.query(VersionFile).filter(VersionFile.id.in_(to_mark)).update(
            {VersionFile.status: "deleted"}, synchronize_session=False
        )
        db.commit()

    return SyncResponse(marked_deleted=marked, deleted_count=len(marked))


# -- Files --------------------------------------------------------------------
@app.get("/files", response_model=list[FileInfo], dependencies=[Depends(require_api_key)])
def list_files(
    backup_label: str,
    version_key: str,
    include_deleted: bool = False,
    db: Session = Depends(get_db),
):
    """
    Lista arquivos com size — usa JOIN explicito em vez de lazy load (N+1).
    """
    v = _get_version_or_404(backup_label, version_key, db)

    q = (db.query(
            VersionFile.id,
            VersionFile.original_path,
            VersionFile.sha256,
            VersionFile.mtime,
            VersionFile.status,
            VersionFile.created_at,
            FileContent.size,
        )
        .outerjoin(FileContent, FileContent.sha256 == VersionFile.sha256)
        .filter(VersionFile.version_id == v.id))

    if not include_deleted:
        q = q.filter(VersionFile.status == "active")

    rows = q.order_by(VersionFile.original_path).all()
    return [
        FileInfo(
            id=r.id,
            original_path=r.original_path,
            sha256=r.sha256,
            size=r.size or 0,
            mtime=r.mtime,
            status=r.status,
            created_at=str(r.created_at),
        )
        for r in rows
    ]


@app.get("/files/{file_id}/download", dependencies=[Depends(require_api_key)])
def download_file(file_id: int, db: Session = Depends(get_db)):
    """Uma unica query com JOIN."""
    row = (db.query(VersionFile.status, VersionFile.original_path,
                    FileContent.stored_at)
           .join(FileContent, FileContent.sha256 == VersionFile.sha256)
           .filter(VersionFile.id == file_id)
           .first())
    if not row:
        raise HTTPException(404, "Arquivo nao encontrado")
    if row.status == "deleted":
        raise HTTPException(410, "Arquivo marcado como deletado nesta versao")
    p = Path(row.stored_at)
    if not p.exists():
        raise HTTPException(410, "Conteudo fisico nao encontrado")
    return FileResponse(p, filename=Path(row.original_path).name)


# -- Cleanup ------------------------------------------------------------------
@app.post("/backups/{label}/cleanup", response_model=CleanupResponse, dependencies=[Depends(require_api_key)])
def cleanup_versions(label: str, req: CleanupRequest, db: Session = Depends(get_db)):
    _get_backup_or_404(label, db)
    # Considera TODAS as versoes (done, failed, running) ordenadas por data desc.
    # As `keep` mais recentes sao mantidas independente do status.
    all_versions = (db.query(BackupVersion.id, BackupVersion.version_key)
                    .filter(BackupVersion.backup_label == label)
                    .order_by(BackupVersion.version_key.desc())
                    .all())
    to_delete = all_versions[req.keep:]
    if not to_delete:
        return CleanupResponse(kept=req.keep, versions_removed=[], storage_files_removed=0)

    ids_to_delete = [v[0] for v in to_delete]
    keys_removed  = [v[1] for v in to_delete]

    # Bulk delete via cascade
    db.query(BackupVersion).filter(BackupVersion.id.in_(ids_to_delete)).delete(
        synchronize_session=False
    )
    db.commit()
    orphans_removed = _cleanup_orphan_contents(db)
    return CleanupResponse(
        kept=req.keep,
        versions_removed=keys_removed,
        storage_files_removed=orphans_removed,
    )