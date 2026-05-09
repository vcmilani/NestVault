"""
Backup Files — Raspberry Pi  v2.5
Otimizacoes de performance:
- Upload faz streaming para disco (nao carrega na RAM)
- Hash calculado durante o stream (single-pass)
- Queries agregadas (func.count, func.sum) em vez de carregar entidades
- Indices no banco + WAL mode
- Cleanup de orfaos em uma unica query
- Limpeza de arquivos ao deletar label/versao feita em background (nao bloqueia o cliente)
"""

from fastapi import FastAPI, Request, HTTPException, Depends, Header, BackgroundTasks
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Optional, Literal
import os, hashlib, secrets, base64, shutil
from pathlib import Path
from datetime import datetime
from collections import defaultdict

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from database import init_db, get_db, SessionLocal, BackupID, BackupVersion, FileContent, VersionFile

# -- Config -------------------------------------------------------------------
_raw_dirs    = os.getenv("STORAGE_DIRS") or os.getenv("STORAGE_DIR", "./storage")
STORAGE_VOLUMES: list[Path] = [Path(p.strip()) for p in _raw_dirs.split(",") if p.strip()]
STORAGE_DIR  = STORAGE_VOLUMES[0]   # alias para retrocompatibilidade
for _v in STORAGE_VOLUMES:
    _v.mkdir(parents=True, exist_ok=True)

API_KEY      = os.getenv("BACKUP_API_KEY", "")
STATIC_DIR   = Path(__file__).parent / "static"
AUTH_ENABLED = bool(API_KEY)

# Buffer de leitura/escrita - 1 MB e bem mais rapido que 64KB no I/O
CHUNK_SIZE = 1024 * 1024

app = FastAPI(title="Backup Files — Raspberry Pi", version="2.8.0")

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

class CheckBatchItem(BaseModel):
    original_path: str
    sha256: str = Field(..., min_length=64, max_length=64)
    size: int = Field(..., ge=0)
    mtime: float

class CheckBatchRequest(BaseModel):
    backup_label: str
    version_key: str
    files: list[CheckBatchItem] = Field(..., min_length=1, max_length=500)

class CheckBatchResultItem(BaseModel):
    needs_upload: bool
    content_exists: bool
    reason: str
    file_id: Optional[int] = None

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
    duration_seconds: Optional[float] = Field(None, description="Duracao do backup em segundos")
    file_count: int
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
    synced: bool

class FileInfo(BaseModel):
    id: int
    original_path: str
    sha256: str
    size: int
    mtime: float
    created_at: str

class CleanupResponse(BaseModel):
    kept: int
    versions_removed: list[str]
    storage_files_removed: int

class OrphanCleanupResponse(BaseModel):
    files_removed: int
    bytes_freed: int

class StorageInfoResponse(BaseModel):
    total_bytes: int
    used_bytes: int
    free_bytes: int
    reclaimable_bytes: int

class CompareFileEntry(BaseModel):
    original_path: str
    sha256: str
    size: int
    mtime: float

class CompareModifiedEntry(BaseModel):
    original_path: str
    v1_sha256: str
    v2_sha256: str
    v1_size: int
    v2_size: int
    size_delta: int

class CompareResponse(BaseModel):
    label: str
    v1: str
    v2: str
    added: list[CompareFileEntry]
    deleted: list[CompareFileEntry]
    modified: list[CompareModifiedEntry]
    summary_unchanged: int


# -- Helpers ------------------------------------------------------------------
def _pick_volume() -> Path:
    """Escolhe o volume com mais espaço livre para o próximo upload."""
    return max(STORAGE_VOLUMES, key=lambda v: shutil.disk_usage(v).free)


def _content_path(sha256: str, volume: Path) -> Path:
    dest = volume / "_content" / sha256[:2] / sha256
    dest.parent.mkdir(parents=True, exist_ok=True)
    return dest


async def _stream_request_to_disk(request: Request, volume: Path) -> tuple[str, int, Path]:
    """
    Faz streaming do body raw da request para disco calculando sha256 em paralelo.
    Sem multipart — o body e o arquivo diretamente (binario puro).
    Tmp escrito no mesmo volume de destino para evitar cross-device move.
    Retorna (sha256, size, tmp_path).
    """
    h    = hashlib.sha256()
    size = 0
    tmp_path = volume / f"_tmp_{os.urandom(8).hex()}"
    try:
        with open(tmp_path, "wb", buffering=0) as f:
            async for chunk in request.stream():
                h.update(chunk)
                f.write(chunk)
                size += len(chunk)
        return h.hexdigest(), size, tmp_path
    except Exception:
        tmp_path.unlink(missing_ok=True)
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
    file_count = (
        db.query(func.count(VersionFile.id))
        .filter(VersionFile.version_id == v.id)
        .scalar()
    ) or 0

    # Tamanho total via JOIN agregado — uma unica query
    total_size = (
        db.query(func.coalesce(func.sum(FileContent.size), 0))
        .join(VersionFile, VersionFile.sha256 == FileContent.sha256)
        .filter(VersionFile.version_id == v.id)
        .scalar()
    ) or 0

    duration = None
    if v.finished_at and v.created_at:
        duration = round((v.finished_at - v.created_at).total_seconds(), 1)

    return VersionInfo(
        id=v.id,
        version_key=v.version_key,
        backup_label=v.backup_label,
        status=v.status,
        created_at=str(v.created_at),
        finished_at=str(v.finished_at) if v.finished_at else None,
        duration_seconds=duration,
        file_count=file_count,
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
            .filter(VersionFile.version_id == latest_id)
            .scalar() or 0
        )
        total_size = (
            db.query(func.coalesce(func.sum(FileContent.size), 0))
            .join(VersionFile, VersionFile.sha256 == FileContent.sha256)
            .filter(VersionFile.version_id == latest_id)
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


def _min_disk_free_percent() -> float:
    """Retorna o menor % livre entre todos os volumes (dispara cleanup se qualquer disco estiver crítico)."""
    return min(shutil.disk_usage(v).free / shutil.disk_usage(v).total * 100
               for v in STORAGE_VOLUMES)


def _auto_cleanup_if_needed(db: Session) -> None:
    free_pct = _min_disk_free_percent()
    if free_pct >= 5.0:
        return

    print(f"[auto-cleanup] Espaço livre mínimo: {free_pct:.1f}% — abaixo de 5%, iniciando limpeza...")

    # Labels com >= 2 versões "done" (as que têm algo a deletar)
    labels_with_versions = (
        db.query(BackupVersion.backup_label)
        .filter(BackupVersion.status == "done")
        .group_by(BackupVersion.backup_label)
        .having(func.count(BackupVersion.id) >= 2)
        .all()
    )

    # Monta fila de versões deletáveis: todas exceto a mais recente de cada label
    deletable: list[BackupVersion] = []
    for (label,) in labels_with_versions:
        versions = (
            db.query(BackupVersion)
            .filter(BackupVersion.backup_label == label, BackupVersion.status == "done")
            .order_by(BackupVersion.version_key.asc())
            .all()
        )
        deletable.extend(versions[:-1])  # mantém sempre a última

    deletable.sort(key=lambda v: v.version_key)  # mais antigas primeiro

    for v in deletable:
        label, key = v.backup_label, v.version_key
        db.delete(v)
        db.commit()
        removed, _ = _cleanup_orphan_contents(db)
        free_pct = _min_disk_free_percent()
        print(f"[auto-cleanup] Removida {label}/{key} — {removed} arquivo(s) do storage — livre mín: {free_pct:.1f}%")
        if free_pct >= 5.0:
            print(f"[auto-cleanup] Espaço normalizado ({free_pct:.1f}%), encerrando.")
            return

    print(f"[auto-cleanup] Concluído. Livre mín: {free_pct:.1f}% — todas as labels com 1 versão.")


def _cleanup_orphan_contents(db: Session) -> tuple[int, int]:
    """
    Remove FileContents nao referenciados por nenhum VersionFile.
    Usa subquery em vez de N+1 queries.
    Retorna (arquivos_removidos, bytes_liberados).
    """
    used_shas = db.query(VersionFile.sha256).distinct().subquery()
    orphans = (
        db.query(FileContent)
        .filter(~FileContent.sha256.in_(select(used_shas)))
        .all()
    )
    removed = 0
    bytes_freed = 0
    for fc in orphans:
        p = Path(fc.stored_at)
        if p.exists():
            p.unlink()
        bytes_freed += fc.size
        db.delete(fc)
        removed += 1
    db.commit()
    return removed, bytes_freed


def _bg_cleanup_orphan_contents() -> None:
    """Background task: cria sua propria sessao DB e limpa conteudos orfaos."""
    db = SessionLocal()
    try:
        count, _ = _cleanup_orphan_contents(db)
        if count:
            print(f"[bg-cleanup] {count} arquivo(s) orfao(s) removido(s) do storage")
    finally:
        db.close()


def _bg_auto_cleanup() -> None:
    """Background task: cria sua propria sessao DB e executa auto-cleanup se necessario."""
    db = SessionLocal()
    try:
        _auto_cleanup_if_needed(db)
    finally:
        db.close()


# -- Dashboard ----------------------------------------------------------------
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    index = STATIC_DIR / "index.html"
    if not index.exists():
        return HTMLResponse("<h1>Dashboard nao encontrado</h1>", status_code=404)
    return HTMLResponse(index.read_text(encoding="utf-8"))


@app.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(status="ok", version="2.8.0", time=datetime.utcnow().isoformat())


@app.get("/storage/info", response_model=StorageInfoResponse, dependencies=[Depends(require_api_key)])
def storage_info(db: Session = Depends(get_db)):
    usages = [shutil.disk_usage(v) for v in STORAGE_VOLUMES]
    usage_total = sum(u.total for u in usages)
    usage_used  = sum(u.used  for u in usages)
    usage_free  = sum(u.free  for u in usages)

    # Keeper = versão "done" mais recente de cada label
    keeper_ids: list[int] = []
    labels = db.query(BackupID.label).all()
    for (label,) in labels:
        latest = (
            db.query(BackupVersion.id)
            .filter(BackupVersion.backup_label == label, BackupVersion.status == "done")
            .order_by(BackupVersion.version_key.desc())
            .first()
        )
        if latest:
            keeper_ids.append(latest[0])

    if keeper_ids:
        kept_shas = (
            db.query(VersionFile.sha256)
            .filter(VersionFile.version_id.in_(keeper_ids))
            .distinct()
            .subquery()
        )
        reclaimable = (
            db.query(func.coalesce(func.sum(FileContent.size), 0))
            .filter(~FileContent.sha256.in_(select(kept_shas)))
            .scalar()
        ) or 0
    else:
        reclaimable = db.query(func.coalesce(func.sum(FileContent.size), 0)).scalar() or 0

    return StorageInfoResponse(
        total_bytes=usage_total,
        used_bytes=usage_used,
        free_bytes=usage_free,
        reclaimable_bytes=int(reclaimable),
    )


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
def delete_backup(label: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    b = _get_backup_or_404(label, db)
    # Cascade da relationship cuida dos VersionFiles automaticamente
    db.query(BackupVersion).filter(BackupVersion.backup_label == label).delete(
        synchronize_session=False
    )
    db.delete(b)
    db.commit()
    background_tasks.add_task(_bg_cleanup_orphan_contents)
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
def finish_version(label: str, version_key: str, req: VersionFinish, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    v = _get_version_or_404(label, version_key, db)
    v.status = req.status
    v.finished_at = datetime.utcnow()
    db.commit()
    if req.status == "done":
        background_tasks.add_task(_bg_auto_cleanup)
    return _version_stats(v, db)


@app.delete("/backups/{label}/versions/{version_key}", response_model=VersionDeletedResponse, dependencies=[Depends(require_api_key)])
def delete_version(label: str, version_key: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    v = _get_version_or_404(label, version_key, db)
    db.delete(v)
    db.commit()
    background_tasks.add_task(_bg_cleanup_orphan_contents)
    return VersionDeletedResponse(
        status="deleted",
        version_key=version_key,
        files_removed_from_storage=0,
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


# -- Check batch --------------------------------------------------------------
@app.post("/check/batch", response_model=list[CheckBatchResultItem], dependencies=[Depends(require_api_key)])
def check_batch(req: CheckBatchRequest, db: Session = Depends(get_db)):
    """Verifica N arquivos em uma unica request. Resultados na mesma ordem da entrada."""
    v = _get_version_or_404(req.backup_label, req.version_key, db)
    results: list[CheckBatchResultItem] = []
    for item in req.files:
        vf = (db.query(VersionFile.id)
              .filter(VersionFile.version_id    == v.id,
                      VersionFile.original_path == item.original_path,
                      VersionFile.sha256        == item.sha256)
              .first())
        if vf:
            results.append(CheckBatchResultItem(
                needs_upload=False, content_exists=True,
                reason="Ja registrado nesta versao", file_id=vf[0]))
            continue
        content_exists = db.query(FileContent.sha256).filter(
            FileContent.sha256 == item.sha256
        ).first() is not None
        results.append(CheckBatchResultItem(
            needs_upload=True,
            content_exists=content_exists,
            reason="Conteudo ja no storage — apenas registrar" if content_exists else "Upload necessario",
        ))
    return results


# -- Upload -------------------------------------------------------------------
@app.post("/upload", response_model=UploadResponse, dependencies=[Depends(require_api_key)])
async def upload_file(
    request: Request,
    backup_label:   str           = Header(..., alias="X-Backup-Label"),
    version_key:    str           = Header(..., alias="X-Version-Key"),
    original_path:  str           = Header(..., alias="X-Original-Path"),
    mtime:          float         = Header(..., alias="X-Mtime"),
    content_sha256: Optional[str] = Header(None, alias="X-Content-Sha256"),
    db: Session = Depends(get_db),
):
    """
    Stream binario puro — sem multipart.
    O body da request E o arquivo diretamente. Sem encoding/decoding MIME.
    Metadados vao nos headers X-*.
    Modo "so registrar": enviar X-Content-Sha256 sem body (content_exists=True no /check).
    """
    v = _get_version_or_404(backup_label, version_key, db)

    try:
        original_path = base64.b64decode(original_path.encode("ascii")).decode("utf-8")
    except Exception:
        pass

    # Modo "so registrar" — conteudo ja existe no storage
    if content_sha256:
        fc = db.query(FileContent).filter(FileContent.sha256 == content_sha256).first()
        if not fc:
            raise HTTPException(400, f"Conteudo sha256={content_sha256} nao encontrado no storage")
        sha256 = content_sha256
    else:
        # Escolhe volume com mais espaço livre; tmp e conteúdo vão pro mesmo disco
        volume = _pick_volume()
        sha256, size, tmp_path = await _stream_request_to_disk(request, volume)

        try:
            fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
            if fc:
                tmp_path.unlink(missing_ok=True)
            else:
                dest = _content_path(sha256, volume)
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
    else:
        vf = VersionFile(version_id=v.id, original_path=original_path,
                         sha256=sha256, mtime=mtime)
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
    """Confirma que a versao foi sincronizada com o cliente."""
    _get_version_or_404(req.backup_label, req.version_key, db)
    return SyncResponse(synced=True)


# -- Files --------------------------------------------------------------------
@app.get("/files", response_model=list[FileInfo], dependencies=[Depends(require_api_key)])
def list_files(
    backup_label: str,
    version_key: str,
    db: Session = Depends(get_db),
):
    """Lista arquivos com size — usa JOIN explicito em vez de lazy load (N+1)."""
    v = _get_version_or_404(backup_label, version_key, db)

    rows = (db.query(
                VersionFile.id,
                VersionFile.original_path,
                VersionFile.sha256,
                VersionFile.mtime,
                VersionFile.created_at,
                FileContent.size,
            )
            .outerjoin(FileContent, FileContent.sha256 == VersionFile.sha256)
            .filter(VersionFile.version_id == v.id)
            .order_by(VersionFile.original_path)
            .all())

    return [
        FileInfo(
            id=r.id,
            original_path=r.original_path,
            sha256=r.sha256,
            size=r.size or 0,
            mtime=r.mtime,
            created_at=str(r.created_at),
        )
        for r in rows
    ]


@app.get("/files/{file_id}/download", dependencies=[Depends(require_api_key)])
def download_file(file_id: int, db: Session = Depends(get_db)):
    """Uma unica query com JOIN."""
    row = (db.query(VersionFile.original_path, FileContent.stored_at)
           .join(FileContent, FileContent.sha256 == VersionFile.sha256)
           .filter(VersionFile.id == file_id)
           .first())
    if not row:
        raise HTTPException(404, "Arquivo nao encontrado")
    p = Path(row.stored_at)
    if not p.exists():
        raise HTTPException(410, "Conteudo fisico nao encontrado")
    return FileResponse(p, filename=Path(row.original_path).name)


# -- Compare ------------------------------------------------------------------
@app.get("/backups/{label}/compare", response_model=CompareResponse, dependencies=[Depends(require_api_key)])
def compare_versions(label: str, v1: str, v2: str, db: Session = Depends(get_db)):
    """Compara arquivos entre duas versoes. v1 = base, v2 = nova. Duas queries SQL + diff em Python."""
    ver1 = _get_version_or_404(label, v1, db)
    ver2 = _get_version_or_404(label, v2, db)

    def _load_files(version_id):
        rows = (db.query(
                    VersionFile.original_path,
                    VersionFile.sha256,
                    VersionFile.mtime,
                    FileContent.size,
                )
                .outerjoin(FileContent, FileContent.sha256 == VersionFile.sha256)
                .filter(VersionFile.version_id == version_id)
                .all())
        return {r.original_path: r for r in rows}

    files1 = _load_files(ver1.id)
    files2 = _load_files(ver2.id)
    paths1, paths2 = set(files1), set(files2)

    added = [
        CompareFileEntry(original_path=p, sha256=files2[p].sha256,
                         size=files2[p].size or 0, mtime=files2[p].mtime)
        for p in sorted(paths2 - paths1)
    ]
    deleted = [
        CompareFileEntry(original_path=p, sha256=files1[p].sha256,
                         size=files1[p].size or 0, mtime=files1[p].mtime)
        for p in sorted(paths1 - paths2)
    ]
    modified, unchanged = [], 0
    for p in sorted(paths1 & paths2):
        f1, f2 = files1[p], files2[p]
        if f1.sha256 != f2.sha256:
            modified.append(CompareModifiedEntry(
                original_path=p,
                v1_sha256=f1.sha256, v2_sha256=f2.sha256,
                v1_size=f1.size or 0, v2_size=f2.size or 0,
                size_delta=(f2.size or 0) - (f1.size or 0),
            ))
        else:
            unchanged += 1

    return CompareResponse(label=label, v1=v1, v2=v2,
                           added=added, deleted=deleted, modified=modified,
                           summary_unchanged=unchanged)


# -- Maintenance --------------------------------------------------------------
@app.post("/maintenance/cleanup-orphans", response_model=OrphanCleanupResponse, dependencies=[Depends(require_api_key)])
def force_cleanup_orphans(db: Session = Depends(get_db)):
    """Remove todos os FileContents nao referenciados por nenhuma versao ativa."""
    files_removed, bytes_freed = _cleanup_orphan_contents(db)
    return OrphanCleanupResponse(files_removed=files_removed, bytes_freed=bytes_freed)


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
    orphans_removed, _ = _cleanup_orphan_contents(db)
    return CleanupResponse(
        kept=req.keep,
        versions_removed=keys_removed,
        storage_files_removed=orphans_removed,
    )