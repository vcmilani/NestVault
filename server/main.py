"""
NestVault  v2.5
Otimizacoes de performance:
- Upload faz streaming para disco (nao carrega na RAM)
- Hash calculado durante o stream (single-pass)
- Queries agregadas (func.count, func.sum) em vez de carregar entidades
- Indices no banco + WAL mode
- Cleanup de orfaos em uma unica query
- Limpeza de arquivos ao deletar label/versao feita em background (nao bloqueia o cliente)
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, Depends, Header, BackgroundTasks
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Optional, Literal
import asyncio, os, hashlib, secrets, base64, shutil, logging
from pathlib import Path
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from database import init_db, get_db, SessionLocal, BackupID, BackupVersion, FileContent, FileContentCopy, VersionFile
import crypto

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backup-server")

# -- Config -------------------------------------------------------------------
_raw_dirs    = os.getenv("STORAGE_DIRS") or os.getenv("STORAGE_DIR", "./storage")
STORAGE_VOLUMES: list[Path] = [Path(p.strip()) for p in _raw_dirs.split(",") if p.strip()]
STORAGE_DIR  = STORAGE_VOLUMES[0]   # alias para retrocompatibilidade
for _v in STORAGE_VOLUMES:
    _v.mkdir(parents=True, exist_ok=True)

API_KEY      = os.getenv("BACKUP_API_KEY", "")
STATIC_DIR   = Path(__file__).parent / "static"
AUTH_ENABLED = bool(API_KEY)

ENCRYPTION_ENABLED = os.getenv("ENCRYPTION_ENABLED", "false").lower() == "true"
_encryption_key: bytes | None = None

# Buffer de leitura/escrita - 1 MB e bem mais rapido que 64KB no I/O
CHUNK_SIZE = 1024 * 1024

REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "1"))
# 1 = sem replicação (padrão — comportamento original, compatível com RAID físico/ZFS)
# 2+ = replicar para N volumes
# 0 = espelhar para todos os volumes saudáveis

# -- Volume health ------------------------------------------------------------
_degraded_volumes: set[Path] = set()


def _safe_disk_usage(v: Path) -> tuple | None:
    """Retorna disk_usage ou None se o volume estiver inacessível (marca como degraded)."""
    try:
        result = shutil.disk_usage(v)
        _degraded_volumes.discard(v)
        return result
    except OSError:
        if v not in _degraded_volumes:
            log.error(f"[volume] {v} inacessível — marcado como degraded")
            _degraded_volumes.add(v)
        return None


def _healthy_volumes() -> list[Path]:
    return [v for v in STORAGE_VOLUMES if v not in _degraded_volumes]


def _target_replicas() -> int:
    healthy = len(_healthy_volumes())
    factor  = REPLICATION_FACTOR if REPLICATION_FACTOR > 0 else len(STORAGE_VOLUMES)
    if healthy < factor:
        log.warning(f"[replication] fator={factor} > volumes saudáveis={healthy}")
    return min(factor, max(1, healthy))


def _ensure_replicas(sha256: str, source_path: Path, db: Session) -> None:
    """Copia para volumes adicionais até atingir _target_replicas()."""
    target  = _target_replicas()
    copies  = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256).all()
    vol_set = {c.volume_path for c in copies}
    added   = []
    for vol in _healthy_volumes():
        if len(copies) >= target:
            break
        if str(vol) in vol_set:
            continue
        try:
            dest = _content_path(sha256, vol)
            log.info(f"[replication] {sha256[:8]}… → {vol}")
            shutil.copy2(str(source_path), str(dest))
            copy = FileContentCopy(sha256=sha256, stored_at=str(dest), volume_path=str(vol))
            db.add(copy)
            copies.append(copy)
            vol_set.add(str(vol))
            added.append(str(vol))
            log.info(f"[replication] {sha256[:8]}… copiado para {vol} com sucesso")
        except OSError as e:
            log.warning(f"[replication] Falha ao replicar {sha256} para {vol}: {e}")
    if added:
        log.info(f"[replication] {sha256[:8]}… replicação concluída — {len(added)} nova(s) cópia(s): {added}")


def _rereplicate_to_volume(v: Path) -> None:
    db = SessionLocal()
    try:
        log.info(f"[rereplicate] Iniciando re-replicação para {v}")
        shas_on_v = {r.sha256 for r in db.query(FileContentCopy.sha256)
                                          .filter(FileContentCopy.volume_path == str(v)).all()}
        target = _target_replicas()
        underfilled = (
            db.query(FileContent.sha256, func.count(FileContentCopy.id).label("cnt"))
            .outerjoin(FileContentCopy, FileContentCopy.sha256 == FileContent.sha256)
            .group_by(FileContent.sha256)
            .having(func.count(FileContentCopy.id) < target)
            .all()
        )
        total_under = len(underfilled)
        count = 0
        for (sha256, _) in underfilled:
            if sha256 in shas_on_v:
                continue
            source = (db.query(FileContentCopy)
                        .filter(FileContentCopy.sha256 == sha256,
                                ~FileContentCopy.volume_path.in_([str(d) for d in _degraded_volumes]))
                        .first())
            if not source:
                continue
            try:
                dest = _content_path(sha256, v)
                log.info(f"[rereplicate] [{count + 1}/{total_under}] {sha256[:8]}… → {v}")
                shutil.copy2(source.stored_at, str(dest))
                db.add(FileContentCopy(sha256=sha256, stored_at=str(dest), volume_path=str(v)))
                count += 1
                log.info(f"[rereplicate] [{count}/{total_under}] {sha256[:8]}… copiado com sucesso")
            except OSError as e:
                log.warning(f"[rereplicate] Erro em {v}: {e} — abortando")
                break
        if count:
            db.commit()
            log.info(f"[rereplicate] {count} arquivo(s) re-replicados para {v}")
        else:
            log.info(f"[rereplicate] Nenhum arquivo sub-replicado encontrado para {v}")
    finally:
        db.close()


def _backfill_content_copies() -> None:
    """Cria FileContentCopy para FileContent sem cópia (instâncias existentes)."""
    db = SessionLocal()
    try:
        existing_shas = {r.sha256 for r in db.query(FileContentCopy.sha256).distinct().all()}
        to_fill = (db.query(FileContent).filter(~FileContent.sha256.in_(existing_shas)).all()
                   if existing_shas else db.query(FileContent).all())
        count = 0
        for fc in to_fill:
            p = Path(fc.stored_at)
            vol = str(p.parents[2])  # {volume}/_content/{xx}/{sha256} → parents[2] = volume
            db.add(FileContentCopy(sha256=fc.sha256, stored_at=fc.stored_at, volume_path=vol))
            count += 1
        if count:
            db.commit()
            log.info(f"[backfill] {count} entrada(s) migradas para file_content_copies")
        else:
            log.info("[backfill] Nenhuma entrada para migrar — file_content_copies já atualizado")
    except Exception as e:
        log.error(f"[backfill] Erro: {e}")
    finally:
        db.close()


async def _volume_health_monitor() -> None:
    """Tenta recuperar volumes degraded a cada 60 s."""
    while True:
        await asyncio.sleep(60)
        for v in list(_degraded_volumes):
            usage = _safe_disk_usage(v)
            if usage:
                log.info(f"[volume] {v} recuperado — iniciando re-replicação")
                asyncio.get_event_loop().run_in_executor(None, _rereplicate_to_volume, v)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _encryption_key
    init_db()
    if ENCRYPTION_ENABLED:
        _encryption_key = crypto.load_key()  # lança ValueError se inválida — falha rápido
        log.info("Criptografia: habilitada (AES-256-GCM)")
    else:
        log.info("Criptografia: desabilitada")
    asyncio.get_event_loop().run_in_executor(None, _backfill_content_copies)
    monitor = asyncio.create_task(_volume_health_monitor())
    log.info(f"Servidor iniciado — {len(STORAGE_VOLUMES)} volume(s): {[str(v) for v in STORAGE_VOLUMES]}")
    log.info(f"Auth: {'habilitada' if AUTH_ENABLED else 'desabilitada'}")
    yield
    monitor.cancel()


app = FastAPI(title="NestVault", version="3.1.1", lifespan=lifespan)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


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

class AbsorbRequest(BaseModel):
    source_version_key: str

class AbsorbResponse(BaseModel):
    inherited: int
    skipped: int


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

class RereplicateResponse(BaseModel):
    replicated: int
    skipped: int
    target_copies: int

class EncryptExistingResponse(BaseModel):
    files_encrypted: int
    bytes_processed: int
    skipped: int

class StorageInfoResponse(BaseModel):
    total_bytes: int
    used_bytes: int
    free_bytes: int
    reclaimable_bytes: int

class DiskVolumeInfo(BaseModel):
    path: str
    total_bytes: int
    used_bytes: int
    free_bytes: int
    content_files: int
    content_bytes: int
    status: Literal["ok", "degraded"]

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
    """Escolhe o volume saudável com mais espaço livre para o próximo upload."""
    healthy = _healthy_volumes()
    if not healthy:
        raise HTTPException(503, "Nenhum volume de storage disponível")
    return max(healthy, key=lambda v: _safe_disk_usage(v).free)


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
    """Retorna o menor % livre entre os volumes saudáveis."""
    usages = [_safe_disk_usage(v) for v in STORAGE_VOLUMES if v not in _degraded_volumes]
    usages = [u for u in usages if u]
    if not usages:
        return 100.0  # tudo degraded → nada a limpar
    return min(u.free / u.total * 100 for u in usages)


def _auto_cleanup_if_needed(db: Session) -> None:
    free_pct = _min_disk_free_percent()
    if free_pct >= 5.0:
        return

    log.warning(f"[auto-cleanup] Espaço livre mínimo: {free_pct:.1f}% — abaixo de 5%, iniciando limpeza...")

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
        log.info(f"[auto-cleanup] Removida {label}/{key} — {removed} arquivo(s) — livre mín: {free_pct:.1f}%")
        if free_pct >= 5.0:
            log.info(f"[auto-cleanup] Espaço normalizado ({free_pct:.1f}%), encerrando.")
            return

    log.info(f"[auto-cleanup] Concluído. Livre mín: {free_pct:.1f}% — todas as labels com 1 versão.")


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
        copies = db.query(FileContentCopy).filter(FileContentCopy.sha256 == fc.sha256).all()
        failed = False
        for copy in copies:
            p = Path(copy.stored_at)
            if p.exists():
                try:
                    p.unlink()
                except OSError as e:
                    log.warning(f"[cleanup-orphans] Não foi possível remover {p}: {e} — pulando")
                    failed = True
                    continue
            db.delete(copy)
        if failed:
            continue
        # fallback: se não havia cópias na nova tabela, tenta o stored_at legado
        if not copies:
            p = Path(fc.stored_at)
            if p.exists():
                try:
                    p.unlink()
                except OSError as e:
                    log.warning(f"[cleanup-orphans] Não foi possível remover {p}: {e} — pulando")
                    continue
        bytes_freed += fc.size
        db.delete(fc)
        removed += 1
    if removed:
        log.debug(f"[cleanup-orphans] {removed} arquivo(s) — {bytes_freed / 1024:.1f} KB liberados")
    db.commit()
    return removed, bytes_freed


def _bg_cleanup_orphan_contents() -> None:
    """Background task: cria sua propria sessao DB e limpa conteudos orfaos."""
    db = SessionLocal()
    try:
        log.info("[bg-cleanup] iniciando limpeza de conteúdos órfãos")
        count, _ = _cleanup_orphan_contents(db)
        if count:
            log.info(f"[bg-cleanup] {count} arquivo(s) orfao(s) removido(s) do storage")
        else:
            log.info("[bg-cleanup] nenhuma limpeza necessária, não havia arquivos órfãos")
    finally:
        db.close()


def _bg_auto_cleanup() -> None:
    """Background task: cria sua propria sessao DB e executa auto-cleanup se necessario."""
    db = SessionLocal()
    try:
        log.info("[bg-auto-cleanup] verificando necessidade de limpeza automática")
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


@app.get("/disks", response_class=HTMLResponse, include_in_schema=False)
def disks_page():
    page = STATIC_DIR / "disks.html"
    if not page.exists():
        return HTMLResponse("<h1>Página não encontrada</h1>", status_code=404)
    return HTMLResponse(page.read_text(encoding="utf-8"))


@app.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(status="ok", version="3.1.1", time=datetime.now(timezone.utc).isoformat())


@app.get("/storage/info", response_model=StorageInfoResponse, dependencies=[Depends(require_api_key)])
def storage_info(db: Session = Depends(get_db)):
    usages = [u for u in (_safe_disk_usage(v) for v in STORAGE_VOLUMES) if u]
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


@app.get("/storage/disks", response_model=list[DiskVolumeInfo], dependencies=[Depends(require_api_key)])
def storage_disks(db: Session = Depends(get_db)):
    result = []
    for v in STORAGE_VOLUMES:
        usage = _safe_disk_usage(v)
        status = "degraded" if usage is None else "ok"
        content_files = (db.query(func.count(FileContentCopy.id))
                           .filter(FileContentCopy.volume_path == str(v))
                           .scalar() or 0)
        content_bytes = (db.query(func.coalesce(func.sum(FileContent.size), 0))
                           .join(FileContentCopy, FileContentCopy.sha256 == FileContent.sha256)
                           .filter(FileContentCopy.volume_path == str(v))
                           .scalar() or 0)
        result.append(DiskVolumeInfo(
            path=str(v),
            total_bytes=usage.total if usage else 0,
            used_bytes=usage.used  if usage else 0,
            free_bytes=usage.free  if usage else 0,
            content_files=content_files,
            content_bytes=int(content_bytes),
            status=status,
        ))
    return result


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
    log.info(f"[delete] Label [{label}] excluído — limpeza de órfãos em background")
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
    v.finished_at = datetime.now(timezone.utc)
    db.commit()
    if req.status == "done":
        log.info(f"[versao] {label}/{version_key} finalizada → disparando auto-cleanup em background")
        background_tasks.add_task(_bg_auto_cleanup)
    return _version_stats(v, db)


@app.post("/backups/{label}/versions/{version_key}/absorb", response_model=AbsorbResponse, dependencies=[Depends(require_api_key)])
def absorb_version(label: str, version_key: str, req: AbsorbRequest, db: Session = Depends(get_db)):
    """
    Herda arquivos da versao fonte que nao existem na versao destino (por original_path).
    Usado no modo acumulativo: novos arquivos sao adicionados pelo upload normal;
    arquivos ausentes do cliente (deletados) sao preservados via absorb da versao anterior.
    """
    dest = _get_version_or_404(label, version_key, db)
    src  = _get_version_or_404(label, req.source_version_key, db)

    existing = db.query(VersionFile.original_path).filter(VersionFile.version_id == dest.id).subquery()
    source_files = (
        db.query(VersionFile)
        .filter(VersionFile.version_id == src.id,
                ~VersionFile.original_path.in_(select(existing)))
        .all()
    )

    total_src = db.query(func.count(VersionFile.id)).filter(VersionFile.version_id == src.id).scalar() or 0
    inherited = len(source_files)

    for vf in source_files:
        db.add(VersionFile(
            version_id=dest.id,
            original_path=vf.original_path,
            sha256=vf.sha256,
            mtime=vf.mtime,
        ))
    db.commit()

    skipped = total_src - inherited
    log.info(f"[absorb] {label}/{version_key} ← {req.source_version_key}: {inherited} herdado(s), {skipped} ja presente(s)")
    return AbsorbResponse(inherited=inherited, skipped=skipped)


@app.delete("/backups/{label}/versions/{version_key}", response_model=VersionDeletedResponse, dependencies=[Depends(require_api_key)])
def delete_version(label: str, version_key: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    v = _get_version_or_404(label, version_key, db)
    db.delete(v)
    db.commit()
    log.info(f"[delete] Versão {label}/{version_key} excluída — limpeza em background")
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
        first_copy = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256).first()
        if first_copy:
            _ensure_replicas(sha256, Path(first_copy.stored_at), db)
    else:
        # Escolhe volume com mais espaço livre; tmp e conteúdo vão pro mesmo disco
        volume = _pick_volume()
        sha256, size, tmp_path = await _stream_request_to_disk(request, volume)

        try:
            fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
            if fc:
                tmp_path.unlink(missing_ok=True)
                first_copy = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256).first()
                if first_copy:
                    _ensure_replicas(sha256, Path(first_copy.stored_at), db)
            else:
                dest = _content_path(sha256, volume)
                shutil.move(str(tmp_path), str(dest))
                if ENCRYPTION_ENABLED:
                    log.info(f"[upload] cifrando {original_path!r} ({size / 1024 / 1024:.2f} MB) — sha256={sha256[:8]}…")
                    tmp_enc = dest.parent / f"_enc_{os.urandom(4).hex()}"
                    try:
                        crypto.encrypt_stream(dest, tmp_enc, _encryption_key)
                        shutil.move(str(tmp_enc), str(dest))
                        log.info(f"[upload] {sha256[:8]}… cifrado com sucesso")
                    except Exception:
                        tmp_enc.unlink(missing_ok=True)
                        raise
                fc = FileContent(sha256=sha256, stored_at=str(dest), size=size,
                                 encrypted=ENCRYPTION_ENABLED)
                db.add(fc)
                db.add(FileContentCopy(sha256=sha256, stored_at=str(dest), volume_path=str(volume)))
                db.flush()
                _ensure_replicas(sha256, dest, db)
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
    row = (db.query(VersionFile.original_path, VersionFile.sha256)
           .filter(VersionFile.id == file_id)
           .first())
    if not row:
        raise HTTPException(404, "Arquivo nao encontrado")

    fc           = db.query(FileContent).filter(FileContent.sha256 == row.sha256).first()
    is_encrypted = fc.encrypted if fc else False
    filename     = Path(row.original_path).name

    copies = (db.query(FileContentCopy)
              .filter(FileContentCopy.sha256 == row.sha256)
              .filter(~FileContentCopy.volume_path.in_([str(v) for v in _degraded_volumes]))
              .all())

    for copy in copies:
        p = Path(copy.stored_at)
        try:
            if p.exists():
                if is_encrypted:
                    return StreamingResponse(
                        crypto.decrypt_chunks(p, _encryption_key),
                        media_type="application/octet-stream",
                        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
                    )
                return FileResponse(p, filename=filename)
        except OSError:
            continue

    # 503 apenas se há cópias em volumes degraded (recuperáveis); 410 se o dado sumiu mesmo
    degraded_str = [str(v) for v in _degraded_volumes]
    has_degraded = bool(degraded_str) and db.query(FileContentCopy).filter(
        FileContentCopy.sha256 == row.sha256,
        FileContentCopy.volume_path.in_(degraded_str),
    ).count()
    raise HTTPException(503 if has_degraded else 410,
                        "Arquivo em volume degraded" if has_degraded else "Conteudo fisico nao encontrado")


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


@app.post("/maintenance/rereplicate", response_model=RereplicateResponse, dependencies=[Depends(require_api_key)])
def force_rereplicate(db: Session = Depends(get_db)):
    """Re-replica conteúdos com menos cópias que REPLICATION_FACTOR. Útil após adicionar um disco novo."""
    target = _target_replicas()
    degraded_strs = [str(d) for d in _degraded_volumes]

    underfilled = (
        db.query(FileContent.sha256, func.count(FileContentCopy.id).label("cnt"))
        .outerjoin(FileContentCopy, FileContentCopy.sha256 == FileContent.sha256)
        .group_by(FileContent.sha256)
        .having(func.count(FileContentCopy.id) < target)
        .all()
    )

    replicated = 0
    skipped = 0
    total_under = len(underfilled)
    log.info(f"[maintenance/rereplicate] {total_under} arquivo(s) com replicação abaixo do fator {target}")
    for idx, (sha256, _) in enumerate(underfilled, 1):
        q = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256)
        if degraded_strs:
            q = q.filter(~FileContentCopy.volume_path.in_(degraded_strs))
        source = q.first()
        if not source:
            log.warning(f"[maintenance/rereplicate] [{idx}/{total_under}] {sha256[:8]}… sem fonte acessível — pulando")
            skipped += 1
            continue
        log.info(f"[maintenance/rereplicate] [{idx}/{total_under}] {sha256[:8]}… replicando a partir de {source.volume_path}")
        _ensure_replicas(sha256, Path(source.stored_at), db)
        replicated += 1

    db.commit()
    log.info(f"[maintenance/rereplicate] concluído — {replicated} replicado(s), {skipped} pulado(s) (sem fonte acessível)")
    return RereplicateResponse(replicated=replicated, skipped=skipped, target_copies=target)


@app.post("/maintenance/encrypt-existing", response_model=EncryptExistingResponse, dependencies=[Depends(require_api_key)])
def encrypt_existing_files(db: Session = Depends(get_db)):
    """Cifra todos os FileContents ainda não cifrados. Requer ENCRYPTION_ENABLED=true no servidor."""
    if not ENCRYPTION_ENABLED:
        raise HTTPException(400, "Criptografia não habilitada no servidor (ENCRYPTION_ENABLED=false)")

    pending       = db.query(FileContent).filter(FileContent.encrypted == False).all()  # noqa: E712
    degraded_strs = [str(v) for v in _degraded_volumes]
    files_encrypted = 0
    bytes_processed = 0
    skipped         = 0
    total           = len(pending)

    log.info(f"[encrypt-existing] {total} arquivo(s) pendente(s) de cifragem")

    for i, fc in enumerate(pending, 1):
        q = db.query(FileContentCopy).filter(FileContentCopy.sha256 == fc.sha256)
        if degraded_strs:
            q = q.filter(~FileContentCopy.volume_path.in_(degraded_strs))
        copies = q.all()

        if not copies:
            log.warning(f"[encrypt-existing] [{i}/{total}] {fc.sha256[:8]}… sem cópia acessível — pulando")
            skipped += 1
            continue

        size_mb = fc.size / 1024 / 1024
        log.info(f"[encrypt-existing] [{i}/{total}] {fc.sha256[:8]}… ({size_mb:.2f} MB) — {len(copies)} cópia(s)")

        success = True
        for copy in copies:
            p = Path(copy.stored_at)
            if not p.exists():
                log.warning(f"[encrypt-existing] [{i}/{total}] arquivo físico não encontrado em {copy.volume_path} — pulando cópia")
                continue
            log.info(f"[encrypt-existing] [{i}/{total}] cifrando cópia em {copy.volume_path}")
            tmp_enc = p.parent / f"_enc_{os.urandom(4).hex()}"
            try:
                crypto.encrypt_stream(p, tmp_enc, _encryption_key)
                shutil.move(str(tmp_enc), str(p))
                log.info(f"[encrypt-existing] [{i}/{total}] cópia em {copy.volume_path} cifrada com sucesso")
            except Exception as e:
                log.warning(f"[encrypt-existing] [{i}/{total}] erro em {p}: {e}")
                tmp_enc.unlink(missing_ok=True)
                success = False
                break

        if success:
            fc.encrypted = True
            db.commit()  # persiste por arquivo — retomada segura se interrompido
            bytes_processed += fc.size
            files_encrypted += 1
            log.info(f"[encrypt-existing] [{i}/{total}] {fc.sha256[:8]}… concluído")
        else:
            skipped += 1

    log.info(f"[encrypt-existing] concluído — {files_encrypted} cifrado(s), {skipped} pulado(s), {bytes_processed / 1024 / 1024:.2f} MB processados")
    return EncryptExistingResponse(
        files_encrypted=files_encrypted,
        bytes_processed=bytes_processed,
        skipped=skipped,
    )


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

    log.info(f"[cleanup] {label}: removendo {len(keys_removed)} versão(ões): {keys_removed}")

    # SQLite nao enforca FK cascades por padrao; deletar VersionFiles antes
    # para que _cleanup_orphan_contents encontre os FileContents orfaos.
    db.query(VersionFile).filter(VersionFile.version_id.in_(ids_to_delete)).delete(
        synchronize_session=False
    )
    db.query(BackupVersion).filter(BackupVersion.id.in_(ids_to_delete)).delete(
        synchronize_session=False
    )
    db.commit()
    orphans_removed, _ = _cleanup_orphan_contents(db)
    log.info(f"[cleanup] {label}: {orphans_removed} arquivo(s) de storage removidos")
    return CleanupResponse(
        kept=req.keep,
        versions_removed=keys_removed,
        storage_files_removed=orphans_removed,
    )