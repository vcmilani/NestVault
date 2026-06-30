"""
NestVault  v7.5.0
Otimizacoes de performance:
- Upload faz streaming para disco (nao carrega na RAM)
- Hash calculado durante o stream (single-pass)
- Queries agregadas (func.count, func.sum) em vez de carregar entidades
- Indices no banco + WAL mode
- Cleanup de orfaos em uma unica query
- Limpeza de arquivos ao deletar label/versao feita em background (nao bloqueia o cliente)

v7.5.0:
- STORAGE_FALLBACK_THRESHOLD_GB agora é respeitado como piso mínimo de espaço por disco
- Quando todos os volumes estão abaixo do limiar, aciona cleanup automático de versões antigas antes de continuar
- Apenas se cleanup não liberar espaço suficiente, usa o volume com mais espaço livre como último recurso (log CRITICAL)
- Alerta via Telegram quando todos os volumes ultrapassam o limiar (se TELEGRAM_BOT_TOKEN/CHAT_ID configurados)

v7.4.0:
- Backup automático do banco de dados (PostgreSQL ou SQLite) para os volumes de storage
- Exporta para _db_backups/ em cada volume saudável; rotação mantém últimos DB_BACKUP_RETENTION (padrão 7) backups
- Agendado automaticamente às 01:00 via APScheduler; acionável manualmente via POST /maintenance/db-backup
- Novo card "Backup do Banco de Dados" na tela de manutenção (baixo risco)
- Variáveis: DB_BACKUP_ENABLED, DB_BACKUP_HOUR, DB_BACKUP_MINUTE, DB_BACKUP_RETENTION

v7.3.1:
- Corrige race condition TOCTOU no orphan cleanup (DELETE condicional por sha256 com NOT EXISTS)
- Corrige FK violation e retry infinito quando SsdCachePendingMove existe para FileContent orfao
- Corrige fixture de testes que nao propagava storage volume correto para storage.py

v7.3.0:
- Remove sistema de backup cloud OAuth (Google Drive direto / OneDrive direto)
- Mantém apenas o rclone como backend de cloud backup
- Tabelas cloud_credentials e cloud_backup_jobs podem ser dropadas manualmente
  (ver tools/migrate_drop_oauth_tables.sql)

v7.2.0:
- Progresso em tempo real para todas as operações de manutenção
- ssd-cache-move: exibe "Movendo: X / Y arquivo(s) (Z%)" em tempo real
- cleanup-by-date: progresso por lote "Removendo: X / Y versões (Z%)"
- nightly-cleanup: status running no início com progresso label-a-label
- encrypt-existing: convertido para background com progresso "Cifrando: X / Y (Z%)"
- cleanup-orphans, rereplicate, reconcile-replication, cleanup-versions: passam a registrar no histórico de atividade
- activity.html: labels adicionados para todos os tipos de job de manutenção
"""

from fastapi import FastAPI, Request, HTTPException, Depends, Header, BackgroundTasks, Query
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Optional, Literal
import asyncio, os, tempfile, hashlib, base64, shutil, logging, time, threading, re
from pathlib import Path
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

from sqlalchemy import func, select, insert, literal, case, delete, exists
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from database import init_db, get_db, SessionLocal, BackupID, BackupVersion, FileContent, FileContentCopy, VersionFile, MaintenanceJob, SsdCachePendingMove, RcloneBackupJob
import crypto
import storage
from auth import require_api_key, API_KEY, AUTH_ENABLED
from cloud.rclone_router import router as rclone_router
import scheduler as sched
from cache_state import _activity_wake, invalidate_activity

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backup-server")

# -- Config (aliases de storage para retrocompatibilidade) --------------------
STORAGE_VOLUMES      = storage.STORAGE_VOLUMES
STORAGE_DIR          = storage.STORAGE_DIR
ENCRYPTION_ENABLED   = storage.ENCRYPTION_ENABLED
CHUNK_SIZE           = storage.CHUNK_SIZE
REPLICATION_FACTOR   = storage.REPLICATION_FACTOR
STORAGE_FALLBACK_THRESHOLD_GB = storage.STORAGE_FALLBACK_THRESHOLD_GB

STATIC_DIR   = Path(__file__).parent / "static"

# Atalhos locais para os helpers de storage
_degraded_volumes        = storage._degraded_volumes
_safe_disk_usage         = storage.safe_disk_usage
_healthy_volumes         = storage.healthy_volumes
_target_replicas         = storage.target_replicas
_ensure_replicas         = storage.ensure_replicas
_rereplicate_to_volume   = storage.rereplicate_to_volume
_rereplicate_all         = storage.rereplicate_all
_cleanup_excess_copies   = storage.cleanup_excess_copies
_backfill_content_copies = storage.backfill_content_copies
_volume_health_monitor   = storage.volume_health_monitor
_volumes_with_free_space = storage.volumes_with_free_space
_ssd_cache_write_dir     = storage.ssd_cache_write_dir
_ssd_content_path        = storage.ssd_content_path


_ssd_move_lock = threading.Lock()

# Sinaliza o desligamento ao _activity_refresh_loop. Necessário porque o loop
# espera em _activity_wake.wait() dentro de uma thread do executor: cancelar a
# task asyncio não interrompe essa thread, então o fechamento do event loop
# bloquearia no join do executor por até _HISTORICAL_FALLBACK_TTL.
_activity_loop_stop = threading.Event()

_reclaimable_cache: dict = {"value": 0, "ts": 0.0}
_RECLAIMABLE_TTL = 60.0

# Cache para dados históricos (recent_versions com diffs, recent_jobs, maintenance_jobs).
# Só atualizado quando invalidate_activity() é chamado (fim de backup/job/manutenção).
# Fallback de 5 min para não ficar preso caso alguma invalidação seja perdida.
_historical_cache: dict = {"data": None, "ts": 0.0}
_HISTORICAL_FALLBACK_TTL = 300.0

_stats_cache: dict = {"data": None, "ts": 0.0}
_STATS_TTL = 300.0


def _get_reclaimable_bytes(db: Session) -> int:
    now = time.monotonic()
    if now - _reclaimable_cache["ts"] < _RECLAIMABLE_TTL:
        return _reclaimable_cache["value"]
    latest_ts_sq = (
        db.query(BackupVersion.backup_label, func.max(BackupVersion.created_at).label("latest_ts"))
        .filter(BackupVersion.status == "done")
        .group_by(BackupVersion.backup_label)
        .subquery()
    )
    keeper_ids = [
        row.id for row in (
            db.query(BackupVersion.id)
            .join(latest_ts_sq,
                  (BackupVersion.backup_label == latest_ts_sq.c.backup_label) &
                  (BackupVersion.created_at   == latest_ts_sq.c.latest_ts))
            .all()
        )
    ]
    if keeper_ids:
        kept_sq = (
            db.query(VersionFile.sha256.label("sha256"))
            .filter(VersionFile.version_id.in_(keeper_ids))
            .distinct()
            .subquery()
        )
        result = (
            db.query(func.coalesce(func.sum(FileContent.size), 0))
            .outerjoin(kept_sq, FileContent.sha256 == kept_sq.c.sha256)
            .filter(kept_sq.c.sha256.is_(None))
            .scalar()
        ) or 0
    else:
        result = db.query(func.coalesce(func.sum(FileContent.size), 0)).scalar() or 0
    _reclaimable_cache.update({"value": int(result), "ts": now})
    return int(result)


def _pick_volume() -> Path:
    try:
        return storage.pick_volume()
    except RuntimeError as e:
        raise HTTPException(503, str(e))


def _cleanup_stale_running_states():
    """Reseta estados 'running' órfãos deixados por um reinício do servidor."""
    db = SessionLocal()
    try:
        stale_rclone_jobs = (
            db.query(RcloneBackupJob)
            .filter(RcloneBackupJob.last_run_status == "running")
            .all()
        )
        for job in stale_rclone_jobs:
            job.last_run_status  = "error"
            job.last_run_message = "Interrompido pelo reinício do servidor"
            log.warning(f"[startup] Job rclone {job.id} ({job.display_name}) estava running — marcado como error")

        stale_versions = (
            db.query(BackupVersion)
            .filter(BackupVersion.status == "running")
            .all()
        )
        for v in stale_versions:
            v.status      = "incomplete"
            v.finished_at = datetime.now()
            log.warning(f"[startup] Versão {v.backup_label}/{v.version_key} estava running — marcada como incomplete")

        stale_maint = (
            db.query(MaintenanceJob)
            .filter(MaintenanceJob.status == "running")
            .all()
        )
        for m in stale_maint:
            m.status      = "error"
            m.finished_at = datetime.now()
            m.summary     = (m.summary or "") + " [interrompido pelo reinício do servidor]"
            log.warning(f"[startup] MaintenanceJob {m.id} ({m.job_type}) estava running — marcado como error")

        if stale_rclone_jobs or stale_versions or stale_maint:
            db.commit()
    finally:
        db.close()

    import shutil as _shutil
    from nightly_cleanup import _TMP_PREFIXES, _TMP_DIR_PREFIXES
    for vol in storage.STORAGE_VOLUMES:
        for prefix in _TMP_PREFIXES:
            for f in vol.glob(f"{prefix}*"):
                if f.is_file():
                    try:
                        f.unlink()
                        log.info(f"[startup] arquivo temporário órfão removido: {f.name}")
                    except OSError as e:
                        log.warning(f"[startup] não foi possível remover {f}: {e}")
        for prefix in _TMP_DIR_PREFIXES:
            for d in vol.glob(f"{prefix}*"):
                try:
                    if d.is_dir():
                        _shutil.rmtree(d, ignore_errors=True)
                    else:
                        d.unlink()
                    log.info(f"[startup] staging temporário órfão removido: {d.name}")
                except OSError as e:
                    log.warning(f"[startup] não foi possível remover {d}: {e}")


def _backup_labels_for_sha256s(db, sha256s: list[str]) -> list[str]:
    if not sha256s:
        return []
    rows = (db.query(BackupVersion.backup_label)
              .join(VersionFile, VersionFile.version_id == BackupVersion.id)
              .filter(VersionFile.sha256.in_(sha256s))
              .distinct()
              .all())
    return sorted(r[0] for r in rows)


def _bg_process_ssd_pending_moves():
    if not _ssd_move_lock.acquire(blocking=False):
        log.debug("[ssd-cache] worker já em execução — ignorando chamada duplicada")
        return
    db = SessionLocal()
    job = MaintenanceJob(job_type="ssd-cache-move", status="running")
    db.add(job)
    db.commit()
    db.refresh(job)
    job_id = job.id
    try:
        from database import SsdCachePendingMove
        total_pending = db.query(SsdCachePendingMove).count()
        processed = 0
        all_moved: list[str] = []
        if total_pending:
            job.summary = f"Movendo: 0 / {total_pending} arquivo(s) (0%)"
            db.commit()
            invalidate_activity()
        while True:
            done, sha256s = storage.process_ssd_pending_moves(db)
            if done == 0:
                break
            processed += done
            all_moved.extend(sha256s)
            if total_pending:
                pct = round(processed / total_pending * 100)
                job = db.get(MaintenanceJob, job_id)
                job.summary = f"Movendo: {processed} / {total_pending} arquivo(s) ({pct}%)"
                db.commit()
                invalidate_activity()
        storage.reconcile_orphaned_ssd_copies(db)
        labels = _backup_labels_for_sha256s(db, all_moved)
        label_str = ", ".join(labels) if labels else "—"
        job = db.get(MaintenanceJob, job_id)
        job.status = "done"
        job.finished_at = datetime.now()
        job.summary = f"{processed} arquivo(s) movidos SSD → HDD — backups: {label_str}"
    except Exception as e:
        log.error(f"[ssd-cache] Erro no worker de move: {e}")
        job = db.get(MaintenanceJob, job_id)
        if job:
            job.status = "error"
            job.finished_at = datetime.now()
            job.summary = str(e)
    finally:
        db.commit()
        invalidate_activity()
        db.close()
        _ssd_move_lock.release()


def _resume_ssd_pending_moves():
    if not storage.SSD_CACHE_DIR:
        return
    if not _ssd_move_lock.acquire(blocking=False):
        log.debug("[ssd-cache] worker já em execução — recovery ignorada")
        return
    db = SessionLocal()
    try:
        recovered = storage.recover_stuck_ssd_files(db)
        total_pending = db.query(SsdCachePendingMove).count()
        if not total_pending:
            return
        log.info(f"[ssd-cache] {total_pending} move(s) pendentes encontrados — retomando")
        summary_prefix = f"Recovery: {total_pending} pendente(s)" + (f", {recovered} recuperado(s) do SSD" if recovered else "")
        job = MaintenanceJob(job_type="ssd-cache-move", status="running", summary=summary_prefix)
        db.add(job)
        db.commit()
        db.refresh(job)
        job_id = job.id
        processed = 0
        all_moved: list[str] = []
        while True:
            done, sha256s = storage.process_ssd_pending_moves(db)
            if done == 0:
                break
            processed += done
            all_moved.extend(sha256s)
            pct = round(processed / total_pending * 100)
            job = db.get(MaintenanceJob, job_id)
            if job:
                job.summary = f"Recovery: Movendo: {processed} / {total_pending} arquivo(s) ({pct}%)"
                db.commit()
                invalidate_activity()
        storage.reconcile_orphaned_ssd_copies(db)
        labels = _backup_labels_for_sha256s(db, all_moved)
        label_str = ", ".join(labels) if labels else "—"
        job = db.get(MaintenanceJob, job_id)
        if job:
            job.status = "done"
            job.finished_at = datetime.now()
            job.summary = f"Recovery: {processed} arquivo(s) movidos SSD → HDD — backups: {label_str}"
        log.info(f"[ssd-cache] recovery concluída — {processed} arquivo(s) movidos para HDD")
    except Exception as e:
        log.error(f"[ssd-cache] Erro na recovery de moves pendentes: {e}")
        if 'job_id' in locals():
            job = db.get(MaintenanceJob, job_id)
            if job:
                job.status = "error"
                job.finished_at = datetime.now()
                job.summary = str(e)
    finally:
        db.commit()
        invalidate_activity()
        db.close()
        _ssd_move_lock.release()


async def _ssd_space_monitor():
    while True:
        await asyncio.sleep(30)
        if not storage.SSD_CACHE_ENABLED or not storage.SSD_CACHE_DIR:
            continue
        db = SessionLocal()
        try:
            if db.query(SsdCachePendingMove).count() > 0 and storage.ssd_cache_write_dir(db) is None:
                asyncio.get_running_loop().run_in_executor(None, _bg_process_ssd_pending_moves)
        finally:
            db.close()


async def lifespan(_: FastAPI):
    init_db()
    _cleanup_stale_running_states()
    if ENCRYPTION_ENABLED:
        storage.encryption_key = crypto.load_key()  # lança ValueError se inválida — falha rápido
        log.info("Criptografia: habilitada (AES-256-GCM)")
    else:
        log.info("Criptografia: desabilitada")
    asyncio.get_running_loop().run_in_executor(None, _backfill_content_copies)
    asyncio.get_running_loop().run_in_executor(None, _resume_ssd_pending_moves)
    _activity_loop_stop.clear()
    _activity_wake.clear()
    monitor         = asyncio.create_task(_volume_health_monitor())
    ssd_monitor     = asyncio.create_task(_ssd_space_monitor())
    activity_refresh = asyncio.create_task(_activity_refresh_loop())
    sched.scheduler.start()
    sched.reload_rclone_jobs_from_db()
    sched.schedule_daily_digest()
    sched.schedule_nightly_cleanup()
    sched.schedule_db_backup()
    log.info(f"Servidor iniciado — {len(STORAGE_VOLUMES)} volume(s): {[str(v) for v in STORAGE_VOLUMES]}")
    if storage.SSD_CACHE_ENABLED and storage.SSD_CACHE_DIR:
        log.info(f"SSD cache: habilitado — {storage.SSD_CACHE_DIR} (max {storage.SSD_CACHE_MAX_GB} GB)")
    log.info(f"Auth: {'habilitada' if AUTH_ENABLED else 'desabilitada'}")
    yield
    # Libera a thread do executor presa em _activity_wake.wait() antes de cancelar,
    # senão o fechamento do event loop bloqueia no join do executor (até 5 min).
    _activity_loop_stop.set()
    _activity_wake.set()
    monitor.cancel()
    ssd_monitor.cancel()
    activity_refresh.cancel()
    sched.scheduler.shutdown(wait=False)


app = FastAPI(title="NestVault", version="7.5.0", lifespan=lifespan)
app.include_router(rclone_router, prefix="/rclone", tags=["rclone"])

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# -- Schemas: Requests --------------------------------------------------------
class BackupCreate(BaseModel):
    label: str
    client_name: Optional[str] = None
    prefix: Optional[str] = None

class BackupRename(BaseModel):
    new_label: str

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
    has_running: bool = False

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
    status: Literal["running", "incomplete", "done", "failed"]
    created_at: str
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = Field(None, description="Duracao do backup em segundos")
    file_count: int
    total_size_bytes: int
    absorbed_count: int = 0

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
    mtime: Optional[float] = None
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

class ReconcileResponse(BaseModel):
    replicated: int
    skipped: int
    cleaned: int
    target_copies: int

class EncryptExistingResponse(BaseModel):
    files_encrypted: int
    bytes_processed: int
    skipped: int

class ValidateIntegrityResponse(BaseModel):
    checked: int
    invalidated: int
    files_removed: int
    labels: list[str]

class MigrateDiskRequest(BaseModel):
    source: str
    destinations: list[str]

class MigrateDiskDestInfo(BaseModel):
    path: str
    free_bytes: int
    capacity_bytes: int

class MigrateDiskPreviewResponse(BaseModel):
    source: str
    files_to_copy: int
    bytes_to_copy: int
    files_already_on_dest: int
    destinations: list[MigrateDiskDestInfo]
    can_proceed: bool
    reason: Optional[str]

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
    is_cache: bool = False

class BackupDiskEntry(BaseModel):
    volume_path: str
    file_count: int
    total_bytes: int

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

class RunningVersionInfo(BaseModel):
    backup_label: str
    version_key: str
    created_at: str
    file_count: int
    total_size_bytes: int
    prev_file_count: Optional[int] = None
    prev_size_bytes: Optional[int] = None

class RecentVersionInfo(BaseModel):
    backup_label: str
    version_key: str
    status: str
    created_at: str
    finished_at: Optional[str]
    duration_seconds: Optional[float]
    file_count: int
    total_size_bytes: int
    absorbed_count: int = 0
    diff_added: Optional[int] = None
    diff_modified: Optional[int] = None
    diff_removed: Optional[int] = None

class MaintenanceJobInfo(BaseModel):
    id: int
    job_type: str
    status: str
    started_at: str
    finished_at: Optional[str]
    summary: Optional[str]

class ActivityResponse(BaseModel):
    running_versions: list[RunningVersionInfo]
    storage: StorageInfoResponse
    disks: list[DiskVolumeInfo]
    recent_versions: list[RecentVersionInfo]
    maintenance_jobs: list[MaintenanceJobInfo]
    server_time: str


class TrendDay(BaseModel):
    date: str
    version_count: int
    total_size_bytes: int

class TopBackupEntry(BaseModel):
    label: str
    client_name: Optional[str]
    version_count: int
    file_count: int
    total_size_bytes: int
    last_version_at: Optional[str]

class MaintenanceTypeStat(BaseModel):
    job_type: str
    done_count: int
    error_count: int
    last_run_at: Optional[str]

class BackupActivityEntry(BaseModel):
    label: str
    client_name: Optional[str]
    last_done_at: Optional[str]
    done_count: int

class ReclaimableLabelEntry(BaseModel):
    label: str
    client_name: Optional[str]
    old_version_count: int
    old_versions_size_bytes: int

class StatsResponse(BaseModel):
    total_backups: int
    total_versions_done: int
    total_versions_failed: int
    storage_used_bytes: int
    storage_total_bytes: int
    storage_used_pct: float
    storage_free_bytes: int
    storage_reclaimable_bytes: int
    dedup_ratio: float
    version_success_rate: float
    avg_duration_seconds: Optional[float]
    stored_bytes: int
    logical_bytes: int
    unique_files: int
    total_file_refs: int
    space_saved_bytes: int
    trend_days: list[TrendDay]
    top_backups: list[TopBackupEntry]
    maintenance_by_type: list[MaintenanceTypeStat]
    last_maintenance_jobs: list[MaintenanceJobInfo]
    rclone_total: int
    rclone_enabled: int
    labels_created_30d: int
    versions_cleaned_total: int
    bytes_freed_by_cleanup: int
    backups_activity: list[BackupActivityEntry]
    reclaimable_by_label: list[ReclaimableLabelEntry]
    server_time: str


# -- Helpers ------------------------------------------------------------------
def _content_path(sha256: str, volume: Path) -> Path:
    dest = volume / "_content" / sha256[:2] / sha256
    dest.parent.mkdir(parents=True, exist_ok=True)
    return dest


def _verify_stored_file(sha256: str, dest: Path, encrypted: bool) -> None:
    h = hashlib.sha256()
    try:
        if encrypted:
            for chunk in crypto.decrypt_chunks(dest, storage.encryption_key):
                h.update(chunk)
        else:
            with open(dest, "rb") as f:
                while True:
                    chunk = f.read(1 << 20)
                    if not chunk:
                        break
                    h.update(chunk)
    except Exception as exc:
        dest.unlink(missing_ok=True)
        raise HTTPException(500, f"Falha ao verificar arquivo no disco: {exc}") from exc

    if h.hexdigest() != sha256:
        dest.unlink(missing_ok=True)
        raise HTTPException(500, "Corrupção detectada: sha256 do disco não confere com o esperado")


def _expected_stored_size(plain_size: int, encrypted: bool) -> int:
    """Tamanho esperado do arquivo no disco a partir do tamanho do plaintext.
    Cifrado: base_nonce + 20 bytes de overhead (4 de comprimento + 16 de tag GCM)
    por chunk de 1 MB — formato definido em crypto.py."""
    if not encrypted:
        return plain_size
    n_chunks = (plain_size + crypto.CHUNK_SIZE - 1) // crypto.CHUNK_SIZE
    return crypto.NONCE_SIZE + plain_size + n_chunks * 20


def _purge_corrupted_content(sha256: str, db: Session) -> None:
    copies = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256).all()
    for copy in copies:
        Path(copy.stored_at).unlink(missing_ok=True)
        db.delete(copy)
    fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
    if fc:
        db.delete(fc)
    db.flush()
    log.warning(f"[integrity] {sha256[:8]}… corrompido — {len(copies)} cópia(s) purgadas do disco e banco")


async def _stream_request_to_disk(request: Request, volume: Path) -> tuple[str, int, Path]:
    """
    Faz streaming do body raw da request para disco calculando sha256 em paralelo.
    Sem multipart — o body e o arquivo diretamente (binario puro).
    Tmp escrito no mesmo volume de destino para evitar cross-device move.
    Retorna (sha256, size, tmp_path).
    """
    h    = hashlib.sha256()
    size = 0
    fd, _tmp = tempfile.mkstemp(dir=volume, prefix="_tmp_")
    os.close(fd)
    tmp_path = Path(_tmp)
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
        absorbed_count=v.absorbed_count or 0,
    )


def _backup_info(b: BackupID, db: Session) -> BackupInfo:
    """Stats agregados — sem carregar todas as versoes."""
    version_count = (
        db.query(func.count(BackupVersion.id))
        .filter(BackupVersion.backup_label == b.label, BackupVersion.status == "done")
        .scalar() or 0
    )
    latest = (
        db.query(BackupVersion.id, BackupVersion.version_key)
        .filter(BackupVersion.backup_label == b.label, BackupVersion.status == "done")
        .order_by(BackupVersion.created_at.desc())
        .first()
    )
    latest_key = latest.version_key if latest else None
    latest_id  = latest.id if latest else None

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

    has_running = db.query(BackupVersion.id).filter(
        BackupVersion.backup_label == b.label,
        BackupVersion.status == "running"
    ).first() is not None

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
        has_running=has_running,
    )


def _auto_cleanup_if_needed(db: Session) -> str | None:
    """Retorna resumo do que foi limpo, ou None se nenhuma limpeza foi necessária."""
    factor = _target_replicas()
    ok = _volumes_with_free_space()
    if ok >= factor:
        return None

    log.warning(
        f"[auto-cleanup] Apenas {ok}/{len(_healthy_volumes())} volume(s) com ≥{STORAGE_FALLBACK_THRESHOLD_GB:.0f} GB livre "
        f"— fator de replicação={factor} não pode ser mantido, iniciando limpeza..."
    )

    total_removed = 0

    # 1ª prioridade: versões incomplete e failed — sempre deletáveis
    stale = (
        db.query(BackupVersion)
        .filter(BackupVersion.status.in_(["incomplete", "failed"]))
        .all()
    )
    if stale:
        stale_ids = [v.id for v in stale]
        db.query(VersionFile).filter(VersionFile.version_id.in_(stale_ids)).delete(synchronize_session=False)
        for v in stale:
            db.delete(v)
        db.commit()
        _cleanup_orphan_contents(db)
        ok = _volumes_with_free_space()
        total_removed += len(stale)
        log.info(f"[auto-cleanup] {len(stale)} versão(ões) incomplete/failed removida(s) — volumes com espaço: {ok}/{len(_healthy_volumes())}")
        if ok >= factor:
            log.info(f"[auto-cleanup] Replicação pode ser mantida ({ok} volume(s) ok), encerrando.")
            return f"{total_removed} versão(ões) removida(s) (incompletas/falhas)"

    # 2ª prioridade: versões done antigas (mantém sempre a mais recente por label)
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
            .order_by(BackupVersion.created_at.asc())
            .all()
        )
        deletable.extend(versions[:-1])  # mantém sempre a última

    deletable.sort(key=lambda v: v.created_at)  # mais antigas primeiro

    for v in deletable:
        label, key = v.backup_label, v.version_key
        db.query(VersionFile).filter(VersionFile.version_id == v.id).delete(synchronize_session=False)
        db.delete(v)
        removed, _ = _cleanup_orphan_contents_no_commit(db)
        db.commit()
        ok = _volumes_with_free_space()
        total_removed += 1
        log.info(f"[auto-cleanup] Removida {label}/{key} — {removed} arquivo(s) — volumes com espaço: {ok}/{len(_healthy_volumes())}")
        if ok >= factor:
            log.info(f"[auto-cleanup] Replicação pode ser mantida ({ok} volume(s) ok), encerrando.")
            return f"{total_removed} versão(ões) removida(s)"

    log.info(f"[auto-cleanup] Concluído — todas as labels com 1 versão. Volumes com espaço: {ok}/{len(_healthy_volumes())}.")
    return f"{total_removed} versão(ões) removida(s)"


def _cleanup_orphan_contents(db: Session) -> tuple[int, int]:
    """
    Remove FileContents nao referenciados por nenhum VersionFile.
    Usa subquery em vez de N+1 queries.
    Retorna (arquivos_removidos, bytes_liberados).
    """
    removed, bytes_freed = _cleanup_orphan_contents_no_commit(db)
    db.commit()
    return removed, bytes_freed


def _cleanup_orphan_contents_no_commit(db: Session, limit: int | None = None) -> tuple[int, int]:
    """Remove FileContents órfãos usando DELETE condicional por sha256.

    Cada sha256 é deletado em sua própria mini-transação que re-verifica no banco se o
    conteúdo ainda está sem referência. Isso elimina a race condition TOCTOU onde uploads
    concorrentes commitam VersionFiles para sha256s identificados como órfãos no snapshot
    inicial mas antes da deleção ser efetivada.

    O 'no_commit' no nome indica que não há um commit final agregado — cada sha256 deletado
    é commitado individualmente. Callers que fazem db.commit() após esta função executam um
    no-op se algum sha256 foi processado.
    """
    used_shas = db.query(VersionFile.sha256).distinct().subquery()
    q = db.query(FileContent).filter(~FileContent.sha256.in_(select(used_shas)))
    if limit is not None:
        q = q.limit(limit)
    candidates = q.all()

    if not candidates:
        return 0, 0

    # Extrai dados antes do commit (objetos expiram após db.commit())
    orphan_shas   = [fc.sha256 for fc in candidates]
    size_by_sha   = {fc.sha256: fc.size      for fc in candidates}
    stored_by_sha = {fc.sha256: fc.stored_at for fc in candidates}
    copies_by_sha: dict[str, list[dict]] = {}
    for c in db.query(FileContentCopy).filter(FileContentCopy.sha256.in_(orphan_shas)).all():
        copies_by_sha.setdefault(c.sha256, []).append({"id": c.id, "stored_at": c.stored_at})
    # SsdCachePendingMove é FK child de FileContent — deve ser deletado antes do parent.
    ssd_moves_by_sha: dict[str, str] = {}
    for m in db.query(SsdCachePendingMove).filter(SsdCachePendingMove.sha256.in_(orphan_shas)).all():
        ssd_moves_by_sha[m.sha256] = m.ssd_path

    # Encerra a transação de leitura (inclusive quaisquer pendências do caller, ex: deleções de
    # VersionFile em _auto_cleanup_if_needed) para que as mini-transações por sha256 enxerguem
    # o estado mais recente do DB.
    db.commit()

    bytes_freed = 0
    removed = 0

    for sha256 in orphan_shas:
        copies   = copies_by_sha.get(sha256, [])
        copy_ids = [c["id"] for c in copies]
        try:
            # FK children de FileContent: deletar antes do parent (respeita PRAGMA foreign_keys=ON).
            if sha256 in ssd_moves_by_sha:
                db.query(SsdCachePendingMove).filter(
                    SsdCachePendingMove.sha256 == sha256
                ).delete(synchronize_session=False)
            if copy_ids:
                db.query(FileContentCopy).filter(
                    FileContentCopy.id.in_(copy_ids)
                ).delete(synchronize_session=False)

            # DELETE atômico: só remove se ainda não há VersionFile apontando para este sha256.
            result = db.execute(
                delete(FileContent).where(
                    (FileContent.sha256 == sha256) &
                    ~exists().where(VersionFile.sha256 == sha256)
                )
            )
            if result.rowcount == 0:
                # Re-referenciado por upload concurrent — desfaz deleção das cópias e segue.
                db.rollback()
                continue
            db.commit()
        except Exception:
            db.rollback()
            raise

        # Deleção física (best-effort): falha deixa arquivo órfão no disco, mas o DB é consistente.
        if sha256 in ssd_moves_by_sha:
            try:
                Path(ssd_moves_by_sha[sha256]).unlink()
            except (FileNotFoundError, OSError):
                pass
        for c in copies:
            try:
                Path(c["stored_at"]).unlink()
            except FileNotFoundError:
                pass
            except OSError as e:
                log.warning(f"[cleanup-orphans] Não foi possível remover {c['stored_at']}: {e}")
        if not copies:
            try:
                Path(stored_by_sha[sha256]).unlink()
            except FileNotFoundError:
                pass
            except OSError as e:
                log.warning(f"[cleanup-orphans] Não foi possível remover {stored_by_sha[sha256]}: {e}")

        bytes_freed += size_by_sha.get(sha256, 0)
        removed += 1

    if removed:
        log.debug(f"[cleanup-orphans] {removed} arquivo(s) — {bytes_freed / 1024:.1f} KB liberados")
    return removed, bytes_freed


_BG_CLEANUP_BATCH = 500
_CLEANUP_BY_DATE_BATCH = 50  # versões por lote para evitar lock prolongado

def _bg_cleanup_orphan_contents() -> None:
    """Background task: cria sua propria sessao DB e limpa conteudos orfaos em lotes."""
    db = SessionLocal()
    try:
        log.info("[bg-cleanup] iniciando limpeza de conteúdos órfãos")
        total = 0
        bytes_total = 0
        while True:
            removed, freed = _cleanup_orphan_contents_no_commit(db, limit=_BG_CLEANUP_BATCH)
            if not removed:
                break
            db.commit()
            total += removed
            bytes_total += freed
            log.debug(f"[bg-cleanup] lote: {removed} arquivo(s) removido(s) (total={total})")
        if total:
            log.info(f"[bg-cleanup] {total} arquivo(s) orfao(s) removido(s) do storage")
            mj = MaintenanceJob(
                job_type="cleanup-orphans",
                status="done",
                finished_at=datetime.now(),
                summary=f"{total} arquivo(s) órfão(s) removido(s), {round(bytes_total/1024/1024, 1)} MB liberados",
            )
            db.add(mj)
            db.commit()
            invalidate_activity()
        else:
            log.info("[bg-cleanup] nenhuma limpeza necessária, não havia arquivos órfãos")
    finally:
        db.close()


def _bg_auto_cleanup() -> None:
    """Background task: cria sua propria sessao DB e executa auto-cleanup se necessario."""
    db = SessionLocal()
    try:
        log.info("[bg-auto-cleanup] verificando necessidade de limpeza automática")
        summary = _auto_cleanup_if_needed(db)
        if summary:
            mj = MaintenanceJob(
                job_type="auto-cleanup",
                status="done",
                finished_at=datetime.now(),
                summary=summary,
            )
            db.add(mj)
            db.commit()
            invalidate_activity()
    finally:
        db.close()


def _bg_cleanup_by_date(version_ids: list[int], scope: str) -> None:
    """Background task: exclui versões por data em lotes para não travar o SQLite."""
    db = SessionLocal()
    try:
        total = len(version_ids)
        total_files = 0
        log.info(f"[bg-cleanup-by-date] iniciando: {total} versão(ões), escopo={scope}")

        mj = MaintenanceJob(
            job_type="cleanup-by-date",
            status="running",
            summary=f"Escopo: {scope} — {total} versão(ões)",
        )
        db.add(mj)
        db.commit()
        mj_id = mj.id

        try:
            for i in range(0, total, _CLEANUP_BY_DATE_BATCH):
                batch = version_ids[i:i + _CLEANUP_BY_DATE_BATCH]
                deleted_files = db.query(VersionFile).filter(VersionFile.version_id.in_(batch)).delete(
                    synchronize_session=False
                )
                db.query(BackupVersion).filter(BackupVersion.id.in_(batch)).delete(
                    synchronize_session=False
                )
                db.commit()
                total_files += deleted_files
                done_so_far = min(i + _CLEANUP_BY_DATE_BATCH, total)
                pct = round(done_so_far / total * 100) if total else 100
                mj = db.get(MaintenanceJob, mj_id)
                if mj:
                    mj.summary = f"Removendo: {done_so_far} / {total} versões ({pct}%)"
                    db.commit()
                    invalidate_activity()
                log.debug(
                    f"[bg-cleanup-by-date] lote {i // _CLEANUP_BY_DATE_BATCH + 1}: "
                    f"{len(batch)} versão(ões), {deleted_files} VersionFile(s)"
                )

            log.info(f"[bg-cleanup-by-date] {total} versão(ões) e {total_files} VersionFile(s) removido(s)")

            orphan_total = 0
            bytes_total = 0
            while True:
                removed, freed = _cleanup_orphan_contents_no_commit(db, limit=_BG_CLEANUP_BATCH)
                if not removed:
                    break
                db.commit()
                orphan_total += removed
                bytes_total += freed

            log.info(f"[bg-cleanup-by-date] concluído — {orphan_total} arquivo(s) de storage liberado(s)")

            mj = db.get(MaintenanceJob, mj_id)
            mj.status = "done"
            mj.finished_at = datetime.now()
            mj.summary = f"{total} versão(ões) removidas, {orphan_total} arquivo(s) liberados ({round(bytes_total/1024/1024, 1)} MB)"
            db.commit()
            invalidate_activity()
        except Exception:
            mj = db.get(MaintenanceJob, mj_id)
            if mj:
                mj.status = "failed"
                mj.finished_at = datetime.now()
                db.commit()
                invalidate_activity()
            raise
    finally:
        db.close()


_MIGRATE_BATCH = 50

def _verify_file_sha256(path: Path, expected: str) -> bool:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest() == expected


def _bg_migrate_disk(source: str, destinations: list[str], job_id: int) -> None:
    """Background task: copia conteúdo de um volume de origem para destinos e remove a origem."""
    db = SessionLocal()
    try:
        mj = db.get(MaintenanceJob, job_id)

        # Carrega todas as cópias que estão no volume de origem
        source_copies = (
            db.query(FileContentCopy)
            .filter(FileContentCopy.volume_path == source)
            .all()
        )
        total = len(source_copies)
        log.info(f"[migrate-disk] {source} → {destinations}: {total} cópia(s) a processar")

        dest_set = set(destinations)

        # Rastreia espaço livre estimado por destino em memória para distribuição
        dest_free: dict[str, int] = {}
        for d in destinations:
            try:
                du = shutil.disk_usage(d)
                dest_free[d] = du.free
            except OSError:
                dest_free[d] = 0

        copied = 0
        already_on_dest = 0
        skipped = 0
        failed_sha256s: set[str] = set()

        # -- Fase 1: copiar arquivos ausentes nos destinos -----------------------
        for i, src_copy in enumerate(source_copies):
            sha256 = src_copy.sha256

            # Verifica se já existe cópia em algum destino
            existing_dest = (
                db.query(FileContentCopy.volume_path)
                .filter(
                    FileContentCopy.sha256 == sha256,
                    FileContentCopy.volume_path.in_(list(dest_set)),
                )
                .first()
            )
            if existing_dest:
                already_on_dest += 1
            else:
                # Escolhe destino com mais espaço livre
                best_dest = max(dest_free, key=lambda d: dest_free[d])
                if dest_free[best_dest] <= 0:
                    log.warning(f"[migrate-disk] sem espaço em destinos — pulando {sha256[:8]}…")
                    skipped += 1
                    failed_sha256s.add(sha256)
                    continue

                src_path = Path(src_copy.stored_at)
                if not src_path.exists():
                    log.warning(f"[migrate-disk] arquivo físico ausente: {src_path} — pulando")
                    skipped += 1
                    failed_sha256s.add(sha256)
                    continue

                dest_path = _content_path(sha256, Path(best_dest))
                try:
                    shutil.copy2(str(src_path), str(dest_path))
                    if not _verify_file_sha256(dest_path, sha256):
                        log.warning(f"[migrate-disk] SHA-256 inválido após cópia: {sha256[:8]}…")
                        dest_path.unlink(missing_ok=True)
                        skipped += 1
                        failed_sha256s.add(sha256)
                        continue

                    fc_copy = FileContentCopy(
                        sha256=sha256,
                        stored_at=str(dest_path),
                        volume_path=best_dest,
                    )
                    db.add(fc_copy)

                    # Atualiza estimativa de espaço livre
                    fc = db.get(FileContent, sha256)
                    if fc:
                        dest_free[best_dest] = max(0, dest_free[best_dest] - fc.size)

                    copied += 1
                except (OSError, Exception) as e:
                    log.warning(f"[migrate-disk] falha ao copiar {sha256[:8]}…: {e}")
                    dest_path.unlink(missing_ok=True)
                    skipped += 1
                    failed_sha256s.add(sha256)
                    continue

            # Commit e atualiza progresso a cada lote
            if (i + 1) % _MIGRATE_BATCH == 0:
                db.commit()
                done = i + 1
                pct = round(done / total * 100) if total else 0
                mj = db.get(MaintenanceJob, job_id)
                if mj:
                    mj.summary = f"Copiando: {done} / {total} arquivos ({pct}%)"
                    db.commit()
                log.debug(f"[migrate-disk] lote: {done}/{total}")

        db.commit()

        # -- Fase 2: remover cópias do disco de origem -------------------------
        removed_physical = 0
        for src_copy in source_copies:
            if src_copy.sha256 in failed_sha256s:
                continue  # Não remove se a cópia de destino falhou
            src_path = Path(src_copy.stored_at)
            db.delete(src_copy)
            try:
                src_path.unlink(missing_ok=True)
                removed_physical += 1
            except OSError as e:
                log.warning(f"[migrate-disk] falha ao remover {src_path}: {e}")

        # Atualiza FileContent.stored_at que ainda apontam para o volume de origem
        stale_fcs = (
            db.query(FileContent)
            .filter(FileContent.stored_at.like(f"{source}%"))
            .all()
        )
        for fc in stale_fcs:
            # Busca uma cópia válida em outro volume
            alt = (
                db.query(FileContentCopy)
                .filter(
                    FileContentCopy.sha256 == fc.sha256,
                    FileContentCopy.volume_path != source,
                )
                .first()
            )
            if alt:
                fc.stored_at = alt.stored_at

        db.commit()

        summary = (
            f"{copied} copiado(s), {already_on_dest} já existiam no destino, "
            f"{skipped} pulado(s) — {removed_physical} arquivo(s) removidos de {source}"
        )
        log.info(f"[migrate-disk] concluído — {summary}")

        mj = db.get(MaintenanceJob, job_id)
        if mj:
            mj.status = "done"
            mj.finished_at = datetime.now()
            mj.summary = summary
            db.commit()
            invalidate_activity()

    except Exception:
        log.exception("[migrate-disk] erro inesperado")
        try:
            mj = db.get(MaintenanceJob, job_id)
            if mj:
                mj.status = "failed"
                mj.finished_at = datetime.now()
                db.commit()
                invalidate_activity()
        except Exception:
            pass
        raise
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


@app.get("/explorer", response_class=HTMLResponse, include_in_schema=False)
def explorer_page():
    page = STATIC_DIR / "explorer.html"
    if not page.exists():
        return HTMLResponse("<h1>Página não encontrada</h1>", status_code=404)
    return HTMLResponse(page.read_text(encoding="utf-8"))


@app.get("/maintenance", response_class=HTMLResponse, include_in_schema=False)
def maintenance_page():
    page = STATIC_DIR / "maintenance.html"
    if not page.exists():
        return HTMLResponse("<h1>Página não encontrada</h1>", status_code=404)
    return HTMLResponse(page.read_text(encoding="utf-8"))


@app.get("/activity", response_class=HTMLResponse, include_in_schema=False)
def activity_page():
    page = STATIC_DIR / "activity.html"
    if not page.exists():
        return HTMLResponse("<h1>Página não encontrada</h1>", status_code=404)
    return HTMLResponse(page.read_text(encoding="utf-8"))


@app.get("/rclone-jobs", response_class=HTMLResponse, include_in_schema=False)
def rclone_jobs_page():
    page = STATIC_DIR / "rclone.html"
    if not page.exists():
        return HTMLResponse("<h1>Página não encontrada</h1>", status_code=404)
    return HTMLResponse(page.read_text(encoding="utf-8"))


@app.get("/stats", response_class=HTMLResponse, include_in_schema=False)
def stats_page():
    page = STATIC_DIR / "stats.html"
    if not page.exists():
        return HTMLResponse("<h1>Página não encontrada</h1>", status_code=404)
    return HTMLResponse(page.read_text(encoding="utf-8"))


def _build_stats_data(db: Session) -> StatsResponse:
    """Agrega estatísticas globais do sistema em ~7 queries. Resultado cacheado por _STATS_TTL."""

    _is_sqlite = not bool(os.getenv("DATABASE_URL"))

    # --- Q1: contagens de backups e versões ---
    counts = db.query(
        func.count(BackupID.id.distinct()).label("total_backups"),
        func.sum(case((BackupVersion.status == "done",   1), else_=0)).label("total_done"),
        func.sum(case((BackupVersion.status == "failed", 1), else_=0)).label("total_failed"),
    ).outerjoin(BackupVersion, BackupVersion.backup_label == BackupID.label
    ).filter(BackupID.status == "active").first()

    total_backups  = int(counts.total_backups  or 0)
    total_done     = int(counts.total_done     or 0)
    total_failed   = int(counts.total_failed   or 0)
    total_attempted = total_done + total_failed
    success_rate   = round(total_done / total_attempted * 100, 1) if total_attempted else 100.0

    # --- Q2: duração média das versões concluídas ---
    if _is_sqlite:
        dur_expr = (func.julianday(BackupVersion.finished_at) - func.julianday(BackupVersion.created_at)) * 86400.0
    else:
        dur_expr = func.extract("epoch", BackupVersion.finished_at - BackupVersion.created_at)

    avg_sec = db.query(func.avg(dur_expr)).filter(
        BackupVersion.status == "done",
        BackupVersion.finished_at.isnot(None),
        BackupVersion.created_at.isnot(None),
    ).scalar()
    avg_duration = round(float(avg_sec), 1) if avg_sec is not None else None

    # --- Q3: storage via helpers existentes ---
    usages = [u for u in (_safe_disk_usage(v) for v in STORAGE_VOLUMES) if u]
    storage_total      = sum(u.total for u in usages)
    storage_used_disk  = sum(u.used  for u in usages)
    storage_free       = sum(u.free  for u in usages)
    storage_reclaimable = _get_reclaimable_bytes(db)
    storage_used_pct   = round(storage_used_disk / storage_total * 100, 1) if storage_total else 0.0

    # --- Q4: deduplicação ---
    dedup_row = db.query(
        func.count(FileContent.sha256).label("unique_files"),
        func.coalesce(func.sum(FileContent.size), 0).label("stored_bytes"),
    ).first()
    unique_files  = int(dedup_row.unique_files  or 0)
    stored_bytes  = int(dedup_row.stored_bytes  or 0)

    logical_row = db.query(
        func.count(VersionFile.id).label("total_refs"),
        func.coalesce(func.sum(FileContent.size), 0).label("logical_bytes"),
    ).join(FileContent, FileContent.sha256 == VersionFile.sha256).first()
    total_refs    = int(logical_row.total_refs    or 0)
    logical_bytes = int(logical_row.logical_bytes or 0)

    space_saved   = max(0, logical_bytes - stored_bytes)
    dedup_ratio   = round(logical_bytes / stored_bytes, 2) if stored_bytes > 0 else 1.0

    # --- Q5: tendência dos últimos 30 dias (duas queries para evitar fan-out) ---
    cutoff = datetime.now() - timedelta(days=30)
    if _is_sqlite:
        date_expr = func.strftime("%Y-%m-%d", BackupVersion.created_at)
    else:
        date_expr = func.to_char(BackupVersion.created_at, "YYYY-MM-DD")

    trend_counts = db.query(
        date_expr.label("day"),
        func.count(BackupVersion.id).label("n"),
    ).filter(BackupVersion.created_at >= cutoff, BackupVersion.status == "done"
    ).group_by(date_expr).order_by(date_expr).all()

    # tamanho por dia via subquery para não multiplicar linhas
    sz_sq = db.query(
        BackupVersion.id.label("vid"),
        BackupVersion.created_at.label("ts"),
        func.coalesce(func.sum(FileContent.size), 0).label("sz"),
    ).join(VersionFile, VersionFile.version_id == BackupVersion.id
    ).join(FileContent, FileContent.sha256 == VersionFile.sha256
    ).filter(BackupVersion.created_at >= cutoff, BackupVersion.status == "done"
    ).group_by(BackupVersion.id, BackupVersion.created_at).subquery()

    if _is_sqlite:
        date_sq = func.strftime("%Y-%m-%d", sz_sq.c.ts)
    else:
        date_sq = func.to_char(sz_sq.c.ts, "YYYY-MM-DD")

    trend_sizes_rows = db.query(
        date_sq.label("day"),
        func.coalesce(func.sum(sz_sq.c.sz), 0).label("sz"),
    ).group_by(date_sq).all()
    size_by_day = {r.day: int(r.sz) for r in trend_sizes_rows}

    trend_days = [TrendDay(date=r.day, version_count=r.n, total_size_bytes=size_by_day.get(r.day, 0))
                  for r in trend_counts]

    # --- Q6: top 10 backups por tamanho da versão mais recente ---
    label_stats = db.query(
        BackupVersion.backup_label.label("label"),
        func.count(BackupVersion.id).label("version_count"),
        func.max(BackupVersion.created_at).label("last_at"),
        func.max(BackupVersion.id).label("latest_id"),
    ).filter(BackupVersion.status == "done"
    ).group_by(BackupVersion.backup_label).all()

    latest_ids = [r.latest_id for r in label_stats]
    if latest_ids:
        size_rows = db.query(
            VersionFile.version_id,
            func.count(VersionFile.id).label("fc"),
            func.coalesce(func.sum(FileContent.size), 0).label("sz"),
        ).join(FileContent, FileContent.sha256 == VersionFile.sha256
        ).filter(VersionFile.version_id.in_(latest_ids)
        ).group_by(VersionFile.version_id).all()
        size_map = {r.version_id: (int(r.fc), int(r.sz)) for r in size_rows}
    else:
        size_map = {}

    all_labels = [r.label for r in label_stats]
    if all_labels:
        names_rows = db.query(BackupID.label, BackupID.client_name).filter(BackupID.label.in_(all_labels)).all()
        names_map = {r.label: r.client_name for r in names_rows}
    else:
        names_map = {}

    top_raw = sorted(
        [(r.label, names_map.get(r.label), int(r.version_count), *size_map.get(r.latest_id, (0, 0)), str(r.last_at))
         for r in label_stats],
        key=lambda x: x[4], reverse=True,
    )[:10]
    top_backups = [TopBackupEntry(
        label=row[0], client_name=row[1], version_count=row[2],
        file_count=row[3], total_size_bytes=row[4], last_version_at=row[5],
    ) for row in top_raw]

    # --- Q7: manutenção por tipo ---
    maint_stats = db.query(
        MaintenanceJob.job_type,
        func.sum(case((MaintenanceJob.status == "done",  1), else_=0)).label("done_count"),
        func.sum(case((MaintenanceJob.status == "error", 1), else_=0)).label("error_count"),
        func.max(MaintenanceJob.started_at).label("last_run_at"),
    ).group_by(MaintenanceJob.job_type).all()

    maintenance_by_type = [MaintenanceTypeStat(
        job_type=r.job_type,
        done_count=int(r.done_count or 0),
        error_count=int(r.error_count or 0),
        last_run_at=str(r.last_run_at) if r.last_run_at else None,
    ) for r in maint_stats]

    last_maint = db.query(MaintenanceJob).order_by(MaintenanceJob.started_at.desc()).limit(5).all()
    last_maintenance_jobs = [MaintenanceJobInfo(
        id=j.id, job_type=j.job_type, status=j.status,
        started_at=str(j.started_at),
        finished_at=str(j.finished_at) if j.finished_at else None,
        summary=j.summary,
    ) for j in last_maint]

    # --- Q8: rclone ---
    rclone_row = db.query(
        func.count(RcloneBackupJob.id).label("total"),
        func.coalesce(func.sum(case((RcloneBackupJob.enabled == True, 1), else_=0)), 0).label("enabled"),
    ).first()
    rclone_total   = int(rclone_row.total   or 0) if rclone_row else 0
    rclone_enabled = int(rclone_row.enabled or 0) if rclone_row else 0

    # --- Q9: ciclo de vida dos labels ---
    labels_created_30d = db.query(func.count(BackupID.id)).filter(
        BackupID.created_at >= cutoff
    ).scalar() or 0

    _cleanup_types = ["cleanup-by-date", "auto-cleanup", "cleanup-versions", "nightly-cleanup"]
    cleanup_summaries = db.query(MaintenanceJob.summary).filter(
        MaintenanceJob.job_type.in_(_cleanup_types),
        MaintenanceJob.status == "done",
        MaintenanceJob.summary.isnot(None),
    ).all()

    _re_ver = re.compile(r'(\d+) versão\(ões\) removidas')
    _re_mb  = re.compile(r'\(([\d.]+) MB\)')
    versions_cleaned_total = 0
    bytes_freed_by_cleanup = 0
    for (summary,) in cleanup_summaries:
        if not summary:
            continue
        m = _re_ver.search(summary)
        if m:
            versions_cleaned_total += int(m.group(1))
        m2 = _re_mb.search(summary)
        if m2:
            bytes_freed_by_cleanup += int(float(m2.group(1)) * 1024 * 1024)

    # --- Q10: atividade por backup (tabelas stale + versões) ---
    _activity_rows = (
        db.query(
            BackupID.label,
            BackupID.client_name,
            func.max(
                case((BackupVersion.status == "done", BackupVersion.finished_at), else_=None)
            ).label("last_done_at"),
            func.count(
                case((BackupVersion.status == "done", BackupVersion.id), else_=None)
            ).label("done_count"),
        )
        .outerjoin(BackupVersion, BackupVersion.backup_label == BackupID.label)
        .filter(BackupID.status == "active")
        .group_by(BackupID.label, BackupID.client_name)
        .all()
    )
    _activity_sorted = sorted(
        _activity_rows,
        key=lambda r: (r.last_done_at is not None, r.last_done_at or datetime.min),
    )
    backups_activity = [
        BackupActivityEntry(
            label=r.label,
            client_name=r.client_name,
            last_done_at=r.last_done_at.isoformat() if r.last_done_at else None,
            done_count=r.done_count or 0,
        )
        for r in _activity_sorted
    ]

    # --- Q11: espaço liberável por label (versões done não-mais-recentes) ---
    # Exclui sha256s presentes em qualquer versão keeper e usa DISTINCT para não
    # inflar pelo mesmo arquivo aparecendo em múltiplas versões antigas.
    reclaimable_by_label: list[ReclaimableLabelEntry] = []
    if latest_ids:
        keeper_sq = (
            db.query(VersionFile.sha256.label("sha256"))
            .filter(VersionFile.version_id.in_(latest_ids))
            .distinct()
            .subquery()
        )
        old_exclusive_sq = (
            db.query(
                BackupVersion.backup_label.label("label"),
                VersionFile.sha256.label("sha256"),
            )
            .join(VersionFile, VersionFile.version_id == BackupVersion.id)
            .outerjoin(keeper_sq, VersionFile.sha256 == keeper_sq.c.sha256)
            .filter(
                BackupVersion.status == "done",
                BackupVersion.id.notin_(latest_ids),
                keeper_sq.c.sha256.is_(None),
            )
            .distinct()
            .subquery()
        )
        old_size_rows = (
            db.query(
                old_exclusive_sq.c.label.label("label"),
                func.coalesce(func.sum(FileContent.size), 0).label("old_versions_size_bytes"),
            )
            .join(FileContent, FileContent.sha256 == old_exclusive_sq.c.sha256)
            .group_by(old_exclusive_sq.c.label)
            .order_by(func.coalesce(func.sum(FileContent.size), 0).desc())
            .all()
        )
        old_cnt_rows = (
            db.query(
                BackupVersion.backup_label.label("label"),
                func.count(BackupVersion.id.distinct()).label("cnt"),
            )
            .filter(BackupVersion.status == "done", BackupVersion.id.notin_(latest_ids))
            .group_by(BackupVersion.backup_label)
            .all()
        )
        old_cnt_map = {r.label: int(r.cnt) for r in old_cnt_rows}

        reclaimable_by_label = [
            ReclaimableLabelEntry(
                label=row.label,
                client_name=names_map.get(row.label),
                old_version_count=old_cnt_map.get(row.label, 0),
                old_versions_size_bytes=int(row.old_versions_size_bytes),
            )
            for row in old_size_rows
            if row.old_versions_size_bytes > 0
        ]

    return StatsResponse(
        total_backups=total_backups,
        total_versions_done=total_done,
        total_versions_failed=total_failed,
        storage_used_bytes=storage_used_disk,
        storage_total_bytes=storage_total,
        storage_used_pct=storage_used_pct,
        storage_free_bytes=storage_free,
        storage_reclaimable_bytes=storage_reclaimable,
        dedup_ratio=dedup_ratio,
        version_success_rate=success_rate,
        avg_duration_seconds=avg_duration,
        stored_bytes=stored_bytes,
        logical_bytes=logical_bytes,
        unique_files=unique_files,
        total_file_refs=total_refs,
        space_saved_bytes=space_saved,
        trend_days=trend_days,
        top_backups=top_backups,
        maintenance_by_type=maintenance_by_type,
        last_maintenance_jobs=last_maintenance_jobs,
        rclone_total=rclone_total,
        rclone_enabled=rclone_enabled,
        labels_created_30d=int(labels_created_30d),
        versions_cleaned_total=versions_cleaned_total,
        bytes_freed_by_cleanup=bytes_freed_by_cleanup,
        backups_activity=backups_activity,
        reclaimable_by_label=reclaimable_by_label,
        server_time=datetime.now().isoformat(),
    )


@app.get("/api/stats", response_model=StatsResponse, dependencies=[Depends(require_api_key)])
def get_stats(db: Session = Depends(get_db)):
    data = _stats_cache["data"]
    if data is None or time.monotonic() - _stats_cache["ts"] > _STATS_TTL:
        data = _build_stats_data(db)
        _stats_cache.update({"data": data, "ts": time.monotonic()})
    return data


def _build_fast_data(db: Session) -> tuple:
    """Queries leves para dados em tempo real: versões em execução, storage e discos.
    Chamado inline a cada request — retorna (running_version_infos, storage_obj, disks_list)."""

    # 1. Versões de backup em execução
    running_vs = (
        db.query(BackupVersion)
        .filter(BackupVersion.status == "running")
        .order_by(BackupVersion.created_at.asc())
        .all()
    )
    stats_map: dict[int, tuple[int, int]] = {}
    if running_vs:
        vids = [v.id for v in running_vs]
        for row in (
            db.query(
                VersionFile.version_id,
                func.count(VersionFile.id).label("fc"),
                func.coalesce(func.sum(FileContent.size), 0).label("sz"),
            )
            .outerjoin(FileContent, FileContent.sha256 == VersionFile.sha256)
            .filter(VersionFile.version_id.in_(vids))
            .group_by(VersionFile.version_id)
            .all()
        ):
            stats_map[row.version_id] = (row.fc, int(row.sz))

    # prev_stats: stats da última versão "done" de cada label em execução.
    # Resolvido em queries agregadas (sem N+1 por label).
    prev_stats: dict[str, tuple[int, int]] = {}
    labels = {v.backup_label for v in running_vs}
    if labels:
        latest_done_sq = (
            db.query(
                BackupVersion.backup_label.label("label"),
                func.max(BackupVersion.created_at).label("ts"),
            )
            .filter(BackupVersion.status == "done", BackupVersion.backup_label.in_(labels))
            .group_by(BackupVersion.backup_label)
            .subquery()
        )
        id_to_label = {
            vid: lbl
            for vid, lbl in (
                db.query(BackupVersion.id, BackupVersion.backup_label)
                .join(latest_done_sq,
                      (BackupVersion.backup_label == latest_done_sq.c.label) &
                      (BackupVersion.created_at == latest_done_sq.c.ts))
                .all()
            )
        }
        if id_to_label:
            for row in (
                db.query(
                    VersionFile.version_id,
                    func.count(VersionFile.id).label("fc"),
                    func.coalesce(func.sum(FileContent.size), 0).label("sz"),
                )
                .outerjoin(FileContent, FileContent.sha256 == VersionFile.sha256)
                .filter(VersionFile.version_id.in_(list(id_to_label.keys())))
                .group_by(VersionFile.version_id)
                .all()
            ):
                prev_stats[id_to_label[row.version_id]] = (row.fc, int(row.sz))

    running_version_infos = [
        RunningVersionInfo(
            backup_label=v.backup_label,
            version_key=v.version_key,
            created_at=str(v.created_at),
            file_count=stats_map.get(v.id, (0, 0))[0],
            total_size_bytes=stats_map.get(v.id, (0, 0))[1],
            prev_file_count=prev_stats[v.backup_label][0] if v.backup_label in prev_stats else None,
            prev_size_bytes=prev_stats[v.backup_label][1] if v.backup_label in prev_stats else None,
        )
        for v in running_vs
    ]

    # 2. Storage — um statvfs por volume, reaproveitado no bloco de disks
    usage_map = {v: _safe_disk_usage(v) for v in STORAGE_VOLUMES}
    usages = [u for u in usage_map.values() if u]
    usage_total = sum(u.total for u in usages)
    usage_used  = sum(u.used  for u in usages)
    usage_free  = sum(u.free  for u in usages)
    storage_obj = StorageInfoResponse(
        total_bytes=usage_total, used_bytes=usage_used,
        free_bytes=usage_free, reclaimable_bytes=_get_reclaimable_bytes(db),
    )

    # 4. Disks — 1 query GROUP BY em vez de 2 queries por volume
    _vol_paths = [str(v) for v in STORAGE_VOLUMES]
    _disk_rows = (
        db.query(
            FileContentCopy.volume_path,
            func.count(FileContentCopy.id).label("cnt"),
            func.coalesce(func.sum(FileContent.size), 0).label("bytes"),
        )
        .join(FileContent, FileContent.sha256 == FileContentCopy.sha256)
        .filter(FileContentCopy.volume_path.in_(_vol_paths))
        .group_by(FileContentCopy.volume_path)
        .all()
    )
    _disk_stats = {r.volume_path: (r.cnt, int(r.bytes)) for r in _disk_rows}
    disks_list = []
    for vol in STORAGE_VOLUMES:
        usage = usage_map[vol]
        files, bytes_ = _disk_stats.get(str(vol), (0, 0))
        disks_list.append(DiskVolumeInfo(
            path=str(vol),
            total_bytes=usage.total if usage else 0,
            used_bytes=usage.used  if usage else 0,
            free_bytes=usage.free  if usage else 0,
            content_files=files, content_bytes=bytes_,
            status="degraded" if usage is None else "ok",
        ))

    return running_version_infos, storage_obj, disks_list


def _build_historical_data(db: Session) -> tuple:
    """Queries pesadas: versões recentes com diffs e manutenções concluídas.
    Só chamado quando invalidate_activity() acorda o loop histórico.
    Retorna (recent_version_infos, maintenance_job_infos)."""
    from datetime import timedelta
    from collections import defaultdict

    cutoff = datetime.now() - timedelta(hours=24)

    # 5. Versões recentes (últimas 24h, status finalizado)
    recent_vs = (
        db.query(BackupVersion)
        .filter(
            BackupVersion.status.in_(["done", "failed", "incomplete"]),
            BackupVersion.finished_at >= cutoff,
        )
        .order_by(BackupVersion.finished_at.desc())
        .limit(30)
        .all()
    )
    rstats: dict[int, tuple[int, int]] = {}
    if recent_vs:
        rvids = [v.id for v in recent_vs]
        for row in (
            db.query(
                VersionFile.version_id,
                func.count(VersionFile.id).label("fc"),
                func.coalesce(func.sum(FileContent.size), 0).label("sz"),
            )
            .outerjoin(FileContent, FileContent.sha256 == VersionFile.sha256)
            .filter(VersionFile.version_id.in_(rvids))
            .group_by(VersionFile.version_id)
            .all()
        ):
            rstats[row.version_id] = (row.fc, int(row.sz))

    done_vs = [v for v in recent_vs if v.status == "done"]
    prev_id_map: dict[int, Optional[int]] = {}
    if done_vs:
        done_labels = {v.backup_label for v in done_vs}
        all_done_rows = (
            db.query(BackupVersion.id, BackupVersion.backup_label, BackupVersion.version_key)
            .filter(BackupVersion.backup_label.in_(done_labels), BackupVersion.status == "done")
            .order_by(BackupVersion.backup_label, BackupVersion.version_key)
            .all()
        )
        by_label: dict[str, list] = defaultdict(list)
        for row in all_done_rows:
            by_label[row.backup_label].append(row)
        for v in done_vs:
            rows = by_label.get(v.backup_label, [])
            idx = next((i for i, r in enumerate(rows) if r.id == v.id), None)
            prev_id_map[v.id] = rows[idx - 1].id if idx is not None and idx > 0 else None

    all_diff_ids = set(prev_id_map.keys()) | {pid for pid in prev_id_map.values() if pid}
    files_by_vid: dict[int, dict[str, str]] = {}
    if all_diff_ids:
        for row in (
            db.query(VersionFile.version_id, VersionFile.original_path, VersionFile.sha256)
            .filter(VersionFile.version_id.in_(all_diff_ids))
            .all()
        ):
            files_by_vid.setdefault(row.version_id, {})[row.original_path] = row.sha256

    diff_map: dict[int, dict] = {}
    for v in done_vs:
        cur = files_by_vid.get(v.id, {})
        prev_id = prev_id_map.get(v.id)
        if prev_id is None:
            diff_map[v.id] = {"added": len(cur), "modified": 0, "removed": 0}
        else:
            prv = files_by_vid.get(prev_id, {})
            diff_map[v.id] = {
                "added":    sum(1 for p in cur if p not in prv),
                "modified": sum(1 for p, h in cur.items() if p in prv and prv[p] != h),
                "removed":  sum(1 for p in prv if p not in cur),
            }

    recent_version_infos = []
    for v in recent_vs:
        fc, sz = rstats.get(v.id, (0, 0))
        duration = None
        if v.finished_at and v.created_at:
            duration = round((v.finished_at - v.created_at).total_seconds(), 1)
        d = diff_map.get(v.id)
        recent_version_infos.append(RecentVersionInfo(
            backup_label=v.backup_label, version_key=v.version_key,
            status=v.status, created_at=str(v.created_at),
            finished_at=str(v.finished_at) if v.finished_at else None,
            duration_seconds=duration, file_count=fc, total_size_bytes=sz,
            absorbed_count=v.absorbed_count or 0,
            diff_added=d["added"] if d else None,
            diff_modified=d["modified"] if d else None,
            diff_removed=d["removed"] if d else None,
        ))

    # 6. Maintenance jobs (em execução + últimas 24h)
    maint_rows = (
        db.query(MaintenanceJob)
        .filter(
            (MaintenanceJob.status == "running") |
            (MaintenanceJob.started_at >= cutoff)
        )
        .order_by(MaintenanceJob.started_at.desc())
        .limit(20)
        .all()
    )
    maintenance_job_infos = [
        MaintenanceJobInfo(
            id=m.id,
            job_type=m.job_type,
            status=m.status,
            started_at=str(m.started_at),
            finished_at=str(m.finished_at) if m.finished_at else None,
            summary=m.summary,
        )
        for m in maint_rows
    ]

    return recent_version_infos, maintenance_job_infos


async def _activity_refresh_loop() -> None:
    """Recalcula o bloco histórico (recent_versions com diffs, maint_jobs)
    apenas quando invalidate_activity() acorda o loop — normalmente ao fim de um backup/job.
    Fallback de 5 min para não ficar preso caso alguma invalidação seja perdida."""
    loop = asyncio.get_running_loop()
    while not _activity_loop_stop.is_set():
        await loop.run_in_executor(None, lambda: _activity_wake.wait(timeout=_HISTORICAL_FALLBACK_TTL))
        _activity_wake.clear()
        if _activity_loop_stop.is_set():
            break

        try:
            def _do_hist():
                db = SessionLocal()
                try:
                    return _build_historical_data(db)
                finally:
                    db.close()
            result = await loop.run_in_executor(None, _do_hist)
            _historical_cache.update({"data": result, "ts": time.monotonic()})
            log.debug("[activity-hist] cache histórico atualizado")
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[activity-hist] Erro ao atualizar cache histórico")


@app.get("/api/activity", response_model=ActivityResponse, dependencies=[Depends(require_api_key)])
def get_activity(db: Session = Depends(get_db)):
    running_versions, storage_obj, disks = _build_fast_data(db)

    hist = _historical_cache["data"]
    if hist is None:
        # Cold start: bloco histórico ainda não foi calculado — faz inline uma vez
        recent_versions, maint_jobs = _build_historical_data(db)
        _historical_cache.update({"data": (recent_versions, maint_jobs),
                                   "ts": time.monotonic()})
    else:
        recent_versions, maint_jobs = hist

    return ActivityResponse(
        running_versions=running_versions,
        storage=storage_obj,
        disks=disks,
        recent_versions=recent_versions,
        maintenance_jobs=maint_jobs,
        server_time=datetime.now().isoformat(),
    )


@app.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(status="ok", version=app.version, time=datetime.now(timezone.utc).isoformat())


@app.get("/storage/info", response_model=StorageInfoResponse, dependencies=[Depends(require_api_key)])
def storage_info(db: Session = Depends(get_db)):
    usages = [u for u in (_safe_disk_usage(v) for v in STORAGE_VOLUMES) if u]
    usage_total = sum(u.total for u in usages)
    usage_used  = sum(u.used  for u in usages)
    usage_free  = sum(u.free  for u in usages)

    return StorageInfoResponse(
        total_bytes=usage_total,
        used_bytes=usage_used,
        free_bytes=usage_free,
        reclaimable_bytes=_get_reclaimable_bytes(db),
    )


@app.get("/storage/disks", response_model=list[DiskVolumeInfo], dependencies=[Depends(require_api_key)])
def storage_disks(db: Session = Depends(get_db)):
    rows = (
        db.query(
            FileContentCopy.volume_path,
            func.count(FileContentCopy.id).label("cnt"),
            func.coalesce(func.sum(FileContent.size), 0).label("bytes"),
        )
        .join(FileContent, FileContent.sha256 == FileContentCopy.sha256)
        .group_by(FileContentCopy.volume_path)
        .all()
    )
    vol_stats = {r.volume_path: (r.cnt, int(r.bytes)) for r in rows}

    result = []
    for v in STORAGE_VOLUMES:
        usage = _safe_disk_usage(v)
        files, bytes_ = vol_stats.get(str(v), (0, 0))
        result.append(DiskVolumeInfo(
            path=str(v),
            total_bytes=usage.total if usage else 0,
            used_bytes=usage.used  if usage else 0,
            free_bytes=usage.free  if usage else 0,
            content_files=files,
            content_bytes=bytes_,
            status="degraded" if usage is None else "ok",
        ))
    if storage.SSD_CACHE_ENABLED and storage.SSD_CACHE_DIR:
        usage = _safe_disk_usage(storage.SSD_CACHE_DIR)
        files, bytes_ = vol_stats.get(str(storage.SSD_CACHE_DIR), (0, 0))
        result.append(DiskVolumeInfo(
            path=str(storage.SSD_CACHE_DIR),
            total_bytes=usage.total if usage else 0,
            used_bytes=usage.used  if usage else 0,
            free_bytes=usage.free  if usage else 0,
            content_files=files,
            content_bytes=bytes_,
            status="degraded" if usage is None else "ok",
            is_cache=True,
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
    """Lista backups com stats — 4 queries fixas independente de N (sem N+1)."""
    q = db.query(BackupID).order_by(BackupID.created_at.desc())
    if client_name:
        q = q.filter(BackupID.client_name == client_name)
    backups = q.all()
    if not backups:
        return []

    # Versão done mais recente + contagem por label — 1 query
    latest_sq = (
        db.query(
            BackupVersion.backup_label,
            func.max(BackupVersion.created_at).label("latest_ts"),
            func.count(BackupVersion.id).label("version_count"),
        )
        .filter(BackupVersion.status == "done")
        .group_by(BackupVersion.backup_label)
        .subquery()
    )
    latest_rows = (
        db.query(BackupVersion.backup_label, BackupVersion.id, BackupVersion.version_key,
                 latest_sq.c.version_count)
        .join(latest_sq, (BackupVersion.backup_label == latest_sq.c.backup_label) &
                         (BackupVersion.created_at   == latest_sq.c.latest_ts))
        .all()
    )
    latest_by_label: dict[str, tuple[int, str, int]] = {
        row.backup_label: (row.id, row.version_key, row.version_count) for row in latest_rows
    }

    # File count + total size para as versões mais recentes — 1 query
    version_ids = [v[0] for v in latest_by_label.values()]
    stats_by_vid: dict[int, tuple[int, int]] = {}
    if version_ids:
        for row in (
            db.query(VersionFile.version_id,
                     func.count(VersionFile.id).label("fc"),
                     func.coalesce(func.sum(FileContent.size), 0).label("sz"))
            .outerjoin(FileContent, FileContent.sha256 == VersionFile.sha256)
            .filter(VersionFile.version_id.in_(version_ids))
            .group_by(VersionFile.version_id)
            .all()
        ):
            stats_by_vid[row.version_id] = (row.fc, int(row.sz))

    # Labels com versão running — 1 query para todos os backups
    labels = [b.label for b in backups]
    running_labels: set[str] = {
        row.backup_label
        for row in db.query(BackupVersion.backup_label)
        .filter(BackupVersion.backup_label.in_(labels),
                BackupVersion.status == "running")
        .distinct()
        .all()
    }

    result = []
    for b in backups:
        latest = latest_by_label.get(b.label)
        vid, latest_key, version_count = latest if latest else (None, None, 0)
        fc_count, total_size = stats_by_vid.get(vid, (0, 0)) if vid else (0, 0)
        result.append(BackupInfo(
            id=b.id, label=b.label, client_name=b.client_name, prefix=b.prefix,
            status=b.status, created_at=str(b.created_at),
            last_version=latest_key, version_count=version_count,
            file_count=fc_count, total_size_bytes=total_size,
            has_running=b.label in running_labels,
        ))
    return result


@app.get("/backups/disk-summary", response_model=dict[str, list[BackupDiskEntry]], dependencies=[Depends(require_api_key)])
def all_backup_disk_summary(db: Session = Depends(get_db)):
    """Retorna disk info da última versão done de cada backup, numa única chamada."""
    from collections import defaultdict

    max_ts_sq = (
        db.query(
            BackupVersion.backup_label,
            func.max(BackupVersion.created_at).label("max_ts"),
        )
        .filter(BackupVersion.status == "done")
        .group_by(BackupVersion.backup_label)
        .subquery()
    )
    latest_rows = (
        db.query(BackupVersion.backup_label, BackupVersion.id)
        .join(
            max_ts_sq,
            (BackupVersion.backup_label == max_ts_sq.c.backup_label)
            & (BackupVersion.created_at == max_ts_sq.c.max_ts)
            & (BackupVersion.status == "done"),
        )
        .all()
    )
    if not latest_rows:
        return {}

    label_by_vid = {row.id: row.backup_label for row in latest_rows}
    vid_list = list(label_by_vid.keys())

    sha_rows = (
        db.query(VersionFile.version_id, VersionFile.sha256)
        .filter(VersionFile.version_id.in_(vid_list))
        .distinct()
        .all()
    )
    if not sha_rows:
        return {row.backup_label: [] for row in latest_rows}

    all_shas = list({r.sha256 for r in sha_rows})

    copy_rows = (
        db.query(FileContentCopy.sha256, FileContentCopy.volume_path, FileContent.size)
        .join(FileContent, FileContent.sha256 == FileContentCopy.sha256)
        .filter(FileContentCopy.sha256.in_(all_shas))
        .all()
    )
    sha_copies: dict[str, list] = defaultdict(list)
    for r in copy_rows:
        sha_copies[r.sha256].append((r.volume_path, r.size))

    label_vol: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    for r in sha_rows:
        label = label_by_vid.get(r.version_id)
        if not label:
            continue
        for vol, size in sha_copies.get(r.sha256, []):
            label_vol[label][vol][0] += 1
            label_vol[label][vol][1] += size

    return {
        label: [
            BackupDiskEntry(volume_path=vol, file_count=stats[0], total_bytes=stats[1])
            for vol, stats in vols.items()
        ]
        for label, vols in label_vol.items()
    }


@app.get("/backups/{label}", response_model=BackupInfo, dependencies=[Depends(require_api_key)])
def get_backup(label: str, db: Session = Depends(get_db)):
    return _backup_info(_get_backup_or_404(label, db), db)


@app.patch("/backups/{label}", response_model=BackupInfo, dependencies=[Depends(require_api_key)])
def rename_backup(label: str, req: BackupRename, db: Session = Depends(get_db)):
    b = _get_backup_or_404(label, db)
    new = req.new_label.strip()
    if not new:
        raise HTTPException(status_code=422, detail="new_label não pode ser vazio")
    if new == label:
        raise HTTPException(status_code=422, detail="Novo label idêntico ao atual")
    if db.query(BackupID).filter(BackupID.label == new).first():
        raise HTTPException(status_code=409, detail=f"Label '{new}' já existe")
    db.query(BackupVersion).filter(BackupVersion.backup_label == label).update(
        {"backup_label": new}, synchronize_session=False
    )
    b.label = new
    db.commit()
    db.refresh(b)
    log.info(f"[rename] Label [{label}] → [{new}]")
    return _backup_info(b, db)


@app.get("/backups/{label}/disks", response_model=list[BackupDiskEntry], dependencies=[Depends(require_api_key)])
def backup_disks(label: str, db: Session = Depends(get_db)):
    _get_backup_or_404(label, db)
    # Escopo: apenas a versão done mais recente — evita escanear todas as versões históricas
    latest_vid = (
        db.query(BackupVersion.id)
        .filter(BackupVersion.backup_label == label,
                BackupVersion.status == "done")
        .order_by(BackupVersion.created_at.desc())
        .limit(1)
        .scalar()
    )
    if not latest_vid:
        return []
    sha_subq = (
        db.query(VersionFile.sha256)
        .filter(VersionFile.version_id == latest_vid)
        .distinct()
        .subquery()
    )
    rows = (
        db.query(
            FileContentCopy.volume_path,
            func.count(FileContentCopy.sha256).label("file_count"),
            func.coalesce(func.sum(FileContent.size), 0).label("total_bytes"),
        )
        .join(FileContent, FileContent.sha256 == FileContentCopy.sha256)
        .filter(FileContentCopy.sha256.in_(sha_subq))
        .group_by(FileContentCopy.volume_path)
        .all()
    )
    return [
        BackupDiskEntry(
            volume_path=r.volume_path,
            file_count=r.file_count,
            total_bytes=int(r.total_bytes),
        )
        for r in rows
    ]


@app.delete("/backups/{label}", response_model=BackupDeletedResponse, dependencies=[Depends(require_api_key)])
def delete_backup(label: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    b = _get_backup_or_404(label, db)
    version_ids = [
        r.id for r in db.query(BackupVersion.id).filter(BackupVersion.backup_label == label).all()
    ]
    if version_ids:
        db.query(VersionFile).filter(VersionFile.version_id.in_(version_ids)).delete(
            synchronize_session=False
        )
    db.query(BackupVersion).filter(BackupVersion.backup_label == label).delete(
        synchronize_session=False
    )
    db.delete(b)
    db.commit()
    invalidate_activity()
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
    updated = (db.query(BackupVersion)
               .filter(BackupVersion.backup_label == label,
                       BackupVersion.status == "running")
               .update({"status": "incomplete"}, synchronize_session=False))
    if updated:
        log.info(f"[versao] {label}: {updated} versão(ões) running → incomplete")
    v = BackupVersion(backup_label=label, version_key=req.version_key)
    db.add(v); db.commit(); db.refresh(v)
    invalidate_activity()
    log.info(f"[versao] {label}/{req.version_key} criada")
    return VersionCreatedResponse(created=True, version=_version_stats(v, db))


@app.get("/backups/{label}/versions", response_model=list[VersionInfo], dependencies=[Depends(require_api_key)])
def list_versions(label: str, db: Session = Depends(get_db)):
    """Lista versoes com stats — 2 queries fixas (sem N+1)."""
    _get_backup_or_404(label, db)
    versions = (db.query(BackupVersion)
                .filter(BackupVersion.backup_label == label)
                .order_by(BackupVersion.created_at.desc())
                .all())
    if not versions:
        return []

    vids = [v.id for v in versions]
    stats_by_vid: dict[int, tuple[int, int]] = {}
    for row in (
        db.query(VersionFile.version_id,
                 func.count(VersionFile.id).label("fc"),
                 func.coalesce(func.sum(FileContent.size), 0).label("sz"))
        .outerjoin(FileContent, FileContent.sha256 == VersionFile.sha256)
        .filter(VersionFile.version_id.in_(vids))
        .group_by(VersionFile.version_id)
        .all()
    ):
        stats_by_vid[row.version_id] = (row.fc, int(row.sz))

    result = []
    for v in versions:
        fc_count, total_size = stats_by_vid.get(v.id, (0, 0))
        duration = None
        if v.finished_at and v.created_at:
            duration = round((v.finished_at - v.created_at).total_seconds(), 1)
        result.append(VersionInfo(
            id=v.id, version_key=v.version_key, backup_label=v.backup_label,
            status=v.status, created_at=str(v.created_at),
            finished_at=str(v.finished_at) if v.finished_at else None,
            duration_seconds=duration, file_count=fc_count, total_size_bytes=total_size,
            absorbed_count=v.absorbed_count or 0,
        ))
    return result


@app.get("/backups/{label}/versions/{version_key}", response_model=VersionInfo, dependencies=[Depends(require_api_key)])
def get_version(label: str, version_key: str, db: Session = Depends(get_db)):
    return _version_stats(_get_version_or_404(label, version_key, db), db)


@app.patch("/backups/{label}/versions/{version_key}", response_model=VersionInfo, dependencies=[Depends(require_api_key)])
def finish_version(label: str, version_key: str, req: VersionFinish, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    v = _get_version_or_404(label, version_key, db)
    v.status = req.status
    v.finished_at = datetime.now()
    db.commit()
    invalidate_activity()
    log.info(f"[versao] {label}/{version_key} → {req.status}")
    if req.status == "done":
        background_tasks.add_task(_bg_auto_cleanup)
        background_tasks.add_task(_bg_process_ssd_pending_moves)
    return _version_stats(v, db)


@app.post("/backups/{label}/versions/{version_key}/absorb", response_model=AbsorbResponse, dependencies=[Depends(require_api_key)])
def absorb_version(label: str, version_key: str, req: AbsorbRequest, db: Session = Depends(get_db)):
    """
    Herda arquivos da versao fonte que nao existem na versao destino (por original_path).
    Usado no modo acumulativo: novos arquivos sao adicionados pelo upload normal;
    arquivos ausentes do cliente (deletados) sao preservados via absorb da versao anterior.
    """
    dest = _get_version_or_404(label, version_key, db)
    if dest.status != "running":
        raise HTTPException(409, f"Versão destino está '{dest.status}' — absorb só permitido em versões running")
    src = _get_version_or_404(label, req.source_version_key, db)

    total_src = (
        db.query(func.count(VersionFile.id))
        .filter(VersionFile.version_id == src.id)
        .scalar() or 0
    )

    existing_sq = select(VersionFile.original_path).where(VersionFile.version_id == dest.id)
    stmt = insert(VersionFile).from_select(
        ["version_id", "original_path", "sha256", "mtime"],
        select(
            literal(dest.id).label("version_id"),
            VersionFile.original_path,
            VersionFile.sha256,
            VersionFile.mtime,
        ).where(
            VersionFile.version_id == src.id,
            ~VersionFile.original_path.in_(existing_sq),
        ),
    )
    result = db.execute(stmt)
    inherited = result.rowcount
    dest.absorbed_count = (dest.absorbed_count or 0) + inherited
    db.commit()

    skipped = total_src - inherited
    log.info(f"[absorb] {label}/{version_key} ← {req.source_version_key}: {inherited} herdado(s), {skipped} ja presente(s)")
    return AbsorbResponse(inherited=inherited, skipped=skipped)


@app.delete("/backups/{label}/versions/{version_key}", response_model=VersionDeletedResponse, dependencies=[Depends(require_api_key)])
def delete_version(label: str, version_key: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    v = _get_version_or_404(label, version_key, db)
    db.delete(v)
    db.commit()
    invalidate_activity()
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
    """Verifica N arquivos em duas queries IN — sem N+1."""
    v = _get_version_or_404(req.backup_label, req.version_key, db)
    paths   = [i.original_path for i in req.files]
    sha256s = [i.sha256 for i in req.files]

    # VersionFiles já registrados nesta versão para estas paths (pode conter sha256 diferente)
    registered: dict[tuple[str, str], int] = {
        (row.original_path, row.sha256): row.id
        for row in (
            db.query(VersionFile.original_path, VersionFile.sha256, VersionFile.id)
            .filter(VersionFile.version_id == v.id,
                    VersionFile.original_path.in_(paths))
            .all()
        )
    }
    # FileContents que já existem no storage
    existing_contents: set[str] = {
        row.sha256
        for row in db.query(FileContent.sha256).filter(FileContent.sha256.in_(sha256s)).all()
    }

    results: list[CheckBatchResultItem] = []
    for item in req.files:
        fid = registered.get((item.original_path, item.sha256))
        if fid is not None:
            results.append(CheckBatchResultItem(
                needs_upload=False, content_exists=True,
                reason="Ja registrado nesta versao", file_id=fid))
        else:
            ce = item.sha256 in existing_contents
            results.append(CheckBatchResultItem(
                needs_upload=True, content_exists=ce,
                reason="Conteudo ja no storage — apenas registrar" if ce else "Upload necessario"))
    return results


# -- Upload -------------------------------------------------------------------
def _store_new_content(
    sha256: str,
    size: int,
    tmp_path: Path,
    volume: Path,
    ssd_dir: Optional[Path],
    backup_label: str,
    version_key: str,
    original_path: str,
    db: Session,
) -> FileContent:
    """Move o tmp para o destino final, cifra/verifica e registra no banco.
    Bloqueante (I/O + criptografia) — deve rodar via asyncio.to_thread para
    não travar o event loop. Retorna o FileContent vencedor (o criado aqui
    ou o de um upload concorrente que chegou primeiro)."""
    use_ssd = ssd_dir is not None
    dest = _ssd_content_path(sha256) if use_ssd else _content_path(sha256, volume)
    shutil.move(str(tmp_path), str(dest))
    if ENCRYPTION_ENABLED:
        log.info(f"[upload] cifrando {original_path!r} ({size / 1024 / 1024:.2f} MB) — sha256={sha256[:8]}…")
        _fd, _tmp_enc = tempfile.mkstemp(dir=dest.parent, prefix="_enc_")
        os.close(_fd)
        tmp_enc = Path(_tmp_enc)
        try:
            crypto.encrypt_stream(dest, tmp_enc, storage.encryption_key)
            shutil.move(str(tmp_enc), str(dest))
            _verify_stored_file(sha256, dest, encrypted=True)
            log.info(f"[upload] {backup_label}/{version_key} ← {original_path!r} — nova cifrada sha256={sha256[:8]}… ({size / 1024 / 1024:.2f} MB)")
        except Exception:
            tmp_enc.unlink(missing_ok=True)
            dest.unlink(missing_ok=True)
            raise
    else:
        log.info(f"[upload] {backup_label}/{version_key} ← {original_path!r} — nova sha256={sha256[:8]}… ({size / 1024 / 1024:.2f} MB)")
    fc = FileContent(sha256=sha256, stored_at=str(dest), size=size,
                     encrypted=ENCRYPTION_ENABLED)
    try:
        db.add(fc)
        if use_ssd:
            db.add(FileContentCopy(sha256=sha256, stored_at=str(dest), volume_path=str(ssd_dir)))
            hdd_dest = volume / "_content" / sha256[:2] / sha256
            db.add(SsdCachePendingMove(
                sha256=sha256,
                ssd_path=str(dest),
                dest_volume=str(volume),
                dest_path=str(hdd_dest),
            ))
        else:
            db.add(FileContentCopy(sha256=sha256, stored_at=str(dest), volume_path=str(volume)))
        db.flush()
    except IntegrityError:
        # Upload concorrente do mesmo sha256 venceu — usar o registro já criado
        db.rollback()
        dest.unlink(missing_ok=True)
        fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
        return fc, None
    # _ensure_replicas é chamado APÓS db.commit() em upload_file para não manter
    # o write lock do SQLite durante as cópias de arquivo para volumes réplica.
    return fc, (dest if not use_ssd else None)


@app.post("/upload", response_model=UploadResponse, dependencies=[Depends(require_api_key)])
async def upload_file(
    request: Request,
    background_tasks: BackgroundTasks,
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
    except Exception as _e:
        log.warning(f"[upload] path não é base64 válido ({original_path!r}): {_e} — usando como plain text")

    replica_source: Optional[Path] = None

    # Modo "so registrar" — conteudo ja existe no storage
    if content_sha256:
        fc = db.query(FileContent).filter(FileContent.sha256 == content_sha256).first()
        if not fc:
            raise HTTPException(400, f"Conteudo sha256={content_sha256} nao encontrado no storage")
        sha256 = content_sha256
        first_copy = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256).first()
        if first_copy:
            await asyncio.to_thread(_ensure_replicas, sha256, Path(first_copy.stored_at), db)
        log.info(f"[upload] {backup_label}/{version_key} ← {original_path!r} — registrada sha256={sha256[:8]}…")
    else:
        # Escolhe volume HDD de destino final; decide se usa SSD como staging.
        # Se todos os volumes estiverem abaixo do limiar, aciona cleanup antes de desistir.
        try:
            volume = _pick_volume()
        except storage.StorageThresholdExceeded as _exc:
            log.warning("[upload] Todos os volumes abaixo do limiar — acionando limpeza automática antes de continuar...")
            from daily_digest import _send_telegram
            await _send_telegram(
                f"⚠️ *NestVault — Alerta de Armazenamento*\n\n"
                f"{_exc}\n\n"
                f"Iniciando limpeza automática de versões antigas..."
            )
            await asyncio.to_thread(_auto_cleanup_if_needed, db)
            try:
                volume = _pick_volume()
            except storage.StorageThresholdExceeded:
                volume = storage.pick_volume_last_resort()
        ssd_dir   = _ssd_cache_write_dir(db)
        write_dir = ssd_dir if ssd_dir else volume
        sha256, size, tmp_path = await _stream_request_to_disk(request, write_dir)

        try:
            fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
            if fc:
                first_copy = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256).first()
                if first_copy:
                    stored = Path(first_copy.stored_at)
                    # Checagem leve: existência + tamanho esperado no disco. A verificação
                    # profunda (decifrar e re-hashear) fica com o job validate-integrity.
                    try:
                        stored_size = stored.stat().st_size
                    except OSError:
                        stored_size = None
                    if stored_size is None:
                        log.warning(f"[integrity] {sha256[:8]}… ausente no disco — purgando e re-enviando")
                        _purge_corrupted_content(sha256, db)
                        fc = None
                    elif stored_size != _expected_stored_size(fc.size, fc.encrypted):
                        log.warning(
                            f"[integrity] {sha256[:8]}… tamanho no disco ({stored_size}) difere do esperado "
                            f"({_expected_stored_size(fc.size, fc.encrypted)}) — purgando e re-enviando"
                        )
                        _purge_corrupted_content(sha256, db)
                        fc = None
                    else:
                        log.info(f"[upload] {backup_label}/{version_key} ← {original_path!r} — dedup sha256={sha256[:8]}… ({size / 1024 / 1024:.2f} MB)")
                        await asyncio.to_thread(_ensure_replicas, sha256, stored, db)
                        tmp_path.unlink(missing_ok=True)
                else:
                    tmp_path.unlink(missing_ok=True)

            if not fc:
                fc, replica_source = await asyncio.to_thread(
                    _store_new_content, sha256, size, tmp_path, volume, ssd_dir,
                    backup_label, version_key, original_path, db,
                )
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

    # Replica copies are created AFTER the write lock is released so that
    # concurrent uploads are not blocked during potentially slow file I/O.
    if replica_source is not None:
        await asyncio.to_thread(_ensure_replicas, sha256, replica_source, db)
        db.commit()

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

    log.info(f"[download] ativando {row.original_path!r} (file_id={file_id}) — sha256={row.sha256[:8]}…, {len(copies)} cópia(s)")

    for copy in copies:
        p = Path(copy.stored_at)
        try:
            p.stat()
        except FileNotFoundError:
            log.error(f"[download] {row.sha256[:8]}… ausente no disco em {p}")
            continue
        except OSError as exc:
            log.error(f"[download] {row.sha256[:8]}… erro ao acessar {p}: {exc}")
            continue
        log.info(f"[download] {row.sha256[:8]}… encontrado no disco em {p}")
        if is_encrypted:
            # filename* (RFC 5987) evita quebra de header / injecao via aspas ou
            # caracteres nao-ASCII no nome do arquivo.
            disposition = f"attachment; filename*=UTF-8''{quote(filename)}"
            return StreamingResponse(
                crypto.decrypt_chunks(p, storage.encryption_key),
                media_type="application/octet-stream",
                headers={"Content-Disposition": disposition},
            )
        return FileResponse(p, filename=filename)

    # 503 apenas se há cópias em volumes degraded (recuperáveis); 410 se o dado sumiu mesmo
    degraded_str = [str(v) for v in _degraded_volumes]
    has_degraded = bool(degraded_str) and db.query(FileContentCopy).filter(
        FileContentCopy.sha256 == row.sha256,
        FileContentCopy.volume_path.in_(degraded_str),
    ).count()
    log.error(f"[download] {row.sha256[:8]}… nenhuma cópia válida encontrada no disco para file_id={file_id}")
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
def _bg_run_nightly_cleanup() -> None:
    from nightly_cleanup import run_nightly_cleanup
    run_nightly_cleanup()


@app.post("/maintenance/nightly-cleanup", dependencies=[Depends(require_api_key)])
def force_nightly_cleanup(background_tasks: BackgroundTasks):
    """Executa manualmente a rotina de limpeza noturna em background."""
    background_tasks.add_task(_bg_run_nightly_cleanup)
    return {"status": "started"}


@app.post("/maintenance/cleanup-orphans", response_model=OrphanCleanupResponse, dependencies=[Depends(require_api_key)])
def force_cleanup_orphans(db: Session = Depends(get_db)):
    """Remove todos os FileContents nao referenciados por nenhuma versao ativa."""
    files_removed, bytes_freed = _cleanup_orphan_contents(db)
    mj = MaintenanceJob(
        job_type="cleanup-orphans",
        status="done",
        finished_at=datetime.now(),
        summary=f"{files_removed} arquivo(s) removido(s) ({round(bytes_freed / 1024 / 1024, 2)} MB)",
    )
    db.add(mj)
    db.commit()
    invalidate_activity()
    return OrphanCleanupResponse(files_removed=files_removed, bytes_freed=bytes_freed)


@app.post("/maintenance/rereplicate", response_model=RereplicateResponse, dependencies=[Depends(require_api_key)])
def force_rereplicate(db: Session = Depends(get_db)):
    """Re-replica conteúdos com menos cópias que REPLICATION_FACTOR. Útil após adicionar um disco novo."""
    replicated, skipped = _rereplicate_all(db)
    target = _target_replicas()
    mj = MaintenanceJob(
        job_type="rereplicate",
        status="done",
        finished_at=datetime.now(),
        summary=f"{replicated} replicado(s), {skipped} pulado(s) — fator alvo: {target}",
    )
    db.add(mj)
    db.commit()
    invalidate_activity()
    return RereplicateResponse(replicated=replicated, skipped=skipped, target_copies=target)


@app.post("/maintenance/reconcile-replication", response_model=ReconcileResponse, dependencies=[Depends(require_api_key)])
def reconcile_replication(db: Session = Depends(get_db)):
    """Remove cópias excedentes e preenche arquivos sub-replicados conforme REPLICATION_FACTOR."""
    cleaned = _cleanup_excess_copies(db)
    replicated, skipped = _rereplicate_all(db)
    target = _target_replicas()
    mj = MaintenanceJob(
        job_type="reconcile-replication",
        status="done",
        finished_at=datetime.now(),
        summary=f"{replicated} replicado(s), {cleaned} cópia(s) excedente(s) removida(s), {skipped} pulado(s) — fator alvo: {target}",
    )
    db.add(mj)
    db.commit()
    invalidate_activity()
    return ReconcileResponse(
        replicated=replicated,
        skipped=skipped,
        cleaned=cleaned,
        target_copies=target,
    )


def _bg_validate_integrity(job_id: int) -> None:
    from nightly_cleanup import validate_latest_versions_integrity
    db = SessionLocal()
    log_lines: list[str] = []

    def _progress(msg: str) -> None:
        log_lines.append(msg)
        mj = db.get(MaintenanceJob, job_id)
        if mj:
            mj.summary = "\n".join(log_lines[-30:])
            db.commit()
        invalidate_activity()

    try:
        result = validate_latest_versions_integrity(db, log_fn=_progress)
        mj = db.get(MaintenanceJob, job_id)
        if mj:
            mj.status = "done"
            mj.finished_at = datetime.now()
            parts = [f"{result['checked']} versão(ões) verificada(s)"]
            if result["invalidated"]:
                parts.append(f"{result['invalidated']} invalidada(s): {', '.join(result['labels'])}")
            else:
                parts.append("nenhuma invalidada")
            if result["files_removed"]:
                parts.append(f"{result['files_removed']} arquivo(s) removido(s)")
            mj.summary = " — ".join(parts)
            db.commit()
    except Exception as e:
        log.exception("[validate-integrity] erro no background")
        mj = db.get(MaintenanceJob, job_id)
        if mj:
            mj.status = "error"
            mj.finished_at = datetime.now()
            mj.summary = f"Erro: {e}"
            db.commit()
    finally:
        invalidate_activity()
        db.close()


@app.post("/maintenance/validate-integrity", dependencies=[Depends(require_api_key)])
def validate_integrity(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Verifica se os arquivos das últimas versões done existem no disco; invalida e limpa registros ausentes."""
    mj = MaintenanceJob(
        job_type="validate-integrity",
        status="running",
        summary="Iniciando verificação de integridade...",
    )
    db.add(mj)
    db.commit()
    db.refresh(mj)
    background_tasks.add_task(_bg_validate_integrity, mj.id)
    return {"scheduled": True, "job_id": mj.id}


def _bg_run_db_backup() -> None:
    from db_backup import run_db_backup
    run_db_backup()


@app.post("/maintenance/db-backup", dependencies=[Depends(require_api_key)])
def force_db_backup(background_tasks: BackgroundTasks):
    """Exporta o banco de dados para os volumes de storage e aplica rotação de backups."""
    background_tasks.add_task(_bg_run_db_backup)
    return {"status": "started"}


def _bg_encrypt_existing(job_id: int) -> None:
    db = SessionLocal()
    try:
        degraded_strs = [str(v) for v in _degraded_volumes]
        pending       = db.query(FileContent).filter(FileContent.encrypted == False).all()  # noqa: E712
        total         = len(pending)
        log.info(f"[encrypt-existing] {total} arquivo(s) pendente(s) de cifragem")

        mj = db.get(MaintenanceJob, job_id)
        if mj:
            mj.summary = f"Cifrando: 0 / {total} arquivo(s) (0%)"
            db.commit()
            invalidate_activity()

        pending_shas = [fc.sha256 for fc in pending]
        copies_q = db.query(FileContentCopy).filter(FileContentCopy.sha256.in_(pending_shas))
        if degraded_strs:
            copies_q = copies_q.filter(~FileContentCopy.volume_path.in_(degraded_strs))
        encrypt_copies_by_sha: dict[str, list] = {}
        for c in copies_q.all():
            encrypt_copies_by_sha.setdefault(c.sha256, []).append(c)

        files_encrypted = 0
        bytes_processed = 0
        skipped         = 0

        for i, fc in enumerate(pending, 1):
            copies = encrypt_copies_by_sha.get(fc.sha256, [])

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
                    crypto.encrypt_stream(p, tmp_enc, storage.encryption_key)
                    shutil.move(str(tmp_enc), str(p))
                    log.info(f"[encrypt-existing] [{i}/{total}] cópia em {copy.volume_path} cifrada com sucesso")
                except Exception as e:
                    log.warning(f"[encrypt-existing] [{i}/{total}] erro em {p}: {e}")
                    tmp_enc.unlink(missing_ok=True)
                    success = False
                    break

            if success:
                fc.encrypted = True
                files_encrypted += 1
                bytes_processed += fc.size
                db.commit()
                log.info(f"[encrypt-existing] [{i}/{total}] {fc.sha256[:8]}… concluído")
            else:
                skipped += 1

            pct = round(i / total * 100) if total else 100
            mj = db.get(MaintenanceJob, job_id)
            if mj:
                mj.summary = f"Cifrando: {i} / {total} arquivo(s) ({pct}%)"
                db.commit()
                invalidate_activity()

        log.info(f"[encrypt-existing] concluído — {files_encrypted} cifrado(s), {skipped} pulado(s), {bytes_processed / 1024 / 1024:.2f} MB processados")
        mj = db.get(MaintenanceJob, job_id)
        if mj:
            mj.status = "done"
            mj.finished_at = datetime.now()
            mj.summary = f"{files_encrypted} arquivo(s) cifrado(s), {skipped} pulado(s) — {bytes_processed / 1024 / 1024:.1f} MB"
            db.commit()
    except Exception as e:
        log.exception("[encrypt-existing] erro no background")
        mj = db.get(MaintenanceJob, job_id)
        if mj:
            mj.status = "error"
            mj.finished_at = datetime.now()
            mj.summary = f"Erro: {e}"
            db.commit()
    finally:
        invalidate_activity()
        db.close()


@app.post("/maintenance/encrypt-existing", dependencies=[Depends(require_api_key)])
def encrypt_existing_files(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Cifra todos os FileContents ainda não cifrados. Requer ENCRYPTION_ENABLED=true no servidor."""
    if not ENCRYPTION_ENABLED:
        raise HTTPException(400, "Criptografia não habilitada no servidor (ENCRYPTION_ENABLED=false)")
    mj = MaintenanceJob(
        job_type="encrypt-existing",
        status="running",
        summary="Iniciando cifragem de arquivos...",
    )
    db.add(mj)
    db.commit()
    db.refresh(mj)
    background_tasks.add_task(_bg_encrypt_existing, mj.id)
    return {"scheduled": True, "job_id": mj.id}


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
    mj = MaintenanceJob(
        job_type="cleanup-versions",
        status="done",
        finished_at=datetime.now(),
        summary=f"{label}: {len(keys_removed)} versão(ões) removida(s), {orphans_removed} arquivo(s) de storage liberado(s)",
    )
    db.add(mj)
    db.commit()
    invalidate_activity()
    return CleanupResponse(
        kept=req.keep,
        versions_removed=keys_removed,
        storage_files_removed=orphans_removed,
    )


# -- Cleanup por data ---------------------------------------------------------
def _latest_done_subquery(db: Session):
    """Subquery com os IDs da versão done mais recente de cada label."""
    return (
        db.query(func.max(BackupVersion.id))
        .filter(BackupVersion.status == "done")
        .group_by(BackupVersion.backup_label)
        .subquery()
    )


@app.get("/maintenance/cleanup-by-date/preview", dependencies=[Depends(require_api_key)])
def cleanup_by_date_preview(before: str, label: Optional[str] = None, db: Session = Depends(get_db)):
    scope = f"label={label}" if label else "todos os labels"
    log.info(f"[cleanup-by-date/preview] consultando antes de {before}, escopo={scope}")
    cutoff = datetime.fromisoformat(before)
    latest_done = _latest_done_subquery(db)
    q = (
        db.query(BackupVersion.backup_label, func.count(BackupVersion.id))
        .filter(BackupVersion.created_at < cutoff)
        .filter(BackupVersion.status != "running")
        .filter(~BackupVersion.id.in_(latest_done))
    )
    if label:
        q = q.filter(BackupVersion.backup_label == label)
    rows = q.group_by(BackupVersion.backup_label).all()
    total = sum(r[1] for r in rows)
    log.info(f"[cleanup-by-date/preview] {total} versão(ões) elegível(is): " +
             ", ".join(f"{r[0]}={r[1]}" for r in rows) if rows else "[cleanup-by-date/preview] nenhuma versão elegível")
    return {"total": total, "per_label": [{"label": r[0], "count": r[1]} for r in rows]}


@app.post("/maintenance/cleanup-by-date", dependencies=[Depends(require_api_key)])
def cleanup_by_date(
    before: str,
    background_tasks: BackgroundTasks,
    label: Optional[str] = None,
    db: Session = Depends(get_db),
):
    scope = f"label={label}" if label else "todos os labels"
    log.info(f"[cleanup-by-date] agendando exclusão antes de {before}, escopo={scope}")
    cutoff = datetime.fromisoformat(before)
    latest_done = _latest_done_subquery(db)
    q = (
        db.query(BackupVersion.id, BackupVersion.backup_label)
        .filter(BackupVersion.created_at < cutoff)
        .filter(BackupVersion.status != "running")
        .filter(~BackupVersion.id.in_(latest_done))
    )
    if label:
        q = q.filter(BackupVersion.backup_label == label)
    rows = q.all()

    per_label: dict[str, int] = {}
    version_ids: list[int] = []
    for vid, lbl in rows:
        per_label[lbl] = per_label.get(lbl, 0) + 1
        version_ids.append(vid)

    if not version_ids:
        log.info("[cleanup-by-date] nenhuma versão elegível — abortando")
        return {"scheduled": 0, "per_label": []}

    log.info(f"[cleanup-by-date] {len(version_ids)} versão(ões) agendada(s): " +
             ", ".join(f"{lbl}={cnt}" for lbl, cnt in per_label.items()))
    background_tasks.add_task(_bg_cleanup_by_date, version_ids, scope)
    return {
        "scheduled": len(version_ids),
        "per_label": [{"label": k, "count": cnt} for k, cnt in per_label.items()],
    }


# -- Migração de disco ---------------------------------------------------------
def _migrate_disk_preview_data(
    source: str,
    destinations: list[str],
    db: Session,
) -> MigrateDiskPreviewResponse:
    """Calcula o preview de migração sem iniciar nenhuma operação."""
    volume_strs = [str(v) for v in STORAGE_VOLUMES]
    if source not in volume_strs:
        raise HTTPException(status_code=400, detail=f"Volume de origem inválido: {source}")
    invalid = [d for d in destinations if d not in volume_strs]
    if invalid:
        raise HTTPException(status_code=400, detail=f"Volume(s) de destino inválido(s): {invalid}")
    if source in destinations:
        raise HTTPException(status_code=400, detail="Origem e destino não podem ser o mesmo volume")
    if not destinations:
        raise HTTPException(status_code=400, detail="Selecione pelo menos um volume de destino")

    dest_set = set(destinations)

    # Todos os sha256 que estão no volume de origem
    source_shas_rows = (
        db.query(FileContentCopy.sha256)
        .filter(FileContentCopy.volume_path == source)
        .all()
    )
    source_shas = {r.sha256 for r in source_shas_rows}

    if not source_shas:
        dest_infos = []
        for d in destinations:
            try:
                du = shutil.disk_usage(d)
                dest_infos.append(MigrateDiskDestInfo(path=d, free_bytes=du.free, capacity_bytes=du.total))
            except OSError:
                dest_infos.append(MigrateDiskDestInfo(path=d, free_bytes=0, capacity_bytes=0))
        return MigrateDiskPreviewResponse(
            source=source,
            files_to_copy=0,
            bytes_to_copy=0,
            files_already_on_dest=0,
            destinations=dest_infos,
            can_proceed=True,
            reason=None,
        )

    # sha256s que já têm cópia em algum destino
    already_rows = (
        db.query(FileContentCopy.sha256)
        .filter(
            FileContentCopy.sha256.in_(list(source_shas)),
            FileContentCopy.volume_path.in_(list(dest_set)),
        )
        .distinct()
        .all()
    )
    already_shas = {r.sha256 for r in already_rows}
    to_copy_shas = source_shas - already_shas

    bytes_to_copy = 0
    if to_copy_shas:
        row = (
            db.query(func.coalesce(func.sum(FileContent.size), 0))
            .filter(FileContent.sha256.in_(list(to_copy_shas)))
            .scalar()
        )
        bytes_to_copy = int(row)

    dest_infos = []
    total_free = 0
    for d in destinations:
        try:
            du = shutil.disk_usage(d)
            dest_infos.append(MigrateDiskDestInfo(path=d, free_bytes=du.free, capacity_bytes=du.total))
            total_free += du.free
        except OSError:
            dest_infos.append(MigrateDiskDestInfo(path=d, free_bytes=0, capacity_bytes=0))

    can_proceed = total_free >= bytes_to_copy
    reason = None if can_proceed else (
        f"Espaço insuficiente nos destinos: "
        f"necessário {round(bytes_to_copy/1024**3, 2)} GB, "
        f"disponível {round(total_free/1024**3, 2)} GB"
    )

    return MigrateDiskPreviewResponse(
        source=source,
        files_to_copy=len(to_copy_shas),
        bytes_to_copy=bytes_to_copy,
        files_already_on_dest=len(already_shas),
        destinations=dest_infos,
        can_proceed=can_proceed,
        reason=reason,
    )


@app.get(
    "/maintenance/migrate-disk/preview",
    response_model=MigrateDiskPreviewResponse,
    dependencies=[Depends(require_api_key)],
)
def migrate_disk_preview(
    source: str,
    destinations: list[str] = Query(default=[]),
    db: Session = Depends(get_db),
):
    log.info(f"[migrate-disk/preview] source={source} destinations={destinations}")
    return _migrate_disk_preview_data(source, destinations, db)


@app.post(
    "/maintenance/migrate-disk",
    dependencies=[Depends(require_api_key)],
)
def migrate_disk(
    req: MigrateDiskRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    log.info(f"[migrate-disk] source={req.source} destinations={req.destinations}")
    preview = _migrate_disk_preview_data(req.source, req.destinations, db)
    if not preview.can_proceed:
        raise HTTPException(status_code=400, detail=preview.reason)

    dest_str = ", ".join(req.destinations)
    mj = MaintenanceJob(
        job_type="disk-migration",
        status="running",
        summary=f"{req.source} → {dest_str}",
    )
    db.add(mj)
    db.commit()
    db.refresh(mj)
    job_id = mj.id

    background_tasks.add_task(_bg_migrate_disk, req.source, req.destinations, job_id)
    log.info(f"[migrate-disk] job #{job_id} agendado")
    return {"scheduled": True, "job_id": job_id}