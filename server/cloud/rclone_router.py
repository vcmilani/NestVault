"""API routes para cloud backup via rclone.

Endpoints montados em /rclone — paralelos ao /cloud existente.
Não há OAuth: o usuário configura os remotes via `rclone config` no servidor.
"""
import asyncio
import re
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy.orm import Session

from auth import require_admin
from database import RcloneBackupJob, SessionLocal, get_db
import scheduler as sched

log = logging.getLogger("backup-server")

router = APIRouter()

_REMOTE_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
_VALID_STRATEGIES = {"auto", "walk", "fast"}

_job_locks: dict[int, asyncio.Lock] = {}


def _validate_strategy(v: Optional[str]) -> Optional[str]:
    if v is not None and v not in _VALID_STRATEGIES:
        raise ValueError(f"strategy deve ser um de {sorted(_VALID_STRATEGIES)}")
    return v


# ---------------------------------------------------------------------------
# Modelos Pydantic
# ---------------------------------------------------------------------------

def _parse_cron(expr: str) -> None:
    """Valida expressão cron com 5 campos. Lança ValueError se inválida."""
    from apscheduler.triggers.cron import CronTrigger
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(
            f"cron_expr inválido: '{expr}' — esperado 5 campos (min hr day month dow)"
        )
    try:
        CronTrigger(
            minute=parts[0], hour=parts[1], day=parts[2],
            month=parts[3], day_of_week=parts[4],
        )
    except Exception as e:
        raise ValueError(f"cron_expr inválido: {e}") from e


class RcloneJobCreate(BaseModel):
    remote_name: str
    remote_path: str = ""
    display_name: str
    target_label: str
    cron_expr: Optional[str] = None
    enabled: bool = True
    strategy: str = "auto"

    @field_validator("remote_name")
    @classmethod
    def _validate_remote_name(cls, v: str) -> str:
        if not _REMOTE_NAME_RE.match(v):
            raise ValueError(
                "remote_name deve conter apenas letras, números, _ ou - (máx 64 chars)"
            )
        return v

    @field_validator("cron_expr")
    @classmethod
    def _validate_cron_expr(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            _parse_cron(v)
        return v

    @field_validator("strategy")
    @classmethod
    def _validate_strategy_create(cls, v: str) -> str:
        return _validate_strategy(v)


class RcloneJobUpdate(BaseModel):
    display_name: Optional[str] = None
    remote_path: Optional[str] = None
    target_label: Optional[str] = None
    cron_expr: Optional[str] = None
    enabled: Optional[bool] = None
    strategy: Optional[str] = None

    @field_validator("cron_expr")
    @classmethod
    def _validate_cron_expr(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            _parse_cron(v)
        return v

    @field_validator("strategy")
    @classmethod
    def _validate_strategy_update(cls, v: Optional[str]) -> Optional[str]:
        return _validate_strategy(v)


class RcloneJobOut(BaseModel):
    id: int
    remote_name: str
    remote_path: str
    display_name: str
    target_label: str
    cron_expr: Optional[str]
    enabled: bool
    strategy: str
    last_run_at: Optional[str]
    last_run_status: Optional[str]
    last_run_message: Optional[str]
    created_at: str

    model_config = {"from_attributes": True}


def _job_out(j: RcloneBackupJob) -> RcloneJobOut:
    return RcloneJobOut(
        id=j.id,
        remote_name=j.remote_name,
        remote_path=j.remote_path,
        display_name=j.display_name,
        target_label=j.target_label,
        cron_expr=j.cron_expr,
        enabled=j.enabled,
        strategy=j.strategy,
        last_run_at=str(j.last_run_at) if j.last_run_at else None,
        last_run_status=j.last_run_status,
        last_run_message=j.last_run_message,
        created_at=str(j.created_at),
    )


def _require_job(job_id: int, db: Session) -> RcloneBackupJob:
    job = db.get(RcloneBackupJob, job_id)
    if not job:
        raise HTTPException(404, "Job rclone não encontrado")
    return job


# ---------------------------------------------------------------------------
# Remotes
# ---------------------------------------------------------------------------

@router.get("/remotes", dependencies=[Depends(require_admin)])
async def list_remotes():
    """Lista os remotes configurados no rclone config do servidor."""
    from cloud.rclone_runner import list_remotes as _list
    try:
        remotes = await _list()
    except Exception as e:
        raise HTTPException(502, f"Erro ao consultar rclone: {e}")
    return {"remotes": remotes}


@router.get("/remotes/{remote_name}/browse", dependencies=[Depends(require_admin)])
async def browse_remote(remote_name: str, path: str = ""):
    """Lista subpastas em remote_name:path (não recursivo)."""
    if not _REMOTE_NAME_RE.match(remote_name):
        raise HTTPException(400, "remote_name inválido")
    from cloud.rclone_runner import browse_remote as _browse
    try:
        folders = await _browse(remote_name, path)
    except RuntimeError as e:
        raise HTTPException(502, str(e))
    return {"folders": folders}


# ---------------------------------------------------------------------------
# Jobs — CRUD
# ---------------------------------------------------------------------------

@router.get("/jobs", response_model=list[RcloneJobOut], dependencies=[Depends(require_admin)])
def list_jobs(db: Session = Depends(get_db)):
    jobs = db.query(RcloneBackupJob).order_by(RcloneBackupJob.created_at.desc()).all()
    return [_job_out(j) for j in jobs]


@router.post("/jobs", response_model=RcloneJobOut, status_code=201, dependencies=[Depends(require_admin)])
def create_job(req: RcloneJobCreate, db: Session = Depends(get_db)):
    job = RcloneBackupJob(
        remote_name=req.remote_name,
        remote_path=req.remote_path.strip("/"),
        display_name=req.display_name,
        target_label=req.target_label,
        cron_expr=req.cron_expr,
        enabled=req.enabled,
        strategy=req.strategy,
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    if job.enabled and job.cron_expr:
        try:
            sched.add_or_update_rclone_job(job.id, job.cron_expr)
        except ValueError as e:
            raise HTTPException(400, str(e))

    log.info(f"[rclone] Job {job.id} criado: {job.remote_name}:{job.remote_path} → {job.target_label}")
    return _job_out(job)


@router.get("/jobs/{job_id}", response_model=RcloneJobOut, dependencies=[Depends(require_admin)])
def get_job(job_id: int, db: Session = Depends(get_db)):
    return _job_out(_require_job(job_id, db))


@router.patch("/jobs/{job_id}", response_model=RcloneJobOut, dependencies=[Depends(require_admin)])
def update_job(job_id: int, req: RcloneJobUpdate, db: Session = Depends(get_db)):
    job = _require_job(job_id, db)

    if req.display_name is not None:
        job.display_name = req.display_name
    if req.remote_path is not None:
        job.remote_path = req.remote_path.strip("/")
    if req.target_label is not None:
        job.target_label = req.target_label
    if req.cron_expr is not None:
        job.cron_expr = req.cron_expr
    if req.enabled is not None:
        job.enabled = req.enabled
    if req.strategy is not None:
        job.strategy = req.strategy

    db.commit()

    if job.enabled and job.cron_expr:
        try:
            sched.add_or_update_rclone_job(job.id, job.cron_expr)
        except ValueError as e:
            raise HTTPException(400, str(e))
    else:
        sched.remove_rclone_job(job.id)

    return _job_out(job)


@router.delete("/jobs/{job_id}", status_code=204, dependencies=[Depends(require_admin)])
def delete_job(job_id: int, db: Session = Depends(get_db)):
    job = _require_job(job_id, db)
    sched.remove_rclone_job(job.id)
    db.delete(job)
    db.commit()
    # Evita acúmulo de locks órfãos (um por job_id que já rodou).
    lock = _job_locks.get(job_id)
    if lock is None or not lock.locked():
        _job_locks.pop(job_id, None)


# ---------------------------------------------------------------------------
# Jobs — execução
# ---------------------------------------------------------------------------

@router.post("/jobs/{job_id}/run", status_code=202, dependencies=[Depends(require_admin)])
async def run_job_now(job_id: int, db: Session = Depends(get_db)):
    _require_job(job_id, db)
    lock = _job_locks.setdefault(job_id, asyncio.Lock())
    if lock.locked():
        raise HTTPException(409, "Job já está em execução")

    from cloud.rclone_runner import run_rclone_backup_job

    async def _run():
        async with lock:
            await run_rclone_backup_job(job_id)

    asyncio.create_task(_run())
    return {"status": "started", "job_id": job_id}


@router.get("/jobs/{job_id}/status", dependencies=[Depends(require_admin)])
def job_status(job_id: int, db: Session = Depends(get_db)):
    job = _require_job(job_id, db)
    return {
        "job_id":            job.id,
        "last_run_at":       str(job.last_run_at) if job.last_run_at else None,
        "last_run_status":   job.last_run_status,
        "last_run_message":  job.last_run_message,
    }


@router.post("/jobs/{job_id}/cancel", dependencies=[Depends(require_admin)])
def cancel_job(job_id: int, db: Session = Depends(get_db)):
    """Reseta um job travado como 'running' (ex: após reinício do servidor).

    Retorna 409 se o job estiver genuinamente em execução (lock ativo).
    """
    job = _require_job(job_id, db)
    lock = _job_locks.get(job_id)
    if lock and lock.locked():
        raise HTTPException(409, "Job está em execução — aguarde a conclusão ou reinicie o servidor")
    if job.last_run_status == "running":
        job.last_run_status  = "error"
        job.last_run_message = "Cancelado manualmente"
        db.commit()
        db.refresh(job)
    return _job_out(job)
