"""APScheduler integrado ao FastAPI — gerencia agendamentos dos rclone backup jobs e digest diário."""
import logging
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

log = logging.getLogger("backup-server")

scheduler = AsyncIOScheduler(timezone="UTC")


def schedule_daily_digest() -> None:
    """Agenda o digest diário. Hora configurável via DIGEST_HOUR (horário local, default 18)."""
    from daily_digest import send_daily_digest
    from datetime import datetime as _dt
    hour     = int(os.getenv("DIGEST_HOUR", "18"))
    local_tz = _dt.now().astimezone().tzinfo
    scheduler.add_job(
        send_daily_digest,
        CronTrigger(hour=hour, minute=0, timezone=local_tz),
        id="daily_digest",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    log.info(f"[scheduler] Daily digest agendado para {hour:02d}:00 (hora local)")


def schedule_nightly_cleanup() -> None:
    """Agenda a limpeza noturna de versões à meia-noite (horário local)."""
    from nightly_cleanup import run_nightly_cleanup
    from datetime import datetime as _dt
    local_tz = _dt.now().astimezone().tzinfo
    scheduler.add_job(
        run_nightly_cleanup,
        CronTrigger(hour=0, minute=0, timezone=local_tz),
        id="nightly_cleanup",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    log.info("[scheduler] Nightly cleanup agendado para 00:00 (hora local)")


def add_or_update_rclone_job(job_id: int, cron_expr: str) -> None:
    """Adiciona ou substitui o agendamento de um rclone job."""
    from cloud.rclone_runner import run_rclone_backup_job
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"cron_expr inválido: '{cron_expr}' — esperado 5 campos (min hr day month dow)")
    trigger = CronTrigger(
        minute=parts[0], hour=parts[1], day=parts[2],
        month=parts[3], day_of_week=parts[4],
    )
    scheduler.add_job(
        run_rclone_backup_job,
        trigger,
        args=[job_id],
        id=f"rclone_job_{job_id}",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    log.info(f"[scheduler] rclone job {job_id} agendado com cron='{cron_expr}'")


def remove_rclone_job(job_id: int) -> None:
    try:
        scheduler.remove_job(f"rclone_job_{job_id}")
        log.info(f"[scheduler] rclone job {job_id} removido do scheduler")
    except Exception:
        pass


def schedule_db_backup() -> None:
    """Agenda o backup do banco de dados. Hora/minuto configuráveis via DB_BACKUP_HOUR/DB_BACKUP_MINUTE."""
    from db_backup import run_db_backup, DB_BACKUP_ENABLED, DB_BACKUP_HOUR, DB_BACKUP_MINUTE
    from datetime import datetime as _dt
    if not DB_BACKUP_ENABLED:
        log.info("[scheduler] Backup do banco desabilitado (DB_BACKUP_ENABLED=false)")
        return
    local_tz = _dt.now().astimezone().tzinfo
    scheduler.add_job(
        run_db_backup,
        CronTrigger(hour=DB_BACKUP_HOUR, minute=DB_BACKUP_MINUTE, timezone=local_tz),
        id="db_backup",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    log.info(f"[scheduler] Backup do banco agendado para {DB_BACKUP_HOUR:02d}:{DB_BACKUP_MINUTE:02d} (hora local)")


def reload_rclone_jobs_from_db() -> None:
    """Restaura todos os rclone jobs habilitados ao iniciar o servidor."""
    from database import SessionLocal, RcloneBackupJob
    db = SessionLocal()
    try:
        jobs = db.query(RcloneBackupJob).filter(
            RcloneBackupJob.enabled == True,   # noqa: E712
            RcloneBackupJob.cron_expr != None, # noqa: E711
        ).all()
        for job in jobs:
            try:
                add_or_update_rclone_job(job.id, job.cron_expr)
            except Exception as e:
                log.warning(f"[scheduler] Não foi possível restaurar rclone job {job.id}: {e}")
        log.info(f"[scheduler] {len(jobs)} rclone job(s) restaurado(s) do banco")
    finally:
        db.close()
