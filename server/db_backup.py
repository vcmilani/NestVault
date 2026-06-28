"""Backup automático do banco de dados (PostgreSQL ou SQLite) para os volumes de storage."""

import logging
import os
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from cache_state import invalidate_activity
from database import DATABASE_URL, DB_PATH, SessionLocal, MaintenanceJob, engine
from storage import healthy_volumes

log = logging.getLogger("backup-server")

DB_BACKUP_ENABLED   = os.getenv("DB_BACKUP_ENABLED", "true").lower() == "true"
DB_BACKUP_RETENTION = int(os.getenv("DB_BACKUP_RETENTION", "7"))
DB_BACKUP_HOUR      = int(os.getenv("DB_BACKUP_HOUR", "1"))
DB_BACKUP_MINUTE    = int(os.getenv("DB_BACKUP_MINUTE", "0"))

_BACKUP_SUBDIR = "_db_backups"


def _backup_postgres(dest: Path) -> None:
    parsed = urlparse(DATABASE_URL)
    env = os.environ.copy()
    if parsed.password:
        env["PGPASSWORD"] = parsed.password
    cmd = [
        "pg_dump",
        f"--host={parsed.hostname or 'localhost'}",
        f"--port={parsed.port or 5432}",
        f"--username={parsed.username or 'postgres'}",
        f"--dbname={parsed.path.lstrip('/') or 'postgres'}",
        "--format=custom",
        f"--file={dest}",
    ]
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"pg_dump falhou: {result.stderr.strip()}")


def _backup_sqlite(dest: Path) -> None:
    src  = sqlite3.connect(str(DB_PATH))
    dst  = sqlite3.connect(str(dest))
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()


def _rotate(backup_dir: Path, pattern: str, retention: int) -> int:
    files = sorted(backup_dir.glob(pattern))
    excess = files[: max(0, len(files) - retention)]
    for f in excess:
        try:
            f.unlink()
        except OSError as e:
            log.warning(f"[db-backup] Não foi possível remover backup antigo {f}: {e}")
    return len(excess)


def run_db_backup() -> dict:
    """Exporta o banco de dados para todos os volumes saudáveis e aplica rotação de backups."""
    db = SessionLocal()
    mj = MaintenanceJob(
        job_type="db-backup",
        status="running",
        summary="Iniciando backup do banco de dados...",
    )
    db.add(mj)
    db.commit()
    db.refresh(mj)
    mj_id = mj.id
    invalidate_activity()

    is_postgres = bool(DATABASE_URL)
    db_type     = "postgresql" if is_postgres else "sqlite"
    ext         = "dump" if is_postgres else "db"
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename    = f"nestvault_db_{timestamp}.{ext}"
    pattern     = f"nestvault_db_*.{ext}"

    volumes = healthy_volumes()
    if not volumes:
        summary = "Nenhum volume saudável disponível para backup do banco"
        log.error(f"[db-backup] {summary}")
        mj = db.get(MaintenanceJob, mj_id)
        if mj:
            mj.status = "error"
            mj.finished_at = datetime.now()
            mj.summary = summary
            db.commit()
        invalidate_activity()
        db.close()
        return {"db_type": db_type, "files": [], "removed": 0, "error": summary}

    saved: list[str] = []
    total_removed = 0
    errors: list[str] = []

    for vol in volumes:
        backup_dir = vol / _BACKUP_SUBDIR
        try:
            backup_dir.mkdir(exist_ok=True)
        except OSError as e:
            log.warning(f"[db-backup] Não foi possível criar {backup_dir}: {e}")
            errors.append(f"{vol.name}: {e}")
            continue

        dest = backup_dir / filename
        mj = db.get(MaintenanceJob, mj_id)
        if mj:
            mj.summary = f"Exportando para {dest}..."
            db.commit()
        invalidate_activity()

        try:
            if is_postgres:
                _backup_postgres(dest)
            else:
                _backup_sqlite(dest)
            saved.append(str(dest))
            log.info(f"[db-backup] Backup salvo: {dest}")
        except Exception as e:
            log.error(f"[db-backup] Falha ao gravar em {dest}: {e}")
            errors.append(f"{vol.name}: {e}")
            continue

        removed = _rotate(backup_dir, pattern, DB_BACKUP_RETENTION)
        total_removed += removed
        if removed:
            log.info(f"[db-backup] {removed} backup(s) antigo(s) removido(s) de {backup_dir}")

    if saved:
        parts = [f"{len(saved)} volume(s) — tipo: {db_type}"]
        if total_removed:
            parts.append(f"{total_removed} backup(s) antigo(s) removido(s)")
        if errors:
            parts.append(f"{len(errors)} erro(s): {'; '.join(errors)}")
        status  = "done"
        summary = " — ".join(parts)
    else:
        status  = "error"
        summary = f"Nenhum backup criado — erros: {'; '.join(errors)}"

    log.info(f"[db-backup] {summary}")
    mj = db.get(MaintenanceJob, mj_id)
    if mj:
        mj.status      = status
        mj.finished_at = datetime.now()
        mj.summary     = summary
        db.commit()
    invalidate_activity()
    db.close()

    return {"db_type": db_type, "files": saved, "removed": total_removed}
