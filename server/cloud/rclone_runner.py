"""Execução de jobs de cloud backup via rclone.

Diferença em relação ao runner.py padrão: não há tokens OAuth — o rclone
gerencia autenticação internamente via ~/.config/rclone/rclone.conf.

Fluxo por job:
  1. Lista arquivos recursivamente via `rclone lsjson`
  2. Skip de arquivos com mtime inalterado (comparação com versão anterior)
  3. Producer: até _PARALLEL_DOWNLOADS downloads via `rclone cat` com SHA-256
  4. Consumer: dedup → store → encrypt → replicate → VersionFile (idêntico ao runner.py)
  5. Finaliza versão
"""
import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import storage
from cache_state import invalidate_activity
from cloud.runner import _process_file_sync, _register_version_file_sync
from database import (
    BackupID, BackupVersion, RcloneBackupJob, SessionLocal, VersionFile,
)

log = logging.getLogger("backup-server")

_PARALLEL_DOWNLOADS = 3
_QUEUE_SIZE = 6


def _fmt_size(n: int) -> str:
    """Formata bytes em string legível (ex: 12.3 MB)."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


# Pastas do OneDrive que exigem autenticação adicional — ignoradas em todos os jobs.
# "Personal Vault" é o nome em inglês; "Cofre Pessoal" é o nome em PT-BR.
_ONEDRIVE_PROTECTED_FOLDERS = {"Personal Vault", "Cofre Pessoal"}


@dataclass
class RcloneFileEntry:
    path: str    # relativo à raiz do remote_path configurado no job
    size: int
    mtime: float  # unix timestamp


# ---------------------------------------------------------------------------
# Helpers de subprocess rclone
# ---------------------------------------------------------------------------

async def _rclone_run(*args: str, timeout: int = 300) -> tuple[bytes, bytes, int]:
    """Executa rclone com os args dados. Nunca usa shell=True."""
    proc = await asyncio.create_subprocess_exec(
        "rclone", *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"rclone {args[0]} excedeu timeout de {timeout}s")
    return stdout, stderr, proc.returncode


async def list_remotes() -> list[str]:
    """Lista remotes configurados no rclone config do sistema."""
    stdout, _, rc = await _rclone_run("listremotes")
    if rc != 0:
        return []
    return [r.rstrip(":") for r in stdout.decode().splitlines() if r.strip()]


async def browse_remote(remote_name: str, remote_path: str = "") -> list[dict]:
    """Lista subpastas (não recursivo) em remote_name:remote_path."""
    src = f"{remote_name}:{remote_path}" if remote_path else f"{remote_name}:"
    stdout, stderr, rc = await _rclone_run("lsjson", "--dirs-only", src, timeout=60)
    if rc != 0:
        raise RuntimeError(f"rclone lsjson falhou ({rc}): {stderr.decode().strip()}")
    items = json.loads(stdout or b"[]")
    return [
        {"name": item["Name"], "path": item["Path"]}
        for item in items
        if item["Name"] not in _ONEDRIVE_PROTECTED_FOLDERS
    ]


async def list_files_recursive(
    remote_name: str, remote_path: str, *, retries: int = 3
) -> list[RcloneFileEntry]:
    """Lista todos os arquivos recursivamente em remote_name:remote_path."""
    src = f"{remote_name}:{remote_path}" if remote_path else f"{remote_name}:"
    for attempt in range(1, retries + 1):
        stdout, stderr, rc = await _rclone_run(
            "lsjson", "--recursive", "--exclude", "Personal Vault/**", src, timeout=3600
        )
        if rc == 0:
            break
        err_msg = stderr.decode().strip()
        if attempt < retries:
            log.warning(
                f"[rclone] lsjson falhou (tentativa {attempt}/{retries}): {err_msg} "
                f"— retentando em 10s"
            )
            await asyncio.sleep(10)
        else:
            raise RuntimeError(f"rclone lsjson falhou ({rc}): {err_msg}")

    result = []
    filtered_protected: set[str] = set()
    for item in json.loads(stdout or b"[]"):
        if item.get("IsDir"):
            continue
        top_folder = item["Path"].split("/")[0]
        if top_folder in _ONEDRIVE_PROTECTED_FOLDERS:
            filtered_protected.add(top_folder)
            continue
        try:
            mtime = datetime.fromisoformat(
                item["ModTime"].replace("Z", "+00:00")
            ).timestamp()
        except Exception:
            mtime = 0.0
        result.append(RcloneFileEntry(
            path=item["Path"],
            size=item.get("Size", 0),
            mtime=mtime,
        ))
    for name in filtered_protected:
        log.info(f"[rclone] pasta protegida ignorada: {name!r}")
    return result


async def _download_to(
    remote_name: str, remote_path: str, file_path: str, dest: Path
) -> tuple[str, int]:
    """Baixa arquivo via `rclone cat` e calcula SHA-256 em single pass."""
    full_remote_path = f"{remote_path}/{file_path}" if remote_path else file_path
    src = f"{remote_name}:{full_remote_path}"
    proc = await asyncio.create_subprocess_exec(
        "rclone", "cat", src,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    h = hashlib.sha256()
    total = 0
    try:
        with open(dest, "wb") as f:
            while True:
                chunk = await asyncio.wait_for(
                    proc.stdout.read(storage.CHUNK_SIZE), timeout=300
                )
                if not chunk:
                    break
                f.write(chunk)
                h.update(chunk)
                total += len(chunk)
    except Exception:
        proc.kill()
        await proc.wait()
        dest.unlink(missing_ok=True)
        raise

    _, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
    if proc.returncode != 0:
        dest.unlink(missing_ok=True)
        raise RuntimeError(
            f"rclone cat falhou ({proc.returncode}): {stderr.decode().strip()}"
        )

    return h.hexdigest(), total


# ---------------------------------------------------------------------------
# Producer-consumer (espelha o padrão de runner.py)
# ---------------------------------------------------------------------------

async def _producer(
    queue: asyncio.Queue,
    all_files: list[RcloneFileEntry],
    prev_files: dict,
    remote_name: str,
    remote_path: str,
    errors: list,
    abort: asyncio.Event,
) -> None:
    semaphore = asyncio.Semaphore(_PARALLEL_DOWNLOADS)
    total = len(all_files)

    async def _download_one(i: int, entry: RcloneFileEntry) -> None:
        if abort.is_set():
            return

        prev = prev_files.get(entry.path)
        if prev and prev[0] == entry.mtime:
            if total >= 10 and (i == 1 or i % max(1, total // 4) == 0 or i == total):
                log.info(f"[rclone-runner] [{i}/{total}] sem alteração — {entry.path}")
            await queue.put(("skip", entry, prev[1]))
            return

        log.info(
            f"[rclone-runner] [{i}/{total}] baixando {entry.path} "
            f"({_fmt_size(entry.size)})"
        )
        volume = storage.pick_volume()
        tmp_path = volume / f"_rclone_tmp_{os.urandom(8).hex()}"

        async with semaphore:
            if abort.is_set():
                return
            try:
                sha256, size = await _download_to(
                    remote_name, remote_path, entry.path, tmp_path
                )
            except Exception as e:
                tmp_path.unlink(missing_ok=True)
                errors.append(f"{entry.path}: {e}")
                log.error(f"[rclone-runner] Erro ao baixar {entry.path}: {e}")
                return

        await queue.put(("file", entry, tmp_path, sha256, size, volume))

    try:
        tasks = [
            asyncio.create_task(_download_one(i, entry))
            for i, entry in enumerate(all_files, 1)
        ]
        await asyncio.gather(*tasks)
    finally:
        await queue.put(None)


async def _consumer(
    queue: asyncio.Queue,
    version_id: int,
    db,
    enc_key: bytes | None,
    errors: list,
    abort: asyncio.Event,
) -> tuple[int, int, int]:
    processed = 0
    skipped   = 0
    bytes_dl  = 0

    while True:
        item = await queue.get()
        if item is None:
            break

        kind = item[0]

        if kind == "skip":
            _, entry, sha256 = item
            try:
                await asyncio.to_thread(
                    _register_version_file_sync, version_id, entry, sha256, db
                )
                processed += 1
                skipped   += 1
            except Exception as e:
                errors.append(f"{entry.path}: {e}")
                log.error(f"[rclone-runner] Erro ao registrar skip {entry.path}: {e}")
                db.rollback()
            continue

        _, entry, tmp_path, sha256, size, volume = item
        tmp_path = Path(tmp_path)
        try:
            await asyncio.to_thread(
                _process_file_sync,
                version_id, entry, tmp_path, sha256, size, volume, enc_key, db,
            )
            processed += 1
            bytes_dl  += size
        except Exception as e:
            tmp_path.unlink(missing_ok=True)
            msg = f"{entry.path}: {e}"
            errors.append(msg)
            log.error(f"[rclone-runner] Erro ao processar {msg}")
            db.rollback()

    return processed, skipped, bytes_dl


# ---------------------------------------------------------------------------
# Entry point principal
# ---------------------------------------------------------------------------

async def run_rclone_backup_job(job_id: int) -> None:
    db = SessionLocal()
    job: RcloneBackupJob | None = None
    version: BackupVersion | None = None
    version_db_id: int | None = None
    try:
        job = db.get(RcloneBackupJob, job_id)
        if not job:
            log.error(f"[rclone-runner] Job {job_id} não encontrado")
            return

        log.info(
            f"[rclone-runner] Iniciando job {job_id}: "
            f"{job.remote_name}:{job.remote_path} → {job.target_label}"
        )
        job.last_run_at      = datetime.now()
        job.last_run_status  = "running"
        job.last_run_message = None
        db.commit()
        invalidate_activity()

        # Garante que o BackupID (label) existe
        if not db.query(BackupID).filter(BackupID.label == job.target_label).first():
            db.add(BackupID(label=job.target_label, client_name="rclone"))
            db.commit()

        # Cria BackupVersion; marca running anteriores como incomplete
        version_key = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        db.query(BackupVersion).filter(
            BackupVersion.backup_label == job.target_label,
            BackupVersion.status == "running",
        ).update({"status": "incomplete"}, synchronize_session=False)
        version = BackupVersion(
            backup_label=job.target_label, version_key=version_key, status="running"
        )
        db.add(version)
        db.commit()
        db.refresh(version)
        version_db_id = version.id

        # Lista arquivos no remote
        log.info(f"[rclone-runner] Listando {job.remote_name}:{job.remote_path}")
        all_files = await list_files_recursive(job.remote_name, job.remote_path)
        total = len(all_files)
        total_size = sum(e.size for e in all_files)
        log.info(f"[rclone-runner] {total} arquivo(s) encontrado(s) ({_fmt_size(total_size)} total)")

        enc_key = storage.encryption_key if storage.ENCRYPTION_ENABLED else None

        # Carrega prev_files para skip por mtime (baseline = última versão done)
        prev_files: dict[str, tuple[float, str]] = {}
        prev_done = (
            db.query(BackupVersion)
            .filter(
                BackupVersion.backup_label == job.target_label,
                BackupVersion.status == "done",
            )
            .order_by(BackupVersion.version_key.desc())
            .first()
        )
        if prev_done:
            for vf in db.query(VersionFile).filter(VersionFile.version_id == prev_done.id).all():
                prev_files[vf.original_path] = (vf.mtime, vf.sha256)
            log.info(
                f"[rclone-runner] {len(prev_files)} arquivo(s) na versão anterior "
                "para comparação de mtime"
            )

        # Arquivos de versão incompleta/falha para resume
        prev_incomplete = (
            db.query(BackupVersion)
            .filter(
                BackupVersion.backup_label == job.target_label,
                BackupVersion.status.in_(["incomplete", "failed"]),
            )
            .order_by(BackupVersion.version_key.desc())
            .first()
        )
        if prev_incomplete:
            resume_files = {
                vf.original_path: (vf.mtime, vf.sha256)
                for vf in db.query(VersionFile)
                .filter(VersionFile.version_id == prev_incomplete.id)
                .all()
            }
            if resume_files:
                prev_files.update(resume_files)
                log.info(
                    f"[rclone-runner] {len(resume_files)} arquivo(s) de versão "
                    "incompleta adicionados para resume"
                )

        errors: list[str] = []
        abort = asyncio.Event()
        queue: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_SIZE)

        t_start = time.monotonic()
        _, (processed, skipped, bytes_dl) = await asyncio.gather(
            _producer(queue, all_files, prev_files, job.remote_name, job.remote_path, errors, abort),
            _consumer(queue, version.id, db, enc_key, errors, abort),
        )
        elapsed = time.monotonic() - t_start

        version.status      = "failed" if (processed == 0 and errors) else "done"
        version.finished_at = datetime.now()
        db.commit()

        downloaded = processed - skipped
        summary = (
            f"{processed}/{total} arquivo(s) processado(s) "
            f"({downloaded} baixado(s) [{_fmt_size(bytes_dl)}], {skipped} sem alteração) "
            f"em {elapsed:.0f}s"
        )
        if errors:
            summary += f", {len(errors)} erro(s): {'; '.join(errors[:3])}"
            if len(errors) > 3:
                summary += f" ... (+{len(errors) - 3})"

        job.last_run_status  = "success" if not errors else "partial"
        job.last_run_message = summary
        db.commit()
        invalidate_activity()
        log.info(f"[rclone-runner] Job {job_id} concluído — {summary}")

    except Exception as e:
        log.exception(f"[rclone-runner] Job {job_id} falhou: {e}")
        db.rollback()
        if version and version_db_id:
            try:
                version.status      = "failed"
                version.finished_at = datetime.now()
                db.commit()
            except Exception:
                pass
        if job:
            try:
                job.last_run_status  = "error"
                job.last_run_message = str(e)
                db.commit()
                invalidate_activity()
            except Exception:
                pass
    finally:
        db.close()
