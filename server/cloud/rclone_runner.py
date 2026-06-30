"""Execução de jobs de cloud backup via rclone.

Diferença em relação ao runner.py padrão: não há tokens OAuth — o rclone
gerencia autenticação internamente via ~/.config/rclone/rclone.conf.

Fluxo por job:
  1. Lista arquivos recursivamente via `rclone lsjson` (streaming, sem timeout total)
  2. Skip de arquivos com mtime inalterado (comparação com versão anterior)
  3. Producer: baixa em lotes via `rclone copy --files-from` a partir da raiz do
     job, calcula SHA-256 localmente e enfileira cada arquivo
  4. Consumer: dedup → store → encrypt → replicate → VersionFile (idêntico ao runner.py)
  5. Finaliza versão

Nota: o download usa `rclone copy` a partir da raiz configurada do job (não
`cat` por arquivo nem `copy` por diretório pai), porque a resolução de paths
explícitos com nomes unicode/acentuados falha em vários backends
('directory not found'); só a listagem/cópia recursiva a partir da raiz
estável usa as entradas reais devolvidas pelo servidor.
"""
import asyncio
import hashlib
import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import shutil

import crypto
import storage
from cache_state import invalidate_activity
from database import (
    BackupID, BackupVersion, FileContent, FileContentCopy,
    RcloneBackupJob, SessionLocal, VersionFile,
)

log = logging.getLogger("backup-server")

_PARALLEL_DOWNLOADS = 4   # --transfers do rclone em cada lote
_QUEUE_SIZE = 6

# Download em lote: cada `rclone copy` faz uma listagem recursiva da raiz do job,
# então agrupamos arquivos para amortizar esse custo, limitando o uso de disco
# do staging por lote.
_BATCH_MAX_FILES = 250
_BATCH_MAX_BYTES = 3 * 1024 ** 3   # 3 GB


def _fmt_size(n: int) -> str:
    """Formata bytes em string legível (ex: 12.3 MB)."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


# Pastas do OneDrive que exigem autenticação adicional — ignoradas em todos os jobs.
# "Personal Vault" é o nome em inglês; "Cofre Pessoal" é o nome em PT-BR.
_ONEDRIVE_PROTECTED_FOLDERS = {"Personal Vault", "Cofre Pessoal"}
_IGNORED_SYSTEM_FILES = {".DS_Store", "Thumbs.db", "desktop.ini"}


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


async def _run_lsjson(*args: str, timeout: int = 14400) -> tuple[bytes, bytes, int]:
    """Executa rclone lsjson com teto total generoso (default 4h).

    lsjson com --fast-list bufferiza toda a saída e só escreve no stdout ao
    final da varredura, então detectar travamento por ausência de dados no
    stdout gera falso-positivo em remotes grandes (iCloud Photos pode levar
    >10min só para listar). Em vez disso, usamos um teto total amplo e
    delegamos a detecção de conexão morta ao próprio rclone via
    --timeout/--contimeout (passados pelo chamador), que aborta IO travado
    em ~5min sem precisar de heurística nossa.
    """
    proc = await asyncio.create_subprocess_exec(
        "rclone", "lsjson", *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"rclone lsjson excedeu timeout total de {timeout}s")
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
        exclude_flags: list[str] = []
        for folder in _ONEDRIVE_PROTECTED_FOLDERS:
            exclude_flags += ["--exclude", f"{folder}/**"]
        stdout, stderr, rc = await _run_lsjson(
            "--recursive", "--fast-list",
            "--drive-skip-dangling-shortcuts",
            "--timeout", "300s",
            "--contimeout", "60s",
            *exclude_flags,
            "--exclude", ".DS_Store",
            "--exclude", "Thumbs.db",
            "--exclude", "desktop.ini",
            src,
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
        if Path(item["Path"]).name in _IGNORED_SYSTEM_FILES:
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


def _hash_file(path: Path) -> tuple[str, int]:
    """Calcula SHA-256 e tamanho de um arquivo local (bloqueante)."""
    h = hashlib.sha256()
    total = 0
    with open(path, "rb") as f:
        while chunk := f.read(storage.CHUNK_SIZE):
            h.update(chunk)
            total += len(chunk)
    return h.hexdigest(), total


async def _bulk_copy(
    remote_name: str, remote_path: str, files_from: Path, staging: Path,
) -> tuple[int, str]:
    """Baixa em lote via `rclone copy --files-from` a partir da raiz do job.

    Por que a raiz do job e não o diretório pai de cada arquivo: o rclone
    resolve um path explícito (ex: remote:Love/Maitê) caminhando e comparando
    nomes componente a componente, o que falha em nomes unicode/acentuados
    ('directory not found'). A listagem recursiva a partir da raiz configurada
    do job usa as entradas devolvidas pelo servidor — o mesmo mecanismo de
    list_files_recursive que funciona — então os caminhos relativos do
    --files-from casam corretamente.
    """
    src = f"{remote_name}:{remote_path}" if remote_path else f"{remote_name}:"
    proc = await asyncio.create_subprocess_exec(
        "rclone", "copy", src, str(staging),
        "--files-from", str(files_from),
        "--fast-list",
        "--drive-skip-dangling-shortcuts",
        "--transfers", str(_PARALLEL_DOWNLOADS),
        "--checkers", "8",
        "--retries", "3",
        "--timeout", "300s",
        "--contimeout", "60s",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    stderr_bytes = await proc.stderr.read()
    await proc.wait()
    return proc.returncode, stderr_bytes.decode(errors="replace").strip()


# ---------------------------------------------------------------------------
# Dedup, store, encrypt, replicate — funções compartilhadas pelo consumer
# ---------------------------------------------------------------------------

def _register_version_file_sync(version_id: int, entry, sha256: str, db) -> None:
    db.add(VersionFile(
        version_id=version_id,
        original_path=entry.path,
        sha256=sha256,
        mtime=entry.mtime,
    ))
    db.commit()


def _process_file_sync(
    version_id: int,
    entry,
    tmp_path: Path,
    sha256: str,
    size: int,
    volume: Path,
    enc_key: bytes | None,
    db,
) -> None:
    """Dedup, store, encrypt, replicate e registro no banco de um arquivo baixado.
    Bloqueante (I/O + criptografia) — roda via asyncio.to_thread."""
    fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
    if fc:
        tmp_path.unlink(missing_ok=True)
        first_copy = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256).first()
        if first_copy:
            storage.ensure_replicas(sha256, Path(first_copy.stored_at), db)
    else:
        dest = storage.content_path(sha256, volume)
        shutil.move(str(tmp_path), str(dest))
        if enc_key:
            _fd, _tmp_enc = tempfile.mkstemp(dir=dest.parent, prefix="_enc_")
            os.close(_fd)
            tmp_enc = Path(_tmp_enc)
            try:
                crypto.encrypt_stream(dest, tmp_enc, enc_key)
                shutil.move(str(tmp_enc), str(dest))
            except Exception:
                tmp_enc.unlink(missing_ok=True)
                raise
        fc = FileContent(sha256=sha256, stored_at=str(dest), size=size, encrypted=bool(enc_key))
        db.add(fc)
        db.add(FileContentCopy(sha256=sha256, stored_at=str(dest), volume_path=str(volume)))
        db.commit()
        storage.ensure_replicas(sha256, dest, db)

    _register_version_file_sync(version_id, entry, sha256, db)


# ---------------------------------------------------------------------------
# Producer-consumer (espelha o padrão de runner.py)
# ---------------------------------------------------------------------------

async def _download_batch(
    queue: asyncio.Queue,
    batch: list[RcloneFileEntry],
    remote_name: str,
    remote_path: str,
    errors: list,
    done_before: int,
    total_dl: int,
) -> None:
    """Baixa um lote em staging, calcula hash, e enfileira cada arquivo."""
    volume = storage.pick_volume()
    staging = Path(tempfile.mkdtemp(dir=volume, prefix="_rclone_stage_"))
    ff_path = Path(f"{staging}.files")
    batch_bytes = sum(e.size for e in batch)
    log.info(
        f"[rclone-runner] baixando lote de {len(batch)} arquivo(s) "
        f"({_fmt_size(batch_bytes)}) "
        f"[{done_before + 1}-{done_before + len(batch)}/{total_dl}]"
    )
    try:
        ff_path.write_text(
            "".join(e.path + "\n" for e in batch), encoding="utf-8"
        )
        rc, err = await _bulk_copy(remote_name, remote_path, ff_path, staging)
        if rc != 0:
            # Falha parcial é possível: o rclone pode ter copiado alguns
            # arquivos antes do erro. Seguimos e tratamos os ausentes abaixo.
            log.warning(f"[rclone-runner] rclone copy do lote retornou {rc}: {err}")

        for entry in batch:
            staged = staging / entry.path
            if not staged.is_file():
                msg = f"{entry.path}: não baixado pelo rclone (verifique permissão/atalho)"
                errors.append(msg)
                log.error(f"[rclone-runner] {msg}")
                continue
            # Move para arquivo plano no volume (rename barato, mesmo FS) e
            # libera o staging logo em seguida.
            _fd, _tmp = tempfile.mkstemp(dir=volume, prefix="_rclone_tmp_")
            os.close(_fd)
            tmp_path = Path(_tmp)
            shutil.move(str(staged), str(tmp_path))
            try:
                sha256, size = await asyncio.to_thread(_hash_file, tmp_path)
            except Exception as e:
                tmp_path.unlink(missing_ok=True)
                errors.append(f"{entry.path}: {e}")
                log.error(f"[rclone-runner] Erro ao ler {entry.path}: {e}")
                continue
            await queue.put(("file", entry, tmp_path, sha256, size, volume))
    finally:
        ff_path.unlink(missing_ok=True)
        shutil.rmtree(staging, ignore_errors=True)


async def _producer(
    queue: asyncio.Queue,
    all_files: list[RcloneFileEntry],
    prev_files: dict,
    remote_name: str,
    remote_path: str,
    errors: list,
    abort: asyncio.Event,
) -> None:
    total = len(all_files)

    # Particiona em "sem alteração" (skip por mtime) e "a baixar".
    to_download: list[RcloneFileEntry] = []
    for i, entry in enumerate(all_files, 1):
        prev = prev_files.get(entry.path)
        if prev and prev[0] == entry.mtime:
            if total >= 10 and (i == 1 or i % max(1, total // 4) == 0 or i == total):
                log.info(f"[rclone-runner] [{i}/{total}] sem alteração — {entry.path}")
            await queue.put(("skip", entry, prev[1]))
        else:
            to_download.append(entry)

    total_dl = len(to_download)
    log.info(
        f"[rclone-runner] {total - total_dl} sem alteração, "
        f"{total_dl} a baixar em lotes"
    )

    try:
        done = 0
        batch: list[RcloneFileEntry] = []
        batch_bytes = 0
        for entry in to_download:
            if abort.is_set():
                break
            batch.append(entry)
            batch_bytes += entry.size
            if len(batch) >= _BATCH_MAX_FILES or batch_bytes >= _BATCH_MAX_BYTES:
                await _download_batch(
                    queue, batch, remote_name, remote_path, errors, done, total_dl
                )
                done += len(batch)
                batch = []
                batch_bytes = 0
        if batch and not abort.is_set():
            await _download_batch(
                queue, batch, remote_name, remote_path, errors, done, total_dl
            )
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
        job.last_run_at      = datetime.now().astimezone().replace(tzinfo=None)
        job.last_run_status  = "running"
        job.last_run_message = None
        db.commit()
        invalidate_activity()

        # Garante que o BackupID (label) existe
        if not db.query(BackupID).filter(BackupID.label == job.target_label).first():
            db.add(BackupID(label=job.target_label, client_name="rclone"))
            db.commit()

        # Cria BackupVersion; marca running anteriores como incomplete
        version_key = datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S")
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
        version.finished_at = datetime.now().astimezone().replace(tzinfo=None)
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
                version.finished_at = datetime.now().astimezone().replace(tzinfo=None)
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
