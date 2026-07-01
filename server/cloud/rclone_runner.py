"""Execução de jobs de cloud backup via rclone.

Diferença em relação ao runner.py padrão: não há tokens OAuth — o rclone
gerencia autenticação internamente via ~/.config/rclone/rclone.conf.

A estratégia é escolhida por backend (`run_rclone_backup_job` → `_uses_walk`):

CAMINHO RÁPIDO (`_run_fast_strategy`) — OneDrive, Google Drive, iCloud Drive e
qualquer backend com listagem recursiva eficiente:
  1. Uma única `rclone lsjson --recursive --fast-list` (varre tudo num só
     processo, com concorrência interna e reuso de conexão).
  2. Skip por mtime vs. última versão done (+ resume de versão incompleta).
  3. Download em lotes via `rclone copy --files-from` escopado à raiz.

WALK INCREMENTAL (`_run_walk_strategy`) — só para iCloud Photos
(iclouddrive/photos), que é lento/rate-limited e não completa listagem recursiva:
  1. Lista UM diretório por vez (`rclone lsjson` não recursivo), enfileirando
     subdiretórios — nunca uma listagem monolítica.
  2. Skip por mtime, ou por já estar registrado nesta versão (resume).
  3. Download por diretório: um único `rclone copy --max-depth 1 --files-from`
     com ingester concorrente (SHA-256 → dedup → store → encrypt → replicate →
     VersionFile), removendo cada arquivo do staging (disco limitado).
  4. Checkpoint em BackupVersion.progress_json: o resume continua na MESMA versão,
     pulando diretórios já concluídos sem re-listar.

Nota: o download usa `rclone copy` (escopado à raiz ou ao diretório) em vez de
`cat`/`copyto` por arquivo, porque a resolução de paths explícitos com nomes
unicode/acentuados falha em vários backends ('directory not found'); a listagem
usa as entradas reais devolvidas pelo servidor.
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

_PARALLEL_DOWNLOADS = 4   # --transfers do rclone

# Caminho rápido (OneDrive/GDrive/iCloud Drive): uma listagem recursiva única e
# download em lotes escopados à raiz.
_QUEUE_SIZE      = 6
_BATCH_MAX_FILES = 250
_BATCH_MAX_BYTES = 3 * 1024 ** 3   # 3 GB

# Walk incremental (iCloud Photos): o backend é lento e com rate-limit, então
# listamos e baixamos um diretório por vez, com checkpoint resumível. O download
# de cada diretório é um único `rclone copy` (lista o diretório só uma vez), com
# ingester concorrente que processa e remove cada arquivo concluído — mantendo o
# uso de disco do staging limitado mesmo em diretórios flat enormes.
_PARTIAL_SUFFIX = ".nvpart"   # sufixo de arquivo incompleto do rclone no staging
_INGEST_POLL    = 2.0         # segundos entre varreduras do ingester

# Backends que exigem o walk incremental (não conseguem listagem recursiva
# eficiente). Critério atual: serviço de fotos do iCloud (iclouddrive/photos).
_WALK_SERVICES = {"photos"}
_MAX_RESUMES   = 5   # versão incompleta é abandonada após este número de resumes falhados


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


async def _remote_config(remote_name: str) -> dict:
    """Retorna a config do remote (type, service, ...) via `rclone config dump`.

    Em falha de detecção retorna {} — o chamador trata como caminho rápido.
    """
    stdout, _, rc = await _rclone_run("config", "dump", timeout=30)
    if rc != 0:
        return {}
    try:
        return json.loads(stdout or b"{}").get(remote_name, {}) or {}
    except Exception:
        return {}


def _uses_walk(cfg: dict) -> bool:
    """Decide a estratégia: walk incremental só para backends lentos (fotos do
    iCloud). Qualquer outro backend usa o caminho rápido (listagem recursiva)."""
    return cfg.get("service") in _WALK_SERVICES


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


async def list_dir_one_level(
    remote_name: str, remote_path: str, rel_dir: str, *, retries: int = 3,
    timeout: int = 1800,
) -> tuple[list[RcloneFileEntry], list[str]]:
    """Lista UM nível de remote_name:{remote_path}/{rel_dir} (não recursivo).

    Retorna (arquivos, subdiretórios), ambos com paths relativos à raiz do job
    (remote_path) — consistente com RcloneFileEntry.path e VersionFile.original_path.

    Base do walk incremental: cada chamada lista só um diretório, então o backend
    (ex: iCloud Photos) nunca precisa entregar a biblioteca inteira de uma vez.
    """
    full = "/".join(p for p in (remote_path, rel_dir) if p)
    src = f"{remote_name}:{full}" if full else f"{remote_name}:"

    stdout = b"[]"
    for attempt in range(1, retries + 1):
        stdout, stderr, rc = await _run_lsjson(
            "--drive-skip-dangling-shortcuts",
            "--timeout", "300s",
            "--contimeout", "60s",
            src,
            timeout=timeout,
        )
        if rc == 0:
            break
        err_msg = stderr.decode().strip()
        if attempt < retries:
            log.warning(
                f"[rclone] lsjson de {rel_dir or '/'} falhou "
                f"(tentativa {attempt}/{retries}): {err_msg} — retentando em 10s"
            )
            await asyncio.sleep(10)
        else:
            raise RuntimeError(f"rclone lsjson falhou ({rc}): {err_msg}")

    files: list[RcloneFileEntry] = []
    subdirs: list[str] = []
    for item in json.loads(stdout or b"[]"):
        name = item["Name"]
        rel = f"{rel_dir}/{name}" if rel_dir else name
        if item.get("IsDir"):
            if name in _ONEDRIVE_PROTECTED_FOLDERS:
                log.info(f"[rclone] pasta protegida ignorada: {name!r}")
                continue
            subdirs.append(rel)
            continue
        if name in _IGNORED_SYSTEM_FILES:
            continue
        try:
            mtime = datetime.fromisoformat(
                item["ModTime"].replace("Z", "+00:00")
            ).timestamp()
        except Exception:
            mtime = 0.0
        files.append(RcloneFileEntry(path=rel, size=item.get("Size", 0), mtime=mtime))
    return files, subdirs


async def list_files_recursive(
    remote_name: str, remote_path: str, *, retries: int = 3
) -> list[RcloneFileEntry]:
    """Lista todos os arquivos recursivamente (caminho rápido — uma chamada).

    Eficiente em backends que suportam listagem recursiva (OneDrive, GDrive,
    iCloud Drive): o rclone varre tudo num só processo, com concorrência interna
    e reuso de conexão. NÃO usar para iCloud Photos (usa o walk incremental).
    """
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


# ---------------------------------------------------------------------------
# Dedup, store, encrypt, replicate
# ---------------------------------------------------------------------------

def _register_version_file_sync(version_id: int, entry, sha256: str, db) -> None:
    """Registra o VersionFile (idempotente — seguro em re-processamento/resume)."""
    existing = (
        db.query(VersionFile)
        .filter(
            VersionFile.version_id == version_id,
            VersionFile.original_path == entry.path,
        )
        .first()
    )
    if existing is not None:
        return
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
# Download por diretório (streaming, bounded-disk) + checkpoint
# ---------------------------------------------------------------------------

async def _download_and_ingest_dir(
    rel_dir: str,
    changed: list[RcloneFileEntry],
    remote_name: str,
    remote_path: str,
    version_id: int,
    db,
    enc_key: bytes | None,
    errors: list,
) -> tuple[int, int]:
    """Baixa (um único rclone copy) e ingere os arquivos novos de um diretório.

    Um `rclone copy --max-depth 1` lista o diretório só uma vez (sem re-varrer a
    raiz). Um ingester concorrente processa cada arquivo já concluído no staging
    (move → SHA-256 → dedup/store/encrypt/replicate) e o remove, mantendo o disco
    do staging limitado mesmo num diretório flat enorme.

    Retorna (baixados, bytes_baixados).
    """
    if not changed:
        return 0, 0

    volume  = storage.pick_volume()
    staging = Path(tempfile.mkdtemp(dir=volume, prefix="_rclone_stage_"))
    ff_path = Path(f"{staging}.files")
    # Arquivos são filhos diretos de rel_dir → --files-from usa só o basename.
    by_name: dict[str, RcloneFileEntry] = {Path(e.path).name: e for e in changed}
    remaining = set(by_name)
    downloaded = 0
    bytes_dl   = 0
    total_bytes = sum(e.size for e in changed)
    log.info(
        f"[rclone-runner] {rel_dir or '/'}: baixando {len(changed)} arquivo(s) "
        f"({_fmt_size(total_bytes)})"
    )

    async def _ingest_ready() -> None:
        nonlocal downloaded, bytes_dl
        for name in list(remaining):
            staged = staging / name
            if not staged.is_file():
                continue  # ainda não concluído (parcial tem sufixo _PARTIAL_SUFFIX)
            _fd, _tmp = tempfile.mkstemp(dir=volume, prefix="_rclone_tmp_")
            os.close(_fd)
            tmp_path = Path(_tmp)
            try:
                shutil.move(str(staged), str(tmp_path))
            except FileNotFoundError:
                tmp_path.unlink(missing_ok=True)
                continue
            entry = by_name[name]
            try:
                sha256, size = await asyncio.to_thread(_hash_file, tmp_path)
                await asyncio.to_thread(
                    _process_file_sync,
                    version_id, entry, tmp_path, sha256, size, volume, enc_key, db,
                )
                downloaded += 1
                bytes_dl   += size
            except Exception as e:
                tmp_path.unlink(missing_ok=True)
                errors.append(f"{entry.path}: {e}")
                log.error(f"[rclone-runner] Erro ao processar {entry.path}: {e}")
                db.rollback()
            remaining.discard(name)

    full = "/".join(p for p in (remote_path, rel_dir) if p)
    src = f"{remote_name}:{full}" if full else f"{remote_name}:"
    try:
        ff_path.write_text("".join(n + "\n" for n in by_name), encoding="utf-8")
        proc = await asyncio.create_subprocess_exec(
            "rclone", "copy", src, str(staging),
            "--files-from", str(ff_path),
            "--max-depth", "1",
            "--drive-skip-dangling-shortcuts",
            "--transfers", str(_PARALLEL_DOWNLOADS),
            "--checkers", "8",
            "--retries", "3",
            "--partial-suffix", _PARTIAL_SUFFIX,
            "--timeout", "300s",
            "--contimeout", "60s",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        # Drena stderr em background (evita travar o processo) e serve de sinal
        # de término: stderr.read() retorna ao EOF, quando o rclone sai.
        drain_task = asyncio.create_task(proc.stderr.read())
        while not drain_task.done():
            await _ingest_ready()
            try:
                await asyncio.wait_for(asyncio.shield(drain_task), timeout=_INGEST_POLL)
            except (TimeoutError, asyncio.CancelledError):
                pass
        stderr_bytes = await drain_task
        rc = await proc.wait()
        await _ingest_ready()  # varredura final após o término do rclone

        if rc != 0:
            err = stderr_bytes.decode(errors="replace").strip()
            log.warning(
                f"[rclone-runner] {rel_dir or '/'}: rclone copy retornou {rc}: {err}"
            )
        for name in remaining:
            entry = by_name[name]
            msg = f"{entry.path}: não baixado pelo rclone"
            errors.append(msg)
            log.error(f"[rclone-runner] {msg}")
    finally:
        ff_path.unlink(missing_ok=True)
        shutil.rmtree(staging, ignore_errors=True)

    return downloaded, bytes_dl


def _load_checkpoint(version) -> tuple[set[str], list[str], int]:
    """Lê (done_dirs, pending_dirs, resume_count) do progress_json da versão."""
    if not version.progress_json:
        return set(), [], 0
    try:
        data = json.loads(version.progress_json)
        return (
            set(data.get("done_dirs", [])),
            list(data.get("pending_dirs", [])),
            int(data.get("resume_count", 0)),
        )
    except Exception:
        return set(), [], 0


def _save_checkpoint_sync(
    version, done_dirs: set[str], pending_dirs: list[str], resume_count: int, db
) -> None:
    """Persiste o checkpoint do walk no progress_json da versão."""
    version.progress_json = json.dumps({
        "done_dirs": sorted(done_dirs),
        "pending_dirs": pending_dirs,
        "resume_count": resume_count,
    })
    db.commit()


# ---------------------------------------------------------------------------
# Caminho rápido: download em lote escopado à raiz (producer/consumer)
# ---------------------------------------------------------------------------

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
    """Entry point: detecta o backend e despacha para a estratégia adequada.

    - iCloud Photos (iclouddrive/photos): walk incremental resumível.
    - demais backends (OneDrive, GDrive, iCloud Drive): listagem recursiva única
      + download em lote (muito mais rápido onde a listagem recursiva funciona).
    """
    db = SessionLocal()
    job: RcloneBackupJob | None = None
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

        cfg = await _remote_config(job.remote_name)
        if _uses_walk(cfg):
            log.info(
                f"[rclone-runner] Estratégia: walk incremental "
                f"(backend {cfg.get('type', '?')}/{cfg.get('service', '?')})"
            )
            await _run_walk_strategy(job, db)
        else:
            log.info(
                f"[rclone-runner] Estratégia: listagem recursiva "
                f"(backend {cfg.get('type', '?')})"
            )
            await _run_fast_strategy(job, db)

    except Exception as e:
        log.exception(f"[rclone-runner] Job {job_id} falhou: {e}")
        db.rollback()
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


async def _run_walk_strategy(job: RcloneBackupJob, db) -> None:
    """Walk incremental resumível (iCloud Photos)."""
    version: BackupVersion | None = None
    try:
        # Marca 'running' órfãos como incomplete ANTES de selecionar o resume,
        # para que um run anterior interrompido (com checkpoint) seja retomável.
        db.query(BackupVersion).filter(
            BackupVersion.backup_label == job.target_label,
            BackupVersion.status == "running",
        ).update({"status": "incomplete"}, synchronize_session=False)
        db.commit()

        # Resume na MESMA versão se houver checkpoint pendente; senão cria nova.
        # Reusar a versão é o que torna a listagem resumível: diretórios já
        # concluídos (done_dirs) não são re-listados nem re-registrados.
        resume_version = (
            db.query(BackupVersion)
            .filter(
                BackupVersion.backup_label == job.target_label,
                BackupVersion.status.in_(["incomplete", "failed"]),
                BackupVersion.progress_json.isnot(None),
            )
            .order_by(BackupVersion.version_key.desc())
            .first()
        )

        if resume_version is not None:
            done_dirs, pending, resume_count = _load_checkpoint(resume_version)

            if resume_count >= _MAX_RESUMES:
                # Muitos resumes sem concluir — abandona esta versão e cria nova.
                resume_version.status = "failed"
                resume_version.progress_json = None   # sai do pool de resumáveis
                resume_version.finished_at = datetime.now().astimezone().replace(tzinfo=None)
                db.commit()
                log.warning(
                    f"[rclone-runner] Versão {resume_version.version_key} abandonada após "
                    f"{resume_count} resumes sem concluir — criando versão nova"
                )
                version_key = datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S")
                version = BackupVersion(
                    backup_label=job.target_label, version_key=version_key, status="running",
                )
                db.add(version)
                db.commit()
                done_dirs, pending, resume_count = set(), [""], 0
            else:
                version = resume_version
                resume_count += 1
                version.status = "running"
                db.commit()
                log.info(
                    f"[rclone-runner] Retomando versão {version.version_key} "
                    f"(resume {resume_count}/{_MAX_RESUMES})"
                )
        else:
            version_key = datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S")
            version = BackupVersion(
                backup_label=job.target_label, version_key=version_key,
                status="running",
            )
            db.add(version)
            db.commit()
            done_dirs, pending, resume_count = set(), [""], 0
        db.refresh(version)

        enc_key = storage.encryption_key if storage.ENCRYPTION_ENABLED else None

        # Baseline de skip-por-mtime: última versão concluída (done).
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

        # Arquivos JÁ registrados nesta versão (de sessões de resume anteriores):
        # pulados sem re-baixar mesmo que o diretório seja re-listado.
        this_version_paths: set[str] = {
            vf.original_path
            for vf in db.query(VersionFile.original_path)
            .filter(VersionFile.version_id == version.id)
            .all()
        }

        if not pending:
            pending = [""]   # safety: checkpoint vazio — recomeça da raiz
        discovered: set[str] = done_dirs | set(pending)
        if this_version_paths:
            log.info(
                f"[rclone-runner] resume: {len(this_version_paths)} arquivo(s) já nesta "
                f"versão, {len(done_dirs)} diretório(s) concluído(s), "
                f"{len(pending)} pendente(s)"
            )

        errors: list[str] = []
        failed_dirs: list[str] = []   # diretórios com erro — re-tentados no resume
        downloaded_total = 0
        bytes_total      = 0
        skipped_total    = 0
        processed_total  = 0

        t_start = time.monotonic()
        while pending:
            rel_dir = pending.pop(0)
            if rel_dir in done_dirs:
                continue

            # Falha de listagem de um diretório não aborta o job: registra,
            # marca para re-tentar no resume e segue para os demais.
            try:
                files, subdirs = await list_dir_one_level(
                    job.remote_name, job.remote_path, rel_dir
                )
            except Exception as e:
                errors.append(f"{rel_dir or '/'} (listagem): {e}")
                log.error(f"[rclone-runner] Erro ao listar {rel_dir or '/'}: {e}")
                if rel_dir not in failed_dirs:
                    failed_dirs.append(rel_dir)
                await asyncio.to_thread(
                    _save_checkpoint_sync, version, done_dirs, pending + failed_dirs, resume_count, db
                )
                continue

            # Enfileira novos subdiretórios (sem duplicar entre resumes).
            for sub in subdirs:
                if sub not in discovered:
                    discovered.add(sub)
                    pending.append(sub)

            # Particiona arquivos do diretório.
            changed: list[RcloneFileEntry] = []
            skip_entries: list[RcloneFileEntry] = []
            for entry in files:
                if entry.path in this_version_paths:
                    skipped_total   += 1
                    processed_total += 1
                    continue
                prev = prev_files.get(entry.path)
                if prev and prev[0] == entry.mtime:
                    skip_entries.append(entry)
                else:
                    changed.append(entry)

            errors_before = len(errors)

            # Registra os "sem alteração" (idempotente) na versão atual.
            for entry in skip_entries:
                try:
                    await asyncio.to_thread(
                        _register_version_file_sync,
                        version.id, entry, prev_files[entry.path][1], db,
                    )
                    skipped_total   += 1
                    processed_total += 1
                    this_version_paths.add(entry.path)
                except Exception as e:
                    errors.append(f"{entry.path}: {e}")
                    log.error(f"[rclone-runner] Erro ao registrar skip {entry.path}: {e}")
                    db.rollback()

            # Baixa e ingere os novos/alterados.
            dl, by = await _download_and_ingest_dir(
                rel_dir, changed, job.remote_name, job.remote_path,
                version.id, db, enc_key, errors,
            )
            downloaded_total += dl
            bytes_total      += by
            processed_total  += dl
            for entry in changed:
                this_version_paths.add(entry.path)

            # Checkpoint: marca o diretório como concluído só se não houve novos
            # erros. Se houve, o diretório vai para failed_dirs e é persistido na
            # fronteira pendente para ser re-tentado no próximo run (resume).
            if len(errors) == errors_before:
                done_dirs.add(rel_dir)
            else:
                failed_dirs.append(rel_dir)
            await asyncio.to_thread(
                _save_checkpoint_sync, version, done_dirs, pending + failed_dirs, resume_count, db
            )
            if files or subdirs:
                log.info(
                    f"[rclone-runner] {rel_dir or '/'}: {len(files)} arquivo(s), "
                    f"{len(subdirs)} subpasta(s) — {len(pending)} pendente(s) na fila"
                )

        elapsed = time.monotonic() - t_start

        if failed_dirs:
            # Walk percorreu tudo, mas alguns diretórios falharam → versão fica
            # 'incomplete' (resumível); o próximo run re-tenta failed_dirs.
            version.status = "incomplete"
            await asyncio.to_thread(
                _save_checkpoint_sync, version, done_dirs, failed_dirs, resume_count, db
            )
        else:
            version.status = "failed" if (processed_total == 0 and errors) else "done"
            version.progress_json = None   # walk concluído — limpa o checkpoint
            version.finished_at = datetime.now().astimezone().replace(tzinfo=None)
        db.commit()

        summary = (
            f"{processed_total} arquivo(s) processado(s) "
            f"({downloaded_total} baixado(s) [{_fmt_size(bytes_total)}], "
            f"{skipped_total} sem alteração) em {elapsed:.0f}s"
        )
        if errors:
            summary += f", {len(errors)} erro(s): {'; '.join(errors[:3])}"
            if len(errors) > 3:
                summary += f" ... (+{len(errors) - 3})"

        job.last_run_status  = "success" if not errors else "partial"
        job.last_run_message = summary
        db.commit()
        invalidate_activity()
        log.info(f"[rclone-runner] Job {job.id} concluído — {summary}")

    except Exception:
        db.rollback()
        if version is not None:
            try:
                version.status      = "failed"
                version.finished_at = datetime.now().astimezone().replace(tzinfo=None)
                db.commit()
            except Exception:
                pass
        raise


async def _run_fast_strategy(job: RcloneBackupJob, db) -> None:
    """Caminho rápido: listagem recursiva única + download em lote escopado à raiz
    (OneDrive, GDrive, iCloud Drive — backends com listagem recursiva eficiente)."""
    version: BackupVersion | None = None
    try:
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
        log.info(f"[rclone-runner] Job {job.id} concluído — {summary}")

    except Exception:
        db.rollback()
        if version is not None:
            try:
                version.status      = "failed"
                version.finished_at = datetime.now().astimezone().replace(tzinfo=None)
                db.commit()
            except Exception:
                pass
        raise
