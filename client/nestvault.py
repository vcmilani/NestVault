"""
NestVault  v4.8.0
Cada execucao de backup cria uma nova versao dentro do label.
Conteudo identico e armazenado uma unica vez no servidor (deduplicacao por sha256).

Uso:
    nestvault backup ~/docs --label "docs" --server http://192.168.1.100:8000
    nestvault versions --label "docs" --server http://192.168.1.100:8000
    nestvault restore /tmp/r --label "docs" --version "2026-04-25T10:42:31"
    nestvault restore /tmp/r --label "docs" --version "2026-04-25T10:42:31" --exclude cache node_modules
    nestvault cleanup --label "docs" --keep 5
    nestvault backups --server http://192.168.1.100:8000
"""

VERSION = "v4.8.0"

import os, sys, hashlib, argparse, base64, socket, threading
from pathlib import Path
from typing import Optional, Callable
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

import requests
from rich.console import Console
from rich.table import Table, Column
from rich.panel import Panel
from rich.progress import (
    Progress, SpinnerColumn, BarColumn, MofNCompleteColumn,
    TextColumn, TransferSpeedColumn, TimeRemainingColumn,
)
from rich import box

# -- Console & tema -----------------------------------------------------------
console = Console(highlight=False)
_verbose = False

AMBER = "#e8a940"
GREEN = "#4ecb8d"
RED   = "#e05c5c"
DIM   = "#5a6666"
TEXT  = "#c8d0ce"


def _header():
    console.print(f"\n  [bold {AMBER}]◈[/bold {AMBER}]  [bold {TEXT}]NESTVAULT[/bold {TEXT}]  [{DIM}]{VERSION}[/{DIM}]\n")


def _kv(key: str, val: str, val_style: str = TEXT):
    console.print(f"  [{DIM}]{key:<12}[/{DIM}]  [{val_style}]{val}[/{val_style}]")


def _info(msg: str):
    console.print(f"  [{TEXT}]{msg}[/{TEXT}]")


def _ok(msg: str):
    console.print(f"  [{GREEN}]{msg}[/{GREEN}]")


def _err(msg: str):
    console.print(f"  [bold {RED}]✗[/bold {RED}]  [{RED}]{msg}[/{RED}]")


def _warn(msg: str):
    console.print(f"  [{AMBER}]⚠[/{AMBER}]  [{AMBER}]{msg}[/{AMBER}]")


def _dim(msg: str):
    if _verbose:
        console.print(f"  [{DIM}]{msg}[/{DIM}]")


def _make_progress() -> Progress:
    return Progress(
        SpinnerColumn(style=AMBER),
        TextColumn(f"[{TEXT}]{{task.description}}"),
        BarColumn(style=DIM, complete_style=AMBER, finished_style=GREEN),
        MofNCompleteColumn(table_column=Column(style=TEXT)),
        TimeRemainingColumn(table_column=Column(style=DIM)),
        console=console,
        transient=False,
    )


def _make_transfer_progress() -> Progress:
    return Progress(
        TextColumn(f"  [{DIM}]{{task.description}}"),
        BarColumn(bar_width=40, style=DIM, complete_style=AMBER, finished_style=GREEN),
        TextColumn(f"[{TEXT}]{{task.completed:.1f}} MB"),
        TransferSpeedColumn(),
        console=console,
        transient=True,
    )


# -- Config -------------------------------------------------------------------
DEFAULT_SERVER = "http://localhost:8000"


def _load_api_key() -> str:
    key = os.getenv("BACKUP_API_KEY", "")
    try:
        key.encode("latin-1")
    except UnicodeEncodeError:
        _err(
            "BACKUP_API_KEY contem caracteres invalidos (ex: aspas curvas).\n"
            "      Copie a chave novamente usando apenas caracteres ASCII simples."
        )
        sys.exit(1)
    return key


API_KEY = _load_api_key()
IGNORED_NAMES = {".DS_Store", "Thumbs.db", "desktop.ini"}
CHUNK_SIZE = 1024 * 1024  # 1 MB


# -- Helpers ------------------------------------------------------------------
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb", buffering=0) as f:
        while chunk := f.read(CHUNK_SIZE):
            h.update(chunk)
    return h.hexdigest()


def _hash_item(item: tuple) -> tuple:
    fp, op, mtime, size = item
    try:
        return op, sha256_file(fp), size, mtime
    except OSError:
        return op, None, size, mtime


def build_headers(extra: Optional[dict] = None) -> dict:
    h = {"X-API-Key": API_KEY}
    if extra:
        h.update(extra)
    return h


def encode_path(path: str) -> str:
    return base64.b64encode(path.encode("utf-8")).decode("ascii")


def fmt_size(size: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def now_key() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S")


def _is_excluded(fp: Path, root: Path, ex: str) -> bool:
    try:
        return ex in fp.relative_to(root).parts
    except ValueError:
        return False


# -- HTTP session -------------------------------------------------------------
_session = requests.Session()
_session.headers.update({"Connection": "keep-alive"})


# -- API calls ----------------------------------------------------------------
def ensure_backup(server, label, client_name, prefix):
    r = _session.post(f"{server}/backups",
                      json={"label": label, "client_name": client_name, "prefix": prefix},
                      headers=build_headers(), timeout=10)
    r.raise_for_status()
    return r.json()


def create_version(server, label, version_key):
    r = _session.post(f"{server}/backups/{label}/versions",
                      json={"version_key": version_key},
                      headers=build_headers(), timeout=10)
    r.raise_for_status()
    return r.json()


def finish_version(server, label, version_key, status="done"):
    r = _session.patch(f"{server}/backups/{label}/versions/{version_key}",
                       json={"status": status},
                       headers=build_headers(), timeout=10)
    r.raise_for_status()


def check_file(server, label, version_key, original_path, sha256, size, mtime):
    r = _session.post(f"{server}/check",
                      json={"backup_label": label, "version_key": version_key,
                            "original_path": original_path,
                            "sha256": sha256, "size": size, "mtime": mtime},
                      headers=build_headers(), timeout=10)
    r.raise_for_status()
    return r.json()


def register_file(server, label, version_key, original_path, mtime, sha256):
    r = _session.post(
        f"{server}/upload",
        headers=build_headers({
            "X-Backup-Label":   label,
            "X-Version-Key":    version_key,
            "X-Original-Path":  encode_path(original_path),
            "X-Mtime":          str(mtime),
            "X-Content-Sha256": sha256,
        }),
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


class _ProgressReader:
    def __init__(self, path: Path, advance: Callable[[int], None]):
        self._f = open(path, "rb", buffering=0)
        self._advance = advance

    def read(self, size: int = -1) -> bytes:
        chunk = self._f.read(size)
        if chunk:
            self._advance(len(chunk))
        return chunk

    def close(self):
        self._f.close()


def upload_file(server, local_path: Path, label, version_key, original_path, mtime,
                progress: Optional[Progress] = None):
    file_size = local_path.stat().st_size
    # Escala com o tamanho: 1s por MB, mínimo 300s para hardware lento ou replicação
    read_timeout = max(300, file_size // (1024 * 1024))

    task_id = None
    if progress and file_size > 0:
        task_id = progress.add_task(
            local_path.name[:40],
            total=file_size / (1024 * 1024),
        )

    def _advance(n: int):
        if progress and task_id is not None:
            progress.update(task_id, advance=n / (1024 * 1024))

    reader = _ProgressReader(local_path, _advance)
    try:
        r = _session.post(
            f"{server}/upload",
            data=reader,
            headers=build_headers({
                "X-Backup-Label":  label,
                "X-Version-Key":   version_key,
                "X-Original-Path": encode_path(original_path),
                "X-Mtime":         str(mtime),
                "Content-Type":    "application/octet-stream",
                "Content-Length":  str(file_size),
            }),
            timeout=(10, read_timeout),
        )
    finally:
        if progress and task_id is not None:
            progress.remove_task(task_id)
        reader.close()
    r.raise_for_status()
    return r.json()


def check_batch(server, label, version_key, items: list[dict]) -> list[dict]:
    r = _session.post(
        f"{server}/check/batch",
        json={"backup_label": label, "version_key": version_key, "files": items},
        headers=build_headers(),
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def absorb_version(server: str, label: str, version_key: str, source_version_key: str) -> dict:
    r = _session.post(
        f"{server}/backups/{label}/versions/{version_key}/absorb",
        json={"source_version_key": source_version_key},
        headers=build_headers(),
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def _server_supports_batch(server: str) -> bool:
    try:
        r = _session.get(f"{server}/health", headers=build_headers(), timeout=5)
        if not r.ok:
            return False
        parts = r.json().get("version", "0.0.0").split(".")
        major, minor = int(parts[0]), int(parts[1])
        return (major, minor) >= (2, 6)
    except Exception:
        return False


def delete_label_api(server, label):
    r = _session.delete(f"{server}/backups/{label}", headers=build_headers(), timeout=30)
    r.raise_for_status()
    return r.json()


def force_cleanup_orphans_api(server):
    r = _session.post(f"{server}/maintenance/cleanup-orphans", headers=build_headers(), timeout=60)
    r.raise_for_status()
    return r.json()


def force_rereplicate_api(server):
    r = _session.post(f"{server}/maintenance/rereplicate", headers=build_headers(), timeout=(10, None))
    r.raise_for_status()
    return r.json()

def force_reconcile_api(server):
    r = _session.post(f"{server}/maintenance/reconcile-replication", headers=build_headers(), timeout=(10, None))
    r.raise_for_status()
    return r.json()


def force_encrypt_existing_api(server):
    r = _session.post(f"{server}/maintenance/encrypt-existing", headers=build_headers(), timeout=(10, None))
    r.raise_for_status()
    return r.json()


def sync_version(server, label, version_key, existing_paths):
    r = _session.post(f"{server}/sync",
                      json={"backup_label": label, "version_key": version_key,
                            "existing_paths": existing_paths},
                      headers=build_headers(), timeout=30)
    r.raise_for_status()
    return r.json()


def _fetch_prev_cache(server, label) -> tuple[Optional[str], dict]:
    try:
        r = _session.get(f"{server}/backups/{label}/versions",
                         headers=build_headers(), timeout=10)
        if not r.ok:
            return None, {}
        last_done = next((v for v in r.json() if v["status"] == "done"), None)
        if not last_done:
            return None, {}
        r2 = _session.get(
            f"{server}/files",
            headers=build_headers(),
            params={"backup_label": label, "version_key": last_done["version_key"]},
            timeout=30,
        )
        if not r2.ok:
            return None, {}
        return last_done["version_key"], {f["original_path"]: f for f in r2.json()}
    except requests.RequestException:
        return None, {}


# -- Backup -------------------------------------------------------------------
def _chunked(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def backup_directory(
    directory, label, server=DEFAULT_SERVER, dry_run=False,
    path_prefix=None, exclude=None, client_name=None, workers=4, verbose=False,
    batch_size=100, hash_workers=None, accumulate=False,
):
    global _verbose
    _verbose = verbose

    root = Path(directory).resolve()
    if not root.exists():
        _err(f"Diretorio nao encontrado: {root}")
        sys.exit(1)

    client_name = client_name or socket.gethostname()
    version_key = now_key()

    if not dry_run:
        ensure_backup(server, label, client_name, path_prefix)
        create_version(server, label, version_key)

    _kv("Label", label, AMBER)
    _kv("Versao", version_key)
    if dry_run:
        _kv("Modo", "dry-run", AMBER)

    prev_done_key, prev_cache = _fetch_prev_cache(server, label) if not dry_run else (None, {})
    if prev_cache:
        _dim(f"Cache: {len(prev_cache)} arquivo(s) da versao anterior")

    pending = []
    collected: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root, onerror=lambda e: _dim(f"Aviso: {e}")):
        for name in filenames:
            collected.append(Path(dirpath) / name)
    for fp in sorted(collected):
        if not fp.is_file() or fp.name in IGNORED_NAMES:
            continue
        if any(_is_excluded(fp, root, ex) for ex in (exclude or [])):
            continue
        op = str(fp) if not path_prefix else str(Path(path_prefix) / fp.relative_to(root))
        pending.append((fp, op))

    total = len(pending)
    _kv("Arquivos", str(total))

    stats = {"uploaded": 0, "registered": 0, "fast": 0, "skipped": 0, "errors": 0}
    lock  = threading.Lock()
    all_paths = [op for _, op in pending]

    use_batch = not dry_run and _server_supports_batch(server)
    effective = 1 if dry_run else workers
    effective_hash = 1 if dry_run else (hash_workers or os.cpu_count() or 4)

    if use_batch:
        _dim(f"Modo batch {batch_size} arq/req  ·  {effective} threads upload  ·  {effective_hash} proc hash")

    console.print()

    with _make_progress() as progress:
        overall = progress.add_task("Aguardando", total=total)

        def _update_bar():
            with lock:
                desc = (
                    f"[{AMBER}]↑ {stats['uploaded']}[/{AMBER}]  "
                    f"[{GREEN}]⊕ {stats['registered']}[/{GREEN}]  "
                    f"[{DIM}]⚡ {stats['fast']}  ✗ {stats['errors']}[/{DIM}]"
                )
            progress.update(overall, advance=1, description=desc)

        if use_batch:
            # ----- Fase 1: cache hits + hashing paralelo + batch check -----
            fast_files   = []
            pending_hash = []

            for fp, op in pending:
                try:
                    stat = fp.stat()
                except OSError:
                    _warn(f"Arquivo desapareceu antes do backup — ignorado: {op}")
                    stats["skipped"] += 1
                    _update_bar()
                    continue
                size   = stat.st_size
                mtime  = stat.st_mtime
                cached = prev_cache.get(op)
                if cached and cached["mtime"] == mtime and cached["size"] == size:
                    fast_files.append((fp, op, mtime, cached["sha256"]))
                else:
                    pending_hash.append((fp, op, mtime, size))

            hashed: dict[str, tuple[str, int, float]] = {}
            if pending_hash:
                _dim(f"Hashing {len(pending_hash)} arquivo(s)  ({len(fast_files)} cache hits)")
                with ProcessPoolExecutor(max_workers=effective_hash) as pool:
                    for op, sha256, size, mtime in pool.map(
                        _hash_item, pending_hash,
                        chunksize=max(1, len(pending_hash) // (effective_hash * 4)),
                    ):
                        if sha256 is None:
                            _warn(f"Arquivo desapareceu durante hashing — ignorado: {op}")
                            stats["skipped"] += 1
                            _update_bar()
                        else:
                            hashed[op] = (sha256, size, mtime)
                _dim("Hashing concluido")

            action_map: dict[str, tuple] = {}
            fp_map = {o: f for f, o, *_ in pending_hash}
            items_to_check = [
                {"original_path": op, "sha256": sha256, "size": size, "mtime": mtime}
                for op, (sha256, size, mtime) in hashed.items()
            ]
            for batch in _chunked(items_to_check, batch_size):
                try:
                    results = check_batch(server, label, version_key, batch)
                    for item, result in zip(batch, results):
                        op = item["original_path"]
                        sha256, size, mtime = hashed[op]
                        fp = fp_map[op]
                        if not result["needs_upload"]:
                            action_map[op] = ("skip", sha256, size, mtime, fp)
                        elif result.get("content_exists"):
                            action_map[op] = ("register", sha256, size, mtime, fp)
                        else:
                            action_map[op] = ("upload", sha256, size, mtime, fp)
                except requests.RequestException as e:
                    _err(f"Batch check: {e}")
                    for item in batch:
                        op = item["original_path"]
                        sha256, size, mtime = hashed[op]
                        action_map[op] = ("upload", sha256, size, mtime, fp_map[op])
                        with lock:
                            stats["errors"] += 1

            # ----- Fase 2: uploads e registers paralelos -----
            def _do_fast(fp, op, mtime, sha256):
                try:
                    _dim(f"FAST  {op}")
                    register_file(server, label, version_key, op, mtime, sha256)
                    with lock:
                        stats["fast"] += 1
                except requests.RequestException as e:
                    _err(f"{op}: {e}")
                    with lock:
                        stats["errors"] += 1
                finally:
                    _update_bar()

            def _do_action(op, action, sha256, size, mtime, fp):
                try:
                    if action == "skip":
                        _dim(f"SKIP  {op}")
                        with lock:
                            stats["skipped"] += 1
                    elif action == "register":
                        _dim(f"REG   {op}  ({fmt_size(size)})")
                        register_file(server, label, version_key, op, mtime, sha256)
                        with lock:
                            stats["registered"] += 1
                    else:
                        _dim(f"UP    {op}  ({fmt_size(size)})")
                        upload_file(server, fp, label, version_key, op, mtime, progress)
                        with lock:
                            stats["uploaded"] += 1
                except requests.RequestException as e:
                    _err(f"{op}: {e}")
                    with lock:
                        stats["errors"] += 1
                finally:
                    _update_bar()

            with ThreadPoolExecutor(max_workers=effective) as pool:
                futures = []
                for fp, op, mtime, sha256 in fast_files:
                    futures.append(pool.submit(_do_fast, fp, op, mtime, sha256))
                for op, (action, sha256, size, mtime, fp) in action_map.items():
                    futures.append(pool.submit(_do_action, op, action, sha256, size, mtime, fp))
                for future in as_completed(futures):
                    if exc := future.exception():
                        _err(f"Erro inesperado: {exc}")
                        with lock:
                            stats["errors"] += 1

        else:
            # ----- Fallback: check individual por worker -----
            def process(fp: Path, op: str):
                try:
                    stat  = fp.stat()
                    size  = stat.st_size
                    mtime = stat.st_mtime
                    cached = prev_cache.get(op)
                    if cached and cached["mtime"] == mtime and cached["size"] == size:
                        _dim(f"FAST  {op}  ({fmt_size(size)})")
                        if not dry_run:
                            register_file(server, label, version_key, op, mtime, cached["sha256"])
                        with lock:
                            stats["fast"] += 1
                        return

                    sha256 = sha256_file(fp)
                    check  = check_file(server, label, version_key, op, sha256, size, mtime)

                    if not check["needs_upload"]:
                        _dim(f"SKIP  {op}")
                        with lock:
                            stats["skipped"] += 1
                    elif check.get("content_exists"):
                        _dim(f"REG   {op}  ({fmt_size(size)})")
                        if not dry_run:
                            register_file(server, label, version_key, op, mtime, sha256)
                        with lock:
                            stats["registered"] += 1
                    else:
                        _dim(f"UP    {op}  ({fmt_size(size)})")
                        if not dry_run:
                            upload_file(server, fp, label, version_key, op, mtime, progress)
                        with lock:
                            stats["uploaded"] += 1

                except OSError as e:
                    _warn(f"{op}: arquivo inacessível — ignorado ({e.strerror})")
                    with lock:
                        stats["skipped"] += 1
                except requests.RequestException as e:
                    _err(f"{op}: {e}")
                    with lock:
                        stats["errors"] += 1
                finally:
                    _update_bar()

            with ThreadPoolExecutor(max_workers=effective) as pool:
                futures = {pool.submit(process, fp, op): op for fp, op in pending}
                for future in as_completed(futures):
                    if exc := future.exception():
                        _err(f"Erro inesperado: {exc}")
                        with lock:
                            stats["errors"] += 1

    absorb_result = None
    if not dry_run:
        try:
            sync_version(server, label, version_key, all_paths)
        except requests.RequestException as e:
            _err(f"Sync: {e}")

        status = "failed" if stats["errors"] else "done"
        if accumulate and status == "done" and prev_done_key:
            try:
                absorb_result = absorb_version(server, label, version_key, prev_done_key)
            except requests.RequestException as e:
                _err(f"Absorb: {e}")
        finish_version(server, label, version_key, status)

    status_color = GREEN if not stats["errors"] else RED
    status_text  = "concluido" if not stats["errors"] else "com erros"

    lines = [
        f"[{DIM}]Label[/{DIM}]        [{AMBER}][{label}][/{AMBER}]",
        f"[{DIM}]Versao[/{DIM}]       [{TEXT}]{version_key}[/{TEXT}]",
        f"[{DIM}]Verificados[/{DIM}]  [{TEXT}]{total}[/{TEXT}]",
        f"[{GREEN}]Enviados[/{GREEN}]     [{GREEN}]{stats['uploaded']}[/{GREEN}]",
        f"[{GREEN}]Registrados[/{GREEN}]  [{GREEN}]{stats['registered']}[/{GREEN}]",
        f"[{DIM}]Cacheados[/{DIM}]    [{DIM}]{stats['fast']}[/{DIM}]",
        f"[{DIM}]Ignorados[/{DIM}]    [{DIM}]{stats['skipped']}[/{DIM}]",
    ]
    if stats["errors"]:
        lines.append(f"[bold {RED}]Erros[/bold {RED}]        [{RED}]{stats['errors']}[/{RED}]")
    if absorb_result is not None:
        lines.append(
            f"[{GREEN}]Herdados[/{GREEN}]     [{GREEN}]{absorb_result['inherited']}[/{GREEN}]"
            f"  [{DIM}](modo acumulativo)[/{DIM}]"
        )

    console.print()
    console.print(Panel(
        "\n".join(lines),
        title=f"[bold {status_color}]◈  backup {status_text}[/bold {status_color}]",
        border_style=status_color,
        padding=(0, 2),
    ))
    console.print()
    return stats


# -- List backups -------------------------------------------------------------
def list_backups(server=DEFAULT_SERVER, client_name=None):
    params = {}
    if client_name:
        params["client_name"] = client_name
    r = _session.get(f"{server}/backups", headers=build_headers(), params=params, timeout=10)
    r.raise_for_status()
    backups = r.json()

    if not backups:
        _info("Nenhum backup encontrado.")
        return

    table = Table(
        box=box.SIMPLE_HEAD,
        show_header=True,
        header_style=f"bold {DIM}",
        border_style=DIM,
        pad_edge=False,
        show_edge=False,
    )
    table.add_column("LABEL",         style=AMBER,  no_wrap=True, min_width=20)
    table.add_column("CLIENTE",       style=TEXT,   no_wrap=True, min_width=16)
    table.add_column("VERSÕES",       style=TEXT,   justify="right")
    table.add_column("ARQUIVOS",      style=TEXT,   justify="right")
    table.add_column("TAMANHO",       style=TEXT,   justify="right")
    table.add_column("ÚLTIMA VERSÃO", style=DIM,    no_wrap=True)

    for b in backups:
        table.add_row(
            b["label"],
            b.get("client_name") or "—",
            str(b["version_count"]),
            str(b["file_count"]),
            fmt_size(b["total_size_bytes"]),
            (b.get("last_version") or "—")[:19],
        )

    console.print()
    console.print(table)
    console.print()


# -- List versions ------------------------------------------------------------
def list_versions(label, server=DEFAULT_SERVER):
    r = _session.get(f"{server}/backups/{label}/versions",
                     headers=build_headers(), timeout=10)
    r.raise_for_status()
    versions = r.json()

    if not versions:
        _info(f"Nenhuma versao encontrada em [{label}].")
        return

    _STATUS_STYLE = {
        "done":    GREEN,
        "failed":  RED,
        "running": AMBER,
    }

    table = Table(
        box=box.SIMPLE_HEAD,
        show_header=True,
        header_style=f"bold {DIM}",
        border_style=DIM,
        pad_edge=False,
        show_edge=False,
        title=f"[{AMBER}][{label}][/{AMBER}]",
        title_justify="left",
    )
    table.add_column("VERSÃO",   style=TEXT, no_wrap=True, min_width=22)
    table.add_column("STATUS",   no_wrap=True)
    table.add_column("ARQUIVOS", style=TEXT, justify="right")
    table.add_column("TAMANHO",  style=TEXT, justify="right")
    table.add_column("DURAÇÃO",  style=DIM,  justify="right")

    for v in versions:
        dur = v.get("duration_seconds")
        if dur is None:
            dur_str = "—"
        elif dur < 60:
            dur_str = f"{dur:.0f}s"
        elif dur < 3600:
            dur_str = f"{dur/60:.1f}min"
        else:
            dur_str = f"{dur/3600:.1f}h"

        status = v["status"]
        sc = _STATUS_STYLE.get(status, TEXT)
        table.add_row(
            v["version_key"],
            f"[{sc}]{status}[/{sc}]",
            str(v["file_count"]),
            fmt_size(v["total_size_bytes"]),
            dur_str,
        )

    console.print()
    console.print(table)
    console.print()


# -- Restore ------------------------------------------------------------------
def restore(destination, label, version_key, server=DEFAULT_SERVER,
            path_prefix=None, dry_run=False, overwrite=False, exclude=None):
    dest_root = Path(destination)
    dest_root.mkdir(parents=True, exist_ok=True)

    _kv("Label",   label, AMBER)
    _kv("Versao",  version_key)
    _kv("Destino", str(dest_root))

    params = {"backup_label": label, "version_key": version_key}
    if path_prefix:
        params["path_prefix"] = path_prefix

    try:
        r = _session.get(f"{server}/files", headers=build_headers(), params=params, timeout=10)
        r.raise_for_status()
        files = r.json()
    except requests.RequestException as e:
        _err(f"Erro ao buscar arquivos: {e}")
        sys.exit(1)

    if not files:
        _info("Nenhum arquivo ativo nesta versao.")
        return

    _kv("Arquivos", str(len(files)))
    console.print()

    stats = {"restored": 0, "skipped": 0, "errors": 0}

    with _make_transfer_progress() as progress:
        for record in files:
            file_id       = record["id"]
            original_path = record["original_path"]
            sha256        = record["sha256"]
            size          = record["size"]

            relative = (original_path[len(path_prefix):].lstrip("/")
                        if path_prefix and original_path.startswith(path_prefix)
                        else original_path.lstrip("/"))
            dest_file = dest_root / relative

            if any(ex in Path(relative).parts for ex in (exclude or [])):
                stats["skipped"] += 1
                continue

            if dest_file.exists() and not overwrite:
                match = sha256_file(dest_file) == sha256
                label_str = "[identico]" if match else "[modificado — use --overwrite]"
                _dim(f"{'SKIP' if match else 'DIFF'}  {relative}  {label_str}")
                stats["skipped"] += 1
                continue

            if dry_run:
                _dim(f"DOWN  {relative}  ({fmt_size(size)})  [dry-run]")
                continue

            try:
                r = _session.get(f"{server}/files/{file_id}/download",
                                 headers=build_headers(), stream=True, timeout=120)
                r.raise_for_status()
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                total_bytes = int(r.headers.get("Content-Length", size))
                task_id = progress.add_task(
                    Path(relative).name[:40],
                    total=total_bytes / (1024 * 1024),
                )
                with open(dest_file, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)
                        progress.update(task_id, advance=len(chunk) / (1024 * 1024))
                progress.remove_task(task_id)

                if sha256_file(dest_file) != sha256:
                    _err(f"Integridade falhou — {relative} removido")
                    dest_file.unlink()
                    stats["errors"] += 1
                    continue
                stats["restored"] += 1

            except requests.RequestException as e:
                _err(f"{relative}: {e}")
                stats["errors"] += 1

    status_color = GREEN if not stats["errors"] else RED
    status_text  = "concluido" if not stats["errors"] else "com erros"

    lines = [
        f"[{DIM}]Versao[/{DIM}]       [{TEXT}]{version_key}[/{TEXT}]",
        f"[{GREEN}]Restaurados[/{GREEN}]  [{GREEN}]{stats['restored']}[/{GREEN}]",
        f"[{DIM}]Ignorados[/{DIM}]    [{DIM}]{stats['skipped']}[/{DIM}]",
    ]
    if stats["errors"]:
        lines.append(f"[bold {RED}]Erros[/bold {RED}]        [{RED}]{stats['errors']}[/{RED}]")

    console.print()
    console.print(Panel(
        "\n".join(lines),
        title=f"[bold {status_color}]◈  restore {status_text}[/bold {status_color}]",
        border_style=status_color,
        padding=(0, 2),
    ))
    console.print()


# -- Cleanup ------------------------------------------------------------------
def _cleanup_label(label, keep, server=DEFAULT_SERVER):
    r = _session.post(
        f"{server}/backups/{label}/cleanup",
        json={"backup_label": label, "keep": keep},
        headers=build_headers(), timeout=30,
    )
    r.raise_for_status()
    result = r.json()
    removed = result.get("versions_removed", [])
    storage = result.get("storage_files_removed", 0)
    kept    = result["kept"]
    _info(
        f"[{AMBER}][{label}][/{AMBER}]  "
        f"[{GREEN}]mantidas={kept}[/{GREEN}]  "
        f"[{RED}]removidas={len(removed)}[/{RED}]  "
        f"[{DIM}]storage={storage}[/{DIM}]"
    )
    for v in removed:
        _dim(f"  - {v}")
    return result


def cleanup(label=None, keep=5, server=DEFAULT_SERVER):
    if label:
        labels = [label]
    else:
        r = _session.get(f"{server}/backups", headers=build_headers(), timeout=10)
        r.raise_for_status()
        labels = [b["label"] for b in r.json()]
        if not labels:
            _info("Nenhum backup encontrado.")
            return
        _kv("Cleanup", f"todos os labels ({len(labels)} encontrados)  keep={keep}")

    console.print()
    total_versions = 0
    total_storage  = 0
    for lbl in labels:
        try:
            result = _cleanup_label(lbl, keep, server)
            total_versions += len(result.get("versions_removed", []))
            total_storage  += result.get("storage_files_removed", 0)
        except requests.RequestException as e:
            _err(f"[{lbl}]: {e}")

    if len(labels) > 1:
        console.print()
        console.print(Panel(
            f"[{DIM}]Labels processados[/{DIM}]  [{TEXT}]{len(labels)}[/{TEXT}]\n"
            f"[{RED}]Versoes removidas[/{RED}]   [{RED}]{total_versions}[/{RED}]\n"
            f"[{DIM}]Arquivos storage[/{DIM}]   [{DIM}]{total_storage}[/{DIM}]",
            title=f"[bold {AMBER}]◈  cleanup[/bold {AMBER}]",
            border_style=AMBER,
            padding=(0, 2),
        ))
    console.print()


# -- Delete label -------------------------------------------------------------
def delete_label(label, server=DEFAULT_SERVER, force=False):
    if not force:
        console.print(
            f"\n  [{RED}]Atenção:[/{RED}] isso remove o label [{AMBER}][{label}][/{AMBER}] "
            f"e [{RED}]TODAS[/{RED}] as suas versoes.\n"
        )
        confirm = input("  Confirmar? [s/N] ")
        if confirm.strip().lower() not in ("s", "sim"):
            _info("Operacao cancelada.")
            return
    try:
        delete_label_api(server, label)
        _ok(f"Label [{label}] excluido. Limpeza do storage iniciada em background.")
    except requests.RequestException as e:
        _err(f"Erro ao excluir label: {e}")
        sys.exit(1)


# -- Cleanup orphans ----------------------------------------------------------
def cleanup_orphans(server=DEFAULT_SERVER):
    _info("Iniciando limpeza de arquivos orfaos...")
    try:
        result = force_cleanup_orphans_api(server)
        files  = result.get("files_removed", 0)
        freed  = result.get("bytes_freed", 0)
        _ok(f"Limpeza concluida: {files} arquivo(s) removido(s), {fmt_size(freed)} liberados")
    except requests.RequestException as e:
        _err(f"Erro na limpeza: {e}")
        sys.exit(1)


# -- Re-replication -----------------------------------------------------------
def rereplicate(server=DEFAULT_SERVER):
    _info("Iniciando re-replicacao de conteudos sub-replicados...")
    try:
        result     = force_rereplicate_api(server)
        replicated = result.get("replicated", 0)
        skipped    = result.get("skipped", 0)
        target     = result.get("target_copies", 0)
        _ok(
            f"Re-replicacao concluida: {replicated} arquivo(s) replicado(s), "
            f"{skipped} pulado(s) — alvo: {target} copia(s)"
        )
        if skipped:
            _warn(
                f"{skipped} arquivo(s) nao puderam ser replicados (volume degraded). "
                f"Recupere o disco e execute novamente."
            )
    except requests.RequestException as e:
        _err(f"Erro na re-replicacao: {e}")
        sys.exit(1)


# -- Reconcile replication ----------------------------------------------------
def reconcile_replication(server=DEFAULT_SERVER):
    _info("Reconciliando replicacao (limpeza de excesso + re-replicacao de faltantes)...")
    try:
        result     = force_reconcile_api(server)
        replicated = result.get("replicated", 0)
        skipped    = result.get("skipped", 0)
        cleaned    = result.get("cleaned", 0)
        target     = result.get("target_copies", 0)
        _ok(
            f"Reconciliacao concluida: {replicated} replicado(s), "
            f"{cleaned} copia(s) excedente(s) removida(s), "
            f"{skipped} pulado(s) — alvo: {target} copia(s)"
        )
        if skipped:
            _warn(
                f"{skipped} arquivo(s) sem fonte acessivel (volume degraded). "
                f"Recupere o disco e execute novamente."
            )
    except requests.RequestException as e:
        _err(f"Erro na reconciliacao: {e}")
        sys.exit(1)


# -- Encrypt existing ---------------------------------------------------------
def encrypt_existing(server=DEFAULT_SERVER):
    _info("Iniciando criptografia de arquivos existentes...")
    try:
        import time
        t0        = time.monotonic()
        result    = force_encrypt_existing_api(server)
        elapsed   = time.monotonic() - t0
        encrypted = result.get("files_encrypted", 0)
        processed = result.get("bytes_processed", 0)
        skipped   = result.get("skipped", 0)
        console.print(Panel(
            f"[{GREEN}]Criptografados[/{GREEN}]  [{GREEN}]{encrypted}[/{GREEN}]\n"
            f"[{DIM}]Processados[/{DIM}]     [{TEXT}]{fmt_size(processed)}[/{TEXT}]\n"
            f"[{DIM}]Ja cifrados[/{DIM}]     [{DIM}]{skipped} (pulados)[/{DIM}]\n"
            f"[{DIM}]Tempo[/{DIM}]           [{TEXT}]{elapsed:.1f}s[/{TEXT}]",
            title=f"[bold {GREEN}]◈  encrypt-existing concluido[/bold {GREEN}]",
            border_style=GREEN,
            padding=(0, 2),
        ))
        if skipped:
            _warn(
                f"{skipped} arquivo(s) pulado(s) — volume degraded ou erro de I/O. "
                f"Recupere o disco e execute novamente."
            )
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            _err("Servidor nao suporta criptografia (requer NestVault v3.1+)")
        elif e.response is not None and e.response.status_code == 400:
            _err("Criptografia nao habilitada no servidor (ENCRYPTION_ENABLED=false)")
        else:
            _err(str(e))
        sys.exit(1)
    except requests.RequestException as e:
        _err(f"Erro na criptografia: {e}")
        sys.exit(1)


# -- CLI ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description=f"NestVault {VERSION} — backup com deduplicacao e criptografia",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # backup
    pb = sub.add_parser("backup", help="Cria nova versao de backup")
    pb.add_argument("directory")
    pb.add_argument("--label", required=True)
    pb.add_argument("--server", default=DEFAULT_SERVER)
    pb.add_argument("--prefix", default=None)
    pb.add_argument("--client", default=None)
    pb.add_argument("--exclude", nargs="+", default=[])
    pb.add_argument("--workers", type=int, default=4)
    pb.add_argument("--hash-workers", type=int, default=None, dest="hash_workers",
                    help="Processos paralelos para calcular SHA256 (padrao: os.cpu_count())")
    pb.add_argument("--batch-size", type=int, default=100, dest="batch_size",
                    help="Arquivos por request no /check/batch (padrao: 100)")
    pb.add_argument("--accumulate", action="store_true",
                    help="Herda arquivos ausentes da versao anterior (ideal para galerias)")
    pb.add_argument("--dry-run", action="store_true")
    pb.add_argument("--verbose", action="store_true",
                    help="Mostra logs de arquivos cacheados e ignorados")

    # backups
    pl = sub.add_parser("backups", help="Lista todos os backups")
    pl.add_argument("--server", default=DEFAULT_SERVER)
    pl.add_argument("--client", default=None)

    # versions
    pv = sub.add_parser("versions", help="Lista versoes de um backup")
    pv.add_argument("--label", required=True)
    pv.add_argument("--server", default=DEFAULT_SERVER)

    # restore
    pr = sub.add_parser("restore", help="Restaura uma versao especifica")
    pr.add_argument("destination")
    pr.add_argument("--label", required=True)
    pr.add_argument("--version", required=True, dest="version_key",
                    help="Chave da versao (ex: 2026-04-25T10:42:31)")
    pr.add_argument("--server", default=DEFAULT_SERVER)
    pr.add_argument("--prefix", default=None)
    pr.add_argument("--overwrite", action="store_true")
    pr.add_argument("--dry-run", action="store_true")
    pr.add_argument("--exclude", nargs="+", default=[],
                    help="Nomes de diretório a ignorar durante o restore")

    # cleanup
    pc = sub.add_parser("cleanup", help="Remove versoes antigas de um ou todos os backups")
    grp = pc.add_mutually_exclusive_group()
    grp.add_argument("--label", default=None, help="Label especifico a limpar")
    grp.add_argument("--all", action="store_true", help="Limpar todos os labels")
    pc.add_argument("--keep", type=int, default=5,
                    help="Versoes a manter por label (padrao: 5)")
    pc.add_argument("--server", default=DEFAULT_SERVER)

    # delete-label
    pdl = sub.add_parser("delete-label", help="Exclui um label e todas as suas versoes")
    pdl.add_argument("--label", required=True)
    pdl.add_argument("--server", default=DEFAULT_SERVER)
    pdl.add_argument("--force", action="store_true", help="Sem confirmacao interativa")

    # cleanup-orphans
    pco = sub.add_parser("cleanup-orphans", help="Forca limpeza de arquivos sem backup associado")
    pco.add_argument("--server", default=DEFAULT_SERVER)

    # rereplicate
    prr = sub.add_parser(
        "rereplicate",
        help="Re-replica arquivos sub-replicados (apos adicionar disco ou alterar REPLICATION_FACTOR)",
    )
    prr.add_argument("--server", default=DEFAULT_SERVER)

    # reconcile-replication
    prc = sub.add_parser(
        "reconcile-replication",
        help="Reconcilia replicacao: remove excesso ou replica faltantes conforme REPLICATION_FACTOR",
    )
    prc.add_argument("--server", default=DEFAULT_SERVER)

    # encrypt-existing
    pee = sub.add_parser(
        "encrypt-existing",
        help="Cifra arquivos existentes no storage (requer ENCRYPTION_ENABLED=true no servidor)",
    )
    pee.add_argument("--server", default=DEFAULT_SERVER)

    args = parser.parse_args()

    if hasattr(args, "label") and args.label is not None and not args.label.strip():
        parser.error("--label nao pode ser vazio")

    if hasattr(args, "server") and not (args.server or "").startswith(("http://", "https://")):
        parser.error(f"--server invalido: {args.server!r}. Use http://host:porta ou https://host:porta")

    _header()

    if args.command == "backup":
        _kv("Comando", "BACKUP")
        backup_directory(
            args.directory, args.label, args.server,
            args.dry_run, args.prefix, args.exclude,
            args.client, args.workers, args.verbose, args.batch_size,
            args.hash_workers, accumulate=args.accumulate,
        )

    elif args.command == "backups":
        list_backups(args.server, args.client)

    elif args.command == "versions":
        list_versions(args.label, args.server)

    elif args.command == "restore":
        _kv("Comando", "RESTORE")
        restore(
            args.destination, args.label, args.version_key,
            args.server, args.prefix, args.dry_run, args.overwrite,
            exclude=args.exclude,
        )

    elif args.command == "cleanup":
        _kv("Comando", "CLEANUP")
        if getattr(args, "all", False):
            cleanup(label=None, keep=args.keep, server=args.server)
        elif args.label:
            cleanup(label=args.label, keep=args.keep, server=args.server)
        else:
            _err("Informe --label <nome> ou --all")
            sys.exit(1)

    elif args.command == "delete-label":
        _kv("Comando", "DELETE-LABEL")
        delete_label(args.label, args.server, args.force)

    elif args.command == "cleanup-orphans":
        _kv("Comando", "CLEANUP-ORPHANS")
        cleanup_orphans(args.server)

    elif args.command == "rereplicate":
        _kv("Comando", "REREPLICATE")
        rereplicate(args.server)

    elif args.command == "reconcile-replication":
        _kv("Comando", "RECONCILE-REPLICATION")
        reconcile_replication(args.server)

    elif args.command == "encrypt-existing":
        _kv("Comando", "ENCRYPT-EXISTING")
        encrypt_existing(args.server)


if __name__ == "__main__":
    import multiprocessing
    # Binarios congelados (PyInstaller) no Linux precisam de 'spawn' explicito para que
    # freeze_support() intercepte corretamente os workers do ProcessPoolExecutor.
    # Sem isso, workers re-executam o binario inteiro e chamam main() com modulo
    # parcialmente inicializado, causando server="" e MissingSchema.
    if getattr(sys, "frozen", False) and sys.platform == "linux":
        multiprocessing.set_start_method("spawn", force=True)
    multiprocessing.freeze_support()
    main()
