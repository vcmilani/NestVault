"""
NestVault  v2.2
Cada execucao de backup cria uma nova versao dentro do label.
Conteudo identico e armazenado uma unica vez no servidor (deduplicacao por sha256).

Uso:
    python backup_client.py backup ~/docs --label "notebook" --server http://192.168.1.100:8000
    python backup_client.py versions --label "notebook" --server http://192.168.1.100:8000
    python backup_client.py restore /tmp/r --label "notebook" --version "2026-04-25T10:42:31"
    python backup_client.py cleanup --label "notebook" --keep 5
    python backup_client.py backups --server http://192.168.1.100:8000
"""

import os, sys, hashlib, argparse, logging, base64, socket, threading
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

import requests
from tqdm import tqdm

# -- Config -------------------------------------------------------------------
DEFAULT_SERVER = "http://localhost:8000"

def _load_api_key() -> str:
    key = os.getenv("BACKUP_API_KEY", "")
    try:
        key.encode("latin-1")
    except UnicodeEncodeError:
        print(
            "ERRO: BACKUP_API_KEY contem caracteres invalidos (ex: aspas curvas).\n"
            "Copie a chave novamente usando apenas caracteres ASCII simples.",
            file=sys.stderr,
        )
        sys.exit(1)
    return key

API_KEY = _load_api_key()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backup")

IGNORED_NAMES = {".DS_Store", "Thumbs.db", "desktop.ini"}


# -- Helpers ------------------------------------------------------------------
# Buffer maior reduz overhead de syscalls em arquivos grandes
CHUNK_SIZE = 1024 * 1024  # 1 MB

def sha256_file(path: Path) -> str:
    """Calcula sha256 lendo o arquivo em chunks. Single-pass, baixa memoria."""
    h = hashlib.sha256()
    with open(path, "rb", buffering=0) as f:
        while chunk := f.read(CHUNK_SIZE):
            h.update(chunk)
    return h.hexdigest()


def _hash_item(item: tuple) -> tuple:
    """Funcao top-level para hashing em ProcessPoolExecutor (necessario para pickle)."""
    fp, op, mtime, size = item
    return op, sha256_file(fp), size, mtime

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


# -- API calls ----------------------------------------------------------------
def ensure_backup(server, label, client_name, prefix):
    r = _session.post(f"{server}/backups",
                      json={"label": label, "client_name": client_name, "prefix": prefix},
                      headers=build_headers(), timeout=10)
    r.raise_for_status(); return r.json()

def create_version(server, label, version_key):
    r = _session.post(f"{server}/backups/{label}/versions",
                      json={"version_key": version_key},
                      headers=build_headers(), timeout=10)
    r.raise_for_status(); return r.json()

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
    r.raise_for_status(); return r.json()

# Sessao HTTP reutilizada — evita TCP handshake a cada request
_session = requests.Session()
_session.headers.update({"Connection": "keep-alive"})

def register_file(server, label, version_key, original_path, mtime, sha256):
    """Registra arquivo cujo conteudo ja existe no storage (sem upload)."""
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
    r.raise_for_status(); return r.json()

class _ProgressReader:
    """
    Wrapper leve em torno de um arquivo aberto.
    Intercepta read() para atualizar a barra de progresso.
    Sem overhead de encoding MIME — stream binario puro.
    """
    def __init__(self, path: Path, bar: tqdm):
        self._f   = open(path, "rb", buffering=0)
        self._bar = bar

    def read(self, size: int = -1) -> bytes:
        chunk = self._f.read(size)
        if chunk:
            self._bar.update(len(chunk))
        return chunk

    def close(self):
        self._f.close()


def upload_file(server, local_path: Path, label, version_key, original_path, mtime):
    """
    Upload de arquivo como stream binario puro (sem multipart).
    O body da request E o arquivo diretamente — sem encoding MIME.
    A barra de progresso e atualizada via _ProgressReader com overhead minimo.
    """
    file_size = local_path.stat().st_size
    bar = tqdm(
        total=file_size, unit="B", unit_scale=True, unit_divisor=1024,
        desc=f"  {local_path.name[:40]}", leave=False,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{rate_fmt}]",
    )
    reader = _ProgressReader(local_path, bar)
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
            timeout=120,
        )
    finally:
        bar.close()
        reader.close()
    r.raise_for_status()
    return r.json()


def check_batch(server, label, version_key, items: list[dict]) -> list[dict]:
    """Verifica N arquivos em uma unica request. items: lista de dicts com original_path, sha256, size, mtime."""
    r = _session.post(
        f"{server}/check/batch",
        json={"backup_label": label, "version_key": version_key, "files": items},
        headers=build_headers(),
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def absorb_version(server: str, label: str, version_key: str, source_version_key: str) -> dict:
    """Herda arquivos ausentes da versao fonte para a versao destino (modo acumulativo)."""
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


def sync_version(server, label, version_key, existing_paths):
    r = _session.post(f"{server}/sync",
                      json={"backup_label": label, "version_key": version_key,
                            "existing_paths": existing_paths},
                      headers=build_headers(), timeout=30)
    r.raise_for_status(); return r.json()


def _fetch_prev_cache(server, label) -> tuple[Optional[str], dict]:
    """
    Busca arquivos da última versão 'done' para evitar recalcular sha256
    de arquivos com mtime+size inalterados.
    Retorna (version_key, {original_path: FileInfo}) ou (None, {}) em caso de erro/ausência.
    """
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
    root = Path(directory).resolve()
    if not root.exists():
        log.error(f"Diretorio nao encontrado: {root}"); sys.exit(1)

    client_name = client_name or socket.gethostname()
    version_key = now_key()

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not dry_run:
        ensure_backup(server, label, client_name, path_prefix)
        create_version(server, label, version_key)
        log.info(f"Backup    : [{label}]")
        log.info(f"Versao    : {version_key}")

    # Cache da versão anterior para evitar leitura de disco em arquivos inalterados
    prev_done_key, prev_cache = _fetch_prev_cache(server, label) if not dry_run else (None, {})
    if prev_cache:
        log.info(f"Cache     : {len(prev_cache)} arquivo(s) da versao anterior carregados")

    # Coleta arquivos
    pending = []
    for fp in sorted(root.rglob("*")):
        if not fp.is_file() or fp.name in IGNORED_NAMES:
            continue
        if any(_is_excluded(fp, root, ex) for ex in (exclude or [])):
            continue
        op = str(fp) if not path_prefix else str(Path(path_prefix) / fp.relative_to(root))
        pending.append((fp, op))

    total = len(pending)
    log.info(f"Arquivos  : {total} encontrados")

    stats = {"uploaded": 0, "registered": 0, "fast": 0, "skipped": 0, "errors": 0}
    lock  = threading.Lock()
    all_paths = [op for _, op in pending]

    use_batch = not dry_run and _server_supports_batch(server)
    if use_batch:
        log.info(f"Modo      : batch ({batch_size} arquivos/request)")
    effective = 1 if dry_run else workers
    effective_hash = 1 if dry_run else (hash_workers or os.cpu_count() or 4)
    if effective > 1:
        log.info(f"Workers   : {effective} threads (upload)  /  {effective_hash} processos (hash)")

    overall = tqdm(
        total=total, unit="arq", desc="Progresso", position=0, leave=True,
        bar_format="  {desc}: {percentage:5.1f}%  {n_fmt}/{total_fmt}  [{elapsed}<{remaining}]",
    )

    def _update_bar():
        overall.update(1)
        with lock:
            overall.set_description(
                f"Progresso  ↑{stats['uploaded']} ⊕{stats['registered']} "
                f"⚡{stats['fast']} ✗{stats['errors']}"
            )

    if use_batch:
        # ------------------------------------------------------------------ #
        # Fase 1 — cache hits + hashing paralelo + verificação em lote        #
        # ------------------------------------------------------------------ #
        fast_files   = []   # (fp, op, mtime, sha256) — cache hit
        pending_hash = []   # (fp, op, mtime, size) — precisam de sha256

        for fp, op in pending:
            stat  = fp.stat()
            size  = stat.st_size
            mtime = stat.st_mtime
            cached = prev_cache.get(op)
            if cached and cached["mtime"] == mtime and cached["size"] == size:
                fast_files.append((fp, op, mtime, cached["sha256"]))
            else:
                pending_hash.append((fp, op, mtime, size))

        # Hashing em paralelo com ProcessPoolExecutor (bypassa GIL para SHA256 CPU-bound)
        hashed: dict[str, tuple[str, int, float]] = {}  # op -> (sha256, size, mtime)
        if pending_hash:
            log.info(f"Hashing   : {len(pending_hash)} arquivo(s) para calcular SHA256 "
                     f"({len(fast_files)} cache hits, {effective_hash} processos)")
            with ProcessPoolExecutor(max_workers=effective_hash) as pool:
                for op, sha256, size, mtime in pool.map(_hash_item, pending_hash, chunksize=max(1, len(pending_hash) // (effective_hash * 4))):
                    hashed[op] = (sha256, size, mtime)
            log.info(f"Hashing   : concluído")

        # Verificação em lote
        # action_map: op -> ("skip"|"register"|"upload", sha256, size, mtime, fp)
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
                log.error(f"  ERRO no batch check: {e}")
                for item in batch:
                    op = item["original_path"]
                    sha256, size, mtime = hashed[op]
                    action_map[op] = ("upload", sha256, size, mtime, fp_map[op])
                    with lock: stats["errors"] += 1

        # ------------------------------------------------------------------ #
        # Fase 2 — uploads e registers em paralelo                            #
        # ------------------------------------------------------------------ #
        def _do_fast(fp, op, mtime, sha256):
            try:
                log.debug(f"FAST   {op}  [mtime+size ok, sem leitura de disco]")
                register_file(server, label, version_key, op, mtime, sha256)
                with lock: stats["fast"] += 1
            except requests.RequestException as e:
                log.error(f"  ERRO em {op}: {e}")
                with lock: stats["errors"] += 1
            finally:
                _update_bar()

        def _do_action(op, action, sha256, size, mtime, fp):
            try:
                if action == "skip":
                    log.debug(f"SKIP   {op}")
                    with lock: stats["skipped"] += 1
                elif action == "register":
                    log.info(f"REG    {op}  ({fmt_size(size)})  [conteudo ja no storage]")
                    register_file(server, label, version_key, op, mtime, sha256)
                    with lock: stats["registered"] += 1
                else:
                    log.info(f"UPLOAD {op}  ({fmt_size(size)})")
                    upload_file(server, fp, label, version_key, op, mtime)
                    with lock: stats["uploaded"] += 1
            except requests.RequestException as e:
                log.error(f"  ERRO em {op}: {e}")
                with lock: stats["errors"] += 1
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
                    log.error(f"  ERRO inesperado: {exc}")
                    with lock: stats["errors"] += 1

    else:
        # ------------------------------------------------------------------ #
        # Fallback — comportamento original (check individual por worker)     #
        # ------------------------------------------------------------------ #
        def process(fp: Path, op: str):
            stat  = fp.stat()
            size  = stat.st_size
            mtime = stat.st_mtime
            try:
                cached = prev_cache.get(op)
                if cached and cached["mtime"] == mtime and cached["size"] == size:
                    log.debug(f"FAST   {op}  ({fmt_size(size)})  [mtime+size ok, sem leitura de disco]")
                    if not dry_run:
                        register_file(server, label, version_key, op, mtime, cached["sha256"])
                    with lock: stats["fast"] += 1
                    return

                sha256 = sha256_file(fp)
                check  = check_file(server, label, version_key, op, sha256, size, mtime)

                if not check["needs_upload"]:
                    log.debug(f"SKIP   {op}")
                    with lock: stats["skipped"] += 1
                elif check.get("content_exists"):
                    log.info(f"REG    {op}  ({fmt_size(size)})  [conteudo ja no storage]")
                    if not dry_run:
                        register_file(server, label, version_key, op, mtime, sha256)
                    with lock: stats["registered"] += 1
                else:
                    log.info(f"UPLOAD {op}  ({fmt_size(size)})")
                    if not dry_run:
                        upload_file(server, fp, label, version_key, op, mtime)
                    with lock: stats["uploaded"] += 1

            except requests.RequestException as e:
                log.error(f"  ERRO em {op}: {e}")
                with lock: stats["errors"] += 1
            finally:
                _update_bar()

        with ThreadPoolExecutor(max_workers=effective) as pool:
            futures = {pool.submit(process, fp, op): op for fp, op in pending}
            for future in as_completed(futures):
                if exc := future.exception():
                    log.error(f"  ERRO inesperado: {exc}")
                    with lock: stats["errors"] += 1

    overall.close()

    absorb_result = None
    if not dry_run:
        try:
            sync_version(server, label, version_key, all_paths)
        except requests.RequestException as e:
            log.error(f"SYNC   Erro: {e}")

        status = "failed" if stats["errors"] else "done"
        finish_version(server, label, version_key, status)
        if accumulate and status == "done" and prev_done_key:
            try:
                absorb_result = absorb_version(server, label, version_key, prev_done_key)
                log.info(f"Absorb    : {absorb_result['inherited']} herdado(s), {absorb_result['skipped']} ja presente(s)")
            except requests.RequestException as e:
                log.error(f"Absorb    : ERRO — {e}")

    log.info("")
    log.info("=" * 55)
    log.info(f"  Backup      : [{label}]")
    log.info(f"  Versao      : {version_key}")
    log.info(f"  Verificados : {total}")
    log.info(f"  Enviados    : {stats['uploaded']}")
    log.info(f"  Registrados : {stats['registered']}  (conteudo ja no storage)")
    log.info(f"  Cacheados   : {stats['fast']}  (mtime+size ok, sem leitura de disco)")
    log.info(f"  Ignorados   : {stats['skipped']}  (retomada de backup interrompido)")
    log.info(f"  Erros       : {stats['errors']}")
    if absorb_result is not None:
        log.info(f"  Herdados    : {absorb_result['inherited']}  (modo acumulativo)")
    log.info("=" * 55)
    return stats

def _is_excluded(fp: Path, root: Path, ex: str) -> bool:
    try:
        fp.relative_to((root / ex).resolve()); return True
    except ValueError:
        return False


# -- List backups -------------------------------------------------------------
def list_backups(server=DEFAULT_SERVER, client_name=None):
    params = {}
    if client_name: params["client_name"] = client_name
    r = _session.get(f"{server}/backups", headers=build_headers(), params=params, timeout=10)
    r.raise_for_status()
    backups = r.json()
    if not backups:
        log.info("Nenhum backup encontrado."); return
    log.info("")
    log.info(f"{'LABEL':30}  {'CLIENTE':20}  {'VERSOES':>7}  {'ARQUIVOS':>8}  {'TAMANHO':>10}  ULTIMA VERSAO")
    log.info("-" * 110)
    for b in backups:
        log.info(
            f"{b['label'][:30]:30}  {(b.get('client_name') or '-')[:20]:20}  "
            f"{b['version_count']:>7}  {b['file_count']:>8}  "
            f"{fmt_size(b['total_size_bytes']):>10}  {(b.get('last_version') or '-')[:19]}"
        )
    log.info("")


# -- List versions ------------------------------------------------------------
def list_versions(label, server=DEFAULT_SERVER):
    r = _session.get(f"{server}/backups/{label}/versions",
                     headers=build_headers(), timeout=10)
    r.raise_for_status()
    versions = r.json()
    if not versions:
        log.info(f"Nenhuma versao encontrada em [{label}]."); return
    log.info("")
    log.info(f"Versoes de [{label}]:")
    log.info(f"  {'VERSAO':22}  {'STATUS':8}  {'ARQUIVOS':>8}  {'TAMANHO':>10}  {'DURACAO':>10}")
    log.info("  " + "-" * 70)
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
        log.info(
            f"  {v['version_key']:22}  {v['status']:8}  "
            f"{v['file_count']:>8}  "
            f"{fmt_size(v['total_size_bytes']):>10}  {dur_str:>10}"
        )
    log.info("")


# -- Restore ------------------------------------------------------------------
def restore(destination, label, version_key, server=DEFAULT_SERVER,
            path_prefix=None, dry_run=False, overwrite=False):
    dest_root = Path(destination)
    dest_root.mkdir(parents=True, exist_ok=True)

    log.info(f"Backup      : [{label}]")
    log.info(f"Versao      : {version_key}")
    log.info(f"Destino     : {dest_root}")

    params = {"backup_label": label, "version_key": version_key}
    if path_prefix: params["path_prefix"] = path_prefix

    try:
        r = _session.get(f"{server}/files", headers=build_headers(), params=params, timeout=10)
        r.raise_for_status()
        files = r.json()
    except requests.RequestException as e:
        log.error(f"Erro ao buscar arquivos: {e}"); sys.exit(1)

    if not files:
        log.info("Nenhum arquivo ativo nesta versao."); return

    log.info(f"{len(files)} arquivo(s) encontrado(s).")
    stats = {"restored": 0, "skipped": 0, "errors": 0}

    for record in files:
        file_id       = record["id"]
        original_path = record["original_path"]
        sha256        = record["sha256"]
        size          = record["size"]

        relative = (original_path[len(path_prefix):].lstrip("/")
                    if path_prefix and original_path.startswith(path_prefix)
                    else original_path.lstrip("/"))
        dest_file = dest_root / relative

        if dest_file.exists() and not overwrite:
            match = sha256_file(dest_file) == sha256
            log.info(f"{'SKIP' if match else 'DIFF'}   {relative}  "
                     f"{'[identico]' if match else '[modificado — use --overwrite]'}")
            stats["skipped"] += 1; continue

        log.info(f"DOWN   {relative}  ({fmt_size(size)})")
        if dry_run:
            log.info("  [dry-run] download nao realizado"); continue

        try:
            r = _session.get(f"{server}/files/{file_id}/download",
                             headers=build_headers(), stream=True, timeout=120)
            r.raise_for_status()
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            total = int(r.headers.get("Content-Length", size))
            bar = tqdm(total=total, unit="B", unit_scale=True, unit_divisor=1024,
                       desc=f"  {Path(relative).name[:40]}", leave=False,
                       bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{rate_fmt}]")
            with open(dest_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk); bar.update(len(chunk))
            bar.close()

            if sha256_file(dest_file) != sha256:
                log.error(f"  INTEGRIDADE FALHOU — {relative} removido")
                dest_file.unlink(); stats["errors"] += 1; continue
            stats["restored"] += 1

        except requests.RequestException as e:
            log.error(f"  ERRO: {e}"); stats["errors"] += 1

    log.info("")
    log.info("=" * 55)
    log.info(f"  Versao      : {version_key}")
    log.info(f"  Restaurados : {stats['restored']}")
    log.info(f"  Ignorados   : {stats['skipped']}")
    log.info(f"  Erros       : {stats['errors']}")
    log.info("=" * 55)


# -- Cleanup ------------------------------------------------------------------
def cleanup_label(label, keep, server=DEFAULT_SERVER):
    """Executa cleanup em um label especifico."""
    r = _session.post(
        f"{server}/backups/{label}/cleanup",
        json={"backup_label": label, "keep": keep},
        headers=build_headers(), timeout=30,
    )
    r.raise_for_status()
    result = r.json()
    removed = result.get("versions_removed", [])
    storage = result.get("storage_files_removed", 0)
    log.info(f"  [{label}]  mantidas={result['kept']}  "
             f"removidas={len(removed)}  storage={storage} arquivo(s) apagado(s)")
    for v in removed:
        log.info(f"    - {v}")
    return result


def cleanup(label=None, keep=5, server=DEFAULT_SERVER):
    """
    Executa cleanup em um label especifico ou em TODOS os labels.
    Se label=None, busca todos os labels do servidor e limpa cada um.
    """
    if label:
        labels = [label]
    else:
        r = _session.get(f"{server}/backups", headers=build_headers(), timeout=10)
        r.raise_for_status()
        labels = [b["label"] for b in r.json()]
        if not labels:
            log.info("Nenhum backup encontrado."); return
        log.info(f"Cleanup em todos os labels ({len(labels)} encontrados), keep={keep}")

    total_versions = 0
    total_storage  = 0
    log.info("")
    for lbl in labels:
        try:
            result = cleanup_label(lbl, keep, server)
            total_versions += len(result.get("versions_removed", []))
            total_storage  += result.get("storage_files_removed", 0)
        except requests.RequestException as e:
            log.error(f"  [{lbl}]  ERRO: {e}")

    if len(labels) > 1:
        log.info("")
        log.info("=" * 50)
        log.info(f"  Labels processados : {len(labels)}")
        log.info(f"  Versoes removidas  : {total_versions}")
        log.info(f"  Arquivos do storage: {total_storage}")
        log.info("=" * 50)


# -- Delete label -------------------------------------------------------------
def delete_label(label, server=DEFAULT_SERVER, force=False):
    if not force:
        confirm = input(f"Tem certeza que deseja excluir o label [{label}] e TODAS as suas versoes? [s/N] ")
        if confirm.strip().lower() not in ("s", "sim"):
            log.info("Operacao cancelada.")
            return
    try:
        delete_label_api(server, label)
        log.info(f"Label [{label}] excluido. Limpeza dos arquivos iniciada em background.")
    except requests.RequestException as e:
        log.error(f"Erro ao excluir label: {e}")
        sys.exit(1)


# -- Cleanup orphans ----------------------------------------------------------
def cleanup_orphans(server=DEFAULT_SERVER):
    log.info("Iniciando limpeza forcada de arquivos orfaos...")
    try:
        result = force_cleanup_orphans_api(server)
        files = result.get("files_removed", 0)
        freed = result.get("bytes_freed", 0)
        log.info(f"Limpeza concluida: {files} arquivo(s) removido(s), {fmt_size(freed)} liberados")
    except requests.RequestException as e:
        log.error(f"Erro na limpeza: {e}")
        sys.exit(1)


# -- CLI ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="NestVault v2.5")
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
                    help="Modo acumulativo: herda arquivos ausentes da versao anterior (ideal para galerias de fotos)")
    pb.add_argument("--dry-run", action="store_true")
    pb.add_argument("--verbose", action="store_true", help="Mostra logs de arquivos cacheados e ignorados")

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

    # cleanup
    pc = sub.add_parser("cleanup", help="Remove versoes antigas de um ou todos os backups")
    grp = pc.add_mutually_exclusive_group()
    grp.add_argument("--label", default=None, help="Label especifico a limpar")
    grp.add_argument("--all", action="store_true", help="Limpar todos os labels de uma vez")
    pc.add_argument("--keep", type=int, default=5,
                    help="Quantas versoes manter por label (padrao: 5)")
    pc.add_argument("--server", default=DEFAULT_SERVER)

    # delete-label
    pdl = sub.add_parser("delete-label", help="Exclui um label e todas as suas versoes")
    pdl.add_argument("--label", required=True, help="Label a excluir")
    pdl.add_argument("--server", default=DEFAULT_SERVER)
    pdl.add_argument("--force", action="store_true", help="Sem confirmacao interativa")

    # cleanup-orphans
    pco = sub.add_parser("cleanup-orphans", help="Forca limpeza de arquivos sem backup associado")
    pco.add_argument("--server", default=DEFAULT_SERVER)

    args = parser.parse_args()

    if args.command == "backup":
        log.info(f"Comando  : BACKUP  label=[{args.label}]")
        backup_directory(args.directory, args.label, args.server,
                         args.dry_run, args.prefix, args.exclude,
                         args.client, args.workers, args.verbose, args.batch_size,
                         args.hash_workers, accumulate=args.accumulate)

    elif args.command == "backups":
        list_backups(args.server, args.client)

    elif args.command == "versions":
        list_versions(args.label, args.server)

    elif args.command == "restore":
        log.info(f"Comando  : RESTORE  label=[{args.label}]  versao={args.version_key}")
        restore(args.destination, args.label, args.version_key,
                args.server, args.prefix, args.dry_run, args.overwrite)

    elif args.command == "cleanup":
        if getattr(args, 'all', False):
            log.info(f"Comando  : CLEANUP  todos os labels  keep={args.keep}")
            cleanup(label=None, keep=args.keep, server=args.server)
        elif args.label:
            log.info(f"Comando  : CLEANUP  label=[{args.label}]  keep={args.keep}")
            cleanup(label=args.label, keep=args.keep, server=args.server)
        else:
            log.error("Informe --label <nome> ou --all")
            sys.exit(1)

    elif args.command == "delete-label":
        log.info(f"Comando  : DELETE-LABEL  label=[{args.label}]")
        delete_label(args.label, args.server, args.force)

    elif args.command == "cleanup-orphans":
        log.info("Comando  : CLEANUP-ORPHANS")
        cleanup_orphans(args.server)


if __name__ == "__main__":
    # Necessario para ProcessPoolExecutor no macOS/Windows (spawn-based multiprocessing)
    from multiprocessing import freeze_support
    freeze_support()
    main()