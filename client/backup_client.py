"""
Backup Client
Cada backup e identificado por um label unico. Backups diferentes sao
completamente isolados no servidor — arquivos de um nunca interferem no outro.

Uso:
    python backup_client.py backup ~/docs --label "meu-notebook" --server http://192.168.1.100:8000
    python backup_client.py backups --server http://192.168.1.100:8000
    python backup_client.py restore /tmp/restore --label "meu-notebook" --server http://192.168.1.100:8000
"""

import os
import sys
import hashlib
import argparse
import logging
import base64
import socket
from pathlib import Path
from typing import Optional

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from tqdm import tqdm
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor

# -- Config -------------------------------------------------------------------
DEFAULT_SERVER = "http://localhost:8000"

def _load_api_key() -> str:
    key = os.getenv("BACKUP_API_KEY", "")
    try:
        key.encode("latin-1")
    except UnicodeEncodeError:
        print(
            "ERRO: BACKUP_API_KEY contem caracteres invalidos (ex: aspas curvas “ ”)."
            "\nCopie a chave novamente usando apenas caracteres ASCII simples.",
            file=__import__("sys").stderr,
        )
        __import__("sys").exit(1)
    return key

API_KEY = _load_api_key()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backup_client")


# -- Helpers ------------------------------------------------------------------
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def build_headers(extra: Optional[dict] = None) -> dict:
    headers = {"X-API-Key": API_KEY}
    if extra:
        headers.update(extra)
    return headers


def encode_path(path: str) -> str:
    return base64.b64encode(path.encode("utf-8")).decode("ascii")


def fmt_size(size: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


# -- API calls ----------------------------------------------------------------
def ensure_backup(server: str, label: str, client_name: str, prefix: Optional[str]) -> dict:
    """Cria o backup se nao existir, ou retorna o existente (idempotente)."""
    resp = requests.post(
        f"{server}/backups",
        json={"label": label, "client_name": client_name, "prefix": prefix},
        headers=build_headers(),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def check_file(server: str, backup_label: str, original_path: str, sha256: str, size: int, mtime: float) -> dict:
    resp = requests.post(
        f"{server}/check",
        json={
            "backup_label": backup_label,
            "original_path": original_path,
            "sha256": sha256,
            "size": size,
            "mtime": mtime,
        },
        headers=build_headers(),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def upload_file(server: str, local_path: Path, backup_label: str, original_path: str, mtime: float) -> dict:
    file_size = local_path.stat().st_size
    filename  = local_path.name

    with open(local_path, "rb") as f:
        encoder = MultipartEncoder(fields={"file": (filename, f, "application/octet-stream")})

        bar = tqdm(
            total=file_size, unit="B", unit_scale=True, unit_divisor=1024,
            desc=f"  {filename[:40]}", leave=False,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{rate_fmt}]",
        )

        def _progress(monitor):
            bar.update(monitor.bytes_read - bar.n)

        monitor = MultipartEncoderMonitor(encoder, _progress)

        resp = requests.post(
            f"{server}/upload",
            data=monitor,
            headers=build_headers({
                "X-Backup-Label":   backup_label,
                "X-Original-Path":  encode_path(original_path),
                "X-Mtime":          str(mtime),
                "Content-Type":     monitor.content_type,
            }),
            timeout=120,
        )
        bar.close()

    resp.raise_for_status()
    return resp.json()


def sync_with_server(server: str, backup_label: str, existing_paths: list) -> dict:
    resp = requests.post(
        f"{server}/sync",
        json={"backup_label": backup_label, "existing_paths": existing_paths},
        headers=build_headers(),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# -- Backup -------------------------------------------------------------------
def backup_directory(
    directory: str,
    label: str,
    server: str = DEFAULT_SERVER,
    dry_run: bool = False,
    path_prefix: Optional[str] = None,
    exclude: Optional[list] = None,
    client_name: Optional[str] = None,
    workers: int = 4,
):
    root = Path(directory).resolve()
    if not root.exists():
        log.error(f"Diretorio nao encontrado: {root}")
        sys.exit(1)

    client_name = client_name or socket.gethostname()

    if not dry_run:
        result = ensure_backup(server, label, client_name, path_prefix)
        action = "Criado" if result["created"] else "Encontrado"
        log.info(f"Backup    : [{label}]  {action}")

    stats = {"checked": 0, "uploaded": 0, "skipped": 0, "errors": 0}
    stats_lock = threading.Lock()
    all_paths: list[str] = []
    all_paths_lock = threading.Lock()

    # Coleta todos os arquivos elegíveis primeiro
    pending = []
    # Arquivos ignorados por padrao
    IGNORED_NAMES = {'.DS_Store', 'Thumbs.db', 'desktop.ini'}

    for file_path in sorted(root.rglob("*")):
        if not file_path.is_file():
            continue

        if file_path.name in IGNORED_NAMES:
            log.debug(f"SKIP   {file_path.name}  [ignorado por padrao]")
            continue
        excluded = False
        for ex in (exclude or []):
            try:
                file_path.relative_to((root / ex).resolve())
                excluded = True
                break
            except ValueError:
                pass
        if excluded:
            continue
        original_path = str(file_path) if not path_prefix else str(
            Path(path_prefix) / file_path.relative_to(root)
        )
        pending.append((file_path, original_path))

    with stats_lock:
        stats["checked"] = len(pending)
    with all_paths_lock:
        all_paths.extend(p[1] for p in pending)

    total_files = len(pending)
    done_count  = 0
    done_lock   = threading.Lock()

    # Barra de progresso geral
    overall = tqdm(
        total=total_files,
        unit="arq",
        desc="Progresso",
        bar_format="  {desc}: {percentage:5.1f}%  {n_fmt}/{total_fmt}  [{elapsed}<{remaining}]",
        position=0,
        leave=True,
    )

    def process_file(file_path: Path, original_path: str):
        nonlocal done_count
        stat  = file_path.stat()
        size  = stat.st_size
        mtime = stat.st_mtime
        try:
            sha256 = sha256_file(file_path)
            check  = check_file(server, label, original_path, sha256, size, mtime)

            if not check["needs_upload"]:
                log.info(f"SKIP   {original_path}")
                with stats_lock:
                    stats["skipped"] += 1
            else:
                log.info(f"UPLOAD {original_path}  ({fmt_size(size)})  [{check['reason']}]")

                if dry_run:
                    log.info("  [dry-run] upload nao realizado")
                else:
                    upload_file(server, file_path, label, original_path, mtime)
                    with stats_lock:
                        stats["uploaded"] += 1

        except requests.RequestException as e:
            log.error(f"  ERRO em {original_path}: {e}")
            with stats_lock:
                stats["errors"] += 1
        finally:
            with done_lock:
                done_count += 1
                overall.update(1)
                overall.set_description(
                    f"Progresso  ✓{stats['uploaded']} ↷{stats['skipped']} ✗{stats['errors']}"
                )

    effective_workers = 1 if dry_run else workers
    if effective_workers > 1:
        log.info(f"Workers   : {effective_workers} threads paralelas")

    with ThreadPoolExecutor(max_workers=effective_workers) as pool:
        futures = {pool.submit(process_file, fp, op): op for fp, op in pending}
        for future in as_completed(futures):
            exc = future.exception()
            if exc:
                log.error(f"  ERRO inesperado: {exc}")
                with stats_lock:
                    stats["errors"] += 1

    overall.close()

    if not dry_run:
        try:
            result = sync_with_server(server, label, all_paths)
            removed = result.get("deleted_count", 0)
            stats["removed"] = removed
            if removed:
                log.info(f"SYNC   {removed} arquivo(s) removido(s) de [{label}]:")
                for p in result.get("deleted", []):
                    log.info(f"  - {p}")
            else:
                log.info(f"SYNC   Nenhum arquivo removido de [{label}]")
        except requests.RequestException as e:
            log.error(f"SYNC   Erro: {e}")
    else:
        log.info("SYNC   [dry-run] nao executado")

    log.info("")
    log.info("=" * 50)
    log.info(f"  Backup      : [{label}]")
    log.info(f"  Verificados : {stats['checked']}")
    log.info(f"  Enviados    : {stats['uploaded']}")
    log.info(f"  Ignorados   : {stats['skipped']}")
    log.info(f"  Erros       : {stats['errors']}")
    log.info(f"  Removidos   : {stats.get('removed', 0)}")
    log.info("=" * 50)

    return stats


# -- List backups -------------------------------------------------------------
def list_backups(server: str = DEFAULT_SERVER, client_name: Optional[str] = None):
    params = {}
    if client_name:
        params["client_name"] = client_name

    resp = requests.get(f"{server}/backups", headers=build_headers(), params=params, timeout=10)
    resp.raise_for_status()
    backups = resp.json()

    if not backups:
        log.info("Nenhum backup encontrado.")
        return

    log.info("")
    log.info(f"{'LABEL':30}  {'CLIENTE':20}  {'STATUS':8}  {'ARQUIVOS':>8}  {'TAMANHO':>10}  ULTIMO RUN")
    log.info("-" * 105)
    for b in backups:
        label      = b["label"][:30]
        client     = (b.get("client_name") or "-")[:20]
        status     = b["status"]
        files      = b["file_count"]
        size       = fmt_size(b["total_size_bytes"])
        last_run   = (b.get("last_run_at") or b["created_at"])[:19]
        log.info(f"{label:30}  {client:20}  {status:8}  {files:>8}  {size:>10}  {last_run}")
    log.info("")


# -- Restore ------------------------------------------------------------------
def restore(
    destination: str,
    label: str,
    server: str = DEFAULT_SERVER,
    path_prefix: Optional[str] = None,
    dry_run: bool = False,
    overwrite: bool = False,
):
    dest_root = Path(destination)
    dest_root.mkdir(parents=True, exist_ok=True)

    log.info(f"Backup      : [{label}]")
    log.info(f"Destino     : {dest_root}")

    params = {"backup_label": label}
    if path_prefix:
        params["path_prefix"] = path_prefix

    try:
        resp = requests.get(f"{server}/files", headers=build_headers(), params=params, timeout=10)
        resp.raise_for_status()
        files = resp.json()
    except requests.RequestException as e:
        log.error(f"Erro ao buscar arquivos de [{label}]: {e}")
        sys.exit(1)

    if not files:
        log.info("Nenhum arquivo encontrado neste backup.")
        return

    log.info(f"{len(files)} arquivo(s) no backup [{label}].")
    stats = {"restored": 0, "skipped": 0, "errors": 0}

    for record in files:
        file_id       = record["id"]
        original_path = record["original_path"]
        sha256        = record["sha256"]
        size          = record["size"]

        if path_prefix and original_path.startswith(path_prefix):
            relative = original_path[len(path_prefix):].lstrip("/")
        else:
            relative = original_path.lstrip("/")

        dest_file = dest_root / relative

        if dest_file.exists() and not overwrite:
            if sha256_file(dest_file) == sha256:
                log.info(f"SKIP   {relative}  [identico]")
            else:
                log.info(f"DIFF   {relative}  [modificado localmente — use --overwrite]")
            stats["skipped"] += 1
            continue

        log.info(f"DOWN   {relative}  ({fmt_size(size)})")

        if dry_run:
            log.info("  [dry-run] download nao realizado")
            continue

        try:
            resp = requests.get(
                f"{server}/files/{file_id}/download",
                headers=build_headers(), stream=True, timeout=120,
            )
            resp.raise_for_status()

            dest_file.parent.mkdir(parents=True, exist_ok=True)
            total = int(resp.headers.get("Content-Length", size))
            bar = tqdm(
                total=total, unit="B", unit_scale=True, unit_divisor=1024,
                desc=f"  {Path(relative).name[:40]}", leave=False,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{rate_fmt}]",
            )
            with open(dest_file, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
                    bar.update(len(chunk))
            bar.close()

            if sha256_file(dest_file) != sha256:
                log.error(f"  INTEGRIDADE FALHOU — {relative} removido")
                dest_file.unlink()
                stats["errors"] += 1
                continue

            stats["restored"] += 1

        except requests.RequestException as e:
            log.error(f"  ERRO ao baixar {relative}: {e}")
            stats["errors"] += 1

    log.info("")
    log.info("=" * 50)
    log.info(f"  Backup      : [{label}]")
    log.info(f"  Restaurados : {stats['restored']}")
    log.info(f"  Ignorados   : {stats['skipped']}")
    log.info(f"  Erros       : {stats['errors']}")
    log.info("=" * 50)

    return stats


# -- CLI ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Cliente de backup para Raspberry Pi")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # -- backup ---------------------------------------------------------------
    p_backup = subparsers.add_parser("backup", help="Envia arquivos para o servidor")
    p_backup.add_argument("directory", help="Diretorio a fazer backup")
    p_backup.add_argument("--label", required=True, help="Identificador unico do backup (ex: notebook-joao)")
    p_backup.add_argument("--server", default=DEFAULT_SERVER)
    p_backup.add_argument("--prefix", default=None, help="Prefixo de path no servidor")
    p_backup.add_argument("--client", default=None, help="Nome do cliente (padrao: hostname)")
    p_backup.add_argument("--exclude", metavar="PASTA", nargs="+", default=[], help="Subpastas a ignorar")
    p_backup.add_argument("--workers", type=int, default=4, help="Numero de uploads paralelos (padrao: 4)")
    p_backup.add_argument("--dry-run", action="store_true", help="Apenas verifica, nao envia")

    # -- backups --------------------------------------------------------------
    p_list = subparsers.add_parser("backups", help="Lista todos os backups disponiveis")
    p_list.add_argument("--server", default=DEFAULT_SERVER)
    p_list.add_argument("--client", default=None, help="Filtrar por nome do cliente")

    # -- restore --------------------------------------------------------------
    p_restore = subparsers.add_parser("restore", help="Restaura arquivos de um backup")
    p_restore.add_argument("destination", help="Pasta de destino")
    p_restore.add_argument("--label", required=True, help="Identificador do backup a restaurar")
    p_restore.add_argument("--server", default=DEFAULT_SERVER)
    p_restore.add_argument("--prefix", default=None, help="Restaurar apenas arquivos com esse prefixo")
    p_restore.add_argument("--overwrite", action="store_true", help="Sobrescreve arquivos existentes")
    p_restore.add_argument("--dry-run", action="store_true", help="Apenas lista, nao baixa")

    args = parser.parse_args()

    if args.command == "backup":
        log.info(f"Comando  : BACKUP")
        log.info(f"Servidor : {args.server}")
        log.info(f"Label    : [{args.label}]")
        log.info(f"Diretorio: {args.directory}")
        if args.exclude:
            log.info(f"Excluindo: {', '.join(args.exclude)}")
        if args.dry_run:
            log.info("Modo     : DRY RUN")
        log.info(f"Workers  : {args.workers}")
        backup_directory(
            directory=args.directory,
            label=args.label,
            server=args.server,
            dry_run=args.dry_run,
            path_prefix=args.prefix,
            exclude=args.exclude,
            client_name=args.client,
            workers=args.workers,
        )

    elif args.command == "backups":
        log.info(f"Comando  : BACKUPS")
        log.info(f"Servidor : {args.server}")
        list_backups(server=args.server, client_name=args.client)

    elif args.command == "restore":
        log.info(f"Comando  : RESTORE")
        log.info(f"Servidor : {args.server}")
        log.info(f"Label    : [{args.label}]")
        log.info(f"Destino  : {args.destination}")
        if args.dry_run:
            log.info("Modo     : DRY RUN")
        restore(
            destination=args.destination,
            label=args.label,
            server=args.server,
            path_prefix=args.prefix,
            dry_run=args.dry_run,
            overwrite=args.overwrite,
        )


if __name__ == "__main__":
    main()