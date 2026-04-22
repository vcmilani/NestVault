"""
Backup Client
Varre um diretorio local e envia apenas os arquivos que foram modificados
ou ainda nao existem no servidor.

Uso:
    python backup_client.py backup ~/documentos --server http://192.168.1.100:8000
    python backup_client.py restore /tmp/restore --server http://192.168.1.100:8000
    python backup_client.py sessions --server http://192.168.1.100:8000
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

import requests
from tqdm import tqdm
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor

# -- Config -------------------------------------------------------------------
DEFAULT_SERVER = "http://localhost:8000"
API_KEY = os.getenv("BACKUP_API_KEY", "change-me-in-production")

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
def create_session(server: str, label: Optional[str], client_name: str, prefix: Optional[str]) -> str:
    resp = requests.post(
        f"{server}/sessions",
        json={"label": label, "client_name": client_name, "prefix": prefix},
        headers=build_headers(),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["session_id"]


def finish_session(server: str, session_id: str, status: str = "done"):
    resp = requests.patch(
        f"{server}/sessions/{session_id}",
        json={"status": status},
        headers=build_headers(),
        timeout=10,
    )
    resp.raise_for_status()


def check_file(server: str, original_path: str, sha256: str, size: int, mtime: float) -> dict:
    resp = requests.post(
        f"{server}/check",
        json={"original_path": original_path, "sha256": sha256, "size": size, "mtime": mtime},
        headers=build_headers(),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def upload_file(server: str, local_path: Path, original_path: str, mtime: float, session_id: Optional[str]) -> dict:
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
        extra = {
            "X-Original-Path": encode_path(original_path),
            "X-Mtime": str(mtime),
            "Content-Type": monitor.content_type,
        }
        if session_id:
            extra["X-Session-Id"] = session_id

        resp = requests.post(f"{server}/upload", data=monitor, headers=build_headers(extra), timeout=120)
        bar.close()

    resp.raise_for_status()
    return resp.json()


def sync_with_server(server: str, existing_paths: list, path_prefix: Optional[str] = None) -> dict:
    payload = {"existing_paths": existing_paths}
    if path_prefix:
        payload["path_prefix"] = path_prefix
    resp = requests.post(f"{server}/sync", json=payload, headers=build_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


# -- Backup -------------------------------------------------------------------
def backup_directory(
    directory: str,
    server: str = DEFAULT_SERVER,
    dry_run: bool = False,
    path_prefix: Optional[str] = None,
    exclude: Optional[list] = None,
    label: Optional[str] = None,
    client_name: Optional[str] = None,
):
    root = Path(directory).resolve()
    if not root.exists():
        log.error(f"Diretorio nao encontrado: {root}")
        sys.exit(1)

    client_name = client_name or socket.gethostname()
    session_id = None

    if not dry_run:
        session_id = create_session(server, label, client_name, path_prefix)
        log.info(f"Sessao    : {session_id}" + (f"  [{label}]" if label else ""))

    stats = {"checked": 0, "uploaded": 0, "skipped": 0, "errors": 0}
    all_paths: list[str] = []

    try:
        for file_path in sorted(root.rglob("*")):
            if not file_path.is_file():
                continue

            # Verifica exclusoes
            excluded = False
            for ex in (exclude or []):
                ex_path = (root / ex).resolve()
                try:
                    file_path.relative_to(ex_path)
                    excluded = True
                    break
                except ValueError:
                    pass
            if excluded:
                continue

            stats["checked"] += 1
            original_path = str(file_path) if not path_prefix else str(
                Path(path_prefix) / file_path.relative_to(root)
            )
            all_paths.append(original_path)

            stat = file_path.stat()
            size  = stat.st_size
            mtime = stat.st_mtime

            try:
                sha256 = sha256_file(file_path)
                check  = check_file(server, original_path, sha256, size, mtime)

                if not check["needs_upload"]:
                    log.info(f"SKIP   {original_path}  [{check['reason']}]")
                    stats["skipped"] += 1
                    continue

                log.info(f"UPLOAD {original_path}  ({fmt_size(size)})  [{check['reason']}]")

                if dry_run:
                    log.info("  [dry-run] upload nao realizado")
                    continue

                result = upload_file(server, file_path, original_path, mtime, session_id)
                log.info(f"  OK  id={result.get('file_id')}  sha256={sha256[:12]}...")
                stats["uploaded"] += 1

            except requests.RequestException as e:
                log.error(f"  ERRO em {original_path}: {e}")
                stats["errors"] += 1

        # Sync
        if not dry_run:
            try:
                result = sync_with_server(server, all_paths, path_prefix)
                removed = result.get("deleted_count", 0)
                stats["removed"] = removed
                if removed:
                    log.info(f"SYNC   {removed} arquivo(s) removido(s) do servidor:")
                    for p in result.get("deleted", []):
                        log.info(f"  - {p}")
                else:
                    log.info("SYNC   Nenhum arquivo removido")
            except requests.RequestException as e:
                log.error(f"SYNC   Erro: {e}")
        else:
            log.info("SYNC   [dry-run] sync nao executado")

        if not dry_run and session_id:
            status = "failed" if stats["errors"] else "done"
            finish_session(server, session_id, status)
            log.info(f"Sessao {session_id} finalizada com status: {status}")

    except Exception as e:
        if not dry_run and session_id:
            finish_session(server, session_id, "failed")
        raise

    log.info("")
    log.info("=" * 50)
    log.info(f"  Sessao      : {session_id or 'dry-run'}")
    log.info(f"  Verificados : {stats['checked']}")
    log.info(f"  Enviados    : {stats['uploaded']}")
    log.info(f"  Ignorados   : {stats['skipped']}")
    log.info(f"  Erros       : {stats['errors']}")
    log.info(f"  Removidos   : {stats.get('removed', 0)}")
    log.info("=" * 50)

    return stats


# -- Sessions -----------------------------------------------------------------
def list_sessions(
    server: str = DEFAULT_SERVER,
    client_name: Optional[str] = None,
    status: Optional[str] = None,
):
    params = {}
    if client_name:
        params["client_name"] = client_name
    if status:
        params["status"] = status

    resp = requests.get(f"{server}/sessions", headers=build_headers(), params=params, timeout=10)
    resp.raise_for_status()
    sessions = resp.json()

    if not sessions:
        log.info("Nenhuma sessao encontrada.")
        return

    log.info("")
    log.info(f"{'ID':36}  {'LABEL':20}  {'CLIENTE':20}  {'STATUS':8}  {'ARQUIVOS':>8}  {'TAMANHO':>10}  INICIO")
    log.info("-" * 120)
    for s in sessions:
        label       = (s.get("label") or "-")[:20]
        client      = (s.get("client_name") or "-")[:20]
        status_str  = s["status"]
        files       = s["file_count"]
        size        = fmt_size(s["total_size_bytes"])
        started     = s["started_at"][:19]
        log.info(f"{s['id']}  {label:20}  {client:20}  {status_str:8}  {files:>8}  {size:>10}  {started}")
    log.info("")


# -- Restore ------------------------------------------------------------------
def restore(
    destination: str,
    server: str = DEFAULT_SERVER,
    session_id: Optional[str] = None,
    path_prefix: Optional[str] = None,
    dry_run: bool = False,
    overwrite: bool = False,
):
    dest_root = Path(destination)
    dest_root.mkdir(parents=True, exist_ok=True)

    log.info(f"Restaurando para : {dest_root}")
    if session_id:
        log.info(f"Sessao           : {session_id}")

    params = {}
    if session_id:
        params["session_id"] = session_id
    if path_prefix:
        params["path_prefix"] = path_prefix

    try:
        resp = requests.get(f"{server}/files", headers=build_headers(), params=params, timeout=10)
        resp.raise_for_status()
        files = resp.json()
    except requests.RequestException as e:
        log.error(f"Erro ao buscar lista de arquivos: {e}")
        sys.exit(1)

    if not files:
        log.info("Nenhum arquivo encontrado para restaurar.")
        return

    log.info(f"{len(files)} arquivo(s) encontrado(s).")
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
                log.info(f"DIFF   {relative}  [modificado localmente — use --overwrite para sobrescrever]")
            stats["skipped"] += 1
            continue

        log.info(f"DOWN   {relative}  ({fmt_size(size)})")

        if dry_run:
            log.info("  [dry-run] download nao realizado")
            continue

        try:
            resp = requests.get(
                f"{server}/files/{file_id}/download",
                headers=build_headers(),
                stream=True,
                timeout=120,
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
                log.error(f"  INTEGRIDADE FALHOU para {relative} — arquivo removido")
                dest_file.unlink()
                stats["errors"] += 1
                continue

            stats["restored"] += 1
            log.info(f"  OK  sha256={sha256[:12]}...")

        except requests.RequestException as e:
            log.error(f"  ERRO ao baixar {relative}: {e}")
            stats["errors"] += 1

    log.info("")
    log.info("=" * 50)
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
    p_backup.add_argument("--server", default=DEFAULT_SERVER)
    p_backup.add_argument("--prefix", default=None, help="Prefixo no servidor (ex: /backups/meu-pc)")
    p_backup.add_argument("--label", default=None, help="Nome amigavel para esta sessao (ex: pre-atualizacao)")
    p_backup.add_argument("--client", default=None, help="Nome do cliente (padrao: hostname da maquina)")
    p_backup.add_argument("--exclude", metavar="PASTA", nargs="+", default=[], help="Subpastas a ignorar")
    p_backup.add_argument("--dry-run", action="store_true", help="Apenas verifica, nao envia")

    # -- sessions -------------------------------------------------------------
    p_sessions = subparsers.add_parser("sessions", help="Lista sessoes de backup disponiveis")
    p_sessions.add_argument("--server", default=DEFAULT_SERVER)
    p_sessions.add_argument("--client", default=None, help="Filtrar por nome do cliente")
    p_sessions.add_argument("--status", default=None, help="Filtrar por status (done, failed, running)")

    # -- restore --------------------------------------------------------------
    p_restore = subparsers.add_parser("restore", help="Baixa arquivos do servidor")
    p_restore.add_argument("destination", help="Pasta de destino")
    p_restore.add_argument("--server", default=DEFAULT_SERVER)
    p_restore.add_argument("--session", default=None, help="ID da sessao a restaurar (ver: sessions)")
    p_restore.add_argument("--prefix", default=None, help="Restaurar apenas arquivos com esse prefixo")
    p_restore.add_argument("--overwrite", action="store_true", help="Sobrescreve arquivos existentes")
    p_restore.add_argument("--dry-run", action="store_true", help="Apenas lista, nao baixa")

    args = parser.parse_args()

    if args.command == "backup":
        log.info(f"Comando  : BACKUP")
        log.info(f"Servidor : {args.server}")
        log.info(f"Diretorio: {args.directory}")
        if args.label:
            log.info(f"Label    : {args.label}")
        if args.exclude:
            log.info(f"Excluindo: {', '.join(args.exclude)}")
        if args.dry_run:
            log.info("Modo     : DRY RUN")
        backup_directory(
            directory=args.directory,
            server=args.server,
            dry_run=args.dry_run,
            path_prefix=args.prefix,
            exclude=args.exclude,
            label=args.label,
            client_name=args.client,
        )

    elif args.command == "sessions":
        log.info(f"Comando  : SESSIONS")
        log.info(f"Servidor : {args.server}")
        list_sessions(server=args.server, client_name=args.client, status=args.status)

    elif args.command == "restore":
        log.info(f"Comando  : RESTORE")
        log.info(f"Servidor : {args.server}")
        log.info(f"Destino  : {args.destination}")
        if args.session:
            log.info(f"Sessao   : {args.session}")
        if args.dry_run:
            log.info("Modo     : DRY RUN")
        restore(
            destination=args.destination,
            server=args.server,
            session_id=args.session,
            path_prefix=args.prefix,
            dry_run=args.dry_run,
            overwrite=args.overwrite,
        )


if __name__ == "__main__":
    main()