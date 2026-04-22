"""
Backup Client
Varre um diretorio local e envia apenas os arquivos que foram modificados
ou ainda nao existem no servidor.

Uso:
    python backup_client.py /caminho/para/backup
    python backup_client.py /caminho/para/backup --server http://192.168.1.100:8000
    python backup_client.py /caminho/para/backup --dry-run
"""

import os
import sys
import hashlib
import argparse
import logging
import base64
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
    """Codifica o path em base64 para trafegar com seguranca no header HTTP.
    Headers HTTP so aceitam latin-1; caminhos com acentos/cedilha causam UnicodeEncodeError."""
    return base64.b64encode(path.encode("utf-8")).decode("ascii")


# -- Core ---------------------------------------------------------------------
def check_file(server: str, original_path: str, sha256: str, size: int, mtime: float) -> dict:
    resp = requests.post(
        f"{server}/check",
        json={
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


def upload_file(server: str, local_path: Path, original_path: str, mtime: float) -> dict:
    file_size = local_path.stat().st_size
    filename  = local_path.name

    with open(local_path, "rb") as f:
        encoder = MultipartEncoder(
            fields={"file": (filename, f, "application/octet-stream")}
        )

        bar = tqdm(
            total=file_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=f"  {filename[:40]}",
            leave=False,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{rate_fmt}]",
        )

        def _progress(monitor: MultipartEncoderMonitor):
            bar.update(monitor.bytes_read - bar.n)

        monitor = MultipartEncoderMonitor(encoder, _progress)

        headers = build_headers({
            "X-Original-Path": encode_path(original_path),
            "X-Mtime": str(mtime),
            "Content-Type": monitor.content_type,
        })

        resp = requests.post(
            f"{server}/upload",
            data=monitor,
            headers=headers,
            timeout=120,
        )
        bar.close()

    resp.raise_for_status()
    return resp.json()



def sync_with_server(
    server: str,
    existing_paths: list,
    path_prefix: Optional[str] = None,
) -> dict:
    """Envia a lista de paths atuais e pede ao servidor para remover os que sumiram."""
    payload = {"existing_paths": existing_paths}
    if path_prefix:
        payload["path_prefix"] = path_prefix
    resp = requests.post(
        f"{server}/sync",
        json=payload,
        headers=build_headers(),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def backup_directory(
    directory: str,
    server: str = DEFAULT_SERVER,
    dry_run: bool = False,
    path_prefix: Optional[str] = None,
    exclude: Optional[list] = None,
):
    root = Path(directory).resolve()
    if not root.exists():
        log.error(f"Diretorio nao encontrado: {root}")
        sys.exit(1)

    stats = {"checked": 0, "uploaded": 0, "skipped": 0, "errors": 0}
    all_paths: list[str] = []  # todos os paths encontrados no cliente

    for file_path in sorted(root.rglob("*")):
        if not file_path.is_file():
            continue

        # Verifica se a pasta mae do arquivo esta na lista de exclusao
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
            log.debug(f"EXCLUIDO {file_path}")
            continue

        stats["checked"] += 1

        original_path = str(file_path) if not path_prefix else str(
            Path(path_prefix) / file_path.relative_to(root)
        )

        all_paths.append(original_path)
        stat = file_path.stat()
        size = stat.st_size
        mtime = stat.st_mtime

        try:
            sha256 = sha256_file(file_path)

            # 1. Verificar metadados
            check = check_file(server, original_path, sha256, size, mtime)

            if not check["needs_upload"]:
                log.info(f"SKIP   {original_path}  [{check['reason']}]")
                stats["skipped"] += 1
                continue

            log.info(f"UPLOAD {original_path}  ({size:,} bytes)  [{check['reason']}]")

            if dry_run:
                log.info("  [dry-run] upload nao realizado")
                continue

            # 2. Fazer upload
            result = upload_file(server, file_path, original_path, mtime)
            log.info(f"  Armazenado  id={result.get('file_id')}  sha256={sha256[:12]}...")
            stats["uploaded"] += 1

        except requests.RequestException as e:
            log.error(f"  ERRO em {original_path}: {e}")
            stats["errors"] += 1

    # Sync — remove do servidor o que nao existe mais no cliente
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
            log.error(f"SYNC   Erro ao sincronizar: {e}")
    else:
        log.info("SYNC   [dry-run] sync nao executado")

    # Resumo
    log.info("")
    log.info("=" * 50)
    log.info(f"  Verificados : {stats['checked']}")
    log.info(f"  Enviados    : {stats['uploaded']}")
    log.info(f"  Ignorados   : {stats['skipped']}")
    log.info(f"  Erros       : {stats['errors']}")
    log.info(f"  Removidos   : {stats.get('removed', 0)}")
    log.info("=" * 50)

    return stats



# -- Restore ------------------------------------------------------------------
def restore(
    destination: str,
    server: str = DEFAULT_SERVER,
    path_prefix: Optional[str] = None,
    dry_run: bool = False,
    overwrite: bool = False,
):
    """
    Baixa todos os arquivos do servidor e reconstroi a estrutura de pastas
    dentro de `destination`.

    O original_path de cada arquivo e usado para montar o caminho relativo:
      - Se tem path_prefix: strip do prefixo e usa o restante
      - Se nao tem: usa o nome do arquivo direto

    Exemplo:
      original_path = /backups/notebook/home/user/docs/relatorio.pdf
      path_prefix   = /backups/notebook
      destino final = <destination>/home/user/docs/relatorio.pdf
    """
    dest_root = Path(destination)
    dest_root.mkdir(parents=True, exist_ok=True)

    log.info(f"Restaurando para: {dest_root}")

    # 1. Buscar lista de arquivos no servidor
    try:
        params = {}
        if path_prefix:
            params["path_prefix"] = path_prefix
        resp = requests.get(
            f"{server}/files",
            headers=build_headers(),
            params=params,
            timeout=10,
        )
        resp.raise_for_status()
        files = resp.json()
    except requests.RequestException as e:
        log.error(f"Erro ao buscar lista de arquivos: {e}")
        sys.exit(1)

    if not files:
        log.info("Nenhum arquivo encontrado no servidor para restaurar.")
        return

    log.info(f"{len(files)} arquivo(s) encontrado(s) no servidor.")

    stats = {"restored": 0, "skipped": 0, "errors": 0}

    for record in files:
        file_id      = record["id"]
        original_path = record["original_path"]
        sha256       = record["sha256"]
        size         = record["size"]

        # Calcula caminho de destino
        if path_prefix and original_path.startswith(path_prefix):
            relative = original_path[len(path_prefix):].lstrip("/")
        else:
            # Sem prefixo: usa o path original completo (strip da barra inicial)
            relative = original_path.lstrip("/")

        dest_file = dest_root / relative

        # Verifica se ja existe e se e identico (pelo sha256)
        if dest_file.exists() and not overwrite:
            local_sha256 = sha256_file(dest_file)
            if local_sha256 == sha256:
                log.info(f"SKIP   {relative}  [identico]")
                stats["skipped"] += 1
                continue
            else:
                log.info(f"DIFF   {relative}  [modificado localmente — use --overwrite para sobrescrever]")
                stats["skipped"] += 1
                continue

        log.info(f"DOWN   {relative}  ({size:,} bytes)")

        if dry_run:
            log.info("  [dry-run] download nao realizado")
            continue

        # Download
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
                total=total,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"  {Path(relative).name[:40]}",
                leave=False,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{rate_fmt}]",
            )
            with open(dest_file, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
                    bar.update(len(chunk))
            bar.close()

            # Valida integridade apos download
            downloaded_sha256 = sha256_file(dest_file)
            if downloaded_sha256 != sha256:
                log.error(f"  INTEGRIDADE FALHOU para {relative} — arquivo removido")
                dest_file.unlink()
                stats["errors"] += 1
                continue

            stats["restored"] += 1
            log.info(f"  OK  sha256={sha256[:12]}...")

        except requests.RequestException as e:
            log.error(f"  ERRO ao baixar {relative}: {e}")
            stats["errors"] += 1

    # Resumo
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
    p_backup.add_argument("--server", default=DEFAULT_SERVER, help="URL do servidor")
    p_backup.add_argument("--dry-run", action="store_true", help="Apenas verifica, nao envia")
    p_backup.add_argument("--prefix", default=None, help="Prefixo no servidor (ex: /backups/meu-pc)")
    p_backup.add_argument(
        "--exclude",
        metavar="PASTA",
        nargs="+",
        default=[],
        help="Subpastas a ignorar (ex: --exclude node_modules .git __pycache__)",
    )

    # -- restore --------------------------------------------------------------
    p_restore = subparsers.add_parser("restore", help="Baixa arquivos do servidor")
    p_restore.add_argument("destination", help="Pasta de destino para restaurar os arquivos")
    p_restore.add_argument("--server", default=DEFAULT_SERVER, help="URL do servidor")
    p_restore.add_argument("--prefix", default=None, help="Restaurar apenas arquivos com esse prefixo")
    p_restore.add_argument("--dry-run", action="store_true", help="Apenas lista, nao baixa")
    p_restore.add_argument("--overwrite", action="store_true", help="Sobrescreve arquivos existentes")

    args = parser.parse_args()

    if args.command == "backup":
        log.info(f"Comando  : BACKUP")
        log.info(f"Servidor : {args.server}")
        log.info(f"Diretorio: {args.directory}")
        if args.dry_run:
            log.info("Modo     : DRY RUN")
        if args.exclude:
            log.info(f"Excluindo : {', '.join(args.exclude)}")
        backup_directory(
            directory=args.directory,
            server=args.server,
            dry_run=args.dry_run,
            path_prefix=args.prefix,
            exclude=args.exclude,
        )

    elif args.command == "restore":
        log.info(f"Comando  : RESTORE")
        log.info(f"Servidor : {args.server}")
        log.info(f"Destino  : {args.destination}")
        if args.prefix:
            log.info(f"Prefixo  : {args.prefix}")
        if args.dry_run:
            log.info("Modo     : DRY RUN")
        restore(
            destination=args.destination,
            server=args.server,
            path_prefix=args.prefix,
            dry_run=args.dry_run,
            overwrite=args.overwrite,
        )


if __name__ == "__main__":
    main()