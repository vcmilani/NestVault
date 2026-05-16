#!/usr/bin/env python3
"""
Migração de paths dos volumes de storage no SQLite do NestVault.

Copie este arquivo para o servidor remoto e execute:
    python3 migrate_paths.py --dry-run          # prévia sem alterar nada
    python3 migrate_paths.py                    # aplica a migração
    python3 migrate_paths.py --db /caminho/backup.db

Dependências: apenas biblioteca padrão do Python 3 (sqlite3, shutil, argparse).

Paths migrados:
    /media/Data/storage_backup      →  /media/Data/disk1
    /media/DataExtra/storage_backup →  /media/DataExtra/disk2
    /media/Data1TB/storage_backup   →  /media/Data1TB/disk_1TB

Tabelas afetadas:
    file_contents        → coluna stored_at
    file_content_copies  → colunas stored_at e volume_path

IMPORTANTE: Pare o servidor antes de executar.
"""

import argparse
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

MIGRATIONS = [
    ("/media/Data/storage_backup",      "/media/Data/disk1"),
    ("/media/DataExtra/storage_backup", "/media/DataExtra/disk2"),
    ("/media/Data1TB/storage_backup",   "/media/Data1TB/disk_1TB"),
]


def replace_prefix(value: str, old: str, new: str) -> str:
    if value and value.startswith(old):
        return new + value[len(old):]
    return value


def migrate(db_path: str, dry_run: bool) -> None:
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"Banco não encontrado: {db_path}")

    if not dry_run:
        backup = path.with_name(
            path.stem + f"_pre_migrate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        )
        shutil.copy2(db_path, backup)
        print(f"[backup] Cópia criada em: {backup}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    total_fc = total_fcc_stored = total_fcc_volume = 0

    for old, new in MIGRATIONS:
        print(f"\n[migração] {old!r}  →  {new!r}")

        # file_contents.stored_at
        cur.execute(
            "SELECT sha256, stored_at FROM file_contents WHERE stored_at LIKE ?",
            (old + "%",),
        )
        rows = cur.fetchall()
        for row in rows:
            new_path = replace_prefix(row["stored_at"], old, new)
            print(f"  [FC ] {row['stored_at']}")
            print(f"      → {new_path}")
            if not dry_run:
                cur.execute(
                    "UPDATE file_contents SET stored_at = ? WHERE sha256 = ?",
                    (new_path, row["sha256"]),
                )
            total_fc += 1

        # file_content_copies.stored_at + volume_path
        cur.execute(
            "SELECT id, stored_at, volume_path FROM file_content_copies WHERE stored_at LIKE ?",
            (old + "%",),
        )
        rows = cur.fetchall()
        for row in rows:
            new_stored = replace_prefix(row["stored_at"], old, new)
            new_vol    = replace_prefix(row["volume_path"], old, new)
            print(f"  [FCC] stored_at:   {row['stored_at']}")
            print(f"      →              {new_stored}")
            print(f"        volume_path: {row['volume_path']}")
            print(f"      →              {new_vol}")
            if not dry_run:
                cur.execute(
                    "UPDATE file_content_copies SET stored_at = ?, volume_path = ? WHERE id = ?",
                    (new_stored, new_vol, row["id"]),
                )
            total_fcc_stored += 1

        # volume_path orfão (stored_at já foi migrado, volume_path ainda não)
        cur.execute(
            "SELECT id, volume_path FROM file_content_copies "
            "WHERE volume_path LIKE ? AND stored_at NOT LIKE ?",
            (old + "%", old + "%"),
        )
        for row in cur.fetchall():
            new_vol = replace_prefix(row["volume_path"], old, new)
            print(f"  [FCC] volume_path orphan: {row['volume_path']}  →  {new_vol}")
            if not dry_run:
                cur.execute(
                    "UPDATE file_content_copies SET volume_path = ? WHERE id = ?",
                    (new_vol, row["id"]),
                )
            total_fcc_volume += 1

    print()
    if dry_run:
        print(
            f"[dry-run] Nenhuma alteração aplicada.\n"
            f"          Seriam atualizados:\n"
            f"            {total_fc} linhas em file_contents.stored_at\n"
            f"            {total_fcc_stored} linhas em file_content_copies\n"
            f"            {total_fcc_volume} linhas com volume_path orfão"
        )
        conn.close()
    else:
        conn.commit()
        conn.close()
        print(
            f"[ok] Migração concluída.\n"
            f"     {total_fc} file_contents atualizados\n"
            f"     {total_fcc_stored} file_content_copies atualizados\n"
            f"     {total_fcc_volume} volume_path orfãos corrigidos\n\n"
            f"[próximo passo] Atualize STORAGE_DIRS no ambiente do servidor:\n"
            f"    STORAGE_DIRS=/media/Data/disk1,/media/DataExtra/disk2,/media/Data1TB/disk_1TB\n"
            f"    Em seguida reinicie o servidor NestVault."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migra paths de volumes no backup.db do NestVault"
    )
    parser.add_argument(
        "--db",
        default="./backup.db",
        help="Caminho para o backup.db (padrão: ./backup.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas mostra o que seria alterado, sem modificar o banco",
    )
    args = parser.parse_args()
    migrate(args.db, args.dry_run)
