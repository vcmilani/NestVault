#!/usr/bin/env python3
"""
migrate_to_sqlite.py — NestVault v7.1
Migra todos os dados de um banco PostgreSQL de volta para SQLite.

Uso:
    python migrate_to_sqlite.py \
        --postgres postgresql://nestvault:senha@localhost/nestvault \
        --sqlite   /caminho/para/backup_restored.db

O script é idempotente: registros já existentes no destino são ignorados
(INSERT OR IGNORE). Pode ser re-executado com segurança.
"""

import argparse
import sys
import time
from datetime import datetime

from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.pool import NullPool

TABLES_IN_ORDER = [
    "backup_ids",
    "file_contents",
    "cloud_credentials",
    "rclone_backup_jobs",
    "cloud_backup_jobs",
    "maintenance_jobs",
    "ssd_cache_pending_moves",
    "backup_versions",
    "file_content_copies",
    "version_files",
]

BATCH_SIZE = 500


def _apply_wal_pragmas(dst_engine):
    """Configura WAL e parâmetros de performance no SQLite de destino."""
    with dst_engine.connect() as conn:
        conn.execute(text("PRAGMA journal_mode=WAL"))
        conn.execute(text("PRAGMA synchronous=NORMAL"))
        conn.execute(text("PRAGMA cache_size=-64000"))
        conn.execute(text("PRAGMA temp_store=MEMORY"))
        conn.execute(text("PRAGMA mmap_size=268435456"))
        conn.execute(text("PRAGMA foreign_keys=OFF"))  # desliga durante a carga em massa
        conn.commit()


def _count(conn, table: str) -> int:
    return conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()


def _migrate_table(src_conn, dst_conn, table: str) -> int:
    """Retorna número de linhas migradas."""
    total = _count(src_conn, table)

    if total == 0:
        print(f"  [{table}] vazia — pulando")
        return 0

    # Descobre colunas via SELECT * LIMIT 0
    result = src_conn.execute(text(f"SELECT * FROM {table} LIMIT 0"))
    columns = list(result.keys())
    cols_str = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(f":{c}" for c in columns)

    insert_sql = text(
        f"INSERT OR IGNORE INTO {table} ({cols_str}) VALUES ({placeholders})"
    )

    migrated = 0
    offset = 0

    while True:
        rows = src_conn.execute(
            text(f'SELECT * FROM "{table}" LIMIT {BATCH_SIZE} OFFSET {offset}')
        ).fetchall()

        if not rows:
            break

        batch = [dict(zip(columns, r)) for r in rows]
        dst_conn.execute(insert_sql, batch)
        dst_conn.commit()
        migrated += len(rows)
        offset += BATCH_SIZE

        print(f"  [{table}] {migrated}/{total}", end="\r", flush=True)

    print(f"  [{table}] {migrated}/{total} OK          ")
    return migrated


def main():
    parser = argparse.ArgumentParser(
        description="Migra dados do NestVault de PostgreSQL para SQLite"
    )
    parser.add_argument("--postgres", required=True, help="URL de conexão PostgreSQL (postgresql://...)")
    parser.add_argument("--sqlite",   required=True, help="Caminho para o arquivo .db SQLite de destino")
    parser.add_argument("--dry-run",  action="store_true", help="Apenas verifica a conexão, não migra dados")
    args = parser.parse_args()

    print(f"\n[1/4] Conectando ao PostgreSQL: {args.postgres}")
    try:
        src_engine = create_engine(args.postgres, pool_pre_ping=True)
        with src_engine.connect() as _:
            pass
    except Exception as e:
        print(f"ERRO ao conectar ao PostgreSQL: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"[2/4] Conectando ao SQLite: {args.sqlite}")
    dst_engine = create_engine(
        f"sqlite:///{args.sqlite}",
        connect_args={"check_same_thread": False},
        poolclass=NullPool,
    )

    with src_engine.connect() as src_conn, dst_engine.connect() as dst_conn:
        src_tables = inspect(src_engine).get_table_names()
        print(f"[3/4] Tabelas encontradas no PostgreSQL: {', '.join(src_tables)}")

        print("[4/4] Criando schema no SQLite e configurando WAL...")
        from database import Base
        Base.metadata.create_all(bind=dst_engine)
        _apply_wal_pragmas(dst_engine)

        existing_rows = 0
        for table in TABLES_IN_ORDER:
            if table in src_tables:
                try:
                    existing_rows += _count(dst_conn, table)
                except Exception:
                    pass

        if existing_rows > 0:
            print(f"\n⚠️  O arquivo SQLite já contém {existing_rows} registros.")
            resp = input("   Continuar mesmo assim? Duplicatas serão ignoradas. [s/N] ").strip().lower()
            if resp != "s":
                print("Migração cancelada.")
                sys.exit(0)

        if args.dry_run:
            print("\n[dry-run] Conexões OK. Nenhum dado foi migrado.")
            return

        print(f"\nIniciando migração — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        t0 = time.time()
        summary: dict[str, int] = {}

        for table in TABLES_IN_ORDER:
            if table not in src_tables:
                print(f"  [{table}] não existe no PostgreSQL — pulando")
                continue
            try:
                summary[table] = _migrate_table(src_conn, dst_conn, table)
            except Exception as e:
                print(f"  [{table}] ERRO: {e}")
                summary[table] = -1

        # Religa foreign keys após carga
        with dst_engine.connect() as conn:
            conn.execute(text("PRAGMA foreign_keys=ON"))
            conn.execute(text("PRAGMA integrity_check"))
            conn.commit()

        elapsed = time.time() - t0

        print("\n" + "=" * 50)
        print(f"Migração concluída em {elapsed:.1f}s\n")
        total_migrated = 0
        for table, mig in summary.items():
            if mig == -1:
                print(f"  {table:<30} {'ERRO':>12}")
            else:
                print(f"  {table:<30} {mig:>8} registros")
                total_migrated += mig
        print("-" * 50)
        print(f"  {'TOTAL migrado':<30} {total_migrated:>8} registros")
        print("=" * 50)
        print("\n✅ Pronto! Para usar o SQLite, remova DATABASE_URL e reinicie o NestVault")
        print(f"   com DB_PATH={args.sqlite} (ou mova o arquivo para o local padrão).")


if __name__ == "__main__":
    main()
