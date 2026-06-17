#!/usr/bin/env python3
"""
migrate_to_postgres.py — NestVault v7.1
Migra todos os dados de um banco SQLite para PostgreSQL.

Uso:
    python migrate_to_postgres.py \
        --sqlite  /caminho/para/backup.db \
        --postgres postgresql://nestvault:senha@localhost/nestvault

O script é idempotente: registros já existentes no destino são ignorados
(INSERT ... ON CONFLICT DO NOTHING). Pode ser re-executado com segurança.
"""

import argparse
import sys
import time
from datetime import datetime

from sqlalchemy import BigInteger, Boolean, create_engine, inspect, text
from sqlalchemy.pool import NullPool

# Tabelas na ordem correta de inserção (respeitando foreign keys)
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


def _count(conn, table: str) -> int:
    return conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()


def _fix_column_types(dst_engine, metadata):
    """
    Corrige tipos de coluna no PostgreSQL que divergem do schema SQLAlchemy.
    Necessário quando a tabela foi criada antes de uma mudança de tipo no modelo
    (ex: Integer → BigInteger para suportar arquivos > 2 GB).
    """
    inspector = inspect(dst_engine)
    existing_tables = inspector.get_table_names()
    fixes = []

    for table_name, table in metadata.tables.items():
        if table_name not in existing_tables:
            continue
        existing_cols = {col["name"]: col for col in inspector.get_columns(table_name)}
        for col in table.columns:
            if col.name not in existing_cols:
                continue
            pg_type = type(existing_cols[col.name]["type"]).__name__.upper()
            # INTEGER (32-bit) deve virar BIGINT quando o modelo declara BigInteger
            if isinstance(col.type, BigInteger) and pg_type == "INTEGER":
                fixes.append((table_name, col.name))

    if not fixes:
        return

    with dst_engine.connect() as conn:
        for table_name, col_name in fixes:
            print(f"  [schema] ALTER TABLE {table_name}.{col_name}: INTEGER → BIGINT")
            conn.execute(text(
                f'ALTER TABLE "{table_name}" ALTER COLUMN "{col_name}" TYPE BIGINT'
            ))
        conn.commit()


def _bool_columns(table: str, metadata) -> set[str]:
    """Retorna nomes de colunas Boolean da tabela no schema SQLAlchemy."""
    if table not in metadata.tables:
        return set()
    return {
        col.name
        for col in metadata.tables[table].columns
        if isinstance(col.type, Boolean)
    }


def _migrate_table(src_conn, dst_conn, table: str, bool_cols: set[str]) -> int:
    total = _count(src_conn, table)
    if total == 0:
        print(f"  [{table}] vazia — pulando")
        return 0

    # Descobre colunas da tabela no SQLite
    cols_result = src_conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    columns = [row[1] for row in cols_result]
    cols_str = ", ".join(columns)
    placeholders = ", ".join(f":{c}" for c in columns)

    insert_sql = text(
        f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    )

    migrated = 0
    offset = 0
    while True:
        rows = src_conn.execute(
            text(f"SELECT {cols_str} FROM {table} LIMIT {BATCH_SIZE} OFFSET {offset}")
        ).fetchall()
        if not rows:
            break

        batch = []
        for row in rows:
            record = dict(zip(columns, row))
            # SQLite armazena Boolean como 0/1; PostgreSQL exige True/False
            for col in bool_cols:
                if col in record and record[col] is not None:
                    record[col] = bool(record[col])
            batch.append(record)

        dst_conn.execute(insert_sql, batch)
        dst_conn.commit()

        migrated += len(rows)
        offset += BATCH_SIZE
        print(f"  [{table}] {migrated}/{total}", end="\r", flush=True)

    print(f"  [{table}] {migrated}/{total} OK          ")
    return migrated


def main():
    parser = argparse.ArgumentParser(
        description="Migra dados do NestVault de SQLite para PostgreSQL"
    )
    parser.add_argument("--sqlite",   required=True, help="Caminho para o arquivo .db do SQLite")
    parser.add_argument("--postgres", required=True, help="URL de conexão PostgreSQL (postgresql://...)")
    parser.add_argument("--dry-run",  action="store_true", help="Apenas verifica a conexão, não migra dados")
    args = parser.parse_args()

    # --- Conexões ---
    print(f"\n[1/4] Conectando ao SQLite: {args.sqlite}")
    src_engine = create_engine(
        f"sqlite:///{args.sqlite}",
        connect_args={"check_same_thread": False},
        poolclass=NullPool,
    )

    print(f"[2/4] Conectando ao PostgreSQL: {args.postgres}")
    try:
        dst_engine = create_engine(args.postgres, pool_pre_ping=True)
    except Exception as e:
        print(f"ERRO ao criar engine PostgreSQL: {e}", file=sys.stderr)
        sys.exit(1)

    with src_engine.connect() as src_conn, dst_engine.connect() as dst_conn:
        # Verifica tabelas no SQLite
        src_tables = inspect(src_engine).get_table_names()
        print(f"[3/4] Tabelas encontradas no SQLite: {', '.join(src_tables)}")

        # Verifica se o PostgreSQL já tem dados
        from database import Base
        print("[4/4] Criando schema no PostgreSQL (se necessário)...")
        Base.metadata.create_all(bind=dst_engine)
        _fix_column_types(dst_engine, Base.metadata)

        existing_rows = 0
        for table in TABLES_IN_ORDER:
            if table in src_tables:
                try:
                    existing_rows += _count(dst_conn, table)
                except Exception:
                    pass

        if existing_rows > 0:
            print(f"\n⚠️  O banco PostgreSQL já contém {existing_rows} registros.")
            resp = input("   Continuar mesmo assim? Duplicatas serão ignoradas. [s/N] ").strip().lower()
            if resp != "s":
                print("Migração cancelada.")
                sys.exit(0)

        if args.dry_run:
            print("\n[dry-run] Conexões OK. Nenhum dado foi migrado.")
            return

        # --- Migração ---
        print(f"\nIniciando migração — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        t0 = time.time()
        summary = {}

        for table in TABLES_IN_ORDER:
            if table not in src_tables:
                print(f"  [{table}] não existe no SQLite — pulando")
                continue
            bool_cols = _bool_columns(table, Base.metadata)
            summary[table] = _migrate_table(src_conn, dst_conn, table, bool_cols)

        elapsed = time.time() - t0

        # --- Resumo ---
        print("\n" + "=" * 50)
        print(f"Migração concluída em {elapsed:.1f}s\n")
        total_rows = 0
        for table, count in summary.items():
            print(f"  {table:<30} {count:>8} registros")
            total_rows += count
        print("-" * 50)
        print(f"  {'TOTAL':<30} {total_rows:>8} registros")
        print("=" * 50)
        print("\n✅ Pronto! Configure DATABASE_URL e reinicie o NestVault.")


if __name__ == "__main__":
    main()
