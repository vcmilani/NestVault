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
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "server"))

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


def _count_safe(conn, table: str) -> int | None:
    """Retorna total de linhas ou None se a tabela estiver corrompida."""
    try:
        return _count(conn, table)
    except Exception:
        return None


def _preflight_check(src_engine) -> list[str]:
    """
    Roda PRAGMA integrity_check no SQLite e retorna lista de problemas.
    Corrupção parcial é reportada mas não impede a migração.
    """
    problems = []
    try:
        with src_engine.connect() as conn:
            rows = conn.execute(text("PRAGMA integrity_check")).fetchall()
            for row in rows:
                msg = row[0]
                if msg != "ok":
                    problems.append(msg)
    except Exception as e:
        problems.append(f"Falha ao rodar integrity_check: {e}")
    return problems


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


def _coerce_row(record: dict, bool_cols: set[str]) -> dict:
    for col in bool_cols:
        if col in record and record[col] is not None:
            record[col] = bool(record[col])
    return record


def _try_fetch(src_conn, cols_str: str, table: str, limit: int, offset: int):
    """Tenta buscar linhas; retorna lista vazia se o range estiver corrompido."""
    try:
        return src_conn.execute(
            text(f"SELECT {cols_str} FROM {table} LIMIT {limit} OFFSET {offset}")
        ).fetchall()
    except Exception:
        return None


def _migrate_table(src_conn, dst_conn, table: str, bool_cols: set[str]) -> tuple[int, int]:
    """Retorna (migrated, skipped)."""
    total = _count_safe(src_conn, table)
    total_str = str(total) if total is not None else "?"

    if total == 0:
        print(f"  [{table}] vazia — pulando")
        return 0, 0

    # Descobre colunas da tabela no SQLite
    cols_result = src_conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    columns = [row[1] for row in cols_result]
    cols_str = ", ".join(columns)
    placeholders = ", ".join(f":{c}" for c in columns)

    insert_sql = text(
        f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    )

    migrated = 0
    skipped  = 0
    offset   = 0

    while True:
        rows = _try_fetch(src_conn, cols_str, table, BATCH_SIZE, offset)

        if rows is None:
            # Batch corrompido — divide em batches menores até chegar a 1 linha
            recovered_in_range = 0
            mini_size = BATCH_SIZE // 2
            mini_offset = offset
            end_offset = offset + BATCH_SIZE

            while mini_size >= 1 and mini_offset < end_offset:
                mini_rows = _try_fetch(src_conn, cols_str, table, mini_size, mini_offset)
                if mini_rows is None:
                    if mini_size == 1:
                        # Linha individual corrompida — pula
                        skipped += 1
                        mini_offset += 1
                    else:
                        mini_size //= 2
                else:
                    if not mini_rows:
                        break
                    batch = [_coerce_row(dict(zip(columns, r)), bool_cols) for r in mini_rows]
                    try:
                        dst_conn.execute(insert_sql, batch)
                        dst_conn.commit()
                        recovered_in_range += len(mini_rows)
                    except Exception:
                        skipped += len(mini_rows)
                    mini_offset += len(mini_rows)

            migrated += recovered_in_range
            offset += BATCH_SIZE

        elif not rows:
            # Fim da tabela
            break

        else:
            batch = [_coerce_row(dict(zip(columns, r)), bool_cols) for r in rows]
            try:
                dst_conn.execute(insert_sql, batch)
                dst_conn.commit()
                migrated += len(rows)
            except Exception as e:
                # Batch falhou (ex: FK violation por sha256 corrompido) — tenta linha a linha
                if skipped == 0:
                    short_err = str(e).split("\n")[0]
                    print(f"\n  [{table}] batch falhou (offset={offset}), tentando linha a linha: {short_err}")
                try:
                    dst_conn.rollback()
                except Exception:
                    pass
                for row in batch:
                    try:
                        dst_conn.execute(insert_sql, [row])
                        dst_conn.commit()
                        migrated += 1
                    except Exception:
                        try:
                            dst_conn.rollback()
                        except Exception:
                            pass
                        skipped += 1
            offset += BATCH_SIZE

        status = f"  [{table}] {migrated}/{total_str}"
        if skipped:
            status += f" ({skipped} corrompidos)"
        print(status, end="\r", flush=True)

    suffix = f" ({skipped} linhas corrompidas ignoradas)" if skipped else ""
    print(f"  [{table}] {migrated}/{total_str} OK{suffix}          ")
    return migrated, skipped


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

        # Verifica integridade do SQLite
        print("[3b/4] Verificando integridade do SQLite...")
        problems = _preflight_check(src_engine)
        if problems:
            print(f"  ⚠️  {len(problems)} problema(s) de integridade detectados:")
            for p in problems[:10]:
                print(f"     {p}")
            if len(problems) > 10:
                print(f"     ... e mais {len(problems) - 10} problemas")
            print("  Continuando — linhas corrompidas serão ignoradas.\n")
            print("  Dica para recuperar o SQLite antes de migrar:")
            print("    sqlite3 backup.db \".recover\" | sqlite3 backup_recovered.db")
            print("    sqlite3 backup_recovered.db \"PRAGMA integrity_check\"\n")
        else:
            print("  ✅ Integridade OK\n")

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
        summary: dict[str, tuple[int, int]] = {}  # table → (migrated, skipped)

        for table in TABLES_IN_ORDER:
            if table not in src_tables:
                print(f"  [{table}] não existe no SQLite — pulando")
                continue
            bool_cols = _bool_columns(table, Base.metadata)
            try:
                summary[table] = _migrate_table(src_conn, dst_conn, table, bool_cols)
            except Exception as e:
                print(f"  [{table}] ❌ ERRO FATAL (tabela ignorada): {e}")
                summary[table] = (0, -1)  # -1 = tabela inacessível

        elapsed = time.time() - t0

        # --- Resumo ---
        print("\n" + "=" * 50)
        print(f"Migração concluída em {elapsed:.1f}s\n")
        total_migrated = 0
        total_skipped = 0
        for table, (mig, skip) in summary.items():
            if skip == -1:
                print(f"  {table:<30} {'INACESSÍVEL':>12}")
            elif skip > 0:
                print(f"  {table:<30} {mig:>8} migrados  {skip:>6} corrompidos ignorados")
            else:
                print(f"  {table:<30} {mig:>8} registros")
            total_migrated += mig
            total_skipped  += max(skip, 0)
        print("-" * 50)
        print(f"  {'TOTAL migrado':<30} {total_migrated:>8} registros")
        if total_skipped:
            print(f"  {'TOTAL ignorado':<30} {total_skipped:>8} linhas corrompidas")
        print("=" * 50)
        if total_skipped:
            print("\n⚠️  Algumas linhas corrompidas foram ignoradas.")
            print("   Para tentar recuperá-las, use:")
            print("     sqlite3 backup.db \".recover\" | sqlite3 backup_recovered.db")
            print("   e rode a migração novamente com --sqlite backup_recovered.db")
        else:
            print("\n✅ Pronto! Configure DATABASE_URL e reinicie o NestVault.")


if __name__ == "__main__":
    main()
