"""
Models do banco de dados — v7.9.0
Suporte dual: SQLite (padrão) ou PostgreSQL (opcional via DATABASE_URL).

SQLite:  configurado via DB_PATH (padrão ./backup.db) — ideal para uso doméstico/NAS.
PostgreSQL: configurado via DATABASE_URL (ex: postgresql://user:pass@host/db) —
            recomendado para ambientes com muitos uploads concorrentes.
"""

from sqlalchemy import (
    create_engine, Column, Integer, BigInteger, String, Float, Boolean,
    DateTime, ForeignKey, UniqueConstraint, Index, event, text, Text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.pool import NullPool
from datetime import datetime, timezone
import os

def _utcnow():
    return datetime.now()

DATABASE_URL = os.getenv("DATABASE_URL")
DB_PATH      = os.getenv("DB_PATH", "./backup.db")

if DATABASE_URL:
    # Valida que o driver PostgreSQL está instalado antes de tentar conectar
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        raise RuntimeError(
            "DATABASE_URL está definida, mas o driver PostgreSQL não está instalado.\n"
            "  Instale com:  pip install -r requirements-postgres.txt\n"
            "  Raspberry Pi: sudo apt install -y python3-psycopg2"
        )
    # PostgreSQL: pool com health-check automático; sem pragmas SQLite
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
else:
    # SQLite: NullPool + WAL para melhor concorrência sem servidor externo
    engine = create_engine(
        f"sqlite:///{DB_PATH}",
        connect_args={"check_same_thread": False, "timeout": 60},
        poolclass=NullPool,
    )

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, _):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")           # leituras nao bloqueiam escritas
        cursor.execute("PRAGMA synchronous=NORMAL")          # mais rapido, ainda seguro
        cursor.execute("PRAGMA cache_size=-64000")           # 64MB de cache
        cursor.execute("PRAGMA temp_store=MEMORY")           # tabelas temp na RAM
        cursor.execute("PRAGMA mmap_size=268435456")         # 256MB mmap
        cursor.execute("PRAGMA foreign_keys=ON")             # enforça FKs declaradas nos modelos
        cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def hash_api_key(raw: str) -> str:
    """SHA-256 hex — a chave em si nunca é persistida em texto puro."""
    import hashlib
    return hashlib.sha256(raw.encode()).hexdigest()


class User(Base):
    __tablename__ = "users"

    id           = Column(Integer, primary_key=True)
    username     = Column(String, nullable=False, unique=True, index=True)
    api_key_hash = Column(String(64), nullable=False, unique=True, index=True)
    role         = Column(String, nullable=False, default="user")  # "admin" | "user"
    is_active    = Column(Boolean, nullable=False, default=True)
    created_at   = Column(DateTime, default=_utcnow)


class BackupID(Base):
    __tablename__ = "backup_ids"

    id            = Column(Integer, primary_key=True)
    label         = Column(String, nullable=False, unique=True, index=True)
    client_name   = Column(String, nullable=True, index=True)
    prefix        = Column(String, nullable=True)
    created_at    = Column(DateTime, default=_utcnow)
    status        = Column(String, default="active")
    # Dono do backup — nullable durante a migração (ver bootstrap_admin_user /
    # backfill_backup_owners); usuário comum só enxerga/restaura labels onde
    # owner_user_id == User.id atual (ou é admin).
    owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)

    versions = relationship("BackupVersion", back_populates="backup",
                            order_by="BackupVersion.version_key.desc()", lazy="dynamic")
    owner    = relationship("User")


class BackupVersion(Base):
    __tablename__ = "backup_versions"
    __table_args__ = (
        UniqueConstraint("backup_label", "version_key", name="uq_label_version"),
        Index("idx_label_status_created", "backup_label", "status", "created_at"),
        Index("idx_version_created", "created_at"),
        Index("idx_version_finished", "finished_at"),
    )

    id             = Column(Integer, primary_key=True)
    backup_label   = Column(String, ForeignKey("backup_ids.label"), nullable=False)
    version_key    = Column(String, nullable=False)
    created_at     = Column(DateTime, default=_utcnow)
    finished_at    = Column(DateTime, nullable=True)
    status         = Column(String, default="running")
    absorbed_count = Column(Integer, nullable=False, default=0, server_default="0")
    # Checkpoint de progresso para jobs rclone resumíveis: JSON com
    # {"done_dirs": [...]} dos diretórios já totalmente processados.
    progress_json  = Column(Text, nullable=True)

    backup = relationship("BackupID", back_populates="versions")
    files  = relationship("VersionFile", back_populates="version", lazy="dynamic",
                          cascade="all, delete-orphan")


class FileContent(Base):
    __tablename__ = "file_contents"

    sha256     = Column(String(64), primary_key=True)
    stored_at  = Column(String, nullable=False)
    size       = Column(BigInteger, nullable=False)
    encrypted  = Column(Boolean, nullable=False, default=False, server_default="0")
    created_at = Column(DateTime, default=_utcnow)

    refs = relationship("VersionFile", back_populates="content", lazy="dynamic")


class FileContentCopy(Base):
    __tablename__ = "file_content_copies"
    __table_args__ = (
        UniqueConstraint("sha256", "volume_path", name="uq_sha256_volume"),
        Index("idx_fcc_volume", "volume_path"),
    )

    id          = Column(Integer, primary_key=True, autoincrement=True)
    sha256      = Column(String(64), ForeignKey("file_contents.sha256"), nullable=False)
    stored_at   = Column(String, nullable=False, unique=True)
    volume_path = Column(String, nullable=False)


class VersionFile(Base):
    __tablename__ = "version_files"
    __table_args__ = (
        UniqueConstraint("version_id", "original_path", name="uq_version_path"),
        Index("idx_sha256", "sha256"),
    )

    id            = Column(Integer, primary_key=True)
    version_id    = Column(Integer, ForeignKey("backup_versions.id"), nullable=False, index=True)
    original_path = Column(String, nullable=False)
    sha256        = Column(String(64), ForeignKey("file_contents.sha256"), nullable=False)
    mtime         = Column(Float, nullable=False)
    created_at    = Column(DateTime, default=_utcnow)

    version = relationship("BackupVersion", back_populates="files")
    content = relationship("FileContent", back_populates="refs")


class MaintenanceJob(Base):
    __tablename__ = "maintenance_jobs"

    id          = Column(Integer, primary_key=True)
    job_type    = Column(String, nullable=False)
    status      = Column(String, nullable=False, default="running")
    started_at  = Column(DateTime, default=_utcnow)
    finished_at = Column(DateTime, nullable=True)
    summary     = Column(String, nullable=True)


class SsdCachePendingMove(Base):
    __tablename__ = "ssd_cache_pending_moves"

    sha256      = Column(String(64), ForeignKey("file_contents.sha256"), primary_key=True)
    ssd_path    = Column(String, nullable=False)
    dest_volume = Column(String, nullable=False)
    dest_path   = Column(String, nullable=False)
    created_at  = Column(DateTime, default=_utcnow)
    retry_count = Column(Integer, nullable=False, default=0)


class RcloneBackupJob(Base):
    __tablename__ = "rclone_backup_jobs"

    id               = Column(Integer, primary_key=True)
    remote_name      = Column(String, nullable=False)
    remote_path      = Column(String, nullable=False, default="")
    display_name     = Column(String, nullable=False)
    target_label     = Column(String, nullable=False)
    cron_expr        = Column(String, nullable=True)
    enabled          = Column(Boolean, nullable=False, default=True)
    # Estratégia de listagem: "auto" (decide por backend), "walk" (incremental
    # dir a dir) ou "fast" (recursiva única) — força a escolha por job.
    strategy         = Column(String, nullable=False, default="auto", server_default="auto")
    last_run_at      = Column(DateTime, nullable=True)
    last_run_status  = Column(String, nullable=True)
    last_run_message = Column(String, nullable=True)
    created_at       = Column(DateTime, default=_utcnow)


class DiskSnapshot(Base):
    __tablename__ = "disk_snapshots"
    __table_args__ = (
        Index("ix_disk_snapshots_volume_sampled", "volume_path", "sampled_at"),
    )

    id          = Column(Integer, primary_key=True, autoincrement=True)
    volume_path = Column(String, nullable=False)
    used_pct    = Column(Float, nullable=False)
    sampled_at  = Column(DateTime, nullable=False, default=_utcnow)


def init_db():
    import logging as _initlog
    _log_init = _initlog.getLogger("backup-server")

    Base.metadata.create_all(bind=engine)

    # Migração que precisa rodar em QUALQUER backend (create_all não adiciona
    # colunas a tabelas já existentes). Em bancos Postgres pré-existentes a
    # coluna progress_json não seria criada de outra forma.
    with engine.connect() as conn:
        try:
            if engine.dialect.name == "sqlite":
                conn.execute(text(
                    "ALTER TABLE backup_versions ADD COLUMN progress_json TEXT"
                ))
            else:
                conn.execute(text(
                    "ALTER TABLE backup_versions ADD COLUMN IF NOT EXISTS progress_json TEXT"
                ))
            conn.commit()
            _log_init.info("[db-migrate] Coluna backup_versions.progress_json garantida")
        except Exception as e:
            if "duplicate column" not in str(e).lower():
                raise

    # Idem para rclone_backup_jobs.strategy (escolha manual de estratégia).
    with engine.connect() as conn:
        try:
            if engine.dialect.name == "sqlite":
                conn.execute(text(
                    "ALTER TABLE rclone_backup_jobs ADD COLUMN strategy TEXT NOT NULL DEFAULT 'auto'"
                ))
            else:
                conn.execute(text(
                    "ALTER TABLE rclone_backup_jobs ADD COLUMN IF NOT EXISTS strategy TEXT NOT NULL DEFAULT 'auto'"
                ))
            conn.commit()
            _log_init.info("[db-migrate] Coluna rclone_backup_jobs.strategy garantida")
        except Exception as e:
            if "duplicate column" not in str(e).lower():
                raise

    # Idem para backup_ids.owner_user_id (backup por usuário).
    with engine.connect() as conn:
        try:
            if engine.dialect.name == "sqlite":
                conn.execute(text(
                    "ALTER TABLE backup_ids ADD COLUMN owner_user_id INTEGER"
                ))
            else:
                conn.execute(text(
                    "ALTER TABLE backup_ids ADD COLUMN IF NOT EXISTS owner_user_id INTEGER"
                ))
            conn.commit()
            _log_init.info("[db-migrate] Coluna backup_ids.owner_user_id garantida")
        except Exception as e:
            if "duplicate column" not in str(e).lower():
                raise

    # Demais migrações manuais apenas para SQLite — no PostgreSQL o schema é
    # criado via create_all (bancos novos) ou migração externa.
    if engine.dialect.name != "sqlite":
        _log_init.info(f"[db] Backend: {engine.dialect.name} — migrações SQLite ignoradas")
        return

    _log_init.info("[db] Backend: SQLite — aplicando migrações incrementais")

    with engine.connect() as conn:
        # Migração: adiciona coluna encrypted em bancos existentes (ignora se já existir)
        try:
            conn.execute(text(
                "ALTER TABLE file_contents ADD COLUMN encrypted INTEGER NOT NULL DEFAULT 0"
            ))
            conn.commit()
            _log_init.info("[db-migrate] Coluna file_contents.encrypted adicionada")
        except Exception as e:
            if "duplicate column" not in str(e).lower():
                raise

        # Migração: adiciona absorbed_count em bancos existentes
        try:
            conn.execute(text(
                "ALTER TABLE backup_versions ADD COLUMN absorbed_count INTEGER NOT NULL DEFAULT 0"
            ))
            conn.commit()
            _log_init.info("[db-migrate] Coluna backup_versions.absorbed_count adicionada")
        except Exception as e:
            if "duplicate column" not in str(e).lower():
                raise

        # (progress_json já é garantida acima para todos os backends)

        # Migração de índices: remove redundantes, cria composto otimizado
        _index_migrations = [
            # Remove índices de coluna única que o composto já cobre
            ("DROP INDEX IF EXISTS backup_versions_backup_label_index",
             "Removendo índice redundante: backup_versions.backup_label"),
            ("DROP INDEX IF EXISTS backup_versions_status_index",
             "Removendo índice redundante: backup_versions.status"),
            ("DROP INDEX IF EXISTS backup_versions_version_key_index",
             "Removendo índice redundante: backup_versions.version_key"),
            # Remove composto antigo (sem created_at) e cria o novo
            ("DROP INDEX IF EXISTS idx_label_status_key",
             "Removendo índice composto antigo: idx_label_status_key"),
            ("CREATE INDEX IF NOT EXISTS idx_label_status_created ON backup_versions (backup_label, status, created_at)",
             "Criando índice composto otimizado: idx_label_status_created"),
            # Remove índice redundante com a unique constraint (sha256, volume_path)
            ("DROP INDEX IF EXISTS idx_fcc_sha256",
             "Removendo índice redundante: file_content_copies.sha256"),
            # Remove índice redundante com a unique constraint (version_id, original_path)
            ("DROP INDEX IF EXISTS version_files_original_path_index",
             "Removendo índice redundante: version_files.original_path"),
        ]
        for stmt, msg in _index_migrations:
            conn.execute(text(stmt))
            _log_init.info(f"[db-migrate] {msg}")
        conn.commit()
        _log_init.info("[db-migrate] Migração de índices concluída")


def bootstrap_admin_user(api_key: str) -> None:
    """Primeira inicialização após a migração para backup por usuário: se
    nenhum User existir ainda, cria um admin cuja chave é o BACKUP_API_KEY
    atual — preserva o acesso de quem já usa essa chave, sem exigir troca
    imediata em nenhum cliente. Idempotente (não faz nada se já há usuários)."""
    if not api_key:
        return
    import logging
    log = logging.getLogger("backup-server")
    db = SessionLocal()
    try:
        if db.query(User).count() > 0:
            return
        admin = User(username="admin", api_key_hash=hash_api_key(api_key),
                     role="admin", is_active=True)
        db.add(admin)
        db.commit()
        db.refresh(admin)
        log.info(f"[db-migrate] Usuário admin '{admin.username}' criado a partir de BACKUP_API_KEY")
        _backfill_backup_owners(db, admin.id)
    finally:
        db.close()


def _backfill_backup_owners(db, owner_user_id: int) -> None:
    """Atribui owner_user_id a todo BackupID ainda sem dono (backups criados
    antes da migração por usuário) — garante que nenhum backup fique órfão."""
    import logging
    log = logging.getLogger("backup-server")
    n = (db.query(BackupID)
         .filter(BackupID.owner_user_id.is_(None))
         .update({"owner_user_id": owner_user_id}, synchronize_session=False))
    db.commit()
    if n:
        log.info(f"[db-migrate] {n} backup(s) existente(s) atribuído(s) ao usuário {owner_user_id}")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()