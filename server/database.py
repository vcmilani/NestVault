"""
Models do banco de dados — v3.0
Ajustes de performance:
- WAL mode no SQLite (escritas nao bloqueiam leituras)
- Indices em colunas usadas em filtros e joins
- Cache mais agressivo
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Boolean,
    DateTime, ForeignKey, UniqueConstraint, Index, event, text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.pool import NullPool
from datetime import datetime, timezone
import os, hashlib, base64

def _utcnow():
    return datetime.now()

DB_PATH = os.getenv("DB_PATH", "./backup.db")

engine = create_engine(
    f"sqlite:///{DB_PATH}",
    connect_args={"check_same_thread": False, "timeout": 30},
    poolclass=NullPool,
)

# Ativa WAL mode + outras pragmas de performance no SQLite
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


class BackupID(Base):
    __tablename__ = "backup_ids"

    id          = Column(Integer, primary_key=True)
    label       = Column(String, nullable=False, unique=True, index=True)
    client_name = Column(String, nullable=True, index=True)
    prefix      = Column(String, nullable=True)
    created_at  = Column(DateTime, default=_utcnow)
    status      = Column(String, default="active")

    versions = relationship("BackupVersion", back_populates="backup",
                            order_by="BackupVersion.version_key.desc()", lazy="dynamic")


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

    backup = relationship("BackupID", back_populates="versions")
    files  = relationship("VersionFile", back_populates="version", lazy="dynamic",
                          cascade="all, delete-orphan")


class FileContent(Base):
    __tablename__ = "file_contents"

    sha256     = Column(String(64), primary_key=True)
    stored_at  = Column(String, nullable=False)
    size       = Column(Integer, nullable=False)
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


class CloudCredential(Base):
    __tablename__ = "cloud_credentials"

    id           = Column(Integer, primary_key=True)
    provider     = Column(String, nullable=False, index=True)  # "gdrive" | "onedrive"
    email        = Column(String, nullable=False)
    display_name = Column(String, nullable=True)
    access_token = Column(String, nullable=True)
    refresh_token = Column(String, nullable=False)  # armazenado criptografado
    token_expiry = Column(DateTime, nullable=True)
    created_at   = Column(DateTime, default=_utcnow)

    jobs = relationship("CloudBackupJob", back_populates="credential", cascade="all, delete-orphan")


class CloudBackupJob(Base):
    __tablename__ = "cloud_backup_jobs"
    __table_args__ = (
        Index("idx_cbj_last_run", "last_run_at"),
    )

    id              = Column(Integer, primary_key=True)
    credential_id   = Column(Integer, ForeignKey("cloud_credentials.id"), nullable=False, index=True)
    folder_id       = Column(String, nullable=False)
    folder_name     = Column(String, nullable=False)
    target_label    = Column(String, nullable=False)
    cron_expr       = Column(String, nullable=True)
    enabled         = Column(Boolean, default=True, nullable=False)
    last_run_at     = Column(DateTime, nullable=True)
    last_run_status = Column(String, nullable=True)   # "running" | "success" | "error"
    last_run_message = Column(String, nullable=True)
    created_at      = Column(DateTime, default=_utcnow)

    credential = relationship("CloudCredential", back_populates="jobs")


class MaintenanceJob(Base):
    __tablename__ = "maintenance_jobs"

    id          = Column(Integer, primary_key=True)
    job_type    = Column(String, nullable=False)
    status      = Column(String, nullable=False, default="running")
    started_at  = Column(DateTime, default=_utcnow)
    finished_at = Column(DateTime, nullable=True)
    summary     = Column(String, nullable=True)


# -- Token encryption ---------------------------------------------------------
import logging as _logging
_log = _logging.getLogger("backup-server")

_cipher_cache: dict[str, object] = {}


def _token_cipher():
    api_key = os.getenv("BACKUP_API_KEY", "")
    if not api_key:
        return None
    if api_key not in _cipher_cache:
        from cryptography.fernet import Fernet
        from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
        from cryptography.hazmat.primitives import hashes
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32,
                         salt=b"nestvault-token-v1", iterations=260_000)
        _cipher_cache[api_key] = Fernet(base64.urlsafe_b64encode(kdf.derive(api_key.encode())))
    return _cipher_cache[api_key]


def _token_cipher_legacy():
    """Chave SHA-256 usada antes da migração para PBKDF2 — apenas para decrypt de fallback."""
    api_key = os.getenv("BACKUP_API_KEY", "")
    if not api_key:
        return None
    from cryptography.fernet import Fernet
    return Fernet(base64.urlsafe_b64encode(hashlib.sha256(api_key.encode()).digest()))


def encrypt_token(token: str) -> str:
    cipher = _token_cipher()
    return cipher.encrypt(token.encode()).decode() if cipher else token


def decrypt_token(encrypted: str) -> str:
    cipher = _token_cipher()
    if not cipher:
        return encrypted
    try:
        return cipher.decrypt(encrypted.encode()).decode()
    except Exception:
        # Fallback: token cifrado com chave legada (SHA-256) antes da migração para PBKDF2
        try:
            legacy = _token_cipher_legacy()
            plaintext = legacy.decrypt(encrypted.encode()).decode()
            _log.info("[token] Token legado detectado — será re-cifrado com PBKDF2 na próxima gravação")
            return plaintext
        except Exception:
            _log.warning("[token] Falha ao decifrar token — pode estar em texto plano (migração sem API_KEY)")
            return encrypted


def init_db():
    import logging as _initlog
    _log_init = _initlog.getLogger("backup-server")

    Base.metadata.create_all(bind=engine)

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


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()