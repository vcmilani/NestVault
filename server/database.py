"""
Models do banco de dados — v3.0
Ajustes de performance:
- WAL mode no SQLite (escritas nao bloqueiam leituras)
- Indices em colunas usadas em filtros e joins
- Cache mais agressivo
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, Float,
    DateTime, ForeignKey, UniqueConstraint, Index, event
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

DB_PATH = os.getenv("DB_PATH", "./backup.db")

engine = create_engine(
    f"sqlite:///{DB_PATH}",
    connect_args={"check_same_thread": False, "timeout": 30},
    pool_pre_ping=True,
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
    cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class BackupID(Base):
    __tablename__ = "backup_ids"

    id          = Column(Integer, primary_key=True)
    label       = Column(String, nullable=False, unique=True, index=True)
    client_name = Column(String, nullable=True, index=True)
    prefix      = Column(String, nullable=True)
    created_at  = Column(DateTime, default=datetime.utcnow)
    status      = Column(String, default="active")

    versions = relationship("BackupVersion", back_populates="backup",
                            order_by="BackupVersion.version_key.desc()", lazy="dynamic")


class BackupVersion(Base):
    __tablename__ = "backup_versions"
    __table_args__ = (
        UniqueConstraint("backup_label", "version_key", name="uq_label_version"),
        Index("idx_label_status_key", "backup_label", "status", "version_key"),
    )

    id           = Column(Integer, primary_key=True)
    backup_label = Column(String, ForeignKey("backup_ids.label"), nullable=False, index=True)
    version_key  = Column(String, nullable=False, index=True)
    created_at   = Column(DateTime, default=datetime.utcnow)
    finished_at  = Column(DateTime, nullable=True)
    status       = Column(String, default="running", index=True)

    backup = relationship("BackupID", back_populates="versions")
    files  = relationship("VersionFile", back_populates="version", lazy="dynamic",
                          cascade="all, delete-orphan")


class FileContent(Base):
    __tablename__ = "file_contents"

    sha256     = Column(String(64), primary_key=True)
    stored_at  = Column(String, nullable=False)
    size       = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    refs = relationship("VersionFile", back_populates="content", lazy="dynamic")


class FileContentCopy(Base):
    __tablename__ = "file_content_copies"
    __table_args__ = (
        UniqueConstraint("sha256", "volume_path", name="uq_sha256_volume"),
        Index("idx_fcc_sha256", "sha256"),
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
    original_path = Column(String, nullable=False, index=True)
    sha256        = Column(String(64), ForeignKey("file_contents.sha256"), nullable=False)
    mtime         = Column(Float, nullable=False)
    created_at    = Column(DateTime, default=datetime.utcnow)

    version = relationship("BackupVersion", back_populates="files")
    content = relationship("FileContent", back_populates="refs")


def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()