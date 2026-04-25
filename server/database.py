"""
Models do banco de dados — v2.0
Suporte a versoes dentro do mesmo label com deduplicacao por conteudo (sha256).
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, Float,
    DateTime, ForeignKey, UniqueConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

DB_PATH = os.getenv("DB_PATH", "./backup.db")
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class BackupID(Base):
    """Identificador de backup. O label e unico e agrupa todas as versoes."""
    __tablename__ = "backup_ids"

    id          = Column(Integer, primary_key=True)
    label       = Column(String, nullable=False, unique=True, index=True)
    client_name = Column(String, nullable=True)
    prefix      = Column(String, nullable=True)
    created_at  = Column(DateTime, default=datetime.utcnow)
    status      = Column(String, default="active")

    versions = relationship("BackupVersion", back_populates="backup",
                            order_by="BackupVersion.version_key.desc()", lazy="dynamic")


class BackupVersion(Base):
    """
    Uma versao (snapshot) dentro de um backup.
    version_key = data/hora ISO no momento em que o backup foi iniciado.
    """
    __tablename__ = "backup_versions"
    __table_args__ = (
        UniqueConstraint("backup_label", "version_key", name="uq_label_version"),
    )

    id           = Column(Integer, primary_key=True)
    backup_label = Column(String, ForeignKey("backup_ids.label"), nullable=False, index=True)
    version_key  = Column(String, nullable=False, index=True)  # ex: "2026-04-25T10:42:31"
    created_at   = Column(DateTime, default=datetime.utcnow)
    finished_at  = Column(DateTime, nullable=True)
    status       = Column(String, default="running")           # running | done | failed

    backup = relationship("BackupID", back_populates="versions")
    files  = relationship("VersionFile", back_populates="version", lazy="dynamic")


class FileContent(Base):
    """
    Arquivo fisico unico — deduplicado por sha256.
    Um mesmo conteudo e armazenado apenas uma vez, mesmo que referenciado
    por multiplas versoes ou labels.
    """
    __tablename__ = "file_contents"

    sha256     = Column(String(64), primary_key=True)
    stored_at  = Column(String, nullable=False)
    size       = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    refs = relationship("VersionFile", back_populates="content", lazy="dynamic")


class VersionFile(Base):
    """
    Referencia de um arquivo em uma versao especifica.
    status = active   -> arquivo existia no cliente nesta versao
    status = deleted  -> arquivo foi removido do cliente (marcado, nao apagado do storage)
    """
    __tablename__ = "version_files"
    __table_args__ = (
        UniqueConstraint("version_id", "original_path", name="uq_version_path"),
    )

    id            = Column(Integer, primary_key=True)
    version_id    = Column(Integer, ForeignKey("backup_versions.id"), nullable=False, index=True)
    original_path = Column(String, nullable=False, index=True)
    sha256        = Column(String(64), ForeignKey("file_contents.sha256"), nullable=False)
    mtime         = Column(Float, nullable=False)
    status        = Column(String, default="active")   # active | deleted
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