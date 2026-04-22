"""
Models do banco de dados (SQLite via SQLAlchemy).
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

DB_PATH = os.getenv("DB_PATH", "./backup.db")
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class BackupID(Base):
    """
    Identificador de backup. O label e unico e serve como chave de isolamento.
    Todos os arquivos de um backup ficam sob seu proprio label — nenhum backup
    interfere nos arquivos de outro.
    """
    __tablename__ = "backup_ids"

    id          = Column(Integer, primary_key=True)
    label       = Column(String, nullable=False, unique=True, index=True)  # identificador unico
    client_name = Column(String, nullable=True)
    prefix      = Column(String, nullable=True)
    created_at  = Column(DateTime, default=datetime.utcnow)
    last_run_at = Column(DateTime, nullable=True)
    status      = Column(String, default="active")   # active | deleted

    files = relationship("FileRecord", back_populates="backup", lazy="dynamic")


class FileRecord(Base):
    __tablename__ = "file_records"
    __table_args__ = (
        # Um mesmo path so pode existir uma vez por backup_label
        UniqueConstraint("backup_label", "original_path", name="uq_backup_path"),
    )

    id            = Column(Integer, primary_key=True, index=True)
    backup_label  = Column(String, ForeignKey("backup_ids.label"), nullable=False, index=True)
    original_path = Column(String, nullable=False, index=True)
    sha256        = Column(String(64), nullable=False, index=True)
    size          = Column(Integer, nullable=False)
    mtime         = Column(Float, nullable=False)
    stored_at     = Column(String, nullable=False)
    created_at    = Column(DateTime, default=datetime.utcnow)
    updated_at    = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    backup = relationship("BackupID", back_populates="files")


def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()