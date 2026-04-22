"""
Models do banco de dados (SQLite via SQLAlchemy).
Leve o suficiente para rodar confortavelmente em uma Raspberry Pi.
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

DB_PATH = os.getenv("DB_PATH", "./backup.db")
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class BackupSession(Base):
    """Representa uma execucao de backup — cada chamada ao cliente gera uma sessao."""
    __tablename__ = "backup_sessions"

    id          = Column(String(36), primary_key=True)   # UUID4
    label       = Column(String, nullable=True)           # nome amigavel opcional
    client_name = Column(String, nullable=True)           # identificador da maquina cliente
    prefix      = Column(String, nullable=True)           # prefixo usado no backup
    started_at  = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    status      = Column(String, default="running")       # running | done | failed

    files = relationship("FileRecord", back_populates="session", lazy="dynamic")


class FileRecord(Base):
    __tablename__ = "file_records"

    id            = Column(Integer, primary_key=True, index=True)
    session_id    = Column(String(36), ForeignKey("backup_sessions.id"), nullable=True, index=True)
    original_path = Column(String, nullable=False, index=True)
    sha256        = Column(String(64), nullable=False, index=True)
    size          = Column(Integer, nullable=False)
    mtime         = Column(Float, nullable=False)
    stored_at     = Column(String, nullable=False)
    created_at    = Column(DateTime, default=datetime.utcnow)

    session = relationship("BackupSession", back_populates="files")


def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()