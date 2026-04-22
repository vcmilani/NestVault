"""
Models do banco de dados (SQLite via SQLAlchemy).
Leve o suficiente para rodar confortavelmente em uma Raspberry Pi.
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

DB_PATH = os.getenv("DB_PATH", "./backup.db")
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class FileRecord(Base):
    __tablename__ = "file_records"

    id           = Column(Integer, primary_key=True, index=True)
    original_path = Column(String, nullable=False, index=True)  # path original no cliente
    sha256       = Column(String(64), nullable=False, index=True)
    size         = Column(Integer, nullable=False)               # bytes
    mtime        = Column(Float, nullable=False)                 # epoch
    stored_at    = Column(String, nullable=False)                # path no servidor
    created_at   = Column(DateTime, default=datetime.utcnow)


def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
