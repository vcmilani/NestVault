from collections import namedtuple
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import database as db_mod
import db_backup

DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])
GB = 1024 ** 3


def _setup(tmp_path, monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    db_mod.Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)

    monkeypatch.setattr(db_backup, "SessionLocal", Session)
    monkeypatch.setattr(db_backup, "DATABASE_URL", None)
    monkeypatch.setattr(db_backup, "DB_PATH", str(tmp_path / "src.db"))
    monkeypatch.setattr(db_backup, "_estimate_db_size", lambda: 1024)


def test_run_db_backup_skips_volume_below_threshold_when_another_has_space(tmp_path, monkeypatch):
    """Volume abaixo do STORAGE_FALLBACK_THRESHOLD_GB deve ser pulado enquanto outro
    volume saudável tiver espaço acima do limiar (Bug B)."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()
    _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(db_backup, "healthy_volumes", lambda: [v1, v2])
    monkeypatch.setattr(db_backup, "STORAGE_FALLBACK_THRESHOLD_GB", 10.0)

    def fake_usage(path):
        # v1 abaixo do limiar (5 GB livres) mesmo tendo espaço de sobra para o dump em si.
        if str(path).startswith(str(v1)):
            return DiskUsage(total=100 * GB, used=95 * GB, free=5 * GB)
        return DiskUsage(total=100 * GB, used=80 * GB, free=20 * GB)

    with patch("db_backup.shutil.disk_usage", side_effect=fake_usage):
        result = db_backup.run_db_backup()

    assert len(result["files"]) == 1
    assert str(v2) in result["files"][0]
    assert not any(str(v1) in f for f in result["files"])


def test_run_db_backup_uses_last_resort_when_all_volumes_below_threshold(tmp_path, monkeypatch):
    """Quando NENHUM volume saudável está acima do limiar, o backup do banco ainda
    deve ser gravado (último recurso) em vez de parar de rodar indefinidamente."""
    v1 = tmp_path / "v1"; v1.mkdir()
    _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(db_backup, "healthy_volumes", lambda: [v1])
    monkeypatch.setattr(db_backup, "STORAGE_FALLBACK_THRESHOLD_GB", 10.0)

    def fake_usage(_path):
        return DiskUsage(total=100 * GB, used=99.8 * GB, free=0.2 * GB)

    with patch("db_backup.shutil.disk_usage", side_effect=fake_usage):
        result = db_backup.run_db_backup()

    assert len(result["files"]) == 1
    assert str(v1) in result["files"][0]
