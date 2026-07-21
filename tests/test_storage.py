from collections import namedtuple
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from fastapi.testclient import TestClient

import database as db_mod
import main as m
import storage as storage_mod

DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


def _seed_admin(Session):
    """Cria o usuário admin (chave 'testkey') usado por todos os clients deste
    módulo — auth é sempre obrigatória, então todo TestClient precisa de um."""
    db = Session()
    try:
        db.add(db_mod.User(username="admin", api_key_hash=db_mod.hash_api_key("testkey"),
                            role="admin", is_active=True))
        db.commit()
    finally:
        db.close()


def _make_client(monkeypatch, volumes, disk_usage_fn):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    db_mod.Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    _seed_admin(Session)

    monkeypatch.setattr(m, "STORAGE_VOLUMES", volumes)
    monkeypatch.setattr(m, "STORAGE_DIR", volumes[0])
    # storage.py lê os globais do próprio módulo — propaga os patches para lá.
    monkeypatch.setattr(storage_mod, "STORAGE_VOLUMES", volumes)
    monkeypatch.setattr(storage_mod, "STORAGE_DIR", volumes[0])

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    m.app.dependency_overrides[db_mod.get_db] = override_get_db

    with patch("main.shutil.disk_usage", side_effect=disk_usage_fn):
        with TestClient(m.app) as c:
            c.headers.update({"X-API-Key": "testkey"})
            yield c

    m.app.dependency_overrides.clear()


# -- single volume ------------------------------------------------------------

def test_storage_info_single_volume(tmp_path, monkeypatch):
    vol = tmp_path / "vol"
    vol.mkdir()

    def fake_usage(path):
        return DiskUsage(total=1000, used=400, free=600)

    for c in _make_client(monkeypatch, [vol], fake_usage):
        r = c.get("/storage/info")
        assert r.status_code == 200
        data = r.json()
        assert data["total_bytes"] == 1000
        assert data["used_bytes"] == 400
        assert data["free_bytes"] == 600
        assert data["reclaimable_bytes"] == 0


# -- dois volumes agregados ---------------------------------------------------

def test_storage_info_two_volumes_aggregated(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    def fake_usage(path):
        if path == v1:
            return DiskUsage(total=1000, used=300, free=700)
        return DiskUsage(total=2000, used=500, free=1500)

    for c in _make_client(monkeypatch, [v1, v2], fake_usage):
        r = c.get("/storage/info")
        assert r.status_code == 200
        data = r.json()
        assert data["total_bytes"] == 3000
        assert data["used_bytes"] == 800
        assert data["free_bytes"] == 2200


# -- reclaimable_bytes --------------------------------------------------------

def test_storage_info_reclaimable(tmp_path, monkeypatch):
    """Versões antigas geram bytes recuperáveis."""
    import base64

    vol = tmp_path / "vol"; vol.mkdir()

    GB = 1024 ** 3
    def fake_usage(path):
        # Acima de STORAGE_FALLBACK_THRESHOLD_GB para o upload usar o volume direto.
        return DiskUsage(total=100 * GB, used=10 * GB, free=90 * GB)

    for c in _make_client(monkeypatch, [vol], fake_usage):
        # Cria backup com duas versões, cada uma com arquivo único
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        c.post("/backups/b1/versions", json={"version_key": "v2"})

        enc = lambda p: base64.b64encode(p.encode()).decode()

        c.post("/upload", content=b"only in v1", headers={
            "X-Backup-Label": "b1", "X-Version-Key": "v1",
            "X-Original-Path": enc("/f1.txt"), "X-Mtime": "1.0",
        })
        c.post("/backups/b1/versions/v1", json={"status": "done"})

        c.post("/upload", content=b"only in v2", headers={
            "X-Backup-Label": "b1", "X-Version-Key": "v2",
            "X-Original-Path": enc("/f2.txt"), "X-Mtime": "2.0",
        })
        c.patch("/backups/b1/versions/v2", json={"status": "done"})

        r = c.get("/storage/info")
        # v2 é a keeper; conteúdo de v1 é reclaimable
        assert r.json()["reclaimable_bytes"] == len(b"only in v1")
