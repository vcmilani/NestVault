import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import database as db_mod
import main as m


def _make_engine():
    # StaticPool garante que todas as sessões usam a mesma conexão in-memory
    return create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


@pytest.fixture
def tmp_vol(tmp_path):
    vol = tmp_path / "vol"
    vol.mkdir()
    return vol


@pytest.fixture
def client(tmp_vol, monkeypatch):
    engine = _make_engine()
    db_mod.Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)

    monkeypatch.setattr(m, "STORAGE_VOLUMES", [tmp_vol])
    monkeypatch.setattr(m, "STORAGE_DIR", tmp_vol)

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    m.app.dependency_overrides[db_mod.get_db] = override_get_db
    with TestClient(m.app) as c:
        yield c
    m.app.dependency_overrides.clear()


@pytest.fixture
def auth_client(tmp_vol, monkeypatch):
    """Client com autenticação habilitada (API_KEY=testkey)."""
    engine = _make_engine()
    db_mod.Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)

    monkeypatch.setattr(m, "STORAGE_VOLUMES", [tmp_vol])
    monkeypatch.setattr(m, "STORAGE_DIR", tmp_vol)
    monkeypatch.setattr(m, "API_KEY", "testkey")
    monkeypatch.setattr(m, "AUTH_ENABLED", True)

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    m.app.dependency_overrides[db_mod.get_db] = override_get_db
    with TestClient(m.app) as c:
        yield c
    m.app.dependency_overrides.clear()


# -- Helpers para criar fixtures de backup/version via API --------------------

def make_backup(client, label="test-backup", client_name=None):
    body = {"label": label}
    if client_name:
        body["client_name"] = client_name
    r = client.post("/backups", json=body)
    assert r.status_code == 200
    return r.json()


def make_version(client, label="test-backup", version_key="2026-01-01T00:00:00"):
    r = client.post(f"/backups/{label}/versions", json={"version_key": version_key})
    assert r.status_code == 200
    return r.json()


def finish_version(client, label, version_key, status="done"):
    r = client.patch(f"/backups/{label}/versions/{version_key}", json={"status": status})
    assert r.status_code == 200
    return r.json()


def upload_file(client, label, version_key, path="/file.txt", content=b"hello", mtime=1000.0):
    import base64
    encoded_path = base64.b64encode(path.encode()).decode()
    r = client.post(
        "/upload",
        content=content,
        headers={
            "X-Backup-Label": label,
            "X-Version-Key": version_key,
            "X-Original-Path": encoded_path,
            "X-Mtime": str(mtime),
        },
    )
    assert r.status_code == 200
    return r.json()
