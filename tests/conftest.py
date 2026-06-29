import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import auth as auth_mod
import database as db_mod
import main as m
import storage as storage_mod


def _make_engine():
    # StaticPool garante que todas as sessões usam a mesma conexão in-memory
    return create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


@pytest.fixture(autouse=True)
def reset_degraded_volumes():
    m._degraded_volumes.clear()
    _reset_module_caches()
    yield
    m._degraded_volumes.clear()
    _reset_module_caches()


def _reset_module_caches():
    """Zera caches/sinais de módulo que persistem entre testes (isolamento)."""
    m._reclaimable_cache.update({"value": 0, "ts": 0.0})
    m._stats_cache.update({"data": None, "ts": 0.0})
    m._historical_cache.update({"data": None, "ts": 0.0})
    m._activity_wake.clear()
    m._activity_loop_stop.clear()


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
    monkeypatch.setattr(storage_mod, "STORAGE_VOLUMES", [tmp_vol])
    monkeypatch.setattr(storage_mod, "STORAGE_DIR", tmp_vol)

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
    monkeypatch.setattr(storage_mod, "STORAGE_VOLUMES", [tmp_vol])
    monkeypatch.setattr(storage_mod, "STORAGE_DIR", tmp_vol)
    # require_api_key (auth.py) lê os globais do módulo auth, não os aliases em main.
    monkeypatch.setattr(auth_mod, "API_KEY", "testkey")
    monkeypatch.setattr(auth_mod, "AUTH_ENABLED", True)
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
