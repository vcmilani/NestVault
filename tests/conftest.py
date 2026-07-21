import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import database as db_mod
import main as m
import storage as storage_mod

ADMIN_KEY = "testkey"


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


def _setup_app(tmp_vol, monkeypatch):
    """Cria engine/Session in-memory isolada e registra os overrides comuns
    (storage, get_db, SessionLocal). Retorna o sessionmaker para os testes
    poderem inserir dados de setup (ex: usuários) direto no banco."""
    engine = _make_engine()
    db_mod.Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)

    monkeypatch.setattr(m, "STORAGE_VOLUMES", [tmp_vol])
    monkeypatch.setattr(m, "STORAGE_DIR", tmp_vol)
    monkeypatch.setattr(storage_mod, "STORAGE_VOLUMES", [tmp_vol])
    monkeypatch.setattr(storage_mod, "STORAGE_DIR", tmp_vol)
    # Background tasks (_bg_*) abrem sua propria sessao via SessionLocal() em vez de
    # Depends(get_db) — aponta para o mesmo engine in-memory do teste, senao elas
    # operariam sobre o banco real (./backup.db) e pareceriam no-op nos testes.
    monkeypatch.setattr(m, "SessionLocal", Session)

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    m.app.dependency_overrides[db_mod.get_db] = override_get_db
    return Session


def _create_user(Session, username, api_key, role="user"):
    db = Session()
    try:
        u = db_mod.User(username=username, api_key_hash=db_mod.hash_api_key(api_key),
                         role=role, is_active=True)
        db.add(u); db.commit(); db.refresh(u)
        return u.id
    finally:
        db.close()


@pytest.fixture
def client(tmp_vol, monkeypatch):
    """Client autenticado como admin por padrão (chave fixa 'testkey') — cobre a
    maioria dos testes, que exercitam a mecânica de backup e não o modelo de
    permissão em si. Admin não sofre restrição de posse, então o comportamento
    observado é equivalente ao antigo modo "sem autenticação"."""
    Session = _setup_app(tmp_vol, monkeypatch)
    _create_user(Session, "admin", ADMIN_KEY, role="admin")
    with TestClient(m.app) as c:
        c.headers.update({"X-API-Key": ADMIN_KEY})
        yield c
    m.app.dependency_overrides.clear()


@pytest.fixture
def auth_client(tmp_vol, monkeypatch):
    """Client SEM chave padrão nos headers — usado para testar o mecanismo de
    autenticação em si (sem chave / chave errada / chave correta). O banco já
    tem um usuário admin válido com a chave ADMIN_KEY."""
    Session = _setup_app(tmp_vol, monkeypatch)
    _create_user(Session, "admin", ADMIN_KEY, role="admin")
    with TestClient(m.app) as c:
        yield c
    m.app.dependency_overrides.clear()


@pytest.fixture
def two_users(tmp_vol, monkeypatch):
    """Três clients sobre o MESMO banco: admin, alice e bob — para testar que
    um usuário comum não enxerga/restaura/escreve backups de outro."""
    Session = _setup_app(tmp_vol, monkeypatch)
    _create_user(Session, "admin", ADMIN_KEY, role="admin")
    _create_user(Session, "alice", "alice-key", role="user")
    _create_user(Session, "bob", "bob-key", role="user")
    with TestClient(m.app) as admin_c:
        admin_c.headers.update({"X-API-Key": ADMIN_KEY})
        alice_c = TestClient(m.app)
        alice_c.headers.update({"X-API-Key": "alice-key"})
        bob_c = TestClient(m.app)
        bob_c.headers.update({"X-API-Key": "bob-key"})
        yield admin_c, alice_c, bob_c
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
