"""Testa autenticação por usuário: sem chave / chave errada / chave válida,
e que endpoints administrativos exigem role=admin."""
import pytest

from fastapi.testclient import TestClient

import main as m
from conftest import ADMIN_KEY


PROTECTED_ENDPOINTS = [
    ("GET",    "/backups"),
    ("POST",   "/backups"),
    ("GET",    "/storage/info"),
    ("POST",   "/check"),
    ("POST",   "/check/batch"),
    ("POST",   "/upload"),
    ("POST",   "/maintenance/cleanup-orphans"),
    ("GET",    "/api/settings"),
    ("PUT",    "/api/settings"),
    ("POST",   "/api/settings/restart"),
]

ADMIN_ONLY_ENDPOINTS = [
    ("GET",  "/storage/info"),
    ("GET",  "/api/stats"),
    ("POST", "/maintenance/cleanup-orphans"),
    ("GET",  "/users"),
    ("GET",  "/api/settings"),
]

# Também restritos a admin, mas não podem ser chamados "a seco" com a chave de
# admin: o PUT exige corpo (422 sem ele) e o restart derrubaria o processo de
# teste. Só entram no teste de 403.
ADMIN_ONLY_NOT_SMOKEABLE = [
    ("PUT",  "/api/settings"),
    ("POST", "/api/settings/restart"),
]


@pytest.mark.parametrize("method,path", PROTECTED_ENDPOINTS)
def test_no_key_returns_401(auth_client, method, path):
    r = auth_client.request(method, path)
    assert r.status_code == 401


@pytest.mark.parametrize("method,path", PROTECTED_ENDPOINTS)
def test_wrong_key_returns_401(auth_client, method, path):
    r = auth_client.request(method, path, headers={"X-API-Key": "wrongkey"})
    assert r.status_code == 401


def test_valid_key_passes(auth_client):
    r = auth_client.get("/backups", headers={"X-API-Key": ADMIN_KEY})
    assert r.status_code == 200


def test_deactivated_user_loses_access(two_users):
    admin, alice, _bob = two_users
    users = admin.get("/users").json()
    alice_id = next(u["id"] for u in users if u["username"] == "alice")

    assert alice.get("/backups").status_code == 200
    r = admin.patch(f"/users/{alice_id}", json={"is_active": False})
    assert r.status_code == 200
    assert alice.get("/backups").status_code == 401


def test_health_never_requires_auth(auth_client):
    r = auth_client.get("/health")
    assert r.status_code == 200


@pytest.mark.parametrize("method,path", ADMIN_ONLY_ENDPOINTS + ADMIN_ONLY_NOT_SMOKEABLE)
def test_regular_user_gets_403_on_admin_endpoints(two_users, method, path):
    _admin, alice, _bob = two_users
    r = alice.request(method, path)
    assert r.status_code == 403


@pytest.mark.parametrize("method,path", ADMIN_ONLY_ENDPOINTS)
def test_admin_can_access_admin_endpoints(two_users, method, path):
    admin, _alice, _bob = two_users
    r = admin.request(method, path)
    assert r.status_code == 200


def test_security_headers_on_dashboard_and_api(client):
    """CSP restringe para onde o painel pode mandar dados (a API key fica em
    localStorage); os cabeçalhos também saem nas respostas da API."""
    for path in ("/", "/health"):
        r = client.get(path)
        csp = r.headers["content-security-policy"]
        assert "connect-src 'self'" in csp
        assert "frame-ancestors 'none'" in csp
        assert r.headers["x-content-type-options"] == "nosniff"


def _user_id(admin_client, username):
    return next(u["id"] for u in admin_client.get("/users").json() if u["username"] == username)


def test_admin_cannot_deactivate_own_account(client):
    """Regressão: o admin podia se desativar e ficar sem acesso — o bootstrap por
    BACKUP_API_KEY só roda com a tabela de usuários vazia, então não havia volta."""
    r = client.patch(f"/users/{_user_id(client, 'admin')}", json={"is_active": False})
    assert r.status_code == 409
    assert client.get("/users").status_code == 200  # continua com acesso


def test_admin_can_deactivate_another_admin_while_one_remains(client):
    r = client.post("/users", json={"username": "admin2", "role": "admin"})
    admin2_key = r.json()["api_key"]
    admin2_id = r.json()["user"]["id"]

    assert client.patch(f"/users/{admin2_id}", json={"is_active": False}).status_code == 200
    assert client.get("/users", headers={"X-API-Key": admin2_key}).status_code == 401
    # Reativar não passa pela trava.
    assert client.patch(f"/users/{admin2_id}", json={"is_active": True}).status_code == 200



# -- Papel e exclusão ---------------------------------------------------------

def test_promote_and_demote_user(two_users):
    admin, alice, _bob = two_users
    alice_id = _user_id(admin, "alice")
    assert alice.get("/users").status_code == 403

    r = admin.patch(f"/users/{alice_id}", json={"role": "admin"})
    assert r.status_code == 200 and r.json()["role"] == "admin"
    assert alice.get("/users").status_code == 200
    assert r.json()["is_active"] is True  # PATCH parcial não mexe no que não veio

    assert admin.patch(f"/users/{alice_id}", json={"role": "user"}).status_code == 200
    assert alice.get("/users").status_code == 403


def test_admin_cannot_change_own_role(client):
    r = client.patch(f"/users/{_user_id(client, 'admin')}", json={"role": "user"})
    assert r.status_code == 409
    assert client.get("/users").status_code == 200


def test_delete_user_requires_reassigning_live_backups(two_users):
    admin, alice, _bob = two_users
    alice_id = _user_id(admin, "alice")
    admin_id = _user_id(admin, "admin")
    assert alice.post("/backups", json={"label": "alice-docs"}).status_code in (200, 201)
    key = admin.post(f"/users/{alice_id}/keys", json={"name": "Mac"}).json()

    r = admin.delete(f"/users/{alice_id}")
    assert r.status_code == 409 and "alice-docs" in r.json()["detail"]

    assert admin.patch("/backups/alice-docs/owner", json={"owner_user_id": admin_id}).status_code == 200
    assert admin.delete(f"/users/{alice_id}").status_code == 200
    assert "alice" not in {u["username"] for u in admin.get("/users").json()}
    assert alice.get("/backups").status_code == 401
    client_key = TestClient(m.app)
    client_key.headers.update({"X-API-Key": key["api_key"]})
    assert client_key.get("/backups").status_code == 401
    assert admin.get("/backups/alice-docs").status_code == 200


def test_admin_cannot_delete_self_and_404(client):
    assert client.delete(f"/users/{_user_id(client, 'admin')}").status_code == 409
    assert client.delete("/users/9999").status_code == 404


def test_regular_user_cannot_delete_or_promote(two_users):
    admin, alice, _bob = two_users
    bob_id = _user_id(admin, "bob")
    assert alice.delete(f"/users/{bob_id}").status_code == 403
    assert alice.patch(f"/users/{_user_id(admin, 'alice')}", json={"role": "admin"}).status_code == 403
