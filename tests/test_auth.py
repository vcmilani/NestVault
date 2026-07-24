"""Testa autenticação por usuário: sem chave / chave errada / chave válida,
e que endpoints administrativos exigem role=admin."""
import pytest

from conftest import ADMIN_KEY


PROTECTED_ENDPOINTS = [
    ("GET",    "/backups"),
    ("POST",   "/backups"),
    ("GET",    "/storage/info"),
    ("POST",   "/check"),
    ("POST",   "/check/batch"),
    ("POST",   "/upload"),
    ("POST",   "/maintenance/cleanup-orphans"),
]

ADMIN_ONLY_ENDPOINTS = [
    ("GET",  "/storage/info"),
    ("GET",  "/api/stats"),
    ("POST", "/maintenance/cleanup-orphans"),
    ("GET",  "/users"),
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


@pytest.mark.parametrize("method,path", ADMIN_ONLY_ENDPOINTS)
def test_regular_user_gets_403_on_admin_endpoints(two_users, method, path):
    _admin, alice, _bob = two_users
    r = alice.request(method, path)
    assert r.status_code == 403


@pytest.mark.parametrize("method,path", ADMIN_ONLY_ENDPOINTS)
def test_admin_can_access_admin_endpoints(two_users, method, path):
    admin, _alice, _bob = two_users
    r = admin.request(method, path)
    assert r.status_code == 200
