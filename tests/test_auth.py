"""Testa que os endpoints protegidos retornam 401 sem chave e 200 com chave válida."""
import pytest


PROTECTED_ENDPOINTS = [
    ("GET",    "/backups"),
    ("POST",   "/backups"),
    ("GET",    "/storage/info"),
    ("POST",   "/check"),
    ("POST",   "/check/batch"),
    ("POST",   "/upload"),
    ("POST",   "/maintenance/cleanup-orphans"),
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
    r = auth_client.get("/backups", headers={"X-API-Key": "testkey"})
    assert r.status_code == 200


def test_no_auth_enabled_allows_access(client):
    """Sem API_KEY configurada, todos os endpoints são acessíveis sem cabeçalho."""
    r = client.get("/backups")
    assert r.status_code == 200


def test_health_never_requires_auth(auth_client):
    r = auth_client.get("/health")
    assert r.status_code == 200
