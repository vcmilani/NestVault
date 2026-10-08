"""Chaves de cliente (api_keys): autenticam o mesmo usuário da chave principal,
com os mesmos backups, mas nunca com poderes de admin — inclusive pelo cookie
de sessão do front cloud."""
import pytest
from fastapi.testclient import TestClient

import main as m
from conftest import make_backup, make_version, finish_version, upload_file
from test_auth import ADMIN_ONLY_ENDPOINTS, ADMIN_ONLY_NOT_SMOKEABLE

V1 = "2026-01-01T00:00:00"


def _user_id(admin, username):
    return next(u["id"] for u in admin.get("/users").json() if u["username"] == username)


def _new_key(admin, username, name="MacBook"):
    uid = _user_id(admin, username)
    r = admin.post(f"/users/{uid}/keys", json={"name": name})
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["key"]["scope"] == "client"
    return uid, data["key"]["id"], data["api_key"]


def _as(key):
    c = TestClient(m.app)  # sem `with`: o lifespan já roda no fixture
    c.headers.update({"X-API-Key": key})
    return c


@pytest.fixture
def admin_client_key(two_users):
    admin, alice, bob = two_users
    _uid, _kid, raw = _new_key(admin, "admin")
    return admin, _as(raw), alice, bob


@pytest.mark.parametrize("method,path", ADMIN_ONLY_ENDPOINTS + ADMIN_ONLY_NOT_SMOKEABLE)
def test_admin_client_key_is_not_admin(admin_client_key, method, path):
    _admin, client_key, _alice, _bob = admin_client_key
    assert client_key.request(method, path).status_code == 403


def test_client_key_sees_only_own_backups(admin_client_key):
    admin, client_key, _alice, bob = admin_client_key
    make_backup(admin, label="admin-docs")
    make_backup(bob, label="bob-docs")
    make_version(bob, label="bob-docs", version_key=V1)
    upload_file(bob, "bob-docs", V1, path="/b/x.txt", content=b"bob")
    finish_version(bob, "bob-docs", V1)

    assert {b["label"] for b in client_key.get("/backups").json()} == {"admin-docs"}
    assert client_key.get("/backups/bob-docs").status_code == 403
    assert client_key.get("/cloud/tree", params={"label": "bob-docs"}).status_code == 403
    # Mesmo backup do próprio usuário (mesmo dono): a chave de cliente escreve nele
    make_version(client_key, label="admin-docs", version_key=V1)

    file_id = next(f["id"] for f in admin.get("/cloud/tree", params={"label": "bob-docs"}).json()["files"])
    assert admin.get(f"/cloud/content/{file_id}").status_code == 200
    assert client_key.get(f"/cloud/content/{file_id}").status_code == 403


def test_me_reports_effective_role(admin_client_key):
    admin, client_key, _alice, _bob = admin_client_key
    assert admin.get("/cloud/me").json() == {"username": "admin", "role": "admin", "scope": "full"}
    assert client_key.get("/cloud/me").json() == {"username": "admin", "role": "user", "scope": "client"}


def test_session_cookie_keeps_client_scope_and_dies_on_revoke(two_users):
    admin, _alice, bob = two_users
    uid, kid, raw = _new_key(admin, "admin")
    make_backup(bob, label="bob-docs")
    make_version(bob, label="bob-docs", version_key=V1)
    upload_file(bob, "bob-docs", V1, path="/b/x.txt", content=b"bob")
    finish_version(bob, "bob-docs", V1)
    file_id = next(f["id"] for f in admin.get("/cloud/tree", params={"label": "bob-docs"}).json()["files"])

    browser = TestClient(m.app)
    r = browser.post("/cloud/session", headers={"X-API-Key": raw})
    assert r.json()["role"] == "user"
    assert browser.get("/cloud/me").json()["scope"] == "client"
    assert browser.get(f"/cloud/content/{file_id}").status_code == 403

    assert admin.delete(f"/users/{uid}/keys/{kid}").status_code == 200
    assert browser.get("/cloud/me").status_code == 401
    assert _as(raw).get("/backups").status_code == 401


def test_list_and_revoke_keys(two_users):
    admin, _alice, _bob = two_users
    uid, kid, raw = _new_key(admin, "alice", name="Notebook")
    keys = admin.get(f"/users/{uid}/keys").json()
    assert [(k["id"], k["name"]) for k in keys] == [(kid, "Notebook")]
    assert _as(raw).get("/backups").status_code == 200
    assert admin.get(f"/users/{uid}/keys").json()[0]["last_used_at"] is not None

    other = _user_id(admin, "bob")
    assert admin.delete(f"/users/{other}/keys/{kid}").status_code == 404  # chave de outro usuário
    assert admin.delete(f"/users/{uid}/keys/{kid}").status_code == 200
    assert admin.get(f"/users/{uid}/keys").json() == []


def test_deactivated_user_loses_client_keys(two_users):
    admin, _alice, _bob = two_users
    uid, _kid, raw = _new_key(admin, "alice")
    assert admin.patch(f"/users/{uid}", json={"is_active": False}).status_code == 200
    assert _as(raw).get("/backups").status_code == 401


def test_regular_user_cannot_manage_keys(two_users):
    admin, alice, _bob = two_users
    uid = _user_id(admin, "alice")
    assert alice.post(f"/users/{uid}/keys", json={"name": "x"}).status_code == 403
    assert alice.get(f"/users/{uid}/keys").status_code == 403
