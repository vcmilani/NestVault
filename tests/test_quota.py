"""Cota por usuário (v10.2.0): uso por sha256 distinto, 507 em conteúdo novo,
registro do que já é seu passa, versão nova bloqueada acima da cota, isolamento
entre usuários, aviso de 90% e a cota exposta em /users e /cloud/me."""
import base64

import pytest

import database as db_mod
import main as m
import notify
import quota
from conftest import make_backup, make_version, finish_version, upload_file

V1 = "2026-01-01T00:00:00"
V2 = "2026-01-02T00:00:00"


@pytest.fixture
def sent(monkeypatch):
    msgs = []
    monkeypatch.setattr(notify, "send_sync", lambda text, **kw: msgs.append(text) or True)
    return msgs


def _uid(name):
    db = m.SessionLocal()
    try:
        return db.query(db_mod.User.id).filter(db_mod.User.username == name).scalar()
    finally:
        db.close()


def _set_quota(admin, name, nbytes, expect=200):
    r = admin.patch(f"/users/{_uid(name)}", json={"quota_bytes": nbytes})
    assert r.status_code == expect, r.text
    return r.json()


def _upload(c, label, key, path, content, expect=200):
    r = c.post("/upload", content=content, headers={
        "X-Backup-Label": label, "X-Version-Key": key,
        "X-Original-Path": base64.b64encode(path.encode()).decode(), "X-Mtime": "1000"})
    assert r.status_code == expect, r.text
    return r


def _usage(name):
    quota.reset()
    db = m.SessionLocal()
    try:
        return quota.usage(db, _uid(name))
    finally:
        db.close()


def test_usage_counts_distinct_content(two_users):
    _admin, alice, _bob = two_users
    make_backup(alice, "a")
    make_version(alice, "a", V1)
    upload_file(alice, "a", V1, "/x.bin", b"1" * 100)
    upload_file(alice, "a", V1, "/copia.bin", b"1" * 100)  # mesmo conteúdo
    upload_file(alice, "a", V1, "/y.bin", b"2" * 50)
    finish_version(alice, "a", V1)
    make_version(alice, "a", V2)
    upload_file(alice, "a", V2, "/x.bin", b"1" * 100)      # mesma coisa em outra versão
    finish_version(alice, "a", V2)
    assert _usage("alice") == 150
    assert _usage("bob") == 0


def test_upload_new_content_over_quota_is_507(two_users, sent):
    admin, alice, _bob = two_users
    _set_quota(admin, "alice", 120)
    make_backup(alice, "a")
    make_version(alice, "a", V1)
    _upload(alice, "a", V1, "/x.bin", b"1" * 100)
    r = _upload(alice, "a", V1, "/y.bin", b"2" * 50, expect=507)
    assert "Cota excedida" in r.json()["detail"]
    # Conteúdo que ela já tem não soma — passa mesmo perto do limite.
    _upload(alice, "a", V1, "/copia.bin", b"1" * 100)
    files = {f["original_path"] for f in alice.get(
        "/files", params={"backup_label": "a", "version_key": V1}).json()}
    assert files == {"/x.bin", "/copia.bin"}
    # 100/120 = 83% < 90%: sem aviso ainda.
    assert sent == []


def test_quota_full_blocks_new_version_and_warns(two_users, sent):
    admin, alice, _bob = two_users
    _set_quota(admin, "alice", 100)
    make_backup(alice, "a")
    make_version(alice, "a", V1)
    _upload(alice, "a", V1, "/x.bin", b"1" * 100)
    assert len(sent) == 1 and "alice" in sent[0] and "100%" in sent[0]
    finish_version(alice, "a", V1)
    r = alice.post("/backups/a/versions", json={"version_key": V2})
    assert r.status_code == 507
    # Aviso não repete.
    _upload(alice, "a", V1, "/copia.bin", b"1" * 100, expect=409)  # versão done
    assert len(sent) == 1
    # Aumentar a cota libera e rearma o aviso.
    info = _set_quota(admin, "alice", 1000)
    assert info["quota_bytes"] == 1000 and info["usage_bytes"] == 100
    assert alice.post("/backups/a/versions", json={"version_key": V2}).status_code == 200


def test_no_quota_is_unlimited_and_null_removes(two_users):
    admin, alice, _bob = two_users
    make_backup(alice, "a")
    make_version(alice, "a", V1)
    _upload(alice, "a", V1, "/x.bin", b"1" * 500)
    _set_quota(admin, "alice", 10)
    _upload(alice, "a", V1, "/y.bin", b"2" * 5, expect=507)
    assert _set_quota(admin, "alice", None)["quota_bytes"] is None
    _upload(alice, "a", V1, "/y.bin", b"2" * 5)


def test_other_users_content_counts_for_both(two_users):
    admin, alice, bob = two_users
    make_backup(alice, "a")
    make_version(alice, "a", V1)
    _upload(alice, "a", V1, "/x.bin", b"1" * 100)
    _set_quota(admin, "bob", 50)
    make_backup(bob, "b")
    make_version(bob, "b", V1)
    # Já existe no storage (da alice), mas é novo para o bob: conta para ele.
    _upload(bob, "b", V1, "/x.bin", b"1" * 100, expect=507)


def test_drive_upload_respects_quota(two_users):
    admin, alice, _bob = two_users
    _set_quota(admin, "alice", 100)
    label = alice.post("/cloud/drives", json={"name": "Docs"}).json()["label"]

    def put(path, content):
        return alice.put("/cloud/drive/upload", content=content, headers={
            "X-Label": label, "X-Path": base64.b64encode(path.encode()).decode()})
    assert put("a.bin", b"1" * 80).status_code == 200
    assert put("b.bin", b"2" * 30).status_code == 507
    me = alice.get("/cloud/me").json()
    assert me["quota_bytes"] == 100 and me["usage_bytes"] == 80


def test_users_list_shows_usage_and_only_admin_sets(two_users):
    admin, alice, _bob = two_users
    make_backup(alice, "a")
    make_version(alice, "a", V1)
    _upload(alice, "a", V1, "/x.bin", b"1" * 64)
    users = {u["username"]: u for u in admin.get("/users").json()}
    assert users["alice"]["usage_bytes"] == 64 and users["alice"]["quota_bytes"] is None
    r = alice.patch(f"/users/{_uid('alice')}", json={"quota_bytes": 10 ** 12})
    assert r.status_code == 403
    _set_quota(admin, "alice", -1, expect=422)
