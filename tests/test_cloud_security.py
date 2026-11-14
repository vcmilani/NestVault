"""Revisão v10.4.0 — segurança do servidor e do Cloud: PIN das ocultas em todas
as rotas do /cloud, zip sem '..', sessão revogável no logout, freio em chave
inválida, bloqueio do PIN persistido, cookie seguro só atrás de proxy confiável,
label validado, docs fechados, segredos mascarados e redigidos no log."""
import logging
import time

import pytest
from fastapi.testclient import TestClient

import auth as auth_mod
import config
import database as db_mod
import main as m
import smart
from cloud import rclone_runner
from conftest import make_backup, make_version, finish_version, upload_file

V1 = "2026-01-01T00:00:00"


def _alice_id(admin):
    return next(u["id"] for u in admin.get("/users").json() if u["username"] == "alice")


def _hidden_backup(c, label="icloud"):
    make_backup(c, label)
    make_version(c, label, V1)
    vis = upload_file(c, label, V1, path="/PrimarySync/All Photos/a.jpg", content=b"A" * 10)
    hid = upload_file(c, label, V1, path="/PrimarySync/Hidden/segredo.jpg", content=b"S" * 10)
    finish_version(c, label, V1)
    return vis["file_id"], hid["file_id"]


# -- PIN das ocultas vale no /cloud inteiro ------------------------------------------

def test_hidden_folders_are_invisible_in_cloud_until_pin_unlock(two_users):
    admin, alice, _bob = two_users
    vis_id, hid_id = _hidden_backup(alice)

    # Sem PIN: tudo aberto, como antes.
    assert {f["name"] for f in alice.get("/cloud/tree", params={"label": "icloud"}).json()["folders"]} == {"All Photos", "Hidden"}

    admin.put(f"/users/{_alice_id(admin)}/hidden-pin", json={"pin": "1234"})

    tree = alice.get("/cloud/tree", params={"label": "icloud"}).json()
    assert {f["name"] for f in tree["folders"]} == {"All Photos"}
    assert alice.get("/cloud/tree", params={"label": "icloud", "path": "Hidden"}).status_code == 404
    assert alice.get("/cloud/search", params={"label": "icloud", "q": "segredo"}).json()["results"] == []
    assert alice.get("/cloud/search", params={"label": "icloud", "q": "a.jpg"}).json()["results"]
    assert alice.get(f"/cloud/content/{hid_id}").status_code == 404
    assert alice.get(f"/cloud/content/{vis_id}").status_code == 200
    assert alice.get(f"/cloud/photo/{hid_id}/full").status_code == 404
    assert alice.get("/cloud/history", params={"label": "icloud", "path": "Hidden/segredo.jpg"}).status_code == 404
    assert alice.post("/cloud/zip", data={"label": "icloud", "items": ["Hidden"]}).status_code == 404
    r = alice.post("/cloud/zip", data={"label": "icloud", "items": [""], "dry_run": True})
    assert r.status_code == 200 and r.json()["files"] == 1

    # Desbloqueado: volta tudo.
    assert alice.post("/cloud/photos/unlock", json={"pin": "1234"}).status_code == 200
    assert {f["name"] for f in alice.get("/cloud/tree", params={"label": "icloud"}).json()["folders"]} == {"All Photos", "Hidden"}
    assert alice.get(f"/cloud/content/{hid_id}").status_code == 200
    assert alice.post("/cloud/zip", data={"label": "icloud", "items": [""], "dry_run": True}).json()["files"] == 2


def test_hidden_thumbs_and_duplicates_respect_pin(two_users):
    admin, alice, _bob = two_users
    make_backup(alice, "dup")
    make_version(alice, "dup", V1)
    big = b"X" * (1024 * 1024 + 1)
    upload_file(alice, "dup", V1, path="/root/Hidden/a.bin", content=big)
    upload_file(alice, "dup", V1, path="/root/Hidden/b.bin", content=big)
    upload_file(alice, "dup", V1, path="/root/public/c.bin", content=big)
    finish_version(alice, "dup", V1)
    assert alice.get("/cloud/duplicates").json()["groups"][0]["count"] == 3

    admin.put(f"/users/{_alice_id(admin)}/hidden-pin", json={"pin": "1234"})
    assert alice.get("/cloud/duplicates").json()["groups"] == []  # só sobrou uma cópia visível


# -- zip sem '..' ---------------------------------------------------------------------

def test_zip_entries_never_escape_with_dotdot(client):
    import io, zipfile
    make_backup(client, "b1")
    make_version(client, "b1", V1)
    upload_file(client, "b1", V1, path="/base/ok.txt", content=b"ok")
    upload_file(client, "b1", V1, path="/base/x/../../etc/evil.txt", content=b"evil")
    finish_version(client, "b1", V1)
    r = client.post("/cloud/zip", data={"label": "b1", "items": [""]})
    assert r.status_code == 200
    names = zipfile.ZipFile(io.BytesIO(r.content)).namelist()
    assert len(names) == 2
    assert all(".." not in n.split("/") and not n.startswith("/") for n in names), names


# -- sessão revogável -----------------------------------------------------------------

def test_logout_revokes_session_token_server_side(two_users):
    _admin, alice, _bob = two_users
    make_backup(alice, "a")
    browser = TestClient(m.app)
    token = browser.post("/cloud/session", headers={"X-API-Key": "alice-key"}).cookies["nv_session"]
    assert browser.get("/cloud/tree").status_code == 200
    browser.delete("/cloud/session")
    browser.cookies.clear()
    browser.cookies.set("nv_session", token)  # token copiado antes do logout
    assert browser.get("/cloud/tree").status_code == 401
    db = m.SessionLocal()
    assert db.query(db_mod.RevokedSession).count() == 1
    db.query(db_mod.RevokedSession).update({"exp": int(time.time()) - 1})
    db.commit()
    assert auth_mod.purge_revoked_sessions(db) == 1
    db.close()


# -- freio em chave inválida -----------------------------------------------------------

def test_too_many_invalid_keys_from_one_ip_get_429(auth_client):
    for _ in range(auth_mod._AUTH_MAX_FAILS):
        assert auth_client.get("/backups", headers={"X-API-Key": "errada"}).status_code == 401
    assert auth_client.get("/backups", headers={"X-API-Key": "errada"}).status_code == 429
    assert auth_client.get("/backups", headers={"X-API-Key": "testkey"}).status_code == 429
    auth_mod._auth_fails.clear()
    assert auth_client.get("/backups", headers={"X-API-Key": "testkey"}).status_code == 200
    assert auth_client.get("/health").status_code == 200  # sem chave não conta


# -- bloqueio do PIN persistido ---------------------------------------------------------

def test_pin_lockout_survives_restart(two_users):
    admin, alice, _bob = two_users
    alice_id = _alice_id(admin)
    admin.put(f"/users/{alice_id}/hidden-pin", json={"pin": "1234"})
    for _ in range(auth_mod.PIN_MAX_FAILS):
        assert alice.post("/cloud/photos/unlock", json={"pin": "0000"}).status_code == 403
    assert alice.post("/cloud/photos/unlock", json={"pin": "1234"}).status_code == 429
    auth_mod._pin_fails.clear()  # "restart": o estado em memória some, o do banco fica
    assert alice.post("/cloud/photos/unlock", json={"pin": "1234"}).status_code == 429
    db = m.SessionLocal()
    u = db.get(db_mod.User, alice_id)
    assert u.pin_locked_until and u.pin_locked_until > time.time()
    db.close()
    # PIN novo pelo admin zera tudo, inclusive no banco.
    admin.put(f"/users/{alice_id}/hidden-pin", json={"pin": "4321"})
    assert alice.post("/cloud/photos/unlock", json={"pin": "4321"}).status_code == 200


# -- cookie seguro só atrás de proxy confiável -----------------------------------------

def test_forwarded_proto_only_trusted_from_configured_proxies(two_users, monkeypatch):
    _admin, alice, _bob = two_users
    hdrs = {"X-API-Key": "alice-key", "X-Forwarded-Proto": "https"}
    r = TestClient(m.app).post("/cloud/session", headers=hdrs)
    assert "secure" not in r.headers["set-cookie"].lower()
    monkeypatch.setitem(config._data["server"], "trusted_proxies", ["testclient"])
    r = TestClient(m.app).post("/cloud/session", headers=hdrs)
    assert "secure" in r.headers["set-cookie"].lower()


# -- label validado ------------------------------------------------------------------------

@pytest.mark.parametrize("bad", ["a/b", "a\\b", " x", "x ", "", "a\x00b", "x" * 129])
def test_backup_label_rejects_unsafe_values(client, bad):
    assert client.post("/backups", json={"label": bad}).status_code == 422


def test_backup_label_accepts_normal_names(client):
    for label in ("docs", "iCloud Photos", "srv_2026.bkp", "fotos-da-família"):
        assert client.post("/backups", json={"label": label}).status_code == 200, label
    assert client.patch("/backups/docs/rename", json={"new_label": "a/b"}).status_code in (404, 422)


# -- pasta do servidor não é alvo de rclone -------------------------------------------------

def test_rclone_job_cannot_target_server_folder(client):
    label = client.post("/cloud/drives", json={"name": "Docs"}).json()["label"]
    r = client.post("/rclone/jobs", json={"remote_name": "gdrive", "display_name": "D", "target_label": label})
    assert r.status_code == 422
    r = client.post("/rclone/jobs", json={"remote_name": "gdrive", "display_name": "D", "target_label": "drive"})
    assert r.status_code == 201
    assert client.patch(f"/rclone/jobs/{r.json()['id']}", json={"target_label": label}).status_code == 422


def test_rclone_version_key_is_unique_within_the_same_second(client):
    make_backup(client, "drive")
    db = m.SessionLocal()
    k1 = rclone_runner._unique_version_key(db, "drive")
    db.add(db_mod.BackupVersion(backup_label="drive", version_key=k1, status="done"))
    db.commit()
    k2 = rclone_runner._unique_version_key(db, "drive")
    assert k2 != k1 and k2.startswith(k1)
    db.close()


# -- docs, mascaramento, log ---------------------------------------------------------------

def test_api_docs_are_closed_by_default(client):
    assert client.get("/docs").status_code == 404
    assert client.get("/openapi.json").status_code == 404


def test_secrets_are_fully_masked(monkeypatch):
    assert config._mask("1234567890abcdef") == "••••••"
    assert config._mask("") == ""


def test_log_filter_redacts_telegram_token():
    rec = logging.LogRecord("httpx", logging.INFO, __file__, 1,
                            'HTTP Request: POST https://api.telegram.org/bot000000000:FAKETOKENFORTESTONLYxxxxxxxxxxxxxx/sendMessage', (), None)
    # Token sintético (zeros + FAKETOKEN…): só precisa ter o formato bot<dígitos>:<20+ chars>.
    assert m._RedactSecrets().filter(rec) is True
    assert "FAKETOKEN" not in rec.getMessage() and "bot<redigido>" in rec.getMessage()


def test_smart_overrides_reject_option_like_devices(monkeypatch):
    monkeypatch.setitem(config._data["smart"], "device_overrides",
                        ["/mnt/x=-d sat", "/mnt/y=/dev/sdb:sat", "/mnt/z=/dev/sdc:--bad"])
    assert smart._overrides() == {"/mnt/y": ("/dev/sdb", "sat")}


def test_ollama_url_must_be_http(isolated_config=None):
    with pytest.raises(config.ConfigError):
        config._check(config.BY_KEY["digest.ollama_url"], "file:///etc/passwd")
    config._check(config.BY_KEY["digest.ollama_url"], "http://localhost:11434")


# -- estatísticas: backups_days ----------------------------------------------------------------

def test_backups_days_excludes_trashed_versions_and_server_folders(two_users):
    _admin, alice, _bob = two_users
    make_backup(alice, "a")
    for key in (V1, "2026-01-02T00:00:00"):
        make_version(alice, "a", key)
        upload_file(alice, "a", key, path="/f.txt", content=key.encode())
        finish_version(alice, "a", key)
    assert alice.delete("/backups/a/versions/2026-01-02T00:00:00").status_code == 200  # vai para a lixeira
    alice.post("/cloud/drives", json={"name": "Docs"})  # pasta do servidor: versão live "done"
    db = m.SessionLocal()
    try:
        days = m._build_stats_data(db).backups_days
    finally:
        db.close()
    assert sum(d.count for d in days) == 1


# -- pastas do servidor: rename só de caixa e lixeira com a pasta na lixeira do admin ----

def _drive(c, name="Docs"):
    r = c.post("/cloud/drives", json={"name": name})
    assert r.status_code == 200, r.text
    return r.json()["label"]


def test_case_only_rename_still_checks_for_conflicts(client):
    label = _drive(client)
    assert client.post("/cloud/drive/mkdir", json={"label": label, "path": "Foo"}).status_code == 200
    assert client.post("/cloud/drive/mkdir", json={"label": label, "path": "foo"}).status_code == 200
    r = client.post("/cloud/drive/rename", json={"label": label, "path": "Foo", "new_name": "foo"})
    assert r.status_code == 409, r.text
    r = client.post("/cloud/drive/rename", json={"label": label, "path": "Foo", "new_name": "FOO"})
    assert r.status_code == 200, r.text


def test_expired_server_trash_is_purged_even_with_folder_in_admin_trash(two_users):
    import base64
    from datetime import datetime, timedelta
    from cloud_ui import drive as cloud_drive
    _admin, alice, _bob = two_users
    label = _drive(alice)
    r = alice.put("/cloud/drive/upload", content=b"conteudo", headers={
        "X-Label": label, "X-Path": base64.b64encode(b"a.txt").decode(), "X-Mtime": "1000"})
    assert r.status_code == 200, r.text
    assert alice.post("/cloud/drive/delete", json={"label": label, "items": ["a.txt"]}).json()["trashed"] == 1
    # Usuário comum exclui a pasta inteira: ela vai para a lixeira do admin, e a
    # versão "trash" dela passa a ter status trashed.
    assert alice.delete(f"/backups/{label}").status_code == 200

    db = m.SessionLocal()
    try:
        db.query(db_mod.ServerTrashEntry).update({"deleted_at": datetime.now() - timedelta(days=100)})
        db.commit()
        trash_vid = (db.query(db_mod.BackupVersion.id)
                     .filter_by(backup_label=label, version_key=cloud_drive.SERVER_TRASH_KEY).scalar())
        assert db.query(db_mod.VersionFile).filter_by(version_id=trash_vid).count() == 1

        assert cloud_drive.purge_expired(db, datetime.now() - timedelta(days=1)) == 1

        assert db.query(db_mod.ServerTrashEntry).count() == 0
        assert db.query(db_mod.VersionFile).filter_by(version_id=trash_vid).count() == 0, \
            "VersionFile com token da lixeira ficou para trás (conteúdo preso no disco)"
    finally:
        db.close()
