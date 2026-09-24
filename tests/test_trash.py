"""Lixeira: exclusões de usuário comum são reversíveis pelo admin até o prazo.

Protege contra uma máquina cliente comprometida — a chave de API mora nela, então
a exclusão feita com essa chave não pode ser a última palavra."""
import base64
from datetime import datetime, timedelta

import database as db_mod
import main as m
from conftest import make_backup, make_version, finish_version, upload_file
from nightly_cleanup import purge_trash, _cleanup_orphan_contents

V1, V2 = "2026-01-01T00:00:00", "2026-01-02T00:00:00"


def _seed(c, label="alice-docs", content=b"precious"):
    make_backup(c, label)
    make_version(c, label, V1)
    up = upload_file(c, label, V1, path="/a.txt", content=content)
    finish_version(c, label, V1)
    make_version(c, label, V2)
    upload_file(c, label, V2, path="/a.txt", content=content)
    finish_version(c, label, V2)
    return up


def _content_on_disk(tmp_vol, sha):
    return (tmp_vol / "_content" / sha[:2] / sha).exists()


def test_user_delete_label_goes_to_trash_and_admin_restores(two_users, tmp_vol):
    admin, alice, _ = two_users
    up = _seed(alice)

    r = alice.delete("/backups/alice-docs")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "deleted" and body["trashed"] is True and body["purge_after"]

    # Some para todo mundo, inclusive o admin, fora da tela da lixeira...
    assert alice.get("/backups").json() == []
    assert all(b["label"] != "alice-docs" for b in admin.get("/backups").json())
    assert alice.get("/backups/alice-docs").status_code == 404
    assert alice.get(f"/files/{up['file_id']}/download").status_code == 404
    # ...mas nada foi apagado.
    assert _content_on_disk(tmp_vol, up["sha256"])
    trash = admin.get("/maintenance/trash").json()
    assert [b["label"] for b in trash["labels"]] == ["alice-docs"]
    assert {v["version_key"] for v in trash["versions"]} == {V1, V2}
    assert trash["labels"][0]["trashed_by"] == "alice"

    r = admin.post("/maintenance/trash/restore", json={"label": "alice-docs"})
    assert r.status_code == 200 and r.json()["label_restored"] is True

    versions = alice.get("/backups/alice-docs/versions").json()
    assert {v["version_key"]: v["status"] for v in versions} == {V1: "done", V2: "done"}
    assert alice.get(f"/files/{up['file_id']}/download").content == b"precious"


def test_user_delete_version_and_cleanup_go_to_trash(two_users):
    admin, alice, _ = two_users
    _seed(alice)

    r = alice.delete(f"/backups/alice-docs/versions/{V1}")
    assert r.status_code == 200 and r.json()["trashed"] is True
    assert [v["version_key"] for v in alice.get("/backups/alice-docs/versions").json()] == [V2]

    r = alice.post("/backups/alice-docs/cleanup", json={"backup_label": "alice-docs", "keep": 0})
    assert r.status_code == 200
    assert r.json()["versions_removed"] == [V2] and r.json()["trashed"] is True
    assert alice.get("/backups/alice-docs/versions").json() == []

    r = admin.post("/maintenance/trash/restore", json={"label": "alice-docs", "version_key": V1})
    assert r.json()["versions_restored"] == [V1]
    assert [v["version_key"] for v in alice.get("/backups/alice-docs/versions").json()] == [V1]


def test_admin_delete_is_still_immediate(client, tmp_vol):
    up = _seed(client, label="adm")
    assert client.delete("/backups/adm").json()["trashed"] is False
    trash = client.get("/maintenance/trash").json()
    assert trash["labels"] == [] and trash["versions"] == []
    db = m.SessionLocal()
    try:
        assert db.query(db_mod.BackupID).filter_by(label="adm").first() is None
    finally:
        db.close()


def test_finished_version_status_is_immutable(two_users):
    _, alice, _ = two_users
    _seed(alice)
    url = f"/backups/alice-docs/versions/{V1}"
    # Antes: done → failed era aceito, e a limpeza noturna apaga versões failed.
    assert alice.patch(url, json={"status": "failed"}).status_code == 409
    assert alice.patch(url, json={"status": "done"}).status_code == 200  # retry idempotente


def test_trashed_version_rejects_writes_and_reuse(two_users):
    _, alice, _ = two_users
    _seed(alice)
    make_version(alice, "alice-docs", "2026-01-03T00:00:00")
    alice.delete("/backups/alice-docs/versions/2026-01-03T00:00:00")

    r = alice.post("/upload", content=b"x", headers={
        "X-Backup-Label": "alice-docs", "X-Version-Key": "2026-01-03T00:00:00",
        "X-Original-Path": base64.b64encode(b"/x").decode(), "X-Mtime": "1"})
    assert r.status_code == 404
    r = alice.post("/backups/alice-docs/versions", json={"version_key": "2026-01-03T00:00:00"})
    assert r.status_code == 409


def test_new_backup_revives_trashed_label_without_old_versions(two_users):
    admin, alice, _ = two_users
    _seed(alice)
    alice.delete("/backups/alice-docs")

    r = alice.post("/backups", json={"label": "alice-docs"})
    assert r.status_code == 200 and r.json()["created"] is True
    assert alice.get("/backups/alice-docs/versions").json() == []
    assert len(admin.get("/maintenance/trash").json()["versions"]) == 2


def test_other_user_cannot_touch_trash(two_users):
    _, alice, bob = two_users
    _seed(alice)
    alice.delete("/backups/alice-docs")
    assert bob.get("/maintenance/trash").status_code == 403
    assert bob.post("/maintenance/trash/restore", json={"label": "alice-docs"}).status_code == 403
    assert alice.post("/maintenance/trash/purge").status_code == 403
    # Nem reativar o label de outra pessoa criando um backup com o mesmo nome.
    assert bob.post("/backups", json={"label": "alice-docs"}).status_code == 403


def test_purge_respects_retention_then_frees_content(two_users, tmp_vol):
    admin, alice, _ = two_users
    up = _seed(alice)
    alice.delete("/backups/alice-docs")

    db = m.SessionLocal()
    try:
        # Dentro do prazo: nada sai.
        assert purge_trash(db, datetime.now() - timedelta(days=14)) == (0, 0)
        # Vencido: versões e label saem; o conteúdo vira órfão e é liberado.
        db.query(db_mod.BackupVersion).update({"trashed_at": datetime.now() - timedelta(days=30)})
        db.query(db_mod.BackupID).update({"trashed_at": datetime.now() - timedelta(days=30)})
        db.commit()
        assert purge_trash(db, datetime.now() - timedelta(days=14)) == (2, 1)
        assert _content_on_disk(tmp_vol, up["sha256"])
        _cleanup_orphan_contents(db)
    finally:
        db.close()
    assert not _content_on_disk(tmp_vol, up["sha256"])
    assert admin.get("/maintenance/trash").json()["labels"] == []


def test_admin_purge_now(two_users):
    admin, alice, _ = two_users
    _seed(alice)
    alice.delete("/backups/alice-docs")
    r = admin.post("/maintenance/trash/purge")
    assert r.json() == {"versions_purged": 2, "labels_purged": 1}
    assert admin.post("/maintenance/trash/restore", json={"label": "alice-docs"}).status_code == 404
