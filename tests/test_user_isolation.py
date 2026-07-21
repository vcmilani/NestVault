"""Backup por usuário: cada usuário só enxerga/escreve/restaura seus próprios
backups; admin enxerga e restaura tudo."""
from conftest import make_backup, make_version, finish_version, upload_file


def test_list_backups_scoped_to_owner(two_users):
    admin, alice, bob = two_users
    make_backup(alice, label="alice-docs")
    make_backup(bob, label="bob-docs")

    alice_labels = {b["label"] for b in alice.get("/backups").json()}
    bob_labels = {b["label"] for b in bob.get("/backups").json()}
    admin_labels = {b["label"] for b in admin.get("/backups").json()}

    assert alice_labels == {"alice-docs"}
    assert bob_labels == {"bob-docs"}
    assert admin_labels == {"alice-docs", "bob-docs"}


def test_cannot_recreate_existing_label_of_another_user(two_users):
    _admin, alice, bob = two_users
    make_backup(alice, label="alice-docs")
    r = bob.post("/backups", json={"label": "alice-docs"})
    assert r.status_code == 403


def test_cannot_read_others_backup(two_users):
    _admin, alice, bob = two_users
    make_backup(alice, label="alice-docs")
    assert bob.get("/backups/alice-docs").status_code == 403


def test_cannot_create_version_on_others_label(two_users):
    _admin, alice, bob = two_users
    make_backup(alice, label="alice-docs")
    r = bob.post("/backups/alice-docs/versions", json={"version_key": "2026-01-01T00:00:00"})
    assert r.status_code == 403


def test_cannot_list_versions_of_others_label(two_users):
    _admin, alice, bob = two_users
    make_backup(alice, label="alice-docs")
    make_version(alice, label="alice-docs")
    assert bob.get("/backups/alice-docs/versions").status_code == 403


def test_cannot_upload_to_others_label(two_users):
    _admin, alice, bob = two_users
    make_backup(alice, label="alice-docs")
    make_version(alice, label="alice-docs", version_key="2026-01-01T00:00:00")

    import base64
    r = bob.post(
        "/upload",
        content=b"intruso",
        headers={
            "X-Backup-Label": "alice-docs",
            "X-Version-Key": "2026-01-01T00:00:00",
            "X-Original-Path": base64.b64encode(b"/evil.txt").decode(),
            "X-Mtime": "1000.0",
        },
    )
    assert r.status_code == 403


def test_cannot_download_others_file(two_users):
    admin, alice, bob = two_users
    make_backup(alice, label="alice-docs")
    make_version(alice, label="alice-docs", version_key="2026-01-01T00:00:00")
    upload_file(alice, "alice-docs", "2026-01-01T00:00:00", path="/secret.txt", content=b"segredo")
    finish_version(alice, "alice-docs", "2026-01-01T00:00:00")

    files = alice.get("/files", params={"backup_label": "alice-docs", "version_key": "2026-01-01T00:00:00"}).json()
    file_id = files[0]["id"]

    # Dono baixa normalmente
    assert alice.get(f"/files/{file_id}/download").status_code == 200
    # Outro usuário comum não pode
    assert bob.get(f"/files/{file_id}/download").status_code == 403
    # Admin pode
    assert admin.get(f"/files/{file_id}/download").status_code == 200


def test_cannot_delete_others_label(two_users):
    _admin, alice, bob = two_users
    make_backup(alice, label="alice-docs")
    assert bob.delete("/backups/alice-docs").status_code == 403
    # label continua existindo
    assert alice.get("/backups/alice-docs").status_code == 200


def test_owner_can_manage_own_backup_end_to_end(two_users):
    _admin, alice, _bob = two_users
    make_backup(alice, label="alice-docs")
    make_version(alice, label="alice-docs", version_key="2026-01-01T00:00:00")
    upload_file(alice, "alice-docs", "2026-01-01T00:00:00")
    finish_version(alice, "alice-docs", "2026-01-01T00:00:00")

    assert alice.get("/backups/alice-docs/versions").status_code == 200
    assert alice.delete("/backups/alice-docs").status_code == 200
