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


# -- Isolamento de conteúdo por sha256 (A2) ------------------------------------
# Sem essas checagens, um usuário podia "adotar" conteúdo de outro citando o sha256
# (sem nunca enviar os bytes) via /upload register-only ou /register/batch, e depois
# baixá-lo — o download escopa por posse do VersionFile resultante, não do
# FileContent em si.

def _upload_body(session, label, version_key, path, content, mtime=1000.0):
    import base64
    return session.post(
        "/upload", content=content,
        headers={
            "X-Backup-Label": label, "X-Version-Key": version_key,
            "X-Original-Path": base64.b64encode(path.encode()).decode(),
            "X-Mtime": str(mtime),
        },
    )


def _register_only(session, label, version_key, path, sha256, mtime=1000.0):
    import base64
    return session.post(
        "/upload",
        headers={
            "X-Backup-Label": label, "X-Version-Key": version_key,
            "X-Original-Path": base64.b64encode(path.encode()).decode(),
            "X-Mtime": str(mtime), "X-Content-Sha256": sha256,
        },
    )


def test_check_does_not_leak_content_exists_across_users(two_users):
    _admin, alice, bob = two_users
    make_backup(alice, label="alice-docs")
    make_version(alice, label="alice-docs", version_key="2026-01-01T00:00:00")
    up = upload_file(alice, "alice-docs", "2026-01-01T00:00:00",
                      path="/segredo.txt", content=b"conteudo secreto da alice")

    make_backup(bob, label="bob-docs")
    make_version(bob, label="bob-docs", version_key="2026-01-01T00:00:00")
    r = bob.post("/check", json={
        "backup_label": "bob-docs", "version_key": "2026-01-01T00:00:00",
        "original_path": "/x", "sha256": up["sha256"], "size": 25, "mtime": 1.0,
    })
    assert r.status_code == 200
    # O hash existe no storage (é da Alice), mas Bob nunca provou possuí-lo — não
    # pode saber que existe.
    assert r.json()["content_exists"] is False


def test_cannot_register_others_content_by_hash(two_users):
    _admin, alice, bob = two_users
    make_backup(alice, label="alice-docs")
    make_version(alice, label="alice-docs", version_key="2026-01-01T00:00:00")
    up = upload_file(alice, "alice-docs", "2026-01-01T00:00:00",
                      path="/segredo.txt", content=b"conteudo secreto da alice")

    make_backup(bob, label="bob-docs")
    make_version(bob, label="bob-docs", version_key="2026-01-01T00:00:00")
    r = _register_only(bob, "bob-docs", "2026-01-01T00:00:00", "/roubado.txt", up["sha256"])
    assert r.status_code == 400

    # Sem VersionFile criado — nada para baixar.
    files = bob.get("/files", params={"backup_label": "bob-docs",
                                       "version_key": "2026-01-01T00:00:00"}).json()
    assert files == []


def test_cannot_register_batch_others_content_by_hash(two_users):
    _admin, alice, bob = two_users
    make_backup(alice, label="alice-docs")
    make_version(alice, label="alice-docs", version_key="2026-01-01T00:00:00")
    up = upload_file(alice, "alice-docs", "2026-01-01T00:00:00",
                      path="/segredo.txt", content=b"conteudo secreto da alice")

    make_backup(bob, label="bob-docs")
    make_version(bob, label="bob-docs", version_key="2026-01-01T00:00:00")
    r = bob.post("/register/batch", json={
        "backup_label": "bob-docs", "version_key": "2026-01-01T00:00:00",
        "files": [{"original_path": "/roubado.txt", "sha256": up["sha256"], "mtime": 1000.0}],
    })
    assert r.status_code == 200
    data = r.json()
    assert data["registered"] == 0
    assert data["missing"] == 1
    assert data["results"][0]["registered"] is False


def test_dedup_still_works_when_user_uploads_real_bytes(two_users):
    """A2 não quebra a deduplicação de verdade: se Bob envia os BYTES reais (mesmo
    conteúdo da Alice), o servidor ainda dedupa no disco — só o atalho "cite o hash e
    pule o envio" exige posse prévia."""
    _admin, alice, bob = two_users
    make_backup(alice, label="alice-docs")
    make_version(alice, label="alice-docs", version_key="2026-01-01T00:00:00")
    up_alice = upload_file(alice, "alice-docs", "2026-01-01T00:00:00",
                            path="/mesmo.txt", content=b"conteudo identico")

    make_backup(bob, label="bob-docs")
    make_version(bob, label="bob-docs", version_key="2026-01-01T00:00:00")
    r = _upload_body(bob, "bob-docs", "2026-01-01T00:00:00", "/mesmo.txt", b"conteudo identico")
    assert r.status_code == 200
    assert r.json()["sha256"] == up_alice["sha256"]

    d = bob.get(f"/files/{r.json()['file_id']}/download")
    assert d.status_code == 200
    assert d.content == b"conteudo identico"


def test_admin_bypasses_content_visibility_scoping(two_users):
    admin, alice, _bob = two_users
    make_backup(alice, label="alice-docs")
    make_version(alice, label="alice-docs", version_key="2026-01-01T00:00:00")
    up = upload_file(alice, "alice-docs", "2026-01-01T00:00:00",
                      path="/f.txt", content=b"conteudo")

    r = admin.post("/check", json={
        "backup_label": "alice-docs", "version_key": "2026-01-01T00:00:00",
        "original_path": "/outro.txt", "sha256": up["sha256"], "size": 8, "mtime": 1.0,
    })
    assert r.status_code == 200
    assert r.json()["content_exists"] is True
