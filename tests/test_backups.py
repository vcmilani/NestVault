from conftest import make_backup, make_version, finish_version


# -- /backups -----------------------------------------------------------------

def test_create_backup(client):
    r = client.post("/backups", json={"label": "mybackup"})
    assert r.status_code == 200
    data = r.json()
    assert data["created"] is True
    assert data["backup"]["label"] == "mybackup"


def test_create_backup_idempotent(client):
    client.post("/backups", json={"label": "mybackup"})
    r = client.post("/backups", json={"label": "mybackup"})
    assert r.status_code == 200
    assert r.json()["created"] is False


def test_create_backup_with_client_name(client):
    r = client.post("/backups", json={"label": "b1", "client_name": "machineA"})
    assert r.json()["backup"]["client_name"] == "machineA"


def test_list_backups_empty(client):
    assert client.get("/backups").json() == []


def test_list_backups(client):
    make_backup(client, "b1")
    make_backup(client, "b2")
    labels = [b["label"] for b in client.get("/backups").json()]
    assert set(labels) == {"b1", "b2"}


def test_list_backups_filter_client_name(client):
    make_backup(client, "b1", client_name="machineA")
    make_backup(client, "b2", client_name="machineB")
    r = client.get("/backups", params={"client_name": "machineA"})
    assert len(r.json()) == 1
    assert r.json()[0]["label"] == "b1"


def test_get_backup(client):
    make_backup(client, "mybackup")
    r = client.get("/backups/mybackup")
    assert r.status_code == 200
    assert r.json()["label"] == "mybackup"


def test_get_backup_not_found(client):
    assert client.get("/backups/nope").status_code == 404


def test_delete_backup(client):
    make_backup(client, "mybackup")
    r = client.delete("/backups/mybackup")
    assert r.status_code == 200
    assert r.json()["status"] == "deleted"
    assert client.get("/backups/mybackup").status_code == 404


def test_delete_backup_not_found(client):
    assert client.delete("/backups/nope").status_code == 404


# -- /backups/{label}/versions ------------------------------------------------

def test_create_version(client):
    make_backup(client, "b1")
    r = client.post("/backups/b1/versions", json={"version_key": "2026-01-01T00:00:00"})
    assert r.status_code == 200
    data = r.json()
    assert data["created"] is True
    assert data["version"]["status"] == "running"


def test_create_version_idempotent(client):
    make_backup(client, "b1")
    client.post("/backups/b1/versions", json={"version_key": "2026-01-01T00:00:00"})
    r = client.post("/backups/b1/versions", json={"version_key": "2026-01-01T00:00:00"})
    assert r.json()["created"] is False


def test_create_version_backup_not_found(client):
    r = client.post("/backups/nope/versions", json={"version_key": "2026-01-01T00:00:00"})
    assert r.status_code == 404


def test_list_versions(client):
    make_backup(client, "b1")
    client.post("/backups/b1/versions", json={"version_key": "2026-01-01T00:00:00"})
    client.post("/backups/b1/versions", json={"version_key": "2026-02-01T00:00:00"})
    versions = client.get("/backups/b1/versions").json()
    assert len(versions) == 2
    assert versions[0]["version_key"] > versions[1]["version_key"]


def test_get_version(client):
    make_backup(client, "b1")
    make_version(client, "b1", "2026-01-01T00:00:00")
    r = client.get("/backups/b1/versions/2026-01-01T00:00:00")
    assert r.status_code == 200
    assert r.json()["version_key"] == "2026-01-01T00:00:00"


def test_get_version_not_found(client):
    make_backup(client, "b1")
    assert client.get("/backups/b1/versions/9999-01-01T00:00:00").status_code == 404


def test_finish_version_done(client):
    make_backup(client, "b1")
    make_version(client, "b1", "2026-01-01T00:00:00")
    r = client.patch("/backups/b1/versions/2026-01-01T00:00:00", json={"status": "done"})
    assert r.status_code == 200
    v = r.json()
    assert v["status"] == "done"
    assert v["finished_at"] is not None


def test_finish_version_failed(client):
    make_backup(client, "b1")
    make_version(client, "b1", "2026-01-01T00:00:00")
    r = client.patch("/backups/b1/versions/2026-01-01T00:00:00", json={"status": "failed"})
    assert r.json()["status"] == "failed"


def test_delete_version(client):
    make_backup(client, "b1")
    make_version(client, "b1", "2026-01-01T00:00:00")
    r = client.delete("/backups/b1/versions/2026-01-01T00:00:00")
    assert r.status_code == 200
    assert r.json()["status"] == "deleted"


def test_create_version_marks_running_as_incomplete(client):
    make_backup(client, "b1")
    client.post("/backups/b1/versions", json={"version_key": "2026-01-01T00:00:00"})
    client.post("/backups/b1/versions", json={"version_key": "2026-02-01T00:00:00"})
    v1 = client.get("/backups/b1/versions/2026-01-01T00:00:00").json()
    assert v1["status"] == "incomplete"


def test_incomplete_version_deleted_with_cleanup(client):
    make_backup(client, "b1")
    client.post("/backups/b1/versions", json={"version_key": "2026-01-01T00:00:00"})
    client.post("/backups/b1/versions", json={"version_key": "2026-02-01T00:00:00"})
    client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 0})
    assert client.get("/backups/b1/versions").json() == []


def test_incomplete_version_deleted_directly(client):
    make_backup(client, "b1")
    client.post("/backups/b1/versions", json={"version_key": "2026-01-01T00:00:00"})
    client.post("/backups/b1/versions", json={"version_key": "2026-02-01T00:00:00"})
    r = client.delete("/backups/b1/versions/2026-01-01T00:00:00")
    assert r.status_code == 200
    assert r.json()["status"] == "deleted"


def test_backup_stats_reflect_last_done_version(client):
    import base64
    make_backup(client, "b1")
    make_version(client, "b1", "2026-01-01T00:00:00")
    encoded = base64.b64encode(b"/file.txt").decode()
    client.post("/upload", content=b"hello world", headers={
        "X-Backup-Label": "b1",
        "X-Version-Key": "2026-01-01T00:00:00",
        "X-Original-Path": encoded,
        "X-Mtime": "1000.0",
    })
    finish_version(client, "b1", "2026-01-01T00:00:00")
    info = client.get("/backups/b1").json()
    assert info["file_count"] == 1
    assert info["total_size_bytes"] == len(b"hello world")
    assert info["last_version"] == "2026-01-01T00:00:00"
    assert info["version_count"] == 1
