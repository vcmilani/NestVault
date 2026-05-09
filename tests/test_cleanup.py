from pathlib import Path
from conftest import make_backup, make_version, upload_file, finish_version


# -- POST /backups/{label}/cleanup --------------------------------------------

def test_cleanup_removes_old_versions(client):
    make_backup(client, "b1")
    for i in range(4):
        make_version(client, "b1", f"2026-0{i+1}-01T00:00:00")

    r = client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 2})
    assert r.status_code == 200
    data = r.json()
    assert data["kept"] == 2
    assert len(data["versions_removed"]) == 2

    versions = client.get("/backups/b1/versions").json()
    assert len(versions) == 2


def test_cleanup_keeps_most_recent(client):
    make_backup(client, "b1")
    keys = ["2026-01-01T00:00:00", "2026-02-01T00:00:00", "2026-03-01T00:00:00"]
    for k in keys:
        make_version(client, "b1", k)

    client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 1})
    versions = client.get("/backups/b1/versions").json()
    assert versions[0]["version_key"] == "2026-03-01T00:00:00"


def test_cleanup_keep_zero(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    r = client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 0})
    assert r.json()["versions_removed"] == ["v1"]
    assert client.get("/backups/b1/versions").json() == []


def test_cleanup_keep_more_than_existing(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    r = client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 10})
    assert r.json()["versions_removed"] == []


def test_cleanup_removes_orphan_storage(client, tmp_vol):
    """Cleanup de versão deve remover arquivo físico exclusivo da versão deletada."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")

    up = upload_file(client, "b1", "v1", path="/exclusive.txt", content=b"only in v1")
    sha = up["sha256"]
    dest = tmp_vol / "_content" / sha[:2] / sha
    assert dest.exists()

    # v2 tem arquivo diferente — v1 fica exclusivo
    upload_file(client, "b1", "v2", path="/other.txt", content=b"only in v2")

    client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 1})

    assert not dest.exists()


def test_cleanup_keeps_shared_storage(client, tmp_vol):
    """Conteúdo compartilhado entre versões não deve ser removido."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")

    up = upload_file(client, "b1", "v1", path="/shared.txt", content=b"shared")
    sha = up["sha256"]
    upload_file(client, "b1", "v2", path="/shared.txt", content=b"shared")

    client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 1})

    dest = tmp_vol / "_content" / sha[:2] / sha
    assert dest.exists()


# -- POST /maintenance/cleanup-orphans ----------------------------------------

def test_cleanup_orphans_removes_unreferenced(client, tmp_vol):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    up = upload_file(client, "b1", "v1", path="/file.txt", content=b"data")
    sha = up["sha256"]

    # Remove a versão, tornando o conteúdo órfão
    client.delete("/backups/b1/versions/v1")

    r = client.post("/maintenance/cleanup-orphans")
    assert r.status_code == 200
    data = r.json()
    assert data["files_removed"] == 1
    assert data["bytes_freed"] == len(b"data")

    dest = tmp_vol / "_content" / sha[:2] / sha
    assert not dest.exists()


def test_cleanup_orphans_nothing_to_remove(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    upload_file(client, "b1", "v1", path="/file.txt", content=b"data")

    r = client.post("/maintenance/cleanup-orphans")
    assert r.json()["files_removed"] == 0


def test_cleanup_orphans_bytes_freed(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    content = b"x" * 200
    upload_file(client, "b1", "v1", path="/big.txt", content=content)
    client.delete("/backups/b1/versions/v1")

    r = client.post("/maintenance/cleanup-orphans")
    assert r.json()["bytes_freed"] == 200
