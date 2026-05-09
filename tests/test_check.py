import base64
from conftest import make_backup, make_version, upload_file


def _check_payload(label, version_key, path="/file.txt", sha256=None, size=5, mtime=1000.0):
    sha256 = sha256 or ("a" * 64)
    return {
        "backup_label": label,
        "version_key": version_key,
        "original_path": path,
        "sha256": sha256,
        "size": size,
        "mtime": mtime,
    }


# -- POST /check --------------------------------------------------------------

def test_check_needs_upload(client):
    """Conteúdo novo: needs_upload=True, content_exists=False."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    r = client.post("/check", json=_check_payload("b1", "v1"))
    assert r.status_code == 200
    data = r.json()
    assert data["needs_upload"] is True
    assert data["content_exists"] is False


def test_check_content_exists_not_registered(client):
    """Conteúdo já no storage mas não registrado nesta versão: needs_upload=True, content_exists=True."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")

    uploaded = upload_file(client, "b1", "v1", path="/file.txt", content=b"hello")
    sha = uploaded["sha256"]

    r = client.post("/check", json=_check_payload("b1", "v2", path="/file.txt", sha256=sha, size=5))
    assert r.status_code == 200
    data = r.json()
    assert data["needs_upload"] is True
    assert data["content_exists"] is True


def test_check_already_registered(client):
    """Arquivo já registrado nesta versão: needs_upload=False."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    uploaded = upload_file(client, "b1", "v1", path="/file.txt", content=b"hello")
    sha = uploaded["sha256"]

    r = client.post("/check", json=_check_payload("b1", "v1", path="/file.txt", sha256=sha, size=5))
    assert r.status_code == 200
    data = r.json()
    assert data["needs_upload"] is False
    assert data["file_id"] == uploaded["file_id"]


def test_check_version_not_found(client):
    make_backup(client, "b1")
    r = client.post("/check", json=_check_payload("b1", "nonexistent"))
    assert r.status_code == 404


# -- POST /check/batch --------------------------------------------------------

def _batch_item(path="/file.txt", sha256=None, size=5, mtime=1000.0):
    return {"original_path": path, "sha256": sha256 or ("b" * 64), "size": size, "mtime": mtime}


def test_check_batch_all_new(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    payload = {
        "backup_label": "b1",
        "version_key": "v1",
        "files": [
            _batch_item("/a.txt", "a" * 64),
            _batch_item("/b.txt", "b" * 64),
        ],
    }
    r = client.post("/check/batch", json=payload)
    assert r.status_code == 200
    results = r.json()
    assert len(results) == 2
    assert all(item["needs_upload"] is True for item in results)
    assert all(item["content_exists"] is False for item in results)


def test_check_batch_order_preserved(client):
    """Resultado mantém a mesma ordem da entrada."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")

    uploaded = upload_file(client, "b1", "v1", path="/existing.txt", content=b"existing")
    sha_existing = uploaded["sha256"]

    payload = {
        "backup_label": "b1",
        "version_key": "v2",
        "files": [
            _batch_item("/new.txt", "c" * 64),
            _batch_item("/existing.txt", sha_existing, size=8),
        ],
    }
    r = client.post("/check/batch", json=payload)
    results = r.json()
    assert results[0]["needs_upload"] is True
    assert results[0]["content_exists"] is False
    assert results[1]["needs_upload"] is True
    assert results[1]["content_exists"] is True


def test_check_batch_already_registered(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    uploaded = upload_file(client, "b1", "v1", path="/file.txt", content=b"hello")
    sha = uploaded["sha256"]

    payload = {
        "backup_label": "b1",
        "version_key": "v1",
        "files": [_batch_item("/file.txt", sha, size=5)],
    }
    r = client.post("/check/batch", json=payload)
    result = r.json()[0]
    assert result["needs_upload"] is False
    assert result["file_id"] == uploaded["file_id"]


def test_check_batch_empty_files_rejected(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    payload = {"backup_label": "b1", "version_key": "v1", "files": []}
    r = client.post("/check/batch", json=payload)
    assert r.status_code == 422
