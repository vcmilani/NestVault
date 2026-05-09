import hashlib
import base64
from pathlib import Path
from conftest import make_backup, make_version


def _headers(label, version_key, path="/file.txt", mtime=1000.0, sha256=None):
    encoded = base64.b64encode(path.encode()).decode()
    h = {
        "X-Backup-Label": label,
        "X-Version-Key": version_key,
        "X-Original-Path": encoded,
        "X-Mtime": str(mtime),
    }
    if sha256:
        h["X-Content-Sha256"] = sha256
    return h


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# -- upload novo --------------------------------------------------------------

def test_upload_new_file(client, tmp_vol):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    content = b"hello world"
    r = client.post("/upload", content=content, headers=_headers("b1", "v1"))
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "registered"
    assert data["uploaded"] is True
    assert data["sha256"] == _sha256(content)

    # arquivo deve existir no disco
    sha = data["sha256"]
    dest = tmp_vol / "_content" / sha[:2] / sha
    assert dest.exists()
    assert dest.read_bytes() == content


def test_upload_creates_version_file_record(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    r = client.post("/upload", content=b"data", headers=_headers("b1", "v1"))
    assert r.json()["file_id"] is not None


def test_upload_path_plain_string(client):
    """Path sem base64 também deve ser aceito."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    r = client.post("/upload", content=b"data", headers={
        "X-Backup-Label": "b1",
        "X-Version-Key": "v1",
        "X-Original-Path": "/plain/path.txt",
        "X-Mtime": "1000.0",
    })
    assert r.status_code == 200


# -- deduplicação de conteúdo -------------------------------------------------

def test_upload_dedup_same_content(client, tmp_vol):
    """Mesmo conteúdo enviado duas vezes → arquivo no disco criado apenas uma vez."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")

    content = b"shared content"
    r1 = client.post("/upload", content=content, headers=_headers("b1", "v1", "/f.txt"))
    r2 = client.post("/upload", content=content, headers=_headers("b1", "v2", "/f.txt"))

    assert r1.json()["sha256"] == r2.json()["sha256"]

    sha = r1.json()["sha256"]
    content_dir = tmp_vol / "_content" / sha[:2]
    files = list(content_dir.iterdir())
    assert len(files) == 1


def test_upload_dedup_uploaded_flag(client):
    """Segunda vez com mesmo conteúdo: uploaded ainda é True (foi streamado e descartado)."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")

    content = b"duplicate"
    client.post("/upload", content=content, headers=_headers("b1", "v1", "/f.txt"))
    r2 = client.post("/upload", content=content, headers=_headers("b1", "v2", "/f.txt"))
    assert r2.json()["uploaded"] is True


# -- modo register-only (X-Content-Sha256 sem body) ---------------------------

def test_upload_register_only(client):
    """Enviar X-Content-Sha256 sem body registra o arquivo usando conteúdo já existente."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")

    content = b"existing content"
    r1 = client.post("/upload", content=content, headers=_headers("b1", "v1", "/f.txt"))
    sha = r1.json()["sha256"]

    r2 = client.post("/upload", content=b"", headers=_headers("b1", "v2", "/f.txt", sha256=sha))
    assert r2.status_code == 200
    data = r2.json()
    assert data["uploaded"] is False
    assert data["sha256"] == sha


def test_upload_register_only_sha_not_found(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    r = client.post("/upload", content=b"", headers=_headers("b1", "v1", sha256="a" * 64))
    assert r.status_code == 400


# -- upsert de VersionFile ----------------------------------------------------

def test_upload_upsert_updates_existing_path(client):
    """Re-upload do mesmo path na mesma versão atualiza sha256 e mtime."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")

    r1 = client.post("/upload", content=b"old", headers=_headers("b1", "v1", mtime=1.0))
    r2 = client.post("/upload", content=b"new", headers=_headers("b1", "v1", mtime=2.0))

    assert r1.json()["file_id"] == r2.json()["file_id"]
    assert r1.json()["sha256"] != r2.json()["sha256"]


# -- versão não encontrada ----------------------------------------------------

def test_upload_version_not_found(client):
    make_backup(client, "b1")
    r = client.post("/upload", content=b"data", headers=_headers("b1", "nonexistent"))
    assert r.status_code == 404
