import base64
from collections import namedtuple
from unittest.mock import patch

import database as db_mod
import main as m
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


# -- Helpers ------------------------------------------------------------------

def _make_engine():
    return create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


def _client_ctx(monkeypatch, volumes, disk_usage_fn=None):
    """Contexto de TestClient com volumes e disk_usage opcionalmente mockado."""
    from fastapi.testclient import TestClient

    engine = _make_engine()
    db_mod.Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)

    monkeypatch.setattr(m, "STORAGE_VOLUMES", volumes)
    monkeypatch.setattr(m, "STORAGE_DIR", volumes[0])

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    m.app.dependency_overrides[db_mod.get_db] = override_get_db

    def _run(patch_ctx=None):
        with TestClient(m.app) as c:
            yield c
        m.app.dependency_overrides.clear()

    if disk_usage_fn:
        with patch("main.shutil.disk_usage", side_effect=disk_usage_fn):
            yield from _run()
    else:
        yield from _run()


def enc(path: str) -> str:
    return base64.b64encode(path.encode()).decode()


def upload(client, label, version_key, path, content):
    r = client.post(
        "/upload",
        content=content,
        headers={
            "X-Backup-Label": label,
            "X-Version-Key": version_key,
            "X-Original-Path": enc(path),
            "X-Mtime": "1.0",
        },
    )
    assert r.status_code == 200
    return r.json()


# -- GET /disks ---------------------------------------------------------------

def test_disks_page_served(tmp_path, monkeypatch):
    vol = tmp_path / "vol"
    vol.mkdir()
    for c in _client_ctx(monkeypatch, [vol]):
        r = c.get("/disks")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        assert "Discos" in r.text


def test_disks_page_not_found_when_missing(tmp_path, monkeypatch):
    vol = tmp_path / "vol"
    vol.mkdir()
    monkeypatch.setattr(m, "STATIC_DIR", tmp_path / "nonexistent")
    for c in _client_ctx(monkeypatch, [vol]):
        r = c.get("/disks")
        assert r.status_code == 404


# -- GET /storage/disks — disk usage ------------------------------------------

def test_storage_disks_single_volume_usage(tmp_path, monkeypatch):
    vol = tmp_path / "vol"
    vol.mkdir()

    def fake_usage(_):
        return DiskUsage(total=10_000, used=3_000, free=7_000)

    for c in _client_ctx(monkeypatch, [vol], fake_usage):
        r = c.get("/storage/disks")
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1
        d = data[0]
        assert d["total_bytes"] == 10_000
        assert d["used_bytes"] == 3_000
        assert d["free_bytes"] == 7_000
        assert d["path"] == str(vol)


def test_storage_disks_two_volumes_separate_usage(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    def fake_usage(path):
        if path == v1:
            return DiskUsage(total=1_000, used=200, free=800)
        return DiskUsage(total=2_000, used=1_500, free=500)

    for c in _client_ctx(monkeypatch, [v1, v2], fake_usage):
        r = c.get("/storage/disks")
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 2

        by_path = {d["path"]: d for d in data}
        assert by_path[str(v1)]["total_bytes"] == 1_000
        assert by_path[str(v1)]["free_bytes"] == 800
        assert by_path[str(v2)]["total_bytes"] == 2_000
        assert by_path[str(v2)]["free_bytes"] == 500


def test_storage_disks_order_matches_storage_volumes(tmp_path, monkeypatch):
    """A ordem retornada deve seguir STORAGE_VOLUMES."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    def fake_usage(_):
        return DiskUsage(total=1_000, used=100, free=900)

    for c in _client_ctx(monkeypatch, [v1, v2], fake_usage):
        r = c.get("/storage/disks")
        paths = [d["path"] for d in r.json()]
        assert paths == [str(v1), str(v2)]


# -- GET /storage/disks — content_files / content_bytes -----------------------

def test_storage_disks_no_files_initially(tmp_path, monkeypatch):
    vol = tmp_path / "vol"
    vol.mkdir()
    for c in _client_ctx(monkeypatch, [vol]):
        r = c.get("/storage/disks")
        assert r.status_code == 200
        d = r.json()[0]
        assert d["content_files"] == 0
        assert d["content_bytes"] == 0


def test_storage_disks_counts_uploaded_files(tmp_path, monkeypatch):
    vol = tmp_path / "vol"
    vol.mkdir()
    for c in _client_ctx(monkeypatch, [vol]):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        upload(c, "b1", "v1", "/a.txt", b"hello")
        upload(c, "b1", "v1", "/b.txt", b"world!")

        r = c.get("/storage/disks")
        d = r.json()[0]
        assert d["content_files"] == 2
        assert d["content_bytes"] == len(b"hello") + len(b"world!")


def test_storage_disks_deduplication_not_double_counted(tmp_path, monkeypatch):
    """Mesmo conteúdo enviado duas vezes conta como 1 arquivo de conteúdo."""
    vol = tmp_path / "vol"
    vol.mkdir()
    for c in _client_ctx(monkeypatch, [vol]):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        upload(c, "b1", "v1", "/a.txt", b"same content")
        upload(c, "b1", "v1", "/b.txt", b"same content")

        r = c.get("/storage/disks")
        d = r.json()[0]
        assert d["content_files"] == 1
        assert d["content_bytes"] == len(b"same content")


def test_storage_disks_content_isolated_per_volume(tmp_path, monkeypatch):
    """Cada volume reporta apenas os arquivos que estão nele."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _client_ctx(monkeypatch, [v1, v2]):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})

        # Força uploads alternados entre v1 e v2 via monkeypatch de _pick_volume
        monkeypatch.setattr(m, "_pick_volume", lambda: v1)
        upload(c, "b1", "v1", "/on_v1.txt", b"content for v1")

        monkeypatch.setattr(m, "_pick_volume", lambda: v2)
        upload(c, "b1", "v1", "/on_v2.txt", b"content for v2 longer")

        r = c.get("/storage/disks")
        assert r.status_code == 200
        by_path = {d["path"]: d for d in r.json()}

        assert by_path[str(v1)]["content_files"] == 1
        assert by_path[str(v1)]["content_bytes"] == len(b"content for v1")

        assert by_path[str(v2)]["content_files"] == 1
        assert by_path[str(v2)]["content_bytes"] == len(b"content for v2 longer")
