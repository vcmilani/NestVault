"""Front cloud — download de seleção (arquivos e pastas) em .zip, em streaming."""
import base64
import io
import os
import zipfile

from fastapi.testclient import TestClient

import crypto
import database as db_mod
import main as m
import storage as storage_mod
from conftest import make_backup, make_version, finish_version, upload_file

V1 = "2026-01-01T00:00:00"
BASE = "/Users/alice/Pictures"


def _seed(c, label="fotos"):
    make_backup(c, label=label)
    make_version(c, label=label, version_key=V1)
    upload_file(c, label, V1, path=f"{BASE}/2024/a.jpg", content=b"A" * 10, mtime=1_700_000_000)
    upload_file(c, label, V1, path=f"{BASE}/2024/sub/b.txt", content=b"texto " * 100)
    upload_file(c, label, V1, path=f"{BASE}/2025/c.mp4", content=b"C" * 30)
    upload_file(c, label, V1, path=f"{BASE}/notas.txt", content=b"n")
    finish_version(c, label, V1)


def _zip(c, label, items, version=None, expect=200):
    data = {"label": label, "items": items}
    if version:
        data["version"] = version
    r = c.post("/cloud/zip", data=data)
    assert r.status_code == expect, r.text
    return r


def _open(r):
    z = zipfile.ZipFile(io.BytesIO(r.content))
    assert z.testzip() is None
    return z


def test_zip_folders_and_files_keep_structure(client):
    _seed(client)
    r = _zip(client, "fotos", ["2024", "notas.txt"])
    assert r.headers["content-type"] == "application/zip"
    assert "attachment" in r.headers["content-disposition"]
    # Seleção na raiz do backup: o zip leva o nome do backup
    assert "filename*=UTF-8''fotos%20%282%20itens%29.zip" in r.headers["content-disposition"]
    z = _open(r)
    assert sorted(z.namelist()) == ["2024/a.jpg", "2024/sub/b.txt", "notas.txt"]
    assert z.read("2024/sub/b.txt") == b"texto " * 100
    # Mídia vai sem recompressão; texto, comprimido
    assert z.getinfo("2024/a.jpg").compress_type == zipfile.ZIP_STORED
    assert z.getinfo("2024/sub/b.txt").compress_type == zipfile.ZIP_DEFLATED
    assert z.getinfo("2024/a.jpg").date_time[0] == 2023


def test_zip_single_folder_name_and_nested_selection(client):
    _seed(client)
    r = _zip(client, "fotos", ["2024/sub", "2024", "2024/a.jpg"])
    assert "filename*=UTF-8''2024.zip" in r.headers["content-disposition"]
    assert sorted(_open(r).namelist()) == ["2024/a.jpg", "2024/sub/b.txt"]
    # Seleção dentro de uma subpasta: o zip leva o nome da subpasta
    r = _zip(client, "fotos", ["2024/sub", "2024/a.jpg"])
    assert "filename*=UTF-8''2024%20%282%20itens%29.zip" in r.headers["content-disposition"]


def test_zip_whole_label_root(client):
    _seed(client)
    r = _zip(client, "fotos", [""])
    assert "filename*=UTF-8''fotos.zip" in r.headers["content-disposition"]
    assert sorted(_open(r).namelist()) == [
        "fotos/2024/a.jpg", "fotos/2024/sub/b.txt", "fotos/2025/c.mp4", "fotos/notas.txt"]


def test_zip_dry_run_and_errors(client):
    _seed(client)
    r = client.post("/cloud/zip", data={"label": "fotos", "items": ["2024"], "dry_run": "true"})
    assert r.json() == {"name": "2024.zip", "files": 2, "size": 10 + 600, "unreadable": 0}
    _zip(client, "fotos", ["nada"], expect=404)
    _zip(client, "fotos", ["../x"], expect=400)
    _zip(client, "outro", ["a"], expect=404)


def test_zip_duplicate_names_from_search_get_suffix(client):
    make_backup(client, label="docs")
    make_version(client, label="docs", version_key=V1)
    upload_file(client, "docs", V1, path="/d/x/r.txt", content=b"1")
    upload_file(client, "docs", V1, path="/d/y/r.txt", content=b"2")
    finish_version(client, "docs", V1)
    z = _open(_zip(client, "docs", ["x/r.txt", "y/r.txt"]))
    assert sorted(z.namelist()) == ["r (2).txt", "r.txt"]


def test_zip_encrypted_content(client, monkeypatch):
    monkeypatch.setattr(storage_mod, "ENCRYPTION_ENABLED", True, raising=False)
    monkeypatch.setattr(storage_mod, "encryption_key", os.urandom(32), raising=False)
    data = os.urandom(2 * crypto.CHUNK_SIZE + 999)
    make_backup(client, label="enc")
    make_version(client, label="enc", version_key=V1)
    upload_file(client, "enc", V1, path="/v/pasta/big.bin", content=data)
    upload_file(client, "enc", V1, path="/v/outro.txt", content=b"o")  # prefixo comum = /v/
    finish_version(client, "enc", V1)
    assert _open(_zip(client, "enc", ["pasta"])).read("pasta/big.bin") == data


def test_zip_lists_unreadable_files(client):
    _seed(client)
    db = m.SessionLocal()
    try:
        sha = (db.query(db_mod.VersionFile.sha256)
               .filter(db_mod.VersionFile.original_path == f"{BASE}/2025/c.mp4").scalar())
        for cp in db.query(db_mod.FileContentCopy).filter_by(sha256=sha):
            os.remove(cp.stored_at)
    finally:
        db.close()
    r = client.post("/cloud/zip", data={"label": "fotos", "items": ["2025", "notas.txt"], "dry_run": "1"})
    assert r.json()["unreadable"] == 1
    z = _open(_zip(client, "fotos", ["2025", "notas.txt"]))
    assert sorted(z.namelist()) == ["_ERROS_NESTVAULT.txt", "notas.txt"]
    assert "2025/c.mp4" in z.read("_ERROS_NESTVAULT.txt").decode()


def test_zip_server_drive_uses_drive_name(client):
    label = client.post("/cloud/drives", json={"name": "Documentos"}).json()["label"]
    for p in ("a/1.txt", "b.txt"):
        client.put("/cloud/drive/upload", content=b"x", headers={
            "X-Label": label, "X-Path": base64.b64encode(p.encode()).decode()})
    r = _zip(client, label, [""])
    assert "filename*=UTF-8''Documentos.zip" in r.headers["content-disposition"]
    assert sorted(_open(r).namelist()) == ["Documentos/a/1.txt", "Documentos/b.txt"]
    r = _zip(client, label, ["a", "b.txt"])
    assert "Documentos%20%282%20itens%29.zip" in r.headers["content-disposition"]


def test_zip_cookie_auth_and_isolation(two_users):
    _admin, alice, bob = two_users
    _seed(alice, "alice-fotos")
    _zip(bob, "alice-fotos", ["2024"], expect=403)
    browser = TestClient(m.app)  # sem header: só o cookie, como o <form> da página
    assert browser.post("/cloud/zip", data={"label": "alice-fotos", "items": ["2024"]}).status_code == 401
    browser.post("/cloud/session", headers={"X-API-Key": "alice-key"})
    r = browser.post("/cloud/zip", data={"label": "alice-fotos", "items": ["2024"]})
    assert r.status_code == 200 and "2024/a.jpg" in _open(r).namelist()
