from pathlib import Path
from conftest import make_backup, make_version, upload_file


def test_list_files_empty(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    r = client.get("/files", params={"backup_label": "b1", "version_key": "v1"})
    assert r.status_code == 200
    assert r.json() == []


def test_list_files(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    upload_file(client, "b1", "v1", path="/b.txt", content=b"bbb")
    upload_file(client, "b1", "v1", path="/a.txt", content=b"aaa")
    files = client.get("/files", params={"backup_label": "b1", "version_key": "v1"}).json()
    assert len(files) == 2
    paths = [f["original_path"] for f in files]
    assert paths == sorted(paths)


def test_list_files_has_correct_size(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    upload_file(client, "b1", "v1", path="/big.txt", content=b"x" * 100)
    files = client.get("/files", params={"backup_label": "b1", "version_key": "v1"}).json()
    assert files[0]["size"] == 100


def test_list_files_version_not_found(client):
    make_backup(client, "b1")
    r = client.get("/files", params={"backup_label": "b1", "version_key": "nope"})
    assert r.status_code == 404


def test_download_file(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    up = upload_file(client, "b1", "v1", path="/hello.txt", content=b"hello!")
    file_id = up["file_id"]
    r = client.get(f"/files/{file_id}/download")
    assert r.status_code == 200
    assert r.content == b"hello!"


def test_download_file_not_found(client):
    r = client.get("/files/99999/download")
    assert r.status_code == 404


def test_download_file_missing_on_disk(client, tmp_vol):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    up = upload_file(client, "b1", "v1", path="/gone.txt", content=b"data")
    sha = up["sha256"]

    # Remove o arquivo físico
    dest = tmp_vol / "_content" / sha[:2] / sha
    dest.unlink()

    r = client.get(f"/files/{up['file_id']}/download")
    assert r.status_code == 410
