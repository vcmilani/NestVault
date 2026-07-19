from conftest import make_backup, make_version, finish_version, upload_file


def _sha(client, label, version, path="/orig.txt", content=b"hello"):
    return upload_file(client, label, version, path=path, content=content)["sha256"]


def _register(client, label, version_key, files):
    return client.post("/register/batch", json={
        "backup_label": label, "version_key": version_key, "files": files})


def _item(path, sha, mtime=1000.0):
    return {"original_path": path, "sha256": sha, "mtime": mtime}


def _paths(client, label, version_key):
    r = client.get("/files", params={"backup_label": label, "version_key": version_key})
    assert r.status_code == 200
    return {f["original_path"]: f["sha256"] for f in r.json()}


# -- POST /register/batch -----------------------------------------------------

def test_register_batch_all_new(client):
    """Lote de paths novos apontando para conteúdo existente: tudo registrado."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")
    sha = _sha(client, "b1", "v1")

    r = _register(client, "b1", "v2", [_item("/a.txt", sha), _item("/b.txt", sha)])
    assert r.status_code == 200
    data = r.json()
    assert data["registered"] == 2
    assert data["missing"] == 0
    assert all(item["registered"] for item in data["results"])
    assert _paths(client, "b1", "v2") == {"/a.txt": sha, "/b.txt": sha}


def test_register_batch_missing_content(client):
    """sha256 inexistente não aborta o lote: item volta registered=False, demais passam."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")
    sha = _sha(client, "b1", "v1")
    ghost = "f" * 64

    r = _register(client, "b1", "v2", [_item("/ok.txt", sha), _item("/ghost.txt", ghost)])
    assert r.status_code == 200
    data = r.json()
    assert data["registered"] == 1
    assert data["missing"] == 1
    by_path = {i["original_path"]: i for i in data["results"]}
    assert by_path["/ok.txt"]["registered"] is True
    assert by_path["/ghost.txt"]["registered"] is False
    assert _paths(client, "b1", "v2") == {"/ok.txt": sha}


def test_register_batch_upsert_existing_path(client):
    """Path já registrado na versão: atualiza sha256/mtime em vez de violar uq_version_path."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    _sha(client, "b1", "v1", path="/f.txt", content=b"old")
    sha_new = _sha(client, "b1", "v1", path="/other.txt", content=b"new")

    r = _register(client, "b1", "v1", [_item("/f.txt", sha_new, mtime=2000.0)])
    assert r.status_code == 200
    assert r.json()["registered"] == 1
    assert _paths(client, "b1", "v1")["/f.txt"] == sha_new


def test_register_batch_duplicate_paths_last_wins(client):
    """Path repetido no mesmo lote: último item vence, sem violar a unique constraint."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")
    sha_a = _sha(client, "b1", "v1", path="/a", content=b"aaa")
    sha_b = _sha(client, "b1", "v1", path="/b", content=b"bbb")

    r = _register(client, "b1", "v2", [_item("/dup.txt", sha_a), _item("/dup.txt", sha_b)])
    assert r.status_code == 200
    assert r.json()["registered"] == 2
    assert _paths(client, "b1", "v2") == {"/dup.txt": sha_b}


def test_register_batch_version_not_running(client):
    """Versão finalizada: 409, como no absorb."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    sha = _sha(client, "b1", "v1")
    finish_version(client, "b1", "v1")

    r = _register(client, "b1", "v1", [_item("/x.txt", sha)])
    assert r.status_code == 409


def test_register_batch_unknown_version(client):
    make_backup(client, "b1")
    r = _register(client, "b1", "nope", [_item("/x.txt", "a" * 64)])
    assert r.status_code == 404


def test_register_batch_empty_files_rejected(client):
    """min_length=1 no schema — lote vazio é 422."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    r = _register(client, "b1", "v1", [])
    assert r.status_code == 422
