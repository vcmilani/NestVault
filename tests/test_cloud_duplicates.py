"""Localizador de duplicados do /cloud (v10.2.0): grupos só do usuário, só a
versão mais recente de cada label, min_size, caminhos iguais aos da árvore e
apagar uma cópia na pasta do servidor (lixeira) enquanto backup segue só leitura."""
import base64

from conftest import make_backup, make_version, finish_version, upload_file

V1 = "2026-01-01T00:00:00"
V2 = "2026-01-02T00:00:00"
BIG = b"F" * 2048
OTHER = b"G" * 4096


def _drive(c, name="Docs"):
    r = c.post("/cloud/drives", json={"name": name})
    assert r.status_code == 200, r.text
    return r.json()["label"]


def _put(c, label, path, content):
    r = c.put("/cloud/drive/upload", content=content, headers={
        "X-Label": label, "X-Path": base64.b64encode(path.encode()).decode(), "X-Mtime": "1000"})
    assert r.status_code == 200, r.text


def _dups(c, **params):
    params.setdefault("min_size", 1)
    r = c.get("/cloud/duplicates", params=params)
    assert r.status_code == 200, r.text
    return r.json()


def _backup(c, label, files, key=V1):
    make_backup(c, label)
    make_version(c, label, key)
    for path, content in files.items():
        upload_file(c, label, key, path, content)
    finish_version(c, label, key)


def test_groups_across_backup_and_server_folder(client):
    _backup(client, "mac", {"/Users/v/fotos/ferias.jpg": BIG, "/Users/v/docs/x.txt": b"x"})
    srv = _drive(client)
    _put(client, srv, "copias/ferias.jpg", BIG)
    _put(client, srv, "outra/ferias (1).jpg", BIG)

    d = _dups(client)
    assert d["group_count"] == 1 and d["repeated_bytes"] == 2 * len(BIG)
    g = d["groups"][0]
    assert g["count"] == 3 and g["size"] == len(BIG)
    files = g["files"]
    # Editáveis primeiro; caminho do backup relativo à base, como na árvore.
    assert [f["editable"] for f in files] == [True, True, False]
    assert {f["path"] for f in files if f["editable"]} == {"copias/ferias.jpg", "outra/ferias (1).jpg"}
    mac = next(f for f in files if not f["editable"])
    # Relativo à raiz do label (a base comum /Users/v/ some, como na árvore).
    assert mac["path"] == "fotos/ferias.jpg"
    folder, name = mac["path"].rsplit("/", 1)
    tree = client.get("/cloud/tree", params={"label": "mac", "path": folder}).json()
    assert [f["name"] for f in tree["files"]] == [name]
    assert files[0]["drive"] == "Docs"


def test_only_latest_version_and_min_size(client):
    make_backup(client, "mac")
    make_version(client, "mac", V1)
    upload_file(client, "mac", V1, "/a/1.bin", BIG)
    upload_file(client, "mac", V1, "/a/2.bin", BIG)
    finish_version(client, "mac", V1)
    make_version(client, "mac", V2)
    upload_file(client, "mac", V2, "/a/1.bin", BIG)
    upload_file(client, "mac", V2, "/a/3.bin", OTHER)
    upload_file(client, "mac", V2, "/a/4.bin", OTHER)
    finish_version(client, "mac", V2)

    d = _dups(client)
    assert d["group_count"] == 1 and d["groups"][0]["size"] == len(OTHER)
    assert _dups(client, min_size=len(OTHER) + 1)["group_count"] == 0


def test_isolated_per_user(two_users):
    _admin, alice, bob = two_users
    _backup(alice, "alice-mac", {"/a/x.bin": BIG})
    _backup(bob, "bob-mac", {"/b/x.bin": BIG})
    assert _dups(alice)["group_count"] == 0
    assert _dups(bob)["group_count"] == 0
    _backup(bob, "bob-pc", {"/c/x.bin": BIG})
    d = _dups(bob)
    assert d["group_count"] == 1 and {f["label"] for f in d["groups"][0]["files"]} == {"bob-mac", "bob-pc"}


def test_delete_copy_in_server_folder_goes_to_trash(client):
    _backup(client, "mac", {"/v/x.bin": BIG})
    srv = _drive(client)
    _put(client, srv, "x.bin", BIG)
    d = _dups(client)
    copy = next(f for f in d["groups"][0]["files"] if f["editable"])

    r = client.post("/cloud/drive/delete", json={"label": copy["label"], "items": [copy["path"]]})
    assert r.status_code == 200 and r.json()["trashed"] == 1
    assert _dups(client)["group_count"] == 0
    assert client.get("/cloud/drive/trash", params={"label": srv}).json()
    # Backup do cliente continua somente leitura.
    r = client.post("/cloud/drive/delete", json={"label": "mac", "items": ["x.bin"]})
    assert r.status_code == 403
