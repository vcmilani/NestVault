"""Front cloud — pastas do servidor: criar, upload, mkdir, mover, renomear,
lixeira, copiar de backup sem gastar espaço, e as travas (backup somente leitura,
cliente não escreve, limpezas não mexem)."""
import base64
from datetime import datetime, timedelta

import database as db_mod
import main as m
from conftest import make_backup, make_version, finish_version, upload_file

V1 = "2026-01-01T00:00:00"


def _drive(c, name="Documentos"):
    r = c.post("/cloud/drives", json={"name": name})
    assert r.status_code == 200, r.text
    return r.json()["label"]


def _put(c, label, path, content=b"x", expect=200):
    r = c.put("/cloud/drive/upload", content=content, headers={
        "X-Label": label, "X-Path": base64.b64encode(path.encode()).decode(), "X-Mtime": "1000"})
    assert r.status_code == expect, r.text
    return r.json()


def _tree(c, label, path=""):
    r = c.get("/cloud/tree", params={"label": label, "path": path})
    assert r.status_code == 200, r.text
    d = r.json()
    return [f["name"] for f in d["folders"]], [f["name"] for f in d["files"]], d


def _post(c, url, body, expect=200):
    r = c.post(url, json=body)
    assert r.status_code == expect, r.text
    return r.json()


def _db():
    return m.SessionLocal()


# -- Criação e listagem --------------------------------------------------------

def test_create_drive_lists_with_name_and_kind(client):
    make_backup(client, "fotos")
    label = _drive(client, "Documentos")
    assert label.startswith("srv-")
    labels = {l["label"]: l for l in client.get("/cloud/tree").json()["labels"]}
    assert labels[label]["kind"] == "server" and labels[label]["name"] == "Documentos"
    assert labels["fotos"]["kind"] == "backup" and labels["fotos"]["name"] == "fotos"
    folders, files, d = _tree(client, label)
    assert d["editable"] is True and d["name"] == "Documentos"
    assert (folders, files) == ([], [])
    # Nome repetido do mesmo usuário
    _post(client, "/cloud/drives", {"name": "Documentos"}, 409)
    # /backups mostra o tipo
    kinds = {b["label"]: b["kind"] for b in client.get("/backups").json()}
    assert kinds[label] == "server" and kinds["fotos"] == "backup"


def test_upload_mkdir_and_content(client):
    label = _drive(client)
    _put(client, label, "a/b/nota.txt", b"ola")
    _post(client, "/cloud/drive/mkdir", {"label": label, "path": "vazia/sub"})
    folders, files, _ = _tree(client, label)
    assert folders == ["a", "vazia"]
    assert _tree(client, label, "vazia")[0] == ["sub"]
    f = _tree(client, label, "a/b")[2]["files"][0]
    assert client.get(f"/cloud/content/{f['id']}").content == b"ola"
    _post(client, "/cloud/drive/mkdir", {"label": label, "path": "a"}, 409)
    _post(client, "/cloud/drive/mkdir", {"label": label, "path": "a/b/nota.txt"}, 409)
    _post(client, "/cloud/drive/mkdir", {"label": label, "path": "../x"}, 400)
    # Pasta no lugar de arquivo e vice-versa
    _put(client, label, "a", b"y", 409)
    _put(client, label, "a/b/nota.txt/z", b"y", 409)


def test_upload_replace_sends_old_to_trash(client):
    label = _drive(client)
    _put(client, label, "doc.txt", b"v1")
    _put(client, label, "doc.txt", b"v2")
    f = _tree(client, label)[2]["files"]
    assert len(f) == 1 and client.get(f"/cloud/content/{f[0]['id']}").content == b"v2"
    trash = client.get("/cloud/drive/trash", params={"label": label}).json()
    assert [t["path"] for t in trash] == ["doc.txt"] and trash[0]["count"] == 1
    # Mesmo conteúdo de novo: nada vai para a lixeira
    _put(client, label, "doc.txt", b"v2")
    assert len(client.get("/cloud/drive/trash", params={"label": label}).json()) == 1


# -- Mover e renomear ----------------------------------------------------------

def test_move_files_and_folders(client):
    label = _drive(client)
    _put(client, label, "a/1.txt", b"1")
    _put(client, label, "a/sub/2.txt", b"2")
    _put(client, label, "solto.txt", b"3")
    _post(client, "/cloud/drive/mkdir", {"label": label, "path": "dest"})
    _post(client, "/cloud/drive/mkdir", {"label": label, "path": "a/vazia"})

    _post(client, "/cloud/drive/move", {"label": label, "items": ["a", "solto.txt"], "dest_dir": "dest"})
    folders, files, _ = _tree(client, label)
    assert folders == ["dest"] and files == []
    assert _tree(client, label, "dest") [:2] == (["a"], ["solto.txt"])
    assert _tree(client, label, "dest/a")[:2] == (["sub", "vazia"], ["1.txt"])

    # Para dentro dele mesmo, destino inexistente, item inexistente, colisão
    _post(client, "/cloud/drive/move", {"label": label, "items": ["dest"], "dest_dir": "dest/a"}, 400)
    _post(client, "/cloud/drive/move", {"label": label, "items": ["dest/solto.txt"], "dest_dir": "nada"}, 404)
    _post(client, "/cloud/drive/move", {"label": label, "items": ["nada"], "dest_dir": ""}, 404)
    _put(client, label, "solto.txt", b"outro")
    _post(client, "/cloud/drive/move", {"label": label, "items": ["dest/solto.txt"], "dest_dir": ""}, 409)


def test_rename(client):
    label = _drive(client)
    _put(client, label, "pasta/x.txt", b"1")
    _put(client, label, "y.txt", b"2")
    assert _post(client, "/cloud/drive/rename",
                 {"label": label, "path": "pasta", "new_name": "Pasta Nova"})["path"] == "Pasta Nova"
    assert _tree(client, label, "Pasta Nova")[1] == ["x.txt"]
    _post(client, "/cloud/drive/rename", {"label": label, "path": "y.txt", "new_name": "Pasta Nova"}, 409)
    _post(client, "/cloud/drive/rename", {"label": label, "path": "y.txt", "new_name": "a/b"}, 400)
    # Só caixa: permitido
    _post(client, "/cloud/drive/rename", {"label": label, "path": "y.txt", "new_name": "Y.txt"})
    assert "Y.txt" in _tree(client, label)[1]


def test_rename_drive(client):
    label = _drive(client, "A")
    _drive(client, "B")
    r = client.patch(f"/cloud/drives/{label}", json={"name": "B"})
    assert r.status_code == 409
    assert client.patch(f"/cloud/drives/{label}", json={"name": "C"}).json()["name"] == "C"


# -- Lixeira -------------------------------------------------------------------

def test_delete_restore_and_purge(client):
    label = _drive(client)
    _put(client, label, "p/1.txt", b"1")
    _put(client, label, "p/2.txt", b"22")
    _put(client, label, "f.txt", b"3")
    _post(client, "/cloud/drive/delete", {"label": label, "items": ["p", "p/1.txt", "f.txt"]})
    assert _tree(client, label)[:2] == ([], [])
    trash = client.get("/cloud/drive/trash", params={"label": label}).json()
    by = {t["path"]: t for t in trash}
    assert set(by) == {"p", "f.txt"}          # p/1.txt foi junto com p
    assert by["p"]["is_dir"] and by["p"]["count"] == 2 and by["p"]["size"] == 3

    # Mesmo caminho excluído duas vezes não colide na lixeira
    _put(client, label, "f.txt", b"4")
    _post(client, "/cloud/drive/delete", {"label": label, "items": ["f.txt"]})
    assert len(client.get("/cloud/drive/trash", params={"label": label}).json()) == 3

    _post(client, "/cloud/drive/restore", {"label": label, "ids": [by["p"]["id"]]})
    assert _tree(client, label, "p")[1] == ["1.txt", "2.txt"]
    # Restaurar onde já existe: 409
    _put(client, label, "f.txt", b"5")
    _post(client, "/cloud/drive/restore", {"label": label, "ids": [by["f.txt"]["id"]]}, 409)

    _post(client, "/cloud/drive/purge", {"label": label, "ids": [by["f.txt"]["id"]]})
    assert len(client.get("/cloud/drive/trash", params={"label": label}).json()) == 1


def test_nightly_purges_expired_server_trash_and_keeps_live(client, monkeypatch):
    import nightly_cleanup as nc
    monkeypatch.setattr(nc, "SessionLocal", m.SessionLocal)
    monkeypatch.setattr(nc, "engine", m.SessionLocal.kw["bind"])

    label = _drive(client)
    _put(client, label, "fica.txt", b"fica")
    _put(client, label, "velho.txt", b"velho")
    _put(client, label, "recente.txt", b"recente")
    _post(client, "/cloud/drive/delete", {"label": label, "items": ["velho.txt"]})
    _post(client, "/cloud/drive/delete", {"label": label, "items": ["recente.txt"]})
    db = _db()
    try:
        e = db.query(db_mod.ServerTrashEntry).filter_by(path="velho.txt").one()
        e.deleted_at = datetime.now() - timedelta(days=365)
        # Versão live "antiga" e única: a retenção não pode tocar
        for v in db.query(db_mod.BackupVersion).filter_by(backup_label=label):
            v.created_at = datetime.now() - timedelta(days=400)
        db.commit()
    finally:
        db.close()

    nc.run_nightly_cleanup()

    assert _tree(client, label)[1] == ["fica.txt"]
    assert [t["path"] for t in client.get("/cloud/drive/trash", params={"label": label}).json()] == ["recente.txt"]
    db = _db()
    try:
        assert db.query(db_mod.FileContent).filter(
            db_mod.FileContent.size == len(b"velho")).count() == 0
    finally:
        db.close()


# -- Copiar de backup ----------------------------------------------------------

def test_copy_from_backup_reuses_content(client):
    make_backup(client, "fotos")
    make_version(client, "fotos", V1)
    upload_file(client, "fotos", V1, path="/Users/a/Pictures/2024/a.jpg", content=b"A" * 10)
    upload_file(client, "fotos", V1, path="/Users/a/Pictures/2024/b.jpg", content=b"B" * 10)
    upload_file(client, "fotos", V1, path="/Users/a/Pictures/c.jpg", content=b"C" * 10)
    finish_version(client, "fotos", V1)
    label = _drive(client)
    db = _db()
    try:
        before = db.query(db_mod.FileContent).count()
    finally:
        db.close()

    n = _post(client, "/cloud/drive/copy", {"src_label": "fotos", "items": ["2024", "c.jpg"],
                                            "dest_label": label, "dest_dir": ""})["copied"]
    assert n == 3
    assert _tree(client, label)[:2] == (["2024"], ["c.jpg"])
    assert _tree(client, label, "2024")[1] == ["a.jpg", "b.jpg"]
    db = _db()
    try:
        assert db.query(db_mod.FileContent).count() == before
    finally:
        db.close()
    # Backup intacto e colisão no destino
    assert _tree(client, "fotos")[:2] == (["2024"], ["c.jpg"])
    _post(client, "/cloud/drive/copy", {"src_label": "fotos", "items": ["c.jpg"],
                                        "dest_label": label}, 409)


# -- Travas --------------------------------------------------------------------

def test_backup_is_read_only(client):
    make_backup(client, "fotos")
    make_version(client, "fotos", V1)
    upload_file(client, "fotos", V1, path="/x/a.txt")
    finish_version(client, "fotos", V1)
    _post(client, "/cloud/drive/mkdir", {"label": "fotos", "path": "nova"}, 403)
    _post(client, "/cloud/drive/delete", {"label": "fotos", "items": ["a.txt"]}, 403)
    _put(client, "fotos", "b.txt", b"x", 403)
    assert _tree(client, "fotos")[2]["editable"] is False


def test_client_endpoints_refuse_server_drive(client):
    label = _drive(client)
    _put(client, label, "a.txt", b"a")
    assert client.post(f"/backups/{label}/versions", json={"version_key": V1}).status_code == 409
    assert client.patch(f"/backups/{label}/versions/trash", json={"status": "done"}).status_code == 409
    assert client.delete(f"/backups/{label}/versions/live").status_code == 409
    assert client.post(f"/backups/{label}/cleanup", json={"backup_label": label, "keep": 0}).status_code == 409
    r = client.post("/upload", content=b"z", headers={
        "X-Backup-Label": label, "X-Version-Key": "trash",
        "X-Original-Path": base64.b64encode(b"z.txt").decode(), "X-Mtime": "1"})
    assert r.status_code == 409
    # Limpezas administrativas não enxergam as versões da pasta do servidor
    assert client.get("/maintenance/cleanup-versions/preview", params={"keep": 0}).json()["total"] == 0
    assert client.get("/maintenance/cleanup-by-date/preview",
                      params={"before": "2999-01-01T00:00:00"}).json()["total"] == 0


def test_write_requires_header_and_ownership(two_users):
    admin, alice, bob = two_users
    label = _drive(alice)
    _put(alice, label, "a.txt", b"a")
    _post(bob, "/cloud/drive/mkdir", {"label": label, "path": "x"}, 403)
    _put(bob, label, "b.txt", b"b", 403)
    # Nomes são por usuário
    _drive(bob, "Documentos")
    # Só cookie de sessão (sem header) não escreve
    alice.post("/cloud/session")
    key = alice.headers.pop("X-API-Key")
    try:
        r = alice.post("/cloud/drive/mkdir", json={"label": label, "path": "y"})
        assert r.status_code == 401
        assert alice.get("/cloud/tree", params={"label": label}).status_code == 200
    finally:
        alice.headers["X-API-Key"] = key


def test_admin_versions_list_shows_live_and_trash(client):
    label = _drive(client)
    _put(client, label, "a.txt", b"a")
    _put(client, label, "b.txt", b"bb")
    _post(client, "/cloud/drive/delete", {"label": label, "items": ["b.txt"]})
    r = client.get(f"/backups/{label}/versions")
    assert r.status_code == 200, r.text
    assert [v["version_key"] for v in r.json()] == ["live", "trash"]  # a final primeiro
    by = {v["version_key"]: v for v in r.json()}
    assert by["live"]["status"] == "done" and by["live"]["file_count"] == 1
    assert by["trash"]["status"] == "server-trash" and by["trash"]["file_count"] == 1
    r = client.get(f"/backups/{label}/versions/trash")
    assert r.status_code == 200 and r.json()["status"] == "server-trash"


def test_delete_drive_label(client):
    label = _drive(client)
    _put(client, label, "a.txt", b"a")
    _post(client, "/cloud/drive/mkdir", {"label": label, "path": "v"})
    _post(client, "/cloud/drive/delete", {"label": label, "items": ["a.txt"]})
    assert client.delete(f"/backups/{label}").status_code == 200
    assert all(l["label"] != label for l in client.get("/cloud/tree").json()["labels"])
