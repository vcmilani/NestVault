"""Front cloud — drive: árvore por pasta, histórico, conteúdo inline com Range e
isolamento entre usuários (header e cookie de sessão)."""
import os

import pytest
from fastapi.testclient import TestClient

import crypto
import main as m
import storage as storage_mod
from conftest import make_backup, make_version, finish_version, upload_file

V1 = "2026-01-01T00:00:00"
V2 = "2026-01-02T00:00:00"


def _seed(c, label="fotos", base="/Users/alice/Pictures"):
    make_backup(c, label=label)
    make_version(c, label=label, version_key=V1)
    upload_file(c, label, V1, path=f"{base}/2024/a.jpg", content=b"A" * 10)
    upload_file(c, label, V1, path=f"{base}/2024/b.jpg", content=b"B" * 20)
    upload_file(c, label, V1, path=f"{base}/2025/viagem/c.mp4", content=b"C" * 30)
    upload_file(c, label, V1, path=f"{base}/notas.txt", content=b"v1")
    finish_version(c, label, V1)


def _children(c, **params):
    r = c.get("/cloud/tree", params=params)
    assert r.status_code == 200, r.text
    return r.json()


# -- Árvore -------------------------------------------------------------------

def test_root_lists_only_own_labels(two_users):
    admin, alice, bob = two_users
    _seed(alice, "alice-fotos")
    _seed(bob, "bob-fotos", base="/home/bob")
    _seed(admin, "admin-docs", base="/srv")

    assert [l["label"] for l in _children(alice)["labels"]] == ["alice-fotos"]
    assert [l["label"] for l in _children(bob)["labels"]] == ["bob-fotos"]
    # O drive é pessoal até para o admin
    assert [l["label"] for l in _children(admin)["labels"]] == ["admin-docs"]


def test_label_root_strips_common_prefix(client):
    _seed(client)
    d = _children(client, label="fotos")
    assert d["path"] == ""
    assert [f["name"] for f in d["folders"]] == ["2024", "2025"]
    assert d["folders"][0]["count"] == 2 and d["folders"][0]["size"] == 30
    assert [f["name"] for f in d["files"]] == ["notas.txt"]
    assert d["is_latest"] is True


def test_subfolder_and_pagination(client):
    _seed(client)
    d = _children(client, label="fotos", path="2024", limit=1, sort="size")
    assert d["total_files"] == 2
    assert [f["name"] for f in d["files"]] == ["b.jpg"]
    d2 = _children(client, label="fotos", path="/2024/", limit=1, offset=1, sort="size")
    assert [f["name"] for f in d2["files"]] == ["a.jpg"]
    nested = _children(client, label="fotos", path="2025")
    assert [f["name"] for f in nested["folders"]] == ["viagem"] and nested["files"] == []


def test_like_wildcards_in_folder_names_are_literal(client):
    make_backup(client, label="w")
    make_version(client, label="w", version_key=V1)
    upload_file(client, "w", V1, path="/r/a_b/x.txt", content=b"1")
    upload_file(client, "w", V1, path="/r/aXb/y.txt", content=b"2")
    finish_version(client, "w", V1)
    d = _children(client, label="w", path="a_b")
    assert [f["name"] for f in d["files"]] == ["x.txt"]


def test_version_selection_and_history(client):
    _seed(client)
    make_version(client, label="fotos", version_key=V2)
    upload_file(client, "fotos", V2, path="/Users/alice/Pictures/notas.txt", content=b"v2-mudou")
    upload_file(client, "fotos", V2, path="/Users/alice/Pictures/2024/a.jpg", content=b"A" * 10)
    finish_version(client, "fotos", V2)

    latest = _children(client, label="fotos")
    assert latest["version_key"] == V2 and [f["name"] for f in latest["folders"]] == ["2024"]
    old = _children(client, label="fotos", version=V1)
    assert old["is_latest"] is False and [f["name"] for f in old["folders"]] == ["2024", "2025"]

    versions = client.get("/cloud/versions", params={"label": "fotos"}).json()
    assert [v["version_key"] for v in versions] == [V2, V1]

    h = client.get("/cloud/history", params={"label": "fotos", "path": "notas.txt"}).json()
    assert [(x["version_key"], x["changed"]) for x in h] == [(V2, True), (V1, True)]
    h = client.get("/cloud/history", params={"label": "fotos", "path": "2024/a.jpg"}).json()
    assert [(x["version_key"], x["changed"]) for x in h] == [(V2, False), (V1, True)]


def test_running_version_is_hidden(client):
    _seed(client)
    make_version(client, label="fotos", version_key=V2)  # running: não aparece
    assert [v["version_key"] for v in client.get("/cloud/versions", params={"label": "fotos"}).json()] == [V1]
    assert client.get("/cloud/tree", params={"label": "fotos", "version": V2}).status_code == 404


def test_trashed_label_is_hidden(two_users):
    _admin, alice, _bob = two_users
    _seed(alice, "alice-fotos")
    fid = _children(alice, label="alice-fotos")["files"][0]["id"]
    assert alice.delete("/backups/alice-fotos").status_code == 200  # usuário comum → lixeira
    assert _children(alice)["labels"] == []
    assert alice.get("/cloud/tree", params={"label": "alice-fotos"}).status_code == 404
    assert alice.get(f"/cloud/content/{fid}").status_code == 404


def test_search(client):
    _seed(client)
    r = client.get("/cloud/search", params={"label": "fotos", "q": "VIAGEM"}).json()
    assert [x["path"] for x in r["results"]] == ["2025/viagem/c.mp4"]


# -- Isolamento ---------------------------------------------------------------

def test_other_user_cannot_browse_or_read(two_users):
    _admin, alice, bob = two_users
    _seed(alice, "alice-fotos")
    fid = _children(alice, label="alice-fotos")["files"][0]["id"]
    for path, params in (("/cloud/tree", {"label": "alice-fotos"}),
                         ("/cloud/versions", {"label": "alice-fotos"}),
                         ("/cloud/search", {"label": "alice-fotos", "q": "a"}),
                         ("/cloud/history", {"label": "alice-fotos", "path": "notas.txt"})):
        assert bob.get(path, params=params).status_code == 403, path
    assert bob.get(f"/cloud/content/{fid}").status_code == 403
    assert alice.get(f"/cloud/content/{fid}").status_code == 200


def test_requires_auth(auth_client):
    assert auth_client.get("/cloud/tree").status_code == 401


# -- Sessão por cookie --------------------------------------------------------

def test_session_cookie_authenticates_reads(two_users):
    _admin, alice, bob = two_users
    _seed(alice, "alice-fotos")
    fid = _children(alice, label="alice-fotos")["files"][0]["id"]

    # Sem `with`: o lifespan já roda no fixture; subir outro reinicia o scheduler.
    browser = TestClient(m.app)  # sem header: só o cookie
    assert browser.get("/cloud/tree").status_code == 401
    r = browser.post("/cloud/session", headers={"X-API-Key": "alice-key"})
    assert r.status_code == 200 and r.json()["username"] == "alice"
    cookie = r.cookies.get("nv_session")
    assert cookie
    assert "httponly" in r.headers["set-cookie"].lower()
    assert "samesite=strict" in r.headers["set-cookie"].lower()

    assert [l["label"] for l in browser.get("/cloud/tree").json()["labels"]] == ["alice-fotos"]
    assert browser.get(f"/cloud/content/{fid}").status_code == 200
    # Cookie não serve para rotas fora do /cloud
    assert browser.get("/backups").status_code == 401

    browser.delete("/cloud/session")
    browser.cookies.clear()
    assert browser.get("/cloud/tree").status_code == 401


def test_session_rejects_tampered_or_rotated(two_users):
    admin, alice, _bob = two_users
    # Sem `with`: o lifespan já roda no fixture; subir outro reinicia o scheduler.
    browser = TestClient(m.app)
    token = browser.post("/cloud/session", headers={"X-API-Key": "alice-key"}).cookies["nv_session"]
    uid, exp, sig = token.split(":")

    browser.cookies.clear()
    browser.cookies.set("nv_session", f"{int(uid) + 1}:{exp}:{sig}")  # troca de usuário
    assert browser.get("/cloud/tree").status_code == 401

    browser.cookies.clear()
    browser.cookies.set("nv_session", token)
    assert browser.get("/cloud/tree").status_code == 200
    # Rotacionar a chave derruba a sessão
    alice_id = next(u["id"] for u in admin.get("/users").json() if u["username"] == "alice")
    assert admin.post(f"/users/{alice_id}/rotate-key").status_code == 200
    assert browser.get("/cloud/tree").status_code == 401


# -- Conteúdo -----------------------------------------------------------------

def _file_id(c, label, path, name):
    return next(f["id"] for f in _children(c, label=label, path=path)["files"] if f["name"] == name)


def test_content_inline_mime_and_range(client):
    make_backup(client, label="v")
    make_version(client, label="v", version_key=V1)
    data = bytes(range(256)) * 40
    upload_file(client, "v", V1, path="/m/clip.mp4", content=data)
    upload_file(client, "v", V1, path="/m/page.html", content=b"<script>alert(1)</script>")
    upload_file(client, "v", V1, path="/m/readme.md", content=b"# oi")
    finish_version(client, "v", V1)

    fid = _file_id(client, "v", "", "clip.mp4")
    r = client.get(f"/cloud/content/{fid}")
    assert r.status_code == 200 and r.content == data
    assert r.headers["content-type"] == "video/mp4"
    assert r.headers["content-disposition"].startswith("inline")
    assert r.headers["accept-ranges"] == "bytes"

    r = client.get(f"/cloud/content/{fid}", headers={"Range": "bytes=100-199"})
    assert r.status_code == 206 and r.content == data[100:200]
    assert r.headers["content-range"] == f"bytes 100-199/{len(data)}"
    r = client.get(f"/cloud/content/{fid}", headers={"Range": "bytes=-10"})
    assert r.status_code == 206 and r.content == data[-10:]
    r = client.get(f"/cloud/content/{fid}", headers={"Range": f"bytes={len(data)}-"})
    assert r.status_code == 416

    r = client.get(f"/cloud/content/{fid}", params={"download": "true"})
    assert r.headers["content-disposition"].startswith("attachment")
    assert r.headers["content-type"] == "application/octet-stream"

    # HTML de backup nunca é renderizado na origem do painel
    html = client.get(f"/cloud/content/{_file_id(client, 'v', '', 'page.html')}")
    assert html.headers["content-type"].startswith("text/plain")
    assert "sandbox" in html.headers["content-security-policy"]
    md = client.get(f"/cloud/content/{_file_id(client, 'v', '', 'readme.md')}")
    assert md.headers["content-type"].startswith("text/plain") and md.content == b"# oi"


@pytest.mark.parametrize("rng", [(0, 0), (5, 1048575), (1048570, 1048600), (2097000, None), (0, None)])
def test_encrypted_range_crosses_chunks(client, monkeypatch, rng):
    monkeypatch.setattr(storage_mod, "ENCRYPTION_ENABLED", True, raising=False)
    monkeypatch.setattr(storage_mod, "encryption_key", os.urandom(32), raising=False)
    data = os.urandom(2 * crypto.CHUNK_SIZE + 12345)
    make_backup(client, label="enc")
    make_version(client, label="enc", version_key=V1)
    upload_file(client, "enc", V1, path="/v/big.mov", content=data)
    finish_version(client, "enc", V1)

    fid = _file_id(client, "enc", "", "big.mov")
    start, end = rng
    header = f"bytes={start}-{'' if end is None else end}"
    r = client.get(f"/cloud/content/{fid}", headers={"Range": header})
    want = data[start:(len(data) if end is None else end + 1)]
    assert r.status_code == 206
    assert r.content == want
    assert int(r.headers["content-length"]) == len(want)


def test_decrypt_range_matches_full_decrypt(tmp_path):
    key = os.urandom(32)
    src, dst = tmp_path / "p", tmp_path / "c"
    data = os.urandom(crypto.CHUNK_SIZE * 3 + 7)
    src.write_bytes(data)
    crypto.encrypt_stream(src, dst, key)
    for s, e in ((0, len(data) - 1), (crypto.CHUNK_SIZE - 1, crypto.CHUNK_SIZE),
                 (len(data) - 3, len(data) - 1), (crypto.CHUNK_SIZE * 2, crypto.CHUNK_SIZE * 2)):
        assert b"".join(crypto.decrypt_range(dst, key, s, e)) == data[s:e + 1]


def test_cloud_page_served(client):
    r = client.get("/cloud")
    assert r.status_code == 200 and "NestVault" in r.text


# -- Papel do usuário (navegação de usuário comum) ----------------------------

def test_me_reports_role_via_header_and_cookie(two_users):
    admin, alice, _bob = two_users
    assert admin.get("/cloud/me").json() == {"username": "admin", "role": "admin"}
    assert alice.get("/cloud/me").json() == {"username": "alice", "role": "user"}
    browser = TestClient(m.app)
    assert browser.get("/cloud/me").status_code == 401
    browser.post("/cloud/session", headers={"X-API-Key": "alice-key"})
    assert browser.get("/cloud/me").json()["role"] == "user"


def test_admin_pages_are_marked_for_the_guard(client):
    for path in ("/", "/disks", "/explorer", "/maintenance", "/activity", "/rclone-jobs",
                 "/stats", "/manage-users", "/settings"):
        html = client.get(path).text
        assert "<body data-admin-page>" in html, path
        assert "permissão de administrador" not in html, path
    for path in ("/cloud", "/photos"):
        assert "data-admin-page" not in client.get(path).text, path
