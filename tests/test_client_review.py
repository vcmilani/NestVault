"""Cliente CLI — correções da revisão v10.4.0: Smart Skip não deixa versão
"done" vazia, exit codes, cleanup sem terminal, restore atômico, exclude com
curingas, symlinks, URLs citadas, markup escapado e prompt de chave."""
import hashlib
import os
import sys

import pytest

import nestvault


# -- decisões puras -----------------------------------------------------------------

def test_excluded_matches_name_and_relative_path_with_globs():
    assert nestvault._excluded("cache.tmp", "sub/cache.tmp", ["*.tmp"])
    assert nestvault._excluded("node_modules", "a/node_modules", ["node_modules"])
    assert nestvault._excluded("build", "docs/build", ["docs/build"])
    assert not nestvault._excluded("build", "src/build", ["docs/build"])
    assert not nestvault._excluded("keep.txt", "keep.txt", ["*.tmp", "cache"])


def test_absorb_ok_requires_inheritance_on_smart_skip():
    prev = {"a": {}}
    assert nestvault._absorb_ok(None, True, prev) is False
    assert nestvault._absorb_ok({"inherited": 0, "skipped": 0}, True, prev) is False
    assert nestvault._absorb_ok({"inherited": 3, "skipped": 0}, True, prev) is True
    # Modo acumulativo: herdar zero é legítimo (nada faltava localmente).
    assert nestvault._absorb_ok({"inherited": 0, "skipped": 5}, False, prev) is True
    assert nestvault._absorb_ok(None, False, prev) is False


class _Resp:
    def __init__(self, payload=None, body=b"", status=200):
        self._payload, self._body = payload, body
        self.ok = status < 400
        self.status_code = status
        self.headers = {"Content-Length": str(len(body))}
    def raise_for_status(self): pass
    def json(self): return self._payload
    def iter_content(self, chunk_size): yield self._body


def test_fetch_prev_cache_skips_empty_done_version(monkeypatch):
    versions = [
        {"version_key": "v3", "status": "done", "file_count": 0},    # absorb falho de antes
        {"version_key": "v2", "status": "failed", "file_count": 0},
        {"version_key": "v1", "status": "done", "file_count": 2},
    ]
    seen = []

    def fake_get(url, **kw):
        seen.append(url)
        if url.endswith("/versions"):
            return _Resp(versions)
        assert kw["params"]["version_key"] == "v1"
        return _Resp([{"original_path": "/a", "sha256": "x", "size": 1, "mtime": 1.0}])

    monkeypatch.setattr(nestvault._session, "get", fake_get)
    monkeypatch.setattr(nestvault, "_load_local_hash_cache", lambda label: {})
    key, cache = nestvault._fetch_prev_cache("http://x", "lbl")
    assert key == "v1" and set(cache) == {"/a"}


def test_labels_are_url_quoted(monkeypatch):
    urls = []
    monkeypatch.setattr(nestvault._session, "get", lambda url, **kw: (urls.append(url), _Resp([]))[1])
    nestvault.list_versions("a#b/c d", server="http://x")
    assert urls == ["http://x/backups/a%23b%2Fc%20d/versions"]


# -- saída: markup escapado ----------------------------------------------------------------

def test_console_helpers_survive_markup_like_names():
    with nestvault.console.capture() as cap:
        nestvault._err("falha em x[/b].txt")
        nestvault._warn("[docs] atrasado")
        nestvault._kv("Label", "[docs]")
    out = cap.get()
    assert "x[/b].txt" in out and "[docs] atrasado" in out and "[docs]" in out


# -- prompt de chave e confirmação sem terminal ----------------------------------------------

def test_prompt_api_key_without_tty_does_not_prompt(monkeypatch):
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False, raising=False)
    monkeypatch.setattr(nestvault, "API_KEY", "k1")
    assert nestvault._prompt_api_key(rejected="k1") is False
    # Outra thread já trocou a chave: sem prompt, e segue com a nova.
    assert nestvault._prompt_api_key(rejected="antiga") is True


def test_confirm_without_tty_exits_2_unless_yes(monkeypatch):
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False, raising=False)
    assert nestvault._confirm("? ", yes=True) is True
    with pytest.raises(SystemExit) as exc:
        nestvault._confirm("? ", yes=False)
    assert exc.value.code == 2


# -- restore atômico ---------------------------------------------------------------------------

def _restore_env(monkeypatch, body, sha, mtime=1_600_000_000.0):
    records = [{"id": 1, "original_path": "/home/ana/doc.txt", "sha256": sha, "size": len(body), "mtime": mtime}]
    monkeypatch.setattr(nestvault._session, "get",
                        lambda url, **kw: _Resp(records) if url.endswith("/files") else _Resp(body=body))


def test_restore_overwrite_keeps_original_when_hash_fails(tmp_path, monkeypatch):
    dest = tmp_path / "home" / "ana" / "doc.txt"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"original intacto")
    _restore_env(monkeypatch, body=b"corrompido", sha="0" * 64)

    stats = nestvault.restore(str(tmp_path), "lbl", "v1", server="http://x", overwrite=True)

    assert stats["errors"] == 1 and stats["restored"] == 0
    assert dest.read_bytes() == b"original intacto"
    assert list(tmp_path.rglob("*.nvpart")) == []


def test_restore_replaces_atomically_and_restores_mtime(tmp_path, monkeypatch):
    dest = tmp_path / "home" / "ana" / "doc.txt"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"velho")
    body = b"novo conteudo"
    _restore_env(monkeypatch, body=body, sha=hashlib.sha256(body).hexdigest(), mtime=1_600_000_000.0)

    stats = nestvault.restore(str(tmp_path), "lbl", "v1", server="http://x", overwrite=True)

    assert stats["restored"] == 1 and stats["errors"] == 0
    assert dest.read_bytes() == body
    assert int(dest.stat().st_mtime) == 1_600_000_000
    assert list(tmp_path.rglob("*.nvpart")) == []


def test_restore_network_error_leaves_no_partial_file(tmp_path, monkeypatch):
    records = [{"id": 1, "original_path": "/home/ana/doc.txt", "sha256": "0" * 64, "size": 5, "mtime": None}]

    class _Boom(_Resp):
        def iter_content(self, chunk_size):
            yield b"par"
            raise nestvault.requests.ConnectionError("caiu")

    monkeypatch.setattr(nestvault._session, "get",
                        lambda url, **kw: _Resp(records) if url.endswith("/files") else _Boom())
    stats = nestvault.restore(str(tmp_path), "lbl", "v1", server="http://x")
    assert stats["errors"] == 1
    assert not (tmp_path / "home" / "ana" / "doc.txt").exists()
    assert list(tmp_path.rglob("*.nvpart")) == []


# -- varredura: symlinks e exclude ---------------------------------------------------------------

@pytest.mark.skipif(sys.platform == "win32", reason="symlink exige privilégio no Windows")
def test_scan_skips_file_symlinks_unless_asked(tmp_path, monkeypatch):
    root = tmp_path / "root"; root.mkdir()
    (root / "real.txt").write_text("x")
    secret = tmp_path / "fora" / "id_rsa"
    secret.parent.mkdir(); secret.write_text("segredo")
    os.symlink(secret, root / "link.txt")
    monkeypatch.setattr(nestvault, "_local_cache_dir", lambda: tmp_path / "cache")

    stats = nestvault.backup_directory(root, "t", dry_run=True)
    assert stats["uploaded"] == 1
    stats = nestvault.backup_directory(root, "t", dry_run=True, follow_symlinks=True)
    assert stats["uploaded"] == 2


def test_scan_exclude_accepts_globs_and_relative_paths(tmp_path, monkeypatch):
    root = tmp_path / "root"
    for rel in ("keep.txt", "a.tmp", "docs/build/out.js", "src/build/keep.js", "cache/x"):
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x")
    monkeypatch.setattr(nestvault, "_local_cache_dir", lambda: tmp_path / "cache")

    stats = nestvault.backup_directory(root, "t", dry_run=True, exclude=["*.tmp", "docs/build", "cache"])
    assert stats["uploaded"] == 2  # keep.txt e src/build/keep.js


# -- main: exit codes e --exclude achatado -----------------------------------------------------------

def test_main_exits_1_when_backup_has_errors(monkeypatch):
    calls = {}

    def fake_backup(directory, label, *a, **kw):
        calls["exclude"] = a[3] if len(a) > 3 else kw.get("exclude")
        return {"uploaded": 0, "registered": 0, "fast": 0, "skipped": 0, "errors": 2}

    monkeypatch.setattr(nestvault, "backup_directory", fake_backup)
    monkeypatch.setattr(sys, "argv", ["nestvault", "backup", "/tmp/x", "--label", "l",
                                      "--exclude", "a", "b", "--exclude", "c"])
    with pytest.raises(SystemExit) as exc:
        nestvault.main()
    assert exc.value.code == 1
    assert calls["exclude"] == ["a", "b", "c"]


def test_main_exits_1_when_restore_has_errors(monkeypatch):
    monkeypatch.setattr(nestvault, "restore", lambda *a, **k: {"restored": 1, "skipped": 0, "errors": 1})
    monkeypatch.setattr(sys, "argv", ["nestvault", "restore", "/tmp/x", "--label", "l", "--version", "v"])
    with pytest.raises(SystemExit) as exc:
        nestvault.main()
    assert exc.value.code == 1


def test_main_cleanup_passes_yes_and_exit_code(monkeypatch):
    seen = {}

    def fake_cleanup(label=None, keep=5, server=None, yes=False):
        seen.update(label=label, keep=keep, yes=yes)
        return 0

    monkeypatch.setattr(nestvault, "cleanup", fake_cleanup)
    monkeypatch.setattr(sys, "argv", ["nestvault", "cleanup", "--all", "--keep", "3", "--yes"])
    nestvault.main()  # exit 0: não levanta
    assert seen == {"label": None, "keep": 3, "yes": True}
