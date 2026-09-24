"""Mapeamento original_path → caminho relativo no restore (client/nestvault.py).

O backup grava str(Path) nativo do cliente, então o restore precisa entender
caminhos de Windows mesmo rodando em POSIX e vice-versa."""
import pytest

import nestvault

R = nestvault._restore_relative


@pytest.mark.parametrize("windows", [False, True])
def test_windows_absolute_path_keeps_drive_as_folder(windows):
    # Antes: no Windows, "C:\\..." continuava absoluto e o restore rejeitava tudo.
    assert R(r"C:\Users\ana\doc.txt", None, windows) == ["C", "Users", "ana", "doc.txt"]


@pytest.mark.parametrize("windows", [False, True])
def test_posix_absolute_path(windows):
    assert R("/home/ana/doc.txt", None, windows) == ["home", "ana", "doc.txt"]


def test_backslash_is_a_filename_char_in_posix_paths():
    assert R("/home/ana/a\\b.txt", None, windows=False) == ["home", "ana", "a\\b.txt"]


@pytest.mark.parametrize("windows", [False, True])
def test_windows_prefix_mode_path(windows):
    # Backup com --prefix docs num cliente Windows grava "docs\\sub\\f.txt".
    assert R(r"docs\sub\f.txt", "docs", windows) == ["sub", "f.txt"]


def test_windows_unc_path():
    assert R(r"\\nas\share\f.txt", None, windows=False) == ["nas", "share", "f.txt"]


def test_prefix_matches_by_component_not_by_string():
    assert R("/data/docs2/x.txt", "/data/docs", windows=False) == ["data", "docs2", "x.txt"]
    assert R("/data/docs/x.txt", "/data/docs", windows=False) == ["x.txt"]


def test_prefix_is_case_insensitive_only_on_windows():
    assert R(r"C:\Users\Ana\x.txt", r"c:\users\ana", windows=True) == ["x.txt"]
    assert R("/Data/x.txt", "/data", windows=False) == ["Data", "x.txt"]


@pytest.mark.parametrize("path", ["/a/../../etc/passwd", r"C:\a\..\..\x", "..", "/", ""])
def test_unsafe_or_empty_paths_are_rejected(path):
    assert R(path, None, windows=False) is None
    assert R(path, None, windows=True) is None


def test_restore_writes_windows_backup_inside_destination(tmp_path, monkeypatch):
    """Ponta a ponta: um backup feito no Windows é restaurado dentro do destino."""
    records = [{"id": 1, "original_path": r"C:\Users\ana\doc.txt",
                "sha256": nestvault.hashlib.sha256(b"oi").hexdigest(), "size": 2}]

    class _Resp:
        def __init__(self, payload=None, body=b""):
            self._payload, self._body = payload, body
            self.headers = {"Content-Length": str(len(body))}
        def raise_for_status(self): pass
        def json(self): return self._payload
        def iter_content(self, chunk_size): yield self._body

    def fake_get(url, **_kw):
        return _Resp(records) if url.endswith("/files") else _Resp(body=b"oi")

    monkeypatch.setattr(nestvault._session, "get", fake_get)
    nestvault.restore(str(tmp_path), "lbl", "v1", server="http://x")

    assert (tmp_path / "C" / "Users" / "ana" / "doc.txt").read_bytes() == b"oi"
