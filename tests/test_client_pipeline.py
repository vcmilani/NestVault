"""Testes unitarios para as funcoes puras do pipeline de backup do client
(client/nestvault.py) introduzidas para portar estrategias de performance do
client macOS: smart skip, idade de versao e o cache local de hash. Nao cobrem
rede/subprocessos — so a logica de decisao."""
from datetime import datetime, timedelta

import nestvault


def _key_days_ago(days: float) -> str:
    dt = datetime.now() - timedelta(days=days)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


# -- _version_age_days ---------------------------------------------------------

def test_version_age_days_recent():
    assert nestvault._version_age_days(_key_days_ago(0)) < 0.01


def test_version_age_days_old():
    age = nestvault._version_age_days(_key_days_ago(10))
    assert 9.9 < age < 10.1


def test_version_age_days_unparseable_is_infinite():
    assert nestvault._version_age_days("not-a-date") == float("inf")


# -- _smart_skip_eligible -------------------------------------------------------

def test_smart_skip_eligible_when_nothing_changed():
    key = _key_days_ago(1)
    assert nestvault._smart_skip_eligible(
        pending_hash=[], current_paths={"a", "b"}, prev_paths={"a", "b"},
        prev_done_key=key, accumulate=False, full_rescan_days=7,
    ) is True


def test_smart_skip_blocked_by_pending_hash():
    key = _key_days_ago(1)
    assert nestvault._smart_skip_eligible(
        pending_hash=[("fp", "a", 0.0, 1)], current_paths={"a"}, prev_paths={"a"},
        prev_done_key=key, accumulate=False, full_rescan_days=7,
    ) is False


def test_smart_skip_blocked_by_path_mismatch_deletion():
    key = _key_days_ago(1)
    assert nestvault._smart_skip_eligible(
        pending_hash=[], current_paths={"a"}, prev_paths={"a", "b"},
        prev_done_key=key, accumulate=False, full_rescan_days=7,
    ) is False


def test_smart_skip_blocked_by_accumulate():
    key = _key_days_ago(1)
    assert nestvault._smart_skip_eligible(
        pending_hash=[], current_paths={"a"}, prev_paths={"a"},
        prev_done_key=key, accumulate=True, full_rescan_days=7,
    ) is False


def test_smart_skip_blocked_without_prev_version():
    assert nestvault._smart_skip_eligible(
        pending_hash=[], current_paths={"a"}, prev_paths={"a"},
        prev_done_key=None, accumulate=False, full_rescan_days=7,
    ) is False


def test_smart_skip_blocked_by_safety_valve_age():
    key = _key_days_ago(10)
    assert nestvault._smart_skip_eligible(
        pending_hash=[], current_paths={"a"}, prev_paths={"a"},
        prev_done_key=key, accumulate=False, full_rescan_days=7,
    ) is False


# -- _chunked --------------------------------------------------------------

def test_chunked_splits_evenly():
    chunks = list(nestvault._chunked(list(range(10)), 3))
    assert chunks == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]


def test_chunked_empty_list():
    assert list(nestvault._chunked([], 5)) == []


# -- cache local de hash ---------------------------------------------------

def test_local_hash_cache_round_trip(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    files = {"a.txt": {"original_path": "a.txt", "sha256": "abc", "size": 10, "mtime": 1.0}}

    nestvault._save_local_hash_cache("mylabel", "2026-01-01T00:00:00", files)
    loaded = nestvault._load_local_hash_cache("mylabel")

    assert loaded["version_key"] == "2026-01-01T00:00:00"
    assert loaded["files"] == files


def test_local_hash_cache_miss_returns_empty_dict(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    assert nestvault._load_local_hash_cache("never-saved-label") == {}


def test_local_hash_cache_skips_saving_empty_snapshot(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    nestvault._save_local_hash_cache("mylabel", "2026-01-01T00:00:00", {})
    assert not nestvault._local_cache_path("mylabel").exists()


# -- diretorio de cache por SO -----------------------------------------------

def test_cache_dir_linux_uses_xdg_cache_home(monkeypatch):
    monkeypatch.setattr(nestvault.sys, "platform", "linux")
    monkeypatch.setenv("XDG_CACHE_HOME", "/xdg-cache")
    assert nestvault._local_cache_dir() == nestvault.Path("/xdg-cache/nestvault")


def test_cache_dir_linux_falls_back_to_dot_cache(monkeypatch):
    monkeypatch.setattr(nestvault.sys, "platform", "linux")
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    assert nestvault._local_cache_dir() == nestvault.Path.home() / ".cache" / "nestvault"


def test_cache_dir_macos_uses_library_caches(monkeypatch):
    monkeypatch.setattr(nestvault.sys, "platform", "darwin")
    assert nestvault._local_cache_dir() == nestvault.Path.home() / "Library" / "Caches" / "nestvault"


def test_cache_dir_windows_uses_localappdata(monkeypatch):
    monkeypatch.setattr(nestvault.sys, "platform", "win32")
    monkeypatch.setenv("LOCALAPPDATA", "C:\\Users\\test\\AppData\\Local")
    assert nestvault._local_cache_dir() == nestvault.Path("C:\\Users\\test\\AppData\\Local") / "nestvault"
