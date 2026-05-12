from pathlib import Path
from unittest.mock import patch
from collections import namedtuple

import pytest
from fastapi import HTTPException

import main as m

DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


def test_pick_volume_single(tmp_path):
    vol = tmp_path / "vol"
    vol.mkdir()
    with patch.object(m, "STORAGE_VOLUMES", [vol]):
        assert m._pick_volume() == vol


def test_pick_volume_picks_most_free(tmp_path):
    v1 = tmp_path / "v1"
    v2 = tmp_path / "v2"
    v1.mkdir(); v2.mkdir()

    def fake_usage(path):
        return DiskUsage(total=1000, used=900, free=100) if path == v1 else DiskUsage(total=1000, used=200, free=800)

    with patch.object(m, "STORAGE_VOLUMES", [v1, v2]):
        with patch("main.shutil.disk_usage", side_effect=fake_usage):
            assert m._pick_volume() == v2


def test_content_path_structure(tmp_path):
    vol = tmp_path / "vol"
    vol.mkdir()
    sha = "a" * 64
    p = m._content_path(sha, vol)
    assert p == vol / "_content" / ("a" * 2) / ("a" * 64)
    assert p.parent.exists()


def test_content_path_different_prefix(tmp_path):
    vol = tmp_path / "vol"
    vol.mkdir()
    sha = "abcdef" + "0" * 58
    p = m._content_path(sha, vol)
    assert p.parts[-2] == "ab"


def test_min_disk_free_percent_single(tmp_path):
    vol = tmp_path / "vol"
    vol.mkdir()
    with patch.object(m, "STORAGE_VOLUMES", [vol]):
        with patch("main.shutil.disk_usage", return_value=DiskUsage(total=1000, used=500, free=500)):
            pct = m._min_disk_free_percent()
    assert abs(pct - 50.0) < 0.01


def test_min_disk_free_percent_returns_minimum(tmp_path):
    v1 = tmp_path / "v1"
    v2 = tmp_path / "v2"
    v1.mkdir(); v2.mkdir()

    def fake_usage(path):
        return DiskUsage(total=1000, used=100, free=900) if path == v1 else DiskUsage(total=1000, used=950, free=50)

    with patch.object(m, "STORAGE_VOLUMES", [v1, v2]):
        with patch("main.shutil.disk_usage", side_effect=fake_usage):
            pct = m._min_disk_free_percent()
    assert abs(pct - 5.0) < 0.01


# -- _safe_disk_usage ---------------------------------------------------------

def test_safe_disk_usage_marks_degraded_on_error(tmp_path):
    v = tmp_path / "vol"; v.mkdir()
    with patch("main.shutil.disk_usage", side_effect=OSError("io error")):
        result = m._safe_disk_usage(v)
    assert result is None
    assert v in m._degraded_volumes


def test_safe_disk_usage_recovers_volume(tmp_path):
    v = tmp_path / "vol"; v.mkdir()
    m._degraded_volumes.add(v)
    with patch("main.shutil.disk_usage", return_value=DiskUsage(total=1000, used=200, free=800)):
        result = m._safe_disk_usage(v)
    assert result is not None
    assert v not in m._degraded_volumes


def test_pick_volume_skips_degraded(tmp_path):
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()
    m._degraded_volumes.add(v1)

    def fake_usage(_):
        return DiskUsage(total=1000, used=200, free=800)

    with patch.object(m, "STORAGE_VOLUMES", [v1, v2]):
        with patch("main.shutil.disk_usage", side_effect=fake_usage):
            result = m._pick_volume()
    assert result == v2


def test_pick_volume_raises_503_when_all_degraded(tmp_path):
    v1 = tmp_path / "v1"; v1.mkdir()
    m._degraded_volumes.add(v1)
    with patch.object(m, "STORAGE_VOLUMES", [v1]):
        with pytest.raises(HTTPException) as exc_info:
            m._pick_volume()
    assert exc_info.value.status_code == 503


def test_min_disk_free_percent_returns_100_when_all_degraded(tmp_path):
    v = tmp_path / "vol"; v.mkdir()
    m._degraded_volumes.add(v)
    with patch.object(m, "STORAGE_VOLUMES", [v]):
        pct = m._min_disk_free_percent()
    assert pct == 100.0


def test_min_disk_free_percent_ignores_degraded(tmp_path):
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()
    m._degraded_volumes.add(v2)

    def fake_usage(_):
        return DiskUsage(total=1000, used=100, free=900)

    with patch.object(m, "STORAGE_VOLUMES", [v1, v2]):
        with patch("main.shutil.disk_usage", side_effect=fake_usage):
            pct = m._min_disk_free_percent()
    assert abs(pct - 90.0) < 0.01
