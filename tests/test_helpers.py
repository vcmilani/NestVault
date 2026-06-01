from pathlib import Path
from unittest.mock import patch
from collections import namedtuple

import pytest
from fastapi import HTTPException

import main as m
import storage

DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


def test_pick_volume_single(tmp_path):
    vol = tmp_path / "vol"
    vol.mkdir()
    with patch.object(storage, "STORAGE_VOLUMES", [vol]):
        with patch("storage.shutil.disk_usage", return_value=DiskUsage(total=1000, used=500, free=500)):
            assert m._pick_volume() == vol


def test_pick_volume_picks_first_declared(tmp_path):
    """Com dois volumes saudáveis acima do limiar, usa o primeiro declarado (maior prioridade)."""
    v1 = tmp_path / "v1"
    v2 = tmp_path / "v2"
    v1.mkdir(); v2.mkdir()

    GB = 1024 ** 3
    def fake_usage(path):
        # v1 tem menos espaço livre que v2, mas v1 é declarado primeiro → deve ser escolhido
        return DiskUsage(total=100*GB, used=80*GB, free=20*GB) if path == v1 else DiskUsage(total=100*GB, used=70*GB, free=30*GB)

    with patch.object(storage, "STORAGE_VOLUMES", [v1, v2]):
        with patch("storage.shutil.disk_usage", side_effect=fake_usage):
            assert m._pick_volume() == v1


def test_pick_volume_priority_cascade(tmp_path):
    """Quando o primeiro disco está esgotado (< threshold), usa o segundo."""
    v1 = tmp_path / "v1"
    v2 = tmp_path / "v2"
    v1.mkdir(); v2.mkdir()

    GB = 1024 ** 3
    def fake_usage(path):
        # v1 com 5 GB livre (abaixo do threshold de 10 GB) → esgotado
        # v2 com 20 GB livre (acima do threshold) → elegível
        if path == v1:
            return DiskUsage(total=100*GB, used=95*GB, free=5*GB)
        return DiskUsage(total=100*GB, used=80*GB, free=20*GB)

    with patch.object(storage, "STORAGE_VOLUMES", [v1, v2]):
        with patch("storage.shutil.disk_usage", side_effect=fake_usage):
            assert m._pick_volume() == v2


def test_pick_volume_fallback_disk_skipped_when_primary_available(tmp_path):
    """Disco de fallback (último) não é usado enquanto primários têm espaço."""
    primary = tmp_path / "primary"
    fallback = tmp_path / "fallback"
    primary.mkdir(); fallback.mkdir()

    GB = 1024 ** 3
    def fake_usage(path):
        # primary tem 20 GB livre (acima do threshold de 10 GB); fallback tem 90 GB
        # mas fallback só deve ser usado se primary esgotar
        if path == primary:
            return DiskUsage(total=100*GB, used=80*GB, free=20*GB)
        return DiskUsage(total=200*GB, used=110*GB, free=90*GB)

    with patch.object(storage, "STORAGE_VOLUMES", [primary, fallback]):
        with patch("storage.shutil.disk_usage", side_effect=fake_usage):
            assert m._pick_volume() == primary


def test_pick_volume_all_exhausted_fallback_to_most_free(tmp_path):
    """Quando todos estão esgotados, usa o com mais espaço livre (último recurso)."""
    v1 = tmp_path / "v1"
    v2 = tmp_path / "v2"
    v1.mkdir(); v2.mkdir()

    GB = 1024 ** 3
    def fake_usage(path):
        # Ambos abaixo do threshold de 10 GB (3 GB e 5 GB)
        if path == v1:
            return DiskUsage(total=100*GB, used=97*GB, free=3*GB)
        return DiskUsage(total=100*GB, used=95*GB, free=5*GB)

    with patch.object(storage, "STORAGE_VOLUMES", [v1, v2]):
        with patch("storage.shutil.disk_usage", side_effect=fake_usage):
            # v2 tem mais espaço → deve ser escolhido como último recurso
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


# -- _safe_disk_usage ---------------------------------------------------------

def test_safe_disk_usage_marks_degraded_on_error(tmp_path):
    v = tmp_path / "vol"; v.mkdir()
    with patch("storage.shutil.disk_usage", side_effect=OSError("io error")):
        result = m._safe_disk_usage(v)
    assert result is None
    assert v in m._degraded_volumes


def test_safe_disk_usage_recovers_volume(tmp_path):
    v = tmp_path / "vol"; v.mkdir()
    m._degraded_volumes.add(v)
    with patch("storage.shutil.disk_usage", return_value=DiskUsage(total=1000, used=200, free=800)):
        result = m._safe_disk_usage(v)
    assert result is not None
    assert v not in m._degraded_volumes


def test_pick_volume_skips_degraded(tmp_path):
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()
    m._degraded_volumes.add(v1)

    def fake_usage(_):
        return DiskUsage(total=1000, used=200, free=800)

    with patch.object(storage, "STORAGE_VOLUMES", [v1, v2]):
        with patch("storage.shutil.disk_usage", side_effect=fake_usage):
            result = m._pick_volume()
    assert result == v2


def test_pick_volume_raises_503_when_all_degraded(tmp_path):
    v1 = tmp_path / "v1"; v1.mkdir()
    m._degraded_volumes.add(v1)
    with patch.object(storage, "STORAGE_VOLUMES", [v1]):
        with pytest.raises(HTTPException) as exc_info:
            m._pick_volume()
    assert exc_info.value.status_code == 503
