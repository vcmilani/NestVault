"""Stats — estimativa de espaço para ligar/subir a replicação."""
from collections import namedtuple
from datetime import datetime

import pytest

import crypto
import database as db_mod
import main as m
import storage as storage_mod

GB = 1024 ** 3
_U = namedtuple("U", ["total", "used", "free"])


@pytest.fixture
def vols(client, tmp_path, monkeypatch):
    """Três volumes falsos com espaço livre controlado por `free[path]`."""
    vs = [tmp_path / f"hdd{i}" for i in (1, 2, 3)]
    for v in vs:
        v.mkdir()
    free = {v: 100 * GB for v in vs}
    monkeypatch.setattr(storage_mod, "STORAGE_VOLUMES", vs)
    monkeypatch.setattr(storage_mod, "REPLICATION_FACTOR", 1)
    monkeypatch.setattr(storage_mod, "STORAGE_FALLBACK_THRESHOLD_GB", 10.0)
    monkeypatch.setattr(storage_mod, "safe_disk_usage",
                        lambda v: _U(200 * GB, 200 * GB - free[v], free[v]) if v in free else None)
    return vs, free


def _content(sha, size, on, encrypted=False, quarantined=False):
    db = m.SessionLocal()
    try:
        db.add(db_mod.FileContent(sha256=sha, stored_at=str(on[0] / sha), size=size,
                                  encrypted=encrypted,
                                  quarantined_at=datetime.now() if quarantined else None))
        for v in on:
            db.add(db_mod.FileContentCopy(sha256=sha, stored_at=str(v / sha), volume_path=str(v)))
        db.commit()
    finally:
        db.close()


def _est(client):
    r = client.get("/api/stats/replication-estimate", params={"refresh": True})
    assert r.status_code == 200, r.text
    d = r.json()
    return d, {o["factor"]: o for o in d["options"]}


def test_factor_2_copies_to_next_volume_in_order(client, vols):
    vs, _ = vols
    _content("a" * 64, 5 * GB, [vs[0]])
    _content("b" * 64, 3 * GB, [vs[1]])
    _content("c" * 64, 1 * GB, [vs[0], vs[1]])     # já tem 2 cópias
    d, o = _est(client)
    assert d["current_factor"] == 1 and d["contents"] == 3 and d["healthy_count"] == 3
    two = o[2]
    assert two["extra_bytes"] == 8 * GB and two["files_to_copy"] == 2 and two["feasible"]
    recv = {p["path"]: p["receives"] for p in two["per_volume"]}
    # Mesma política do ensure_replicas: primeiro volume saudável que não tem o conteúdo
    assert recv == {str(vs[0]): 3 * GB, str(vs[1]): 5 * GB, str(vs[2]): 0}
    # fator 3: a e b precisam de mais 2 cópias cada; c, de mais 1
    assert o[3]["extra_bytes"] == 2 * 5 * GB + 2 * 3 * GB + 1 * GB
    # fator 1: o conteúdo com 2 cópias liberaria uma
    assert o[1]["extra_bytes"] == 0 and o[1]["freed_bytes"] == 1 * GB


def test_many_contents_in_same_group_sum_once(client, vols):
    vs, _ = vols
    for i, sz in enumerate((1, 2, 3)):
        _content(str(i) * 64, sz * GB, [vs[0]])
    _, o = _est(client)
    assert o[2]["extra_bytes"] == 6 * GB and o[2]["files_to_copy"] == 3
    assert o[3]["extra_bytes"] == 12 * GB


def test_not_enough_space_reports_shortfall(client, vols):
    vs, free = vols
    free[vs[1]] = 12 * GB                            # usável = 12 − 10 (piso) = 2 GB
    _content("a" * 64, 5 * GB, [vs[0]])
    _, o = _est(client)
    two = o[2]
    assert not two["feasible"] and two["reason"] == "falta espaço"
    assert two["shortfall_bytes"] == 3 * GB
    pv = {p["path"]: p for p in two["per_volume"]}
    assert pv[str(vs[1])]["shortfall"] == 3 * GB and pv[str(vs[1])]["free_after"] == 7 * GB


def test_degraded_volume_means_missing_disks(client, vols):
    vs, free = vols
    del free[vs[2]]                                  # safe_disk_usage → None
    _content("a" * 64, 1 * GB, [vs[0]])
    d, o = _est(client)
    assert d["healthy_count"] == 2
    assert o[2]["feasible"]
    assert not o[3]["feasible"] and o[3]["reason"] == "faltam discos"


def test_encrypted_overhead_and_quarantine(client, vols):
    vs, _ = vols
    size = 3 * crypto.CHUNK_SIZE + 1                 # 4 chunks
    _content("e" * 64, size, [vs[0]], encrypted=True)
    _content("q" * 64, 7 * GB, [vs[0]], quarantined=True)
    _, o = _est(client)
    assert o[2]["extra_bytes"] == size + crypto.NONCE_SIZE + 4 * 20


def test_content_without_healthy_copy_is_unreplicable(client, vols):
    vs, free = vols
    _content("a" * 64, 2 * GB, [vs[2]])
    del free[vs[2]]
    d, o = _est(client)
    assert d["unreplicable_contents"] == 1 and d["unreplicable_bytes"] == 2 * GB
    assert o[2]["extra_bytes"] == 0


def test_requires_admin(two_users):
    _admin, alice, _bob = two_users
    assert alice.get("/api/stats/replication-estimate").status_code == 403
