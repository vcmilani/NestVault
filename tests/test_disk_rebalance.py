"""Testa o rebalanceamento entre discos: storage.rebalance_sources/destinations/
rebalance_disks (server/storage.py), os endpoints /maintenance/rebalance-disks*
e o agendamento automático (server/scheduler.py)."""
import base64
import hashlib
from collections import namedtuple
from pathlib import Path

import database as db_mod
import main as m
import storage as storage_mod
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])
GB = 1024 ** 3


# -- Helpers de baixo nível (sem app, direto no storage.py) --------------------

def _make_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    db_mod.Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine)()


def _seed_file(db, volume, seed: str, size_bytes: int) -> str:
    """Cria um FileContent/FileContentCopy com um arquivo físico pequeno, mas com
    `size` inflado no banco — permite testar a matemática de espaço sem gravar
    arquivos gigantes em disco."""
    content = f"content-{seed}".encode()
    sha256 = hashlib.sha256(content).hexdigest()
    path = storage_mod.content_path(sha256, volume)
    path.write_bytes(content)
    db.add(db_mod.FileContent(sha256=sha256, stored_at=str(path), size=size_bytes))
    db.add(db_mod.FileContentCopy(sha256=sha256, stored_at=str(path), volume_path=str(volume)))
    db.commit()
    return sha256


def _patch_threshold(monkeypatch, volumes, threshold_gb, disk_usage_fn):
    monkeypatch.setattr(storage_mod, "STORAGE_VOLUMES", volumes)
    monkeypatch.setattr(storage_mod, "STORAGE_FALLBACK_THRESHOLD_GB", threshold_gb)
    monkeypatch.setattr(storage_mod.shutil, "disk_usage", disk_usage_fn)
    storage_mod._degraded_volumes.clear()


# -- rebalance_sources / rebalance_destinations --------------------------------

def test_rebalance_sources_below_threshold(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    def fake_usage(path):
        if path == v1:
            return DiskUsage(total=100 * GB, used=97 * GB, free=3 * GB)
        return DiskUsage(total=100 * GB, used=20 * GB, free=80 * GB)

    _patch_threshold(monkeypatch, [v1, v2], 10.0, fake_usage)

    assert storage_mod.rebalance_sources() == [v1]
    assert storage_mod.rebalance_destinations(exclude={v1}) == [v2]


def test_rebalance_sources_empty_when_all_above_threshold(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"; v1.mkdir()

    def fake_usage(_):
        return DiskUsage(total=100 * GB, used=10 * GB, free=90 * GB)

    _patch_threshold(monkeypatch, [v1], 10.0, fake_usage)
    assert storage_mod.rebalance_sources() == []


# -- rebalance_disks — algoritmo -----------------------------------------------

def test_rebalance_moves_only_enough_to_clear_threshold(tmp_path, monkeypatch):
    """Com 3 arquivos de 0.5 GiB em v1 (0.5 GiB livres, limiar 1 GiB, meta 1.2 GiB),
    apenas os arquivos necessários para atingir a meta devem ser movidos — não os 3."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()
    db = _make_session()

    v1_free = int(0.5 * GB)

    def fake_usage(path):
        if path == v1:
            return DiskUsage(total=10 * GB, used=10 * GB - v1_free, free=v1_free)
        return DiskUsage(total=200 * GB, used=100 * GB, free=100 * GB)

    _patch_threshold(monkeypatch, [v1, v2], 1.0, fake_usage)

    file_size = int(0.5 * GB)
    for i in range(3):
        _seed_file(db, v1, f"f{i}", file_size)

    result = storage_mod.rebalance_disks(db)

    # 0.5 (livre) + 0.5 (1º arquivo) = 1.0 GiB < meta (1.2 GiB) -> continua
    # 1.0 + 0.5 (2º arquivo) = 1.5 GiB >= meta -> pára
    assert result["moved"] == 2
    assert result["sources"][0]["files_moved"] == 2

    remaining_v1 = db.query(db_mod.FileContentCopy).filter_by(volume_path=str(v1)).count()
    moved_to_v2 = db.query(db_mod.FileContentCopy).filter_by(volume_path=str(v2)).count()
    assert remaining_v1 == 1
    assert moved_to_v2 == 2

    v1_files = list((v1 / "_content").rglob("*"))
    v1_files = [f for f in v1_files if f.is_file()]
    v2_files = [f for f in (v2 / "_content").rglob("*") if f.is_file()]
    assert len(v1_files) == 1
    assert len(v2_files) == 2


def test_rebalance_fills_higher_priority_destination_first(tmp_path, monkeypatch):
    """v1 e v2 abaixo do limiar, v3 e v4 com espaço — mesmo v4 tendo MUITO mais
    espaço livre em termos absolutos, tudo deve ir para v3 primeiro (ordem de
    prioridade de storage.dirs), nunca para "quem tem mais espaço livre agora"."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()
    v3 = tmp_path / "v3"; v3.mkdir()
    v4 = tmp_path / "v4"; v4.mkdir()
    db = _make_session()

    v1_free = int(0.3 * GB)
    v2_free = int(0.3 * GB)
    v3_free = int(5 * GB)
    v4_free = int(50 * GB)  # bem mais espaço livre que v3, mas prioridade menor

    def fake_usage(path):
        if path == v1:
            return DiskUsage(total=10 * GB, used=10 * GB - v1_free, free=v1_free)
        if path == v2:
            return DiskUsage(total=10 * GB, used=10 * GB - v2_free, free=v2_free)
        if path == v3:
            return DiskUsage(total=200 * GB, used=200 * GB - v3_free, free=v3_free)
        return DiskUsage(total=200 * GB, used=200 * GB - v4_free, free=v4_free)

    _patch_threshold(monkeypatch, [v1, v2, v3, v4], 1.0, fake_usage)

    file_size = int(0.5 * GB)
    for i in range(2):
        _seed_file(db, v1, f"v1-{i}", file_size)
    for i in range(2):
        _seed_file(db, v2, f"v2-{i}", file_size)

    result = storage_mod.rebalance_disks(db)

    assert result["moved"] == 4
    assert db.query(db_mod.FileContentCopy).filter_by(volume_path=str(v3)).count() == 4
    assert db.query(db_mod.FileContentCopy).filter_by(volume_path=str(v4)).count() == 0
    assert db.query(db_mod.FileContentCopy).filter_by(volume_path=str(v1)).count() == 0
    assert db.query(db_mod.FileContentCopy).filter_by(volume_path=str(v2)).count() == 0


def test_rebalance_spills_to_next_priority_destination_once_full(tmp_path, monkeypatch):
    """v3 (maior prioridade entre os destinos) só recebe até ficar sem espaço
    acima do limiar; o que sobrar transborda para v4, nunca antes disso."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v3 = tmp_path / "v3"; v3.mkdir()
    v4 = tmp_path / "v4"; v4.mkdir()
    db = _make_session()

    v1_free = int(0.1 * GB)
    v3_free = int(1.5 * GB)   # cabe 1 arquivo de 0.6 GiB antes de cair abaixo do limiar (1 GiB)
    v4_free = int(50 * GB)

    def fake_usage(path):
        if path == v1:
            return DiskUsage(total=10 * GB, used=10 * GB - v1_free, free=v1_free)
        if path == v3:
            return DiskUsage(total=200 * GB, used=200 * GB - v3_free, free=v3_free)
        return DiskUsage(total=200 * GB, used=200 * GB - v4_free, free=v4_free)

    _patch_threshold(monkeypatch, [v1, v3, v4], 1.0, fake_usage)

    file_size = int(0.6 * GB)
    sha_a = _seed_file(db, v1, "a", file_size)
    sha_b = _seed_file(db, v1, "b", file_size)

    result = storage_mod.rebalance_disks(db)

    # 0.1 (livre) + 0.6 (1º) = 0.7 GiB < meta (1.2 GiB) -> continua
    # 0.7 + 0.6 (2º) = 1.3 GiB >= meta -> pára
    assert result["moved"] == 2

    copy_a = db.query(db_mod.FileContentCopy).filter_by(sha256=sha_a).first()
    copy_b = db.query(db_mod.FileContentCopy).filter_by(sha256=sha_b).first()
    # 1º arquivo: v3 tem 1.5 GiB >= limiar -> recebe. Depois disso v3 fica com
    # 0.9 GiB (< limiar de 1 GiB) -> 2º arquivo transborda para v4.
    assert copy_a.volume_path == str(v3)
    assert copy_b.volume_path == str(v4)


def test_rebalance_verifies_sha256_and_leaves_content_readable(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()
    db = _make_session()

    def fake_usage(path):
        if path == v1:
            return DiskUsage(total=10 * GB, used=10 * GB, free=0)
        return DiskUsage(total=200 * GB, used=100 * GB, free=100 * GB)

    _patch_threshold(monkeypatch, [v1, v2], 1.0, fake_usage)
    sha256 = _seed_file(db, v1, "only-file", int(0.2 * GB))

    storage_mod.rebalance_disks(db)

    copy = db.query(db_mod.FileContentCopy).filter_by(sha256=sha256).first()
    assert copy.volume_path == str(v2)
    assert storage_mod.file_sha256(Path(copy.stored_at)) == sha256

    fc = db.get(db_mod.FileContent, sha256)
    assert fc.stored_at == copy.stored_at


def test_rebalance_dry_run_does_not_modify_anything(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()
    db = _make_session()

    def fake_usage(path):
        if path == v1:
            return DiskUsage(total=10 * GB, used=10 * GB, free=0)
        return DiskUsage(total=200 * GB, used=100 * GB, free=100 * GB)

    _patch_threshold(monkeypatch, [v1, v2], 1.0, fake_usage)
    sha256 = _seed_file(db, v1, "only-file", int(0.2 * GB))

    result = storage_mod.rebalance_disks(db, dry_run=True)

    assert result["moved"] == 1
    assert db.query(db_mod.FileContentCopy).filter_by(volume_path=str(v1)).count() == 1
    assert db.query(db_mod.FileContentCopy).filter_by(volume_path=str(v2)).count() == 0
    copy = db.query(db_mod.FileContentCopy).filter_by(sha256=sha256).first()
    assert Path(copy.stored_at).exists()


def test_rebalance_no_sources_returns_empty_result(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"; v1.mkdir()
    db = _make_session()

    def fake_usage(_):
        return DiskUsage(total=100 * GB, used=10 * GB, free=90 * GB)

    _patch_threshold(monkeypatch, [v1], 10.0, fake_usage)
    result = storage_mod.rebalance_disks(db)
    assert result == {"moved": 0, "bytes_moved": 0, "skipped": 0, "sources": [], "destinations": []}


# -- Endpoints (via TestClient) -------------------------------------------------

def _seed_users(Session):
    db = Session()
    try:
        db.add(db_mod.User(username="admin", api_key_hash=db_mod.hash_api_key("testkey"),
                            role="admin", is_active=True))
        db.add(db_mod.User(username="bob", api_key_hash=db_mod.hash_api_key("bobkey"),
                            role="user", is_active=True))
        db.commit()
    finally:
        db.close()


def _client_ctx(monkeypatch, volumes, disk_usage_fn, threshold_gb=10.0):
    from fastapi.testclient import TestClient

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    db_mod.Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    _seed_users(Session)

    monkeypatch.setattr(storage_mod, "STORAGE_VOLUMES", volumes)
    monkeypatch.setattr(storage_mod, "STORAGE_DIR", volumes[0])
    monkeypatch.setattr(storage_mod, "STORAGE_FALLBACK_THRESHOLD_GB", threshold_gb)
    monkeypatch.setattr(storage_mod.shutil, "disk_usage", disk_usage_fn)
    monkeypatch.setattr(m.shutil, "disk_usage", disk_usage_fn)
    monkeypatch.setattr(m, "SessionLocal", Session)
    storage_mod._degraded_volumes.clear()

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    m.app.dependency_overrides[db_mod.get_db] = override_get_db
    with TestClient(m.app) as c:
        yield c
    m.app.dependency_overrides.clear()


def test_preview_reports_nothing_to_do_when_all_volumes_healthy(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    def fake_usage(_):
        return DiskUsage(total=100 * GB, used=10 * GB, free=90 * GB)

    for c in _client_ctx(monkeypatch, [v1, v2], fake_usage):
        c.headers.update({"X-API-Key": "testkey"})
        r = c.get("/maintenance/rebalance-disks/preview")
        assert r.status_code == 200
        data = r.json()
        assert data["can_proceed"] is False
        assert data["files_to_move"] == 0
        assert data["sources"] == []


def test_manual_trigger_requires_admin(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"; v1.mkdir()

    def fake_usage(_):
        return DiskUsage(total=100 * GB, used=10 * GB, free=90 * GB)

    for c in _client_ctx(monkeypatch, [v1], fake_usage):
        c.headers.update({"X-API-Key": "bobkey"})
        r = c.post("/maintenance/rebalance-disks")
        assert r.status_code == 403


def test_manual_trigger_400_when_nothing_to_rebalance(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"; v1.mkdir()

    def fake_usage(_):
        return DiskUsage(total=100 * GB, used=10 * GB, free=90 * GB)

    for c in _client_ctx(monkeypatch, [v1], fake_usage):
        c.headers.update({"X-API-Key": "testkey"})
        r = c.post("/maintenance/rebalance-disks")
        assert r.status_code == 400


def test_manual_trigger_moves_files_and_completes_job(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    v1_free = int(0.5 * GB)

    def fake_usage(path):
        if path == v1:
            return DiskUsage(total=10 * GB, used=10 * GB - v1_free, free=v1_free)
        return DiskUsage(total=200 * GB, used=100 * GB, free=100 * GB)

    for c in _client_ctx(monkeypatch, [v1, v2], fake_usage, threshold_gb=1.0):
        c.headers.update({"X-API-Key": "testkey"})
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})

        # Simula um arquivo já existente em v1 mesmo com o disco abaixo do limiar
        # (ex.: gravado antes do limiar ter sido reduzido).
        monkeypatch.setattr(m, "_pick_volume", lambda: v1)
        path_b64 = base64.b64encode(b"/a.txt").decode()
        r = c.post("/upload", content=b"conteudo do arquivo a", headers={
            "X-Backup-Label": "b1", "X-Version-Key": "v1",
            "X-Original-Path": path_b64, "X-Mtime": "1.0",
        })
        assert r.status_code == 200

        preview = c.get("/maintenance/rebalance-disks/preview").json()
        assert preview["can_proceed"] is True
        assert preview["files_to_move"] == 1

        r = c.post("/maintenance/rebalance-disks")
        assert r.status_code == 200
        job_id = r.json()["job_id"]

        # TestClient executa BackgroundTasks de forma síncrona antes do response
        # retornar — o job já deve estar concluído aqui.
        db2 = m.SessionLocal()
        try:
            job = db2.get(db_mod.MaintenanceJob, job_id)
            assert job.status == "done"
            assert db2.query(db_mod.FileContentCopy).filter_by(volume_path=str(v2)).count() == 1
            assert db2.query(db_mod.FileContentCopy).filter_by(volume_path=str(v1)).count() == 0
        finally:
            db2.close()


def test_manual_trigger_409_when_already_running(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"; v1.mkdir()

    def fake_usage(_):
        return DiskUsage(total=10 * GB, used=9 * GB, free=1 * GB)

    for c in _client_ctx(monkeypatch, [v1], fake_usage, threshold_gb=5.0):
        c.headers.update({"X-API-Key": "testkey"})
        db2 = m.SessionLocal()
        try:
            db2.add(db_mod.MaintenanceJob(job_type="disk-rebalance", status="running", summary="em andamento"))
            db2.commit()
        finally:
            db2.close()

        r = c.post("/maintenance/rebalance-disks")
        assert r.status_code == 409


# -- Agendamento automático (scheduler.py) --------------------------------------

def test_schedule_disk_rebalance_check_toggles_job(tmp_path, monkeypatch):
    import config as cfg
    import scheduler as sched_mod

    monkeypatch.setenv("NESTVAULT_CONFIG", str(tmp_path / "config.json"))
    saved = (cfg._data, cfg._path, cfg._boot_snapshot)
    try:
        cfg.load(tmp_path / "config.json")

        cfg.save({"storage": {"auto_rebalance_enabled": True, "rebalance_check_interval_minutes": 20}})
        sched_mod.schedule_disk_rebalance_check()
        job = sched_mod.scheduler.get_job("disk_rebalance_check")
        assert job is not None

        cfg.save({"storage": {"auto_rebalance_enabled": False}})
        sched_mod.schedule_disk_rebalance_check()
        assert sched_mod.scheduler.get_job("disk_rebalance_check") is None
    finally:
        cfg._data, cfg._path, cfg._boot_snapshot = saved
        if sched_mod.scheduler.get_job("disk_rebalance_check"):
            sched_mod.scheduler.remove_job("disk_rebalance_check")
