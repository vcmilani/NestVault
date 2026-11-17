"""Migração de disco (_bg_migrate_disk): nada sai da origem sem uma cópia
conferida no destino, arquivos que chegam durante a fase 1 não são apagados,
e conteúdo cifrado migra (antes, o hash direto do ciphertext nunca batia)."""
import hashlib
import os
from collections import namedtuple
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

import crypto
import database as db_mod
import main as m
import storage as storage_mod

_DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


@pytest.fixture
def env(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 't.db'}",
                           connect_args={"check_same_thread": False}, poolclass=NullPool)

    @event.listens_for(engine, "connect")
    def _fk(conn, _):
        conn.execute("PRAGMA foreign_keys=ON")

    db_mod.Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    v1, v2 = tmp_path / "v1", tmp_path / "v2"
    v1.mkdir(); v2.mkdir()
    monkeypatch.setattr(storage_mod, "STORAGE_VOLUMES", [v1, v2])
    monkeypatch.setattr(storage_mod, "STORAGE_DIR", v1)
    monkeypatch.setattr(storage_mod, "REPLICATION_FACTOR", 1)
    monkeypatch.setattr(storage_mod, "ENCRYPTION_ENABLED", False, raising=False)
    monkeypatch.setattr(storage_mod, "encryption_key", None)
    monkeypatch.setattr(storage_mod.shutil, "disk_usage",
                        lambda p: _DiskUsage(200 * 1024 ** 3, 100 * 1024 ** 3, 100 * 1024 ** 3))
    monkeypatch.setattr(m.shutil, "disk_usage",
                        lambda p: _DiskUsage(200 * 1024 ** 3, 100 * 1024 ** 3, 100 * 1024 ** 3))
    monkeypatch.setattr(m, "SessionLocal", Session)
    storage_mod._degraded_volumes.clear()
    storage_mod.EXCLUDED_VOLUMES.clear()
    yield Session, v1, v2
    storage_mod._degraded_volumes.clear()
    storage_mod.EXCLUDED_VOLUMES.clear()


def _sha(b):
    return hashlib.sha256(b).hexdigest()


def _seed_plain(Session, vol, data):
    sha = _sha(data)
    p = storage_mod.content_path(sha, vol)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    db = Session()
    db.add(db_mod.FileContent(sha256=sha, stored_at=str(p), size=len(data)))
    db.add(db_mod.FileContentCopy(sha256=sha, stored_at=str(p), volume_path=str(vol)))
    db.commit(); db.close()
    return sha, p


def _seed_encrypted(Session, vol, data, key, tmp_path):
    sha = _sha(data)
    plain = tmp_path / f"plain-{sha[:8]}"
    plain.write_bytes(data)
    p = storage_mod.content_path(sha, vol)
    p.parent.mkdir(parents=True, exist_ok=True)
    crypto.encrypt_stream(plain, p, key)
    db = Session()
    db.add(db_mod.FileContent(sha256=sha, stored_at=str(p), size=len(data), encrypted=True))
    db.add(db_mod.FileContentCopy(sha256=sha, stored_at=str(p), volume_path=str(vol)))
    db.commit(); db.close()
    return sha, p


def _run_migration(Session, source, destinations):
    db = Session()
    mj = db_mod.MaintenanceJob(job_type="disk-migration", status="running", summary="")
    db.add(mj); db.commit(); job_id = mj.id; db.close()
    m._bg_migrate_disk(str(source), [str(d) for d in destinations], job_id)
    db = Session()
    try:
        return db.get(db_mod.MaintenanceJob, job_id)
    finally:
        db.close()


def _copies(Session, sha):
    db = Session()
    try:
        return {c.volume_path: c.stored_at for c in db.query(db_mod.FileContentCopy).filter_by(sha256=sha)}
    finally:
        db.close()


def test_migration_moves_everything_and_keeps_content_readable(env):
    Session, v1, v2 = env
    seeded = [_seed_plain(Session, v1, f"file-{i}".encode()) for i in range(3)]

    mj = _run_migration(Session, v1, [v2])

    assert mj.status == "done"
    for sha, old_path in seeded:
        copies = _copies(Session, sha)
        assert list(copies) == [str(v2)]
        assert storage_mod.file_sha256(Path(copies[str(v2)])) == sha
        assert not old_path.exists()
        db = Session()
        assert db.get(db_mod.FileContent, sha).stored_at == copies[str(v2)]
        db.close()
    assert str(v1) not in storage_mod.EXCLUDED_VOLUMES


def test_migration_keeps_files_written_to_source_during_phase_1(env, monkeypatch):
    """Um arquivo que chega à origem durante a fase 1 (upload, réplica, move
    SSD → HDD) não pode sair na fase 2 sem ter sido copiado — era a única cópia.
    E, enquanto o job roda, a origem está fechada para escrita (EXCLUDED_VOLUMES)."""
    Session, v1, v2 = env
    sha_a, _ = _seed_plain(Session, v1, b"antes-a")
    sha_b, _ = _seed_plain(Session, v1, b"antes-b")
    real_copy = storage_mod.copy_verified
    late: dict = {}

    def copy_then_sneak_a_file(*args, **kwargs):
        assert str(v1) in storage_mod.EXCLUDED_VOLUMES
        assert storage_mod.pick_volume() == v2  # escrita nova vai para fora da origem
        if not late:
            late["sha"], late["path"] = _seed_plain(Session, v1, b"chegou-durante-a-migracao")
        return real_copy(*args, **kwargs)

    monkeypatch.setattr(storage_mod, "copy_verified", copy_then_sneak_a_file)

    mj = _run_migration(Session, v1, [v2])

    assert mj.status == "done"
    # O arquivo tardio pode ter sido alcançado pela paginação da fase 1 (e então
    # migrado) ou não (e então fica na origem) — o que não pode é sumir: tem de
    # existir uma cópia registrada e íntegra em algum volume.
    late_copies = _copies(Session, late["sha"])
    assert late_copies, "arquivo que chegou durante a fase 1 foi apagado sem cópia"
    for stored_at in late_copies.values():
        assert storage_mod.file_sha256(Path(stored_at)) == late["sha"]
    for sha in (sha_a, sha_b):
        assert list(_copies(Session, sha)) == [str(v2)]


def test_migration_does_not_trust_corrupt_destination_row(env):
    """Linha de cópia no destino com arquivo truncado: a origem só sai depois
    de o destino ser recopiado e conferido."""
    Session, v1, v2 = env
    sha, src = _seed_plain(Session, v1, b"conteudo-integro")
    bad = storage_mod.content_path(sha, v2)
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_bytes(b"lixo")
    db = Session()
    db.add(db_mod.FileContentCopy(sha256=sha, stored_at=str(bad), volume_path=str(v2)))
    db.commit(); db.close()

    mj = _run_migration(Session, v1, [v2])

    assert mj.status == "done"
    assert storage_mod.file_sha256(bad) == sha, "destino corrompido tinha que ser recopiado"
    assert not src.exists()
    assert list(_copies(Session, sha)) == [str(v2)]


def test_migration_skips_when_destination_copy_fails_and_keeps_source(env, monkeypatch):
    Session, v1, v2 = env
    sha, src = _seed_plain(Session, v1, b"nao-vai-copiar")

    def boom(*a, **k):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(storage_mod, "copy_verified", boom)
    mj = _run_migration(Session, v1, [v2])

    assert mj.status == "done"
    assert src.exists()
    assert _copies(Session, sha) == {str(v1): str(src)}
    assert "1 pulado(s)" in mj.summary


def test_migration_handles_encrypted_content(env, monkeypatch, tmp_path):
    """Com criptografia, o hash direto do arquivo cifrado nunca bate com o sha256
    — toda migração pulava todos os arquivos. copy_verified entende o formato."""
    Session, v1, v2 = env
    key = os.urandom(32)
    monkeypatch.setattr(storage_mod, "ENCRYPTION_ENABLED", True, raising=False)
    monkeypatch.setattr(storage_mod, "encryption_key", key)
    sha, src = _seed_encrypted(Session, v1, b"segredo" * 1000, key, tmp_path)

    mj = _run_migration(Session, v1, [v2])

    assert mj.status == "done"
    copies = _copies(Session, sha)
    assert list(copies) == [str(v2)]
    assert storage_mod.content_matches(Path(copies[str(v2)]), sha, True, key)
    assert not src.exists()


def test_pick_volume_skips_excluded_volume(env, monkeypatch):
    Session, v1, v2 = env
    monkeypatch.setattr(storage_mod, "EXCLUDED_VOLUMES", {str(v1)})
    assert storage_mod.pick_volume() == v2
    assert storage_mod.pick_volume_for_size(10) == v2
    assert storage_mod.writable_volumes() == [v2]
    assert storage_mod.healthy_volumes() == [v1, v2]  # leitura continua enxergando a origem
