"""Gravações conferidas: réplicas (storage.ensure_replicas / rereplicate_to_volume)
e ingestão do rclone (_process_file_sync) — atômicas, verificadas e tolerantes a
registro concorrente do mesmo sha256."""
import hashlib
import os
from collections import namedtuple

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

import crypto
import database as db_mod
import storage as storage_mod
import cloud.rclone_runner as rr
from cloud.rclone_runner import RcloneFileEntry

_DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Banco em arquivo com FKs ligadas (como em produção) e dois volumes."""
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
    monkeypatch.setattr(storage_mod, "REPLICATION_FACTOR", 2)
    monkeypatch.setattr(storage_mod, "encryption_key", None)
    monkeypatch.setattr(storage_mod.shutil, "disk_usage",
                        lambda p: _DiskUsage(200 * 1024 ** 3, 100 * 1024 ** 3, 100 * 1024 ** 3))
    monkeypatch.setattr(rr, "SessionLocal", Session)
    storage_mod._degraded_volumes.clear()
    yield Session, v1, v2
    storage_mod._degraded_volumes.clear()


def _sha(b):
    return hashlib.sha256(b).hexdigest()


def _seed_plain(Session, vol, data):
    sha = _sha(data)
    p = storage_mod.content_path(sha, vol)
    p.write_bytes(data)
    db = Session()
    db.add(db_mod.FileContent(sha256=sha, stored_at=str(p), size=len(data)))
    db.add(db_mod.FileContentCopy(sha256=sha, stored_at=str(p), volume_path=str(vol)))
    db.commit(); db.close()
    return sha, p


def _copies(Session, sha):
    db = Session()
    try:
        return sorted(c.volume_path for c in db.query(db_mod.FileContentCopy).filter_by(sha256=sha))
    finally:
        db.close()


def _stray_files(vol):
    return [p for p in (vol / "_content").rglob("*") if p.is_file()] if (vol / "_content").exists() else []


# -- 10: réplicas --------------------------------------------------------------

def test_replica_is_created_and_verified(env):
    Session, v1, v2 = env
    sha, src = _seed_plain(Session, v1, b"precious")
    db = Session()
    storage_mod.ensure_replicas(sha, src, db); db.commit(); db.close()
    assert storage_mod.content_path(sha, v2).read_bytes() == b"precious"
    assert _copies(Session, sha) == sorted([str(v1), str(v2)])


def test_corrupted_source_is_not_propagated(env):
    """Regressão: copy2 direto espalhava uma origem corrompida para as réplicas."""
    Session, v1, v2 = env
    sha, src = _seed_plain(Session, v1, b"precious")
    src.write_bytes(b"PRECIOUS")  # mesmo tamanho, conteúdo errado
    db = Session()
    storage_mod.ensure_replicas(sha, src, db); db.commit(); db.close()
    assert _stray_files(v2) == []
    assert _copies(Session, sha) == [str(v1)]


def test_encrypted_source_with_wrong_size_is_not_propagated(env, monkeypatch):
    Session, v1, v2 = env
    key = os.urandom(32)
    data = b"precious"
    sha = _sha(data)
    plain = v1 / "plain"; plain.write_bytes(data)
    src = storage_mod.content_path(sha, v1)
    crypto.encrypt_stream(plain, src, key)
    with open(src, "ab") as f:
        f.write(b"lixo")
    db = Session()
    db.add(db_mod.FileContent(sha256=sha, stored_at=str(src), size=len(data), encrypted=True))
    db.add(db_mod.FileContentCopy(sha256=sha, stored_at=str(src), volume_path=str(v1)))
    db.commit()
    storage_mod.ensure_replicas(sha, src, db); db.commit(); db.close()
    assert _stray_files(v2) == []


def test_failed_copy_leaves_no_partial_file(env, monkeypatch):
    """Regressão: ENOSPC no meio do copy2 deixava um arquivo parcial no caminho final."""
    Session, v1, v2 = env
    sha, src = _seed_plain(Session, v1, b"precious")

    def half_copy_then_enospc(s, d):
        with open(d, "wb") as f:
            f.write(b"prec")
        raise OSError(28, "No space left on device")
    monkeypatch.setattr(storage_mod, "_copy_with_sha256", half_copy_then_enospc)

    db = Session()
    storage_mod.ensure_replicas(sha, src, db); db.commit(); db.close()
    assert _stray_files(v2) == [], "nem o destino final nem o temporário podem sobrar"
    assert _copies(Session, sha) == [str(v1)]


def test_concurrent_registration_of_same_replica_does_not_fail(env):
    """Duas sessões registrando a mesma cópia: antes o IntegrityError derrubava a
    transação de quem chegava depois (em /register/batch, o lote inteiro)."""
    Session, v1, v2 = env
    sha, _ = _seed_plain(Session, v1, b"precious")
    dest = str(storage_mod.content_path(sha, v2))
    first, second = Session(), Session()
    storage_mod.insert_copy_row(first, sha, dest, str(v2)); first.commit()
    storage_mod.insert_copy_row(second, sha, dest, str(v2)); second.commit()
    first.close(); second.close()
    assert _copies(Session, sha) == sorted([str(v1), str(v2)])


def test_rereplicate_to_volume_skips_corrupted_source(env, monkeypatch):
    Session, v1, v2 = env
    monkeypatch.setattr(storage_mod, "SessionLocal", Session, raising=False)
    monkeypatch.setattr(db_mod, "SessionLocal", Session)
    bad_sha, bad_src = _seed_plain(Session, v1, b"precious")
    bad_src.write_bytes(b"PRECIOUS")
    good_sha, _ = _seed_plain(Session, v1, b"fine")

    storage_mod.rereplicate_to_volume(v2)

    assert _copies(Session, bad_sha) == [str(v1)]
    assert _copies(Session, good_sha) == sorted([str(v1), str(v2)])
    assert not storage_mod.content_path(bad_sha, v2).exists()


# -- 11: ingestão do rclone ---------------------------------------------------

def _version(Session):
    db = Session()
    db.add(db_mod.BackupID(label="drive"))
    v = db_mod.BackupVersion(backup_label="drive", version_key="k")
    db.add(v); db.commit()
    vid = v.id
    db.close()
    return vid


def _downloaded(tmp_path, data, name="dl"):
    p = tmp_path / name
    p.write_bytes(data)
    return p


def _entry(path="/a.txt", size=8):
    return RcloneFileEntry(path=path, size=size, mtime=1.0)


def test_rclone_dedup_repairs_missing_content(env, tmp_path):
    """Regressão: a dedup do rclone registrava o VersionFile sem conferir que o
    conteúdo existia em disco — um backup "done" apontando para o nada."""
    Session, v1, v2 = env
    sha, src = _seed_plain(Session, v1, b"precious")
    src.unlink()
    vid = _version(Session)

    rr._process_file_sync(vid, _entry(), _downloaded(tmp_path, b"precious"),
                          sha, 8, v1, None)

    assert src.read_bytes() == b"precious"
    assert storage_mod.content_path(sha, v2).read_bytes() == b"precious"  # e replicou


def test_rclone_new_encrypted_content_is_verified(env, tmp_path, monkeypatch):
    """Regressão: sem verificação depois de cifrar, um arquivo que não decifra
    era registrado como íntegro."""
    Session, v1, _ = env
    key = os.urandom(32)
    monkeypatch.setattr(storage_mod, "encryption_key", key)
    vid = _version(Session)
    data = b"precious"
    sha = _sha(data)

    def broken_encrypt(src, dst, k):
        dst.write_bytes(b"\x00" * storage_mod.expected_stored_size(len(data), True))
    monkeypatch.setattr(crypto, "encrypt_stream", broken_encrypt)

    with pytest.raises(storage_mod.ContentVerificationFailed):
        rr._process_file_sync(vid, _entry(), _downloaded(tmp_path, data), sha, 8, v1, key)

    db = Session()
    assert db.get(db_mod.FileContent, sha) is None
    db.close()
    assert _stray_files(v1) == []


def test_rclone_new_encrypted_content_roundtrip(env, tmp_path, monkeypatch):
    Session, v1, _ = env
    key = os.urandom(32)
    monkeypatch.setattr(storage_mod, "encryption_key", key)
    vid = _version(Session)
    data = b"precious"
    sha = _sha(data)

    rr._process_file_sync(vid, _entry(), _downloaded(tmp_path, data), sha, 8, v1, key)

    stored = storage_mod.content_path(sha, v1)
    assert b"".join(crypto.decrypt_chunks(stored, key)) == data


def test_rclone_concurrent_registration_keeps_winner_file(env, tmp_path):
    """Outro processo registrou o mesmo sha256 no mesmo volume entre a checagem e
    o INSERT: antes o IntegrityError abortava o arquivo; agora usa o existente."""
    Session, v1, _ = env
    data = b"precious"
    sha = _sha(data)
    vid = _version(Session)
    db = Session()
    rr._store_new_content_sync(db, sha, 8, _downloaded(tmp_path, data, "a"), v1, None)
    db.close()

    db = Session()
    assert rr._store_new_content_sync(db, sha, 8, _downloaded(tmp_path, data, "b"), v1, None) is None
    db.close()
    assert storage_mod.content_path(sha, v1).read_bytes() == data
    rr._register_version_file_sync(vid, _entry(), sha)
    db = Session()
    assert db.query(db_mod.VersionFile).filter_by(version_id=vid).count() == 1
    db.close()


def test_rclone_concurrent_registration_on_other_volume_removes_orphan(env, tmp_path):
    Session, v1, v2 = env
    sha, _ = _seed_plain(Session, v2, b"precious")  # vencedor gravou em v2
    db = Session()
    assert rr._store_new_content_sync(db, sha, 8, _downloaded(tmp_path, b"precious"), v1, None) is None
    db.close()
    assert not storage_mod.content_path(sha, v1).exists(), "cópia sem linha no banco é lixo permanente"
