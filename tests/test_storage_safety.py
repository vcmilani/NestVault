"""Travas contra perda de dados no storage: publicação exclusiva do conteúdo
(dois uploads do mesmo sha256 com criptografia), rebalance que só apaga depois
do commit, orphan cleanup que reivindica o arquivo antes do commit, backfill
tolerante a conflito, volume que volta de degraded e move SSD → HDD sem apagar
réplica registrada."""
import errno
import hashlib
import os
import threading
from pathlib import Path

import pytest

import crypto
import database as db_mod
import main as m
import nightly_cleanup
import storage as storage_mod
from conftest import make_backup, make_version, upload_file
from test_cleanup_by_date import _label_with_two_done_versions
from test_disk_rebalance import DiskUsage, GB, _make_session, _patch_threshold, _seed_file
from database import FileContent, FileContentCopy, VersionFile


def _sha(b):
    return hashlib.sha256(b).hexdigest()


# -- publish_content -----------------------------------------------------------

def test_publish_content_is_exclusive(tmp_path):
    dest = tmp_path / "_content" / "ab" / "abcd"
    a = tmp_path / "a"; a.write_bytes(b"primeiro")
    b = tmp_path / "b"; b.write_bytes(b"segundo")

    assert storage_mod.publish_content(a, dest) is True
    assert storage_mod.publish_content(b, dest) is False

    assert dest.read_bytes() == b"primeiro"
    assert not a.exists()      # publicado: o temporário foi consumido
    assert b.exists()          # perdedor: o caller decide o que fazer com o dele


# -- dois uploads simultâneos do mesmo sha256 novo, com criptografia -----------

def test_concurrent_uploads_of_same_new_content_with_encryption(client, tmp_vol, monkeypatch):
    """Antes: o plaintext ia direto para o caminho final e era cifrado no lugar.
    O segundo upload cifrava o ciphertext do primeiro, a verificação falhava e
    apagava o arquivo que o primeiro já tinha commitado."""
    key = os.urandom(32)
    monkeypatch.setattr(storage_mod, "ENCRYPTION_ENABLED", True, raising=False)
    monkeypatch.setattr(storage_mod, "encryption_key", key, raising=False)
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    content = b"mesmo-conteudo-" * 200
    sha = _sha(content)

    # Os dois chegam juntos à cifragem — força a janela da corrida.
    barrier = threading.Barrier(2, timeout=10)
    real_encrypt = crypto.encrypt_stream

    def encrypt_in_lockstep(src, dst, k):
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            pass
        return real_encrypt(src, dst, k)

    monkeypatch.setattr(crypto, "encrypt_stream", encrypt_in_lockstep)

    results = {}

    def go(path):
        results[path] = upload_file(client, "b1", "v1", path=path, content=content)

    t1 = threading.Thread(target=go, args=("/a.txt",))
    t2 = threading.Thread(target=go, args=("/b.txt",))
    t1.start(); t2.start(); t1.join(); t2.join()

    assert set(results) == {"/a.txt", "/b.txt"}
    db = m.SessionLocal()
    try:
        fcs = db.query(FileContent).filter_by(sha256=sha).all()
        assert len(fcs) == 1 and fcs[0].encrypted
        stored = Path(fcs[0].stored_at)
        assert stored.exists()
        assert storage_mod.content_matches(stored, sha, True, key)
        assert db.query(VersionFile).filter_by(sha256=sha).count() == 2
    finally:
        db.close()
    leftovers = [p for p in tmp_vol.rglob("_enc_*")] + [p for p in tmp_vol.glob("_tmp_*")]
    assert leftovers == []


# -- rebalance ------------------------------------------------------------------

def _two_volumes(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    def fake_usage(path):
        if path == v1:
            return DiskUsage(total=10 * GB, used=10 * GB, free=0)
        return DiskUsage(total=200 * GB, used=100 * GB, free=100 * GB)

    _patch_threshold(monkeypatch, [v1, v2], 1.0, fake_usage)
    monkeypatch.setattr(storage_mod, "encryption_key", None)
    return v1, v2


def test_rebalance_commit_failure_keeps_source_files(tmp_path, monkeypatch):
    """O unlink da origem só pode vir depois do commit: um rollback no meio do
    lote devolvia as linhas de origem apontando para arquivos já apagados."""
    v1, v2 = _two_volumes(tmp_path, monkeypatch)
    db = _make_session(tmp_path / "test.db")
    monkeypatch.setattr(storage_mod, "_REBALANCE_BATCH", 1)
    shas = [_seed_file(db, v1, f"f{i}", int(0.5 * GB)) for i in range(3)]
    src_paths = {s: Path(db.query(FileContentCopy).filter_by(sha256=s).one().stored_at) for s in shas}

    real_commit = db.commit
    calls = {"n": 0}

    def flaky_commit():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("database is locked")
        real_commit()

    monkeypatch.setattr(db, "commit", flaky_commit)

    with pytest.raises(RuntimeError):
        storage_mod.rebalance_disks(db)

    for s, p in src_paths.items():
        assert p.exists(), "arquivo de origem apagado antes do commit"
        assert storage_mod.file_sha256(p) == s
        copy = db.query(FileContentCopy).filter_by(sha256=s).all()
        assert [c.volume_path for c in copy] == [str(v1)]


def test_rebalance_recopies_when_destination_row_is_corrupt(tmp_path, monkeypatch):
    v1, v2 = _two_volumes(tmp_path, monkeypatch)
    db = _make_session(tmp_path / "test.db")
    sha = _seed_file(db, v1, "x", int(0.5 * GB))
    src = Path(db.query(FileContentCopy).filter_by(sha256=sha).one().stored_at)
    bad = storage_mod.content_path(sha, v2)
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_bytes(b"truncado")
    db.add(FileContentCopy(sha256=sha, stored_at=str(bad), volume_path=str(v2)))
    db.commit()

    result = storage_mod.rebalance_disks(db)

    assert result["moved"] == 1
    assert storage_mod.file_sha256(bad) == sha
    assert not src.exists()
    assert [c.volume_path for c in db.query(FileContentCopy).filter_by(sha256=sha)] == [str(v2)]


def test_rebalance_moves_encrypted_content(tmp_path, monkeypatch):
    """Antes, file_sha256(dest) != sha256 para todo arquivo cifrado: o rebalance
    pulava tudo e o job automático repetia as mesmas cópias a cada intervalo."""
    v1, v2 = _two_volumes(tmp_path, monkeypatch)
    key = os.urandom(32)
    monkeypatch.setattr(storage_mod, "encryption_key", key)
    db = _make_session(tmp_path / "test.db")
    data = b"segredo" * 500
    sha = _sha(data)
    plain = tmp_path / "plain"; plain.write_bytes(data)
    src = storage_mod.content_path(sha, v1)
    src.parent.mkdir(parents=True, exist_ok=True)
    crypto.encrypt_stream(plain, src, key)
    db.add(FileContent(sha256=sha, stored_at=str(src), size=int(0.5 * GB), encrypted=True))
    db.add(FileContentCopy(sha256=sha, stored_at=str(src), volume_path=str(v1)))
    db.commit()

    result = storage_mod.rebalance_disks(db)

    assert result["moved"] == 1
    copy = db.query(FileContentCopy).filter_by(sha256=sha).one()
    assert copy.volume_path == str(v2)
    assert storage_mod.content_matches(Path(copy.stored_at), sha, True, key)
    assert not src.exists()


# -- orphan cleanup -------------------------------------------------------------

def _orphan_one(client):
    """Deixa o conteúdo da versão antiga sem referência e devolve (db, sha, path)."""
    _label_with_two_done_versions(client)
    db = m.SessionLocal()
    antigo = (db.query(VersionFile)
                .join(db_mod.BackupVersion, VersionFile.version_id == db_mod.BackupVersion.id)
                .filter(db_mod.BackupVersion.version_key == "2020-01-01T00:00:00").one())
    sha = antigo.sha256
    path = Path(db.get(FileContent, sha).stored_at)
    db.query(VersionFile).filter(VersionFile.sha256 == sha).delete(synchronize_session=False)
    db.commit()
    return db, sha, path


def test_orphan_cleanup_does_not_delete_file_republished_after_claim(client, monkeypatch):
    """Upload do mesmo sha256 que chega entre o commit e o unlink publica um
    arquivo novo no mesmo caminho — ele tem de sobreviver."""
    db, sha, path = _orphan_one(client)
    real_claim = nightly_cleanup._claim_for_purge

    def claim_then_republish(paths):
        claimed = real_claim(paths)
        path.write_bytes(b"conteudo novo publicado por upload concorrente")
        return claimed

    monkeypatch.setattr(nightly_cleanup, "_claim_for_purge", claim_then_republish)
    try:
        removed, _ = nightly_cleanup._cleanup_orphan_contents(db)
        db.commit()
    finally:
        db.close()

    assert removed == 1
    assert path.exists() and path.read_bytes().startswith(b"conteudo novo")
    assert not path.with_name(path.name + nightly_cleanup.PURGE_SUFFIX).exists()


def test_orphan_cleanup_restores_claimed_files_when_commit_fails(client, monkeypatch):
    db, sha, path = _orphan_one(client)
    state = {"claimed": False}
    real_claim = nightly_cleanup._claim_for_purge

    def claim_and_flag(paths):
        state["claimed"] = True
        return real_claim(paths)

    real_commit = db.commit

    def commit_fails_after_claim():
        if state["claimed"]:
            state["claimed"] = False
            raise RuntimeError("database is locked")
        real_commit()

    monkeypatch.setattr(nightly_cleanup, "_claim_for_purge", claim_and_flag)
    monkeypatch.setattr(db, "commit", commit_fails_after_claim)
    try:
        with pytest.raises(RuntimeError):
            nightly_cleanup._cleanup_orphan_contents(db)
    finally:
        db.close()

    assert path.exists(), "arquivo reivindicado não voltou ao lugar após o rollback"
    assert not path.with_name(path.name + nightly_cleanup.PURGE_SUFFIX).exists()
    db = m.SessionLocal()
    assert db.get(FileContent, sha) is not None
    db.close()


def test_tmp_sweep_collects_stale_purge_files(tmp_path):
    vol = tmp_path / "vol"
    d = vol / "_content" / "ab"; d.mkdir(parents=True, exist_ok=True)
    stale = d / ("a" * 64 + nightly_cleanup.PURGE_SUFFIX)
    stale.write_bytes(b"x")
    os.utime(stale, (0, 0))
    removed, _, failed = nightly_cleanup._cleanup_stale_tmp_files([vol])
    assert removed == 1 and failed == []
    assert not stale.exists()


# -- backfill -------------------------------------------------------------------

def test_backfill_continues_after_integrity_error(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"
    db = _make_session(tmp_path / "test.db")
    Session = type(db)
    monkeypatch.setattr(db_mod, "SessionLocal", lambda: _make_session(tmp_path / "test.db"))
    shas = []
    for i in range(3):
        data = f"c{i}".encode(); sha = _sha(data)
        p = storage_mod.content_path(sha, v1); p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(data)
        db.add(FileContent(sha256=sha, stored_at=str(p), size=len(data)))
        shas.append((sha, p))
    db.commit()
    # A segunda já tem a linha de cópia: o INSERT dela conflita.
    sha_mid, p_mid = sorted(shas)[1]
    db.add(FileContentCopy(sha256=sha_mid, stored_at=str(p_mid), volume_path=str(v1)))
    db.commit(); db.close()

    storage_mod.backfill_content_copies()

    check = _make_session(tmp_path / "test.db")
    assert check.query(FileContentCopy).count() == 3
    check.close()


# -- volume que volta de degraded ----------------------------------------------

def test_recovered_volume_is_reported_once(tmp_path, monkeypatch):
    v1 = tmp_path / "v1"; v1.mkdir()
    monkeypatch.setattr(storage_mod, "STORAGE_VOLUMES", [v1])
    monkeypatch.setattr(storage_mod.shutil, "disk_usage", lambda p: DiskUsage(10, 5, 5))
    storage_mod._degraded_volumes.clear()
    storage_mod.drain_recovered_volumes()
    storage_mod._degraded_volumes.add(v1)

    # Qualquer leitura que dá certo (stats, Atividade...) tira o volume de degraded —
    # mas agora ele fica anotado para o monitor re-replicar.
    assert storage_mod.safe_disk_usage(v1) is not None
    assert storage_mod.healthy_volumes() == [v1]
    assert storage_mod.drain_recovered_volumes() == [v1]
    assert storage_mod.drain_recovered_volumes() == []


# -- move SSD → HDD -------------------------------------------------------------

def test_ssd_move_enospc_does_not_delete_registered_destination_copy(tmp_path, monkeypatch):
    """O caminho final pode já ser uma réplica registrada por ensure_replicas
    (dedup durante a pendência). Um ENOSPC na cópia não pode apagá-la."""
    db = _make_session(tmp_path / "test.db")
    data = b"conteudo"; sha = _sha(data)
    ssd_dir = tmp_path / "ssd"; ssd_file = storage_mod.content_path(sha, ssd_dir)
    ssd_file.parent.mkdir(parents=True, exist_ok=True); ssd_file.write_bytes(data)
    hdd = tmp_path / "hdd1"; dest = storage_mod.content_path(sha, hdd)
    dest.parent.mkdir(parents=True, exist_ok=True); dest.write_bytes(data)
    db.add(FileContent(sha256=sha, stored_at=str(ssd_file), size=len(data)))
    db.add(FileContentCopy(sha256=sha, stored_at=str(ssd_file), volume_path=str(ssd_dir)))
    db.add(FileContentCopy(sha256=sha, stored_at=str(dest), volume_path=str(hdd)))
    db.add(db_mod.SsdCachePendingMove(sha256=sha, ssd_path=str(ssd_file),
                                      dest_volume=str(hdd), dest_path=str(dest)))
    db.commit()

    def enospc(_src, _dst):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(storage_mod, "_copy_with_sha256", enospc)
    monkeypatch.setattr(storage_mod, "pick_volume_for_size", lambda _s, exclude=frozenset(): None)
    monkeypatch.setattr(storage_mod, "safe_disk_usage", lambda v: DiskUsage(0, 0, 0))

    storage_mod.process_ssd_pending_moves(db)

    assert dest.exists() and dest.read_bytes() == data
    assert ssd_file.exists()
    assert [p for p in dest.parent.glob("_tmp_*")] == []


# -- encrypt-existing com cópia em volume degraded -----------------------------

def test_encrypt_existing_defers_content_with_copy_on_degraded_volume(client, tmp_vol, monkeypatch):
    """Marcar encrypted=True com uma cópia ainda em plaintext num disco fora do ar
    faria readable_copy servir plaintext como cifrado quando ele voltasse."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    upload_file(client, "b1", "v1", path="/a.txt", content=b"fica-em-claro")
    upload_file(client, "b1", "v1", path="/b.txt", content=b"vai-cifrar")
    sha_a, sha_b = _sha(b"fica-em-claro"), _sha(b"vai-cifrar")

    offline = tmp_vol.parent / "offline"
    db = m.SessionLocal()
    db.add(FileContentCopy(sha256=sha_a, stored_at=str(storage_mod.content_path(sha_a, offline)),
                           volume_path=str(offline)))
    db.commit(); db.close()
    storage_mod._degraded_volumes.add(offline)

    monkeypatch.setattr(storage_mod, "ENCRYPTION_ENABLED", True, raising=False)
    monkeypatch.setattr(storage_mod, "encryption_key", os.urandom(32), raising=False)
    db = m.SessionLocal()
    mj = db_mod.MaintenanceJob(job_type="encrypt-existing", status="running", summary="")
    db.add(mj); db.commit(); job_id = mj.id; db.close()
    m._bg_encrypt_existing(job_id)

    db = m.SessionLocal()
    try:
        assert db.get(FileContent, sha_a).encrypted is False
        assert db.get(FileContent, sha_b).encrypted is True
    finally:
        db.close()
