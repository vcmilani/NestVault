"""Testa o walk incremental + checkpoint/resume do rclone_runner (job de fotos).

Mocka a listagem (list_dir_one_level) e o download (_download_and_ingest_dir)
para exercitar só a lógica de walk/checkpoint sem rclone nem rede.
"""
import datetime as _dt
import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import database as db_mod
import storage as storage_mod
import cloud.rclone_runner as rr
from cloud.rclone_runner import RcloneFileEntry
from database import BackupVersion, VersionFile, RcloneBackupJob


@pytest.fixture
def session_factory(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    db_mod.Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    monkeypatch.setattr(rr, "SessionLocal", Session)
    monkeypatch.setattr(storage_mod, "ENCRYPTION_ENABLED", False, raising=False)
    return Session


def _make_job(Session, remote_path=""):
    db = Session()
    job = RcloneBackupJob(
        remote_name="victor_icloud_photos", remote_path=remote_path,
        display_name="Fotos", target_label="fotos",
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    jid = job.id
    db.close()
    return jid


def _fe(path, mtime=1.0, size=10):
    return RcloneFileEntry(path=path, size=size, mtime=mtime)


def _install_tree(monkeypatch, tree, downloaded_paths, fail_dirs=None,
                  list_fail_dirs=None):
    """tree: {rel_dir: (files:[RcloneFileEntry], subdirs:[str])}.
    Registra os paths baixados em downloaded_paths; fail_dirs força erro de
    download; list_fail_dirs força erro de listagem."""
    fail_dirs = fail_dirs or set()
    list_fail_dirs = list_fail_dirs or set()

    async def fake_list(remote_name, remote_path, rel_dir, **kw):
        if rel_dir in list_fail_dirs:
            raise RuntimeError("lsjson simulado falhou")
        return tree[rel_dir]

    async def fake_download(rel_dir, changed, remote_name, remote_path,
                            version_id, db, enc_key, errors):
        if rel_dir in fail_dirs:
            for e in changed:
                errors.append(f"{e.path}: erro simulado")
            return 0, 0
        dl = 0
        by = 0
        for e in changed:
            # Simula ingest: registra VersionFile e marca baixado.
            rr._register_version_file_sync(version_id, e, "sha_" + e.path, db)
            downloaded_paths.append(e.path)
            dl += 1
            by += e.size
        return dl, by

    monkeypatch.setattr(rr, "list_dir_one_level", fake_list)
    monkeypatch.setattr(rr, "_download_and_ingest_dir", fake_download)


@pytest.mark.asyncio
async def test_walk_completes_and_clears_checkpoint(session_factory, monkeypatch):
    Session = session_factory
    jid = _make_job(Session)
    tree = {
        "": ([], ["PrimarySync", "SharedSync-X"]),
        "PrimarySync": ([_fe("PrimarySync/a.jpg"), _fe("PrimarySync/b.jpg")],
                        ["PrimarySync/2024"]),
        "PrimarySync/2024": ([_fe("PrimarySync/2024/c.jpg")], []),
        "SharedSync-X": ([_fe("SharedSync-X/d.jpg")], []),
    }
    downloaded = []
    _install_tree(monkeypatch, tree, downloaded)

    await rr.run_rclone_backup_job(jid)

    db = Session()
    ver = db.query(BackupVersion).filter_by(backup_label="fotos").one()
    assert ver.status == "done"
    assert ver.progress_json is None
    paths = {vf.original_path for vf in db.query(VersionFile).filter_by(version_id=ver.id)}
    assert paths == {
        "PrimarySync/a.jpg", "PrimarySync/b.jpg",
        "PrimarySync/2024/c.jpg", "SharedSync-X/d.jpg",
    }
    assert sorted(downloaded) == sorted(paths)
    db.close()


@pytest.mark.asyncio
async def test_failed_dir_keeps_version_incomplete_and_resumes(session_factory, monkeypatch):
    Session = session_factory
    jid = _make_job(Session)
    tree = {
        "": ([], ["A", "B"]),
        "A": ([_fe("A/ok.jpg")], []),
        "B": ([_fe("B/bad.jpg")], []),
    }
    downloaded = []
    # 1ª execução: diretório B falha.
    _install_tree(monkeypatch, tree, downloaded, fail_dirs={"B"})
    await rr.run_rclone_backup_job(jid)

    db = Session()
    ver = db.query(BackupVersion).filter_by(backup_label="fotos").one()
    assert ver.status == "incomplete"
    cp = json.loads(ver.progress_json)
    assert "A" in cp["done_dirs"] and "" in cp["done_dirs"]
    assert cp["pending_dirs"] == ["B"]          # B re-tentado no resume
    assert downloaded == ["A/ok.jpg"]
    db.close()

    # 2ª execução (resume): B agora funciona. Não deve re-baixar A.
    downloaded.clear()
    _install_tree(monkeypatch, tree, downloaded, fail_dirs=set())
    await rr.run_rclone_backup_job(jid)

    db = Session()
    vers = db.query(BackupVersion).filter_by(backup_label="fotos").all()
    assert len(vers) == 1                       # resume continua a MESMA versão
    ver = vers[0]
    assert ver.status == "done"
    assert ver.progress_json is None
    assert downloaded == ["B/bad.jpg"]          # só B foi baixado no resume
    paths = {vf.original_path for vf in db.query(VersionFile).filter_by(version_id=ver.id)}
    assert paths == {"A/ok.jpg", "B/bad.jpg"}
    db.close()


@pytest.mark.asyncio
async def test_list_failure_one_dir_does_not_abort_others(session_factory, monkeypatch):
    Session = session_factory
    jid = _make_job(Session)
    tree = {
        "": ([], ["A", "B"]),
        "A": ([_fe("A/ok.jpg")], []),
        "B": ([_fe("B/x.jpg")], []),   # listagem de B vai falhar
    }
    downloaded = []
    _install_tree(monkeypatch, tree, downloaded, list_fail_dirs={"B"})
    await rr.run_rclone_backup_job(jid)

    db = Session()
    ver = db.query(BackupVersion).filter_by(backup_label="fotos").one()
    # A foi processado apesar da falha em B; versão fica incomplete com B pendente.
    assert downloaded == ["A/ok.jpg"]
    assert ver.status == "incomplete"
    cp = json.loads(ver.progress_json)
    assert cp["pending_dirs"] == ["B"]
    assert "A" in cp["done_dirs"]
    db.close()


class _IncDateTime(_dt.datetime):
    """now() avança 1s a cada chamada — evita colisão de version_key entre dois
    runs no mesmo segundo (granularidade do version_key é de segundos)."""
    _n = 0

    @classmethod
    def now(cls, tz=None):
        _IncDateTime._n += 1
        return _dt.datetime.now(tz) + _dt.timedelta(seconds=_IncDateTime._n)


@pytest.mark.asyncio
async def test_done_version_skips_unchanged_by_mtime(session_factory, monkeypatch):
    Session = session_factory
    monkeypatch.setattr(rr, "datetime", _IncDateTime)
    jid = _make_job(Session)
    tree = {"": ([_fe("a.jpg", mtime=5.0)], [])}
    downloaded = []
    _install_tree(monkeypatch, tree, downloaded)
    await rr.run_rclone_backup_job(jid)
    assert downloaded == ["a.jpg"]

    # 2ª execução: mesmo mtime → deve pular (nova versão, sem novo download).
    downloaded.clear()
    _install_tree(monkeypatch, tree, downloaded)
    await rr.run_rclone_backup_job(jid)

    db = Session()
    vers = db.query(BackupVersion).filter_by(backup_label="fotos").order_by(
        BackupVersion.id).all()
    assert len(vers) == 2                       # backup completo → nova versão
    assert vers[1].status == "done"
    assert downloaded == []                      # mtime inalterado → sem download
    # A nova versão ainda registra o arquivo (snapshot completo).
    paths = {vf.original_path for vf in db.query(VersionFile).filter_by(version_id=vers[1].id)}
    assert paths == {"a.jpg"}
    db.close()
