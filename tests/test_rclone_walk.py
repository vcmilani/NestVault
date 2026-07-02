"""Testa o walk incremental + checkpoint/resume do rclone_runner (job de fotos).

Mocka a listagem (list_dir_one_level) e o download (_download_and_ingest_dir)
para exercitar só a lógica de walk/checkpoint sem rclone nem rede.
"""
import asyncio
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


def _make_job(Session, remote_path="", strategy="auto"):
    db = Session()
    job = RcloneBackupJob(
        remote_name="victor_icloud_photos", remote_path=remote_path,
        display_name="Fotos", target_label="fotos", strategy=strategy,
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

    # Força a estratégia walk (sem chamar `rclone config dump` real).
    async def fake_cfg(remote_name):
        return {"type": "iclouddrive", "service": "photos"}
    monkeypatch.setattr(rr, "_remote_config", fake_cfg)

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


# -- Dispatch por backend -----------------------------------------------------

def test_uses_walk_discriminator():
    assert rr._uses_walk({"type": "iclouddrive", "service": "photos"}) is True
    assert rr._uses_walk({"type": "iclouddrive", "service": "drive"}) is False
    assert rr._uses_walk({"type": "onedrive"}) is False
    assert rr._uses_walk({"type": "drive"}) is False
    assert rr._uses_walk({}) is False


@pytest.mark.asyncio
async def test_remote_config_parses_dump(monkeypatch):
    async def fake_run(*args, **kw):
        dump = json.dumps({
            "victor_icloud_photos": {"type": "iclouddrive", "service": "photos"},
            "onedrive": {"type": "onedrive"},
        }).encode()
        return dump, b"", 0
    monkeypatch.setattr(rr, "_rclone_run", fake_run)
    assert (await rr._remote_config("onedrive"))["type"] == "onedrive"
    assert (await rr._remote_config("victor_icloud_photos"))["service"] == "photos"
    assert await rr._remote_config("inexistente") == {}

    async def fail_run(*args, **kw):
        return b"", b"erro", 1
    monkeypatch.setattr(rr, "_rclone_run", fail_run)
    assert await rr._remote_config("qualquer") == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("cfg,expect_walk", [
    ({"type": "iclouddrive", "service": "photos"}, True),
    ({"type": "onedrive"}, False),
])
async def test_run_dispatches_by_backend(session_factory, monkeypatch, cfg, expect_walk):
    Session = session_factory
    jid = _make_job(Session)
    calls = {"walk": 0, "fast": 0}

    async def fake_cfg(remote_name):
        return cfg

    async def spy_walk(job, db):
        calls["walk"] += 1

    async def spy_fast(job, db):
        calls["fast"] += 1

    monkeypatch.setattr(rr, "_remote_config", fake_cfg)
    monkeypatch.setattr(rr, "_run_walk_strategy", spy_walk)
    monkeypatch.setattr(rr, "_run_fast_strategy", spy_fast)

    await rr.run_rclone_backup_job(jid)

    assert calls["walk"] == (1 if expect_walk else 0)
    assert calls["fast"] == (0 if expect_walk else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy,cfg,expect_walk", [
    ("auto", {"type": "iclouddrive", "service": "photos"}, True),
    ("auto", {"type": "onedrive"}, False),
    ("walk", {"type": "onedrive"}, True),      # força walk mesmo em backend "rápido"
    ("fast", {"type": "iclouddrive", "service": "photos"}, False),  # força fast mesmo no iCloud Photos
])
async def test_strategy_override_dispatch(session_factory, monkeypatch, strategy, cfg, expect_walk):
    Session = session_factory
    jid = _make_job(Session, strategy=strategy)
    calls = {"walk": 0, "fast": 0}

    async def fake_cfg(remote_name):
        return cfg

    async def spy_walk(job, db):
        calls["walk"] += 1

    async def spy_fast(job, db):
        calls["fast"] += 1

    monkeypatch.setattr(rr, "_remote_config", fake_cfg)
    monkeypatch.setattr(rr, "_run_walk_strategy", spy_walk)
    monkeypatch.setattr(rr, "_run_fast_strategy", spy_fast)

    await rr.run_rclone_backup_job(jid)

    assert calls["walk"] == (1 if expect_walk else 0)
    assert calls["fast"] == (0 if expect_walk else 1)


@pytest.mark.asyncio
async def test_list_dir_skips_recently_deleted(monkeypatch):
    """'Recently Deleted' do iCloud Photos é ignorada no walk, sem erro."""
    payload = json.dumps([
        {"Name": "Recently Deleted", "IsDir": True},
        {"Name": "2024", "IsDir": True},
        {"Name": "a.jpg", "IsDir": False, "Size": 10,
         "ModTime": "2024-01-01T00:00:00Z"},
    ]).encode()

    async def fake_lsjson(*args, **kw):
        return payload, b"", 0
    monkeypatch.setattr(rr, "_run_lsjson", fake_lsjson)

    files, subdirs = await rr.list_dir_one_level("victor_icloud_photos", "", "")

    assert subdirs == ["2024"]
    assert [f.path for f in files] == ["a.jpg"]


@pytest.mark.asyncio
async def test_stale_checkpoint_with_recently_deleted_is_skipped(session_factory, monkeypatch):
    """Checkpoint pré-existente com 'Recently Deleted' pendente (ex: de antes
    do fix de pastas protegidas) é descartado no pop, sem tentar listar."""
    Session = session_factory
    jid = _make_job(Session)
    tree = {
        "": ([_fe("ok.jpg")], []),
    }
    downloaded = []
    _install_tree(monkeypatch, tree, downloaded)

    async def fake_list(remote_name, remote_path, rel_dir, **kw):
        if rel_dir == "Recently Deleted":
            raise AssertionError("não deveria tentar listar Recently Deleted")
        return tree[rel_dir]
    monkeypatch.setattr(rr, "list_dir_one_level", fake_list)

    # Cria uma versão incompleta com checkpoint já contendo o pendente "sujo".
    db = Session()
    ver = BackupVersion(
        backup_label="fotos", version_key="2024-01-01T00:00:00",
        status="incomplete",
        progress_json=json.dumps({
            "done_dirs": [], "pending_dirs": ["", "Recently Deleted"], "resume_count": 0,
        }),
    )
    db.add(ver)
    db.commit()
    db.close()

    await rr.run_rclone_backup_job(jid)

    db = Session()
    ver = db.query(BackupVersion).filter_by(backup_label="fotos").one()
    assert ver.status == "done"
    assert ver.progress_json is None
    db.close()


@pytest.mark.asyncio
async def test_max_resumes_abandons_version_and_creates_new(session_factory, monkeypatch):
    """Após _MAX_RESUMES resumes sem concluir, versão vira 'failed' e
    o próximo run cria versão nova."""
    Session = session_factory
    monkeypatch.setattr(rr, "datetime", _IncDateTime)
    jid = _make_job(Session)
    tree = {
        "": ([], ["A", "B"]),
        "A": ([_fe("A/ok.jpg")], []),
        "B": ([_fe("B/bad.jpg")], []),   # B sempre falha na listagem
    }
    downloaded = []
    _install_tree(monkeypatch, tree, downloaded, list_fail_dirs={"B"})

    # O contador é salvo no FINAL de cada run falhado. A sequência é:
    #   Run 1 (inicial): salva resume_count=0
    #   Run 2..N+1 (resumes): carrega N-1, incrementa para N, salva N
    #   Run N+2 (abandon): carrega _MAX_RESUMES → check >= _MAX_RESUMES → abandon + nova versão
    # Total necessário: _MAX_RESUMES + 2
    for _ in range(rr._MAX_RESUMES + 2):
        await rr.run_rclone_backup_job(jid)

    db = Session()
    vers = db.query(BackupVersion).filter_by(backup_label="fotos").order_by(
        BackupVersion.id).all()

    # Versão original abandonada (failed, checkpoint limpo)
    assert vers[0].status == "failed"
    assert vers[0].progress_json is None
    # Nova versão foi criada no último run
    assert len(vers) == 2
    assert vers[1].status == "incomplete"   # B ainda falha, mas é um novo início
    db.close()


@pytest.mark.asyncio
async def test_walk_prefetches_next_dir_listing_during_download(session_factory, monkeypatch):
    """A listagem do próximo dir começa antes do download do dir atual
    terminar — comprova o pipeline de profundidade 1."""
    Session = session_factory
    jid = _make_job(Session)
    tree = {
        "": ([], ["A", "B"]),
        "A": ([_fe("A/ok.jpg")], []),
        "B": ([_fe("B/ok.jpg")], []),
    }
    events: list[str] = []
    download_gate = asyncio.Event()

    async def fake_list(remote_name, remote_path, rel_dir, **kw):
        events.append(f"list-start:{rel_dir}")
        return tree[rel_dir]

    async def fake_download(rel_dir, changed, remote_name, remote_path, version_id, db, enc_key, errors):
        events.append(f"download-start:{rel_dir}")
        if rel_dir == "A":
            # Segura o download de A até a listagem de B já ter começado.
            await asyncio.wait_for(download_gate.wait(), timeout=1)
        dl = 0
        for e in changed:
            rr._register_version_file_sync(version_id, e, "sha_" + e.path, db)
            dl += 1
        return dl, sum(e.size for e in changed)

    async def fake_cfg(remote_name):
        return {"type": "iclouddrive", "service": "photos"}
    monkeypatch.setattr(rr, "_remote_config", fake_cfg)
    monkeypatch.setattr(rr, "list_dir_one_level", fake_list)
    monkeypatch.setattr(rr, "_download_and_ingest_dir", fake_download)

    async def _release_gate_soon():
        # dá tempo do prefetch de B disparar antes de liberar o download de A
        await asyncio.sleep(0.05)
        download_gate.set()
    asyncio.create_task(_release_gate_soon())

    await rr.run_rclone_backup_job(jid)

    # A listagem de B deve ter começado ANTES do download de A ser liberado
    # (ou seja, enquanto o download de A ainda estava em andamento).
    assert events.index("list-start:B") < events.index("download-start:B")
    # prova real do pipeline: list-start:B aconteceu durante download-start:A,
    # não depois dele terminar.
    assert "list-start:B" in events[events.index("download-start:A"):]
