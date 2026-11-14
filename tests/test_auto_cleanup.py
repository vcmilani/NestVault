"""Limpeza automática por pressão de disco e tetos de upload (revisão v10.4.0).

Antes, qualquer upload com o disco abaixo do limiar apagava — de todos os
usuários, sem lixeira — as versões done antigas de cada label, e o body de um
upload não tinha limite nenhum. Agora só o lixo (incomplete/failed) do dono do
backup sai, o histórico done fica, o upload recebe 507 quando não há espaço, e
o body é recusado antes de ocupar disco quando passa do teto."""
import base64
import threading
from datetime import datetime, timedelta

import pytest

import daily_digest
import main as m
import quota
import storage as storage_mod
from conftest import make_backup, make_version, finish_version, upload_file
from database import BackupID, BackupVersion, FileContent, VersionFile


@pytest.fixture
def telegram(monkeypatch):
    sent: list[str] = []

    async def fake_send(text, *a, **k):
        sent.append(text)
        return True

    monkeypatch.setattr(daily_digest, "_send_telegram", fake_send)
    monkeypatch.setattr(m, "_storage_full_notified_at", 0.0)
    return sent


def _disk_full(monkeypatch):
    def no_volume():
        raise storage_mod.StorageThresholdExceeded("Todos os volumes abaixo de 10 GB (vol: 0.1 GB)")
    monkeypatch.setattr(m, "_pick_volume", no_volume)
    monkeypatch.setattr(m, "_volumes_with_free_space", lambda: 0)


def _raw_upload(client, label, vkey, path, content, **extra_headers):
    headers = {
        "X-Backup-Label": label, "X-Version-Key": vkey,
        "X-Original-Path": base64.b64encode(path.encode()).decode(), "X-Mtime": "1.0",
    }
    headers.update(extra_headers)
    return client.post("/upload", content=content, headers=headers)


def _statuses(client, label):
    return {v["version_key"]: v["status"] for v in client.get(f"/backups/{label}/versions").json()}


# -- escopo por dono -------------------------------------------------------------

def test_disk_pressure_keeps_everyone_history_and_fails_upload_with_507(two_users, telegram, monkeypatch):
    admin, alice, bob = two_users
    for c, label in ((alice, "alice-docs"), (bob, "bob-docs")):
        make_backup(c, label)
        for key in ("2026-01-01T00:00:00", "2026-02-01T00:00:00", "2026-03-01T00:00:00"):
            make_version(c, label, key)
            upload_file(c, label, key, path="/f.txt", content=f"{label}-{key}".encode())
            finish_version(c, label, key)
    # Lixo da alice: duas failed (a mais recente fica) e uma incomplete antiga.
    for key in ("2026-04-01T00:00:00", "2026-05-01T00:00:00"):
        make_version(alice, "alice-docs", key)
        finish_version(alice, "alice-docs", key, status="failed")
    make_version(alice, "alice-docs", "2026-06-01T00:00:00")
    # Lixo do bob: uma failed — de outro dono, não pode ser tocada.
    make_version(bob, "bob-docs", "2026-04-01T00:00:00")
    finish_version(bob, "bob-docs", "2026-04-01T00:00:00", status="failed")
    db = m.SessionLocal()
    old = datetime.now() - timedelta(days=2)
    db.query(BackupVersion).filter_by(version_key="2026-06-01T00:00:00").update(
        {"status": "incomplete", "created_at": old})
    db.commit(); db.close()

    make_version(alice, "alice-docs", "2026-07-01T00:00:00")
    _disk_full(monkeypatch)
    r = _raw_upload(alice, "alice-docs", "2026-07-01T00:00:00", "/novo.txt", b"nao cabe")

    assert r.status_code == 507
    assert "administrador" in r.json()["detail"]
    assert telegram and "Storage cheio" in telegram[0]

    a = _statuses(alice, "alice-docs")
    # Histórico done intacto; failed antiga e incomplete antiga saíram; failed mais
    # recente (diagnóstico) e a versão em escrita ficaram.
    assert {k: v for k, v in a.items() if v == "done"} == {
        "2026-01-01T00:00:00": "done", "2026-02-01T00:00:00": "done", "2026-03-01T00:00:00": "done"}
    assert "2026-04-01T00:00:00" not in a and "2026-06-01T00:00:00" not in a
    assert a["2026-05-01T00:00:00"] == "failed"
    assert a["2026-07-01T00:00:00"] == "running"
    # Nada do bob foi tocado — nem o lixo dele.
    b = _statuses(bob, "bob-docs")
    assert len(b) == 4 and b["2026-04-01T00:00:00"] == "failed"


def test_auto_cleanup_protects_rclone_checkpoint_and_recent_incomplete(client, monkeypatch):
    make_backup(client, "b1")
    make_version(client, "b1", "ckpt")
    make_version(client, "b1", "writing")
    make_version(client, "b1", "junk")
    upload_file(client, "b1", "writing", path="/a.txt", content=b"ainda escrevendo")
    db = m.SessionLocal()
    old = datetime.now() - timedelta(days=1)
    db.query(BackupVersion).filter_by(version_key="ckpt").update(
        {"status": "incomplete", "progress_json": '{"done_dirs": ["/x"]}', "created_at": old})
    db.query(BackupVersion).filter_by(version_key="writing").update({"status": "incomplete"})
    db.query(BackupVersion).filter_by(version_key="junk").update({"status": "incomplete", "created_at": old})
    db.commit()
    admin_id = db.query(BackupID).filter_by(label="b1").one().owner_user_id
    monkeypatch.setattr(m, "_volumes_with_free_space", lambda: 0)

    summary, _ = m._auto_cleanup_if_needed(db, owner_user_id=admin_id)
    db.close()

    assert summary.startswith("1 versão")
    assert set(_statuses(client, "b1")) == {"ckpt", "writing"}


def test_auto_cleanup_without_junk_reports_and_removes_nothing(client, monkeypatch):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    upload_file(client, "b1", "v1", path="/a.txt", content=b"x")
    finish_version(client, "b1", "v1")
    db = m.SessionLocal()
    monkeypatch.setattr(m, "_volumes_with_free_space", lambda: 0)

    summary, freed = m._auto_cleanup_if_needed(db)
    db.close()

    assert freed == 0 and "Nenhuma versão removível" in summary
    assert _statuses(client, "b1") == {"v1": "done"}


def test_concurrent_auto_cleanups_are_serialized(client, monkeypatch):
    make_backup(client, "b1")
    for i in range(6):
        make_version(client, "b1", f"f{i}")
        finish_version(client, "b1", f"f{i}", status="failed")
    monkeypatch.setattr(m, "_volumes_with_free_space", lambda: 0)
    errors: list[BaseException] = []

    def run():
        db = m.SessionLocal()
        try:
            m._auto_cleanup_with_job(db)
        except BaseException as e:  # noqa: BLE001
            errors.append(e)
        finally:
            db.close()

    threads = [threading.Thread(target=run) for _ in range(3)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert errors == []
    assert _statuses(client, "b1") == {"f5": "failed"}  # a failed mais recente fica


# -- teto de upload ---------------------------------------------------------------

def test_upload_over_limit_is_refused_by_content_length_without_writing(client, tmp_vol, monkeypatch):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    monkeypatch.setattr(m, "_upload_limit", lambda db, user, write_dir: (10, "max"))

    r = _raw_upload(client, "b1", "v1", "/big.bin", b"x" * 100)

    assert r.status_code == 413
    assert "limite de upload" in r.json()["detail"]
    assert list(tmp_vol.glob("_tmp_*")) == []
    db = m.SessionLocal()
    assert db.query(FileContent).count() == 0 and db.query(VersionFile).count() == 0
    db.close()


def test_upload_over_limit_is_cut_while_streaming(client, tmp_vol, monkeypatch):
    """Sem Content-Length (chunked) o corte acontece durante o stream, e o tmp sai."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    monkeypatch.setattr(m, "_upload_limit", lambda db, user, write_dir: (10, "disk"))

    r = _raw_upload(client, "b1", "v1", "/big.bin", iter([b"x" * 8, b"y" * 8, b"z" * 8]))

    assert r.status_code == 507
    assert "Sem espaço" in r.json()["detail"]
    assert list(tmp_vol.glob("_tmp_*")) == []
    assert m.SessionLocal().query(FileContent).count() == 0


def test_upload_within_limit_still_works(client, monkeypatch):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    monkeypatch.setattr(m, "_upload_limit", lambda db, user, write_dir: (10, "max"))
    assert _raw_upload(client, "b1", "v1", "/ok.bin", b"x" * 10).status_code == 200


def test_upload_limit_combines_setting_and_free_space(client, tmp_vol, monkeypatch):
    real_get = m.config.get
    monkeypatch.setattr(m.config, "get", lambda key, *a, **k: 2 if key == "storage.max_upload_gb" else real_get(key, *a, **k))
    db = m.SessionLocal()
    user = db.query(m.User).first()
    # conftest: 100 GB livres → o setting (2 GB) é o menor
    assert m._upload_limit(db, user, tmp_vol) == (2 * 1024 ** 3, "max")
    monkeypatch.setattr(storage_mod, "safe_disk_usage", lambda v: m.shutil.disk_usage(v)._replace(free=1024 ** 3))
    assert m._upload_limit(db, user, tmp_vol) == (1024 ** 3 - m._DISK_RESERVE_BYTES, "disk")
    db.close()


# -- reserva de cota ------------------------------------------------------------------

class _U:
    def __init__(self, uid, quota_bytes):
        self.id = uid
        self.quota_bytes = quota_bytes
        self.quota_alerted = False


def test_quota_reservation_counts_inflight_uploads(client, monkeypatch):
    quota.reset()
    monkeypatch.setattr(quota, "owns", lambda db, uid, sha: False)
    monkeypatch.setattr(quota, "usage_all", lambda db: {7: 0})
    user = _U(7, 100)
    db = m.SessionLocal()

    assert quota.check_new_content(db, user, "a" * 64, 60) is True
    assert quota.usage(db, 7) == 60                       # reservado conta como uso
    with pytest.raises(m.HTTPException) as exc:            # 60 + 60 > 100
        quota.check_new_content(db, user, "b" * 64, 60)
    assert exc.value.status_code == 507

    quota.release(7, 60)                                   # upload falhou
    assert quota.usage(db, 7) == 0
    assert quota.check_new_content(db, user, "b" * 64, 60) is True
    quota.commit_new_content(db, user, 60)                 # consome a reserva, soma ao uso
    assert quota.reserved(7) == 0
    db.close()


# -- versão incomplete continua gravável -----------------------------------------------

def test_register_batch_and_absorb_accept_incomplete_version(client):
    make_backup(client, "b1")
    make_version(client, "b1", "old")
    up = upload_file(client, "b1", "old", path="/a.txt", content=b"abc")
    finish_version(client, "b1", "old")
    make_version(client, "b1", "cur")
    db = m.SessionLocal()
    db.query(BackupVersion).filter_by(version_key="cur").update({"status": "incomplete"})
    db.commit(); db.close()

    r = client.post("/register/batch", json={"backup_label": "b1", "version_key": "cur",
                                             "files": [{"original_path": "/a.txt", "sha256": up["sha256"], "mtime": 1.0}]})
    assert r.status_code == 200, r.text
    r = client.post("/backups/b1/versions/cur/absorb", json={"source_version_key": "old"})
    assert r.status_code == 200, r.text
    assert client.patch("/backups/b1/versions/cur", json={"status": "done"}).status_code == 200


# -- limpeza por data com fuso -------------------------------------------------------------

def test_cleanup_by_date_accepts_timezone_aware_before(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    finish_version(client, "b1", "v1")
    make_version(client, "b1", "v2")
    finish_version(client, "b1", "v2")
    future = (datetime.now().astimezone() + timedelta(days=1)).isoformat()
    r = client.get("/maintenance/cleanup-by-date/preview", params={"before": future})
    assert r.status_code == 200
    assert r.json()["total"] == 1  # a done mais recente nunca entra


# -- GET /files paginado e /sync limitado -----------------------------------------------------

def test_files_pagination_and_total_header(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    for i in range(3):
        upload_file(client, "b1", "v1", path=f"/f{i}.txt", content=f"c{i}".encode())

    r = client.get("/files", params={"backup_label": "b1", "version_key": "v1", "limit": 2})
    assert r.status_code == 200 and len(r.json()) == 2 and r.headers["X-Total-Count"] == "3"
    r = client.get("/files", params={"backup_label": "b1", "version_key": "v1", "limit": 2, "offset": 2})
    assert [f["original_path"] for f in r.json()] == ["/f2.txt"]
    assert len(client.get("/files", params={"backup_label": "b1", "version_key": "v1"}).json()) == 3


def test_sync_rejects_absurd_path_lists(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    r = client.post("/sync", json={"backup_label": "b1", "version_key": "v1",
                                   "existing_paths": ["/x"] * 200_001})
    assert r.status_code == 422
