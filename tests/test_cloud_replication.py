"""Replicação dedicada do Cloud (cloud.replication_factor, v10.3.0): arquivos das
pastas do servidor ganham mais cópias; saindo do Cloud e existindo num backup,
voltam ao fator do storage. E o espaço usado/disponível do /cloud/me."""
import base64

import pytest

import cloud_replicas
import database as db_mod
import main as m
import storage as storage_mod
from conftest import _fake_ample_disk_usage, _FAKE_FREE_BYTES
from test_replication import _copies_in, _mk_client, _upload


@pytest.fixture
def vols(tmp_path):
    out = []
    for name in ("v1", "v2", "v3"):
        v = tmp_path / name
        v.mkdir()
        out.append(v)
    return out


@pytest.fixture
def client3(vols, monkeypatch):
    """3 volumes, fator do storage 1, fator do Cloud 3."""
    monkeypatch.setattr(storage_mod.shutil, "disk_usage", _fake_ample_disk_usage)
    monkeypatch.setattr(storage_mod, "CLOUD_REPLICATION_FACTOR", 3)
    yield from _mk_client(monkeypatch, vols, replication_factor=1)


def _n(vols, sha):
    return sum(len(_copies_in(v, sha)) for v in vols)


def _rows(sha):
    db = m.SessionLocal()
    try:
        return db.query(db_mod.FileContentCopy).filter(db_mod.FileContentCopy.sha256 == sha).count()
    finally:
        db.close()


def _drive(c, name="Documentos"):
    r = c.post("/cloud/drives", json={"name": name})
    assert r.status_code == 200, r.text
    return r.json()["label"]


def _put(c, label, path, content):
    r = c.put("/cloud/drive/upload", content=content, headers={
        "X-Label": label, "X-Path": base64.b64encode(path.encode()).decode(), "X-Mtime": "1000"})
    assert r.status_code == 200, r.text
    return r.json()["sha256"]


def _post(c, url, body):
    r = c.post(url, json=body)
    assert r.status_code == 200, r.text
    return r.json()


def _backup_with(c, content, label="b1"):
    c.post("/backups", json={"label": label})
    c.post(f"/backups/{label}/versions", json={"version_key": "v1"})
    sha = _upload(c, label, "v1", "/file.txt", content)["sha256"]
    c.patch(f"/backups/{label}/versions/v1", json={"status": "done"})
    return sha


def _trash_ids(c, label):
    return [e["id"] for e in c.get("/cloud/drive/trash", params={"label": label}).json()]


# -- Fator dedicado -------------------------------------------------------------

def test_upload_to_drive_gets_cloud_factor(client3, vols):
    label = _drive(client3)
    sha = _put(client3, label, "nota.txt", b"so no cloud")
    assert _n(vols, sha) == 3 and _rows(sha) == 3


def test_backup_upload_keeps_storage_factor(client3, vols):
    sha = _backup_with(client3, b"so no backup")
    assert _n(vols, sha) == 1


def test_cloud_factor_zero_follows_storage(client3, vols, monkeypatch):
    monkeypatch.setattr(storage_mod, "CLOUD_REPLICATION_FACTOR", 0)
    sha = _put(client3, _drive(client3), "a.txt", b"fator igual")
    assert _n(vols, sha) == 1


def test_cloud_factor_never_below_storage(client3, vols, monkeypatch):
    monkeypatch.setattr(storage_mod, "REPLICATION_FACTOR", 2)
    monkeypatch.setattr(storage_mod, "CLOUD_REPLICATION_FACTOR", 1)
    sha = _put(client3, _drive(client3), "a.txt", b"nunca abaixo")
    assert _n(vols, sha) == 2


def test_copy_from_backup_upgrades_to_cloud_factor(client3, vols):
    sha = _backup_with(client3, b"vem do backup")
    label = _drive(client3)
    _post(client3, "/cloud/drive/copy", {"src_label": "b1", "items": ["file.txt"],
                                          "dest_label": label})
    assert _n(vols, sha) == 3


def test_dedup_upload_upgrades_existing_content(client3, vols):
    sha = _backup_with(client3, b"mesmo conteudo")
    assert _n(vols, sha) == 1
    assert _put(client3, _drive(client3), "igual.txt", b"mesmo conteudo") == sha
    assert _n(vols, sha) == 3


# -- Saindo do Cloud ------------------------------------------------------------

def test_deleted_from_cloud_and_in_backup_drops_to_storage_factor(client3, vols):
    sha = _backup_with(client3, b"nos dois")
    label = _drive(client3)
    _post(client3, "/cloud/drive/copy", {"src_label": "b1", "items": ["file.txt"],
                                          "dest_label": label})
    assert _n(vols, sha) == 3
    _post(client3, "/cloud/drive/delete", {"label": label, "items": ["file.txt"]})
    assert _n(vols, sha) == 1 and _rows(sha) == 1


def test_deleted_from_cloud_only_keeps_cloud_factor_until_purge(client3, vols):
    label = _drive(client3)
    sha = _put(client3, label, "unico.txt", b"so existe aqui")
    _post(client3, "/cloud/drive/delete", {"label": label, "items": ["unico.txt"]})
    assert _n(vols, sha) == 3  # na lixeira ainda dá para restaurar
    _post(client3, "/cloud/drive/restore", {"label": label, "ids": _trash_ids(client3, label)})
    assert _n(vols, sha) == 3


def test_replaced_file_in_backup_drops_old_content(client3, vols):
    old = _backup_with(client3, b"versao antiga")
    label = _drive(client3)
    _post(client3, "/cloud/drive/copy", {"src_label": "b1", "items": ["file.txt"],
                                          "dest_label": label})
    new = _put(client3, label, "file.txt", b"versao nova")
    assert _n(vols, new) == 3
    assert _n(vols, old) == 1  # foi para a lixeira e existe no backup


def test_purge_of_shared_content_drops_to_storage_factor(client3, vols):
    label = _drive(client3)
    sha = _put(client3, label, "a.txt", b"compartilhado depois")
    # O mesmo conteúdo chega por um backup enquanto está no Cloud: continua 3.
    _backup_with(client3, b"compartilhado depois")
    assert _n(vols, sha) == 3
    _post(client3, "/cloud/drive/delete", {"label": label, "items": ["a.txt"]})
    assert _n(vols, sha) == 1


def test_degraded_volume_postpones_downgrade(client3, vols):
    sha = _backup_with(client3, b"adiado")
    label = _drive(client3)
    _post(client3, "/cloud/drive/copy", {"src_label": "b1", "items": ["file.txt"],
                                          "dest_label": label})
    storage_mod._degraded_volumes.add(vols[2])
    try:
        _post(client3, "/cloud/drive/delete", {"label": label, "items": ["file.txt"]})
        assert _rows(sha) == 3
    finally:
        storage_mod._degraded_volumes.discard(vols[2])
    db = m.SessionLocal()
    try:
        assert cloud_replicas.reconcile_all(db) == (0, 2)
    finally:
        db.close()
    assert _n(vols, sha) == 1


def test_trashing_whole_drive_drops_shared_content(client3, vols):
    sha = _backup_with(client3, b"pasta inteira")
    label = _drive(client3)
    _post(client3, "/cloud/drive/copy", {"src_label": "b1", "items": ["file.txt"],
                                          "dest_label": label})
    assert client3.delete(f"/backups/{label}").status_code == 200
    assert _n(vols, sha) == 1


# -- Manutenção global ----------------------------------------------------------

def test_reconcile_replication_keeps_cloud_copies(client3, vols):
    cloud_sha = _put(client3, _drive(client3), "a.txt", b"do cloud")
    backup_sha = _backup_with(client3, b"do backup")
    # Uma cópia excedente de verdade, no conteúdo de backup.
    db = m.SessionLocal()
    try:
        fc = db.get(db_mod.FileContent, backup_sha)
        storage_mod.ensure_replicas(backup_sha, storage_mod.Path(fc.stored_at), db, cloud=True)
        db.commit()
    finally:
        db.close()
    assert _n(vols, backup_sha) == 3
    r = client3.post("/maintenance/reconcile-replication")
    assert r.status_code == 200, r.text
    assert r.json()["cleaned"] == 2
    assert _n(vols, cloud_sha) == 3 and _n(vols, backup_sha) == 1


def test_rereplicate_fills_cloud_content(client3, vols):
    sha = _put(client3, _drive(client3), "a.txt", b"perdeu copias")
    db = m.SessionLocal()
    try:
        fc = db.get(db_mod.FileContent, sha)
        for c in db.query(db_mod.FileContentCopy).filter(db_mod.FileContentCopy.sha256 == sha):
            if c.stored_at != fc.stored_at:
                storage_mod.Path(c.stored_at).unlink()
                db.delete(c)
        db.commit()
    finally:
        db.close()
    assert _n(vols, sha) == 1
    assert client3.post("/maintenance/rereplicate").status_code == 200
    assert _n(vols, sha) == 3


def test_reconcile_job_after_factor_change(client3, vols, monkeypatch):
    monkeypatch.setattr(storage_mod, "CLOUD_REPLICATION_FACTOR", 0)
    sha = _put(client3, _drive(client3), "a.txt", b"muda o fator")
    assert _n(vols, sha) == 1
    monkeypatch.setattr(storage_mod, "CLOUD_REPLICATION_FACTOR", 2)
    cloud_replicas.reconcile_job()
    assert _n(vols, sha) == 2
    db = m.SessionLocal()
    try:
        mj = (db.query(db_mod.MaintenanceJob)
              .filter(db_mod.MaintenanceJob.job_type == "cloud-replication").one())
        assert mj.status == "done" and "1 cópia(s) criada(s)" in mj.summary
    finally:
        db.close()


# -- Espaço no /cloud/me --------------------------------------------------------

def test_me_reports_usage_and_available_without_quota(client3, vols):
    _put(client3, _drive(client3), "a.txt", b"12345")
    me = client3.get("/cloud/me").json()
    threshold = storage_mod.STORAGE_FALLBACK_THRESHOLD_GB * 1024 ** 3
    expected_free = int(3 * (_FAKE_FREE_BYTES - threshold) // 3)
    assert me["quota_bytes"] is None
    assert me["usage_bytes"] == 5
    assert me["storage_free_bytes"] == expected_free == me["available_bytes"]


def test_me_available_is_min_of_quota_and_disk(client3, vols):
    db = m.SessionLocal()
    try:
        admin = db.query(db_mod.User).filter(db_mod.User.username == "admin").one()
        admin.quota_bytes = 1000
        db.commit()
    finally:
        db.close()
    _put(client3, _drive(client3), "a.txt", b"x" * 100)
    me = client3.get("/cloud/me").json()
    assert me["quota_bytes"] == 1000 and me["usage_bytes"] == 100
    assert me["available_bytes"] == 900


def test_settings_change_starts_cloud_reconcile(client3, monkeypatch):
    calls = []
    monkeypatch.setattr(cloud_replicas, "start_reconcile", lambda: calls.append(1))
    try:
        r = client3.put("/api/settings", json={"cloud": {"replication_factor": 2}})
        assert r.status_code == 200, r.text
        assert storage_mod.CLOUD_REPLICATION_FACTOR == 2 and calls == [1]
        client3.put("/api/settings", json={"cloud": {"replication_factor": 2}})
        assert calls == [1]  # sem mudança, sem job
    finally:
        import config
        config.save({"cloud": {"replication_factor": 0}})


def test_reconcile_trims_cloud_content_to_cloud_factor(client3, vols, monkeypatch):
    sha = _put(client3, _drive(client3), "a.txt", b"cloud acima do alvo")
    assert _n(vols, sha) == 3
    monkeypatch.setattr(storage_mod, "CLOUD_REPLICATION_FACTOR", 2)
    r = client3.post("/maintenance/reconcile-replication")
    assert r.status_code == 200 and r.json()["cleaned"] == 1
    assert _n(vols, sha) == 2
