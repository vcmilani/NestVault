"""Alerta de backup atrasado (v10.2.0): intervalo por label, um alerta por
episódio, aviso de "voltou" e as exclusões (sem intervalo, lixeira, pasta do servidor)."""
from datetime import datetime, timedelta

import pytest

import config
import database as db_mod
import main as m
import notify
import stale_alerts
from conftest import make_backup, make_version, finish_version, upload_file

V1 = "2026-01-01T00:00:00"
V2 = "2026-01-02T00:00:00"


@pytest.fixture
def sent(monkeypatch):
    msgs = []
    monkeypatch.setattr(notify, "send_sync", lambda text, **kw: msgs.append(text) or True)
    monkeypatch.setattr(stale_alerts, "SessionLocal", lambda: m.SessionLocal())
    return msgs


def _done(c, label, key):
    make_version(c, label, key)
    upload_file(c, label, key, "/a.txt", key.encode())
    finish_version(c, label, key)


def _age_versions(label, hours):
    db = m.SessionLocal()
    try:
        old = datetime.now() - timedelta(hours=hours)
        db.query(db_mod.BackupVersion).filter(db_mod.BackupVersion.backup_label == label) \
          .update({"created_at": old})
        db.query(db_mod.BackupID).filter(db_mod.BackupID.label == label).update({"created_at": old})
        db.commit()
    finally:
        db.close()


def _set(c, label, hours, expect=200):
    r = c.put(f"/backups/{label}/alert", json={"expected_interval_hours": hours})
    assert r.status_code == expect, r.text
    return r.json()


def test_alert_once_then_recovered(client, sent):
    make_backup(client, "mac")
    _done(client, "mac", V1)
    _set(client, "mac", 24)
    _age_versions("mac", 31)  # 24h + 25% = 30h

    assert stale_alerts.check_stale_backups()["alerted"] == ["mac"]
    assert len(sent) == 1 and "mac" in sent[0] and "24h" in sent[0]
    info = {b["label"]: b for b in client.get("/backups").json()}["mac"]
    assert info["is_stale"] is True and info["expected_interval_hours"] == 24

    # Segunda rodada: mesmo episódio, sem spam.
    assert stale_alerts.check_stale_backups()["alerted"] == []
    assert len(sent) == 1

    _done(client, "mac", V2)
    assert stale_alerts.check_stale_backups()["recovered"] == ["mac"]
    assert len(sent) == 2 and "voltou" in sent[1]
    assert client.get("/backups/mac").json()["is_stale"] is False


def test_within_grace_is_not_stale(client, sent, monkeypatch):
    make_backup(client, "mac")
    _done(client, "mac", V1)
    _set(client, "mac", 24)
    _age_versions("mac", 29)
    assert stale_alerts.check_stale_backups()["alerted"] == []
    monkeypatch.setitem(config._data["alerts"], "stale_grace_pct", 0)
    assert stale_alerts.check_stale_backups()["alerted"] == ["mac"]


def test_label_without_versions_counts_from_creation(client, sent):
    make_backup(client, "novo")
    _set(client, "novo", 1)
    _age_versions("novo", 2)
    assert stale_alerts.check_stale_backups()["alerted"] == ["novo"]
    assert "desde a criação" in sent[0]


def test_ignored_without_interval_trashed_or_server(client, sent):
    make_backup(client, "sem-alerta")
    _done(client, "sem-alerta", V1)
    make_backup(client, "lixo")
    _done(client, "lixo", V1)
    _set(client, "lixo", 1)
    _age_versions("sem-alerta", 100)
    _age_versions("lixo", 100)
    db = m.SessionLocal()
    try:
        db.query(db_mod.BackupID).filter(db_mod.BackupID.label == "lixo") \
          .update({"status": db_mod.TRASHED_STATUS})
        db.commit()
    finally:
        db.close()
    assert stale_alerts.check_stale_backups()["alerted"] == []
    assert sent == []

    srv = client.post("/cloud/drives", json={"name": "Docs"}).json()["label"]
    _set(client, srv, 24, expect=409)


def test_disable_clears_and_rename_keeps(client, sent):
    make_backup(client, "mac")
    _done(client, "mac", V1)
    _set(client, "mac", 24)
    r = client.patch("/backups/mac", json={"new_label": "macbook"})
    assert r.status_code == 200 and r.json()["expected_interval_hours"] == 24
    assert _set(client, "macbook", None)["expected_interval_hours"] is None


def test_user_cannot_set_alert_on_other_users_label(two_users, sent):
    _admin, alice, bob = two_users
    make_backup(alice, "alice-mac")
    r = bob.put("/backups/alice-mac/alert", json={"expected_interval_hours": 24})
    assert r.status_code in (403, 404)
