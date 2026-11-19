"""Resumo por versão (version_summary) e painéis globais por impressão digital
(stats_panels).

O principal: as stats calculadas a partir dos resumos têm que dar exatamente o
mesmo que a agregação direta de version_files (o jeito antigo), inclusive depois
de mudanças que invalidam resumos (versão do meio apagada, pasta do Cloud
alterada, versão em andamento recebendo arquivos).
"""
import base64
import threading
from collections import defaultdict

from sqlalchemy import func

import database as db_mod
import main as m
import stats_panels
import version_diff
import version_summary
from database import BackupVersion, FileContent, VersionFile
from conftest import make_backup, make_version, finish_version, upload_file


def _done(client, label, key, files):
    make_version(client, label, key)
    for path, content in files.items():
        upload_file(client, label, key, path=path, content=content)
    finish_version(client, label, key)


def _drive(client, name="Docs"):
    r = client.post("/cloud/drives", json={"name": name})
    assert r.status_code == 200, r.text
    return r.json()["label"]


def _drive_put(client, label, path, content):
    r = client.put("/cloud/drive/upload", content=content, headers={
        "X-Label": label, "X-Path": base64.b64encode(path.encode()).decode(),
        "X-Mtime": str(1000 + len(content))})
    assert r.status_code == 200, r.text


def _reference(db):
    """As agregações do jeito antigo, direto de version_files."""
    refs, logical = db.query(
        func.count(VersionFile.id), func.coalesce(func.sum(FileContent.size), 0),
    ).join(FileContent, FileContent.sha256 == VersionFile.sha256).one()

    done = db.query(BackupVersion).filter(BackupVersion.status == "done").all()
    sizes = dict(db.query(VersionFile.version_id, func.sum(FileContent.size))
                 .join(FileContent, FileContent.sha256 == VersionFile.sha256)
                 .group_by(VersionFile.version_id).all())
    size_by_day = defaultdict(int)
    for v in done:
        size_by_day[v.created_at.strftime("%Y-%m-%d")] += int(sizes.get(v.id, 0))

    lag = version_diff.done_version_lag_sq(db)
    diffs = version_diff.diff_counts_by_version(db, db.query(lag).subquery())
    by_day = defaultdict(lambda: {"added": 0, "modified": 0, "removed": 0})
    for v in done:
        for k, n in diffs.get(v.id, {}).items():
            by_day[v.created_at.strftime("%Y-%m-%d")][k] += n
    return {"refs": int(refs), "logical": int(logical),
            "size_by_day": dict(size_by_day), "changes_by_day": dict(by_day)}


def _assert_stats_match_reference(client):
    data = client.get("/api/stats", params={"refresh": True}).json()
    db = m.SessionLocal()
    try:
        ref = _reference(db)
    finally:
        db.close()
    assert data["summaries_pending"] == 0
    assert data["total_file_refs"] == ref["refs"]
    assert data["logical_bytes"] == ref["logical"]
    assert {d["date"]: d["total_size_bytes"] for d in data["trend_days"]} == ref["size_by_day"]
    assert {d["date"]: {k: d[k] for k in ("added", "modified", "removed")}
            for d in data["changes_days"]} == ref["changes_by_day"]
    return data


def _scenario(client):
    make_backup(client, "b1")
    make_backup(client, "b2")
    _done(client, "b1", "v1", {"/a": b"AAAA", "/b": b"BB"})
    _done(client, "b1", "v2", {"/a": b"AAAA", "/b": b"BB-novo", "/c": b"C"})
    _done(client, "b1", "v3", {"/a": b"AAAA-2"})
    _done(client, "b2", "v1", {"/x": b"AAAA"})           # dedupe entre labels
    make_version(client, "b2", "v2")                      # em andamento: ao vivo
    upload_file(client, "b2", "v2", path="/y", content=b"YYY")
    make_version(client, "b2", "v0")
    upload_file(client, "b2", "v0", path="/z", content=b"falhou")
    finish_version(client, "b2", "v0", status="failed")
    drive = _drive(client)
    _drive_put(client, drive, "doc.txt", b"documento")
    return drive


def test_stats_dos_resumos_batem_com_a_agregacao_direta(client):
    drive = _scenario(client)
    _assert_stats_match_reference(client)

    # Mudanças que invalidam resumos: versão do meio apagada (o diff de v3 muda
    # de base), pasta do Cloud alterada e versão em andamento recebendo arquivo.
    assert client.delete("/backups/b1/versions/v2").status_code == 200
    _drive_put(client, drive, "doc.txt", b"documento alterado")
    _drive_put(client, drive, "novo.txt", b"n")
    upload_file(client, "b2", "v2", path="/y2", content=b"mais")
    _assert_stats_match_reference(client)


def test_resumo_gravado_e_refeito_quando_a_anterior_some(client):
    _scenario(client)
    client.get("/api/stats", params={"refresh": True})
    db = m.SessionLocal()
    try:
        ids = {v.version_key: v.id for v in
               db.query(BackupVersion).filter(BackupVersion.backup_label == "b1")}
        v3 = db.get(BackupVersion, ids["v3"])
        assert (v3.sum_status, v3.sum_prev_id) == ("done", ids["v2"])
        # running e a pasta do Cloud nunca recebem resumo
        running = (db.query(BackupVersion)
                   .filter(BackupVersion.backup_label == "b2", BackupVersion.version_key == "v2").one())
        assert running.sum_status is None
        srv = (db.query(BackupVersion).join(db_mod.BackupID,
               db_mod.BackupID.label == BackupVersion.backup_label)
               .filter(db_mod.BackupID.kind == db_mod.SERVER_KIND).all())
        assert srv and all(v.sum_status is None for v in srv)
    finally:
        db.close()

    client.delete("/backups/b1/versions/v2")
    client.get("/api/stats", params={"refresh": True})
    db = m.SessionLocal()
    try:
        v3 = db.get(BackupVersion, ids["v3"])
        assert v3.sum_prev_id == ids["v1"]
        assert (v3.sum_added, v3.sum_modified, v3.sum_removed) == (0, 1, 1)
    finally:
        db.close()


def test_pendentes_alem_do_limite_ficam_para_o_resumidor(client, monkeypatch):
    make_backup(client, "b1")
    _done(client, "b1", "v1", {"/a": b"AAAA"})
    monkeypatch.setattr(version_summary, "INLINE_MAX", 0)
    data = client.get("/api/stats", params={"refresh": True}).json()
    assert data["summaries_pending"] == 1
    assert data["logical_bytes"] == 0  # fora dos totais até ser resumida

    db = m.SessionLocal()
    try:
        assert version_summary.summarize_pending(db) == (1, 0)
    finally:
        db.close()
    data = client.get("/api/stats", params={"refresh": True}).json()
    assert data["summaries_pending"] == 0
    assert data["logical_bytes"] == 4


def test_thread_do_resumidor_resume_as_pendentes(client, monkeypatch):
    make_backup(client, "b1")
    _done(client, "b1", "v1", {"/a": b"AAAA"})
    s = version_summary.Summarizer()
    monkeypatch.setattr(s, "STARTUP_DELAY", 0)
    s.start(m.SessionLocal)
    try:
        import time
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            db = m.SessionLocal()
            try:
                if not version_summary.pending_ids(db):
                    break
            finally:
                db.close()
            time.sleep(0.05)
    finally:
        s.stop()
        s._thread.join(5)
    db = m.SessionLocal()
    try:
        v = db.query(BackupVersion).filter(BackupVersion.backup_label == "b1").one()
        assert (v.sum_status, v.sum_file_count, v.sum_logical_bytes) == ("done", 1, 4)
    finally:
        db.close()


# -- Painéis globais -----------------------------------------------------------

def _count_computes(monkeypatch, panel):
    calls = []
    orig = panel._compute

    def counting(db):
        calls.append(1)
        return orig(db)

    monkeypatch.setattr(panel, "_compute", counting)
    return calls


def test_painel_so_recalcula_quando_os_dados_mudam(client, monkeypatch):
    make_backup(client, "b1")
    _done(client, "b1", "v1", {"/a": b"AAAA"})
    calls = _count_computes(monkeypatch, stats_panels.PANELS["exclusive"])

    client.get("/api/stats")
    client.get("/api/stats")
    client.get("/api/stats", params={"refresh": True})
    assert len(calls) == 1  # nada mudou: nem o Recalcular geral refaz o painel

    _done(client, "b1", "v2", {"/b": b"B"})
    client.get("/api/stats")
    assert len(calls) == 2


def test_pasta_do_cloud_alterada_muda_a_impressao_digital(client):
    drive = _drive(client)
    _drive_put(client, drive, "a.txt", b"um")
    db = m.SessionLocal()
    try:
        fp1 = stats_panels.fingerprint(db)
    finally:
        db.close()
    _drive_put(client, drive, "a.txt", b"outro conteudo")
    db = m.SessionLocal()
    try:
        assert stats_panels.fingerprint(db) != fp1
    finally:
        db.close()


def test_painel_em_background_mostra_calculando(client, monkeypatch):
    make_backup(client, "a")
    _done(client, "a", "v1", {"/x": b"xyz"})
    monkeypatch.setattr(stats_panels, "SYNC", False)
    panel = stats_panels.PANELS["exclusive"]
    gate = threading.Event()
    orig = panel._compute

    def gated(db):
        gate.wait(10)
        return orig(db)

    monkeypatch.setattr(panel, "_compute", gated)

    first = client.get("/api/stats").json()
    assert first["panels"]["exclusive"]["computing"] is True
    assert first["panels"]["exclusive"]["age_seconds"] is None
    assert first["exclusive_by_label"] == []

    gate.set()
    assert panel._lock.acquire(timeout=10)
    panel._lock.release()
    after = client.get("/api/stats").json()
    assert after["panels"]["exclusive"]["computing"] is False
    assert after["panels"]["exclusive"]["age_seconds"] is not None
    assert [e["label"] for e in after["exclusive_by_label"]] == ["a"]


def test_refresh_de_painel(client):
    make_backup(client, "a")
    _done(client, "a", "v1", {"/x": b"xyz"})
    r = client.post("/api/stats/panels/exclusive/refresh")
    assert r.status_code == 200
    assert r.json()["age_seconds"] is not None  # SYNC nos testes: já calculado
    assert client.post("/api/stats/panels/nao-existe/refresh").status_code == 404


def test_refresh_de_painel_exige_admin(two_users):
    _admin, alice, _bob = two_users
    assert alice.post("/api/stats/panels/exclusive/refresh").status_code == 403
