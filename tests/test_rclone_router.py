"""Rotas /rclone/jobs — cancelamento de job travado."""
import database as db_mod
import main as m
from cloud import rclone_runner


def _make_job(client, remote="gdrive"):
    r = client.post("/rclone/jobs", json={
        "remote_name": remote, "display_name": "Drive", "target_label": "drive",
    })
    assert r.status_code == 201
    return r.json()["id"]


def _set_status(job_id, status):
    db = m.SessionLocal()
    try:
        db.get(db_mod.RcloneBackupJob, job_id).last_run_status = status
        db.commit()
    finally:
        db.close()


def test_cancel_resets_stuck_running_job(client):
    """Regressão: o endpoint consultava `_job_locks`, que não existe mais — todo
    cancelamento terminava em NameError (500)."""
    job_id = _make_job(client)
    _set_status(job_id, "running")

    r = client.post(f"/rclone/jobs/{job_id}/cancel")
    assert r.status_code == 200
    assert r.json()["last_run_status"] == "error"
    assert r.json()["last_run_message"] == "Cancelado manualmente"


def test_cancel_refuses_job_actually_running(client, monkeypatch):
    job_id = _make_job(client)
    _set_status(job_id, "running")
    monkeypatch.setattr(rclone_runner, "_running_jobs", {job_id})

    assert client.post(f"/rclone/jobs/{job_id}/cancel").status_code == 409


def test_cancel_allowed_when_other_job_holds_the_same_remote(client, monkeypatch):
    """Outro job no mesmo remote não impede cancelar este, que está só travado."""
    stuck = _make_job(client, remote="gdrive")
    other = _make_job(client, remote="gdrive")
    _set_status(stuck, "running")
    monkeypatch.setattr(rclone_runner, "_busy_remotes", {"gdrive"})
    monkeypatch.setattr(rclone_runner, "_running_jobs", {other})

    assert client.post(f"/rclone/jobs/{stuck}/cancel").status_code == 200
