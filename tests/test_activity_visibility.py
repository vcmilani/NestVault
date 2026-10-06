"""Execuções que precisam aparecer na tela de Atividade (MaintenanceJob ou versão
com finished_at) e registrar erro com motivo — regressões da revisão de
visibilidade/logs (v9.3.2)."""
import pytest

import cloud.rclone_runner as rr
import database as db_mod
import main as m
import storage as storage_mod
from database import BackupVersion, MaintenanceJob

from conftest import make_backup, make_version
from test_rclone_walk import session_factory, _make_job  # noqa: F401
from test_verified_writes import env, _seed_plain  # noqa: F401


def _jobs(Session, job_type=None):
    db = Session()
    try:
        q = db.query(MaintenanceJob)
        if job_type:
            q = q.filter(MaintenanceJob.job_type == job_type)
        return [(j.status, j.summary) for j in q.order_by(MaintenanceJob.id).all()]
    finally:
        db.close()


def _session(client):
    return m.SessionLocal


# -- Versões interrompidas -----------------------------------------------------

def test_versao_running_substituida_aparece_na_atividade(client):
    """create_version marca a running anterior como incomplete — com finished_at,
    senão ela nunca entra em recent_versions (filtro por finished_at)."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")

    recent = client.get("/api/activity").json()["recent_versions"]
    v1 = [v for v in recent if v["version_key"] == "v1"]
    assert v1 and v1[0]["status"] == "incomplete"


@pytest.mark.asyncio
async def test_rclone_supersede_grava_finished_at(session_factory):
    Session = session_factory
    db = Session()
    db.add(db_mod.BackupID(label="fotos", client_name="rclone"))
    db.add(BackupVersion(backup_label="fotos", version_key="k1", status="running"))
    db.commit()
    assert rr._supersede_running_versions(db, "fotos") == 1
    db.commit()
    v = db.query(BackupVersion).filter_by(version_key="k1").one()
    assert v.status == "incomplete" and v.finished_at is not None
    db.close()


# -- rclone sem versão ------------------------------------------------------------

@pytest.mark.asyncio
async def test_rclone_pulado_por_remote_ocupado_aparece_na_atividade(session_factory, monkeypatch):
    Session = session_factory
    jid = _make_job(Session)
    monkeypatch.setattr(rr, "is_remote_busy", lambda name: True)
    await rr.run_rclone_backup_job(jid)
    [(status, summary)] = _jobs(Session, "rclone-backup")
    assert status == "skipped" and "em andamento no remote" in summary


@pytest.mark.asyncio
async def test_rclone_falha_antes_da_versao_aparece_na_atividade(session_factory, monkeypatch):
    Session = session_factory
    jid = _make_job(Session)

    async def boom(remote_name):
        raise RuntimeError("rclone config quebrada")
    monkeypatch.setattr(rr, "_remote_config", boom)
    await rr.run_rclone_backup_job(jid)
    [(status, summary)] = _jobs(Session, "rclone-backup")
    assert status == "error" and "rclone config quebrada" in summary


# -- Re-replicação ao recuperar volume ----------------------------------------------

def test_rereplicate_to_volume_registra_job(env, monkeypatch):
    Session, v1, v2 = env
    monkeypatch.setattr(db_mod, "SessionLocal", Session)
    _seed_plain(Session, v1, b"fine")
    storage_mod.rereplicate_to_volume(v2)
    [(status, summary)] = _jobs(Session, "rereplicate")
    assert status == "done" and "1 re-replicado(s)" in summary


def test_rereplicate_to_volume_erro_nao_some(env, monkeypatch):
    """Rodava num Future descartado: a exceção não chegava a lugar nenhum."""
    Session, v1, v2 = env
    monkeypatch.setattr(db_mod, "SessionLocal", Session)

    def boom():
        raise RuntimeError("falha simulada")
    monkeypatch.setattr(storage_mod, "target_replicas", boom)
    storage_mod.rereplicate_to_volume(v2)  # não pode lançar
    [(status, summary)] = _jobs(Session, "rereplicate")
    assert status == "error" and "falha simulada" in summary


# -- Limpeza automática ---------------------------------------------------------

def test_auto_cleanup_sem_pressao_nao_cria_job(client):
    db = _session(client)()
    try:
        assert m._auto_cleanup_with_job(db) == (None, 0)
    finally:
        db.close()
    assert _jobs(_session(client), "auto-cleanup") == []


def test_auto_cleanup_erro_fica_registrado(client, monkeypatch):
    monkeypatch.setattr(m, "_volumes_with_free_space", lambda: 0)

    def boom(db, exclude_version_id=None):
        raise RuntimeError("disco sumiu")
    monkeypatch.setattr(m, "_auto_cleanup_if_needed", boom)
    db = _session(client)()
    try:
        with pytest.raises(RuntimeError):
            m._auto_cleanup_with_job(db)
    finally:
        db.close()
    [(status, summary)] = _jobs(_session(client), "auto-cleanup")
    assert status == "error" and "disco sumiu" in summary


def test_auto_cleanup_sucesso_fecha_job(client, monkeypatch):
    monkeypatch.setattr(m, "_volumes_with_free_space", lambda: 0)
    monkeypatch.setattr(m, "_auto_cleanup_if_needed",
                        lambda db, exclude_version_id=None: ("2 versão(ões) removida(s)", 10))
    db = _session(client)()
    try:
        m._auto_cleanup_with_job(db)
    finally:
        db.close()
    assert _jobs(_session(client), "auto-cleanup") == [("done", "2 versão(ões) removida(s)")]


# -- Manutenções síncronas e status de erro --------------------------------------

def test_rereplicate_endpoint_erro_fica_registrado(client, monkeypatch):
    def boom(db):
        raise RuntimeError("falha na réplica")
    monkeypatch.setattr(m, "_rereplicate_all", boom)
    with pytest.raises(RuntimeError):
        client.post("/maintenance/rereplicate")
    [(status, summary)] = _jobs(_session(client), "rereplicate")
    assert status == "error" and "falha na réplica" in summary


def test_bulk_delete_erro_usa_status_error_com_motivo(client, monkeypatch):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    db = _session(client)()
    vid = db.query(BackupVersion.id).filter_by(version_key="v1").scalar()
    db.close()

    def boom(*a, **kw):
        raise RuntimeError("storage indisponível")
    monkeypatch.setattr(m, "_cleanup_orphan_contents_no_commit", boom)
    with pytest.raises(RuntimeError):
        m._bg_bulk_delete_versions([(vid, "b1", "v1")], "b1", "cleanup-versions")
    [(status, summary)] = _jobs(_session(client), "cleanup-versions")
    assert status == "error"
    assert "storage indisponível" in summary and "parou em" in summary


def test_stats_conta_failed_legado_como_erro(client):
    db = _session(client)()
    db.add(MaintenanceJob(job_type="disk-migration", status="failed"))
    db.add(MaintenanceJob(job_type="disk-migration", status="error"))
    db.commit()
    db.close()
    by_type = {r["job_type"]: r for r in client.get("/api/stats").json()["maintenance_by_type"]}
    assert by_type["disk-migration"]["error_count"] == 2
