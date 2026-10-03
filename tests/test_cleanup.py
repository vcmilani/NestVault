import logging
from pathlib import Path

import main as m
from database import MaintenanceJob

from conftest import make_backup, make_version, upload_file, finish_version


# -- POST /backups/{label}/cleanup --------------------------------------------

def test_cleanup_removes_old_versions(client):
    make_backup(client, "b1")
    for i in range(4):
        make_version(client, "b1", f"2026-0{i+1}-01T00:00:00")

    r = client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 2})
    assert r.status_code == 200
    data = r.json()
    assert data["kept"] == 2
    assert len(data["versions_removed"]) == 2

    versions = client.get("/backups/b1/versions").json()
    assert len(versions) == 2


def test_cleanup_keeps_most_recent(client):
    make_backup(client, "b1")
    keys = ["2026-01-01T00:00:00", "2026-02-01T00:00:00", "2026-03-01T00:00:00"]
    for k in keys:
        make_version(client, "b1", k)

    client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 1})
    versions = client.get("/backups/b1/versions").json()
    assert versions[0]["version_key"] == "2026-03-01T00:00:00"


def test_cleanup_keep_zero(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    r = client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 0})
    assert r.json()["versions_removed"] == ["v1"]
    assert client.get("/backups/b1/versions").json() == []


def test_cleanup_keep_more_than_existing(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    r = client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 10})
    assert r.json()["versions_removed"] == []


def test_cleanup_removes_orphan_storage(client, tmp_vol):
    """Cleanup de versão deve remover arquivo físico exclusivo da versão deletada."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")

    up = upload_file(client, "b1", "v1", path="/exclusive.txt", content=b"only in v1")
    sha = up["sha256"]
    dest = tmp_vol / "_content" / sha[:2] / sha
    assert dest.exists()

    # v2 tem arquivo diferente — v1 fica exclusivo
    upload_file(client, "b1", "v2", path="/other.txt", content=b"only in v2")

    client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 1})

    assert not dest.exists()


def test_cleanup_keeps_shared_storage(client, tmp_vol):
    """Conteúdo compartilhado entre versões não deve ser removido."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")

    up = upload_file(client, "b1", "v1", path="/shared.txt", content=b"shared")
    sha = up["sha256"]
    upload_file(client, "b1", "v2", path="/shared.txt", content=b"shared")

    client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 1})

    dest = tmp_vol / "_content" / sha[:2] / sha
    assert dest.exists()


# -- POST /maintenance/cleanup-orphans ----------------------------------------

def test_cleanup_orphans_removes_unreferenced(client, tmp_vol):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    up = upload_file(client, "b1", "v1", path="/file.txt", content=b"data")
    sha = up["sha256"]

    # Remove a versão, tornando o conteúdo órfão
    client.delete("/backups/b1/versions/v1")

    r = client.post("/maintenance/cleanup-orphans")
    assert r.status_code == 200
    assert r.json()["scheduled"] is True

    dest = tmp_vol / "_content" / sha[:2] / sha
    assert not dest.exists()


def test_cleanup_orphans_nothing_to_remove(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    upload_file(client, "b1", "v1", path="/file.txt", content=b"data")

    r = client.post("/maintenance/cleanup-orphans")
    assert r.json()["scheduled"] is True


def test_cleanup_orphans_bytes_freed(client, tmp_vol):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    content = b"x" * 200
    up = upload_file(client, "b1", "v1", path="/big.txt", content=content)
    sha = up["sha256"]
    client.delete("/backups/b1/versions/v1")

    r = client.post("/maintenance/cleanup-orphans")
    assert r.json()["scheduled"] is True

    dest = tmp_vol / "_content" / sha[:2] / sha
    assert not dest.exists()


# -- Cleanup assíncrono e escopo "todos os labels" (v9.3.1) -------------------
# O caminho de admin de /backups/{label}/cleanup era síncrono: as versões saíam do
# banco e, na mesma request, a limpeza de órfãos apagava os arquivos do disco —
# minutos de trabalho, que estouravam o timeout de 30 s do cliente CLI e o do
# navegador. Agora as duas etapas rodam na mesma background task da limpeza por
# data, e existe um endpoint de manutenção que aceita todos os labels de uma vez.

def test_cleanup_por_label_agenda_em_background(client):
    make_backup(client, "b1")
    for i in range(3):
        make_version(client, "b1", f"2026-0{i+1}-01T00:00:00")

    r = client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 1})
    assert r.status_code == 200
    d = r.json()
    # As versões removidas são conhecidas de imediato; o storage, não.
    assert d["scheduled"] is True
    assert d["storage_files_removed"] == 0
    assert sorted(d["versions_removed"]) == ["2026-01-01T00:00:00", "2026-02-01T00:00:00"]
    # A task já rodou (TestClient executa BackgroundTasks na resposta).
    assert [v["version_key"] for v in client.get("/backups/b1/versions").json()] == ["2026-03-01T00:00:00"]


def test_cleanup_por_label_registra_job_de_manutencao(client, tmp_vol):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")
    up = upload_file(client, "b1", "v1", path="/so-na-v1.txt", content=b"exclusivo da v1")
    dest = tmp_vol / "_content" / up["sha256"][:2] / up["sha256"]

    client.post("/backups/b1/cleanup", json={"backup_label": "b1", "keep": 1})

    db = m.SessionLocal()
    try:
        job = (db.query(MaintenanceJob)
                 .filter(MaintenanceJob.job_type == "cleanup-versions")
                 .order_by(MaintenanceJob.id.desc()).first())
        assert job is not None and job.status == "done"
        assert job.summary.startswith("label=b1: 1 versão(ões) removidas, 1 arquivo(s) liberados")
        assert job.bytes_freed > 0
    finally:
        db.close()
    assert not dest.exists()


def test_cleanup_versions_todos_os_labels_corta_label_a_label(client):
    make_backup(client, "b1")
    for i in range(3):
        make_version(client, "b1", f"2026-0{i+1}-01T00:00:00")
    make_backup(client, "b2")
    for i in range(2):
        make_version(client, "b2", f"2026-0{i+1}-01T00:00:00")

    r = client.post("/maintenance/cleanup-versions", params={"keep": 1})
    assert r.status_code == 200
    d = r.json()
    assert d["scheduled"] == 3
    assert {row["label"]: row["count"] for row in d["per_label"]} == {"b1": 2, "b2": 1}

    # Cada label fica com a sua mais recente — o corte é por label, não global.
    assert [v["version_key"] for v in client.get("/backups/b1/versions").json()] == ["2026-03-01T00:00:00"]
    assert [v["version_key"] for v in client.get("/backups/b2/versions").json()] == ["2026-02-01T00:00:00"]


def test_cleanup_versions_escopo_de_um_label_nao_toca_nos_outros(client):
    make_backup(client, "b1")
    for i in range(3):
        make_version(client, "b1", f"2026-0{i+1}-01T00:00:00")
    make_backup(client, "b2")
    for i in range(3):
        make_version(client, "b2", f"2026-0{i+1}-01T00:00:00")

    r = client.post("/maintenance/cleanup-versions", params={"keep": 1, "label": "b1"})
    assert r.json()["scheduled"] == 2
    assert len(client.get("/backups/b1/versions").json()) == 1
    assert len(client.get("/backups/b2/versions").json()) == 3


def test_cleanup_versions_nada_elegivel(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    r = client.post("/maintenance/cleanup-versions", params={"keep": 5})
    assert r.status_code == 200
    assert r.json() == {"scheduled": 0, "per_label": []}


def test_cleanup_versions_preview_conta_sem_remover(client):
    make_backup(client, "b1")
    for i in range(4):
        make_version(client, "b1", f"2026-0{i+1}-01T00:00:00")
    make_backup(client, "b2")
    make_version(client, "b2", "2026-01-01T00:00:00")

    r = client.get("/maintenance/cleanup-versions/preview", params={"keep": 2})
    assert r.status_code == 200
    d = r.json()
    assert d["total"] == 2
    assert {row["label"]: row["count"] for row in d["per_label"]} == {"b1": 2}

    # Preview não remove nada.
    assert len(client.get("/backups/b1/versions").json()) == 4


def test_cleanup_versions_keep_negativo_rejeitado(client):
    r = client.post("/maintenance/cleanup-versions", params={"keep": -1})
    assert r.status_code == 422
    r = client.get("/maintenance/cleanup-versions/preview", params={"keep": -1})
    assert r.status_code == 422


def test_cleanup_versions_exige_admin(two_users):
    _admin, alice, _bob = two_users
    assert alice.post("/maintenance/cleanup-versions", params={"keep": 0}).status_code == 403
    assert alice.get("/maintenance/cleanup-versions/preview", params={"keep": 0}).status_code == 403


def test_cleanup_de_usuario_comum_continua_sincrono_na_lixeira(two_users):
    """O caminho não-admin só marca linhas — é rápido e não vira background."""
    _admin, alice, _bob = two_users
    r = alice.post("/backups", json={"label": "alice-docs", "client_name": "pc"})
    assert r.status_code == 200
    alice.post("/backups/alice-docs/versions", json={"backup_label": "alice-docs", "version_key": "v1"})
    alice.post("/backups/alice-docs/versions", json={"backup_label": "alice-docs", "version_key": "v2"})

    r = alice.post("/backups/alice-docs/cleanup", json={"backup_label": "alice-docs", "keep": 1})
    assert r.status_code == 200
    d = r.json()
    assert d["trashed"] is True
    assert d["scheduled"] is False
    assert d["versions_removed"] == ["v1"]


def test_cleanup_versions_loga_as_duas_etapas(client, caplog):
    """Mesma task da limpeza por data, então o log tem o prefixo do job_type."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")
    upload_file(client, "b1", "v1", path="/so-na-v1.txt", content=b"exclusivo da v1")

    with caplog.at_level(logging.DEBUG, logger="backup-server"):
        client.post("/maintenance/cleanup-versions", params={"keep": 1})

    msgs = [rec.getMessage() for rec in caplog.records]
    assert any("[bg-cleanup-versions] etapa 1/2 lote" in msg and "b1/v1" in msg for msg in msgs), msgs
    assert any("[bg-cleanup-versions] etapa 2/2 lote" in msg for msg in msgs), msgs
    assert any("[bg-cleanup-versions] removido " in msg for msg in msgs), msgs
