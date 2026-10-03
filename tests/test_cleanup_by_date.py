"""Testa /maintenance/cleanup-by-date(/preview) — em particular a validação do
parâmetro `before` (N5): um valor malformado deve virar 400, não 500."""
import logging
import re
from pathlib import Path
from unittest import mock

import main as m
import nightly_cleanup
from database import BackupVersion, FileContent, MaintenanceJob, VersionFile

from conftest import make_backup, make_version, finish_version, upload_file


def test_cleanup_by_date_preview_invalid_before_returns_400(client):
    r = client.get("/maintenance/cleanup-by-date/preview", params={"before": "not-a-date"})
    assert r.status_code == 400


def test_cleanup_by_date_invalid_before_returns_400(client):
    r = client.post("/maintenance/cleanup-by-date", params={"before": "not-a-date"})
    assert r.status_code == 400


def test_cleanup_by_date_preview_valid_before(client):
    make_backup(client, "lbl")
    make_version(client, "lbl", "2020-01-01T00:00:00")
    finish_version(client, "lbl", "2020-01-01T00:00:00")

    r = client.get("/maintenance/cleanup-by-date/preview", params={"before": "2026-01-01T00:00:00"})
    assert r.status_code == 200


# -- Progresso e log das duas etapas (v9.3.1) --------------------------------
# A exclusão por data roda em duas etapas: a 1 apaga linhas, a 2 apaga arquivos do
# disco. Até a v9.3.0 só a 1 reportava, então o job ficava em "100%" durante toda a
# etapa 2 e não havia log nenhum do que estava sendo removido.

def _label_with_two_done_versions(client):
    """Duas versões done no mesmo label, com conteúdo próprio cada uma.

    A mais nova é a `done` mais recente e portanto preservada; a antiga é a que a
    limpeza remove, e o conteúdo dela fica órfão (conteúdos distintos, sem dedup)."""
    make_backup(client, "lbl")
    make_version(client, "lbl", "2020-01-01T00:00:00")
    upload_file(client, "lbl", "2020-01-01T00:00:00", path="/antigo.txt", content=b"conteudo antigo")
    finish_version(client, "lbl", "2020-01-01T00:00:00")

    make_version(client, "lbl", "2020-06-01T00:00:00")
    upload_file(client, "lbl", "2020-06-01T00:00:00", path="/novo.txt", content=b"conteudo novo")
    finish_version(client, "lbl", "2020-06-01T00:00:00")


def test_cleanup_by_date_loga_as_duas_etapas(client, caplog):
    """Etapa 1 nomeia label/version_key; etapa 2 reporta lote e path de cada arquivo."""
    _label_with_two_done_versions(client)

    with caplog.at_level(logging.DEBUG, logger="backup-server"):
        r = client.post("/maintenance/cleanup-by-date", params={"before": "2099-01-01T00:00:00"})
    assert r.status_code == 200
    assert r.json()["scheduled"] == 1

    msgs = [rec.getMessage() for rec in caplog.records]

    # Etapa 1: a versão removida aparece como label/version_key.
    etapa1 = [m for m in msgs if "etapa 1/2 lote" in m]
    assert etapa1, msgs
    assert "lbl/2020-01-01T00:00:00" in etapa1[0]

    # Etapa 2: linha de lote em INFO + path de cada arquivo em DEBUG.
    assert any("etapa 2/2 lote" in m for m in msgs), msgs
    removidos = [m for m in msgs if "[bg-cleanup-by-date] removido " in m]
    assert removidos, msgs
    assert any(rec.levelno == logging.DEBUG for rec in caplog.records
               if "[bg-cleanup-by-date] removido " in rec.getMessage())


def test_cleanup_by_date_etapa2_reporta_progresso_no_summary(client):
    """A etapa 2 atualiza o summary — era ela que deixava o job parado em 100%."""
    _label_with_two_done_versions(client)

    summaries = []
    real_invalidate = m.invalidate_activity

    def _spy():
        db = m.SessionLocal()
        try:
            job = (db.query(MaintenanceJob)
                     .filter(MaintenanceJob.job_type == "cleanup-by-date")
                     .order_by(MaintenanceJob.id.desc()).first())
            if job and job.summary:
                summaries.append(job.summary)
        finally:
            db.close()
        real_invalidate()

    with mock.patch.object(m, "invalidate_activity", _spy):
        r = client.post("/maintenance/cleanup-by-date", params={"before": "2099-01-01T00:00:00"})
    assert r.status_code == 200

    assert any(s.startswith("Etapa 1/2 — removendo versões:") for s in summaries), summaries
    assert any(s.startswith("Etapa 2/2 — liberando arquivos:") for s in summaries), summaries


def test_cleanup_by_date_summary_final_mantem_formato(client):
    """O backfill _RE_SUMMARY_MB de database.py faz regex sobre esse texto."""
    _label_with_two_done_versions(client)
    r = client.post("/maintenance/cleanup-by-date", params={"before": "2099-01-01T00:00:00"})
    assert r.status_code == 200

    db = m.SessionLocal()
    try:
        job = (db.query(MaintenanceJob)
                 .filter(MaintenanceJob.job_type == "cleanup-by-date")
                 .order_by(MaintenanceJob.id.desc()).first())
        assert job.status == "done"
        assert re.match(r"^1 versão\(ões\) removidas, 1 arquivo\(s\) liberados \([\d.]+ MB\)$", job.summary), job.summary
        assert job.bytes_freed > 0
    finally:
        db.close()


def test_cleanup_orphan_contents_chama_on_delete_so_no_que_saiu_do_disco(client):
    """O callback recebe um path por arquivo físico removido — e nada do conteúdo
    que ainda tem referência viva."""
    _label_with_two_done_versions(client)

    db = m.SessionLocal()
    try:
        antigo = (db.query(VersionFile)
                    .join(BackupVersion, VersionFile.version_id == BackupVersion.id)
                    .filter(BackupVersion.version_key == "2020-01-01T00:00:00").one())
        novo = (db.query(VersionFile)
                  .join(BackupVersion, VersionFile.version_id == BackupVersion.id)
                  .filter(BackupVersion.version_key == "2020-06-01T00:00:00").one())
        sha_orfao, sha_vivo = antigo.sha256, novo.sha256
        path_orfao = db.get(FileContent, sha_orfao).stored_at
        assert Path(path_orfao).exists()

        # Órfã o conteúdo antigo removendo só a referência dele.
        db.query(VersionFile).filter(VersionFile.sha256 == sha_orfao).delete(synchronize_session=False)
        db.commit()

        vistos = []
        removed, freed = nightly_cleanup._cleanup_orphan_contents(
            db, on_delete=lambda sha, path, size: vistos.append((sha, path, size))
        )
        db.commit()

        assert removed == 1
        assert [v[0] for v in vistos] == [sha_orfao]
        assert vistos[0][1] == path_orfao
        assert vistos[0][2] == freed > 0
        assert not Path(path_orfao).exists()
        assert db.get(FileContent, sha_vivo) is not None
    finally:
        db.close()


def test_cleanup_orphan_contents_nao_chama_on_delete_se_arquivo_ja_sumiu(client):
    """FileNotFoundError no unlink não é uma deleção — o callback não dispara."""
    _label_with_two_done_versions(client)

    db = m.SessionLocal()
    try:
        vf = (db.query(VersionFile)
                .join(BackupVersion, VersionFile.version_id == BackupVersion.id)
                .filter(BackupVersion.version_key == "2020-01-01T00:00:00").one())
        sha = vf.sha256
        Path(db.get(FileContent, sha).stored_at).unlink()  # arquivo já não está no disco

        db.query(VersionFile).filter(VersionFile.sha256 == sha).delete(synchronize_session=False)
        db.commit()

        vistos = []
        removed, _ = nightly_cleanup._cleanup_orphan_contents(
            db, on_delete=lambda *a: vistos.append(a)
        )
        db.commit()

        assert removed == 1          # a linha sai do banco
        assert vistos == []          # mas nenhum arquivo foi removido do disco
    finally:
        db.close()
