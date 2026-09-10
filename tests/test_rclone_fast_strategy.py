"""Testa o caminho rápido do rclone_runner (listagem recursiva única + download em
lote) — em particular o status final da versão quando o job termina com progresso
parcial (N2): a versão não pode virar 'done' com a maioria dos arquivos ausentes,
senão ela vira baseline de retenção e de skip-por-mtime do próximo run, que passaria
a pular (por mtime "igual") justamente os arquivos que nunca chegaram a ser baixados.

Mocka list_files_recursive e _bulk_copy — o resto do pipeline (_download_batch,
_producer/_consumer, hash, dedupe/store/registro) roda de verdade contra um volume
de storage temporário e um banco de teste (sqlite in-memory).
"""
import pytest

import cloud.rclone_runner as rr
from cloud.rclone_runner import RcloneFileEntry
from database import BackupVersion, RcloneBackupJob, VersionFile

from test_rclone_walk import session_factory, _make_job  # noqa: F401 — reaproveita os fixtures


def _fe(path, mtime=1.0, size=10):
    return RcloneFileEntry(path=path, size=size, mtime=mtime)


def _install_fast_mocks(monkeypatch, files, fail_prefixes=frozenset(),
                        fail_reason=None):
    """files: lista de RcloneFileEntry retornada por list_files_recursive.
    fail_prefixes: paths com esse prefixo não chegam ao staging — mesmo
    mecanismo de falha usado nos testes do walk.
    fail_reason=None simula o caso "sumiu do staging sem linha ERROR" (rc=0),
    que é como o rclone pula atalho órfão/arquivo sem permissão; com
    fail_reason, devolve rc=1 + linha ERROR (falha de verdade, re-tentável)."""
    async def fake_cfg(remote_name):
        return {"type": "onedrive"}  # backend "rápido" — não decide walk

    async def fake_list_files_recursive(remote_name, remote_path, **kw):
        return files

    async def fake_bulk_copy(remote_name, remote_path, files_from, staging,
                             *, ignore_size=False):
        paths = [p for p in files_from.read_text().splitlines() if p]
        failed = []
        for p in paths:
            if any(p.startswith(pref) for pref in fail_prefixes):
                failed.append(p)
                continue  # arquivo não chega ao staging — falha
            dest = staging / p
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"x" * 10)
        if failed and fail_reason:
            err = "\n".join(
                f"2026/09/09 00:26:13 ERROR : {p}: {fail_reason}" for p in failed
            )
            return 1, err
        return 0, ""

    monkeypatch.setattr(rr, "_remote_config", fake_cfg)
    monkeypatch.setattr(rr, "list_files_recursive", fake_list_files_recursive)
    monkeypatch.setattr(rr, "_bulk_copy", fake_bulk_copy)


def _get_version(Session, label):
    db = Session()
    try:
        return (
            db.query(BackupVersion)
            .filter(BackupVersion.backup_label == label)
            .order_by(BackupVersion.created_at.desc())
            .first()
        )
    finally:
        db.close()


@pytest.mark.asyncio
async def test_fast_strategy_partial_failure_marks_incomplete_not_done(session_factory, monkeypatch):
    """Regressão (N2): 1 de 3 arquivos baixado, 2 falham — a versão deve ficar
    'incomplete' (resumível), nunca 'done'."""
    Session = session_factory
    jid = _make_job(Session, remote_path="")
    _install_fast_mocks(
        monkeypatch,
        files=[_fe("ok/a.txt"), _fe("bad/b.txt"), _fe("bad/c.txt")],
        fail_prefixes={"bad/"},
        fail_reason="Failed to copy: 421 Misdirected Request",
    )

    await rr.run_rclone_backup_job(jid)

    version = _get_version(Session, "fotos")
    assert version is not None
    assert version.status == "incomplete"


@pytest.mark.asyncio
async def test_fast_strategy_total_failure_marks_failed(session_factory, monkeypatch):
    """Nenhum arquivo processado com sucesso: continua 'failed', como antes."""
    Session = session_factory
    jid = _make_job(Session, remote_path="")
    _install_fast_mocks(
        monkeypatch,
        files=[_fe("bad/a.txt"), _fe("bad/b.txt")],
        fail_prefixes={"bad/"},
        fail_reason="Failed to copy: 421 Misdirected Request",
    )

    await rr.run_rclone_backup_job(jid)

    version = _get_version(Session, "fotos")
    assert version is not None
    assert version.status == "failed"


@pytest.mark.asyncio
async def test_fast_strategy_full_success_marks_done(session_factory, monkeypatch):
    """Sem nenhum erro, o comportamento de sempre é preservado: 'done'."""
    Session = session_factory
    jid = _make_job(Session, remote_path="")
    _install_fast_mocks(
        monkeypatch,
        files=[_fe("ok/a.txt"), _fe("ok/b.txt")],
        fail_prefixes=set(),
    )

    await rr.run_rclone_backup_job(jid)

    version = _get_version(Session, "fotos")
    assert version is not None
    assert version.status == "done"


@pytest.mark.asyncio
async def test_fast_strategy_permission_failure_marks_done(session_factory, monkeypatch):
    """Arquivos sem permissão/atalho órfão (pulados pelo rclone, sem linha ERROR)
    não impedem 'done': nenhum retry os traria, e prender a versão em
    'incomplete' só faria o run seguinte repetir a mesma falha para sempre."""
    Session = session_factory
    jid = _make_job(Session, remote_path="")
    _install_fast_mocks(
        monkeypatch,
        files=[_fe("ok/a.txt"), _fe("bad/b.txt"), _fe("bad/c.txt")],
        fail_prefixes={"bad/"},   # sem fail_reason — falha permanente
    )

    await rr.run_rclone_backup_job(jid)

    version = _get_version(Session, "fotos")
    assert version is not None
    assert version.status == "done"

    db = Session()
    try:
        paths = {
            vf.original_path
            for vf in db.query(VersionFile).filter_by(version_id=version.id)
        }
        assert paths == {"ok/a.txt"}   # os inacessíveis ficam de fora da versão
        job = db.get(RcloneBackupJob, jid)
        assert job.last_run_status == "success"
        assert "ignorado(s) sem permissão/atalho" in job.last_run_message
    finally:
        db.close()
