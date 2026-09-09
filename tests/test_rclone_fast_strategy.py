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
from database import BackupVersion

from test_rclone_walk import session_factory, _make_job  # noqa: F401 — reaproveita os fixtures


def _fe(path, mtime=1.0, size=10):
    return RcloneFileEntry(path=path, size=size, mtime=mtime)


def _install_fast_mocks(monkeypatch, files, fail_prefixes=frozenset()):
    """files: lista de RcloneFileEntry retornada por list_files_recursive.
    fail_prefixes: paths com esse prefixo "não são baixados pelo rclone" (staged
    ausente) — mesmo mecanismo de falha usado nos testes do walk."""
    async def fake_cfg(remote_name):
        return {"type": "onedrive"}  # backend "rápido" — não decide walk

    async def fake_list_files_recursive(remote_name, remote_path, **kw):
        return files

    async def fake_bulk_copy(remote_name, remote_path, files_from, staging,
                             *, ignore_size=False):
        paths = [p for p in files_from.read_text().splitlines() if p]
        for p in paths:
            if any(p.startswith(pref) for pref in fail_prefixes):
                continue  # simula "não baixado pelo rclone" — arquivo falha
            dest = staging / p
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"x" * 10)
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
