"""A Session do job rclone não pode ficar com transação aberta durante os trechos
longos (config do remote, listagem, downloads).

No PostgreSQL, uma transação aberta por horas fica "idle in transaction"; com
idle_in_transaction_session_timeout o servidor derruba a conexão e o job falha
no commit final com "SSL connection has been closed unexpectedly".
"""
import pytest

import cloud.rclone_runner as rr

from test_rclone_walk import session_factory, _make_job, _install_tree, _fe  # noqa: F401
from test_rclone_fast_strategy import _install_fast_mocks, _get_version


def _track_runner_session(monkeypatch, Session):
    """A primeira Session aberta pelo job é a do event loop (run_rclone_backup_job)."""
    created = []

    def factory(*a, **kw):
        s = Session(*a, **kw)
        created.append(s)
        return s

    monkeypatch.setattr(rr, "SessionLocal", factory)
    return created


def _wrap_checking(monkeypatch, name, created, seen):
    orig = getattr(rr, name)

    async def wrapper(*a, **kw):
        seen.append((name, created[0].in_transaction()))
        return await orig(*a, **kw)

    monkeypatch.setattr(rr, name, wrapper)


@pytest.mark.asyncio
async def test_fast_strategy_sem_transacao_aberta_nos_awaits_longos(session_factory, monkeypatch):
    Session = session_factory
    jid = _make_job(Session, remote_path="")
    _install_fast_mocks(monkeypatch, files=[_fe("ok/a.txt"), _fe("ok/b.txt")])
    created = _track_runner_session(monkeypatch, Session)
    seen = []
    for name in ("_remote_config", "list_files_recursive", "_bulk_copy"):
        _wrap_checking(monkeypatch, name, created, seen)

    await rr.run_rclone_backup_job(jid)

    assert _get_version(Session, "fotos").status == "done"
    assert {n for n, _ in seen} == {"_remote_config", "list_files_recursive", "_bulk_copy"}
    assert [n for n, in_txn in seen if in_txn] == []


@pytest.mark.asyncio
async def test_walk_sem_transacao_aberta_nos_awaits_longos(session_factory, monkeypatch):
    Session = session_factory
    jid = _make_job(Session)
    tree = {
        "": ([], ["PrimarySync"]),
        "PrimarySync": ([_fe("PrimarySync/a.jpg"), _fe("PrimarySync/b.jpg")], []),
    }
    _install_tree(monkeypatch, tree, [])
    created = _track_runner_session(monkeypatch, Session)
    seen = []
    for name in ("_remote_config", "list_dir_one_level", "_bulk_copy"):
        _wrap_checking(monkeypatch, name, created, seen)

    await rr.run_rclone_backup_job(jid)

    assert _get_version(Session, "fotos").status == "done"
    assert {n for n, _ in seen} == {"_remote_config", "list_dir_one_level", "_bulk_copy"}
    assert [n for n, in_txn in seen if in_txn] == []
