"""Testa as defesas específicas do backend iCloud no rclone_runner:

- packages do macOS (.pages/.numbers/.playgroundbook/.xcodeproj), em que a API
  reporta o tamanho descompactado do bundle mas entrega o zip — o rclone acusa
  "corrupted on transfer: sizes differ" e descarta o arquivo, o que impedia a
  versão de fechar como 'done';
- propagação do motivo real do stderr do rclone para `errors` (antes qualquer
  falha virava "não baixado pelo rclone (verifique permissão/atalho)");
- serialização por remote, que evita dois processos rclone reautenticando em
  paralelo e invalidando os cookies um do outro ("Invalid global session");
- backoff maior no retry quando o erro é de sessão;
- listagem recursiva parcial: um 421 no meio da varredura não pode
  descartar tudo que o rclone já tinha listado.

Mesmo padrão dos demais testes de rclone: mocka _bulk_copy/list_files_recursive
e deixa o resto do pipeline rodar de verdade.
"""
import asyncio

import pytest

import cloud.rclone_runner as rr
from cloud.rclone_runner import RcloneFileEntry
from database import BackupVersion

from test_rclone_walk import session_factory, _make_job  # noqa: F401 — reaproveita os fixtures


def _fe(path, mtime=1.0, size=10):
    return RcloneFileEntry(path=path, size=size, mtime=mtime)


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


@pytest.fixture(autouse=True)
def _clean_busy_remotes():
    """O set de remotes ocupados é global ao módulo — isola cada teste."""
    rr._busy_remotes.clear()
    yield
    rr._busy_remotes.clear()


# ---------------------------------------------------------------------------
# Parser do stderr
# ---------------------------------------------------------------------------

def test_parse_rclone_errors_strips_partial_suffix():
    """O rclone reporta o arquivo temporário (<nome>.<hex>.partial); o erro
    precisa casar com o path real do entry."""
    stderr = (
        "2026/09/09 00:26:13 ERROR : Playgrounds/Gráfico.playgroundbook.f49f2d34.partial: "
        "corrupted on transfer: sizes differ src() 1152540 vs dst(Local file system at /x) 401659"
    )
    reasons = rr._parse_rclone_errors(stderr)
    assert "Playgrounds/Gráfico.playgroundbook" in reasons
    assert "sizes differ" in reasons["Playgrounds/Gráfico.playgroundbook"]


def test_parse_rclone_errors_ignores_attempt_aggregate():
    """'Attempt N/M failed with X errors' é um agregado do run, não um arquivo."""
    stderr = (
        "2026/09/09 00:26:15 ERROR : Attempt 3/3 failed with 4 errors and: "
        "corrupted on transfer: sizes differ src() 289274 vs dst(...) 162943\n"
        "2026/09/09 00:26:15 ERROR : Numbers/Calculo Salário.numbers.4929b98d.partial: "
        "corrupted on transfer: sizes differ src() 143298 vs dst(...) 105505"
    )
    reasons = rr._parse_rclone_errors(stderr)
    assert list(reasons) == ["Numbers/Calculo Salário.numbers"]


def test_parse_rclone_errors_captures_listing_session_error():
    """Erro de listagem sem sufixo .partial (o 421 do iCloud) também é capturado."""
    stderr = (
        "2026/09/09 00:21:05 ERROR : Love/Maitê/Saúde/Geneticista/Exames: error listing: "
        'HTTP error 421 (421 Misdirected Request) returned body: "{\\"reason\\":\\"Invalid '
        'global session\\",\\"error\\":2}"'
    )
    reasons = rr._parse_rclone_errors(stderr)
    assert "Love/Maitê/Saúde/Geneticista/Exames" in reasons
    assert "421" in reasons["Love/Maitê/Saúde/Geneticista/Exames"]


def test_parse_rclone_errors_truncates_long_reason():
    stderr = "2026/09/09 00:26:13 ERROR : a/b.txt: " + "x" * 500
    reason = rr._parse_rclone_errors(stderr)["a/b.txt"]
    assert len(reason) == rr._MAX_REASON_LEN + 1  # + reticências


# ---------------------------------------------------------------------------
# Retry com --ignore-size (packages do iCloud)
# ---------------------------------------------------------------------------

def _install_icloud_mocks(monkeypatch, files, sizes_differ_paths=frozenset()):
    """sizes_differ_paths: paths que só são entregues pelo rclone quando
    --ignore-size está presente — reproduz o comportamento dos packages."""
    calls = []

    async def fake_cfg(remote_name):
        return {"type": "iclouddrive", "service": "drive"}

    async def fake_list_files_recursive(remote_name, remote_path, **kw):
        return files

    async def fake_bulk_copy(remote_name, remote_path, files_from, staging,
                             *, ignore_size=False):
        calls.append(ignore_size)
        paths = [p for p in files_from.read_text().splitlines() if p]
        failed = []
        for p in paths:
            if p in sizes_differ_paths and not ignore_size:
                failed.append(p)
                continue
            dest = staging / p
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"x" * 10)
        if failed:
            stderr = "\n".join(
                f"2026/09/09 00:26:13 ERROR : {p}.f49f2d34.partial: corrupted on "
                f"transfer: sizes differ src() 289274 vs dst(Local file system at /x) 162943"
                for p in failed
            )
            return 1, stderr
        return 0, ""

    monkeypatch.setattr(rr, "_remote_config", fake_cfg)
    monkeypatch.setattr(rr, "list_files_recursive", fake_list_files_recursive)
    monkeypatch.setattr(rr, "_bulk_copy", fake_bulk_copy)
    return calls


@pytest.mark.asyncio
async def test_package_retried_with_ignore_size_and_version_is_done(session_factory, monkeypatch):
    """Regressão: os 4 packages que travavam o job em 'incomplete' agora são
    baixados no retry com --ignore-size, e a versão fecha como 'done'."""
    Session = session_factory
    jid = _make_job(Session, remote_path="")
    calls = _install_icloud_mocks(
        monkeypatch,
        files=[_fe("ok/a.txt"), _fe("Numbers/Calculo Salário.numbers")],
        sizes_differ_paths={"Numbers/Calculo Salário.numbers"},
    )

    await rr.run_rclone_backup_job(jid)

    assert calls == [False, True], "esperado 1 retry, com --ignore-size"
    version = _get_version(Session, "fotos")
    assert version.status == "done"


@pytest.mark.asyncio
async def test_no_retry_when_error_is_not_sizes_differ(session_factory, monkeypatch):
    """Sem 'sizes differ' no stderr, a verificação de tamanho continua valendo:
    nada de retry silencioso relaxando integridade."""
    Session = session_factory
    jid = _make_job(Session, remote_path="")
    calls = []

    async def fake_cfg(remote_name):
        return {"type": "iclouddrive", "service": "drive"}

    async def fake_list_files_recursive(remote_name, remote_path, **kw):
        return [_fe("bad/a.txt")]

    async def fake_bulk_copy(remote_name, remote_path, files_from, staging,
                             *, ignore_size=False):
        calls.append(ignore_size)
        return 1, "2026/09/09 00:26:13 ERROR : bad/a.txt: permission denied"

    monkeypatch.setattr(rr, "_remote_config", fake_cfg)
    monkeypatch.setattr(rr, "list_files_recursive", fake_list_files_recursive)
    monkeypatch.setattr(rr, "_bulk_copy", fake_bulk_copy)

    await rr.run_rclone_backup_job(jid)

    assert calls == [False], "não deve retentar quando o erro não é de tamanho"


@pytest.mark.asyncio
async def test_real_reason_reaches_errors_not_generic_message(session_factory, monkeypatch):
    """O motivo do rclone chega ao last_run_message, em vez do genérico
    'não baixado pelo rclone (verifique permissão/atalho)'."""
    Session = session_factory
    jid = _make_job(Session, remote_path="")

    async def fake_cfg(remote_name):
        return {"type": "iclouddrive", "service": "drive"}

    async def fake_list_files_recursive(remote_name, remote_path, **kw):
        return [_fe("ok/a.txt"), _fe("bad/b.txt")]

    async def fake_bulk_copy(remote_name, remote_path, files_from, staging,
                             *, ignore_size=False):
        dest = staging / "ok/a.txt"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"x" * 10)
        return 1, "2026/09/09 00:21:05 ERROR : bad/b.txt: error listing: HTTP error 421"

    monkeypatch.setattr(rr, "_remote_config", fake_cfg)
    monkeypatch.setattr(rr, "list_files_recursive", fake_list_files_recursive)
    monkeypatch.setattr(rr, "_bulk_copy", fake_bulk_copy)

    await rr.run_rclone_backup_job(jid)

    db = Session()
    try:
        from database import RcloneBackupJob
        msg = db.get(RcloneBackupJob, jid).last_run_message
    finally:
        db.close()
    assert "421" in msg
    assert "verifique permissão/atalho" not in msg


# ---------------------------------------------------------------------------
# Serialização por remote
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_second_run_on_same_remote_is_skipped(session_factory, monkeypatch):
    """Dois runs concorrentes no mesmo remote: o segundo é descartado sem criar
    uma segunda BackupVersion (evita duas sessões rclone no mesmo rclone.conf)."""
    Session = session_factory
    jid = _make_job(Session, remote_path="")
    started = asyncio.Event()
    release = asyncio.Event()

    async def fake_cfg(remote_name):
        return {"type": "iclouddrive", "service": "drive"}

    async def fake_list_files_recursive(remote_name, remote_path, **kw):
        started.set()
        await release.wait()
        return [_fe("ok/a.txt")]

    async def fake_bulk_copy(remote_name, remote_path, files_from, staging,
                             *, ignore_size=False):
        dest = staging / "ok/a.txt"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"x" * 10)
        return 0, ""

    monkeypatch.setattr(rr, "_remote_config", fake_cfg)
    monkeypatch.setattr(rr, "list_files_recursive", fake_list_files_recursive)
    monkeypatch.setattr(rr, "_bulk_copy", fake_bulk_copy)

    first = asyncio.create_task(rr.run_rclone_backup_job(jid))
    await started.wait()

    assert rr.is_remote_busy("victor_icloud_photos")
    await rr.run_rclone_backup_job(jid)   # segundo disparo, durante o primeiro

    release.set()
    await first

    db = Session()
    try:
        assert db.query(BackupVersion).count() == 1
    finally:
        db.close()
    assert not rr.is_remote_busy("victor_icloud_photos"), "slot deve ser liberado no fim"


@pytest.mark.asyncio
async def test_slot_released_on_failure(session_factory, monkeypatch):
    """Um run que estoura exceção não pode deixar o remote marcado como ocupado
    para sempre — senão todos os runs seguintes seriam descartados."""
    Session = session_factory
    jid = _make_job(Session, remote_path="")

    async def fake_cfg(remote_name):
        raise RuntimeError("rclone config dump falhou")

    monkeypatch.setattr(rr, "_remote_config", fake_cfg)

    await rr.run_rclone_backup_job(jid)

    assert not rr.is_remote_busy("victor_icloud_photos")


# ---------------------------------------------------------------------------
# Backoff em erro de sessão
# ---------------------------------------------------------------------------

def test_is_session_error_recognizes_real_421():
    msg = (
        'error listing: HTTP error 421 (421 Misdirected Request) returned body: '
        '"{\\"reason\\":\\"Invalid global session\\",\\"error\\":2}"'
    )
    assert rr._is_session_error(msg)


@pytest.mark.parametrize("msg", [
    "directory not found",
    "corrupted on transfer: sizes differ src() 289274 vs dst() 162943",
])
def test_is_session_error_ignores_unrelated(msg):
    assert not rr._is_session_error(msg)


@pytest.mark.asyncio
async def test_session_error_uses_longer_backoff(monkeypatch):
    """Erro de sessão espera mais que os 10s padrão — o backend precisa de tempo
    para reautenticar antes que o retry faça sentido."""
    slept = []

    async def fake_sleep(d):
        slept.append(d)

    monkeypatch.setattr(rr.asyncio, "sleep", fake_sleep)

    await rr._sleep_before_retry(1, "HTTP error 421 Invalid global session", "lsjson")
    await rr._sleep_before_retry(2, "HTTP error 421 Invalid global session", "lsjson")
    await rr._sleep_before_retry(1, "directory not found", "lsjson")

    assert slept == [*rr._SESSION_RETRY_BACKOFF, 10]


# ---------------------------------------------------------------------------
# Listagem parcial (421 no meio da varredura recursiva)
# ---------------------------------------------------------------------------

_LSJSON_OK = (
    b'[\n'
    b'{"Path":"a/f1.txt","Name":"f1.txt","Size":3,"ModTime":"2026-09-09T08:00:00Z","IsDir":false},\n'
    b'{"Path":"z/f2.txt","Name":"f2.txt","Size":4,"ModTime":"2026-09-09T08:00:00Z","IsDir":false}\n'
    b']\n'
)
# Stderr real do job 8: o rclone aborta em 2 diretórios mas fecha o array JSON
# com tudo que já tinha listado.
_LSJSON_421 = (
    b'2026/09/09 08:14:13 ERROR : Documents/x: error listing: HTTP error 421 '
    b'(421 Misdirected Request) returned body: "{\\"reason\\":\\"Invalid global session\\"}"\n'
    b'2026/09/09 08:14:21 NOTICE: Failed to lsjson with 2 errors: last error was: '
    b'error in ListJSON: HTTP error 421 (421 Misdirected Request)'
)


def _fake_lsjson(monkeypatch, runs):
    """Mocka _run_lsjson devolvendo `runs` em sequência (repete o último)."""
    calls = []

    async def fake(*args, **kw):
        calls.append(args)
        return runs[min(len(calls) - 1, len(runs) - 1)]

    monkeypatch.setattr(rr, "_run_lsjson", fake)

    async def no_sleep(*a, **kw):
        pass

    monkeypatch.setattr(rr, "_sleep_before_retry", no_sleep)
    return calls


@pytest.mark.asyncio
async def test_partial_listing_is_used_and_reported(monkeypatch):
    """Erro em algumas pastas não pode matar o job inteiro: aproveita o que o
    rclone listou e registra o motivo, para a versão fechar 'incomplete'."""
    calls = _fake_lsjson(monkeypatch, [(_LSJSON_OK, _LSJSON_421, 1)])
    errors: list[str] = []

    files = await rr.list_files_recursive("victor_icloud", "", errors=errors)

    assert [f.path for f in files] == ["a/f1.txt", "z/f2.txt"]
    assert len(calls) == 3          # só aceita o parcial depois de esgotar os retries
    assert len(errors) == 1 and errors[0].startswith("listagem incompleta:")
    assert "421" in errors[0]


@pytest.mark.asyncio
async def test_partial_listing_keeps_most_complete_attempt(monkeypatch):
    """Com a sessão morrendo, a retry pode voltar vazia — vale a melhor tentativa."""
    _fake_lsjson(monkeypatch, [
        (_LSJSON_OK, _LSJSON_421, 1),
        (b"[]", _LSJSON_421, 1),
    ])
    files = await rr.list_files_recursive("victor_icloud", "")
    assert len(files) == 2


@pytest.mark.asyncio
async def test_listing_without_any_output_raises_with_reconnect_hint(monkeypatch):
    """Sem nada listado não há backup possível — a mensagem tem de dizer o que fazer."""
    _fake_lsjson(monkeypatch, [(b"", _LSJSON_421, 1)])

    with pytest.raises(RuntimeError) as exc:
        await rr.list_files_recursive("victor_icloud", "")

    assert "rclone config reconnect victor_icloud:" in str(exc.value)


@pytest.mark.asyncio
async def test_truncated_stdout_does_not_break_parsing(monkeypatch):
    """JSON cortado no meio não pode virar JSONDecodeError solto."""
    _fake_lsjson(monkeypatch, [(b'[{"Path":"a"', _LSJSON_421, 1)])

    with pytest.raises(RuntimeError, match="rclone lsjson falhou"):
        await rr.list_files_recursive("victor_icloud", "")


def test_lsjson_failure_hint_only_for_session_errors():
    assert "reconnect" not in rr._lsjson_failure(1, "directory not found", "onedrive")
