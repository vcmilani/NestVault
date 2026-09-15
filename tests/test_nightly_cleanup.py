from datetime import datetime, time as dt_time, timedelta

from database import BackupVersion

from conftest import make_backup, make_version, finish_version, upload_file
from nightly_cleanup import (
    _version_fingerprint,
    _prune_unchanged_versions,
    _versions_to_keep,
    run_nightly_cleanup,
)


def _mkver(db, label, key, created_at, status="done"):
    v = BackupVersion(backup_label=label, version_key=key, status=status, created_at=created_at)
    db.add(v)
    db.commit()
    db.refresh(v)
    return v


def _mkfile(db, version_id, path, sha256, mtime=1000.0):
    from database import VersionFile, FileContent

    if not db.query(FileContent).filter(FileContent.sha256 == sha256).first():
        db.add(FileContent(sha256=sha256, stored_at=f"/dev/null/{sha256}", size=1))
        db.commit()
    db.add(VersionFile(version_id=version_id, original_path=path, sha256=sha256, mtime=mtime))
    db.commit()


# -- _prune_unchanged_versions -------------------------------------------------

def test_prune_three_identical_versions_keeps_first_and_last(client):
    import main as m

    make_backup(client, "lbl")
    Session = m.SessionLocal
    db = Session()
    try:
        base = datetime(2026, 1, 1, 0, 0, 0)
        v1 = _mkver(db, "lbl", "k1", base)
        v2 = _mkver(db, "lbl", "k2", base + timedelta(hours=1))
        v3 = _mkver(db, "lbl", "k3", base + timedelta(hours=2))
        for v in (v1, v2, v3):
            _mkfile(db, v.id, "/a.txt", "s" * 64)

        to_delete = _prune_unchanged_versions(db, [v1, v2, v3])
        assert to_delete == [v2.id]
    finally:
        db.close()


def test_prune_two_blocks_keeps_first_of_each_and_last(client):
    import main as m

    make_backup(client, "lbl")
    Session = m.SessionLocal
    db = Session()
    try:
        base = datetime(2026, 1, 1, 0, 0, 0)
        vA1 = _mkver(db, "lbl", "kA1", base)
        vA2 = _mkver(db, "lbl", "kA2", base + timedelta(hours=1))
        vB1 = _mkver(db, "lbl", "kB1", base + timedelta(hours=2))
        vB2 = _mkver(db, "lbl", "kB2", base + timedelta(hours=3))
        vB3 = _mkver(db, "lbl", "kB3", base + timedelta(hours=4))
        for v in (vA1, vA2):
            _mkfile(db, v.id, "/a.txt", "a" * 64)
        for v in (vB1, vB2, vB3):
            _mkfile(db, v.id, "/a.txt", "b" * 64)

        to_delete = set(_prune_unchanged_versions(db, [vA1, vA2, vB1, vB2, vB3]))
        assert to_delete == {vA2.id, vB2.id}
    finally:
        db.close()


def test_prune_no_change_when_content_differs(client):
    import main as m

    make_backup(client, "lbl")
    Session = m.SessionLocal
    db = Session()
    try:
        base = datetime(2026, 1, 1, 0, 0, 0)
        v1 = _mkver(db, "lbl", "k1", base)
        v2 = _mkver(db, "lbl", "k2", base + timedelta(hours=1))
        _mkfile(db, v1.id, "/a.txt", "a" * 64)
        _mkfile(db, v2.id, "/a.txt", "b" * 64)

        assert _prune_unchanged_versions(db, [v1, v2]) == []
    finally:
        db.close()


def test_prune_no_change_when_file_deleted(client):
    import main as m

    make_backup(client, "lbl")
    Session = m.SessionLocal
    db = Session()
    try:
        base = datetime(2026, 1, 1, 0, 0, 0)
        v1 = _mkver(db, "lbl", "k1", base)
        v2 = _mkver(db, "lbl", "k2", base + timedelta(hours=1))
        _mkfile(db, v1.id, "/a.txt", "a" * 64)
        # v2 tem zero arquivos (deletado) -> fingerprint diferente

        assert _prune_unchanged_versions(db, [v1, v2]) == []
    finally:
        db.close()


def test_prune_empty_versions_middle_pruned_but_not_after_full(client):
    import main as m

    make_backup(client, "lbl")
    Session = m.SessionLocal
    db = Session()
    try:
        base = datetime(2026, 1, 1, 0, 0, 0)
        v1 = _mkver(db, "lbl", "k1", base)  # vazia
        v2 = _mkver(db, "lbl", "k2", base + timedelta(hours=1))  # vazia (igual v1)
        v3 = _mkver(db, "lbl", "k3", base + timedelta(hours=2))  # com arquivo (mudou)

        _mkfile(db, v3.id, "/a.txt", "a" * 64)

        to_delete = _prune_unchanged_versions(db, [v1, v2, v3])
        assert to_delete == [v2.id]
    finally:
        db.close()


def test_prune_single_version_untouched(client):
    import main as m

    make_backup(client, "lbl")
    Session = m.SessionLocal
    db = Session()
    try:
        v1 = _mkver(db, "lbl", "k1", datetime(2026, 1, 1))
        _mkfile(db, v1.id, "/a.txt", "a" * 64)
        assert _prune_unchanged_versions(db, [v1]) == []
    finally:
        db.close()


def test_version_fingerprint_matches_for_identical_content(client):
    import main as m

    make_backup(client, "lbl")
    Session = m.SessionLocal
    db = Session()
    try:
        v1 = _mkver(db, "lbl", "k1", datetime(2026, 1, 1))
        v2 = _mkver(db, "lbl", "k2", datetime(2026, 1, 2))
        _mkfile(db, v1.id, "/a.txt", "a" * 64)
        _mkfile(db, v2.id, "/a.txt", "a" * 64)

        assert _version_fingerprint(db, v1.id) == _version_fingerprint(db, v2.id)
    finally:
        db.close()


# -- _versions_to_keep (regressão das faixas existentes) ----------------------

def test_versions_to_keep_ranges():
    now = datetime(2026, 6, 15, 12, 0, 0)

    v_recent = BackupVersion(id=1, created_at=now - timedelta(hours=2))
    v_3days_a = BackupVersion(id=2, created_at=now - timedelta(days=3, hours=1))
    v_3days_b = BackupVersion(id=3, created_at=now - timedelta(days=3, hours=5))
    v_60days = BackupVersion(id=4, created_at=now - timedelta(days=60))
    v_60days_same_week = BackupVersion(id=5, created_at=now - timedelta(days=60, hours=3))
    v_200days = BackupVersion(id=6, created_at=now - timedelta(days=200))
    v_200days_same_month = BackupVersion(id=7, created_at=now - timedelta(days=201))

    keep = _versions_to_keep(
        [v_recent, v_3days_a, v_3days_b, v_60days, v_60days_same_week, v_200days, v_200days_same_month],
        now,
    )

    assert v_recent.id in keep
    # dentro de 1-30 dias: só a mais recente do dia calendário sobrevive
    assert v_3days_a.id in keep
    assert v_3days_b.id not in keep
    # dentro de 30-180 dias: 1 por semana ISO -> só a mais recente
    assert v_60days.id in keep
    assert v_60days_same_week.id not in keep
    # acima de 180 dias: 1 por mês -> só a mais recente
    assert v_200days.id in keep
    assert v_200days_same_month.id not in keep


# -- Fim a fim: run_nightly_cleanup -------------------------------------------

def test_run_nightly_cleanup_prunes_unchanged_versions(client, monkeypatch):
    import main as m
    import nightly_cleanup as nc

    # nightly_cleanup importa SessionLocal/engine direto de database.py — o fixture
    # `client` só troca m.SessionLocal, então sem isto a chamada abaixo vazaria
    # para o banco real (./backup.db) em vez do banco de teste em memória.
    monkeypatch.setattr(nc, "SessionLocal", m.SessionLocal)
    monkeypatch.setattr(nc, "engine", m.SessionLocal.kw["bind"])

    make_backup(client, "lbl")
    Session = m.SessionLocal
    db = Session()
    try:
        now = datetime.now()
        # 3 versões idênticas recentes (dentro de 24h) + 1 versão que muda
        v1 = _mkver(db, "lbl", "k1", now - timedelta(hours=3))
        v2 = _mkver(db, "lbl", "k2", now - timedelta(hours=2))
        v3 = _mkver(db, "lbl", "k3", now - timedelta(hours=1))
        for v in (v1, v2, v3):
            _mkfile(db, v.id, "/a.txt", "a" * 64)
    finally:
        db.close()

    run_nightly_cleanup()

    db = Session()
    try:
        remaining = {
            v.version_key
            for v in db.query(BackupVersion).filter(BackupVersion.backup_label == "lbl").all()
        }
        assert remaining == {"k1", "k3"}

        mj = (
            db.query(m.MaintenanceJob)
            .filter(m.MaintenanceJob.job_type == "nightly-cleanup")
            .order_by(m.MaintenanceJob.id.desc())
            .first()
        )
        assert mj is not None
        assert "sem alteração" in mj.summary
    finally:
        db.close()


def test_run_nightly_cleanup_combines_retention_and_prune_in_same_run(client, monkeypatch):
    """Regressão: quando a retenção já apaga alguma versão done (done_to_delete
    não vazio) e a poda de sem-alteração roda em seguida no mesmo label, acessar
    atributos de uma versão recém-deletada explode com ObjectDeletedError (o
    commit() de _delete_versions expira todos os objetos da sessão)."""
    import main as m
    import nightly_cleanup as nc

    monkeypatch.setattr(nc, "SessionLocal", m.SessionLocal)
    monkeypatch.setattr(nc, "engine", m.SessionLocal.kw["bind"])

    make_backup(client, "lbl")
    Session = m.SessionLocal
    db = Session()
    try:
        now = datetime.now()
        # Ancorado ao meio-dia da data alvo, não a `now - 12h`: a retenção agrupa por
        # dia de calendário, então com `now` de madrugada as duas versões caíam em dias
        # diferentes e o teste falhava entre 00h e 12h — justamente quando a limpeza roda.
        day10 = datetime.combine((now - timedelta(days=10)).date(), dt_time(12, 0))
        # Mesmo dia calendário, 10 dias atrás: retenção mantém só a mais recente do dia.
        v1 = _mkver(db, "lbl", "k1", day10)
        v2 = _mkver(db, "lbl", "k2", day10 + timedelta(hours=2))
        # Dia seguinte, conteúdo igual a v2: sobrevive à retenção, mas é podada por igualdade.
        v3 = _mkver(db, "lbl", "k3", day10 + timedelta(days=1))
        # Recente, conteúdo diferente: sempre mantida (última do label).
        v4 = _mkver(db, "lbl", "k4", now - timedelta(hours=1))
        for v in (v1, v2, v3):
            _mkfile(db, v.id, "/a.txt", "a" * 64)
        _mkfile(db, v4.id, "/a.txt", "b" * 64)
    finally:
        db.close()

    run_nightly_cleanup()  # não deve levantar ObjectDeletedError

    db = Session()
    try:
        remaining = {
            v.version_key
            for v in db.query(BackupVersion).filter(BackupVersion.backup_label == "lbl").all()
        }
        assert remaining == {"k2", "k4"}
    finally:
        db.close()


# -- Limpeza de versões stale (failed/incomplete) -----------------------------
# Regra: uma versão failed/incomplete é removida assim que existir uma done MAIS NOVA
# que ela — sem exigência de idade. A última falha do label (sem done posterior) é
# sempre preservada: é ela que descreve o estado atual e da qual o rclone retoma.

def test_stale_version_removed_immediately_when_newer_done_exists(client, monkeypatch):
    import main as m
    import nightly_cleanup as nc

    # run_nightly_cleanup() abre sua própria sessão via nightly_cleanup.SessionLocal —
    # só main.SessionLocal é patchado pelo fixture `client`, então sem isso a limpeza
    # rodaria contra o banco de produção em vez do banco in-memory do teste.
    monkeypatch.setattr(nc, "SessionLocal", m.SessionLocal)
    monkeypatch.setattr(nc, "engine", m.SessionLocal.kw["bind"])

    make_backup(client, "lbl")
    Session = m.SessionLocal
    db = Session()
    try:
        now = datetime.now()
        # Falhou há 1 hora e já foi sucedida por uma done: sai na mesma limpeza.
        _mkver(db, "lbl", "kfail", now - timedelta(hours=2), status="failed")
        _mkver(db, "lbl", "kdone", now - timedelta(hours=1), status="done")
    finally:
        db.close()

    run_nightly_cleanup()

    db = Session()
    try:
        remaining = {
            v.version_key
            for v in db.query(BackupVersion).filter(BackupVersion.backup_label == "lbl").all()
        }
        assert "kfail" not in remaining, "failed com done mais nova deveria sair imediatamente"
        assert "kdone" in remaining
    finally:
        db.close()


def test_stale_version_removed_when_older_than_newest_done(client, monkeypatch):
    import main as m
    import nightly_cleanup as nc

    monkeypatch.setattr(nc, "SessionLocal", m.SessionLocal)
    monkeypatch.setattr(nc, "engine", m.SessionLocal.kw["bind"])

    make_backup(client, "lbl")
    Session = m.SessionLocal
    db = Session()
    try:
        now = datetime.now()
        failed = _mkver(db, "lbl", "kfail", now - timedelta(days=8), status="failed")
        _mkver(db, "lbl", "kdone", now, status="done")
    finally:
        db.close()

    run_nightly_cleanup()

    db = Session()
    try:
        remaining = {
            v.version_key
            for v in db.query(BackupVersion).filter(BackupVersion.backup_label == "lbl").all()
        }
        assert "kfail" not in remaining, "versão failed anterior à done mais nova deveria ser removida"
        assert "kdone" in remaining
    finally:
        db.close()


def test_latest_stale_version_kept_when_no_newer_done(client, monkeypatch):
    """Trava de segurança da regra: a falha mais recente do label — sem nenhuma done
    depois dela — descreve o estado atual do backup e é de onde o rclone retoma
    (progress_json). Nunca é removida, por mais antiga que seja."""
    import main as m
    import nightly_cleanup as nc

    monkeypatch.setattr(nc, "SessionLocal", m.SessionLocal)
    monkeypatch.setattr(nc, "engine", m.SessionLocal.kw["bind"])

    make_backup(client, "lbl")
    Session = m.SessionLocal
    db = Session()
    try:
        now = datetime.now()
        _mkver(db, "lbl", "kdone", now - timedelta(days=40), status="done")
        # Incompleta mais nova que a única done: é o estado corrente do label.
        _mkver(db, "lbl", "kinc", now - timedelta(days=30), status="incomplete")
    finally:
        db.close()

    run_nightly_cleanup()

    db = Session()
    try:
        remaining = {
            v.version_key
            for v in db.query(BackupVersion).filter(BackupVersion.backup_label == "lbl").all()
        }
        assert "kinc" in remaining, "última incompleta sem done posterior não pode ser removida"
    finally:
        db.close()
