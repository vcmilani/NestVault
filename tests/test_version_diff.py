"""Equivalência dos diffs de versão (added/modified/removed).

Os três consumidores — /api/activity (_build_historical_data), /api/stats
(_build_stats_data.changes_days) e o digest diário (_version_diff) — precisam
concordar sobre o mesmo cenário. Estes testes fixam esse contrato ANTES da
troca do diff em Python pela contagem em SQL, para provar que a reescrita não
muda nenhum número.
"""
import daily_digest
import main as m
from database import BackupVersion
from conftest import make_backup, make_version, upload_file, finish_version


def _done_version(client, label, key, files):
    """Cria a versão, sobe os arquivos e finaliza como done.

    Finaliza ANTES de criar a próxima: create_version transiciona a versão
    running anterior do mesmo label para 'incomplete', e uma versão incomplete
    não entra no cálculo de diff (só 'done' tem predecessora).
    """
    make_version(client, label, key)
    for path, content in files.items():
        upload_file(client, label, key, path=path, content=content)
    finish_version(client, label, key, status="done")


def _setup_mixed(client, label="b1"):
    """v1 → v2 com exatamente 1 adicionado, 1 modificado, 1 removido, 1 inalterado."""
    make_backup(client, label)
    _done_version(client, label, "v1", {
        "/kept.txt":    b"same",
        "/changed.txt": b"old",
        "/gone.txt":    b"deleted",
    })
    _done_version(client, label, "v2", {
        "/kept.txt":    b"same",
        "/changed.txt": b"new",
        "/fresh.txt":   b"added",
    })
    return label


def _diff_by_key(recent_versions):
    return {v.version_key: (v.diff_added, v.diff_modified, v.diff_removed)
            for v in recent_versions}


# -- _build_historical_data (/api/activity) -----------------------------------

def test_historical_diff_counts_mixed(client):
    _setup_mixed(client)
    db = m.SessionLocal()
    try:
        recent, _ = m._build_historical_data(db)
    finally:
        db.close()

    diffs = _diff_by_key(recent)
    assert diffs["v2"] == (1, 1, 1)
    # v1 não tem predecessora done: tudo conta como adicionado.
    assert diffs["v1"] == (3, 0, 0)


def test_historical_diff_first_version_counts_everything_as_added(client):
    make_backup(client, "b1")
    _done_version(client, "b1", "v1", {"/a.txt": b"a", "/b.txt": b"b"})

    db = m.SessionLocal()
    try:
        recent, _ = m._build_historical_data(db)
    finally:
        db.close()

    assert _diff_by_key(recent)["v1"] == (2, 0, 0)


def test_historical_diff_empty_version(client):
    """Versão done sem nenhum arquivo: todos os contadores zerados, e a
    predecessora inteira aparece como removida na versão seguinte."""
    make_backup(client, "b1")
    _done_version(client, "b1", "v1", {"/a.txt": b"a"})
    _done_version(client, "b1", "v2", {})

    db = m.SessionLocal()
    try:
        recent, _ = m._build_historical_data(db)
    finally:
        db.close()

    diffs = _diff_by_key(recent)
    assert diffs["v1"] == (1, 0, 0)
    assert diffs["v2"] == (0, 0, 1)


def test_historical_diff_is_per_label(client):
    """A predecessora tem que ser do MESMO label — labels distintos não se misturam."""
    _setup_mixed(client, "b1")
    make_backup(client, "b2")
    _done_version(client, "b2", "v1", {"/only.txt": b"x"})

    db = m.SessionLocal()
    try:
        recent, _ = m._build_historical_data(db)
    finally:
        db.close()

    by_label = {(v.backup_label, v.version_key): (v.diff_added, v.diff_modified, v.diff_removed)
                for v in recent}
    assert by_label[("b2", "v1")] == (1, 0, 0)
    assert by_label[("b1", "v2")] == (1, 1, 1)


def test_activity_endpoint_exposes_same_diff(client):
    _setup_mixed(client)
    r = client.get("/api/activity")
    assert r.status_code == 200
    by_key = {v["version_key"]: v for v in r.json()["recent_versions"]}
    assert (by_key["v2"]["diff_added"],
            by_key["v2"]["diff_modified"],
            by_key["v2"]["diff_removed"]) == (1, 1, 1)


# -- _build_stats_data.changes_days (/api/stats) ------------------------------

def test_stats_changes_days_matches_historical(client):
    _setup_mixed(client)
    db = m.SessionLocal()
    try:
        stats = m._build_stats_data(db)
    finally:
        db.close()

    # v1 e v2 caem no mesmo dia (criadas agora): 3+1 adicionados, 0+1 modificados,
    # 0+1 removidos.
    assert len(stats.changes_days) == 1
    day = stats.changes_days[0]
    assert (day.added, day.modified, day.removed) == (4, 1, 1)


def test_stats_changes_days_empty_without_versions(client):
    make_backup(client, "b1")
    db = m.SessionLocal()
    try:
        stats = m._build_stats_data(db)
    finally:
        db.close()
    assert stats.changes_days == []


# -- daily_digest._version_diff -----------------------------------------------

def test_digest_version_diff_matches_historical(client):
    label = _setup_mixed(client)
    db = m.SessionLocal()
    try:
        v2 = (db.query(BackupVersion)
              .filter(BackupVersion.backup_label == label, BackupVersion.version_key == "v2")
              .one())
        assert daily_digest._version_diff(db, v2) == {
            "added": 1, "modified": 1, "removed": 1, "total": 3,
        }

        v1 = (db.query(BackupVersion)
              .filter(BackupVersion.backup_label == label, BackupVersion.version_key == "v1")
              .one())
        assert daily_digest._version_diff(db, v1) == {
            "added": 3, "modified": 0, "removed": 0, "total": 3,
        }
    finally:
        db.close()
