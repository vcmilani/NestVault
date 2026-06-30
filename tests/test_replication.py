import base64

import database as db_mod
import main as m
import storage as storage_mod
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


# -- Helpers ------------------------------------------------------------------

def _make_engine():
    return create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


def _mk_client(monkeypatch, volumes, replication_factor=2):
    engine = _make_engine()
    db_mod.Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)

    monkeypatch.setattr(m, "STORAGE_VOLUMES", volumes)
    monkeypatch.setattr(m, "STORAGE_DIR", volumes[0])
    monkeypatch.setattr(m, "REPLICATION_FACTOR", replication_factor)
    # As funções de storage.py leem os globais do próprio módulo storage — é
    # preciso propagar os patches para lá, não só para os aliases em main.
    monkeypatch.setattr(storage_mod, "STORAGE_VOLUMES", volumes)
    monkeypatch.setattr(storage_mod, "STORAGE_DIR", volumes[0])
    monkeypatch.setattr(storage_mod, "REPLICATION_FACTOR", replication_factor)

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    m.app.dependency_overrides[db_mod.get_db] = override_get_db
    with TestClient(m.app) as c:
        yield c
    m.app.dependency_overrides.clear()


def _enc(path: str) -> str:
    return base64.b64encode(path.encode()).decode()


def _upload(client, label, version_key, path, content):
    r = client.post(
        "/upload",
        content=content,
        headers={
            "X-Backup-Label": label,
            "X-Version-Key": version_key,
            "X-Original-Path": _enc(path),
            "X-Mtime": "1.0",
        },
    )
    assert r.status_code == 200
    return r.json()


def _copies_in(vol, sha):
    return list((vol / "_content").rglob(sha)) if (vol / "_content").exists() else []


# -- Upload replication -------------------------------------------------------

def test_upload_replicates_to_both_volumes(tmp_path, monkeypatch):
    """Com REPLICATION_FACTOR=2, o arquivo deve ficar fisicamente em ambos os volumes."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        r = _upload(c, "b1", "v1", "/file.txt", b"replicated content")
        sha = r["sha256"]

        assert len(_copies_in(v1, sha)) == 1, "cópia não encontrada em v1"
        assert len(_copies_in(v2, sha)) == 1, "cópia não encontrada em v2"


def test_upload_factor_1_single_copy(tmp_path, monkeypatch):
    """REPLICATION_FACTOR=1 (padrão) não replica — arquivo fica em apenas 1 volume."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=1):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        r = _upload(c, "b1", "v1", "/file.txt", b"single copy")
        sha = r["sha256"]

        total = len(_copies_in(v1, sha)) + len(_copies_in(v2, sha))
        assert total == 1, "deve existir exatamente 1 cópia física"


def test_upload_factor_0_mirrors_to_all_volumes(tmp_path, monkeypatch):
    """REPLICATION_FACTOR=0 espelha para todos os volumes."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()
    v3 = tmp_path / "v3"; v3.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2, v3], replication_factor=0):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        r = _upload(c, "b1", "v1", "/file.txt", b"mirror all")
        sha = r["sha256"]

        for v in [v1, v2, v3]:
            assert len(_copies_in(v, sha)) == 1, f"cópia não encontrada em {v}"


def test_upload_skips_degraded_volume(tmp_path, monkeypatch):
    """Upload com REPLICATION_FACTOR=2 e v1 degraded — escreve só em v2."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        m._degraded_volumes.add(v1)
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        r = _upload(c, "b1", "v1", "/file.txt", b"only healthy")
        sha = r["sha256"]

        assert _copies_in(v1, sha) == [], "nenhuma cópia deve estar em v1 degraded"
        assert len(_copies_in(v2, sha)) == 1


def test_upload_existing_content_replicates_to_new_volume(tmp_path, monkeypatch):
    """Conteúdo já existente é replicado para volume que não o tinha."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        m._degraded_volumes.add(v2)  # v2 fora durante o primeiro upload

        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        r = _upload(c, "b1", "v1", "/a.txt", b"shared content")
        sha = r["sha256"]

        # v2 se recupera
        m._degraded_volumes.discard(v2)

        # Segundo upload com mesmo conteúdo dispara a replicação para v2
        _upload(c, "b1", "v1", "/b.txt", b"shared content")

        assert len(_copies_in(v1, sha)) == 1
        assert len(_copies_in(v2, sha)) == 1


# -- Download fallback --------------------------------------------------------

def test_download_falls_back_to_replica(tmp_path, monkeypatch):
    """Se a cópia primária está ausente, download serve via réplica."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        r = _upload(c, "b1", "v1", "/file.txt", b"resilient content")
        sha = r["sha256"]
        file_id = r["file_id"]

        # Remove o arquivo físico de v1 (se estiver lá)
        p = v1 / "_content" / sha[:2] / sha
        if p.exists():
            p.unlink()

        resp = c.get(f"/files/{file_id}/download")
        assert resp.status_code == 200
        assert resp.content == b"resilient content"


def test_download_503_when_all_copies_in_degraded_volumes(tmp_path, monkeypatch):
    """503 quando todas as cópias estão em volumes degraded."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        r = _upload(c, "b1", "v1", "/file.txt", b"blocked")
        file_id = r["file_id"]

        m._degraded_volumes.add(v1)
        m._degraded_volumes.add(v2)

        resp = c.get(f"/files/{file_id}/download")
        assert resp.status_code == 503


def test_download_410_no_copies_in_db(tmp_path, monkeypatch):
    """410 quando o arquivo não possui nenhuma cópia registrada."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        r = _upload(c, "b1", "v1", "/file.txt", b"will vanish")
        sha = r["sha256"]
        file_id = r["file_id"]

        # Remove fisicamente de todos os volumes
        for v in [v1, v2]:
            p = v / "_content" / sha[:2] / sha
            if p.exists():
                p.unlink()

        resp = c.get(f"/files/{file_id}/download")
        assert resp.status_code == 410


# -- Cleanup multi-copy -------------------------------------------------------

def test_cleanup_removes_all_physical_copies(tmp_path, monkeypatch):
    """Cleanup de órfão deve apagar o arquivo em todos os volumes."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        r = _upload(c, "b1", "v1", "/file.txt", b"orphan content")
        sha = r["sha256"]

        assert len(_copies_in(v1, sha)) == 1
        assert len(_copies_in(v2, sha)) == 1

        # Deleta a versão → conteúdo vira órfão
        c.delete("/backups/b1/versions/v1")
        # Cleanup via endpoint de manutenção (usa a sessão do request, não SessionLocal)
        r = c.post("/maintenance/cleanup-orphans")
        assert r.status_code == 200

        assert _copies_in(v1, sha) == [], "cópia v1 deve ser removida"
        assert _copies_in(v2, sha) == [], "cópia v2 deve ser removida"


# -- /maintenance/rereplicate -------------------------------------------------

def test_rereplicate_fills_single_copy_to_target(tmp_path, monkeypatch):
    """Arquivo com 1 cópia deve ganhar réplica no segundo volume via /maintenance/rereplicate."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=1):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        r = _upload(c, "b1", "v1", "/file.txt", b"needs replica")
        sha = r["sha256"]

        total = len(_copies_in(v1, sha)) + len(_copies_in(v2, sha))
        assert total == 1

        monkeypatch.setattr(m, "REPLICATION_FACTOR", 2)
        monkeypatch.setattr(storage_mod, "REPLICATION_FACTOR", 2)
        r = c.post("/maintenance/rereplicate")
        assert r.status_code == 200
        data = r.json()
        assert data["replicated"] == 1
        assert data["skipped"] == 0
        assert data["target_copies"] == 2

        assert len(_copies_in(v1, sha)) == 1
        assert len(_copies_in(v2, sha)) == 1


def test_rereplicate_skips_already_replicated(tmp_path, monkeypatch):
    """Arquivos já com cópias suficientes não são contados como replicados."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        _upload(c, "b1", "v1", "/file.txt", b"already replicated")

        r = c.post("/maintenance/rereplicate")
        assert r.status_code == 200
        data = r.json()
        assert data["replicated"] == 0
        assert data["skipped"] == 0


def test_rereplicate_skips_when_source_degraded(tmp_path, monkeypatch):
    """Se a única cópia está em volume degraded (mas há saudáveis), conta como skipped."""
    # 3 volumes: v1 recebe a cópia, depois fica degraded; v2/v3 saudáveis garantem target=2
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()
    v3 = tmp_path / "v3"; v3.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2, v3], replication_factor=1):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        monkeypatch.setattr(m, "_pick_volume", lambda: v1)
        r = _upload(c, "b1", "v1", "/file.txt", b"source will be degraded")

        # v1 degraded: cópia inacessível. v2/v3 saudáveis → target=min(2,2)=2 → underfilled
        m._degraded_volumes.add(v1)
        monkeypatch.setattr(m, "REPLICATION_FACTOR", 2)
        monkeypatch.setattr(storage_mod, "REPLICATION_FACTOR", 2)

        r = c.post("/maintenance/rereplicate")
        assert r.status_code == 200
        assert r.json()["skipped"] == 1
        assert r.json()["replicated"] == 0

        m._degraded_volumes.discard(v1)


def test_rereplicate_dedup_path_triggers_replication(tmp_path, monkeypatch):
    """Upload via caminho dedup (X-Content-Sha256) deve acionar replicação."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=1):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        r = _upload(c, "b1", "v1", "/a.txt", b"dedup content")
        sha = r["sha256"]

        total = len(_copies_in(v1, sha)) + len(_copies_in(v2, sha))
        assert total == 1

        # Ativa replicação e reenvia via caminho dedup (X-Content-Sha256, sem body)
        monkeypatch.setattr(m, "REPLICATION_FACTOR", 2)
        monkeypatch.setattr(storage_mod, "REPLICATION_FACTOR", 2)
        resp = c.post(
            "/upload",
            content=b"",
            headers={
                "X-Backup-Label": "b1",
                "X-Version-Key": "v1",
                "X-Original-Path": _enc("/b.txt"),
                "X-Mtime": "1.0",
                "X-Content-Sha256": sha,
            },
        )
        assert resp.status_code == 200

        assert len(_copies_in(v1, sha)) == 1
        assert len(_copies_in(v2, sha)) == 1


# -- /storage/disks with replication ------------------------------------------

def test_storage_disks_counts_copies_per_volume(tmp_path, monkeypatch):
    """/storage/disks conta cópias por volume, não arquivos lógicos."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        _upload(c, "b1", "v1", "/a.txt", b"hello")
        _upload(c, "b1", "v1", "/b.txt", b"world!")

        r = c.get("/storage/disks")
        assert r.status_code == 200
        by_path = {d["path"]: d for d in r.json()}

        # Com fator 2 e 2 volumes, cada volume deve ter 2 arquivos
        assert by_path[str(v1)]["content_files"] == 2
        assert by_path[str(v2)]["content_files"] == 2
        assert by_path[str(v1)]["content_bytes"] == len(b"hello") + len(b"world!")
        assert by_path[str(v2)]["content_bytes"] == len(b"hello") + len(b"world!")
