import base64
from collections import namedtuple
from pathlib import Path

import database as db_mod
import main as m
import storage as storage_mod
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


# -- Helpers ------------------------------------------------------------------

def _make_engine():
    return create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


def _seed_admin(Session):
    """Cria o usuário admin (chave 'testkey') usado por todos os clients deste
    módulo — auth é sempre obrigatória, então todo TestClient precisa de um."""
    db = Session()
    try:
        db.add(db_mod.User(username="admin", api_key_hash=db_mod.hash_api_key("testkey"),
                            role="admin", is_active=True))
        db.commit()
    finally:
        db.close()


def _mk_client(monkeypatch, volumes, replication_factor=2):
    engine = _make_engine()
    db_mod.Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    _seed_admin(Session)

    # As funções de storage.py leem os globais do próprio módulo storage — é
    # preciso propagar os patches para lá, não só para os aliases em main.
    monkeypatch.setattr(storage_mod, "STORAGE_VOLUMES", volumes)
    monkeypatch.setattr(storage_mod, "STORAGE_DIR", volumes[0])
    monkeypatch.setattr(storage_mod, "REPLICATION_FACTOR", replication_factor)
    # Background tasks (_bg_*) abrem sua propria sessao via SessionLocal() em vez de
    # Depends(get_db) — aponta para o mesmo engine in-memory do teste.
    monkeypatch.setattr(m, "SessionLocal", Session)

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    m.app.dependency_overrides[db_mod.get_db] = override_get_db
    with TestClient(m.app) as c:
        c.headers.update({"X-API-Key": "testkey"})
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

    def fake_usage(path):
        # v1 "morto" de verdade — só assim safe_disk_usage() o mantém degraded;
        # marcar direto em _degraded_volumes não basta, pois qualquer chamada de
        # disk_usage bem-sucedida em v1 (ex: um refresh de stats em background)
        # o "cura" de volta antes do upload rodar.
        if path == v1:
            raise OSError("disco morto")
        return DiskUsage(total=200_000_000_000, used=100_000_000_000, free=100_000_000_000)

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        monkeypatch.setattr(storage_mod.shutil, "disk_usage", fake_usage)
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
        # Cleanup via endpoint de manutenção (roda em background)
        r = c.post("/maintenance/cleanup-orphans")
        assert r.status_code == 200
        assert r.json()["scheduled"] is True

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


# -- Dedup com cópia ilegível: reparar sem apagar nada -------------------------

def _healthy_usage(path):
    return DiskUsage(total=200_000_000_000, used=100_000_000_000, free=100_000_000_000)


def _db_copies(sha):
    db = m.SessionLocal()
    try:
        return {c.volume_path: c.stored_at
                for c in db.query(db_mod.FileContentCopy).filter_by(sha256=sha).all()}
    finally:
        db.close()


def test_dedup_primary_missing_keeps_replica_and_repairs(tmp_path, monkeypatch):
    """Regressão: a cópia primária sumiu do disco e o mesmo conteúdo é enviado de
    novo. Antes, _purge_corrupted_content apagava do disco TODAS as cópias —
    inclusive a réplica boa em v2 — e o upload ainda terminava em 500."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        sha = _upload(c, "b1", "v1", "/a.txt", b"precious")["sha256"]
        (v1 / "_content" / sha[:2] / sha).unlink()

        c.post("/backups/b1/versions", json={"version_key": "v2"})
        r = _upload(c, "b1", "v2", "/a.txt", b"precious")

        assert r["sha256"] == sha
        assert (v2 / "_content" / sha[:2] / sha).read_bytes() == b"precious"
        assert set(_db_copies(sha)) == {str(v1), str(v2)}
        assert c.get(f"/files/{r['file_id']}/download").content == b"precious"


def test_dedup_primary_on_degraded_volume_touches_nothing(tmp_path, monkeypatch):
    """Primária num disco fora do ar: a réplica legível em v2 basta para a dedup.
    Nada é apagado — nem arquivo nem linha da cópia em v1, que pode voltar."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        sha = _upload(c, "b1", "v1", "/a.txt", b"precious")["sha256"]
        before = _db_copies(sha)

        def fake_usage(path):
            if path == v1:
                raise OSError("disco fora do ar")
            return _healthy_usage(path)
        monkeypatch.setattr(storage_mod.shutil, "disk_usage", fake_usage)
        m._degraded_volumes.add(v1)
        hidden = v1 / "_content" / sha[:2] / sha
        hidden.rename(hidden.with_suffix(".offline"))  # ilegível enquanto o disco está fora

        c.post("/backups/b1/versions", json={"version_key": "v2"})
        _upload(c, "b1", "v2", "/a.txt", b"precious")

        assert _db_copies(sha) == before
        assert (v2 / "_content" / sha[:2] / sha).read_bytes() == b"precious"
        assert not hidden.exists(), "v1 fora do ar não pode receber escrita de reparo"


def test_dedup_all_copies_missing_repairs_from_upload(tmp_path, monkeypatch):
    """Nenhuma cópia legível: o upload regrava o conteúdo em vez de dar 500,
    e as versões antigas voltam a ser restauráveis."""
    v1 = tmp_path / "v1"; v1.mkdir()

    for c in _mk_client(monkeypatch, [v1], replication_factor=1):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        old = _upload(c, "b1", "v1", "/a.txt", b"precious")
        sha = old["sha256"]
        (v1 / "_content" / sha[:2] / sha).unlink()

        c.post("/backups/b1/versions", json={"version_key": "v2"})
        _upload(c, "b1", "v2", "/a.txt", b"precious")

        assert c.get(f"/files/{old['file_id']}/download").content == b"precious"
        assert set(_db_copies(sha)) == {str(v1)}


def test_dedup_truncated_copy_is_repaired(tmp_path, monkeypatch):
    """Cópia com tamanho errado no disco é regravada no lugar."""
    v1 = tmp_path / "v1"; v1.mkdir()

    for c in _mk_client(monkeypatch, [v1], replication_factor=1):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        sha = _upload(c, "b1", "v1", "/a.txt", b"precious")["sha256"]
        stored = v1 / "_content" / sha[:2] / sha
        stored.write_bytes(b"prec")

        c.post("/backups/b1/versions", json={"version_key": "v2"})
        _upload(c, "b1", "v2", "/a.txt", b"precious")

        assert stored.read_bytes() == b"precious"


def test_dedup_repair_keeps_encrypted_format_and_clears_quarantine(tmp_path, monkeypatch):
    """O reparo grava no formato do FileContent (cifrado) e tira da quarentena."""
    import os as _os
    from datetime import datetime as _dt
    v1 = tmp_path / "v1"; v1.mkdir()
    key = _os.urandom(32)
    monkeypatch.setattr(storage_mod, "ENCRYPTION_ENABLED", True)
    monkeypatch.setattr(m.crypto, "load_key", lambda: key)  # o lifespan carrega a chave

    for c in _mk_client(monkeypatch, [v1], replication_factor=1):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        old = _upload(c, "b1", "v1", "/a.txt", b"precious")
        sha = old["sha256"]
        stored = v1 / "_content" / sha[:2] / sha
        stored.unlink()
        db = m.SessionLocal()
        fc = db.get(db_mod.FileContent, sha)
        assert fc.encrypted
        fc.quarantined_at = _dt.now(); fc.quarantine_reason = "sumiu"
        db.commit(); db.close()

        c.post("/backups/b1/versions", json={"version_key": "v2"})
        _upload(c, "b1", "v2", "/a.txt", b"precious")

        assert stored.read_bytes() != b"precious"  # cifrado no disco
        assert c.get(f"/files/{old['file_id']}/download").content == b"precious"
        db = m.SessionLocal()
        assert db.get(db_mod.FileContent, sha).quarantined_at is None
        db.close()


# -- Reconciliação: nunca apagar a única cópia legível -------------------------

def test_reconcile_with_degraded_volume_deletes_nothing(tmp_path, monkeypatch):
    """Regressão: com v1 fora do ar, target_replicas() caía de 2 para 1 e a
    reconciliação apagava a cópia do disco SAUDÁVEL, mantendo a do disco offline."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        sha = _upload(c, "b1", "v1", "/a.txt", b"precious")["sha256"]

        def fake_usage(path):
            if path == v1:
                raise OSError("disco fora do ar")
            return _healthy_usage(path)
        monkeypatch.setattr(storage_mod.shutil, "disk_usage", fake_usage)
        m._degraded_volumes.add(v1)

        r = c.post("/maintenance/reconcile-replication")
        assert r.status_code == 200
        assert r.json()["cleaned"] == 0
        assert (v2 / "_content" / sha[:2] / sha).exists()
        assert set(_db_copies(sha)) == {str(v1), str(v2)}


def test_cleanup_excess_never_keeps_unreadable_primary(tmp_path, monkeypatch):
    """Fator 1 com duas cópias, primária sumida do disco (volume ainda não marcado
    degraded): a única cópia legível é preservada."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        sha = _upload(c, "b1", "v1", "/a.txt", b"precious")["sha256"]
        (v1 / "_content" / sha[:2] / sha).unlink()
        monkeypatch.setattr(storage_mod, "REPLICATION_FACTOR", 1)

        db = m.SessionLocal()
        try:
            assert storage_mod.cleanup_excess_copies(db) == 0
        finally:
            db.close()
        assert (v2 / "_content" / sha[:2] / sha).read_bytes() == b"precious"


def test_cleanup_excess_removes_real_excess(tmp_path, monkeypatch):
    """Excesso de verdade (duas cópias legíveis, fator 1) continua sendo removido,
    preservando a primária."""
    v1 = tmp_path / "v1"; v1.mkdir()
    v2 = tmp_path / "v2"; v2.mkdir()

    for c in _mk_client(monkeypatch, [v1, v2], replication_factor=2):
        c.post("/backups", json={"label": "b1"})
        c.post("/backups/b1/versions", json={"version_key": "v1"})
        sha = _upload(c, "b1", "v1", "/a.txt", b"precious")["sha256"]
        monkeypatch.setattr(storage_mod, "REPLICATION_FACTOR", 1)

        db = m.SessionLocal()
        try:
            primary = db.get(db_mod.FileContent, sha).stored_at
            assert storage_mod.cleanup_excess_copies(db) == 1
        finally:
            db.close()
        assert list(_db_copies(sha).values()) == [primary]
        assert Path(primary).exists()
