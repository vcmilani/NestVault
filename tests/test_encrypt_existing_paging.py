"""Paginação keyset de _bg_encrypt_existing.

O loop percorre file_contents em páginas ordenadas por sha256, avançando sempre
pelo maior sha256 já visto. Isso importa porque as linhas viram encrypted=True
durante a execução e as que falham continuam False: um OFFSET puliria linhas
conforme o conjunto encolhe, e "pegue os N primeiros não cifrados" nunca sairia
do primeiro item que falha.
"""
import os

import main as m
import storage as storage_mod
from database import FileContent, FileContentCopy, MaintenanceJob
from conftest import make_backup, make_version, upload_file


def _seed_plaintext(client, n):
    """Sobe n arquivos distintos com a criptografia ainda DESLIGADA."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    for i in range(n):
        upload_file(client, "b1", "v1", path=f"/f{i}.txt", content=f"conteudo-{i}".encode())


def _enable_encryption(monkeypatch):
    """Liga a criptografia DEPOIS do conteúdo já gravado em claro — é exatamente
    o cenário que o encrypt-existing existe para resolver."""
    monkeypatch.setattr(storage_mod, "ENCRYPTION_ENABLED", True, raising=False)
    monkeypatch.setattr(storage_mod, "encryption_key", os.urandom(32), raising=False)


def _run_job():
    db = m.SessionLocal()
    try:
        mj = MaintenanceJob(job_type="encrypt-existing", status="running", summary="")
        db.add(mj); db.commit(); db.refresh(mj)
        job_id = mj.id
    finally:
        db.close()
    m._bg_encrypt_existing(job_id)
    return job_id


def _pending_shas():
    db = m.SessionLocal()
    try:
        return [r.sha256 for r in
                db.query(FileContent).filter(FileContent.encrypted == False).all()]  # noqa: E712
    finally:
        db.close()


def test_encrypts_across_multiple_pages(client, monkeypatch):
    """Mais conteúdo que uma página: tudo tem que ser cifrado mesmo assim."""
    _seed_plaintext(client, 5)
    assert len(_pending_shas()) == 5, "o setup precisa gravar em claro"

    _enable_encryption(monkeypatch)
    monkeypatch.setattr(m, "_ENCRYPT_PAGE", 2)
    _run_job()

    assert _pending_shas() == []


def test_page_boundary_failure_does_not_loop_forever(client, monkeypatch):
    """Um item que sempre falha não pode prender o loop na mesma página.

    Some com o registro de cópia do primeiro sha256 na ordem de varredura: ele
    cai no ramo "sem cópia acessível", continua encrypted=False, e o loop tem
    que passar adiante em vez de repescá-lo na página seguinte.
    """
    _seed_plaintext(client, 5)

    db = m.SessionLocal()
    try:
        stuck = db.query(FileContent).order_by(FileContent.sha256).first().sha256
        db.query(FileContentCopy).filter(FileContentCopy.sha256 == stuck).delete(
            synchronize_session=False)
        db.commit()
    finally:
        db.close()

    _enable_encryption(monkeypatch)
    monkeypatch.setattr(m, "_ENCRYPT_PAGE", 2)
    job_id = _run_job()   # trava aqui se a paginação regredir

    assert _pending_shas() == [stuck]

    db = m.SessionLocal()
    try:
        mj = db.get(MaintenanceJob, job_id)
        assert mj.status == "done"
        assert "4 arquivo(s) cifrado(s)" in mj.summary
        assert "1 pulado(s)" in mj.summary
    finally:
        db.close()


def test_encrypted_content_is_still_readable(client, monkeypatch):
    """Cifragem em massa não pode quebrar o download: o servidor decifra na volta."""
    _seed_plaintext(client, 2)
    _enable_encryption(monkeypatch)
    _run_job()

    files = client.get("/files", params={"backup_label": "b1", "version_key": "v1"}).json()
    assert len(files) == 2

    r = client.get(f"/files/{files[0]['id']}/download")
    assert r.status_code == 200
    assert r.content.startswith(b"conteudo-")
