"""Execução de um job de cloud backup.

Fluxo por job:
  1. Renova access_token se próximo do vencimento
  2. Garante que o BackupID (label) existe
  3. Cria nova BackupVersion
  4. Lista arquivos recursivamente no folder configurado
  5. Para cada arquivo: download → SHA-256 → dedup check → store → VersionFile
  6. Finaliza versão
"""
import os, shutil, logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import storage
import crypto
from database import (
    SessionLocal, BackupID, BackupVersion, FileContent,
    FileContentCopy, VersionFile, CloudCredential, CloudBackupJob, decrypt_token,
)

log = logging.getLogger("backup-server")


def _get_provider(provider_name: str):
    if provider_name == "gdrive":
        from cloud.gdrive import GoogleDriveProvider
        return GoogleDriveProvider()
    if provider_name == "onedrive":
        from cloud.onedrive import OneDriveProvider
        return OneDriveProvider()
    raise ValueError(f"Provider desconhecido: {provider_name}")


async def _fresh_access_token(credential: CloudCredential, db) -> str:
    """Retorna access_token válido, renovando via refresh_token se necessário."""
    needs_refresh = (
        not credential.access_token
        or credential.token_expiry is None
        or credential.token_expiry <= datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=5)
    )
    if needs_refresh:
        provider = _get_provider(credential.provider)
        refresh_token = decrypt_token(credential.refresh_token)
        tokens = await provider.refresh_tokens(refresh_token)
        credential.access_token = tokens["access_token"]
        credential.token_expiry = tokens.get("expiry")
        db.commit()
        log.info(f"[cloud] Token renovado para {credential.email} ({credential.provider})")
    return credential.access_token


async def run_cloud_backup_job(job_id: int) -> None:
    db = SessionLocal()
    job: CloudBackupJob | None = None
    version: BackupVersion | None = None
    try:
        job = db.get(CloudBackupJob, job_id)
        if not job:
            log.error(f"[cloud-runner] Job {job_id} não encontrado")
            return

        log.info(f"[cloud-runner] Iniciando job {job_id}: {job.credential.provider}/{job.folder_name} → {job.target_label}")
        job.last_run_at     = datetime.now(timezone.utc).replace(tzinfo=None)
        job.last_run_status = "running"
        job.last_run_message = None
        db.commit()

        provider     = _get_provider(job.credential.provider)
        access_token = await _fresh_access_token(job.credential, db)

        # Garante que o BackupID existe
        backup = db.query(BackupID).filter(BackupID.label == job.target_label).first()
        if not backup:
            backup = BackupID(label=job.target_label, client_name=f"cloud:{job.credential.provider}")
            db.add(backup)
            db.commit()

        # Cria BackupVersion; marca running anteriores como incomplete
        version_key = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y%m%dT%H%M%S")
        db.query(BackupVersion).filter(
            BackupVersion.backup_label == job.target_label,
            BackupVersion.status == "running",
        ).update({"status": "incomplete"}, synchronize_session=False)
        version = BackupVersion(backup_label=job.target_label, version_key=version_key, status="running")
        db.add(version)
        db.commit()
        db.refresh(version)

        # Lista arquivos no cloud
        log.info(f"[cloud-runner] Listando arquivos em {job.folder_name} ({job.folder_id})")
        all_files = await provider.list_folder_recursive(access_token, job.folder_id)
        total     = len(all_files)
        log.info(f"[cloud-runner] {total} arquivo(s) encontrado(s)")

        enc_key = storage.encryption_key if storage.ENCRYPTION_ENABLED else None

        # Carrega arquivos da última versão concluída para skip por mtime
        prev_version = (
            db.query(BackupVersion)
            .filter(BackupVersion.backup_label == job.target_label, BackupVersion.status == "done")
            .order_by(BackupVersion.version_key.desc())
            .first()
        )
        prev_files: dict[str, tuple[float, str]] = {}  # path → (mtime, sha256)
        if prev_version:
            for vf in db.query(VersionFile).filter(VersionFile.version_id == prev_version.id).all():
                prev_files[vf.original_path] = (vf.mtime, vf.sha256)
            log.info(f"[cloud-runner] {len(prev_files)} arquivo(s) na versão anterior para comparação de mtime")

        processed = 0
        skipped   = 0
        errors: list[str] = []

        for entry in all_files:
            tmp_path: Path | None = None
            try:
                # Se mtime não mudou desde o último backup, reutiliza sha256 sem baixar
                prev = prev_files.get(entry.path)
                if prev and prev[0] == entry.mtime:
                    sha256 = prev[1]
                    db.add(VersionFile(
                        version_id=version.id,
                        original_path=entry.path,
                        sha256=sha256,
                        mtime=entry.mtime,
                    ))
                    db.commit()
                    processed += 1
                    skipped   += 1
                    continue

                volume   = storage.pick_volume()
                tmp_path = volume / f"_cloud_tmp_{os.urandom(8).hex()}"

                # Renova token periodicamente (a cada 100 arquivos)
                if processed % 100 == 0 and processed > 0:
                    access_token = await _fresh_access_token(job.credential, db)

                sha256, size = await provider.download_file_to(
                    access_token, entry.file_id, tmp_path, storage.CHUNK_SIZE
                )

                fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
                if fc:
                    # Conteúdo já existe — apenas garante replicação
                    tmp_path.unlink(missing_ok=True)
                    first_copy = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256).first()
                    if first_copy:
                        storage.ensure_replicas(sha256, Path(first_copy.stored_at), db)
                else:
                    dest = storage.content_path(sha256, volume)
                    shutil.move(str(tmp_path), str(dest))
                    tmp_path = None
                    if enc_key:
                        tmp_enc = dest.parent / f"_enc_{os.urandom(4).hex()}"
                        try:
                            crypto.encrypt_stream(dest, tmp_enc, enc_key)
                            shutil.move(str(tmp_enc), str(dest))
                        except Exception:
                            tmp_enc.unlink(missing_ok=True)
                            raise
                    fc = FileContent(sha256=sha256, stored_at=str(dest), size=size, encrypted=bool(enc_key))
                    db.add(fc)
                    db.add(FileContentCopy(sha256=sha256, stored_at=str(dest), volume_path=str(volume)))
                    db.flush()
                    storage.ensure_replicas(sha256, dest, db)

                db.add(VersionFile(
                    version_id=version.id,
                    original_path=entry.path,
                    sha256=sha256,
                    mtime=entry.mtime,
                ))
                db.commit()
                processed += 1

            except Exception as e:
                if tmp_path:
                    tmp_path.unlink(missing_ok=True)
                msg = f"{entry.path}: {e}"
                errors.append(msg)
                log.error(f"[cloud-runner] Erro ao processar {msg}")
                db.rollback()

        version.status      = "failed" if (processed == 0 and errors) else "done"
        version.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
        db.commit()

        downloaded = processed - skipped
        summary = f"{processed}/{total} arquivo(s) processado(s) ({downloaded} baixado(s), {skipped} sem alteração)"
        if errors:
            summary += f", {len(errors)} erro(s): {'; '.join(errors[:3])}"
            if len(errors) > 3:
                summary += f" ... (+{len(errors) - 3})"

        job.last_run_status  = "success" if not errors else "partial"
        job.last_run_message = summary
        db.commit()
        log.info(f"[cloud-runner] Job {job_id} concluído — {summary}")

    except Exception as e:
        log.exception(f"[cloud-runner] Job {job_id} falhou: {e}")
        if version and version.id:
            try:
                version.status      = "failed"
                version.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
                db.commit()
            except Exception:
                pass
        if job:
            try:
                job.last_run_status  = "error"
                job.last_run_message = str(e)
                db.commit()
            except Exception:
                pass
    finally:
        db.close()
