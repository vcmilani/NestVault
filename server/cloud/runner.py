"""Execução de um job de cloud backup.

Fluxo por job:
  1. Renova access_token se próximo do vencimento
  2. Garante que o BackupID (label) existe
  3. Cria nova BackupVersion
  4. Lista arquivos recursivamente no folder configurado
  5. Producer: para cada arquivo faz download → SHA-256 e enfileira
  6. Consumer (simultâneo): dedup check → store → encrypt (executor) → VersionFile
  7. Finaliza versão
"""
import os, shutil, logging, asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path

import storage
import crypto
from database import (
    SessionLocal, BackupID, BackupVersion, FileContent,
    FileContentCopy, VersionFile, CloudCredential, CloudBackupJob, decrypt_token,
)

log = logging.getLogger("backup-server")

_QUEUE_SIZE = 4  # máximo de tmp files aguardando processamento


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


async def _force_refresh_token(credential: CloudCredential, db) -> str:
    """Invalida o token local e força renovação imediata."""
    credential.token_expiry = None
    return await _fresh_access_token(credential, db)


async def _producer(
    queue: asyncio.Queue,
    all_files: list,
    prev_files: dict,
    credential: CloudCredential,
    provider,
    db,
    errors: list,
    abort: asyncio.Event,
) -> None:
    """Baixa arquivos e enfileira para processamento. Envia sentinel None ao terminar."""
    downloads = 0
    try:
        access_token = await _fresh_access_token(credential, db)
        for entry in all_files:
            if abort.is_set():
                break
            prev = prev_files.get(entry.path)
            if prev and prev[0] == entry.mtime:
                await queue.put(("skip", entry, prev[1]))
                continue

            volume   = storage.pick_volume()
            tmp_path = volume / f"_cloud_tmp_{os.urandom(8).hex()}"

            if downloads > 0 and downloads % 10 == 0:
                access_token = await _fresh_access_token(credential, db)

            try:
                sha256, size = await provider.download_file_to(
                    access_token, entry.file_id, tmp_path, storage.CHUNK_SIZE
                )
                await queue.put(("file", entry, tmp_path, sha256, size, volume))
                downloads += 1
            except Exception as e:
                status = getattr(getattr(e, "response", None), "status_code", None)
                if status in (401, 403):
                    try:
                        log.warning(f"[cloud-runner] {status} em {entry.path}, renovando token e tentando novamente")
                        access_token = await _force_refresh_token(credential, db)
                        sha256, size = await provider.download_file_to(
                            access_token, entry.file_id, tmp_path, storage.CHUNK_SIZE
                        )
                        await queue.put(("file", entry, tmp_path, sha256, size, volume))
                        downloads += 1
                        continue
                    except Exception as e2:
                        e = e2
                tmp_path.unlink(missing_ok=True)
                errors.append(f"{entry.path}: {e}")
                log.error(f"[cloud-runner] Erro no download de {entry.path}: {e}")
    finally:
        await queue.put(None)


async def _consumer(
    queue: asyncio.Queue,
    version_id: int,
    db,
    enc_key: bytes | None,
    errors: list,
    abort: asyncio.Event,
) -> tuple[int, int]:
    """Processa itens da fila: dedup, store, encrypt, replicate, DB. Retorna (processed, skipped)."""
    loop = asyncio.get_running_loop()
    processed = 0
    skipped   = 0

    while True:
        item = await queue.get()
        if item is None:
            break

        kind = item[0]

        if kind == "skip":
            _, entry, sha256 = item
            db.add(VersionFile(
                version_id=version_id,
                original_path=entry.path,
                sha256=sha256,
                mtime=entry.mtime,
            ))
            db.commit()
            processed += 1
            skipped   += 1
            continue

        _, entry, tmp_path, sha256, size, volume = item
        tmp_path = Path(tmp_path)
        try:
            fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
            if fc:
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
                        await loop.run_in_executor(None, crypto.encrypt_stream, dest, tmp_enc, enc_key)
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
                version_id=version_id,
                original_path=entry.path,
                sha256=sha256,
                mtime=entry.mtime,
            ))
            db.commit()
            processed += 1

        except Exception as e:
            if tmp_path:
                Path(tmp_path).unlink(missing_ok=True)
            msg = f"{entry.path}: {e}"
            errors.append(msg)
            log.error(f"[cloud-runner] Erro ao processar {msg}")
            db.rollback()

    return processed, skipped


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
        job.last_run_at      = datetime.now()
        job.last_run_status  = "running"
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
        version_key = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
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
        prev_files: dict[str, tuple[float, str]] = {}
        if prev_version:
            for vf in db.query(VersionFile).filter(VersionFile.version_id == prev_version.id).all():
                prev_files[vf.original_path] = (vf.mtime, vf.sha256)
            log.info(f"[cloud-runner] {len(prev_files)} arquivo(s) na versão anterior para comparação de mtime")

        errors: list[str] = []
        abort = asyncio.Event()
        queue: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_SIZE)

        _, (processed, skipped) = await asyncio.gather(
            _producer(queue, all_files, prev_files, job.credential, provider, db, errors, abort),
            _consumer(queue, version.id, db, enc_key, errors, abort),
        )

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
