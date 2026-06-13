"""
Shared storage state and helpers.

Extracted from main.py so that the cloud backup module can reuse
volume selection, replication, and encryption logic without
creating a circular import.
"""
import os, errno, shutil, logging, asyncio, threading, hashlib
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

log = logging.getLogger("backup-server")

# -- Config -------------------------------------------------------------------
_raw_dirs = os.getenv("STORAGE_DIRS") or os.getenv("STORAGE_DIR", "./storage")
STORAGE_VOLUMES: list[Path] = [Path(p.strip()) for p in _raw_dirs.split(",") if p.strip()]
STORAGE_DIR = STORAGE_VOLUMES[0]
for _v in STORAGE_VOLUMES:
    _v.mkdir(parents=True, exist_ok=True)

ENCRYPTION_ENABLED = os.getenv("ENCRYPTION_ENABLED", "false").lower() == "true"
encryption_key: bytes | None = None  # set by main.py lifespan: storage.encryption_key = ...

CHUNK_SIZE = 1024 * 1024
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "1"))
# Limiar absoluto (GB) abaixo do qual um volume é considerado esgotado para escrita e para o auto-cleanup.
STORAGE_FALLBACK_THRESHOLD_GB = float(os.getenv("STORAGE_FALLBACK_THRESHOLD_GB", "10.0"))

# -- SSD cache config ---------------------------------------------------------
SSD_CACHE_ENABLED = os.getenv("SSD_CACHE_ENABLED", "false").lower() == "true"
SSD_CACHE_MAX_GB  = float(os.getenv("SSD_CACHE_MAX_GB", "20.0"))
_ssd_cache_raw    = os.getenv("SSD_CACHE_DIR", "")
SSD_CACHE_DIR: Path | None = Path(_ssd_cache_raw) if _ssd_cache_raw else None
if SSD_CACHE_DIR:
    (SSD_CACHE_DIR / "_content").mkdir(parents=True, exist_ok=True)

# -- Volume health ------------------------------------------------------------
_degraded_volumes: set[Path] = set()
_deg_lock = threading.Lock()


def safe_disk_usage(v: Path):
    try:
        result = shutil.disk_usage(v)
        with _deg_lock:
            _degraded_volumes.discard(v)
        return result
    except OSError:
        with _deg_lock:
            if v not in _degraded_volumes:
                log.error(f"[volume] {v} inacessível — marcado como degraded")
            _degraded_volumes.add(v)
        return None


def healthy_volumes() -> list[Path]:
    return [v for v in STORAGE_VOLUMES if v not in _degraded_volumes]


def target_replicas() -> int:
    n = len(healthy_volumes())
    factor = REPLICATION_FACTOR if REPLICATION_FACTOR > 0 else len(STORAGE_VOLUMES)
    if n < factor:
        log.warning(f"[replication] fator={factor} > volumes saudáveis={n}")
    return min(factor, max(1, n))


def pick_volume() -> Path:
    hvols_set = set(healthy_volumes())
    if not hvols_set:
        raise RuntimeError("Nenhum volume de storage disponível")

    # Percorre em ordem de declaração (prioridade decrescente).
    # Usa o primeiro volume que ainda tem espaço acima do limiar de esgotamento.
    for vol in STORAGE_VOLUMES:
        if vol not in hvols_set:
            continue
        usage = safe_disk_usage(vol)
        if usage and usage.free > STORAGE_FALLBACK_THRESHOLD_GB * 1024 ** 3:
            return vol

    # Todos os volumes estão esgotados — último recurso: o com mais espaço livre.
    return max(hvols_set, key=lambda v: (safe_disk_usage(v) or type("_", (), {"free": 0})()).free)


def content_path(sha256: str, volume: Path) -> Path:
    dest = volume / "_content" / sha256[:2] / sha256
    dest.parent.mkdir(parents=True, exist_ok=True)
    return dest


def ensure_replicas(sha256: str, source_path: Path, db) -> None:
    from database import FileContentCopy
    target = target_replicas()
    copies = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256).all()
    vol_set = {c.volume_path for c in copies}

    target_vols = []
    for vol in healthy_volumes():
        if len(copies) + len(target_vols) >= target:
            break
        if str(vol) not in vol_set:
            target_vols.append(vol)

    if not target_vols:
        return

    def _copy(vol):
        dest = content_path(sha256, vol)
        shutil.copy2(str(source_path), str(dest))
        return vol, dest

    added = []
    with ThreadPoolExecutor(max_workers=len(target_vols)) as pool:
        futures = {pool.submit(_copy, vol): vol for vol in target_vols}
        for future in as_completed(futures):
            vol = futures[future]
            try:
                _, dest = future.result()
                copy = FileContentCopy(sha256=sha256, stored_at=str(dest), volume_path=str(vol))
                db.add(copy)
                copies.append(copy)
                added.append(str(vol))
            except OSError as e:
                log.warning(f"[replication] Falha ao replicar {sha256[:8]}… para {vol}: {e}")

    if added:
        log.info(f"[replication] {sha256[:8]}… → {len(added)} nova(s) cópia(s): {added}")


def rereplicate_all(db) -> tuple[int, int]:
    from database import FileContent, FileContentCopy
    from sqlalchemy import func
    target = target_replicas()
    degraded_strs = [str(d) for d in _degraded_volumes]
    underfilled = (
        db.query(FileContent.sha256, func.count(FileContentCopy.id).label("cnt"))
        .outerjoin(FileContentCopy, FileContentCopy.sha256 == FileContent.sha256)
        .group_by(FileContent.sha256)
        .having(func.count(FileContentCopy.id) < target)
        .all()
    )
    replicated = skipped = 0
    log.info(f"[rereplicate-all] {len(underfilled)} arquivo(s) sub-replicado(s) — alvo: {target}")
    for sha256, _ in underfilled:
        q = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256)
        if degraded_strs:
            q = q.filter(~FileContentCopy.volume_path.in_(degraded_strs))
        source = q.first()
        if not source:
            log.warning(f"[rereplicate-all] {sha256[:8]}… sem fonte acessível — pulando")
            skipped += 1
            continue
        ensure_replicas(sha256, Path(source.stored_at), db)
        replicated += 1
    db.commit()
    log.info(f"[rereplicate-all] concluído — {replicated} replicado(s), {skipped} pulado(s)")
    return replicated, skipped


def cleanup_excess_copies(db) -> int:
    from database import FileContent, FileContentCopy
    from sqlalchemy import func
    target = target_replicas()
    overfilled = (
        db.query(FileContent.sha256, func.count(FileContentCopy.id).label("cnt"))
        .outerjoin(FileContentCopy, FileContentCopy.sha256 == FileContent.sha256)
        .group_by(FileContent.sha256)
        .having(func.count(FileContentCopy.id) > target)
        .all()
    )
    removed = 0
    log.info(f"[cleanup-excess] {len(overfilled)} arquivo(s) com cópias excedentes — alvo: {target}")
    for sha256, _ in overfilled:
        primary = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
        primary_path = primary.stored_at if primary else None
        copies = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256).all()

        def _sort_key(c: FileContentCopy):
            is_primary = c.stored_at == primary_path
            is_healthy = Path(c.volume_path) not in _degraded_volumes
            return (not is_primary, not is_healthy)

        copies.sort(key=_sort_key)
        kept = copies[:target]
        for copy in copies[target:]:
            try:
                Path(copy.stored_at).unlink(missing_ok=True)
            except OSError as e:
                log.warning(f"[cleanup-excess] Falha ao deletar {copy.stored_at}: {e}")
            db.delete(copy)
            removed += 1
        # Garantir que FileContent.stored_at aponta para uma cópia ainda existente
        if primary and kept and primary.stored_at not in {c.stored_at for c in kept}:
            primary.stored_at = kept[0].stored_at
    if removed:
        db.commit()
    log.info(f"[cleanup-excess] concluído — {removed} cópia(s) excedente(s) removida(s)")
    return removed


def volumes_with_free_space() -> int:
    count = 0
    for v in STORAGE_VOLUMES:
        if v in _degraded_volumes:
            continue
        u = safe_disk_usage(v)
        if u and u.free >= STORAGE_FALLBACK_THRESHOLD_GB * 1024 ** 3:
            count += 1
    return count


def rereplicate_to_volume(v: Path) -> None:
    from database import SessionLocal, FileContent, FileContentCopy
    from sqlalchemy import func
    db = SessionLocal()
    try:
        log.info(f"[rereplicate] Iniciando re-replicação para {v}")
        shas_on_v = {r.sha256 for r in db.query(FileContentCopy.sha256)
                     .filter(FileContentCopy.volume_path == str(v)).all()}
        t = target_replicas()
        underfilled = (
            db.query(FileContent.sha256, func.count(FileContentCopy.id).label("cnt"))
            .outerjoin(FileContentCopy, FileContentCopy.sha256 == FileContent.sha256)
            .group_by(FileContent.sha256)
            .having(func.count(FileContentCopy.id) < t)
            .all()
        )
        count = 0
        for (sha256, _) in underfilled:
            if sha256 in shas_on_v:
                continue
            source = (db.query(FileContentCopy)
                      .filter(FileContentCopy.sha256 == sha256,
                              ~FileContentCopy.volume_path.in_([str(d) for d in _degraded_volumes]))
                      .first())
            if not source:
                continue
            try:
                dest = content_path(sha256, v)
                shutil.copy2(source.stored_at, str(dest))
                db.add(FileContentCopy(sha256=sha256, stored_at=str(dest), volume_path=str(v)))
                count += 1
            except OSError as e:
                log.warning(f"[rereplicate] Erro em {v}: {e} — abortando")
                break
        if count:
            db.commit()
            log.info(f"[rereplicate] {count} arquivo(s) re-replicados para {v}")
        else:
            log.info(f"[rereplicate] Nenhum arquivo sub-replicado encontrado para {v}")
    finally:
        db.close()


def backfill_content_copies() -> None:
    from database import SessionLocal, FileContent, FileContentCopy
    from sqlalchemy.exc import IntegrityError
    db = SessionLocal()
    try:
        existing_shas = {r.sha256 for r in db.query(FileContentCopy.sha256).distinct().all()}
        to_fill = (db.query(FileContent).filter(~FileContent.sha256.in_(existing_shas)).all()
                   if existing_shas else db.query(FileContent).all())
        count = 0
        for fc in to_fill:
            p = Path(fc.stored_at)
            vol = str(p.parents[2])
            try:
                db.add(FileContentCopy(sha256=fc.sha256, stored_at=fc.stored_at, volume_path=vol))
                db.flush()
                count += 1
            except IntegrityError:
                db.rollback()  # upload concorrente já criou a entrada — ok
        if count:
            db.commit()
            log.info(f"[backfill] {count} entrada(s) migradas para file_content_copies")
        else:
            log.info("[backfill] Nenhuma entrada para migrar — file_content_copies já atualizado")
    except Exception as e:
        log.error(f"[backfill] Erro: {e}")
    finally:
        db.close()


async def volume_health_monitor() -> None:
    while True:
        await asyncio.sleep(60)
        with _deg_lock:
            degraded_snapshot = list(_degraded_volumes)
        for v in degraded_snapshot:
            usage = safe_disk_usage(v)
            if usage:
                log.info(f"[volume] {v} recuperado — iniciando re-replicação")
                asyncio.get_running_loop().run_in_executor(None, rereplicate_to_volume, v)


# -- SSD cache helpers --------------------------------------------------------

def ssd_content_path(sha256: str) -> Path:
    assert SSD_CACHE_DIR is not None
    dest = SSD_CACHE_DIR / "_content" / sha256[:2] / sha256
    dest.parent.mkdir(parents=True, exist_ok=True)
    return dest


def ssd_cache_write_dir(db) -> "Path | None":
    """Returns SSD_CACHE_DIR if enabled and cache budget is not exceeded, else None."""
    if not SSD_CACHE_ENABLED or not SSD_CACHE_DIR:
        return None
    try:
        usage = shutil.disk_usage(SSD_CACHE_DIR)
        if usage.free < 2 * 1024 ** 3:
            log.debug("[ssd-cache] SSD com menos de 2 GB livre — fallback para HDD")
            return None
    except OSError:
        return None
    from database import SsdCachePendingMove, FileContent
    from sqlalchemy import func
    used_bytes = (
        db.query(func.coalesce(func.sum(FileContent.size), 0))
        .join(SsdCachePendingMove, SsdCachePendingMove.sha256 == FileContent.sha256)
        .scalar()
    ) or 0
    if used_bytes >= SSD_CACHE_MAX_GB * 1024 ** 3:
        log.debug(f"[ssd-cache] limite de {SSD_CACHE_MAX_GB} GB atingido — fallback para HDD")
        return None
    return SSD_CACHE_DIR


def _file_sha256_raw(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def _copy_with_sha256(src: Path, dst: Path) -> str:
    """Copia src → dst calculando o sha256 da origem na mesma leitura.
    Equivale a copy2 + hash da origem, mas com uma leitura a menos."""
    h = hashlib.sha256()
    with open(src, "rb") as fin, open(dst, "wb") as fout:
        while chunk := fin.read(1 << 20):
            h.update(chunk)
            fout.write(chunk)
    shutil.copystat(str(src), str(dst))
    return h.hexdigest()


def _mark_versions_failed_for_sha256(sha256: str, db) -> None:
    from database import VersionFile, BackupVersion
    version_ids = [
        r.version_id for r in
        db.query(VersionFile.version_id)
        .filter(VersionFile.sha256 == sha256)
        .distinct()
        .all()
    ]
    if not version_ids:
        return
    for v in db.query(BackupVersion).filter(BackupVersion.id.in_(version_ids)).all():
        v.status = "failed"
        v.finished_at = datetime.now()
        log.error(
            f"[ssd-cache] {v.backup_label}/{v.version_key} marcada como failed "
            f"— arquivo {sha256[:8]}… não pôde ser movido para HDD após 5 tentativas"
        )


def recover_stuck_ssd_files(db) -> int:
    """Cria SsdCachePendingMove para arquivos presos no SSD sem move pendente e sem cópia HDD.
    Chamado no startup para recuperar de uploads interrompidos."""
    if not SSD_CACHE_DIR:
        return 0
    from database import FileContent, FileContentCopy, SsdCachePendingMove
    stuck = (
        db.query(FileContentCopy)
        .filter(FileContentCopy.volume_path == str(SSD_CACHE_DIR))
        .outerjoin(SsdCachePendingMove, SsdCachePendingMove.sha256 == FileContentCopy.sha256)
        .filter(SsdCachePendingMove.sha256.is_(None))
        .all()
    )
    created = 0
    for ssd_copy in stuck:
        sha256 = ssd_copy.sha256
        if not Path(ssd_copy.stored_at).exists():
            continue  # arquivo ausente — reconcile_orphaned_ssd_copies trata
        # Se já existe cópia HDD, reconcile cuida do cleanup do SSD
        hdd_copy = (
            db.query(FileContentCopy)
            .filter(FileContentCopy.sha256 == sha256,
                    FileContentCopy.volume_path != str(SSD_CACHE_DIR))
            .first()
        )
        if hdd_copy:
            continue
        fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
        if fc and fc.stored_at != ssd_copy.stored_at and Path(fc.stored_at).exists():
            continue  # FileContent já aponta para HDD
        # Sem cópia HDD — criar move pendente
        try:
            dest_volume = pick_volume()
        except RuntimeError:
            log.error(f"[ssd-cache] recover: {sha256[:8]}… nenhum volume HDD disponível")
            continue
        dest_path = content_path(sha256, dest_volume)
        db.merge(SsdCachePendingMove(
            sha256=sha256,
            ssd_path=str(ssd_copy.stored_at),
            dest_volume=str(dest_volume),
            dest_path=str(dest_path),
        ))
        try:
            db.commit()
        except Exception as e:
            db.rollback()
            log.warning(f"[ssd-cache] recover: {sha256[:8]}… erro ao criar move pendente: {e}")
            continue
        log.info(f"[ssd-cache] recover: {sha256[:8]}… move pendente criado → {dest_path}")
        created += 1
    if created:
        log.info(f"[ssd-cache] recover: {created} arquivo(s) preso(s) no SSD recuperado(s)")
    return created


def reconcile_orphaned_ssd_copies(db) -> int:
    """Corrige FileContentCopy SSD sem SsdCachePendingMove correspondente (órfãos).
    Retorna quantidade de registros corrigidos."""
    if not SSD_CACHE_ENABLED or not SSD_CACHE_DIR:
        return 0
    from database import FileContent, FileContentCopy, SsdCachePendingMove
    orphans = (
        db.query(FileContentCopy)
        .filter(FileContentCopy.volume_path == str(SSD_CACHE_DIR))
        .outerjoin(SsdCachePendingMove, SsdCachePendingMove.sha256 == FileContentCopy.sha256)
        .filter(SsdCachePendingMove.sha256.is_(None))
        .all()
    )
    fixed = 0
    for ssd_copy in orphans:
        sha256 = ssd_copy.sha256
        fallback = (
            db.query(FileContentCopy)
            .filter(FileContentCopy.sha256 == sha256,
                    FileContentCopy.volume_path != str(SSD_CACHE_DIR))
            .first()
        )
        fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
        if fallback:
            if fc and fc.stored_at == ssd_copy.stored_at:
                fc.stored_at = fallback.stored_at
            db.delete(ssd_copy)
            db.commit()
            log.info(f"[ssd-cache] reconciliação: {sha256[:8]}… órfão SSD removido → {fallback.stored_at}")
            fixed += 1
        elif not Path(ssd_copy.stored_at).exists():
            db.delete(ssd_copy)
            db.commit()
            log.error(f"[ssd-cache] reconciliação: {sha256[:8]}… FileContentCopy SSD órfã removida (arquivo não existe)")
            fixed += 1
        else:
            log.warning(f"[ssd-cache] reconciliação: {sha256[:8]}… arquivo no SSD sem cópia HDD e sem move pendente")
    return fixed


def process_ssd_pending_moves(db) -> int:
    """Move up to 10 pending SSD-cached files to their HDD destination. Returns count moved."""
    from database import SsdCachePendingMove, FileContent, FileContentCopy
    from sqlalchemy.exc import IntegrityError
    # Collect only sha256 keys upfront; commits inside the loop expire session objects,
    # so we re-query each row fresh to avoid "Instance has been deleted" errors.
    pending_sha256s = [m.sha256 for m in db.query(SsdCachePendingMove).limit(10).all()]
    completed = 0
    for sha256 in pending_sha256s:
        move = db.query(SsdCachePendingMove).filter(SsdCachePendingMove.sha256 == sha256).first()
        if move is None:
            continue  # processed by concurrent worker
        ssd_path = Path(move.ssd_path)
        if not ssd_path.exists():
            log.warning(f"[ssd-cache] {move.sha256[:8]}… arquivo ausente no SSD — removendo registro")
            db.delete(move)
            db.commit()
            continue
        _redirected = False
        while True:
            dest_path = Path(move.dest_path)
            dest_volume = Path(move.dest_volume)
            try:
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                ssd_hash = _copy_with_sha256(ssd_path, dest_path)
                hdd_hash = _file_sha256_raw(dest_path)
                if ssd_hash != hdd_hash:
                    dest_path.unlink(missing_ok=True)
                    move.retry_count += 1
                    log.warning(f"[ssd-cache] {move.sha256[:8]}… cópia corrompida no HDD — retry {move.retry_count}")
                    if move.retry_count >= 5:
                        log.error(f"[ssd-cache] {move.sha256[:8]}… atingiu 5 retries (hash mismatch) — abandonando move")
                        try:
                            _mark_versions_failed_for_sha256(move.sha256, db)
                        except Exception as _ex:
                            log.error(f"[ssd-cache] erro ao marcar versões como failed: {_ex}")
                        db.delete(move)
                    db.commit()
                    break
                fc = db.query(FileContent).filter(FileContent.sha256 == move.sha256).first()
                if fc:
                    fc.stored_at = str(dest_path)
                ssd_copy = (db.query(FileContentCopy)
                            .filter(FileContentCopy.sha256 == move.sha256,
                                    FileContentCopy.stored_at == move.ssd_path)
                            .first())
                if ssd_copy:
                    db.delete(ssd_copy)
                db.add(FileContentCopy(sha256=move.sha256, stored_at=str(dest_path), volume_path=str(dest_volume)))
                db.flush()
                ensure_replicas(move.sha256, dest_path, db)
                db.delete(move)
                db.commit()
                ssd_path.unlink(missing_ok=True)
                completed += 1
                log.info(f"[ssd-cache] {move.sha256[:8]}… movido SSD → {dest_path}")
                break
            except IntegrityError:
                db.rollback()
                dest_path.unlink(missing_ok=True)
                log.debug(f"[ssd-cache] {move.sha256[:8]}… já processado por worker concorrente — OK")
                break
            except OSError as e:
                dest_path.unlink(missing_ok=True)
                if e.errno == errno.ENOSPC and not _redirected:
                    try:
                        new_vol = pick_volume()
                    except RuntimeError:
                        new_vol = None
                    if new_vol and str(new_vol) != move.dest_volume:
                        new_dest = content_path(sha256, new_vol)
                        old_vol_name = Path(move.dest_volume).name
                        move.dest_volume = str(new_vol)
                        move.dest_path = str(new_dest)
                        move.retry_count = 0
                        db.commit()
                        _redirected = True
                        log.warning(
                            f"[ssd-cache] {sha256[:8]}… disco cheio em {old_vol_name} "
                            f"— redirecionado para {new_vol.name}, retentando"
                        )
                        continue  # retry imediato com novo destino
                move.retry_count += 1
                log.warning(f"[ssd-cache] Erro ao mover {move.sha256[:8]}…: {e} — retry {move.retry_count}")
                if move.retry_count >= 5:
                    log.error(f"[ssd-cache] {move.sha256[:8]}… atingiu 5 retries — abandonando move")
                    try:
                        _mark_versions_failed_for_sha256(move.sha256, db)
                    except Exception as _ex:
                        log.error(f"[ssd-cache] erro ao marcar versões como failed: {_ex}")
                    db.delete(move)
                db.commit()
                break
    return completed
