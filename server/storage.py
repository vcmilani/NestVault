"""
Shared storage state and helpers.

Extracted from main.py so that the cloud backup module can reuse
volume selection, replication, and encryption logic without
creating a circular import.
"""
import os, shutil, logging, asyncio, threading
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
CLEANUP_MIN_FREE_PCT = float(os.getenv("STORAGE_MIN_FREE_PCT", "5.0"))
# Limiar absoluto (GB) abaixo do qual um volume é considerado esgotado e o próximo da fila assume.
STORAGE_FALLBACK_THRESHOLD_GB = float(os.getenv("STORAGE_FALLBACK_THRESHOLD_GB", "10.0"))

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
        if u and u.free / u.total * 100 >= CLEANUP_MIN_FREE_PCT:
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
