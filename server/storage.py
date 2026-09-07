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

import config

log = logging.getLogger("backup-server")

# -- Config -------------------------------------------------------------------
# Os valores vêm de config.json (ver server/config.py). Os que não exigem
# reinício são reatribuídos aqui por config.apply_runtime() quando a tela
# /settings salva — por isso as funções abaixo leem os globais, nunca cópias.
STORAGE_VOLUMES: list[Path] = [Path(p) for p in config.get("storage.dirs")]
STORAGE_DIR = STORAGE_VOLUMES[0]
for _v in STORAGE_VOLUMES:
    _v.mkdir(parents=True, exist_ok=True)

ENCRYPTION_ENABLED = config.get("storage.encryption_enabled")
encryption_key: bytes | None = None  # set by main.py lifespan: storage.encryption_key = ...

CHUNK_SIZE = 1024 * 1024
REPLICATION_FACTOR = config.get("storage.replication_factor")
# Limiar absoluto (GB) abaixo do qual um volume é considerado esgotado para escrita e para o auto-cleanup.
STORAGE_FALLBACK_THRESHOLD_GB = config.get("storage.fallback_threshold_gb")

# -- Rebalanceamento entre discos ----------------------------------------------
AUTO_REBALANCE_ENABLED = config.get("storage.auto_rebalance_enabled")
REBALANCE_CHECK_INTERVAL_MINUTES = config.get("storage.rebalance_check_interval_minutes")
# Margem sobre STORAGE_FALLBACK_THRESHOLD_GB usada como meta de espaço livre ao
# rebalancear — evita que o disco de origem volte a disparar o gatilho no ciclo seguinte.
REBALANCE_TARGET_FACTOR = 1.2

# -- SSD cache config ---------------------------------------------------------
SSD_CACHE_ENABLED = config.get("ssd_cache.enabled")
SSD_CACHE_MAX_GB  = config.get("ssd_cache.max_gb")
SSD_CACHE_IDLE_DELAY_MINUTES = config.get("ssd_cache.idle_delay_minutes")
_ssd_cache_raw    = config.get("ssd_cache.dir")
SSD_CACHE_DIR: Path | None = Path(_ssd_cache_raw) if _ssd_cache_raw else None
if SSD_CACHE_DIR:
    (SSD_CACHE_DIR / "_content").mkdir(parents=True, exist_ok=True)

# -- Exceptions ---------------------------------------------------------------

class StorageThresholdExceeded(Exception):
    """Todos os volumes estão abaixo de STORAGE_FALLBACK_THRESHOLD_GB."""

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

    # Memoiza a leitura de espaço: no máximo um statvfs por volume por chamada.
    # O caminho feliz continua barato (para no primeiro volume com folga, um
    # statvfs só); o que muda é o caminho de disco cheio, onde a mensagem de erro
    # relia cada volume mais 1–2 vezes — e ele é justamente o mais quente, porque
    # todo upload passa por aqui quando o storage aperta.
    _usage_cache: dict[Path, object] = {}

    def _usage(v: Path):
        if v not in _usage_cache:
            _usage_cache[v] = safe_disk_usage(v)
        return _usage_cache[v]

    # Percorre em ordem de declaração (prioridade decrescente).
    # Usa o primeiro volume que ainda tem espaço acima do limiar de esgotamento.
    for vol in STORAGE_VOLUMES:
        if vol not in hvols_set:
            continue
        usage = _usage(vol)
        if usage and usage.free > STORAGE_FALLBACK_THRESHOLD_GB * 1024 ** 3:
            return vol

    # Todos os volumes abaixo do limiar — sinaliza para o call site tentar cleanup primeiro.
    free_info = ", ".join(
        f"{v.name}: {_usage(v).free / 1024**3:.1f} GB"
        for v in sorted(hvols_set)
        if _usage(v)
    )
    raise StorageThresholdExceeded(
        f"Todos os volumes abaixo de {STORAGE_FALLBACK_THRESHOLD_GB:.0f} GB ({free_info})"
    )


def pick_volume_last_resort() -> Path:
    """Usado apenas quando cleanup não liberou espaço suficiente. Loga CRITICAL."""
    hvols_set = set(healthy_volumes())
    if not hvols_set:
        raise RuntimeError("Nenhum volume de storage disponível")
    vol = max(hvols_set, key=lambda v: getattr(safe_disk_usage(v), "free", 0))
    usage = safe_disk_usage(vol)
    free_gb = usage.free / 1024**3 if usage else 0
    log.critical(
        f"[storage] ÚLTIMO RECURSO: escrevendo em {vol.name} ({free_gb:.2f} GB livres) "
        f"abaixo do limiar de {STORAGE_FALLBACK_THRESHOLD_GB:.0f} GB — "
        f"considere aumentar o storage ou reduzir STORAGE_FALLBACK_THRESHOLD_GB"
    )
    return vol


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
    sha256_list = [sha256 for sha256, _ in underfilled]
    copy_query = db.query(FileContentCopy).filter(FileContentCopy.sha256.in_(sha256_list))
    if degraded_strs:
        copy_query = copy_query.filter(~FileContentCopy.volume_path.in_(degraded_strs))
    source_map: dict[str, FileContentCopy] = {}
    for copy in copy_query.all():
        if copy.sha256 not in source_map:
            source_map[copy.sha256] = copy
    for sha256, _ in underfilled:
        source = source_map.get(sha256)
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
    from collections import defaultdict
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
    sha256_list = [sha256 for sha256, _ in overfilled]
    fc_map = {
        fc.sha256: fc
        for fc in db.query(FileContent).filter(FileContent.sha256.in_(sha256_list)).all()
    }
    copies_map: dict[str, list] = defaultdict(list)
    for copy in db.query(FileContentCopy).filter(FileContentCopy.sha256.in_(sha256_list)).all():
        copies_map[copy.sha256].append(copy)
    for sha256, _ in overfilled:
        primary = fc_map.get(sha256)
        primary_path = primary.stored_at if primary else None
        copies = copies_map.get(sha256, [])

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


def rebalance_sources() -> list[Path]:
    """Volumes saudáveis com espaço livre abaixo do limiar — precisam de alívio."""
    threshold_bytes = STORAGE_FALLBACK_THRESHOLD_GB * 1024 ** 3
    sources = []
    for v in healthy_volumes():
        usage = safe_disk_usage(v)
        if usage and usage.free < threshold_bytes:
            sources.append(v)
    return sources


def rebalance_destinations(exclude: set = frozenset()) -> list[Path]:
    """Volumes saudáveis, fora de `exclude`, com espaço livre acima do limiar —
    na ordem de prioridade declarada em storage.dirs (mesma ordem de STORAGE_VOLUMES)."""
    threshold_bytes = STORAGE_FALLBACK_THRESHOLD_GB * 1024 ** 3
    dests = []
    for v in healthy_volumes():
        if v in exclude:
            continue
        usage = safe_disk_usage(v)
        if usage and usage.free > threshold_bytes:
            dests.append(v)
    return dests


def _pick_rebalance_dest(destinations: list[str], dest_free: dict[str, int], threshold_bytes: float) -> str | None:
    """Primeiro destino com espaço acima do limiar, na ordem de prioridade — mesmo
    critério de pick_volume() para uploads normais: prioridade declarada, não
    "quem tem mais espaço livre". Preenche o disco 3 até a meta antes de tocar
    no disco 4, em vez de espalhar entre os dois só porque ambos têm espaço."""
    for d in destinations:
        if dest_free[d] >= threshold_bytes:
            return d
    return None


def _remove_rebalance_source_copy(db, copy_id: int, stored_at: str, sha256: str, source_volume: str) -> None:
    """Apaga a cópia de origem já liberada (copiada para um destino, ou já
    replicada lá) e realinha FileContent.stored_at se ele apontava para ela."""
    from database import FileContent, FileContentCopy
    path = Path(stored_at)
    stale_stored_at = stored_at
    db.query(FileContentCopy).filter(FileContentCopy.id == copy_id).delete(synchronize_session=False)
    try:
        path.unlink(missing_ok=True)
    except OSError as e:
        log.warning(f"[rebalance] falha ao remover {path}: {e}")
    fc = db.get(FileContent, sha256)
    if fc and fc.stored_at == stale_stored_at:
        alt = (
            db.query(FileContentCopy)
            .filter(FileContentCopy.sha256 == sha256,
                    FileContentCopy.volume_path != source_volume)
            .first()
        )
        if alt:
            fc.stored_at = alt.stored_at


_REBALANCE_BATCH = 50


def rebalance_disks(db, dry_run: bool = False) -> dict:
    """Move o necessário (não necessariamente tudo) dos volumes abaixo do
    limiar de espaço livre para volumes com espaço sobrando, até cada origem
    atingir STORAGE_FALLBACK_THRESHOLD_GB * REBALANCE_TARGET_FACTOR de folga.

    Destinos são preenchidos em ordem de prioridade (storage.dirs), igual a
    pick_volume(): o próximo destino só recebe arquivos depois que o anterior
    na ordem de prioridade fica sem espaço acima do limiar — nunca "quem tem
    mais espaço livre agora".

    dry_run=True calcula o mesmo plano sem copiar/apagar nada — usado pelo preview.
    """
    from database import FileContent, FileContentCopy
    from sqlalchemy import and_, or_

    threshold_bytes = STORAGE_FALLBACK_THRESHOLD_GB * 1024 ** 3
    target_bytes = threshold_bytes * REBALANCE_TARGET_FACTOR

    sources = rebalance_sources()
    if not sources:
        return {"moved": 0, "bytes_moved": 0, "skipped": 0, "sources": [], "destinations": []}

    destinations = rebalance_destinations(exclude=set(sources))
    dest_strs = [str(d) for d in destinations]
    dest_free: dict[str, int] = {}
    for d in destinations:
        usage = safe_disk_usage(d)
        dest_free[str(d)] = usage.free if usage else 0

    moved = total_bytes_moved = total_skipped = 0
    source_reports = []

    for source in sources:
        source_str = str(source)
        usage = safe_disk_usage(source)
        free = usage.free if usage else 0

        # Candidatos em páginas keyset por (size DESC, id), não .all(): o volume de
        # origem pode ter centenas de milhares de cópias, e o loop quase sempre para
        # nas primeiras (o break sai assim que a origem atinge a folga alvo). A chave
        # composta preserva exatamente a ordem "maiores primeiro" do comportamento
        # anterior — paginar só por id moveria os maiores DE CADA PÁGINA.
        def _candidate_pages():
            last = None   # (size, id) do último candidato já visto
            while True:
                q = (
                    db.query(FileContentCopy.id, FileContentCopy.sha256,
                             FileContentCopy.stored_at, FileContent.size)
                    .join(FileContent, FileContent.sha256 == FileContentCopy.sha256)
                    .filter(FileContentCopy.volume_path == source_str)
                )
                if last is not None:
                    last_size, last_id = last
                    q = q.filter(or_(FileContent.size < last_size,
                                     and_(FileContent.size == last_size,
                                          FileContentCopy.id > last_id)))
                page = (q.order_by(FileContent.size.desc(), FileContentCopy.id)
                         .limit(_REBALANCE_BATCH).all())
                if not page:
                    return
                last = (page[-1].size, page[-1].id)
                yield page

        src_moved = src_bytes = src_skipped = 0

        for copy in (c for page in _candidate_pages() for c in page):
            size = copy.size
            if free >= target_bytes or not dest_strs:
                break
            sha256 = copy.sha256

            existing_dest = (
                db.query(FileContentCopy.volume_path)
                .filter(FileContentCopy.sha256 == sha256,
                        FileContentCopy.volume_path.in_(dest_strs))
                .first()
            )

            if existing_dest:
                resolved_dest = existing_dest.volume_path
            else:
                best_dest = _pick_rebalance_dest(dest_strs, dest_free, threshold_bytes)
                if best_dest is None:
                    src_skipped += 1
                    continue
                src_path = Path(copy.stored_at)
                if not src_path.exists():
                    src_skipped += 1
                    continue
                if not dry_run:
                    dest_path = content_path(sha256, Path(best_dest))
                    try:
                        shutil.copy2(str(src_path), str(dest_path))
                        if file_sha256(dest_path) != sha256:
                            dest_path.unlink(missing_ok=True)
                            src_skipped += 1
                            continue
                        db.add(FileContentCopy(sha256=sha256, stored_at=str(dest_path), volume_path=best_dest))
                    except OSError as e:
                        log.warning(f"[rebalance] falha ao copiar {sha256[:8]}… para {best_dest}: {e}")
                        src_skipped += 1
                        continue
                dest_free[best_dest] -= size
                resolved_dest = best_dest

            if not dry_run:
                detail = " (já replicado no destino — apenas liberando a origem)" if existing_dest else ""
                log.info(f"[rebalance] {copy.stored_at} ({fmt_bytes(size)}, sha256={sha256[:8]}…) "
                         f"{source_str} → {resolved_dest}{detail}")
                _remove_rebalance_source_copy(db, copy.id, copy.stored_at, sha256, source_str)
                src_moved += 1
                if src_moved % _REBALANCE_BATCH == 0:
                    db.commit()
            else:
                src_moved += 1

            free += size
            src_bytes += size

        if not dry_run:
            db.commit()

        moved += src_moved
        total_bytes_moved += src_bytes
        total_skipped += src_skipped
        source_reports.append({
            "path": source_str,
            "free_before": usage.free if usage else 0,
            "free_after_estimate": free,
            "files_moved": src_moved,
            "bytes_moved": src_bytes,
            "skipped": src_skipped,
        })

    log.info(f"[rebalance] {moved} arquivo(s), {total_bytes_moved / 1024**3:.2f} GB "
             f"{'(simulado) ' if dry_run else ''}movido(s) de {len(sources)} origem(ns) "
             f"para {len(destinations)} destino(s)")

    return {
        "moved": moved,
        "bytes_moved": total_bytes_moved,
        "skipped": total_skipped,
        "sources": source_reports,
        "destinations": [{"path": str(d), "free_bytes": dest_free[str(d)]} for d in destinations],
    }


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


_BACKFILL_BATCH = 1000

def backfill_content_copies() -> None:
    """Cria FileContentCopy para FileContents antigos sem entrada. Processa em
    batches via NOT EXISTS para não carregar a tabela inteira na RAM no boot."""
    from database import SessionLocal, FileContent, FileContentCopy
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy import exists
    db = SessionLocal()
    try:
        count = 0
        while True:
            batch = (
                db.query(FileContent)
                .filter(~exists().where(FileContentCopy.sha256 == FileContent.sha256))
                .order_by(FileContent.sha256)
                .limit(_BACKFILL_BATCH)
                .all()
            )
            if not batch:
                break
            for fc in batch:
                p = Path(fc.stored_at)
                vol = str(p.parents[2])
                try:
                    db.add(FileContentCopy(sha256=fc.sha256, stored_at=fc.stored_at, volume_path=vol))
                    db.flush()
                    count += 1
                except IntegrityError:
                    # Upload concorrente já criou a entrada; o rollback desfaz o batch
                    # atual, mas o NOT EXISTS re-seleciona as linhas na próxima volta.
                    db.rollback()
                    break
            db.commit()
            if len(batch) < _BACKFILL_BATCH:
                break
        if count:
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
        if usage.free < STORAGE_FALLBACK_THRESHOLD_GB * 1024 ** 3:
            log.debug(
                f"[ssd-cache] SSD com menos de {STORAGE_FALLBACK_THRESHOLD_GB:.0f} GB livre — fallback para HDD"
            )
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


def fmt_bytes(n: float) -> str:
    """Formata bytes de forma legível. Helper único do servidor (era duplicado
    em db_backup, daily_digest e rclone_runner)."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def file_sha256(path: Path) -> str:
    """SHA-256 de um arquivo em chunks de 1 MB. Helper único do servidor."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


# Alias retrocompatível (nome antigo usado pelos workers de SSD cache).
_file_sha256_raw = file_sha256


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
        except (RuntimeError, StorageThresholdExceeded):
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


def process_ssd_pending_moves(db) -> tuple[int, list[str]]:
    """Move up to 10 pending SSD-cached files to their HDD destination. Returns (count, sha256s) moved."""
    from database import SsdCachePendingMove, FileContent, FileContentCopy
    from sqlalchemy.exc import IntegrityError
    # Collect only sha256 keys upfront; commits inside the loop expire session objects,
    # so we re-query each row fresh to avoid "Instance has been deleted" errors.
    pending_sha256s = [m.sha256 for m in db.query(SsdCachePendingMove).limit(10).all()]
    completed = 0
    moved_sha256s: list[str] = []
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
                moved_sha256s.append(sha256)
                log.info(f"[ssd-cache] {move.sha256[:8]}… movido SSD → {dest_path}")
                break
            except IntegrityError:
                db.rollback()
                dest_path.unlink(missing_ok=True)
                # Distingue "worker concorrente venceu" (FileContent existe, SsdCachePendingMove
                # já foi removido pelo vencedor) de "orphan cleanup deletou FileContent"
                # (FileContent sumiu mas SsdCachePendingMove ainda existe → loop infinito).
                fc_exists = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
                if fc_exists is None:
                    stale = db.query(SsdCachePendingMove).filter(SsdCachePendingMove.sha256 == sha256).first()
                    if stale:
                        db.delete(stale)
                        db.commit()
                    ssd_path.unlink(missing_ok=True)
                    log.info(f"[ssd-cache] {sha256[:8]}… FileContent removido pelo orphan cleanup — move cancelado, arquivo SSD removido")
                else:
                    log.debug(f"[ssd-cache] {sha256[:8]}… já processado por worker concorrente — OK")
                break
            except OSError as e:
                dest_path.unlink(missing_ok=True)
                if e.errno == errno.ENOSPC and not _redirected:
                    try:
                        new_vol = pick_volume()
                    except (RuntimeError, StorageThresholdExceeded):
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
    return completed, moved_sha256s
