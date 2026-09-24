"""
Shared storage state and helpers.

Extracted from main.py so that the cloud backup module can reuse
volume selection, replication, and encryption logic without
creating a circular import.
"""
import os, errno, shutil, logging, asyncio, threading, hashlib, tempfile
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


# Amostra por volume em volume_looks_sane(). 32 é barato (32 stat()).
_SANITY_SAMPLE = 32
# Abaixo disto, "toda a amostra ausente" não é evidência de disco fora do ar:
# num volume com 1 ou 2 conteúdos é exatamente o que uma deleção legítima
# produz, e concluir "não confiável" pularia a integridade para sempre num
# install pequeno — pior que verificá-la.
_SANITY_MIN_EVIDENCE = 8


def volume_looks_sane(vol: Path, db) -> bool:
    """O volume contém, de fato, o conteúdo que o banco diz que ele contém?

    safe_disk_usage() só detecta o mountpoint sumido: ele faz um statvfs, que
    continua respondendo quando o disco está montado mas com o _content/
    ilegível, e responde sobre o ROOTFS quando o mkdir do import (linha 23)
    recriou o mountpoint de um disco que não subiu — caso em que o volume passa
    por saudável e vazio.

    A pergunta aqui é outra e não depende de estado novo em disco: o banco
    registra cópias neste volume; o volume corrobora?
    """
    from sqlalchemy import func
    from database import FileContentCopy

    expected = (
        db.query(func.count(FileContentCopy.id))
        .filter(FileContentCopy.volume_path == str(vol))
        .scalar()
    ) or 0
    if expected == 0:
        return True  # volume novo ou vazio — não há o que conferir

    # Sinal 1: o próprio _content/ não está lá, embora o banco espere cópias.
    # É a assinatura exata do disco que não subiu e teve o mountpoint recriado
    # vazio pelo mkdir do import — e não depende do tamanho da amostra, porque
    # apagar arquivos nunca apaga o diretório.
    try:
        os.stat(vol / "_content")
    except OSError as e:
        log.error(
            f"[volume] {vol} não confiável — o banco registra {expected} cópia(s) "
            f"mas {vol / '_content'} não está acessível ({e.__class__.__name__}): "
            f"disco desmontado, mountpoint recriado vazio ou diretório ilegível"
        )
        return False

    # Sinal 2: _content/ existe mas nada do que o banco registra está nele.
    # Só conclui com amostra grande (ver _SANITY_MIN_EVIDENCE).
    if expected < _SANITY_MIN_EVIDENCE:
        return True

    sample = [
        r[0] for r in db.query(FileContentCopy.stored_at)
        .filter(FileContentCopy.volume_path == str(vol))
        .limit(_SANITY_SAMPLE)
        .all()
    ]
    for p in sample:
        try:
            os.stat(p)
            return True
        except OSError:
            continue

    log.error(
        f"[volume] {vol} não confiável — o banco registra {expected} cópia(s) e "
        f"nenhuma das {len(sample)} amostradas está acessível, embora _content/ exista"
    )
    return False


def target_replicas() -> int:
    n = len(healthy_volumes())
    factor = REPLICATION_FACTOR if REPLICATION_FACTOR > 0 else len(STORAGE_VOLUMES)
    if n < factor:
        log.warning(f"[replication] fator={factor} > volumes saudáveis={n}")
    return min(factor, max(1, n))


def configured_replicas() -> int:
    """Fator de replicação configurado, limitado ao número de volumes declarados.

    Diferente de target_replicas(), NÃO encolhe quando um volume fica degraded.
    Quem decide o que APAGAR tem de usar este valor: com target_replicas(), um
    disco fora do ar baixava o alvo (2 → 1) e as cópias do disco saudável
    passavam a ser "excedentes"."""
    total = len(STORAGE_VOLUMES)
    factor = REPLICATION_FACTOR if REPLICATION_FACTOR > 0 else total
    return max(1, min(factor, total))


def expected_stored_size(plain_size: int, encrypted: bool) -> int:
    """Tamanho esperado do arquivo no disco a partir do tamanho do plaintext.
    Cifrado: base_nonce + 20 bytes de overhead (4 de comprimento + 16 de tag GCM)
    por chunk de 1 MB — formato definido em crypto.py."""
    if not encrypted:
        return plain_size
    import crypto
    n_chunks = (plain_size + crypto.CHUNK_SIZE - 1) // crypto.CHUNK_SIZE
    return crypto.NONCE_SIZE + plain_size + n_chunks * 20


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


class ReplicaSourceCorrupt(Exception):
    """A origem de uma cópia não tem o conteúdo esperado — não deve ser propagada."""


def copy_verified(src: Path, dest: Path, sha256: str, encrypted: bool,
                  expected_size: int | None = None) -> None:
    """Copia src → dest de forma atômica e conferida.

    Grava num temporário _tmp_ ao lado do destino (varrido pela limpeza de
    temporários) e só renomeia depois de conferir. Antes, as réplicas eram um
    copy2 direto para o caminho final: um ENOSPC no meio deixava um arquivo
    parcial no lugar definitivo, e uma origem corrompida era espalhada em
    silêncio para todas as réplicas.

    Conferência: o que foi gravado (relido do disco) tem de bater com o que foi
    lido da origem. Sem criptografia, o hash da origem também tem de ser o
    próprio sha256; com criptografia, confere o tamanho esperado (decifrar para
    re-hashear fica com o validate-integrity). Levanta ReplicaSourceCorrupt se a
    origem não confere e OSError em falha de E/S."""
    src_size = os.stat(src).st_size
    if expected_size is not None and src_size != expected_size:
        raise ReplicaSourceCorrupt(
            f"{src}: {src_size} B no disco, esperado {expected_size} B"
        )
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=dest.parent, prefix="_tmp_")
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        src_hash = _copy_with_sha256(src, tmp)
        if not encrypted and src_hash != sha256:
            raise ReplicaSourceCorrupt(f"{src}: sha256 da origem não confere")
        if file_sha256(tmp) != src_hash:
            raise OSError(errno.EIO, f"cópia gravada em {dest.parent} não confere com a origem")
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)


def insert_copy_row(db, sha256: str, stored_at: str, volume_path: str) -> None:
    """Registra uma FileContentCopy tolerando o registro concorrente da mesma cópia.

    Duas réplicas do mesmo sha256 para o mesmo volume ao mesmo tempo (upload +
    /register/batch, dois jobs rclone...) batiam na unique (sha256, volume_path)
    — e o IntegrityError derrubava a transação inteira de quem chegou depois, que
    em /register/batch era o lote todo de réplicas. Os dois gravam o mesmo
    arquivo content-addressed (via os.replace), então basta uma linha."""
    from database import FileContentCopy
    db.flush()  # o INSERT abaixo não passa pelo autoflush do ORM
    dialect = db.get_bind().dialect.name
    if dialect == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as _insert
    elif dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as _insert
    else:  # pragma: no cover - backends suportados são só esses dois
        db.add(FileContentCopy(sha256=sha256, stored_at=stored_at, volume_path=volume_path))
        return
    db.execute(
        _insert(FileContentCopy)
        .values(sha256=sha256, stored_at=stored_at, volume_path=volume_path)
        .on_conflict_do_nothing()
    )


def ensure_replicas(sha256: str, source_path: Path, db) -> None:
    from database import FileContent, FileContentCopy
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

    fc = db.get(FileContent, sha256)
    if fc is None:
        return
    encrypted = bool(fc.encrypted)
    expected = expected_stored_size(fc.size, encrypted)

    def _copy(vol):
        dest = content_path(sha256, vol)
        copy_verified(source_path, dest, sha256, encrypted, expected)
        return dest

    added = []
    with ThreadPoolExecutor(max_workers=len(target_vols)) as pool:
        futures = {pool.submit(_copy, vol): vol for vol in target_vols}
        for future in as_completed(futures):
            vol = futures[future]
            try:
                dest = future.result()
            except ReplicaSourceCorrupt as e:
                log.error(f"[replication] {sha256[:8]}… origem não confere — réplica NÃO criada em {vol}: {e}")
                continue
            except OSError as e:
                log.warning(f"[replication] Falha ao replicar {sha256[:8]}… para {vol}: {e}")
                continue
            insert_copy_row(db, sha256, str(dest), str(vol))
            added.append(str(vol))

    if added:
        log.info(f"[replication] {sha256[:8]}… → {len(added)} nova(s) cópia(s): {added}")


class ContentRepairUnavailable(Exception):
    """O conteúdo precisa ser regravado, mas está cifrado e a chave não está carregada."""


class ContentVerificationFailed(Exception):
    """O arquivo gravado não decifra/re-hasheia para o sha256 esperado."""


def content_matches(path: Path, sha256: str, encrypted: bool, key: bytes | None) -> bool:
    """Decifra (se preciso) e re-hasheia `path`; True se confere com `sha256`."""
    import crypto
    h = hashlib.sha256()
    try:
        if encrypted:
            for chunk in crypto.decrypt_chunks(path, key):
                h.update(chunk)
        else:
            with open(path, "rb") as f:
                while chunk := f.read(1 << 20):
                    h.update(chunk)
    except Exception:
        return False
    return h.hexdigest() == sha256


def stage_content(src: Path, dest: Path, sha256: str, encrypted: bool,
                  key: bytes | None) -> None:
    """Grava `src` (plaintext) em `dest` no formato pedido, conferido e atômico.

    Consome `src`. O destino só é substituído (os.replace) depois de o arquivo
    temporário decifrar e re-hashear para `sha256`."""
    import crypto
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, staged_name = tempfile.mkstemp(dir=dest.parent, prefix="_tmp_")
    os.close(fd)
    staged = Path(staged_name)
    try:
        if encrypted:
            crypto.encrypt_stream(src, staged, key)
        else:
            shutil.move(str(src), str(staged))
        if not content_matches(staged, sha256, encrypted, key):
            raise ContentVerificationFailed(
                f"{sha256[:8]}… gravado em {dest.parent} não confere após "
                f"{'cifrar' if encrypted else 'gravar'}"
            )
        os.replace(staged, dest)
    finally:
        staged.unlink(missing_ok=True)
        src.unlink(missing_ok=True)


def dedup_or_repair(db, fc, tmp_path: Path, volume: Path, what: str) -> "Path | None":
    """Conteúdo recebido cujo sha256 já tem FileContent: deduplica ou repara.

    Dedup exige UMA cópia legível (existência + tamanho esperado; a verificação
    profunda fica com o validate-integrity). Cópias em volume degraded nem são
    consultadas — o disco pode só estar fora do ar. Sem nenhuma legível, regrava
    o conteúdo a partir dos bytes recebidos (mesmo sha256, logo fonte válida) —
    sem apagar nada: as outras cópias, inclusive as de discos fora do ar, ficam
    como estão. Grava no formato do FileContent (fc.encrypted), que vale para
    todas as cópias, e não no da config atual.

    Consome `tmp_path`. Retorna o path a partir do qual criar réplicas depois do
    commit (None quando deduplicou — aí as réplicas já foram garantidas aqui).
    Compartilhado por /upload e pela ingestão do rclone."""
    from database import FileContentCopy
    sha256 = fc.sha256
    copies = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256).all()
    degraded = {str(v) for v in _degraded_volumes}
    expected = expected_stored_size(fc.size, fc.encrypted)
    unreadable: list[str] = []
    for c in copies:
        if c.volume_path in degraded:
            continue
        try:
            stored_size = os.stat(c.stored_at).st_size
        except OSError:
            unreadable.append(f"{c.stored_at} (ausente)")
            continue
        if stored_size != expected:
            unreadable.append(f"{c.stored_at} ({stored_size} B, esperado {expected} B)")
            continue
        log.info(f"[dedup] {what} — sha256={sha256[:8]}… ({fc.size / 1024 / 1024:.2f} MB)")
        ensure_replicas(sha256, Path(c.stored_at), db)
        tmp_path.unlink(missing_ok=True)
        return None

    log.warning(
        f"[integrity] {sha256[:8]}… nenhuma cópia legível "
        f"({len(copies)} registrada(s), {len(unreadable)} ilegível(is) em volume saudável"
        + (f": {'; '.join(unreadable)}" if unreadable else "")
        + f") — reparando com os bytes recebidos ({what})"
    )
    if fc.encrypted and encryption_key is None:
        tmp_path.unlink(missing_ok=True)
        raise ContentRepairUnavailable(
            f"Conteudo {sha256[:8]}… precisa de reparo, mas esta cifrado e a chave "
            "de criptografia nao esta carregada"
        )
    # Reaproveita a linha do próprio volume, se houver: a constraint (sha256,
    # volume_path) não permite uma segunda, e o arquivo dela está ilegível.
    own_row = next((c for c in copies if c.volume_path == str(volume)), None)
    dest = Path(own_row.stored_at) if own_row else content_path(sha256, volume)
    stage_content(tmp_path, dest, sha256, bool(fc.encrypted), encryption_key)

    if own_row is None:
        insert_copy_row(db, sha256, str(dest), str(volume))
    fc.stored_at = str(dest)
    # O conteúdo voltou a existir: sai da quarentena, senão o purge da
    # quarentena apagaria justamente a cópia recém-reparada.
    fc.quarantined_at = None
    fc.quarantine_reason = None
    db.flush()
    log.warning(f"[integrity] {sha256[:8]}… reparado a partir de {what} → {dest}")
    return dest


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
    """Remove cópias além do fator de replicação configurado.

    Só apaga o que é comprovadamente redundante: com algum volume degraded a
    limpeza inteira é adiada, e uma cópia só conta para o alvo se o arquivo está
    no disco com o tamanho esperado. Uma cópia ausente, truncada ou num disco que
    sumiu sem ainda ter sido marcado degraded nunca é "mantida" no lugar de uma
    cópia legível."""
    from database import FileContent, FileContentCopy
    from sqlalchemy import func
    from collections import defaultdict
    if _degraded_volumes:
        log.warning(
            f"[cleanup-excess] adiado — volume(s) degraded: "
            f"{', '.join(sorted(str(v) for v in _degraded_volumes))}"
        )
        return 0
    target = configured_replicas()
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
        if primary is None:
            continue
        primary_path = primary.stored_at
        expected = expected_stored_size(primary.size, primary.encrypted)

        # Só entram na conta as cópias legíveis. As ausentes/truncadas não são
        # tocadas aqui (nem arquivo nem linha) — são problema de integridade, não
        # de excesso, e apagá-las esconderia a evidência do validate-integrity.
        readable = []
        for c in copies_map.get(sha256, []):
            try:
                if os.stat(c.stored_at).st_size == expected:
                    readable.append(c)
            except OSError:
                pass
        if len(readable) <= target:
            continue

        readable.sort(key=lambda c: c.stored_at != primary_path)
        kept = readable[:target]
        for copy in readable[target:]:
            try:
                Path(copy.stored_at).unlink(missing_ok=True)
            except OSError as e:
                log.warning(f"[cleanup-excess] Falha ao deletar {copy.stored_at}: {e}")
            db.delete(copy)
            removed += 1
        # Garantir que FileContent.stored_at aponta para uma cópia ainda existente
        if primary.stored_at not in {c.stored_at for c in kept}:
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
            fc = db.get(FileContent, sha256)
            try:
                dest = content_path(sha256, v)
                copy_verified(Path(source.stored_at), dest, sha256, bool(fc.encrypted),
                              expected_stored_size(fc.size, bool(fc.encrypted)))
                insert_copy_row(db, sha256, str(dest), str(v))
                count += 1
            except ReplicaSourceCorrupt as e:
                log.error(f"[rereplicate] {sha256[:8]}… origem não confere — pulando: {e}")
                continue
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
    # Corpo blindado pelo mesmo motivo do _ssd_space_monitor: uma exceção solta
    # aqui encerrava a task e volume degradado nunca mais era reavaliado.
    while True:
        await asyncio.sleep(60)
        try:
            with _deg_lock:
                degraded_snapshot = list(_degraded_volumes)
            for v in degraded_snapshot:
                usage = safe_disk_usage(v)
                if usage:
                    log.info(f"[volume] {v} recuperado — iniciando re-replicação")
                    asyncio.get_running_loop().run_in_executor(None, rereplicate_to_volume, v)
        except Exception:
            log.exception("[volume] Erro no monitor de saúde — seguindo para o próximo ciclo")


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


def tmp_sweep_dirs() -> list[Path]:
    """Diretórios varridos em busca de temporários órfãos (_tmp_*, _enc_*, staging
    do rclone): os volumes de storage MAIS o diretório do SSD cache.

    O SSD cache não faz parte de STORAGE_VOLUMES, então as varreduras — startup e
    limpeza noturna — nunca olhavam para ele: um crash durante o upload ou a
    cifragem de um arquivo em staging deixava lixo permanente no SSD, invisível
    para todas as reconciliações (que partem do banco) e consumindo espaço que o
    orçamento do cache não enxerga."""
    dirs = list(STORAGE_VOLUMES)
    if SSD_CACHE_DIR and SSD_CACHE_DIR not in dirs:
        dirs.append(SSD_CACHE_DIR)
    return dirs


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


# Tentativas de mover um arquivo SSD → HDD antes de estacionar a pendência.
MAX_SSD_MOVE_RETRIES = 5


def _park_ssd_move(move, reason: str) -> None:
    """Desiste de mover por ora, sem perder nada.

    A pendência fica no banco com retry_count >= MAX_SSD_MOVE_RETRIES: o worker e
    o monitor passam a ignorá-la, e a reconciliação não a recria (ela ainda
    existe). O arquivo continua no SSD como cópia registrada e legível — a falha
    é do destino, não do conteúdo. Antes, este ponto marcava como 'failed' toda
    versão que referenciava o sha256, e a limpeza noturna apagava essas versões.
    unpark_ssd_moves() devolve as pendências à fila (chamado no boot)."""
    log.error(
        f"[ssd-cache] {move.sha256[:8]}… {reason} após {move.retry_count} tentativa(s) — "
        f"move estacionado; o arquivo segue no SSD ({move.ssd_path}) e será retentado "
        f"no próximo reinício do servidor"
    )


def unpark_ssd_moves(db) -> int:
    """Devolve à fila as pendências estacionadas. Retorna quantas foram reativadas."""
    from database import SsdCachePendingMove
    n = (db.query(SsdCachePendingMove)
         .filter(SsdCachePendingMove.retry_count >= MAX_SSD_MOVE_RETRIES)
         .update({"retry_count": 0}, synchronize_session=False))
    if n:
        db.commit()
        log.info(f"[ssd-cache] {n} move(s) estacionado(s) devolvido(s) à fila")
    return n


def active_ssd_moves_query(db):
    """Pendências que o worker ainda deve tentar (exclui as estacionadas)."""
    from database import SsdCachePendingMove
    return db.query(SsdCachePendingMove).filter(
        SsdCachePendingMove.retry_count < MAX_SSD_MOVE_RETRIES
    )


def _create_pending_move_for_ssd_copy(db, ssd_copy) -> bool:
    """Cria o SsdCachePendingMove de um arquivo que está apenas no SSD.
    Devolve True se a pendência foi criada.

    Compartilhado pela recuperação de startup e pela reconciliação: antes, só a
    primeira sabia consertar esse estado, então um arquivo no SSD sem cópia no
    HDD e sem pendência ficava parado até o próximo reinício do servidor."""
    from database import FileContent, SsdCachePendingMove
    sha256 = ssd_copy.sha256
    fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
    if fc and fc.stored_at != ssd_copy.stored_at and Path(fc.stored_at).exists():
        return False  # FileContent já aponta para um arquivo fora do SSD
    try:
        dest_volume = pick_volume()
    except (RuntimeError, StorageThresholdExceeded):
        log.error(f"[ssd-cache] {sha256[:8]}… nenhum volume HDD disponível para criar o move pendente")
        return False
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
        log.warning(f"[ssd-cache] {sha256[:8]}… erro ao criar move pendente: {e}")
        return False
    log.info(f"[ssd-cache] {sha256[:8]}… move pendente criado → {dest_path}")
    return True


def recover_stuck_ssd_files(db) -> int:
    """Cria SsdCachePendingMove para arquivos presos no SSD sem move pendente e sem cópia HDD.
    Chamado no startup para recuperar de uploads interrompidos."""
    if not SSD_CACHE_DIR:
        return 0
    from database import FileContentCopy, SsdCachePendingMove
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
        # Sem cópia HDD — criar move pendente
        if _create_pending_move_for_ssd_copy(db, ssd_copy):
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
        elif _create_pending_move_for_ssd_copy(db, ssd_copy):
            # Arquivo só existe no SSD e não havia ninguém para movê-lo. Antes
            # isto era apenas um warning e o arquivo ficava no SSD até o próximo
            # reinício — recover_stuck_ssd_files, que conserta o mesmo estado, só
            # roda no startup.
            log.warning(f"[ssd-cache] reconciliação: {sha256[:8]}… arquivo no SSD sem cópia HDD "
                        f"e sem move pendente — pendência recriada")
            fixed += 1
        else:
            log.error(f"[ssd-cache] reconciliação: {sha256[:8]}… arquivo no SSD sem cópia HDD e sem move "
                      f"pendente — não foi possível recriar a pendência")
    return fixed


def process_ssd_pending_moves(db) -> tuple[int, list[str]]:
    """Move up to 10 pending SSD-cached files to their HDD destination. Returns (count, sha256s) moved."""
    from database import SsdCachePendingMove, FileContent, FileContentCopy
    from sqlalchemy.exc import IntegrityError
    # Collect only sha256 keys upfront; commits inside the loop expire session objects,
    # so we re-query each row fresh to avoid "Instance has been deleted" errors.
    pending_sha256s = [m.sha256 for m in active_ssd_moves_query(db).limit(10).all()]
    completed = 0
    moved_sha256s: list[str] = []
    for sha256 in pending_sha256s:
        move = db.query(SsdCachePendingMove).filter(SsdCachePendingMove.sha256 == sha256).first()
        if move is None:
            continue  # processed by concurrent worker
        ssd_path = Path(move.ssd_path)
        ssd_path_str = move.ssd_path   # capturado antes: `move` expira em rollback
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
                    if move.retry_count >= MAX_SSD_MOVE_RETRIES:
                        _park_ssd_move(move, "cópia no HDD não confere (hash)")
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
                # dest_path NÃO é removido aqui (era, antes): é o caminho
                # content-addressed do sha256 e o conteúdo acabou de ser conferido
                # por hash. Se o conflito veio de um worker concorrente que já
                # registrou essa mesma cópia, apagar o arquivo destruiria a única
                # cópia que o banco diz existir — mesmo motivo documentado em
                # _store_new_content. Só é apagado quando o próprio FileContent
                # sumiu (abaixo), aí não há registro para proteger.
                fc_exists = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
                stale = db.query(SsdCachePendingMove).filter(SsdCachePendingMove.sha256 == sha256).first()
                if fc_exists is None:
                    # Orphan cleanup deletou o FileContent no meio do move.
                    if stale:
                        db.delete(stale)
                        db.commit()
                    dest_path.unlink(missing_ok=True)
                    ssd_path.unlink(missing_ok=True)
                    log.info(f"[ssd-cache] {sha256[:8]}… FileContent removido pelo orphan cleanup — move cancelado, arquivo SSD removido")
                    break
                if stale is None:
                    # Worker concorrente venceu e já removeu a pendência.
                    log.debug(f"[ssd-cache] {sha256[:8]}… já processado por worker concorrente — OK")
                    break
                # A pendência sobreviveu ao conflito. Se a cópia do destino já está
                # registrada, uma tentativa anterior gravou tudo menos a remoção da
                # linha de move (o commit do `finally` do worker persiste esse
                # meio-estado quando ensure_replicas levanta exceção). Sem
                # tratamento, toda tentativa seguinte batia na constraint e saía
                # por aqui sem contar retry — o arquivo ficava preso no SSD para
                # sempre e o monitor abria um job de move a cada 30s.
                hdd_copy = (db.query(FileContentCopy)
                            .filter(FileContentCopy.sha256 == sha256,
                                    FileContentCopy.stored_at == str(dest_path))
                            .first())
                if hdd_copy is None:
                    # Conflito em outra linha (réplica) — conta a tentativa para
                    # não retentar indefinidamente.
                    stale.retry_count += 1
                    log.warning(f"[ssd-cache] {sha256[:8]}… conflito de integridade sem cópia registrada "
                                f"no destino — retry {stale.retry_count}")
                    if stale.retry_count >= MAX_SSD_MOVE_RETRIES:
                        _park_ssd_move(stale, "conflito de integridade no destino")
                    db.commit()
                    break
                # Cópia do destino registrada e verificada: conclui a escrituração
                # em vez de copiar de novo. ensure_replicas não é chamado aqui de
                # propósito — foi justamente ele que provavelmente falhou na
                # tentativa anterior, e uma exceção dentro deste except abortaria
                # o lote inteiro; réplicas faltantes são responsabilidade do job
                # reconcile-replication.
                if fc_exists.stored_at == ssd_path_str:
                    fc_exists.stored_at = str(dest_path)
                ssd_copy = (db.query(FileContentCopy)
                            .filter(FileContentCopy.sha256 == sha256,
                                    FileContentCopy.stored_at == ssd_path_str)
                            .first())
                if ssd_copy:
                    db.delete(ssd_copy)
                db.delete(stale)
                db.commit()
                ssd_path.unlink(missing_ok=True)
                completed += 1
                moved_sha256s.append(sha256)
                log.info(f"[ssd-cache] {sha256[:8]}… move concluído — cópia no destino já registrada "
                         f"por tentativa anterior ({dest_path})")
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
                if move.retry_count >= MAX_SSD_MOVE_RETRIES:
                    _park_ssd_move(move, f"erro de E/S ({e})")
                db.commit()
                break
    return completed, moved_sha256s
