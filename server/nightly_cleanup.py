"""Rotina de limpeza noturna com política de retenção progressiva de versões."""

import hashlib
import logging
import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

from database import SessionLocal, BackupID, BackupVersion, FileContent, FileContentCopy, VersionFile, MaintenanceJob, SsdCachePendingMove, engine, TRASHED_STATUS
from sqlalchemy import func, delete, exists
from cache_state import invalidate_activity

log = logging.getLogger("backup-server")

_SIX_HOURS  = timedelta(hours=6)
_ONE_DAY    = timedelta(hours=24)
_ONE_MONTH  = timedelta(days=30)
_SIX_MONTHS = timedelta(days=180)
_BATCH      = 50

_TMP_PREFIXES = ("_cloud_tmp_", "_rclone_tmp_", "_tmp_", "_enc_")
# Prefixos de diretórios de staging temporários (download em lote do rclone).
_TMP_DIR_PREFIXES = ("_rclone_stage_",)


def _delete_versions(db, version_ids: list[int]) -> None:
    """Deleta versões e seus VersionFiles em lotes."""
    for i in range(0, len(version_ids), _BATCH):
        batch = version_ids[i:i + _BATCH]
        db.query(VersionFile).filter(VersionFile.version_id.in_(batch)).delete(synchronize_session=False)
        db.query(BackupVersion).filter(BackupVersion.id.in_(batch)).delete(synchronize_session=False)
        db.commit()


def purge_trash(db, cutoff: datetime | None) -> tuple[int, int]:
    """Apaga de vez o que está na lixeira há mais tempo que `cutoff` (None = tudo).

    Remove só as linhas: os arquivos ficam para _cleanup_orphan_contents, que só
    apaga conteúdo sem nenhuma outra referência. Um label sai junto com a última
    versão dele. Retorna (versões, labels) removidos."""
    q = db.query(BackupVersion.id).filter(BackupVersion.status == TRASHED_STATUS)
    if cutoff is not None:
        q = q.filter(BackupVersion.trashed_at < cutoff)
    version_ids = [r.id for r in q.all()]
    if version_ids:
        _delete_versions(db, version_ids)

    lq = db.query(BackupID).filter(BackupID.status == TRASHED_STATUS)
    if cutoff is not None:
        lq = lq.filter(BackupID.trashed_at < cutoff)
    labels_removed = 0
    for b in lq.all():
        if db.query(BackupVersion.id).filter(BackupVersion.backup_label == b.label).first() is None:
            db.delete(b)
            labels_removed += 1
    db.commit()
    if version_ids or labels_removed:
        log.info(f"[trash] {len(version_ids)} versão(ões) e {labels_removed} label(s) apagados da lixeira")
    return len(version_ids), labels_removed


def orphan_filter():
    """Predicado "este FileContent não é referenciado por nenhuma versão".

    NOT EXISTS correlacionado, não `NOT IN (SELECT DISTINCT sha256 ...)`: o
    segundo materializa o DISTINCT inteiro de version_files e o compara contra
    cada linha de file_contents — o mesmo anti-join que o CHANGELOG v7.12
    descreve como catastrófico no SQLite. Aqui cada linha faz uma sondagem
    pontual no índice idx_sha256 e para no primeiro acerto.
    """
    return ~exists().where(VersionFile.sha256 == FileContent.sha256)


def _cleanup_orphan_contents(db, limit: int | None = None) -> tuple[int, int]:
    """Remove FileContents sem referência e seus arquivos físicos. Retorna (removidos, bytes_liberados).

    Implementação canônica, compartilhada com main.py (era duplicada lá).
    Cada sha256 é deletado em sua própria mini-transação; não há um commit final
    agregado — callers que fazem db.commit() depois executam um no-op.

    Usa DELETE condicional por sha256 para eliminar a race condition TOCTOU: o banco
    re-verifica no momento da deleção se o sha256 ainda está sem referência, protegendo
    arquivos que foram re-referenciados por uploads concorrentes após o snapshot inicial.
    """
    q = db.query(FileContent).filter(orphan_filter())
    if limit is not None:
        q = q.limit(limit)
    candidates = q.all()

    if not candidates:
        return 0, 0

    # Extrai dados antes do commit (objetos expiram após db.commit())
    orphan_shas   = [fc.sha256 for fc in candidates]
    size_by_sha   = {fc.sha256: fc.size      for fc in candidates}
    stored_by_sha = {fc.sha256: fc.stored_at for fc in candidates}
    copies_by_sha: dict[str, list[dict]] = {}
    for c in db.query(FileContentCopy).filter(FileContentCopy.sha256.in_(orphan_shas)).all():
        copies_by_sha.setdefault(c.sha256, []).append({"id": c.id, "stored_at": c.stored_at})
    # SsdCachePendingMove é FK child de FileContent — deve ser deletado antes do parent.
    ssd_moves_by_sha: dict[str, str] = {}
    for m in db.query(SsdCachePendingMove).filter(SsdCachePendingMove.sha256.in_(orphan_shas)).all():
        ssd_moves_by_sha[m.sha256] = m.ssd_path

    # Encerra a transação de leitura: próximas operações enxergam o estado mais recente do DB,
    # inclusive VersionFiles commitados por uploads concorrentes desde o snapshot acima.
    db.commit()

    bytes_freed = 0
    removed = 0

    for sha256 in orphan_shas:
        copies   = copies_by_sha.get(sha256, [])
        copy_ids = [c["id"] for c in copies]
        try:
            # FK children de FileContent: deletar antes do parent (respeita PRAGMA foreign_keys=ON).
            if sha256 in ssd_moves_by_sha:
                db.query(SsdCachePendingMove).filter(
                    SsdCachePendingMove.sha256 == sha256
                ).delete(synchronize_session=False)
            if copy_ids:
                db.query(FileContentCopy).filter(
                    FileContentCopy.id.in_(copy_ids)
                ).delete(synchronize_session=False)

            # DELETE atômico: só remove se realmente não há VersionFile apontando para este sha256.
            result = db.execute(
                delete(FileContent).where(
                    (FileContent.sha256 == sha256) &
                    ~exists().where(VersionFile.sha256 == sha256)
                )
            )
            if result.rowcount == 0:
                # Re-referenciado por upload concurrent — desfaz deleção das cópias e segue.
                db.rollback()
                continue
            db.commit()
        except Exception:
            db.rollback()
            raise

        # Deleção física (best-effort): falha deixa arquivo órfão no disco, mas o DB é consistente.
        if sha256 in ssd_moves_by_sha:
            try:
                Path(ssd_moves_by_sha[sha256]).unlink()
            except (FileNotFoundError, OSError):
                pass
        for c in copies:
            try:
                Path(c["stored_at"]).unlink()
            except FileNotFoundError:
                pass
            except OSError as e:
                log.warning(f"[cleanup-orphans] Não foi possível remover {c['stored_at']}: {e}")
        if not copies:
            try:
                Path(stored_by_sha[sha256]).unlink()
            except FileNotFoundError:
                pass
            except OSError as e:
                log.warning(f"[cleanup-orphans] Não foi possível remover {stored_by_sha[sha256]}: {e}")

        bytes_freed += size_by_sha.get(sha256, 0)
        removed += 1

    return removed, bytes_freed


def _safe_glob(vol: Path, pattern: str) -> list[Path]:
    """vol.glob(pattern) que NÃO falha em silêncio.

    Path.glob engole o OSError do scandir por baixo: um volume desmontado ou
    com diretório ilegível rendia zero candidatos e zero linhas de log, e o
    resumo da noite ficava idêntico ao de uma varredura saudável.
    """
    try:
        return list(vol.glob(pattern))
    except OSError as e:
        log.warning(f"[cleanup-tmp] não foi possível listar {vol}/{pattern}: {e}")
        raise


def _cleanup_stale_tmp_files(volumes: list[Path], max_age_hours: float = 24.0) -> tuple[int, int, list[str]]:
    """Remove arquivos temporários órfãos com mais de max_age_hours horas.

    Retorna (removidos, bytes_liberados, volumes_que_falharam). Cada volume é
    isolado: um disco com problema não aborta a varredura dos outros nem
    derruba o job inteiro — mesma razão do isolamento por label em
    run_nightly_cleanup (um label ruim não pode impedir a limpeza dos demais).
    """
    cutoff = time.time() - max_age_hours * 3600
    removed = 0
    bytes_freed = 0
    volumes_failed: list[str] = []
    for vol in volumes:
        try:
            r, b = _sweep_one_volume(vol, cutoff)
            removed += r
            bytes_freed += b
        except Exception as e:
            log.exception(f"[cleanup-tmp] volume {vol} falhou — seguindo para o próximo")
            volumes_failed.append(f"{vol} ({e.__class__.__name__})")
    return removed, bytes_freed, volumes_failed


def _sweep_one_volume(vol: Path, cutoff: float) -> tuple[int, int]:
    """Varre um único volume. Levanta se a listagem falhar (o caller isola)."""
    removed = 0
    bytes_freed = 0

    # "_enc_*" (cifragem de upload/encrypt-existing/rclone) é criado dentro de
    # _content/<2-hex>/, não na raiz do volume — glob(f"{prefix}*") sozinho nunca via
    # esses arquivos, então um crash durante a cifragem deixava lixo permanente ali.
    candidates = _safe_glob(vol, "_content/*/_enc_*")
    for prefix in _TMP_PREFIXES:
        candidates += _safe_glob(vol, f"{prefix}*")
    for f in candidates:
        if not f.is_file():
            continue
        try:
            st = f.stat()
            if st.st_mtime < cutoff:
                bytes_freed += st.st_size
                f.unlink()
                removed += 1
                log.info(f"[cleanup-tmp] removido {f.name} ({st.st_size} bytes)")
        except FileNotFoundError:
            pass
        except OSError as e:
            log.warning(f"[cleanup-tmp] não foi possível remover {f}: {e}")

    # Diretórios de staging órfãos (e o arquivo-sidecar .files de mesmo prefixo).
    for prefix in _TMP_DIR_PREFIXES:
        for d in _safe_glob(vol, f"{prefix}*"):
            try:
                if d.stat().st_mtime >= cutoff:
                    continue
                if d.is_dir():
                    shutil.rmtree(d, ignore_errors=True)
                else:
                    d.unlink()
                removed += 1
                log.info(f"[cleanup-tmp] staging órfão removido: {d.name}")
            except FileNotFoundError:
                pass
            except OSError as e:
                log.warning(f"[cleanup-tmp] não foi possível remover {d}: {e}")

    return removed, bytes_freed


def _versions_to_keep(done_versions: list[BackupVersion], now: datetime) -> set[int]:
    """Calcula quais IDs de versões done devem ser preservadas pela política de retenção."""
    cutoff_day        = now - _ONE_DAY
    cutoff_month      = now - _ONE_MONTH
    cutoff_six_months = now - _SIX_MONTHS

    keep: set[int] = set()
    seen_days:   set = set()
    seen_weeks:  set[tuple] = set()
    seen_months: set[tuple] = set()

    # Iterar do mais recente para o mais antigo garante que a versão guardada por período é a mais nova
    for v in sorted(done_versions, key=lambda x: x.created_at, reverse=True):
        age = v.created_at

        if age >= cutoff_day:
            keep.add(v.id)

        elif age >= cutoff_month:
            day_key = age.date()
            if day_key not in seen_days:
                seen_days.add(day_key)
                keep.add(v.id)

        elif age >= cutoff_six_months:
            iso = age.isocalendar()
            week_key = (iso[0], iso[1])
            if week_key not in seen_weeks:
                seen_weeks.add(week_key)
                keep.add(v.id)

        else:
            month_key = (age.year, age.month)
            if month_key not in seen_months:
                seen_months.add(month_key)
                keep.add(v.id)

    return keep


def _version_fingerprint(db, version_id: int) -> str:
    """Hash do conjunto (original_path, sha256) da versão — identifica conteúdo idêntico."""
    h = hashlib.sha256()
    rows = (
        db.query(VersionFile.original_path, VersionFile.sha256)
        .filter(VersionFile.version_id == version_id)
        .order_by(VersionFile.original_path)
        .yield_per(1000)
    )
    for path, sha256 in rows:
        h.update(f"{path}\0{sha256}\n".encode())
    return h.hexdigest()


def _prune_unchanged_versions(db, done_versions: list[BackupVersion]) -> list[int]:
    """IDs de versões done idênticas à done imediatamente anterior do mesmo label.

    Recebe as versões já sobreviventes da política de retenção, ordenadas por
    created_at ascendente. Preserva a primeira versão de cada bloco de conteúdo
    idêntico (onde a mudança apareceu) e sempre a última done do label, mesmo
    que idêntica — restore, /files e a validação de integridade dependem dela.
    """
    if len(done_versions) < 2:
        return []

    ordered = sorted(done_versions, key=lambda v: (v.created_at, v.id))
    last_id = ordered[-1].id

    to_delete: list[int] = []
    prev_fp: str | None = None
    for v in ordered:
        fp = _version_fingerprint(db, v.id)
        if prev_fp is not None and fp == prev_fp and v.id != last_id:
            to_delete.append(v.id)
        else:
            prev_fp = fp
    return to_delete


PRESENT, MISSING, UNKNOWN = "present", "missing", "unknown"


def untrusted_volumes(db) -> list[Path]:
    """Volumes cuja resposta 'esse arquivo não existe' NÃO pode ser levada a sério.

    Duas fontes: os degradados (statvfs falhou) e os que o banco diz conter
    cópias mas não mostram nenhuma (storage.volume_looks_sane). Refresca a
    saúde antes de ler o set — run_nightly_cleanup roda às 00:00 e, sem isto,
    _degraded_volumes pode estar horas obsoleto: safe_disk_usage é o único
    writer do set, e o caminho de limpeza nunca o chamava.
    """
    import storage

    bad: list[Path] = []
    for v in storage.STORAGE_VOLUMES:
        storage.safe_disk_usage(v)          # atualiza _degraded_volumes
        if v in storage._degraded_volumes:
            bad.append(v)
        elif not storage.volume_looks_sane(v, db):
            bad.append(v)
    return bad


def validate_latest_versions_integrity(db, log_fn=None, untrusted=None) -> dict:
    """Verifica se os arquivos das últimas versões 'done' existem no disco.

    Coloca em QUARENTENA (sem apagar nada) os conteúdos cuja ausência foi
    provada, e marca as versões afetadas com integrity_status='suspect'.

    A versão anterior desta função apagava VersionFile + FileContentCopy +
    FileContent e marcava as versões como 'failed', tudo a partir de um
    Path.exists() — que engole OSError e devolve False tanto para "apagado"
    quanto para "volume fora do ar". Um disco desmontado às 00:00 destruía
    assim as linhas das réplicas BOAS em discos saudáveis, e como toda
    reconciliação parte de FileContent, remontar o disco não restaurava nada.

    log_fn(msg) é chamado em cada evento relevante para reportar progresso em tempo real."""

    def _log(msg: str) -> None:
        log.info(msg)
        if log_fn:
            log_fn(msg)

    if untrusted is None:
        untrusted = untrusted_volumes(db)
    untrusted_str = {str(v) for v in untrusted}
    if untrusted_str:
        # Com um volume fora do ar não dá para provar a ausência de NADA: a
        # cópia sobrevivente pode estar justamente nele. Melhor não verificar
        # do que verificar errado e destruir metadados.
        msg = ("[integrity] PULADA — volume(s) não confiável(is): "
               + ", ".join(sorted(untrusted_str))
               + ". Nenhum conteúdo foi avaliado.")
        _log(msg)
        return {"checked": 0, "quarantined": 0, "invalidated": 0, "files_removed": 0,
                "labels": [], "skipped": True, "untrusted": sorted(untrusted_str)}

    max_ts_sq = (
        db.query(
            BackupVersion.backup_label,
            func.max(BackupVersion.created_at).label("max_ts"),
        )
        .filter(BackupVersion.status == "done")
        .group_by(BackupVersion.backup_label)
        .subquery()
    )
    latest_versions = (
        db.query(BackupVersion)
        .join(
            max_ts_sq,
            (BackupVersion.backup_label == max_ts_sq.c.backup_label)
            & (BackupVersion.created_at == max_ts_sq.c.max_ts)
            & (BackupVersion.status == "done"),
        )
        .all()
    )

    total = len(latest_versions)
    _log(f"[integrity] {total} label(s) para verificar")

    checked = 0
    quarantined = 0
    labels: list[str] = []
    state_cache: dict[str, str] = {}

    def _probe(path: str) -> str:
        """PRESENT / MISSING / UNKNOWN para um caminho.

        Path.exists() não serve aqui: ele engole todo OSError e colapsa
        "não existe" com "não deu para perguntar". os.stat separa os dois —
        só FileNotFoundError (ENOENT) é prova de ausência; EIO, ENODEV,
        EACCES e afins são desconhecimento, não evidência.
        """
        try:
            os.stat(path)
            return PRESENT
        except FileNotFoundError:
            return MISSING
        except OSError as e:
            log.warning(f"[integrity] {path} ilegível ({e.__class__.__name__}: {e}) — tratado como desconhecido")
            return UNKNOWN

    def _content_state(sha256: str) -> str:
        """Estado agregado das cópias de um conteúdo.

        PRESENT se QUALQUER cópia existe. MISSING só se TODAS as cópias
        responderam ENOENT — uma única UNKNOWN já invalida a conclusão.
        """
        if sha256 in state_cache:
            return state_cache[sha256]

        paths: list[str] = []
        fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
        if fc:
            paths.append(fc.stored_at)
        paths += [
            r[0] for r in db.query(FileContentCopy.stored_at)
            .filter(FileContentCopy.sha256 == sha256).all()
        ]

        if not paths:
            state = MISSING
        else:
            states = {_probe(p) for p in dict.fromkeys(paths)}
            if PRESENT in states:
                state = PRESENT
            elif UNKNOWN in states:
                state = UNKNOWN
            else:
                state = MISSING

        state_cache[sha256] = state
        return state

    for version in latest_versions:
        checked += 1
        _log(f"[integrity] ({checked}/{total}) verificando {version.backup_label}/{version.version_key}...")
        sha256s = [
            r[0]
            for r in db.query(VersionFile.sha256)
            .filter(VersionFile.version_id == version.id)
            .distinct()
            .all()
        ]
        states = {s: _content_state(s) for s in sha256s}
        missing = [s for s, st in states.items() if st == MISSING]
        unknown = [s for s, st in states.items() if st == UNKNOWN]

        if unknown:
            _log(f"[integrity] ({checked}/{total}) {version.backup_label}/{version.version_key} — "
                 f"{len(unknown)} arquivo(s) não verificável(is) (I/O); nada foi alterado")
        if not missing:
            if not unknown:
                _log(f"[integrity] ({checked}/{total}) {version.backup_label}/{version.version_key} — OK ({len(sha256s)} arquivo(s))")
            continue

        # Quarentena, não deleção. Preserva FileContent/FileContentCopy/VersionFile
        # para que a perda seja reversível: remontar o disco ou rodar
        # tools/rebuild_content_index.py readota as cópias a partir destas linhas.
        affected_ids: set[int] = set()
        now = datetime.now()
        for sha256 in missing:
            ids = [
                r[0] for r in db.query(VersionFile.version_id)
                .filter(VersionFile.sha256 == sha256)
                .distinct()
                .all()
            ]
            affected_ids.update(ids)
            fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
            if fc and fc.quarantined_at is None:
                fc.quarantined_at = now
                fc.quarantine_reason = "ausente em todos os volumes confiáveis"
                quarantined += 1
                _log(f"[integrity] {sha256[:8]}… em QUARENTENA (ausente no disco; registros preservados)")

        affected_versions = (
            db.query(BackupVersion)
            .filter(BackupVersion.id.in_(affected_ids), BackupVersion.status == "done")
            .all()
        )
        for av in affected_versions:
            if av.integrity_status != "suspect":
                av.integrity_status = "suspect"
                if av.backup_label not in labels:
                    labels.append(av.backup_label)
                _log(
                    f"[integrity] versão {av.backup_label}/{av.version_key} "
                    f"marcada como suspeita — arquivo(s) ausente(s) no disco"
                )
        db.commit()

    _log(f"[integrity] concluído: {checked} verificadas, {quarantined} conteúdo(s) em quarentena")
    return {"checked": checked, "quarantined": quarantined, "invalidated": 0,
            "files_removed": 0, "labels": labels, "skipped": False, "untrusted": []}


def run_nightly_cleanup() -> None:
    """Executa a limpeza noturna de versões conforme política de retenção."""
    db = SessionLocal()
    mj = MaintenanceJob(
        job_type="nightly-cleanup",
        status="running",
        summary="Iniciando limpeza noturna...",
    )
    db.add(mj)
    db.commit()
    db.refresh(mj)
    mj_id = mj.id
    invalidate_activity()
    try:
        now = datetime.now()

        log.info("[nightly-cleanup] iniciando limpeza noturna")

        # 0. Marcar versões "running" sem atividade de arquivos há mais de 6h como incompletas
        _cutoff_activity = now - _SIX_HOURS
        _last_file_subq = (
            db.query(
                VersionFile.version_id,
                func.max(VersionFile.created_at).label("last_file_at"),
            )
            .group_by(VersionFile.version_id)
            .subquery()
        )
        stale_running = (
            db.query(BackupVersion)
            .outerjoin(_last_file_subq, BackupVersion.id == _last_file_subq.c.version_id)
            .filter(
                BackupVersion.status == "running",
                func.coalesce(_last_file_subq.c.last_file_at, BackupVersion.created_at)
                < _cutoff_activity,
            )
            .all()
        )
        total_stale_running = len(stale_running)
        if stale_running:
            for v in stale_running:
                v.status = "incomplete"
                v.finished_at = now
            db.commit()
            log.info(
                f"[nightly-cleanup] {total_stale_running} versão(ões) 'running' "
                f"sem atividade de arquivos há 6h+ marcada(s) como 'incomplete'"
            )

        labels = [row[0] for row in db.query(BackupID.label).all()]
        total_labels = len(labels)

        total_stale     = 0
        total_day       = 0
        total_week      = 0
        total_month     = 0
        total_unchanged = 0
        labels_touched  = 0
        labels_failed: list[str] = []

        for idx, label in enumerate(labels, 1):
            mj = db.get(MaintenanceJob, mj_id)
            if mj:
                mj.summary = f"Processando label {idx} / {total_labels}: {label}"
                db.commit()
                invalidate_activity()

            try:
                versions = (
                    db.query(BackupVersion)
                    .filter(BackupVersion.backup_label == label)
                    .order_by(BackupVersion.created_at.desc())
                    .all()
                )
                if not versions:
                    continue

                done_versions  = [v for v in versions if v.status == "done"]
                stale_versions = [v for v in versions if v.status in ("failed", "incomplete")]

                # Conjunto de datas das versões done para comparação
                done_dates = {v.created_at for v in done_versions}

                # 1. Limpar stale (failed/incomplete) assim que existir uma done MAIS NOVA
                # que elas. Uma tentativa que falhou e já foi sucedida por um backup completo
                # não descreve mais nenhum estado do label — só ocupa espaço e polui a lista
                # de versões. A exigência de "mais de 1 semana" da v9.0.0 fazia essas versões
                # se acumularem por semanas (visível sobretudo nos labels rclone, que falham
                # parcialmente com frequência) e foi removida.
                #
                # O "mais nova" continua sendo a trava de segurança: a falha mais recente do
                # label, sem nenhuma done depois dela, é preservada — é ela que descreve o
                # estado atual do backup e é dela que o rclone retoma (progress_json).
                stale_to_delete: list[int] = []
                for v in stale_versions:
                    if any(d > v.created_at for d in done_dates):
                        stale_to_delete.append(v.id)

                if stale_to_delete:
                    _delete_versions(db, stale_to_delete)
                    total_stale += len(stale_to_delete)
                    log.debug(f"[nightly-cleanup] {label}: {len(stale_to_delete)} versão(ões) stale removida(s)")

                # 2. Aplicar política de retenção nas versões done
                if not done_versions:
                    continue

                keep_ids = _versions_to_keep(done_versions, now)
                done_to_delete = [v.id for v in done_versions if v.id not in keep_ids]
                # Capturado antes de _delete_versions() abaixo: o commit() dela expira todos os
                # objetos da sessão, e reacessar atributos de uma instância já deletada explode
                # com ObjectDeletedError — então survivors precisa vir do keep_ids já calculado.
                survivors = [v for v in done_versions if v.id in keep_ids]

                if done_to_delete:
                    # Separar por período para contagem macro
                    for v in done_versions:
                        if v.id not in keep_ids:
                            if v.created_at < now - _SIX_MONTHS:
                                total_month += 1
                            elif v.created_at < now - _ONE_MONTH:
                                total_week += 1
                            else:
                                total_day += 1

                    _delete_versions(db, done_to_delete)
                    log.debug(f"[nightly-cleanup] {label}: {len(done_to_delete)} versão(ões) done removida(s) por retenção")

                # 3. Podar versões done sem alteração de conteúdo em relação à anterior
                # (preserva a primeira de cada bloco idêntico e sempre a última done do label)
                unchanged_to_delete = _prune_unchanged_versions(db, survivors)
                if unchanged_to_delete:
                    _delete_versions(db, unchanged_to_delete)
                    total_unchanged += len(unchanged_to_delete)
                    log.debug(f"[nightly-cleanup] {label}: {len(unchanged_to_delete)} versão(ões) sem alteração removida(s)")

                if stale_to_delete or done_to_delete or unchanged_to_delete:
                    labels_touched += 1
            except Exception:
                # Um label problemático não pode mais abortar a rotina inteira: antes,
                # qualquer erro aqui propagava para o except externo e todos os labels
                # seguintes (os rclone são os últimos criados, logo os últimos da fila)
                # ficavam sem limpeza naquela noite, silenciosamente.
                db.rollback()
                labels_failed.append(label)
                log.exception(
                    f"[nightly-cleanup] erro ao limpar o label {label} — "
                    f"seguindo para o próximo"
                )
                continue

        # Lixeira: o que venceu o prazo sai antes da limpeza de órfãos, para os
        # arquivos serem liberados ainda nesta rodada.
        import config
        trash_cutoff = now - timedelta(days=config.get("storage.trash_retention_days"))
        try:
            total_trash, trash_labels = purge_trash(db, trash_cutoff)
        except Exception:
            db.rollback()
            log.exception("[nightly-cleanup] erro ao esvaziar a lixeira — seguindo")
            total_trash, trash_labels = 0, 0

        total_removed = total_stale + total_day + total_week + total_month + total_unchanged + total_trash

        # Limpeza de conteúdos órfãos após todas as exclusões
        mj = db.get(MaintenanceJob, mj_id)
        if mj:
            mj.summary = "Limpando arquivos órfãos..."
            db.commit()
            invalidate_activity()
        orphans_removed, bytes_freed = _cleanup_orphan_contents(db)

        # Limpeza de arquivos temporários órfãos (mais de 24h)
        mj = db.get(MaintenanceJob, mj_id)
        if mj:
            mj.summary = "Limpando arquivos temporários órfãos..."
            db.commit()
            invalidate_activity()
        # Volumes não confiáveis são apurados uma vez e reusados pelas duas fases
        # seguintes: varrer ou julgar ausência num disco fora do ar é o que
        # destruía metadados.
        untrusted = untrusted_volumes(db)
        untrusted_str = {str(v) for v in untrusted}

        from storage import tmp_sweep_dirs
        sweep_dirs = [d for d in tmp_sweep_dirs() if str(d) not in untrusted_str]
        tmp_removed, tmp_bytes, tmp_failed = _cleanup_stale_tmp_files(sweep_dirs, max_age_hours=24.0)

        # Validação de integridade das últimas versões done.
        # Fases 5 e 6 envolvidas em try/except próprios: antes, qualquer erro
        # aqui subia para o except externo, marcava o job failed e dava raise —
        # e o VACUUM, que fica DEPOIS do try, nunca rodava.
        mj = db.get(MaintenanceJob, mj_id)
        if mj:
            mj.summary = "Verificando integridade das últimas versões..."
            db.commit()
            invalidate_activity()
        try:
            integrity = validate_latest_versions_integrity(db, untrusted=untrusted)
            _integrity_err = ""
        except Exception as e:
            db.rollback()
            log.exception("[nightly-cleanup] erro na validação de integridade — demais fases preservadas")
            integrity = {"checked": 0, "quarantined": 0, "labels": [], "skipped": True, "untrusted": []}
            _integrity_err = f"; ATENÇÃO: integridade falhou ({e.__class__.__name__}) — ver logs"

        if integrity.get("skipped") and integrity.get("untrusted"):
            _integrity_note = (
                "; ATENÇÃO: integridade PULADA — volume(s) não confiável(is): "
                + ", ".join(integrity["untrusted"])
            )
        elif integrity["quarantined"]:
            _integrity_note = (
                f"; integridade: {integrity['quarantined']} conteúdo(s) em quarentena "
                f"em {integrity['checked']} versão(ões) verificada(s) "
                f"({', '.join(integrity['labels'][:5])}) — nada foi apagado"
            )
        elif integrity["checked"] > 0:
            _integrity_note = f"; integridade: {integrity['checked']} versões OK"
        else:
            _integrity_note = ""
        _integrity_note += _integrity_err

        _tmp_note = (
            f"; {tmp_removed} arquivo(s) temporário(s) removido(s) ({round(tmp_bytes/1024/1024, 1)} MB)"
            if tmp_removed else ""
        )
        if tmp_failed:
            _tmp_note += f"; ATENÇÃO: varredura de temporários falhou em {', '.join(tmp_failed)}"
        if untrusted_str:
            _tmp_note += f"; volume(s) pulado(s) por não confiabilidade: {', '.join(sorted(untrusted_str))}"

        removed_parts = []
        if total_stale:
            removed_parts.append(f"{total_stale} stale (failed/incomplete)")
        if total_day:
            removed_parts.append(f"{total_day} done por dia")
        if total_week:
            removed_parts.append(f"{total_week} done por semana")
        if total_month:
            removed_parts.append(f"{total_month} done por mês")
        if total_unchanged:
            removed_parts.append(f"{total_unchanged} sem alteração")
        if total_trash:
            removed_parts.append(f"{total_trash} da lixeira (prazo vencido)")

        stale_running_note = (
            f"; {total_stale_running} running sem atividade 6h+ → incomplete"
            if total_stale_running else ""
        )

        # Falha por label é visível no resumo: sem isto, um label que nunca é
        # limpo (versões stale que não somem) não tem como ser percebido pela tela.
        failed_note = (
            f"; ATENÇÃO: {len(labels_failed)} label(s) com erro e sem limpeza "
            f"({', '.join(labels_failed[:5])}"
            + (f" +{len(labels_failed) - 5}" if len(labels_failed) > 5 else "")
            + ") — ver logs do servidor"
            if labels_failed else ""
        )

        if total_removed:
            summary = (
                f"{total_removed} versão(ões) removida(s) em {labels_touched} label(s)"
                + (f": {', '.join(removed_parts)}" if removed_parts else "")
                + (f"; {orphans_removed} arquivo(s) de storage liberado(s) ({round(bytes_freed/1024/1024, 1)} MB)" if orphans_removed else "")
                + _tmp_note
                + _integrity_note
                + stale_running_note
                + failed_note
            )
            log.info(f"[nightly-cleanup] {summary}")
        else:
            summary = ("Nenhuma versão removida — política de retenção satisfeita"
                       + _tmp_note + _integrity_note + stale_running_note + failed_note)
            log.info(f"[nightly-cleanup] {summary}")

        mj = db.get(MaintenanceJob, mj_id)
        if mj:
            mj.status = "done"
            mj.finished_at = datetime.now()
            mj.summary = summary
            mj.bytes_freed = bytes_freed + tmp_bytes
            db.commit()
        invalidate_activity()

    except Exception:
        log.exception("[nightly-cleanup] Erro durante limpeza noturna")
        try:
            mj = db.get(MaintenanceJob, mj_id)
            if mj:
                mj.status = "failed"
                mj.finished_at = datetime.now()
                mj.summary = "Erro durante execução — ver logs do servidor"
                db.commit()
            invalidate_activity()
        except Exception:
            pass
        raise
    finally:
        db.close()

    # VACUUM fora de transação para compactar o arquivo SQLite.
    # O SQLite só libera espaço em disco com VACUUM — deletar linhas apenas
    # marca páginas como livres na freelist, sem encolher o arquivo.
    if engine.dialect.name != "sqlite":
        log.info("[nightly-cleanup] Backend não-SQLite — VACUUM ignorado")
    else:
        try:
            # AUTOCOMMIT pela API do SQLAlchemy, não mutando o driver: a versão
            # anterior fazia `raw.isolation_level = None` numa conexão do pool e
            # nunca restaurava o valor. A conexão voltava para o pool em
            # autocommit permanente, e a partir daí qualquer sessão que a pegasse
            # rodava sem transação — commit/rollback viravam no-op, inclusive o
            # rollback de proteção de _cleanup_orphan_contents. execution_options
            # restaura o nível original ao devolver a conexão ao pool.
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.exec_driver_sql("VACUUM")
            log.info("[nightly-cleanup] VACUUM concluído — espaço em disco liberado")
        except Exception as exc:
            log.warning("[nightly-cleanup] Falha ao executar VACUUM (não crítico): %s", exc)
