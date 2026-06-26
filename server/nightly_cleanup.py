"""Rotina de limpeza noturna com política de retenção progressiva de versões."""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from database import SessionLocal, BackupID, BackupVersion, FileContent, FileContentCopy, VersionFile, MaintenanceJob, engine
from sqlalchemy import func, select, text
from cache_state import invalidate_activity

log = logging.getLogger("backup-server")

_SIX_HOURS  = timedelta(hours=6)
_ONE_DAY    = timedelta(hours=24)
_ONE_MONTH  = timedelta(days=30)
_SIX_MONTHS = timedelta(days=180)
_BATCH      = 50


def _delete_versions(db, version_ids: list[int]) -> None:
    """Deleta versões e seus VersionFiles em lotes."""
    for i in range(0, len(version_ids), _BATCH):
        batch = version_ids[i:i + _BATCH]
        db.query(VersionFile).filter(VersionFile.version_id.in_(batch)).delete(synchronize_session=False)
        db.query(BackupVersion).filter(BackupVersion.id.in_(batch)).delete(synchronize_session=False)
        db.commit()


def _cleanup_orphan_contents(db) -> tuple[int, int]:
    """Remove FileContents sem referência e seus arquivos físicos. Retorna (removidos, bytes_liberados)."""
    used_shas = db.query(VersionFile.sha256).distinct().subquery()
    orphans = db.query(FileContent).filter(~FileContent.sha256.in_(select(used_shas))).all()

    bytes_freed = 0
    safe_to_delete: list[FileContent] = []
    orphan_shas = [fc.sha256 for fc in orphans]
    copies_by_sha: dict[str, list] = {}
    if orphan_shas:
        for c in db.query(FileContentCopy).filter(FileContentCopy.sha256.in_(orphan_shas)).all():
            copies_by_sha.setdefault(c.sha256, []).append(c)

    for fc in orphans:
        copies = copies_by_sha.get(fc.sha256, [])
        failed = False
        for copy in copies:
            p = Path(copy.stored_at)
            try:
                p.unlink()
            except FileNotFoundError:
                pass
            except OSError as e:
                log.warning(f"[nightly-cleanup] Não foi possível remover {p}: {e} — pulando")
                failed = True
                continue
            db.delete(copy)
        if failed:
            continue
        if not copies:
            p = Path(fc.stored_at)
            try:
                p.unlink()
            except FileNotFoundError:
                pass
            except OSError as e:
                log.warning(f"[nightly-cleanup] Não foi possível remover {p}: {e} — pulando")
                continue
        bytes_freed += fc.size
        safe_to_delete.append(fc)

    if safe_to_delete:
        db.flush()
        for fc in safe_to_delete:
            db.delete(fc)
    db.commit()
    return len(safe_to_delete), bytes_freed


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


def validate_latest_versions_integrity(db, log_fn=None) -> dict:
    """Verifica se todos os arquivos das últimas versões 'done' existem no disco.
    Remove registros de arquivos ausentes e invalida versões afetadas.
    log_fn(msg) é chamado em cada evento relevante para reportar progresso em tempo real."""

    def _log(msg: str) -> None:
        log.info(msg)
        if log_fn:
            log_fn(msg)

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
    invalidated = 0
    files_removed = 0
    labels: list[str] = []
    exists_cache: dict[str, bool] = {}

    def _exists(sha256: str) -> bool:
        if sha256 in exists_cache:
            return exists_cache[sha256]
        fc = db.query(FileContent).filter(FileContent.sha256 == sha256).first()
        if fc and Path(fc.stored_at).exists():
            exists_cache[sha256] = True
            return True
        copies = db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256).all()
        found = any(Path(c.stored_at).exists() for c in copies)
        exists_cache[sha256] = found
        return found

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
        missing = [s for s in sha256s if not _exists(s)]
        if not missing:
            _log(f"[integrity] ({checked}/{total}) {version.backup_label}/{version.version_key} — OK ({len(sha256s)} arquivo(s))")
            continue

        # Coleta todas as versões que referenciam os arquivos ausentes ANTES de deletar
        affected_ids: set[int] = set()
        for sha256 in missing:
            ids = [
                r[0] for r in db.query(VersionFile.version_id)
                .filter(VersionFile.sha256 == sha256)
                .distinct()
                .all()
            ]
            affected_ids.update(ids)
            db.query(VersionFile).filter(VersionFile.sha256 == sha256).delete(synchronize_session=False)
            db.query(FileContentCopy).filter(FileContentCopy.sha256 == sha256).delete(synchronize_session=False)
            db.query(FileContent).filter(FileContent.sha256 == sha256).delete(synchronize_session=False)
            files_removed += 1
            _log(f"[integrity] {sha256[:8]}… removido do banco (arquivo ausente no disco)")

        # Invalida todas as versões afetadas (não só a latest)
        affected_versions = (
            db.query(BackupVersion)
            .filter(BackupVersion.id.in_(affected_ids), BackupVersion.status == "done")
            .all()
        )
        for av in affected_versions:
            av.status = "failed"
            av.finished_at = datetime.now()
            invalidated += 1
            if av.backup_label not in labels:
                labels.append(av.backup_label)
            _log(
                f"[integrity] versão {av.backup_label}/{av.version_key} "
                f"invalidada — arquivo(s) ausente(s) no disco"
            )
        db.commit()

    _log(f"[integrity] concluído: {checked} verificadas, {invalidated} invalidadas, {files_removed} arquivo(s) removidos")
    return {"checked": checked, "invalidated": invalidated, "files_removed": files_removed, "labels": labels}


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

        total_stale   = 0
        total_day     = 0
        total_week    = 0
        total_month   = 0
        labels_touched = 0

        for idx, label in enumerate(labels, 1):
            mj = db.get(MaintenanceJob, mj_id)
            if mj:
                mj.summary = f"Processando label {idx} / {total_labels}: {label}"
                db.commit()
                invalidate_activity()

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

            # 1. Limpar stale (failed/incomplete) com mais de 1 semana que tenham done mais recente
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

            if stale_to_delete or done_to_delete:
                labels_touched += 1

        total_removed = total_stale + total_day + total_week + total_month

        # Limpeza de conteúdos órfãos após todas as exclusões
        mj = db.get(MaintenanceJob, mj_id)
        if mj:
            mj.summary = "Limpando arquivos órfãos..."
            db.commit()
            invalidate_activity()
        orphans_removed, bytes_freed = _cleanup_orphan_contents(db)

        # Validação de integridade das últimas versões done
        mj = db.get(MaintenanceJob, mj_id)
        if mj:
            mj.summary = "Verificando integridade das últimas versões..."
            db.commit()
            invalidate_activity()
        integrity = validate_latest_versions_integrity(db)
        if integrity["invalidated"]:
            _integrity_note = (
                f"; integridade: {integrity['invalidated']}/{integrity['checked']} "
                f"versões invalidadas, {integrity['files_removed']} arquivo(s) removido(s)"
            )
        elif integrity["checked"] > 0:
            _integrity_note = f"; integridade: {integrity['checked']} versões OK"
        else:
            _integrity_note = ""

        removed_parts = []
        if total_stale:
            removed_parts.append(f"{total_stale} stale (failed/incomplete)")
        if total_day:
            removed_parts.append(f"{total_day} done por dia")
        if total_week:
            removed_parts.append(f"{total_week} done por semana")
        if total_month:
            removed_parts.append(f"{total_month} done por mês")

        stale_running_note = (
            f"; {total_stale_running} running sem atividade 6h+ → incomplete"
            if total_stale_running else ""
        )

        if total_removed:
            summary = (
                f"{total_removed} versão(ões) removida(s) em {labels_touched} label(s)"
                + (f": {', '.join(removed_parts)}" if removed_parts else "")
                + (f"; {orphans_removed} arquivo(s) de storage liberado(s) ({round(bytes_freed/1024/1024, 1)} MB)" if orphans_removed else "")
                + _integrity_note
                + stale_running_note
            )
            log.info(f"[nightly-cleanup] {summary}")
        else:
            summary = "Nenhuma versão removida — política de retenção satisfeita" + _integrity_note + stale_running_note
            log.info(f"[nightly-cleanup] {summary}")

        mj = db.get(MaintenanceJob, mj_id)
        if mj:
            mj.status = "done"
            mj.finished_at = datetime.now()
            mj.summary = summary
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
            raw = engine.raw_connection()
            raw.isolation_level = None  # autocommit — VACUUM não pode rodar dentro de transação
            raw.execute("VACUUM")
            raw.close()
            log.info("[nightly-cleanup] VACUUM concluído — espaço em disco liberado")
        except Exception as exc:
            log.warning("[nightly-cleanup] Falha ao executar VACUUM (não crítico): %s", exc)
