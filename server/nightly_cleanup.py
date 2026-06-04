"""Rotina de limpeza noturna com política de retenção progressiva de versões."""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from database import SessionLocal, BackupID, BackupVersion, FileContent, FileContentCopy, VersionFile, MaintenanceJob
from sqlalchemy import select

log = logging.getLogger("backup-server")

_ONE_WEEK   = timedelta(days=7)
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
            if p.exists():
                try:
                    p.unlink()
                except OSError as e:
                    log.warning(f"[nightly-cleanup] Não foi possível remover {p}: {e} — pulando")
                    failed = True
                    continue
            db.delete(copy)
        if failed:
            continue
        if not copies:
            p = Path(fc.stored_at)
            if p.exists():
                try:
                    p.unlink()
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
    cutoff_month      = now - _ONE_MONTH
    cutoff_six_months = now - _SIX_MONTHS

    keep: set[int] = set()
    seen_weeks:  set[tuple] = set()
    seen_months: set[tuple] = set()

    # Iterar do mais recente para o mais antigo garante que a versão guardada por período é a mais nova
    for v in sorted(done_versions, key=lambda x: x.created_at, reverse=True):
        age = v.created_at

        if age >= cutoff_month:
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


def run_nightly_cleanup() -> None:
    """Executa a limpeza noturna de versões conforme política de retenção."""
    db = SessionLocal()
    try:
        now = datetime.now()
        stale_cutoff = now - _ONE_WEEK

        log.info("[nightly-cleanup] iniciando limpeza noturna")

        labels = [row[0] for row in db.query(BackupID.label).all()]

        total_stale   = 0
        total_week    = 0
        total_month   = 0
        labels_touched = 0

        for label in labels:
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
                if v.created_at >= stale_cutoff:
                    continue
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
                        else:
                            total_week += 1

                _delete_versions(db, done_to_delete)
                log.debug(f"[nightly-cleanup] {label}: {len(done_to_delete)} versão(ões) done removida(s) por retenção")

            if stale_to_delete or done_to_delete:
                labels_touched += 1

        total_removed = total_stale + total_week + total_month

        # Limpeza de conteúdos órfãos após todas as exclusões
        orphans_removed, bytes_freed = _cleanup_orphan_contents(db)

        summary_parts = []
        if total_stale:
            summary_parts.append(f"{total_stale} stale (failed/incomplete)")
        if total_week:
            summary_parts.append(f"{total_week} done por semana")
        if total_month:
            summary_parts.append(f"{total_month} done por mês")

        if total_removed:
            summary = (
                f"{total_removed} versão(ões) removida(s) em {labels_touched} label(s)"
                + (f": {', '.join(summary_parts)}" if summary_parts else "")
                + (f"; {orphans_removed} arquivo(s) de storage liberado(s) ({round(bytes_freed/1024/1024, 1)} MB)" if orphans_removed else "")
            )
            log.info(f"[nightly-cleanup] {summary}")
        else:
            summary = "Nenhuma versão removida — política de retenção satisfeita"
            log.info(f"[nightly-cleanup] {summary}")

        mj = MaintenanceJob(
            job_type="nightly-cleanup",
            status="done",
            finished_at=datetime.now(),
            summary=summary,
        )
        db.add(mj)
        db.commit()

    except Exception:
        log.exception("[nightly-cleanup] Erro durante limpeza noturna")
        try:
            mj = MaintenanceJob(
                job_type="nightly-cleanup",
                status="failed",
                finished_at=datetime.now(),
                summary="Erro durante execução — ver logs do servidor",
            )
            db.add(mj)
            db.commit()
        except Exception:
            pass
        raise
    finally:
        db.close()
