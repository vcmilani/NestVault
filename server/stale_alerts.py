"""Alerta de backup atrasado — dead-man switch por label (v10.2.0).

Cada label pode declarar o intervalo esperado entre backups
(BackupID.expected_interval_hours). O job periódico (scheduler.schedule_stale_check)
compara com a última versão done; passou do intervalo + alerts.stale_grace_pct
→ um aviso no Telegram. Quando o label volta a ter versão nova, um segundo aviso
de "voltou" fecha o episódio. stale_alerted_at garante um alerta por episódio.

Só labels de backup (escritos por cliente) entram: pasta do servidor do /cloud
não tem cadência. Jobs rclone entram de graça — gravam versões no target_label.
"""
import logging
from datetime import datetime, timedelta

from sqlalchemy import func, or_

import config
import notify
from database import BackupID, BackupVersion, SessionLocal, SERVER_KIND, TRASHED_STATUS

log = logging.getLogger("backup-server")


def deadline(interval_hours: int, last_ok: datetime) -> datetime:
    """Momento a partir do qual o label conta como atrasado."""
    grace = max(0, config.get("alerts.stale_grace_pct")) / 100
    return last_ok + timedelta(hours=interval_hours * (1 + grace))


def is_stale(b: BackupID, last_done_at: datetime | None, now: datetime | None = None) -> bool:
    """True se o label tem intervalo definido e passou do prazo. Sem nenhuma
    versão done, conta a partir da criação do label."""
    if not b.expected_interval_hours or b.expected_interval_hours <= 0:
        return False
    if (b.kind or "backup") == SERVER_KIND:
        return False
    ref = last_done_at or b.created_at
    if ref is None:
        return False
    return (now or datetime.now()) > deadline(b.expected_interval_hours, ref)


def _fmt_age(delta: timedelta) -> str:
    hours = delta.total_seconds() / 3600
    if hours < 48:
        return f"{hours:.0f}h"
    return f"{hours / 24:.1f} dias"


def check_stale_backups(now: datetime | None = None) -> dict:
    """Uma rodada do job. Devolve {"alerted": [...], "recovered": [...]} (para testes/log)."""
    now = now or datetime.now()
    alerted, recovered = [], []
    db = SessionLocal()
    try:
        labels = (db.query(BackupID)
                  .filter(BackupID.expected_interval_hours.isnot(None),
                          or_(BackupID.status.is_(None), BackupID.status != TRASHED_STATUS))
                  .all())
        if not labels:
            return {"alerted": alerted, "recovered": recovered}
        last_done = dict(
            db.query(BackupVersion.backup_label, func.max(BackupVersion.created_at))
            .filter(BackupVersion.backup_label.in_([b.label for b in labels]),
                    BackupVersion.status == "done")
            .group_by(BackupVersion.backup_label)
            .all()
        )
        for b in labels:
            last = last_done.get(b.label)
            stale = is_stale(b, last, now)
            if stale and b.stale_alerted_at is None:
                ref = last or b.created_at
                since = f"há {_fmt_age(now - ref)}" if last else f"desde a criação ({_fmt_age(now - ref)})"
                name = b.client_name or b.label
                text = (f"⚠️ NestVault: backup '{b.label}' ({name}) sem versão nova {since} "
                        f"— esperado a cada {b.expected_interval_hours}h.")
                notify.send_sync(text, tag="stale")
                b.stale_alerted_at = now
                alerted.append(b.label)
                log.warning(f"[stale] {b.label}: atrasado ({since})")
            elif not stale and b.stale_alerted_at is not None:
                # Voltou só quando há versão depois do alerta; label que apenas
                # perdeu o intervalo (alerta desligado) é limpo sem aviso.
                if last and last > b.stale_alerted_at:
                    notify.send_sync(f"✅ NestVault: backup '{b.label}' voltou — "
                                     f"última versão em {last:%d/%m %H:%M}.", tag="stale")
                    recovered.append(b.label)
                    log.info(f"[stale] {b.label}: voltou")
                b.stale_alerted_at = None
        db.commit()
    finally:
        db.close()
    return {"alerted": alerted, "recovered": recovered}
