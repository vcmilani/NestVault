"""Registro diário do uso total de armazenamento, usado no gráfico de flutuação de disco
ocupado da página de Estatísticas. Mantém uma amostra por dia (upsert) com retenção de 90 dias."""

import logging
from datetime import date, timedelta

import storage
from database import SessionLocal, DiskUsageDaily

log = logging.getLogger("backup-server")

_RETENTION_DAYS = 90


def record_disk_usage_snapshot() -> None:
    """Grava (ou atualiza) a amostra de uso de disco do dia atual e poda amostras antigas."""
    usages = [u for u in (storage.safe_disk_usage(v) for v in storage.STORAGE_VOLUMES) if u]
    if not usages:
        log.warning("[disk-history] nenhum volume de armazenamento acessível — amostra não registrada")
        return

    total_bytes = sum(u.total for u in usages)
    used_bytes = sum(u.used for u in usages)
    used_pct = round(used_bytes / total_bytes * 100, 2) if total_bytes else 0.0
    today = date.today().isoformat()

    db = SessionLocal()
    try:
        row = db.query(DiskUsageDaily).filter(DiskUsageDaily.date == today).first()
        if row:
            row.used_bytes = used_bytes
            row.total_bytes = total_bytes
            row.used_pct = used_pct
        else:
            db.add(DiskUsageDaily(
                date=today, used_bytes=used_bytes, total_bytes=total_bytes, used_pct=used_pct,
            ))

        cutoff = (date.today() - timedelta(days=_RETENTION_DAYS)).isoformat()
        db.query(DiskUsageDaily).filter(DiskUsageDaily.date < cutoff).delete(synchronize_session=False)

        db.commit()
        log.info(f"[disk-history] amostra de {today} registrada: {used_pct}% usado")
    finally:
        db.close()
