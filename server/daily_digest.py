"""
Daily digest do NestVault: coleta atividade do dia, gera resumo com IA e envia via Telegram.

Variáveis de ambiente:
  TELEGRAM_BOT_TOKEN  — token do bot (obrigatório para envio)
  TELEGRAM_CHAT_ID    — chat_id do destinatário (obrigatório para envio)
  ANTHROPIC_API_KEY   — usa Claude Haiku se definida; caso contrário tenta Ollama
  OLLAMA_URL          — URL do Ollama local (default: http://localhost:11434)
  OLLAMA_MODEL        — modelo Ollama (default: llama3)
"""
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import func

from database import (
    CloudBackupJob,
    CloudCredential,
    FileContent,
    BackupVersion,
    SessionLocal,
)

log = logging.getLogger("backup-server")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
OLLAMA_URL         = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL       = os.getenv("OLLAMA_MODEL", "llama3")


def _today_utc_range() -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _collect_stats() -> dict:
    db = SessionLocal()
    try:
        start, end = _today_utc_range()

        versions = (
            db.query(BackupVersion)
            .filter(BackupVersion.created_at >= start, BackupVersion.created_at < end)
            .all()
        )
        by_status: dict[str, int] = {}
        for v in versions:
            by_status[v.status] = by_status.get(v.status, 0) + 1

        files_row = db.query(
            func.count(FileContent.sha256),
            func.coalesce(func.sum(FileContent.size), 0),
        ).filter(
            FileContent.created_at >= start,
            FileContent.created_at < end,
        ).first()

        cloud_jobs = (
            db.query(CloudBackupJob, CloudCredential)
            .join(CloudCredential, CloudBackupJob.credential_id == CloudCredential.id)
            .filter(
                CloudBackupJob.last_run_at >= start,
                CloudBackupJob.last_run_at < end,
            )
            .all()
        )

        return {
            "date": start.strftime("%d/%m/%Y"),
            "backups": {
                "total": len(versions),
                "by_status": by_status,
                "labels": list({v.backup_label for v in versions}),
            },
            "storage": {
                "new_files": int(files_row[0] or 0),
                "new_bytes": int(files_row[1] or 0),
                "new_bytes_human": _fmt_bytes(int(files_row[1] or 0)),
            },
            "cloud_jobs": [
                {
                    "folder": job.folder_name,
                    "target": job.target_label,
                    "provider": cred.provider,
                    "account": cred.email,
                    "status": job.last_run_status,
                    "message": job.last_run_message,
                }
                for job, cred in cloud_jobs
            ],
        }
    finally:
        db.close()


def _fallback_message(stats: dict) -> str:
    b = stats["backups"]
    s = stats["storage"]
    c = stats["cloud_jobs"]
    lines = [f"*NestVault — Resumo {stats['date']}*\n"]

    if b["total"] == 0:
        lines.append("Nenhum backup realizado hoje.")
    else:
        status_str = ", ".join(f"{k}: {v}" for k, v in b["by_status"].items())
        lines.append(f"*Backups:* {b['total']} ({status_str})")
        lines.append(f"*Labels:* {', '.join(b['labels']) or '—'}")

    if s["new_files"] > 0:
        lines.append(f"*Novos arquivos:* {s['new_files']} ({s['new_bytes_human']})")

    if c:
        lines.append("\n*Jobs Cloud:*")
        for job in c:
            icon = "✅" if job["status"] == "success" else "❌"
            lines.append(f"  {icon} {job['folder']} → {job['target']} ({job['provider']})")
    else:
        lines.append("Nenhum job cloud executado hoje.")

    return "\n".join(lines)


async def _call_claude(stats: dict) -> str:
    prompt = (
        "Com base nos dados de atividade do NestVault abaixo, escreva um resumo amigável "
        "em português. Destaque o que funcionou bem e sinalize erros se houver. "
        "Seja conciso (máximo 10 linhas).\n\n"
        f"Dados:\n{json.dumps(stats, ensure_ascii=False, indent=2)}"
    )
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 512,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        r.raise_for_status()
        return r.json()["content"][0]["text"].strip()


async def _call_ollama(stats: dict) -> str:
    prompt = (
        "Você é um assistente que resume atividades de backup. "
        "Com base nos dados abaixo, escreva um resumo amigável em português do NestVault no dia. "
        "Destaque sucessos e erros. Seja conciso (máximo 10 linhas). Texto simples, sem markdown.\n\n"
        f"Dados:\n{json.dumps(stats, ensure_ascii=False, indent=2)}"
    )
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
        )
        r.raise_for_status()
        return r.json()["response"].strip()


async def _generate_summary(stats: dict) -> str:
    if ANTHROPIC_API_KEY:
        try:
            return await _call_claude(stats)
        except Exception as e:
            log.warning(f"[digest] Claude API falhou ({e}) — tentando Ollama")

    try:
        return await _call_ollama(stats)
    except Exception as e:
        log.warning(f"[digest] Ollama falhou ({e}) — usando resumo estruturado")

    return _fallback_message(stats)


async def _send_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("[digest] TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID não configurados — digest não enviado")
        return
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
        )
    if r.status_code == 200:
        log.info("[digest] Resumo enviado via Telegram")
    else:
        log.error(f"[digest] Telegram retornou {r.status_code}: {r.text}")


async def send_daily_digest() -> None:
    log.info("[digest] Gerando resumo diário...")
    try:
        stats = _collect_stats()
        summary = await _generate_summary(stats)
        await _send_telegram(summary)
    except Exception as e:
        log.error(f"[digest] Erro inesperado: {e}", exc_info=True)
