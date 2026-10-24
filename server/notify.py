"""Envio de alertas para o administrador (v10.2.0).

Hoje o único canal é o bot do Telegram já configurado para o digest diário
(digest.telegram_bot_token / digest.telegram_chat_id). Lê a config a cada envio,
então uma troca de token pela tela /settings vale na hora.

Sem token/chat_id configurado o alerta só vai para o log — nunca levanta: quem
chama (jobs do scheduler, requests) não deve falhar porque o Telegram caiu.
"""
import logging

import httpx

import config

log = logging.getLogger("backup-server")

_TIMEOUT = 15


def _target() -> tuple[str, str] | None:
    token = config.get("digest.telegram_bot_token")
    chat_id = config.get("digest.telegram_chat_id")
    if not token or not chat_id:
        return None
    return token, chat_id


def _payload(chat_id: str, text: str, parse_mode: str | None) -> dict:
    body = {"chat_id": chat_id, "text": text}
    if parse_mode:
        body["parse_mode"] = parse_mode
    return body


def _report(r: httpx.Response, tag: str) -> bool:
    if r.status_code == 200:
        log.info(f"[{tag}] mensagem enviada via Telegram")
        return True
    log.error(f"[{tag}] Telegram retornou {r.status_code}: {r.text}")
    return False


async def send(text: str, *, tag: str = "notify", parse_mode: str | None = None) -> bool:
    """Envia `text` ao admin. True se o Telegram aceitou."""
    target = _target()
    if not target:
        log.warning(f"[{tag}] Telegram não configurado — alerta só no log: {text}")
        return False
    token, chat_id = target
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            r = await client.post(f"https://api.telegram.org/bot{token}/sendMessage",
                                  json=_payload(chat_id, text, parse_mode))
    except Exception as e:
        log.error(f"[{tag}] falha ao enviar para o Telegram: {e}")
        return False
    return _report(r, tag)


def send_sync(text: str, *, tag: str = "notify", parse_mode: str | None = None) -> bool:
    """Mesmo que send(), para jobs síncronos (threads do scheduler/requests sync)."""
    target = _target()
    if not target:
        log.warning(f"[{tag}] Telegram não configurado — alerta só no log: {text}")
        return False
    token, chat_id = target
    try:
        r = httpx.post(f"https://api.telegram.org/bot{token}/sendMessage",
                       json=_payload(chat_id, text, parse_mode), timeout=_TIMEOUT)
    except Exception as e:
        log.error(f"[{tag}] falha ao enviar para o Telegram: {e}")
        return False
    return _report(r, tag)
