"""Cota de armazenamento por usuário (v10.2.0).

Uso de um usuário = soma dos tamanhos dos sha256 DISTINTOS referenciados pelas
versões (fora da lixeira do admin) dos labels dele. Coerente com a dedup: o mesmo
arquivo em dez pastas ou cem versões conta uma vez; conteúdo que ele divide com
outra conta conta para as duas.

Calcular isso varre version_files, então o resultado de TODOS os usuários sai de
uma query agrupada e fica em cache por _TTL. Entre recálculos, add_bytes() soma o
que cada upload novo acrescenta — o número não fica para trás durante um backup.
Depois de uma limpeza o uso pode demorar até _TTL para cair (erra para o lado
conservador: bloquear um pouco depois, nunca deixar passar da cota).
"""
import logging
import threading
import time

from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

import config
import notify
from database import BackupID, BackupVersion, FileContent, User, VersionFile, TRASHED_STATUS
from storage import fmt_bytes

log = logging.getLogger("backup-server")

_TTL = 600.0

_lock = threading.Lock()
_cache: dict = {"usage": {}, "ts": 0.0}


def reset() -> None:
    with _lock:
        _cache.update({"usage": {}, "ts": 0.0})


def _compute_all(db: Session) -> dict[int, int]:
    distinct = (
        db.query(BackupID.owner_user_id.label("uid"), FileContent.sha256, FileContent.size)
        .join(BackupVersion, BackupVersion.backup_label == BackupID.label)
        .join(VersionFile, VersionFile.version_id == BackupVersion.id)
        .join(FileContent, FileContent.sha256 == VersionFile.sha256)
        .filter(BackupID.owner_user_id.isnot(None), BackupVersion.status != TRASHED_STATUS)
        .distinct()
        .subquery()
    )
    rows = (db.query(distinct.c.uid, func.coalesce(func.sum(distinct.c.size), 0))
            .group_by(distinct.c.uid).all())
    return {uid: int(total) for uid, total in rows}


def usage_all(db: Session) -> dict[int, int]:
    """{user_id: bytes} de todos os usuários com algum backup."""
    with _lock:
        if time.monotonic() - _cache["ts"] < _TTL:
            return dict(_cache["usage"])
    usage = _compute_all(db)
    with _lock:
        _cache.update({"usage": usage, "ts": time.monotonic()})
    return dict(usage)


def usage(db: Session, user_id: int) -> int:
    return usage_all(db).get(user_id, 0)


def add_bytes(user_id: int, n: int) -> None:
    """Conteúdo novo para o usuário acabou de entrar (só ajusta um cache válido)."""
    if n <= 0:
        return
    with _lock:
        if _cache["ts"]:
            _cache["usage"][user_id] = _cache["usage"].get(user_id, 0) + n


def owns(db: Session, user_id: int, sha256: str) -> bool:
    """O usuário já tem este conteúdo em algum label dele (então não soma no uso)."""
    return db.query(
        db.query(VersionFile.id)
        .join(BackupVersion, BackupVersion.id == VersionFile.version_id)
        .join(BackupID, BackupID.label == BackupVersion.backup_label)
        .filter(VersionFile.sha256 == sha256,
                BackupID.owner_user_id == user_id,
                BackupVersion.status != TRASHED_STATUS)
        .exists()
    ).scalar()


def _exceeded(user: User, used: int, extra: int = 0) -> HTTPException:
    return HTTPException(507, (
        f"Cota excedida: {fmt_bytes(used)} usados de {fmt_bytes(user.quota_bytes)}"
        + (f" (o arquivo tem {fmt_bytes(extra)})" if extra else "")
        + " — libere espaço ou peça mais cota ao administrador"))


def require_room(db: Session, user: User, extra: int = 0) -> None:
    """507 se o usuário já está na cota (extra=0) ou se `extra` bytes a mais a
    estourariam. Sem cota: no-op, sem query."""
    if user.quota_bytes is None:
        return
    used = usage(db, user.id)
    if used + extra > user.quota_bytes or (extra == 0 and used >= user.quota_bytes):
        raise _exceeded(user, used, extra)


def check_new_content(db: Session, user: User, sha256: str, size: int) -> bool:
    """Para um conteúdo prestes a ser registrado: True se ele é novo para o usuário
    (e cabe na cota); levanta 507 se não couber. Sem cota sempre True/sem query
    de uso (só o owns, que é indexado)."""
    if user.quota_bytes is None:
        return False
    if owns(db, user.id, sha256):
        return False
    require_room(db, user, size)
    return True


def commit_new_content(db: Session, user: User, size: int) -> None:
    """Contabiliza o conteúdo novo e manda o aviso de alerts.quota_warn_pct uma vez."""
    add_bytes(user.id, size)
    warn_pct = config.get("alerts.quota_warn_pct")
    if not warn_pct or not user.quota_bytes or user.quota_alerted:
        return
    used = usage(db, user.id)
    if used * 100 >= user.quota_bytes * warn_pct:
        db.query(User).filter(User.id == user.id).update({"quota_alerted": True})
        db.commit()
        user.quota_alerted = True
        notify.send_sync(
            f"📦 NestVault: {user.username} chegou a {used * 100 // user.quota_bytes}% da cota "
            f"({fmt_bytes(used)} de {fmt_bytes(user.quota_bytes)}).", tag="quota")


def reset_alert_if_below(db: Session, user: User) -> None:
    """Rearma o aviso quando o uso volta para baixo do limiar (ex.: cota aumentada)."""
    warn_pct = config.get("alerts.quota_warn_pct")
    if not user.quota_alerted:
        return
    if not user.quota_bytes or not warn_pct or usage(db, user.id) * 100 < user.quota_bytes * warn_pct:
        user.quota_alerted = False
