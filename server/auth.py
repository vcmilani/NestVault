import hashlib
import hmac
import os
import secrets
import time
from pathlib import Path

from fastapi import Header, HTTPException, Depends, Request
from typing import Optional
from sqlalchemy.orm import Session

import config
from database import get_db, User, hash_api_key


def get_current_user(x_api_key: Optional[str] = Header(None),
                      db: Session = Depends(get_db)) -> User:
    if not x_api_key:
        raise HTTPException(401, "API key ausente")
    key_hash = hash_api_key(x_api_key)
    # A comparação é a própria query: o WHERE já exige igualdade exata do hash, então
    # um `user` retornado tem, por definição, api_key_hash == key_hash — um
    # secrets.compare_digest() aqui comparava dois valores já garantidos iguais.
    user = (db.query(User)
            .filter(User.api_key_hash == key_hash, User.is_active == True)  # noqa: E712
            .first())
    if not user:
        raise HTTPException(401, "API key invalida")
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(403, "Acao restrita a administradores")
    return user


def require_owner_or_admin(owner_user_id: Optional[int], user: User) -> None:
    """Levanta 403 se `user` não é dono de owner_user_id nem admin. Backups
    sem dono (owner_user_id None — pré-migração) são tratados como acessíveis
    só por admin, nunca por usuário comum."""
    if user.role == "admin":
        return
    if owner_user_id is not None and owner_user_id == user.id:
        return
    raise HTTPException(403, "Voce nao tem permissao sobre este backup")


# -- Sessão por cookie (front cloud) ------------------------------------------
# <img>/<video> não mandam o header X-API-Key, então as páginas /cloud e /photos
# trocam a chave por um cookie HttpOnly. O cookie só autentica GETs do router
# /cloud (somente leitura) e é SameSite=Strict — não abre CSRF nas rotas de escrita,
# que continuam exigindo o header.

SESSION_COOKIE = "nv_session"
SESSION_TTL    = 7 * 24 * 3600

_session_secret: Optional[bytes] = None


def _secret() -> bytes:
    """Segredo de assinatura, gerado no primeiro uso ao lado do config.json (0600).
    Persistido para que reiniciar o servidor não derrube as sessões abertas."""
    global _session_secret
    if _session_secret is None:
        path = Path(config.path()).parent / "session.key"
        try:
            _session_secret = bytes.fromhex(path.read_text().strip())
        except (FileNotFoundError, ValueError):
            raw = secrets.token_hex(32)
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write(raw)
            _session_secret = bytes.fromhex(raw)
    return _session_secret


def _sign(user_id: int, exp: int, api_key_hash: str) -> str:
    # O hash da chave entra na assinatura: rotacionar a chave invalida as sessões.
    msg = f"{user_id}:{exp}:{api_key_hash}".encode()
    return hmac.new(_secret(), msg, hashlib.sha256).hexdigest()


def make_session_token(user: User) -> str:
    exp = int(time.time()) + SESSION_TTL
    return f"{user.id}:{exp}:{_sign(user.id, exp, user.api_key_hash)}"


def _user_from_session(token: str, db: Session) -> Optional[User]:
    try:
        uid_s, exp_s, sig = token.split(":")
        uid, exp = int(uid_s), int(exp_s)
    except ValueError:
        return None
    if exp < time.time():
        return None
    user = db.get(User, uid)
    if not user or not user.is_active:
        return None
    if not hmac.compare_digest(sig, _sign(uid, exp, user.api_key_hash)):
        return None
    return user


def get_user_header_or_cookie(request: Request,
                               x_api_key: Optional[str] = Header(None),
                               db: Session = Depends(get_db)) -> User:
    """Header X-API-Key (como get_current_user) ou, na falta dele, o cookie de sessão."""
    if x_api_key:
        return get_current_user(x_api_key, db)
    token = request.cookies.get(SESSION_COOKIE)
    user = _user_from_session(token, db) if token else None
    if not user:
        raise HTTPException(401, "Sessao ausente ou expirada")
    return user
