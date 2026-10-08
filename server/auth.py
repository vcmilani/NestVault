import hashlib
import hmac
import logging
import os
import secrets
import time
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import Header, HTTPException, Depends, Request
from typing import Optional
from sqlalchemy.orm import Session

import config
from database import get_db, ApiKey, User, hash_api_key

log = logging.getLogger("backup-server")


# Escopo da chave usada no request, num atributo Python do User (não é coluna,
# nunca vai para o banco): "full" = chave principal (users.api_key_hash), com o
# papel do usuário; "client" = chave adicional (api_keys), nunca admin.
# key_id: 0 para a principal, ApiKey.id para as adicionais (entra no cookie).
FULL_SCOPE = "full"

# last_used_at é só informativo — gravá-lo a cada request disputaria o lock de
# escrita do SQLite com os uploads de um backup em andamento.
_LAST_USED_EVERY = timedelta(minutes=10)


def _with_scope(user: User, scope: str, key_id: int) -> User:
    user.key_scope = scope
    user.key_id = key_id
    return user


def _touch(db: Session, key: ApiKey) -> None:
    now = datetime.now()
    if key.last_used_at is None or now - key.last_used_at >= _LAST_USED_EVERY:
        key.last_used_at = now
        db.commit()


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
    if user:
        return _with_scope(user, FULL_SCOPE, 0)
    row = (db.query(ApiKey, User)
           .join(User, User.id == ApiKey.user_id)
           .filter(ApiKey.key_hash == key_hash, User.is_active == True)  # noqa: E712
           .first())
    if not row:
        raise HTTPException(401, "API key invalida")
    key, user = row
    _touch(db, key)
    return _with_scope(user, key.scope, key.id)


def is_admin(user: User) -> bool:
    """Poder de admin = papel admin E chave principal. Uma chave de cliente de um
    admin enxerga o mesmo que um usuário comum. Sem escopo marcado (User que não
    veio da autenticação), nega."""
    return user.role == "admin" and getattr(user, "key_scope", None) == FULL_SCOPE


def effective_role(user: User) -> str:
    return "admin" if is_admin(user) else "user"


def require_admin(user: User = Depends(get_current_user)) -> User:
    if not is_admin(user):
        raise HTTPException(403, "Acao restrita a administradores")
    return user


def require_owner_or_admin(owner_user_id: Optional[int], user: User) -> None:
    """Levanta 403 se `user` não é dono de owner_user_id nem admin. Backups
    sem dono (owner_user_id None — pré-migração) são tratados como acessíveis
    só por admin, nunca por usuário comum."""
    if is_admin(user):
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
            log.info(f"[cloud] segredo de sessao gerado em {path}")
    return _session_secret


def _sign(user_id: int, key_id: int, exp: int, key_hash: str) -> str:
    # O hash da chave usada entra na assinatura: rotacionar ou revogar a chave
    # invalida as sessões abertas com ela.
    msg = f"{user_id}:{key_id}:{exp}:{key_hash}".encode()
    return hmac.new(_secret(), msg, hashlib.sha256).hexdigest()


def _session_key(db: Session, user: User, key_id: int) -> Optional[tuple[str, str]]:
    """(hash, escopo) da chave `key_id` do usuário — None se ela não existe mais."""
    if key_id == 0:
        return user.api_key_hash, FULL_SCOPE
    key = db.get(ApiKey, key_id)
    if not key or key.user_id != user.id:
        return None
    return key.key_hash, key.scope


def make_session_token(db: Session, user: User) -> str:
    """Token amarrado à chave que abriu a sessão — o cookie herda o escopo dela."""
    key_id = getattr(user, "key_id", 0)
    key_hash, _scope = _session_key(db, user, key_id)
    exp = int(time.time()) + SESSION_TTL
    return f"{user.id}:{key_id}:{exp}:{_sign(user.id, key_id, exp, key_hash)}"


def _user_from_session(token: str, db: Session) -> Optional[User]:
    try:
        uid_s, kid_s, exp_s, sig = token.split(":")
        uid, kid, exp = int(uid_s), int(kid_s), int(exp_s)
    except ValueError:
        return None  # inclusive o formato antigo uid:exp:sig, anterior às chaves de cliente
    if exp < time.time():
        return None
    user = db.get(User, uid)
    if not user or not user.is_active:
        return None
    key = _session_key(db, user, kid)
    if not key:
        return None
    key_hash, scope = key
    if not hmac.compare_digest(sig, _sign(uid, kid, exp, key_hash)):
        return None
    return _with_scope(user, scope, kid)


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
