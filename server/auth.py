from fastapi import Header, HTTPException, Depends
from typing import Optional
from sqlalchemy.orm import Session

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
