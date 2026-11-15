"""Pastas ocultas (álbum Hidden do iCloud, pastas com ponto) fora do /photos.

O PIN das ocultas (users.hidden_pin_hash) só valia para a galeria: /tree,
/search, /history, /content, /photo/{id}/full, /thumb, /zip e /duplicates
serviam a pasta oculta sem PIN para quem tivesse a chave ou o cookie de sessão.
Com PIN definido e sem o cookie de desbloqueio (POST /photos/unlock), essas
rotas omitem o que está em pasta oculta — e respondem 404 ao pedido direto, a
mesma resposta de "não existe", para não confirmar nada."""
from fastapi import HTTPException, Request
from sqlalchemy.orm import Session

from auth import hidden_unlocked
from database import BackupID, BackupVersion, User, VersionFile, TRASHED_STATUS
from .media import HIDDEN_FOLDERS, in_hidden_folder


def locked(request: Request, user: User) -> bool:
    """Usuário com PIN e sem o cookie de desbloqueio."""
    return bool(user.hidden_pin_hash) and not hidden_unlocked(request, user)


def dir_is_hidden(rel: str) -> bool:
    """A pasta `rel` (relativa à raiz do label) é oculta ou está dentro de uma."""
    return any(s.lower() in HIDDEN_FOLDERS or s.startswith(".") for s in rel.split("/") if s)


def file_is_hidden(rel: str) -> bool:
    """O arquivo `rel` está dentro de uma pasta oculta (o nome dele não conta)."""
    return in_hidden_folder(rel)


def rel_of(original_path: str, base: str) -> str:
    return original_path[len(base):] if original_path.startswith(base) else original_path


def require_visible_dir(request: Request, user: User, rel: str) -> None:
    if locked(request, user) and dir_is_hidden(rel):
        raise HTTPException(404, f"'{rel}' nao encontrado")


def file_rel(db: Session, label: str, original_path: str) -> str:
    from . import tree
    base = tree.label_base(db, label, tree.done_versions(db, label))
    return rel_of(original_path, base)


def require_visible_file(request: Request, user: User, db: Session, label: str, original_path: str) -> None:
    if locked(request, user) and file_is_hidden(file_rel(db, label, original_path)):
        raise HTTPException(404, "Arquivo nao encontrado")


def sha_is_hidden(db: Session, user: User, sha256: str) -> bool:
    """Todos os caminhos deste conteúdo nos labels do usuário estão em pasta
    oculta (um caminho visível basta para mostrar a miniatura)."""
    from . import tree
    rows = (db.query(VersionFile.original_path, BackupVersion.backup_label)
            .join(BackupVersion, BackupVersion.id == VersionFile.version_id)
            .join(BackupID, BackupID.label == BackupVersion.backup_label)
            .filter(VersionFile.sha256 == sha256,
                    BackupVersion.status != TRASHED_STATUS,
                    BackupID.owner_user_id == user.id,
                    tree.live_label_filter())
            .all())
    if not rows:
        return False
    bases: dict[str, str] = {}
    for path, label in rows:
        if label not in bases:
            bases[label] = tree.label_base(db, label, tree.done_versions(db, label))
        if not file_is_hidden(rel_of(path, bases[label])):
            return False
    return True


def filter_duplicates(result: dict) -> dict:
    """Tira das duplicatas as cópias em pasta oculta e os grupos que ficam com uma só."""
    groups = []
    for g in result["groups"]:
        files = [f for f in g["files"] if not file_is_hidden(f["path"])]
        if len(files) < 2:
            continue
        groups.append({**g, "files": files, "count": len(files),
                       "repeated_bytes": g["size"] * (len(files) - 1)})
    return {**result, "groups": groups, "group_count": len(groups),
            "repeated_bytes": sum(g["repeated_bytes"] for g in groups)}
