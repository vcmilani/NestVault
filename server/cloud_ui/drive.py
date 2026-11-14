"""Pastas do servidor — a parte editável do /cloud (file server).

Um backup é escrito por um cliente e reescrito a cada rodada: editar no servidor
não sobrevive ao próximo backup (o arquivo movido volta no caminho antigo). Por
isso os backups ficam somente leitura e a edição vive em labels próprios,
kind == SERVER_KIND, sem cliente:

- versão "live" (status done): o conteúdo atual, editado no lugar. Mover e
  renomear são UPDATEs em original_path — nenhum byte é copiado;
- versão "trash" (SERVER_TRASH_STATUS): o que foi excluído, com o caminho
  prefixado pelo token da ServerTrashEntry. Continua referenciando o conteúdo,
  então a limpeza de órfãos não o apaga até a entrada vencer;
- server_folders: pastas criadas vazias (as outras são derivadas dos caminhos).

Caminhos aqui são sempre relativos à raiz da pasta, sem barras nas pontas.
"""
import logging
import secrets
from datetime import datetime

from fastapi import HTTPException
from sqlalchemy import exists, func, insert, literal, or_, select
from sqlalchemy.orm import Session

from database import (BackupID, BackupVersion, FileContent, ServerFolder, ServerTrashEntry,
                      User, VersionFile, SERVER_KIND, SERVER_LIVE_KEY, SERVER_TRASH_KEY,
                      SERVER_TRASH_STATUS)
from . import tree

log = logging.getLogger("backup-server")

READ_ONLY = "Backup e somente leitura — copie os arquivos para uma pasta do servidor"
MAX_NAME = 255


# -- Nomes e caminhos ----------------------------------------------------------

def check_name(name: str) -> str:
    name = (name or "").strip()
    if not name or name in (".", "..") or "/" in name or "\\" in name or "\0" in name:
        raise HTTPException(400, f"Nome invalido: {name!r}")
    if len(name) > MAX_NAME:
        raise HTTPException(400, "Nome muito longo")
    return name


def check_path(path: str, allow_root: bool = False) -> str:
    rel = tree.norm_rel(path)
    if not rel:
        if allow_root:
            return ""
        raise HTTPException(400, "Caminho vazio")
    for seg in rel.split("/"):
        check_name(seg)
    return rel


def join(parent: str, name: str) -> str:
    return f"{parent}/{name}" if parent else name


def basename(path: str) -> str:
    return path.rsplit("/", 1)[-1]


def _under(path: str, root: str) -> bool:
    return path == root or path.startswith(root + "/")


# -- Pasta do servidor ---------------------------------------------------------

class Drive:
    def __init__(self, b: BackupID, live: BackupVersion, trash: BackupVersion):
        self.b, self.live, self.trash = b, live, trash
        # sha256 que entraram ou saíram do Cloud nesta operação: quem chama passa
        # para cloud_replicas.apply() (réplicas dedicadas do Cloud).
        self.touched: set[str] = set()

    @property
    def label(self) -> str:
        return self.b.label


def create(db: Session, user: User, name: str) -> BackupID:
    name = check_name(name)
    dup = (db.query(BackupID.id)
           .filter(BackupID.owner_user_id == user.id, BackupID.kind == SERVER_KIND,
                   BackupID.client_name == name, tree.live_label_filter())
           .first())
    if dup:
        raise HTTPException(409, f"Ja existe uma pasta do servidor chamada '{name}'")
    label = "srv-" + secrets.token_hex(5)
    now = datetime.now()
    b = BackupID(label=label, client_name=name, kind=SERVER_KIND, owner_user_id=user.id,
                 photos_enabled=True)
    db.add(b)
    db.flush()
    db.add(BackupVersion(backup_label=label, version_key=SERVER_LIVE_KEY, status="done",
                         created_at=now, finished_at=now))
    db.add(BackupVersion(backup_label=label, version_key=SERVER_TRASH_KEY,
                         status=SERVER_TRASH_STATUS, created_at=now))
    db.commit()
    db.refresh(b)
    log.info(f"[drive] {user.username} criou a pasta do servidor '{name}' ({label})")
    return b


def get(db: Session, label: str, user: User) -> Drive:
    """Pasta do servidor do usuário (ou qualquer uma, para admin) — 403 em backup."""
    b = tree.get_label_or_404(db, label, user)
    if b.kind != SERVER_KIND:
        raise HTTPException(403, READ_ONLY)
    versions = {v.version_key: v for v in
                db.query(BackupVersion).filter(BackupVersion.backup_label == label).all()}
    live, trash = versions.get(SERVER_LIVE_KEY), versions.get(SERVER_TRASH_KEY)
    if live is None or trash is None:
        raise HTTPException(500, f"Pasta do servidor '{label}' sem as versoes live/trash")
    return Drive(b, live, trash)


def rename_drive(db: Session, d: Drive, name: str, user: User) -> None:
    name = check_name(name)
    dup = (db.query(BackupID.id)
           .filter(BackupID.owner_user_id == d.b.owner_user_id, BackupID.kind == SERVER_KIND,
                   BackupID.client_name == name, BackupID.id != d.b.id,
                   tree.live_label_filter())
           .first())
    if dup:
        raise HTTPException(409, f"Ja existe uma pasta do servidor chamada '{name}'")
    old = d.b.client_name
    d.b.client_name = name
    db.commit()
    log.info(f"[drive] {user.username} renomeou a pasta do servidor '{old}' → '{name}' ({d.label})")


def forget(db: Session, label: str) -> None:
    """Antes de apagar o BackupID: as linhas próprias das pastas do servidor
    apontam para ele (FK). Não faz commit."""
    db.query(ServerFolder).filter(ServerFolder.label == label).delete(synchronize_session=False)
    db.query(ServerTrashEntry).filter(ServerTrashEntry.label == label).delete(synchronize_session=False)


def relabel(db: Session, old: str, new: str) -> None:
    """Rename de label (PATCH /backups/{label}). Não faz commit."""
    db.query(ServerFolder).filter(ServerFolder.label == old) \
        .update({ServerFolder.label: new}, synchronize_session=False)
    db.query(ServerTrashEntry).filter(ServerTrashEntry.label == old) \
        .update({ServerTrashEntry.label: new}, synchronize_session=False)


# -- Consultas -----------------------------------------------------------------

def _is_file(db: Session, d: Drive, path: str) -> bool:
    return db.query(exists().where(VersionFile.version_id == d.live.id,
                                   VersionFile.original_path == path)).scalar()


def _is_dir(db: Session, d: Drive, path: str) -> bool:
    return (db.query(exists().where(
                VersionFile.version_id == d.live.id,
                VersionFile.original_path.startswith(path + "/", autoescape=True))).scalar()
            or db.query(exists().where(
                ServerFolder.label == d.label,
                or_(ServerFolder.path == path,
                    ServerFolder.path.startswith(path + "/", autoescape=True)))).scalar())


def kind_at(db: Session, d: Drive, path: str) -> str | None:
    """"file", "dir" ou None."""
    if _is_file(db, d, path):
        return "file"
    if _is_dir(db, d, path):
        return "dir"
    return None


def _require_free(db: Session, d: Drive, path: str) -> None:
    if kind_at(db, d, path):
        raise HTTPException(409, f"Ja existe '{path}' no destino")


def _require_dir(db: Session, d: Drive, path: str) -> None:
    if path and kind_at(db, d, path) != "dir":
        raise HTTPException(404, f"Pasta '{path}' nao encontrada")


# -- Edição --------------------------------------------------------------------

def mkdir(db: Session, d: Drive, path: str, user: User) -> str:
    path = check_path(path)
    k = kind_at(db, d, path)
    if k == "file":
        raise HTTPException(409, f"Ja existe um arquivo '{path}'")
    if k == "dir":
        raise HTTPException(409, f"A pasta '{path}' ja existe")
    db.add(ServerFolder(label=d.label, path=path))
    db.commit()
    log.info(f"[drive] {user.username} criou a pasta {d.label}:{path!r}")
    return path


def _relocate(db: Session, d: Drive, src: str, dst: str) -> None:
    """Move arquivo ou pasta `src` para `dst` (já validado). Não faz commit."""
    n = len(src)
    db.query(VersionFile).filter(VersionFile.version_id == d.live.id,
                                 VersionFile.original_path == src) \
        .update({VersionFile.original_path: dst}, synchronize_session=False)
    db.query(VersionFile).filter(VersionFile.version_id == d.live.id,
                                 VersionFile.original_path.startswith(src + "/", autoescape=True)) \
        .update({VersionFile.original_path: dst + func.substr(VersionFile.original_path, n + 1)},
                synchronize_session=False)
    db.query(ServerFolder).filter(ServerFolder.label == d.label,
                                  or_(ServerFolder.path == src,
                                      ServerFolder.path.startswith(src + "/", autoescape=True))) \
        .update({ServerFolder.path: dst + func.substr(ServerFolder.path, n + 1)},
                synchronize_session=False)


def move(db: Session, d: Drive, items: list[str], dest_dir: str, user: User) -> int:
    """Move itens para `dest_dir`. Tudo ou nada: qualquer conflito aborta antes de mexer."""
    dest_dir = check_path(dest_dir, allow_root=True)
    _require_dir(db, d, dest_dir)
    plan: list[tuple[str, str]] = []
    targets: set[str] = set()
    for raw in items:
        src = check_path(raw)
        if not kind_at(db, d, src):
            raise HTTPException(404, f"'{src}' nao encontrado")
        if _under(dest_dir, src):
            raise HTTPException(400, f"Nao da para mover '{src}' para dentro dele mesmo")
        dst = join(dest_dir, basename(src))
        if dst == src:
            continue
        if dst in targets:
            raise HTTPException(409, f"Dois itens chamados '{basename(src)}'")
        _require_free(db, d, dst)
        targets.add(dst)
        plan.append((src, dst))
    for src, dst in plan:
        _relocate(db, d, src, dst)
    db.commit()
    if plan:
        log.info(f"[drive] {user.username} moveu {len(plan)} item(ns) em {d.label} para "
                 f"{dest_dir or '/'!r}: " + ", ".join(s for s, _ in plan[:5])
                 + (f" … (+{len(plan) - 5})" if len(plan) > 5 else ""))
    return len(plan)


def rename(db: Session, d: Drive, path: str, new_name: str, user: User) -> str:
    src = check_path(path)
    new_name = check_name(new_name)
    if not kind_at(db, d, src):
        raise HTTPException(404, f"'{src}' nao encontrado")
    parent = src.rsplit("/", 1)[0] if "/" in src else ""
    dst = join(parent, new_name)
    if dst == src:
        return dst
    # Só troca maiúscula/minúscula: não é colisão com ele mesmo.
    if dst.lower() != src.lower():
        _require_free(db, d, dst)
    _relocate(db, d, src, dst)
    db.commit()
    log.info(f"[drive] {user.username} renomeou {d.label}:{src!r} → {new_name!r}")
    return dst


TOKEN_LEN = 23  # "%Y%m%d%H%M%S-" + 8 hex


def _trash_one(db: Session, d: Drive, path: str, is_dir: bool, user: User, now: datetime) -> None:
    token = f"{now:%Y%m%d%H%M%S}-{secrets.token_hex(4)}"
    d.touched.update(r[0] for r in db.query(VersionFile.sha256)
                     .filter(*tree.files_under(d.live.id, path)).distinct())
    db.query(VersionFile).filter(*tree.files_under(d.live.id, path)) \
        .update({VersionFile.version_id: d.trash.id,
                 VersionFile.original_path: token + "/" + VersionFile.original_path},
                synchronize_session=False)
    db.query(ServerFolder).filter(ServerFolder.label == d.label,
                                  or_(ServerFolder.path == path,
                                      ServerFolder.path.startswith(path + "/", autoescape=True))) \
        .delete(synchronize_session=False)
    db.add(ServerTrashEntry(label=d.label, token=token, path=path, is_dir=is_dir,
                            deleted_at=now, deleted_by=user.id))


def delete(db: Session, d: Drive, items: list[str], user: User) -> int:
    now = datetime.now()
    paths = []
    for raw in items:
        p = check_path(raw)
        k = kind_at(db, d, p)
        if not k:
            raise HTTPException(404, f"'{p}' nao encontrado")
        paths.append((p, k == "dir"))
    # Item dentro de outro item também selecionado já vai junto com o pai.
    roots = [(p, isd) for p, isd in paths
             if not any(q != p and _under(p, q) for q, _ in paths)]
    for p, isd in roots:
        _trash_one(db, d, p, isd, user, now)
    db.commit()
    log.info(f"[drive] {user.username} enviou {len(roots)} item(ns) de {d.label} para a lixeira: "
             + ", ".join(p for p, _ in roots[:5]) + (f" … (+{len(roots) - 5})" if len(roots) > 5 else ""))
    return len(roots)


def trash_list(db: Session, d: Drive) -> list[dict]:
    entries = (db.query(ServerTrashEntry).filter(ServerTrashEntry.label == d.label)
               .order_by(ServerTrashEntry.deleted_at.desc(), ServerTrashEntry.id.desc()).all())
    if not entries:
        return []
    # Contagem/tamanho por token: o token é o primeiro segmento do caminho na versão trash.
    head = func.substr(VersionFile.original_path, 1, TOKEN_LEN)
    stats = dict(
        (r.tok, (r.count, int(r.size or 0))) for r in
        db.query(head.label("tok"), func.count(VersionFile.id).label("count"),
                 func.sum(FileContent.size).label("size"))
        .outerjoin(FileContent, FileContent.sha256 == VersionFile.sha256)
        .filter(VersionFile.version_id == d.trash.id)
        .group_by(head).all()
    )
    return [{"id": e.id, "path": e.path, "name": basename(e.path), "is_dir": bool(e.is_dir),
             "deleted_at": str(e.deleted_at),
             "count": stats.get(e.token, (0, 0))[0], "size": stats.get(e.token, (0, 0))[1]}
            for e in entries]


def _entries(db: Session, d: Drive, ids: list[int]) -> list[ServerTrashEntry]:
    entries = (db.query(ServerTrashEntry)
               .filter(ServerTrashEntry.label == d.label, ServerTrashEntry.id.in_(ids)).all())
    if len(entries) != len(set(ids)):
        raise HTTPException(404, "Item da lixeira nao encontrado")
    return entries


def restore(db: Session, d: Drive, ids: list[int], user: User) -> int:
    entries = _entries(db, d, ids)
    seen: set[str] = set()
    for e in entries:
        if e.path in seen or kind_at(db, d, e.path):
            raise HTTPException(409, f"Ja existe '{e.path}' — renomeie ou mova antes de restaurar")
        seen.add(e.path)
    for e in entries:
        n = len(e.token) + 1
        d.touched.update(_entry_shas(db, d.trash.id, e))
        moved = (db.query(VersionFile)
                 .filter(VersionFile.version_id == d.trash.id,
                         VersionFile.original_path.startswith(e.token + "/", autoescape=True))
                 .update({VersionFile.version_id: d.live.id,
                          VersionFile.original_path: func.substr(VersionFile.original_path, n + 1)},
                         synchronize_session=False))
        if e.is_dir and not moved:
            db.add(ServerFolder(label=d.label, path=e.path))
        db.delete(e)
    db.commit()
    log.info(f"[drive] {user.username} restaurou {len(entries)} item(ns) da lixeira de {d.label}: "
             + ", ".join(e.path for e in entries[:5]))
    return len(entries)


def _entry_shas(db: Session, trash_version_id: int, e: ServerTrashEntry) -> set[str]:
    return {r[0] for r in db.query(VersionFile.sha256)
            .filter(VersionFile.version_id == trash_version_id,
                    VersionFile.original_path.startswith(e.token + "/", autoescape=True))
            .distinct()}


def purge_entries(db: Session, entries: list[ServerTrashEntry], trash_version_id: int) -> int:
    """Apaga de vez as entradas (e os VersionFile delas). Não faz commit; o conteúdo
    sem outra referência sai na próxima limpeza de órfãos."""
    rows = 0
    for e in entries:
        rows += (db.query(VersionFile)
                 .filter(VersionFile.version_id == trash_version_id,
                         VersionFile.original_path.startswith(e.token + "/", autoescape=True))
                 .delete(synchronize_session=False))
        db.delete(e)
    return rows


def purge(db: Session, d: Drive, ids: list[int], user: User) -> int:
    entries = _entries(db, d, ids)
    names = [e.path for e in entries]
    for e in entries:
        d.touched.update(_entry_shas(db, d.trash.id, e))
    purge_entries(db, entries, d.trash.id)
    db.commit()
    log.info(f"[drive] {user.username} excluiu de vez {len(names)} item(ns) da lixeira de "
             f"{d.label}: " + ", ".join(names[:5]))
    return len(names)


def purge_expired(db: Session, cutoff: datetime) -> int:
    """Limpeza noturna: entradas da lixeira das pastas do servidor mais velhas que cutoff."""
    entries = db.query(ServerTrashEntry).filter(ServerTrashEntry.deleted_at < cutoff).all()
    if not entries:
        return 0
    trash_ids = dict(db.query(BackupVersion.backup_label, BackupVersion.id)
                     .filter(BackupVersion.version_key == SERVER_TRASH_KEY,
                             BackupVersion.status == SERVER_TRASH_STATUS,
                             BackupVersion.backup_label.in_({e.label for e in entries}))
                     .all())
    count = 0
    for e in entries:
        tid = trash_ids.get(e.label)
        if tid is None:
            db.delete(e)
            continue
        purge_entries(db, [e], tid)
        count += 1
    db.commit()
    return count


def copy(db: Session, src_label: str, src_version: str | None, items: list[str],
         d: Drive, dest_dir: str, user: User) -> int:
    """Copia arquivos/pastas de um backup (ou de outra pasta do servidor) para `dest_dir`.
    Só cria VersionFile apontando para o mesmo sha256 — nenhum byte novo no disco."""
    tree.get_label_or_404(db, src_label, user)
    versions = tree.done_versions(db, src_label)
    v = tree.pick_version(versions, src_version)
    base = tree.label_base(db, src_label, versions)
    dest_dir = check_path(dest_dir, allow_root=True)
    _require_dir(db, d, dest_dir)

    plan: list[tuple[str, str]] = []
    targets: set[str] = set()
    for raw in items:
        rel = check_path(raw)
        full = base + rel
        has = db.query(exists().where(*tree.files_under(v.id, full))).scalar()
        if not has:
            raise HTTPException(404, f"'{rel}' nao encontrado em {src_label}")
        if src_label == d.label and _under(dest_dir, rel):
            raise HTTPException(400, f"Nao da para copiar '{rel}' para dentro dele mesmo")
        dst = join(dest_dir, basename(rel))
        if dst in targets:
            raise HTTPException(409, f"Dois itens chamados '{basename(rel)}'")
        _require_free(db, d, dst)
        targets.add(dst)
        plan.append((full, dst))

    total = 0
    for full, dst in plan:
        d.touched.update(r[0] for r in db.query(VersionFile.sha256)
                         .filter(*tree.files_under(v.id, full)).distinct())
        stmt = insert(VersionFile).from_select(
            ["version_id", "original_path", "sha256", "mtime"],
            select(literal(d.live.id), literal(dst) + func.substr(VersionFile.original_path, len(full) + 1),
                   VersionFile.sha256, VersionFile.mtime)
            .where(*tree.files_under(v.id, full)),
        )
        total += db.execute(stmt).rowcount
    db.commit()
    log.info(f"[drive] {user.username} copiou {len(plan)} item(ns) ({total} arquivo(s)) de "
             f"{src_label}/{v.version_key} para {d.label}:{dest_dir or '/'!r}")
    return total


def prepare_upload(db: Session, d: Drive, path: str) -> str:
    """Valida o destino de um upload antes de gravar o conteúdo."""
    path = check_path(path)
    if _is_dir(db, d, path):
        raise HTTPException(409, f"Ja existe uma pasta '{path}'")
    parent = path.rsplit("/", 1)[0] if "/" in path else ""
    # Um arquivo no lugar de uma pasta do caminho: "a.txt/b.txt" com a.txt existindo.
    p = parent
    while p:
        if _is_file(db, d, p):
            raise HTTPException(409, f"'{p}' e um arquivo, nao uma pasta")
        p = p.rsplit("/", 1)[0] if "/" in p else ""
    return path


def put_file(db: Session, d: Drive, path: str, sha256: str, mtime: float, user: User) -> int:
    """Grava/substitui o arquivo em `path`. O anterior, se o conteúdo mudou, vai para a lixeira."""
    d.touched.add(sha256)
    vf = (db.query(VersionFile)
          .filter(VersionFile.version_id == d.live.id, VersionFile.original_path == path).first())
    if vf and vf.sha256 == sha256:
        vf.mtime = mtime
        db.commit()
        return vf.id
    if vf:
        _trash_one(db, d, path, False, user, datetime.now())
        db.flush()
    vf = VersionFile(version_id=d.live.id, original_path=path, sha256=sha256, mtime=mtime)
    db.add(vf)
    db.commit()
    db.refresh(vf)
    return vf.id
