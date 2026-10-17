"""Download de vários arquivos/pastas do /cloud num .zip gerado em streaming.

O zip é escrito direto na resposta, sem arquivo temporário nem o conteúdo inteiro
na memória: zipfile aceita destino não-seekable (grava data descriptors), e aqui o
destino é um buffer que o gerador esvazia a cada bloco.

Mídia e arquivos já comprimidos vão STORED — recomprimir JPEG/vídeo num Pi só gasta
CPU; o resto vai DEFLATED nível 1, barato e suficiente para texto/documentos.

Tudo que depende do banco (lista de arquivos, cópia legível de cada um) é resolvido
ANTES do streaming: a sessão da request já foi fechada quando o corpo começa a sair.
"""
import logging
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy.orm import Session

import crypto
import storage
from database import FileContent, User, VersionFile
from . import tree

log = logging.getLogger("backup-server")

MAX_FILES = 50_000
_BLOCK = 1024 * 1024

_STORED_EXT = {
    "jpg", "jpeg", "png", "gif", "webp", "avif", "heic", "heif",
    "mp4", "m4v", "mov", "mkv", "webm", "3gp", "avi",
    "mp3", "m4a", "aac", "ogg", "opus", "flac",
    "zip", "gz", "tgz", "bz2", "xz", "7z", "rar", "zst",
    "pdf", "docx", "xlsx", "pptx", "odt", "ods", "epub",
}


@dataclass
class Entry:
    name: str           # caminho dentro do zip
    path: Path | None   # cópia legível (None = sem cópia legível → vai para o relatório)
    encrypted: bool
    size: int
    mtime: float


class _Sink:
    """Destino do ZipFile: acumula bytes que o gerador vai entregando."""
    def __init__(self):
        self.buf = bytearray()
        self.pos = 0

    def write(self, b) -> int:
        self.buf += b
        self.pos += len(b)
        return len(b)

    def tell(self) -> int:
        return self.pos

    def flush(self):
        pass

    def take(self) -> bytes:
        out = bytes(self.buf)
        self.buf.clear()
        return out


def _unique(name: str, used: set[str]) -> str:
    if name not in used:
        used.add(name)
        return name
    stem, dot, ext = name.rpartition(".")
    if not dot or "/" in ext:
        stem, ext = name, ""
    i = 2
    while True:
        cand = f"{stem} ({i}).{ext}" if ext else f"{stem} ({i})"
        if cand not in used:
            used.add(cand)
            return cand
        i += 1


def collect(db: Session, label: str, version: str | None, items: list[str],
            user: User) -> tuple[list[Entry], str]:
    """(entradas, nome sugerido do .zip). Cada item selecionado vira uma pasta/arquivo
    na raiz do zip, com o próprio nome — como baixar uma seleção num drive. O item
    "" é o label inteiro (baixar a raiz)."""
    b = tree.get_label_or_404(db, label, user)
    versions = tree.done_versions(db, label)
    v = tree.pick_version(versions, version)
    base = tree.label_base(db, label, versions)
    display = b.client_name if b.kind == "server" and b.client_name else label
    rels = []
    for raw in items:
        rel = tree.norm_rel(raw)
        if any(seg in (".", "..") for seg in rel.split("/")):
            raise HTTPException(400, f"Item invalido: {raw!r}")
        rels.append(rel)
    if not rels:
        raise HTTPException(400, "Nada selecionado")
    if "" in rels:
        rels = [""]
    # Item dentro de outro também selecionado já vai junto com o pai.
    rels = [r for r in dict.fromkeys(rels)
            if not any(o and o != r and r.startswith(o + "/") for o in rels)]

    entries: list[Entry] = []
    used: set[str] = set()
    readable: dict[str, Path | None] = {}
    for rel in rels:
        full = base + rel
        if rel:
            where = tree.files_under(v.id, full)
        else:  # raiz: tudo abaixo do prefixo comum; `full` sem a barra final
            where = (VersionFile.version_id == v.id,
                     VersionFile.original_path.startswith(base, autoescape=True))
            full = base[:-1] if base.endswith("/") else base
        rows = (db.query(VersionFile.original_path, VersionFile.sha256, VersionFile.mtime,
                         FileContent.size, FileContent.encrypted)
                .join(FileContent, FileContent.sha256 == VersionFile.sha256)
                .filter(*where)
                .order_by(VersionFile.original_path)
                .all())
        if not rows:
            raise HTTPException(404, f"'{rel}' nao encontrado")
        if len(entries) + len(rows) > MAX_FILES:
            raise HTTPException(413, f"Selecao grande demais para um zip (mais de {MAX_FILES} arquivos)")
        # Prefixo único por item (dois itens com o mesmo nome vindos da busca).
        top = _unique(rel.rsplit("/", 1)[-1] if rel else display, used)
        for r in rows:
            if r.sha256 not in readable:
                readable[r.sha256] = storage.readable_copy(db, r.sha256, "cloud-zip")[0]
            inner = r.original_path[len(full):]  # "" (o próprio arquivo) ou "/sub/x"
            if not inner.startswith("/") and inner:
                inner = "/" + inner  # raiz sem prefixo comum (base == "")
            entries.append(Entry(name=top + inner, path=readable[r.sha256],
                                 encrypted=bool(r.encrypted), size=int(r.size or 0),
                                 mtime=r.mtime or 0))

    if len(rels) == 1:
        zip_name = (rels[0].rsplit("/", 1)[-1] if rels[0] else display) + ".zip"
    else:
        # Nome da pasta de onde a seleção saiu (ou da pasta do servidor/backup, na raiz).
        folder = rels[0].rsplit("/", 2)[-2] if "/" in rels[0] else display
        zip_name = f"{folder} ({len(rels)} itens).zip"
    return entries, zip_name


def _date_time(mtime: float):
    try:
        t = datetime.fromtimestamp(mtime)
    except (OverflowError, OSError, ValueError):
        t = datetime(1980, 1, 1)
    if t.year < 1980:
        t = datetime(1980, 1, 1)
    return t.timetuple()[:6]


def _chunks(e: Entry):
    if e.encrypted:
        yield from crypto.decrypt_chunks(e.path, storage.encryption_key)
        return
    with open(e.path, "rb") as f:
        while True:
            b = f.read(_BLOCK)
            if not b:
                return
            yield b


def stream(entries: list[Entry], who: str):
    sink = _Sink()
    failed: list[str] = []
    total = 0
    zf = zipfile.ZipFile(sink, "w", allowZip64=True)
    try:
        for e in entries:
            if e.path is None:
                failed.append(f"{e.name}: nenhuma copia legivel no servidor")
                continue
            ext = e.name.rsplit(".", 1)[-1].lower() if "." in e.name else ""
            zi = zipfile.ZipInfo(e.name, date_time=_date_time(e.mtime))
            zi.file_size = e.size  # define zip64 no cabeçalho local para arquivo > 4 GB
            if ext in _STORED_EXT:
                zi.compress_type = zipfile.ZIP_STORED
            else:
                zi.compress_type = zipfile.ZIP_DEFLATED
                zi._compresslevel = 1
            zi.external_attr = 0o644 << 16
            try:
                with zf.open(zi, "w", force_zip64=e.size > 0xFFFF0000) as w:
                    for chunk in _chunks(e):
                        w.write(chunk)
                        total += len(chunk)
                        if len(sink.buf) >= _BLOCK:
                            yield sink.take()
            except Exception as exc:
                # Erro no meio (tag GCM inválida, disco sumiu): os headers já foram.
                # A entrada sai truncada; anota e segue com as próximas.
                log.error(f"[cloud] zip {who}: {e.name!r} falhou no meio: {exc}")
                failed.append(f"{e.name}: falhou durante a leitura ({exc.__class__.__name__})")
            yield sink.take()
        if failed:
            zf.writestr("_ERROS_NESTVAULT.txt",
                        "Arquivos que nao puderam ser incluidos:\n\n" + "\n".join(failed) + "\n")
        zf.close()
        yield sink.take()
        log.info(f"[cloud] zip {who}: {len(entries) - len(failed)} arquivo(s), "
                 f"{total // (1024 * 1024)} MB" + (f", {len(failed)} com erro" if failed else ""))
    except GeneratorExit:
        log.info(f"[cloud] zip {who}: download interrompido pelo cliente")
        raise
