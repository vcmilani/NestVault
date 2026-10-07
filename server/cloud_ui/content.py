"""Streaming de conteúdo para visualização no navegador: inline, com Range.

Range é o que deixa o <video> fazer seek; para arquivo cifrado ele é mapeado para os
chunks de 1 MB (crypto.decrypt_range), sem decifrar o arquivo inteiro.

Segurança: o arquivo é do usuário, mas é servido NA ORIGEM do painel — um .html ou
.svg de backup aberto inline rodaria script com acesso à API key em localStorage.
Por isso só tipos de mídia/PDF vão inline com o próprio Content-Type; texto vai como
text/plain; o resto vira download. Tudo sai com CSP própria (sandbox onde dá).
"""
import logging
import mimetypes
from pathlib import Path
from urllib.parse import quote

from fastapi import HTTPException, Request
from fastapi.responses import Response, StreamingResponse

import crypto
import storage

log = logging.getLogger("backup-server")

for _ext, _type in ((".heic", "image/heic"), (".heif", "image/heif"), (".webp", "image/webp"),
                    (".avif", "image/avif"), (".mkv", "video/x-matroska"), (".m4v", "video/mp4"),
                    (".mov", "video/quicktime"), (".3gp", "video/3gpp"), (".md", "text/markdown")):
    mimetypes.add_type(_type, _ext)

_READ_BLOCK = 256 * 1024

# Sem 'sandbox' no PDF: o visualizador de PDF do Chrome se recusa a abrir em
# documento sandboxed. O PDF não roda script na origem do painel de qualquer forma.
_CSP_SANDBOX = ("sandbox; default-src 'none'; img-src 'self' data:; media-src 'self'; "
                "style-src 'unsafe-inline'; frame-ancestors 'self'")
_CSP_PDF     = ("default-src 'none'; img-src 'self' data:; style-src 'unsafe-inline'; "
                "object-src 'self'; frame-ancestors 'self'")

_TEXTLIKE = {"application/json", "application/xml", "application/javascript",
             "application/x-sh", "application/x-yaml", "application/toml"}


def guess_mime(name: str) -> str:
    return mimetypes.guess_type(name)[0] or "application/octet-stream"


def _inline_type(mime: str) -> str | None:
    """Content-Type para servir inline, ou None (→ força download)."""
    if mime.startswith(("video/", "audio/")) or mime == "application/pdf":
        return mime
    if mime.startswith("image/"):
        return mime  # svg incluso: dentro de <img> não roda script; aberto direto, cai no sandbox
    if mime.startswith("text/") or mime in _TEXTLIKE:
        return "text/plain; charset=utf-8"
    return None


def parse_range(header: str | None, size: int):
    """(start, end) inclusivo; None = ignorar e mandar tudo; "invalid" = 416.
    Só range único — multipart/byteranges não é usado por <video> nem <img>."""
    if not header or not header.startswith("bytes=") or size == 0:
        return None
    spec = header[6:].strip()
    if "," in spec or "-" not in spec:
        return None
    a, b = spec.split("-", 1)
    try:
        if a == "":
            n = int(b)
            if n <= 0:
                return "invalid"
            return max(0, size - n), size - 1
        start = int(a)
        end = int(b) if b else size - 1
    except ValueError:
        return None
    if start >= size or end < start:
        return "invalid"
    return start, min(end, size - 1)


def _read_plain(path: Path, start: int, end: int):
    remaining = end - start + 1
    with open(path, "rb") as f:
        f.seek(start)
        while remaining > 0:
            block = f.read(min(_READ_BLOCK, remaining))
            if not block:
                break
            remaining -= len(block)
            yield block


def _guarded(gen, sha256: str):
    # Erro no meio do stream (tag GCM inválida, disco sumiu) não vira 500 — os
    # headers já foram. Loga e encerra; o navegador vê a resposta truncada.
    try:
        yield from gen
    except Exception as exc:
        log.error(f"[cloud] stream de {sha256[:8]}… interrompido: {exc}")


def stream(request: Request, db, *, sha256: str, name: str, size: int, encrypted: bool,
           download: bool) -> Response:
    path, has_degraded = storage.readable_copy(db, sha256, "cloud")
    if path is None:
        raise HTTPException(503 if has_degraded else 410,
                            "Arquivo em volume degraded" if has_degraded else "Conteudo fisico nao encontrado")
    if not encrypted:
        size = path.stat().st_size

    mime   = guess_mime(name)
    inline = None if download else _inline_type(mime)
    headers = {
        "Accept-Ranges": "bytes",
        # O conteúdo de um file_id nunca muda (sha256 fixo).
        "Cache-Control": "private, max-age=86400",
        "ETag": f'"{sha256}"',
        "Content-Security-Policy": _CSP_PDF if inline == "application/pdf" else _CSP_SANDBOX,
    }
    if inline:
        headers["Content-Disposition"] = f"inline; filename*=UTF-8''{quote(name)}"
        media_type = inline
    else:
        headers["Content-Disposition"] = f"attachment; filename*=UTF-8''{quote(name)}"
        media_type = "application/octet-stream"

    if request.headers.get("if-none-match") == headers["ETag"]:
        return Response(status_code=304, headers=headers)

    rng = parse_range(request.headers.get("range"), size)
    if rng == "invalid":
        return Response(status_code=416, headers={**headers, "Content-Range": f"bytes */{size}"})
    if size == 0:
        return Response(b"", media_type=media_type, headers=headers)

    start, end = rng if rng else (0, size - 1)
    if encrypted:
        body = crypto.decrypt_range(path, storage.encryption_key, start, end)
    else:
        body = _read_plain(path, start, end)
    headers["Content-Length"] = str(end - start + 1)
    status = 200
    if rng:
        status = 206
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    return StreamingResponse(_guarded(body, sha256), status_code=status,
                             media_type=media_type, headers=headers)
