"""
Criptografia em repouso — AES-256-GCM em chunks de 1 MB.

Formato do arquivo cifrado:
  [12 bytes  base_nonce]
  [4 bytes   len(ciphertext_chunk_0)][ciphertext_chunk_0 = plaintext + 16-byte GCM tag]
  [4 bytes   len(ciphertext_chunk_1)][ciphertext_chunk_1]
  ...

O nonce de cada chunk é: base_nonce XOR chunk_index (12 bytes, little-endian).
"""

import os
import base64
from pathlib import Path
from typing import Generator

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.exceptions import InvalidTag  # re-exportado para os callers

__all__ = ["load_key", "encrypt_stream", "decrypt_chunks", "decrypt_range", "InvalidTag"]

NONCE_SIZE = 12       # bytes — padrão AES-GCM
CHUNK_SIZE = 1 << 20  # 1 MB de plaintext por chunk


def load_key(raw: str | None = None) -> bytes:
    """Decodifica a chave. Sem argumento, lê storage.encryption_key do config.json."""
    if raw is None:
        import config
        raw = config.get("storage.encryption_key")
    if not raw:
        raise ValueError(
            "storage.encryption_key não definida — necessária quando storage.encryption_enabled=true"
        )
    try:
        key = base64.b64decode(raw)
    except Exception:
        raise ValueError("storage.encryption_key inválida: deve estar em Base64")
    if len(key) != 32:
        raise ValueError(
            f"storage.encryption_key deve ter 32 bytes após decodificação (atual: {len(key)})"
        )
    return key


def _chunk_nonce(base: bytes, index: int) -> bytes:
    idx = index.to_bytes(NONCE_SIZE, "little")
    return bytes(b ^ i for b, i in zip(base, idx))


def encrypt_stream(src: Path, dst: Path, key: bytes) -> None:
    """Cifra src → dst. dst é escrito atomicamente via arquivo temporário gerenciado pelo caller."""
    aesgcm     = AESGCM(key)
    base_nonce = os.urandom(NONCE_SIZE)

    with open(src, "rb") as fin, open(dst, "wb") as fout:
        fout.write(base_nonce)
        chunk_idx = 0
        while True:
            chunk = fin.read(CHUNK_SIZE)
            if not chunk:
                break
            ct = aesgcm.encrypt(_chunk_nonce(base_nonce, chunk_idx), chunk, None)
            fout.write(len(ct).to_bytes(4, "little"))
            fout.write(ct)
            chunk_idx += 1


def decrypt_chunks(path: Path, key: bytes) -> Generator[bytes, None, None]:
    """Gerador de chunks decifrados — adequado para FastAPI StreamingResponse."""
    aesgcm = AESGCM(key)

    with open(path, "rb") as f:
        base_nonce = f.read(NONCE_SIZE)
        if len(base_nonce) < NONCE_SIZE:
            raise ValueError("Arquivo cifrado corrompido: nonce ausente")
        chunk_idx = 0
        while True:
            raw_len = f.read(4)
            if not raw_len:
                break
            if len(raw_len) < 4:
                raise ValueError("Arquivo cifrado corrompido: comprimento de chunk incompleto")
            ct_len = int.from_bytes(raw_len, "little")
            ct     = f.read(ct_len)
            if len(ct) < ct_len:
                raise ValueError("Arquivo cifrado corrompido: chunk truncado")
            yield aesgcm.decrypt(_chunk_nonce(base_nonce, chunk_idx), ct, None)
            chunk_idx += 1


# Todo chunk, exceto o último, carrega exatamente CHUNK_SIZE de plaintext — então
# o chunk i começa num offset fixo e dá para pular direto até ele.
_TAG_SIZE   = 16
_CHUNK_SPAN = 4 + CHUNK_SIZE + _TAG_SIZE


def decrypt_range(path: Path, key: bytes, start: int, end: int) -> Generator[bytes, None, None]:
    """Plaintext dos bytes [start, end] (inclusivo) sem decifrar o arquivo inteiro —
    é o que permite Range/seek de vídeo em arquivo cifrado."""
    if end < start:
        return
    aesgcm    = AESGCM(key)
    chunk_idx = start // CHUNK_SIZE
    skip      = start - chunk_idx * CHUNK_SIZE
    remaining = end - start + 1

    with open(path, "rb") as f:
        base_nonce = f.read(NONCE_SIZE)
        if len(base_nonce) < NONCE_SIZE:
            raise ValueError("Arquivo cifrado corrompido: nonce ausente")
        f.seek(NONCE_SIZE + chunk_idx * _CHUNK_SPAN)
        while remaining > 0:
            raw_len = f.read(4)
            if len(raw_len) < 4:
                raise ValueError("Arquivo cifrado corrompido: range além do fim")
            ct_len = int.from_bytes(raw_len, "little")
            ct     = f.read(ct_len)
            if len(ct) < ct_len:
                raise ValueError("Arquivo cifrado corrompido: chunk truncado")
            plain = aesgcm.decrypt(_chunk_nonce(base_nonce, chunk_idx), ct, None)
            piece = plain[skip:skip + remaining]
            skip = 0
            remaining -= len(piece)
            yield piece
            chunk_idx += 1
