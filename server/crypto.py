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

__all__ = ["load_key", "encrypt_stream", "decrypt_chunks", "InvalidTag"]

NONCE_SIZE = 12       # bytes — padrão AES-GCM
CHUNK_SIZE = 1 << 20  # 1 MB de plaintext por chunk


def load_key() -> bytes:
    raw = os.getenv("ENCRYPTION_KEY", "")
    if not raw:
        raise ValueError("ENCRYPTION_KEY não definida — necessária quando ENCRYPTION_ENABLED=true")
    try:
        key = base64.b64decode(raw)
    except Exception:
        raise ValueError("ENCRYPTION_KEY inválida: deve estar em Base64")
    if len(key) != 32:
        raise ValueError(
            f"ENCRYPTION_KEY deve ter 32 bytes após decodificação (atual: {len(key)})"
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
