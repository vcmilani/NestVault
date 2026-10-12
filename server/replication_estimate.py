"""Estimativa: quanto espaço a replicação precisaria para cada fator, e se cabe.

Simula a mesma política de storage.ensure_replicas (e, por ela, do rereplicate
que roda depois de subir o fator): para cada conteúdo, as cópias que faltam vão
para os volumes saudáveis que ainda não o têm, NA ORDEM DE DECLARAÇÃO, sem olhar
espaço livre. Então a conta por disco é exata para o acervo atual — o que ela não
prevê é o crescimento (backups novos).

"Cabe" = cada disco fica com livre ≥ storage.fallback_threshold_gb depois das
cópias. Abaixo desse piso o volume para de receber uploads, então passar dele já
é problema mesmo que o disco não encha de vez.

A agregação é uma máscara de bits dos volumes que têm cada conteúdo, somada em
SQL e agrupada por máscara: no máximo 2^N grupos, barato com milhões de arquivos.
"""
import threading
import time
from datetime import datetime
from pathlib import Path

from sqlalchemy import case, func
from sqlalchemy.orm import Session

import crypto
import storage
from database import FileContent, FileContentCopy

_TTL = 600
_cache: dict = {"ts": 0.0, "key": None, "data": None}
_lock = threading.Lock()


def _groups(db: Session, vols: list[Path]) -> list[tuple[int, int, int]]:
    """[(máscara de volumes com cópia, nº de conteúdos, bytes de UMA cópia de todos eles)]."""
    bit = sum(case((FileContentCopy.volume_path == str(v), 1 << i), else_=0)
              for i, v in enumerate(vols))
    # (sha256, volume_path) é único (uq_sha256_volume): a soma dos bits é a máscara.
    per = (db.query(FileContentCopy.sha256.label("sha256"), func.sum(bit).label("mask"))
           .group_by(FileContentCopy.sha256)
           .subquery())
    # Tamanho no disco: com cifragem, nonce + 20 bytes por chunk de 1 MB (crypto.py).
    chunks = (FileContent.size + crypto.CHUNK_SIZE - 1) // crypto.CHUNK_SIZE
    stored = FileContent.size + case(
        (FileContent.encrypted.is_(True), crypto.NONCE_SIZE + chunks * 20), else_=0)
    rows = (db.query(per.c.mask, func.count(), func.coalesce(func.sum(stored), 0))
            .join(FileContent, FileContent.sha256 == per.c.sha256)
            .filter(FileContent.quarantined_at.is_(None))
            .group_by(per.c.mask)
            .all())
    return [(int(m or 0), int(n), int(b)) for m, n, b in rows]


def _current_factor() -> int:
    f = storage.REPLICATION_FACTOR
    return len(storage.STORAGE_VOLUMES) if f <= 0 else f


def compute(db: Session) -> dict:
    vols = list(storage.STORAGE_VOLUMES)
    healthy = set(storage.healthy_volumes())
    floor = int(storage.STORAGE_FALLBACK_THRESHOLD_GB * 1024 ** 3)

    info = []
    for v in vols:
        u = storage.safe_disk_usage(v) if v in healthy else None
        if u is None:
            healthy.discard(v)
        info.append({"path": str(v), "healthy": u is not None,
                     "total": u.total if u else 0, "free": u.free if u else 0,
                     "used": u.used if u else 0,
                     "usable": max(0, u.free - floor) if u else 0})
    order = [i for i, v in enumerate(vols) if v in healthy]  # ordem de declaração
    hmask = sum(1 << i for i in order)

    groups = _groups(db, vols)
    total_contents = sum(n for _, n, _ in groups)
    orphan = [(n, b) for m, n, b in groups if not (m & hmask)]  # (contagem, bytes do grupo)  # sem cópia em volume saudável

    options = []
    for k in range(1, max(len(order), 1) + 1):
        recv = {i: 0 for i in order}
        extra = freed = files = 0
        for mask, n, size in groups:  # size = bytes de uma cópia do grupo inteiro
            have = [i for i in order if mask & (1 << i)]
            if not have:
                continue  # nada legível para copiar
            c = len(have)
            if c < k:
                targets = [i for i in order if not mask & (1 << i)][:k - c]
                for i in targets:
                    recv[i] += size
                extra += size * len(targets)
                files += n
            elif c > k:
                freed += size * (c - k)
        per_volume = []
        shortfall = 0
        for i in order:
            vi = info[i]
            after = vi["free"] - recv[i]
            short = max(0, recv[i] - vi["usable"])
            shortfall += short
            per_volume.append({"path": vi["path"], "receives": recv[i], "free_after": after,
                               "used_pct_after": round(100 * (vi["total"] - after) / vi["total"], 1)
                               if vi["total"] else None,
                               "shortfall": short})
        options.append({
            "factor": k, "extra_bytes": extra, "freed_bytes": freed, "files_to_copy": files,
            "feasible": shortfall == 0, "shortfall_bytes": shortfall,
            "reason": None if shortfall == 0 else "falta espaço",
            "per_volume": per_volume,
        })
    # Fator acima do nº de discos saudáveis: não há onde pôr as cópias.
    for k in range(len(order) + 1, len(vols) + 1):
        options.append({"factor": k, "extra_bytes": None, "freed_bytes": 0, "files_to_copy": 0,
                        "feasible": False, "shortfall_bytes": None, "reason": "faltam discos",
                        "per_volume": []})

    return {
        "volumes": info, "healthy_count": len(order), "current_factor": _current_factor(),
        "floor_bytes": floor, "contents": total_contents,
        "unreplicable_contents": sum(n for n, _ in orphan),
        "unreplicable_bytes": sum(b for _, b in orphan),
        "options": options, "computed_at": datetime.now().isoformat(timespec="seconds"),
    }


def get(db: Session, refresh: bool = False) -> dict:
    key = (tuple(str(v) for v in storage.STORAGE_VOLUMES), tuple(sorted(map(str, storage.healthy_volumes()))),
           storage.REPLICATION_FACTOR, storage.STORAGE_FALLBACK_THRESHOLD_GB)
    now = time.time()
    with _lock:
        if not refresh and _cache["data"] is not None and _cache["key"] == key and now - _cache["ts"] < _TTL:
            return _cache["data"]
    data = compute(db)
    with _lock:
        _cache.update({"ts": now, "key": key, "data": data})
    return data
