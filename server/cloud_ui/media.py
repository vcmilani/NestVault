"""Fotos: indexação em background (EXIF + miniaturas) e a timeline por usuário.

Indexação
---------
A fila não é uma tabela: "pendente" é todo sha256 de foto/vídeo referenciado por uma
versão done e ainda sem MediaInfo. Assim os backups que já existiam entram sozinhos
(backfill) e os novos também — sem migração e sem estado a manter em sincronia.

Varre as versões da mais nova para a mais antiga, para que a timeline fique útil
cedo. Cada conteúdo é processado uma vez (dedup por sha256). Uma foto por vez, num
único thread, dentro da janela de horário de config.photos.* — pensado para Pi.

Miniaturas ficam em <volume>/_thumbs/; se o original está cifrado, a miniatura
também é cifrada (é tão pessoal quanto a foto).
"""
import json
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import and_, exists, func, or_
from sqlalchemy.orm import Session

import config
import crypto
import storage
from database import (BackupID, BackupVersion, FileContent, MediaInfo, User, VersionFile,
                      TRASHED_STATUS)
from . import tree

log = logging.getLogger("backup-server")

IMAGE_EXT = ("jpg", "jpeg", "png", "gif", "webp", "heic", "heif", "avif", "bmp", "tif", "tiff")
VIDEO_EXT = ("mp4", "m4v", "mov", "3gp", "mkv", "webm", "avi")

SM_BOX = (960, 320)   # grade: altura ~320px, panorâmica até 960 de largura
LG_MAX = 1600         # visualizador
MAX_ATTEMPTS = 3
RETRY_AFTER = timedelta(hours=1)
BATCH = 50
# Vídeo cifrado precisa ser decifrado inteiro num temporário para o ffmpeg ler
# (o moov do MP4 pode estar no fim). Acima disso, fica sem miniatura.
VIDEO_DECRYPT_MAX = 2 * 1024 ** 3

AUTOSTART = True  # tests/conftest desliga: o thread usaria o banco do teste em paralelo

try:  # HEIC/HEIF (iPhone) — opcional: sem o pacote, essas fotos ficam sem miniatura
    import pillow_heif
    pillow_heif.register_heif_opener()
except ImportError:  # pragma: no cover
    pass


def kind_of(name: str) -> str | None:
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    if ext in IMAGE_EXT:
        return "image"
    if ext in VIDEO_EXT:
        return "video"
    return None


def media_path_filter():
    p = func.lower(VersionFile.original_path)
    return or_(*[p.like(f"%.{e}") for e in IMAGE_EXT + VIDEO_EXT])


# -- Estado/geração -----------------------------------------------------------
# A geração sobe a cada lote gravado; o cache da timeline a usa como chave.
_generation = 0
_gen_lock = threading.Lock()


def bump_generation() -> None:
    global _generation
    with _gen_lock:
        _generation += 1


# -- Extração -----------------------------------------------------------------

def _parse_exif_dt(raw) -> float | None:
    if not raw:
        return None
    try:
        dt = datetime.strptime(str(raw).strip().rstrip("\x00")[:19], "%Y:%m:%d %H:%M:%S")
    except ValueError:
        return None
    # "0000:00:00 00:00:00" e relógio de câmera desacertado no futuro não servem
    if dt.year < 1980 or dt > datetime.now() + timedelta(days=1):
        return None
    return dt.timestamp()


def _image(path: Path) -> tuple[dict, "object"]:
    from PIL import Image, ImageOps

    with Image.open(path) as im:
        w, h = im.size
        exif = im.getexif()
        orientation = exif.get(274)
        sub = exif.get_ifd(0x8769)
        taken = (_parse_exif_dt(sub.get(36867)) or _parse_exif_dt(sub.get(36868))
                 or _parse_exif_dt(exif.get(306)))
        # draft: o JPEG já é decodificado em escala reduzida (1/2, 1/4, 1/8) — é o que
        # torna a miniatura de uma foto de 12 MP barata num Pi. No-op em outros formatos.
        im.draft("RGB", (LG_MAX, LG_MAX))
        img = ImageOps.exif_transpose(im)
        if img.mode in ("RGBA", "LA", "P"):
            img = img.convert("RGBA")
            bg = Image.new("RGB", img.size, (255, 255, 255))
            bg.paste(img, mask=img.split()[-1])
            img = bg
        else:
            img = img.convert("RGB")
    if orientation in (5, 6, 7, 8):
        w, h = h, w
    return {"width": w, "height": h, "taken_ts": taken, "duration": None}, img


def _ffprobe(path: Path) -> dict:
    out = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", str(path)],
        capture_output=True, timeout=60, check=True).stdout
    return json.loads(out or b"{}")


def video_meta_from_probe(probe: dict) -> dict:
    fmt = probe.get("format") or {}
    vs = next((s for s in probe.get("streams") or [] if s.get("codec_type") == "video"), {})
    w, h = vs.get("width"), vs.get("height")
    rot = (vs.get("tags") or {}).get("rotate")
    for sd in vs.get("side_data_list") or []:
        rot = sd.get("rotation", rot)
    try:
        if w and h and abs(int(float(rot or 0))) % 180 == 90:
            w, h = h, w
    except ValueError:
        pass
    taken = None
    ct = (fmt.get("tags") or {}).get("creation_time") or (vs.get("tags") or {}).get("creation_time")
    if ct:
        try:
            # creation_time vem em UTC ("...Z"); vira epoch de verdade.
            dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
            if dt.year >= 1980:
                taken = dt.timestamp()
        except ValueError:
            pass
    try:
        duration = float(fmt.get("duration")) if fmt.get("duration") else None
    except ValueError:
        duration = None
    return {"width": w, "height": h, "taken_ts": taken, "duration": duration}


def _video(path: Path, workdir: Path):
    if not (shutil.which("ffprobe") and shutil.which("ffmpeg")):
        return {"width": None, "height": None, "taken_ts": None, "duration": None}, None
    meta = video_meta_from_probe(_ffprobe(path))
    frame = workdir / "frame.jpg"
    at = min(1.0, (meta["duration"] or 0) / 2)
    subprocess.run(["ffmpeg", "-v", "quiet", "-y", "-ss", f"{at:.2f}", "-i", str(path),
                    "-frames:v", "1", "-vf", f"scale='min({LG_MAX},iw)':-2", str(frame)],
                   capture_output=True, timeout=120, check=True)
    from PIL import Image
    with Image.open(frame) as im:
        img = im.convert("RGB")
    return meta, img


# -- Arquivos -----------------------------------------------------------------

def _thumbs_root() -> Path:
    vols = storage.healthy_volumes() or storage.STORAGE_VOLUMES
    return vols[0] / "_thumbs"


@contextmanager
def _workdir():
    root = _thumbs_root() / ".tmp"
    root.mkdir(parents=True, exist_ok=True)
    d = Path(tempfile.mkdtemp(dir=root))
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


def _plain_copy(src: Path, fc: FileContent, kind: str, workdir: Path) -> Path | None:
    """Caminho legível em texto claro: o próprio arquivo, ou um temporário decifrado."""
    if not fc.encrypted:
        return src
    if kind == "video" and fc.size > VIDEO_DECRYPT_MAX:
        return None
    dst = workdir / "plain"
    with open(dst, "wb") as f:
        for chunk in crypto.decrypt_chunks(src, storage.encryption_key):
            f.write(chunk)
    return dst


def _save_thumb(img, box, dest: Path, encrypt: bool, workdir: Path) -> str:
    t = img.copy()
    t.thumbnail(box)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = workdir / dest.name
    t.save(tmp, "JPEG", quality=82, optimize=True, progressive=True)
    if encrypt:
        crypto.encrypt_stream(tmp, workdir / (dest.name + ".enc"), storage.encryption_key)
        tmp = workdir / (dest.name + ".enc")
    os.replace(tmp, dest)
    return str(dest)


def _remove_thumbs(mi: MediaInfo) -> None:
    for p in (mi.thumb_sm, mi.thumb_lg):
        if p:
            try:
                os.remove(p)
            except OSError:
                pass


def process_one(db: Session, sha256: str, name: str) -> bool:
    """Indexa um conteúdo. Nunca levanta: falha vira status=failed com o erro."""
    kind = kind_of(name)
    fc = db.get(FileContent, sha256)
    if kind is None or fc is None or fc.quarantined_at is not None:
        return False
    mi = db.get(MediaInfo, sha256) or MediaInfo(sha256=sha256, kind=kind, attempts=0)
    mi.kind = kind
    mi.attempts = (mi.attempts or 0) + 1
    mi.processed_at = datetime.now()
    try:
        src, _ = storage.readable_copy(db, sha256, "photos")
        if src is None:
            raise RuntimeError("nenhuma copia legivel")
        with _workdir() as wd:
            plain = _plain_copy(src, fc, kind, wd)
            if plain is None:
                meta, img = {"width": None, "height": None, "taken_ts": None, "duration": None}, None
            elif kind == "image":
                meta, img = _image(plain)
            else:
                meta, img = _video(plain, wd)
            _remove_thumbs(mi)
            mi.thumb_sm = mi.thumb_lg = None
            if img is not None:
                base = _thumbs_root() / sha256[:2]
                enc = bool(fc.encrypted)
                mi.thumb_sm = _save_thumb(img, SM_BOX, base / f"{sha256}_sm.jpg", enc, wd)
                mi.thumb_lg = _save_thumb(img, (LG_MAX, LG_MAX), base / f"{sha256}_lg.jpg", enc, wd)
                mi.thumb_encrypted = enc
        mi.width, mi.height = meta["width"], meta["height"]
        mi.taken_ts, mi.duration = meta["taken_ts"], meta["duration"]
        mi.status, mi.error = "done", None
        ok = True
    except Exception as exc:
        mi.status, mi.error = "failed", f"{type(exc).__name__}: {exc}"[:500]
        log.warning(f"[photos] {sha256[:8]}… ({name}) falhou (tentativa {mi.attempts}): {mi.error}")
        ok = False
    db.add(mi)
    db.commit()
    return ok


def _not_done_filter():
    """Sem MediaInfo, ou falhou com tentativas sobrando e já passou o intervalo."""
    retry_before = datetime.now() - RETRY_AFTER
    settled = exists().where(and_(
        MediaInfo.sha256 == VersionFile.sha256,
        or_(MediaInfo.status == "done",
            MediaInfo.attempts >= MAX_ATTEMPTS,
            MediaInfo.processed_at > retry_before),
    ))
    return ~settled


def pending_in_version(db: Session, version_id: int, limit: int) -> list[tuple[str, str]]:
    return (db.query(VersionFile.sha256, func.min(VersionFile.original_path))
            .join(FileContent, FileContent.sha256 == VersionFile.sha256)
            .filter(VersionFile.version_id == version_id,
                    FileContent.quarantined_at.is_(None),
                    media_path_filter(), _not_done_filter())
            .group_by(VersionFile.sha256)
            .limit(limit)
            .all())


def sweep_orphans(db: Session) -> int:
    """Remove MediaInfo (e miniaturas) de conteúdo que já saiu de file_contents."""
    orphans = (db.query(MediaInfo)
               .filter(~exists().where(FileContent.sha256 == MediaInfo.sha256))
               .limit(1000).all())
    for mi in orphans:
        _remove_thumbs(mi)
        db.delete(mi)
    if orphans:
        db.commit()
        bump_generation()
    return len(orphans)


def in_window(now: datetime | None = None) -> bool:
    start, end = config.get("photos.window_start_hour"), config.get("photos.window_end_hour")
    if start == end:
        return True
    h = (now or datetime.now()).hour
    return start <= h < end if start < end else (h >= start or h < end)


class Indexer:
    """Um thread, acordado a cada 10 min ou quando uma versão termina (wake())."""

    INTERVAL = 600
    RESCAN_EVERY = 6 * 3600

    def __init__(self):
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._scanned: set[int] = set()   # versões já varridas por inteiro
        self._scanned_at = 0.0
        self.running = False
        self.processed = 0
        self.failed = 0

    def start(self, session_factory) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, args=(session_factory,),
                                        name="photos-indexer", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def wake(self) -> None:
        self._wake.set()

    def _should_continue(self) -> bool:
        return (not self._stop.is_set() and config.get("photos.indexing_enabled") and in_window())

    def _loop(self, session_factory) -> None:
        self._stop.wait(30)  # deixa o boot terminar antes de disputar disco/banco
        # Sobras de um processamento interrompido (queda de energia, kill). Só este
        # thread escreve em .tmp, então antes do primeiro ciclo é seguro apagar tudo.
        shutil.rmtree(_thumbs_root() / ".tmp", ignore_errors=True)
        while not self._stop.is_set():
            if self._should_continue():
                try:
                    self.run_once(session_factory)
                except Exception as exc:  # pragma: no cover - o loop não pode morrer
                    log.error(f"[photos] ciclo de indexacao falhou: {exc}")
            self._wake.wait(self.INTERVAL)
            self._wake.clear()

    def run_once(self, session_factory, should_continue=None) -> int:
        """Processa tudo o que estiver pendente (ou até should_continue() dizer não)."""
        should_continue = should_continue or self._should_continue
        if time.time() - self._scanned_at > self.RESCAN_EVERY:
            # Recomeça a varredura de tempos em tempos: é o que pega as falhas a
            # retentar em versões já varridas.
            self._scanned.clear()
            self._scanned_at = time.time()
        db = session_factory()
        done = 0
        self.running = True
        try:
            sweep_orphans(db)
            # Só backups marcados para Fotos: não gasta CPU do Pi com, por exemplo,
            # os JPGs escaneados de um backup de documentos.
            version_ids = [vid for (vid,) in db.query(BackupVersion.id)
                           .join(BackupID, BackupID.label == BackupVersion.backup_label)
                           .filter(BackupVersion.status == "done",
                                   BackupID.photos_enabled.is_(True),
                                   tree.live_label_filter())
                           .order_by(BackupVersion.id.desc()).all()]
            for vid in version_ids:
                if vid in self._scanned:
                    continue
                while True:
                    if not should_continue():
                        return done
                    batch = pending_in_version(db, vid, BATCH)
                    if not batch:
                        break
                    for sha256, path in batch:
                        if not should_continue():
                            bump_generation()
                            return done
                        if process_one(db, sha256, path.rsplit("/", 1)[-1]):
                            self.processed += 1
                        else:
                            self.failed += 1
                        done += 1
                    bump_generation()
                self._scanned.add(vid)
            if done:
                log.info(f"[photos] indexacao: {done} arquivo(s) processado(s) neste ciclo")
            return done
        finally:
            self.running = False
            db.close()


indexer = Indexer()


# -- Timeline -----------------------------------------------------------------

_timeline_cache: dict[int, tuple[tuple, list[dict]]] = {}
_timeline_lock = threading.Lock()


def _latest_versions(db: Session, user: User) -> dict[str, tuple[BackupVersion, str]]:
    """label → (última versão done, raiz do label) dos labels do próprio usuário
    marcados para aparecer em Fotos."""
    out = {}
    labels = [b.label for b in db.query(BackupID.label)
              .filter(BackupID.owner_user_id == user.id, BackupID.photos_enabled.is_(True),
                      tree.live_label_filter()).all()]
    for label in labels:
        versions = tree.done_versions(db, label)
        if versions:
            out[label] = (versions[0], tree.label_base(db, label, versions))
    return out


def timeline(db: Session, user: User) -> list[dict]:
    """Todas as fotos/vídeos das últimas versões done do usuário, mais recentes
    primeiro, sem repetir conteúdo. Cacheado até mudar o conjunto de versões ou a
    indexação gravar um lote novo."""
    latest = _latest_versions(db, user)
    key = (tuple(sorted((l, v.id) for l, (v, _) in latest.items())), _generation)
    with _timeline_lock:
        hit = _timeline_cache.get(user.id)
        if hit and hit[0] == key:
            return hit[1]

    by_vid = {v.id: (label, base) for label, (v, base) in latest.items()}
    items: list[dict] = []
    if by_vid:
        rows = (db.query(VersionFile.id, VersionFile.sha256, VersionFile.original_path,
                         VersionFile.mtime, VersionFile.version_id,
                         MediaInfo.status, MediaInfo.attempts, MediaInfo.taken_ts, MediaInfo.width,
                         MediaInfo.height, MediaInfo.duration, MediaInfo.thumb_sm)
                .outerjoin(MediaInfo, MediaInfo.sha256 == VersionFile.sha256)
                .filter(VersionFile.version_id.in_(list(by_vid)), media_path_filter())
                .all())
        seen: set[str] = set()
        for r in rows:
            if r.sha256 in seen:
                continue
            seen.add(r.sha256)
            label, base = by_vid[r.version_id]
            rel = r.original_path[len(base):] if r.original_path.startswith(base) else r.original_path
            name = rel.rsplit("/", 1)[-1]
            if r.status == "done":
                state = "done"
            elif r.status == "failed" and (r.attempts or 0) >= MAX_ATTEMPTS:
                state = "failed"
            else:
                state = "pending"
            items.append({
                "id": r.id, "sha256": r.sha256, "kind": kind_of(name),
                "ts": r.taken_ts if r.taken_ts else r.mtime,
                "dated": bool(r.taken_ts),
                "w": r.width, "h": r.height, "duration": r.duration,
                "thumb": bool(r.thumb_sm), "state": state,
                "label": label, "path": rel, "name": name,
            })
        items.sort(key=lambda x: (x["ts"], x["id"]), reverse=True)

    with _timeline_lock:
        _timeline_cache[user.id] = (key, items)
    return items


def page(items: list[dict], before_ts: float | None, before_id: int | None, limit: int) -> list[dict]:
    """Página keyset: itens estritamente "depois" de (before_ts, before_id) na ordem desc."""
    if before_ts is None:
        return items[:limit]
    bid = before_id if before_id is not None else float("inf")
    for i, it in enumerate(items):
        if (it["ts"], it["id"]) < (before_ts, bid):
            return items[i:i + limit]
    return []


def months(items: list[dict]) -> list[dict]:
    out: dict[str, dict] = {}
    for it in items:
        key = datetime.fromtimestamp(it["ts"]).strftime("%Y-%m")
        m = out.get(key)
        if m is None:
            # Cursor para pular direto ao mês: o primeiro item dele.
            out[key] = {"month": key, "count": 1, "ts": it["ts"], "id": it["id"]}
        else:
            m["count"] += 1
    return list(out.values())


def indexing_status(items: list[dict]) -> dict:
    total = len(items)
    done = sum(1 for i in items if i["state"] == "done")
    failed = sum(1 for i in items if i["state"] == "failed")
    return {"total": total, "indexed": done, "failed": failed, "pending": total - done - failed,
            "running": indexer.running,
            "enabled": bool(config.get("photos.indexing_enabled")),
            "in_window": in_window()}


def thumb_visible_to(db: Session, user: User, sha256: str) -> bool:
    if user.role == "admin":
        return True
    return db.query(exists().where(and_(
        VersionFile.sha256 == sha256,
        BackupVersion.id == VersionFile.version_id,
        BackupVersion.status != TRASHED_STATUS,
        BackupID.label == BackupVersion.backup_label,
        BackupID.owner_user_id == user.id,
        tree.live_label_filter(),
    ))).scalar()


def photo_labels(db: Session, user: User) -> list[dict]:
    """Os backups do usuário com a escolha de entrar ou não em Fotos e quantas
    fotos/vídeos a última versão de cada um tem (para decidir com informação)."""
    out = []
    for b in (db.query(BackupID)
              .filter(BackupID.owner_user_id == user.id, tree.live_label_filter())
              .order_by(BackupID.label).all()):
        versions = tree.done_versions(db, b.label)
        count = 0
        if versions:
            count = (db.query(func.count(func.distinct(VersionFile.sha256)))
                     .filter(VersionFile.version_id == versions[0].id, media_path_filter())
                     .scalar()) or 0
        out.append({"label": b.label, "client_name": b.client_name,
                    "enabled": bool(b.photos_enabled), "media_count": count})
    return out
