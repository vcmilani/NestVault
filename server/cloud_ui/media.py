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
from auth import is_admin
from cache_state import invalidate_activity
from database import (BackupID, BackupVersion, FileContent, MaintenanceJob, MediaInfo, User,
                      VersionFile, TRASHED_STATUS)
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


# Pastas "ocultas": o álbum Hidden do iCloud Photos (o rclone usa o nome em inglês;
# os demais são o que um export manual em PT-BR costuma ter) e pastas com ponto.
HIDDEN_FOLDERS = {"hidden", "ocultas", "ocultos", "oculta", "oculto"}


def in_hidden_folder(rel: str) -> bool:
    return any(seg.lower() in HIDDEN_FOLDERS or seg.startswith(".")
               for seg in rel.split("/")[:-1] if seg)


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
        if not p:
            continue
        try:
            os.remove(p)
        except FileNotFoundError:
            pass
        except OSError as exc:
            log.warning(f"[photos] nao foi possivel apagar a miniatura {p}: {exc}")


def _fmt_ts(ts: float | None) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M") if ts else "sem data de captura"


def process_one(db: Session, sha256: str, name: str) -> bool:
    """Indexa um conteúdo. Nunca levanta: falha vira status=failed com o erro.
    Toda saída — sucesso, falha, ignorado — deixa uma linha de log."""
    t0 = time.monotonic()
    kind = kind_of(name)
    fc = db.get(FileContent, sha256)
    mi = db.get(MediaInfo, sha256) or MediaInfo(sha256=sha256, kind=kind or "image", attempts=0)
    mi.attempts = (mi.attempts or 0) + 1
    mi.processed_at = datetime.now()

    skip = ("extensao nao suportada" if kind is None else
            "conteudo ausente do indice" if fc is None else
            "conteudo em quarentena" if fc.quarantined_at is not None else None)
    if skip:
        # Grava como falha definitiva: sem o registro, o item voltaria à fila a cada ciclo.
        mi.status, mi.error, mi.attempts = "failed", f"ignorado: {skip}", MAX_ATTEMPTS
        db.add(mi)
        db.commit()
        log.info(f"[photos] {sha256[:8]}… ({name}) ignorado: {skip}")
        return False

    mi.kind = kind
    no_thumb = None
    try:
        src, has_degraded = storage.readable_copy(db, sha256, "photos")
        if src is None:
            raise RuntimeError("nenhuma copia legivel" + (" (so em volume degraded)" if has_degraded else ""))
        with _workdir() as wd:
            plain = _plain_copy(src, fc, kind, wd)
            if plain is None:
                meta, img = {"width": None, "height": None, "taken_ts": None, "duration": None}, None
                no_thumb = f"video cifrado acima de {VIDEO_DECRYPT_MAX // 1024 ** 3} GB"
            elif kind == "image":
                meta, img = _image(plain)
            else:
                meta, img = _video(plain, wd)
                if img is None:
                    no_thumb = "ffmpeg/ffprobe nao encontrados no PATH"
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
        ok = False
    db.add(mi)
    db.commit()

    dt = time.monotonic() - t0
    if ok:
        dims = f"{mi.width}x{mi.height}" if mi.width else "dimensoes desconhecidas"
        thumb = "miniaturas ok" + (" (cifradas)" if mi.thumb_encrypted and mi.thumb_sm else "") \
            if mi.thumb_sm else f"sem miniatura ({no_thumb})"
        log.info(f"[photos] {sha256[:8]}… {name} — {kind} {dims}, {_fmt_ts(mi.taken_ts)}, {thumb}, {dt:.2f}s")
    else:
        final = mi.attempts >= MAX_ATTEMPTS
        log.warning(f"[photos] {sha256[:8]}… {name} falhou (tentativa {mi.attempts}/{MAX_ATTEMPTS}"
                    f"{', desistindo' if final else f', nova tentativa em {RETRY_AFTER}'}): {mi.error}")
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
        log.info(f"[photos] {len(orphans)} registro(s) de midia orfao(s) removido(s) "
                 f"(conteudo ja apagado do storage), com as miniaturas")
    return len(orphans)


def in_window(now: datetime | None = None) -> bool:
    start, end = config.get("photos.window_start_hour"), config.get("photos.window_end_hour")
    if start == end:
        return True
    h = (now or datetime.now()).hour
    return start <= h < end if start < end else (h >= start or h < end)


def pause_reason() -> str | None:
    if not config.get("photos.indexing_enabled"):
        return "desligada em Config (photos.indexing_enabled)"
    if not in_window():
        return (f"fora da janela {config.get('photos.window_start_hour'):02d}h–"
                f"{config.get('photos.window_end_hour'):02d}h")
    return None


class Indexer:
    """Um thread, acordado a cada 10 min ou quando uma versão termina (wake()).

    Cada ciclo que encontra trabalho vira um MaintenanceJob "photos-index", com
    progresso a cada lote — a indexação aparece na Atividade como os outros jobs."""

    INTERVAL = 600
    RESCAN_EVERY = 6 * 3600

    def __init__(self):
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._scanned: set[int] = set()   # versões já varridas por inteiro
        self._scanned_at = 0.0
        self._paused: str | None = None
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
        reason = pause_reason()
        log.info("[photos] indexador iniciado — primeiro ciclo em 30s"
                 + (f" (por ora pausado: {reason})" if reason else ""))

    def stop(self) -> None:
        if self._thread and self._thread.is_alive():
            log.info("[photos] indexador encerrando")
        self._stop.set()
        self._wake.set()

    def wake(self) -> None:
        self._wake.set()

    def _should_continue(self) -> bool:
        return not self._stop.is_set() and pause_reason() is None

    def _loop(self, session_factory) -> None:
        self._stop.wait(30)  # deixa o boot terminar antes de disputar disco/banco
        # Sobras de um processamento interrompido (queda de energia, kill). Só este
        # thread escreve em .tmp, então antes do primeiro ciclo é seguro apagar tudo.
        tmp = _thumbs_root() / ".tmp"
        if tmp.exists() and any(tmp.iterdir()):
            log.info(f"[photos] removendo temporarios de um processamento interrompido em {tmp}")
        shutil.rmtree(tmp, ignore_errors=True)
        while not self._stop.is_set():
            reason = pause_reason()
            if reason != self._paused:
                # Loga só a transição, não a cada 10 min parado.
                log.info(f"[photos] indexacao pausada: {reason}" if reason else "[photos] indexacao retomada")
                self._paused = reason
            if reason is None:
                try:
                    self.run_once(session_factory)
                except Exception:  # o loop não pode morrer
                    log.exception("[photos] ciclo de indexacao falhou")
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
        done = failed = 0
        job_id = None
        t0 = time.monotonic()
        stopped = None
        self.running = True

        def progress(final: bool = False) -> None:
            nonlocal job_id
            text_ = f"{done} arquivo(s) processado(s), {failed} falha(s)"
            if job_id is None:
                mj = MaintenanceJob(job_type="photos-index", status="running",
                                    summary=f"Indexando fotos: {text_}")
                db.add(mj)
                db.commit()
                job_id = mj.id
            else:
                mj = db.get(MaintenanceJob, job_id)
                if final:
                    mj.status, mj.finished_at = "done", datetime.now()
                    mj.summary = (f"{text_} em {time.monotonic() - t0:.0f}s"
                                  + (f" — pausado: {stopped}" if stopped else ""))
                else:
                    mj.summary = f"Indexando fotos: {text_}"
                db.commit()
            invalidate_activity()

        try:
            sweep_orphans(db)
            # Só backups marcados para Fotos: não gasta CPU do Pi com, por exemplo,
            # os JPGs escaneados de um backup de documentos.
            versions = (db.query(BackupVersion.id, BackupVersion.backup_label, BackupVersion.version_key)
                        .join(BackupID, BackupID.label == BackupVersion.backup_label)
                        .filter(BackupVersion.status == "done",
                                BackupID.photos_enabled.is_(True),
                                tree.live_label_filter())
                        .order_by(BackupVersion.id.desc()).all())
            for vid, label, vkey in versions:
                if vid in self._scanned:
                    continue
                first = True
                while stopped is None:
                    if not should_continue():
                        stopped = pause_reason() or "servidor encerrando"
                        break
                    batch = pending_in_version(db, vid, BATCH)
                    if not batch:
                        break
                    if job_id is None:
                        log.info("[photos] ciclo de indexacao iniciado (job photos-index)")
                        progress()
                    if first:
                        log.info(f"[photos] processando {label}/{vkey}")
                        first = False
                    for sha256, path in batch:
                        if not should_continue():
                            stopped = pause_reason() or "servidor encerrando"
                            break
                        if process_one(db, sha256, path.rsplit("/", 1)[-1]):
                            self.processed += 1
                        else:
                            self.failed += 1
                            failed += 1
                        done += 1
                    bump_generation()
                    progress()
                if stopped:
                    break
                self._scanned.add(vid)
            if job_id is not None:
                progress(final=True)
                log.info(f"[photos] ciclo de indexacao concluido: {done} processado(s), {failed} falha(s) "
                         f"em {time.monotonic() - t0:.0f}s" + (f" — pausado: {stopped}" if stopped else ""))
            return done
        except Exception as exc:
            if job_id is not None:
                try:
                    db.rollback()
                    mj = db.get(MaintenanceJob, job_id)
                    mj.status, mj.finished_at = "error", datetime.now()
                    mj.summary = f"Erro: {type(exc).__name__}: {exc} — parou em: {done} processado(s)"
                    db.commit()
                    invalidate_activity()
                except Exception:
                    log.exception("[photos] nao foi possivel fechar o job photos-index como erro")
            raise
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
        # Uma foto oculta no iCloud pode aparecer também em outro álbum: o conteúdo
        # inteiro conta como oculto, não só a cópia que está na pasta Hidden.
        hidden_shas = set()
        for r in rows:
            label, base = by_vid[r.version_id]
            rel = r.original_path[len(base):] if r.original_path.startswith(base) else r.original_path
            if in_hidden_folder(rel):
                hidden_shas.add(r.sha256)
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
                "hidden": r.sha256 in hidden_shas,
            })
        items = pair_live_photos(items)
        items.sort(key=lambda x: (x["ts"], x["id"]), reverse=True)

    with _timeline_lock:
        _timeline_cache[user.id] = (key, items)
    return items


# Live Photo (iPhone): a foto e um vídeo curto com o MESMO nome-base na mesma pasta —
# IMG_1234.HEIC + IMG_1234.MOV. Na galeria viram um item só: a foto, com o vídeo
# anexado em "live"; o .MOV não aparece solto.
LIVE_STILL_EXT = ("heic", "heif", "jpg", "jpeg")
LIVE_MOTION_EXT = ("mov", "mp4")
# A de iPhone tem ~3 s. Duração conhecida acima disto = vídeo comum que só
# coincide no nome — não some da galeria. Sem ffprobe a duração é desconhecida e
# vale só o nome.
LIVE_MAX_DURATION = 6.0


def pair_live_photos(items: list[dict]) -> list[dict]:
    groups: dict[tuple, dict[str, list[dict]]] = {}
    for it in items:
        stem, _, ext = it["path"].rpartition(".")
        ext = ext.lower()
        if ext in LIVE_STILL_EXT:
            role = "still"
        elif ext in LIVE_MOTION_EXT:
            role = "motion"
        else:
            continue
        g = groups.setdefault((it["label"], stem.lower()), {"still": [], "motion": []})
        g[role].append(it)

    absorbed: set[int] = set()
    for g in groups.values():
        if not g["still"] or not g["motion"]:
            continue
        motion = min(g["motion"], key=lambda m: m["id"])
        if motion["duration"] and motion["duration"] > LIVE_MAX_DURATION:
            continue
        live = {"id": motion["id"], "sha256": motion["sha256"], "name": motion["name"],
                "duration": motion["duration"], "thumb": motion["thumb"]}
        for still in g["still"]:
            still["live"] = live
        absorbed.add(motion["id"])
    return [it for it in items if it["id"] not in absorbed]


def visible(items: list[dict], show_hidden: bool) -> list[dict]:
    return items if show_hidden else [i for i in items if not i["hidden"]]


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
    if is_admin(user):
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
