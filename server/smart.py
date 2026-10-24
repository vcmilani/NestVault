"""Saúde dos discos de storage via SMART (v10.2.0).

A cada smart.check_interval_hours lê `smartctl -j -a` de cada volume em
storage.STORAGE_VOLUMES, grava uma SmartSnapshot e avisa no Telegram quando o
disco piora em relação à leitura anterior:
  - o autoteste geral (smart_status.passed) passou a falhar;
  - setores realocados (ID 5), pendentes (197) ou incorrigíveis (198 / NVMe
    media_errors) aumentaram;
  - a temperatura cruzou smart.max_temp_c (só na subida, não a cada leitura).

Só Linux: o volume vira dispositivo por `findmnt` + `lsblk` (partição → disco).
Bridges USB costumam exigir `-d sat`, tentado automaticamente quando a leitura
padrão não traz dados. smart.device_overrides cobre o resto.
"""
import json
import logging
import re
import shutil
import subprocess
import sys
import threading
from datetime import datetime, timedelta

import config
import notify
import storage
from database import SessionLocal, SmartSnapshot

log = logging.getLogger("backup-server")

_RETENTION_DAYS = 90
_TIMEOUT = 60

# Última falha de leitura por volume (exibida na página Discos). Só memória:
# um disco "indisponível" não precisa de histórico.
_last_errors: dict[str, str] = {}
_run_lock = threading.Lock()


class SmartUnavailable(Exception):
    """Não foi possível ler o SMART deste volume (motivo legível na mensagem)."""


# -- Volume → dispositivo -----------------------------------------------------

def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=_TIMEOUT)


def _overrides() -> dict[str, tuple[str, str | None]]:
    """smart.device_overrides: ["/mnt/hd1=/dev/sda", "/mnt/hd2=/dev/sdb:sat"]."""
    out: dict[str, tuple[str, str | None]] = {}
    for raw in config.get("smart.device_overrides") or []:
        vol, sep, dev = str(raw).partition("=")
        if not sep or not vol.strip() or not dev.strip():
            log.warning(f"[smart] override ignorado (esperado volume=dispositivo): {raw!r}")
            continue
        dev, _, dtype = dev.strip().partition(":")
        out[vol.strip().rstrip("/") or "/"] = (dev, dtype or None)
    return out


def parent_disk(source: str) -> str:
    """'/dev/sda1' → '/dev/sda'; '/dev/nvme0n1p2' → '/dev/nvme0n1'. Usa lsblk
    quando disponível (cobre LVM/dm), senão tira o sufixo da partição."""
    if shutil.which("lsblk"):
        try:
            r = _run(["lsblk", "-no", "PKNAME", source])
            pk = r.stdout.strip().splitlines()
            if r.returncode == 0 and pk and pk[0].strip():
                return f"/dev/{pk[0].strip()}"
        except Exception:
            pass
    m = re.match(r"^(/dev/(?:nvme\d+n\d+|mmcblk\d+))p\d+$", source)
    if m:
        return m.group(1)
    m = re.match(r"^(/dev/[a-z]+)\d+$", source)
    return m.group(1) if m else source


def resolve_device(volume: str) -> tuple[str, str | None]:
    """Dispositivo (e tipo do -d, se houver) que hospeda `volume`."""
    key = str(volume).rstrip("/") or "/"
    ov = _overrides().get(key)
    if ov:
        return ov
    if not sys.platform.startswith("linux"):
        raise SmartUnavailable("leitura SMART só é suportada no Linux")
    if not shutil.which("findmnt"):
        raise SmartUnavailable("findmnt não encontrado")
    r = _run(["findmnt", "-no", "SOURCE", "--target", str(volume)])
    source = r.stdout.strip().splitlines()[0].strip() if r.stdout.strip() else ""
    if r.returncode != 0 or not source.startswith("/dev/"):
        raise SmartUnavailable(f"volume não está num dispositivo de bloco ({source or 'desconhecido'})")
    return parent_disk(source), None


# -- smartctl -----------------------------------------------------------------

def _smartctl_cmd(device: str, dtype: str | None) -> list[str]:
    cmd = ["smartctl", "-j", "-a"]
    if dtype:
        cmd += ["-d", dtype]
    cmd.append(device)
    if config.get("smart.use_sudo"):
        cmd = ["sudo", "-n"] + cmd
    return cmd


def _has_data(data: dict) -> bool:
    return "smart_status" in data or "nvme_smart_health_information_log" in data \
        or "ata_smart_attributes" in data


def read_smartctl(device: str, dtype: str | None) -> dict:
    """JSON do smartctl. Sem tipo explícito e sem dados, tenta de novo com -d sat
    (bridge USB-SATA, o caso comum de HD externo no Pi)."""
    if not shutil.which("smartctl"):
        raise SmartUnavailable("smartctl não instalado (pacote smartmontools)")
    tries = [dtype] if dtype else [None, "sat"]
    last_msg = ""
    for t in tries:
        r = _run(_smartctl_cmd(device, t))
        if "sudo" in r.stderr and "password" in r.stderr:
            raise SmartUnavailable("sudo pede senha — libere o smartctl no sudoers ou desligue smart.use_sudo")
        try:
            data = json.loads(r.stdout or "{}")
        except json.JSONDecodeError:
            data = {}
        if _has_data(data):
            return data
        msgs = (data.get("smartctl") or {}).get("messages") or []
        last_msg = "; ".join(m.get("string", "") for m in msgs if m.get("string")) \
            or r.stderr.strip() or f"smartctl saiu com código {r.returncode}"
    raise SmartUnavailable(last_msg)


def _ata_raw(data: dict, attr_id: int) -> int | None:
    for row in (data.get("ata_smart_attributes") or {}).get("table") or []:
        if row.get("id") == attr_id:
            raw = row.get("raw") or {}
            val = raw.get("value")
            return int(val) if isinstance(val, (int, float)) else None
    return None


def parse(data: dict) -> dict:
    """Extrai os campos de SmartSnapshot do JSON do smartctl (ATA/SAT ou NVMe)."""
    nvme = data.get("nvme_smart_health_information_log") or {}
    status = data.get("smart_status") or {}
    temp = (data.get("temperature") or {}).get("current")
    if temp is None:
        temp = nvme.get("temperature")
    hours = (data.get("power_on_time") or {}).get("hours")
    if hours is None:
        hours = nvme.get("power_on_hours")
    passed = status.get("passed")
    if passed is None and nvme:
        passed = not nvme.get("critical_warning")
    uncorrectable = _ata_raw(data, 198)
    if uncorrectable is None and nvme:
        uncorrectable = nvme.get("media_errors")
    return {
        "passed": passed,
        "temp_c": int(temp) if isinstance(temp, (int, float)) else None,
        "power_on_hours": int(hours) if isinstance(hours, (int, float)) else None,
        "reallocated": _ata_raw(data, 5),
        "pending": _ata_raw(data, 197),
        "uncorrectable": uncorrectable,
        "model": data.get("model_name") or data.get("model_family"),
    }


def health(s: dict | SmartSnapshot) -> str:
    """"fail" | "warn" | "ok" para o card da página Discos."""
    g = s.get if isinstance(s, dict) else (lambda k: getattr(s, k))
    if g("passed") is False or (g("uncorrectable") or 0) > 0 or (g("pending") or 0) > 0:
        return "fail"
    max_temp = config.get("smart.max_temp_c")
    if (g("reallocated") or 0) > 0 or (max_temp and (g("temp_c") or 0) >= max_temp):
        return "warn"
    return "ok"


# -- Comparação e alertas -----------------------------------------------------

_COUNTERS = (("reallocated", "setores realocados"),
             ("pending", "setores pendentes"),
             ("uncorrectable", "erros incorrigíveis"))


def worsened(prev: SmartSnapshot | None, cur: dict) -> list[str]:
    """Motivos de alerta da leitura atual em relação à anterior (vazio = nada novo)."""
    reasons = []
    if cur["passed"] is False and (prev is None or prev.passed is not False):
        reasons.append("autoteste SMART reprovado")
    for key, name in _COUNTERS:
        now_v = cur.get(key)
        before = getattr(prev, key) if prev is not None else 0
        if now_v and now_v > (before or 0):
            reasons.append(f"{name}: {before or 0} → {now_v}")
    max_temp = config.get("smart.max_temp_c")
    t = cur.get("temp_c")
    if max_temp and t is not None and t >= max_temp:
        if prev is None or prev.temp_c is None or prev.temp_c < max_temp:
            reasons.append(f"temperatura {t}°C (limite {max_temp}°C)")
    return reasons


def check_volume(db, volume: str) -> SmartSnapshot:
    """Lê, grava e alerta um volume. Levanta SmartUnavailable se não der para ler."""
    vol = str(volume)
    device, dtype = resolve_device(vol)
    cur = parse(read_smartctl(device, dtype))
    prev = (db.query(SmartSnapshot)
            .filter(SmartSnapshot.volume_path == vol)
            .order_by(SmartSnapshot.sampled_at.desc(), SmartSnapshot.id.desc())
            .first())
    reasons = worsened(prev, cur)
    snap = SmartSnapshot(volume_path=vol, device=device, **cur)
    db.add(snap)
    db.commit()
    db.refresh(snap)
    if reasons:
        name = f"{vol} ({device}{', ' + cur['model'] if cur['model'] else ''})"
        notify.send_sync(f"🩺 NestVault: disco {name} piorou — " + "; ".join(reasons) + ".",
                         tag="smart")
        log.warning(f"[smart] {vol}: " + "; ".join(reasons))
    return snap


def run_smart_check() -> dict[str, str]:
    """Uma rodada em todos os volumes. Devolve {volume: "ok"|"warn"|"fail"|erro}."""
    if not _run_lock.acquire(blocking=False):
        log.info("[smart] leitura já em andamento — ignorando")
        return {}
    result: dict[str, str] = {}
    db = SessionLocal()
    try:
        for v in storage.STORAGE_VOLUMES:
            vol = str(v)
            try:
                snap = check_volume(db, vol)
                _last_errors.pop(vol, None)
                result[vol] = health(snap)
            except SmartUnavailable as e:
                _last_errors[vol] = str(e)
                result[vol] = f"indisponível: {e}"
                log.info(f"[smart] {vol}: indisponível — {e}")
            except Exception as e:
                db.rollback()
                _last_errors[vol] = f"erro: {e}"
                result[vol] = f"erro: {e}"
                log.error(f"[smart] {vol}: erro na leitura — {e}", exc_info=True)
        cutoff = datetime.now() - timedelta(days=_RETENTION_DAYS)
        db.query(SmartSnapshot).filter(SmartSnapshot.sampled_at < cutoff).delete(synchronize_session=False)
        db.commit()
    finally:
        db.close()
        _run_lock.release()
    return result


def summary(db, days: int = 30) -> list[dict]:
    """Última leitura + série de temperatura por volume, para GET /storage/smart."""
    since = datetime.now() - timedelta(days=days)
    out = []
    for v in storage.STORAGE_VOLUMES:
        vol = str(v)
        rows = (db.query(SmartSnapshot)
                .filter(SmartSnapshot.volume_path == vol, SmartSnapshot.sampled_at >= since)
                .order_by(SmartSnapshot.sampled_at.asc(), SmartSnapshot.id.asc())
                .all())
        last = rows[-1] if rows else (
            db.query(SmartSnapshot).filter(SmartSnapshot.volume_path == vol)
            .order_by(SmartSnapshot.sampled_at.desc()).first())
        entry = {"volume_path": vol, "error": _last_errors.get(vol), "latest": None,
                 "health": "unknown",
                 "temps": [{"at": r.sampled_at.isoformat(), "c": r.temp_c}
                           for r in rows if r.temp_c is not None]}
        if last:
            entry["latest"] = {
                "device": last.device, "model": last.model,
                "sampled_at": last.sampled_at.isoformat(),
                "passed": last.passed, "temp_c": last.temp_c,
                "power_on_hours": last.power_on_hours,
                "reallocated": last.reallocated, "pending": last.pending,
                "uncorrectable": last.uncorrectable,
            }
            entry["health"] = health(last)
        out.append(entry)
    return out
