"""Métricas de sistema (CPU, memória, temperatura, load, uptime) via /proc — sem
dependências externas. O servidor roda em Linux (bare-metal systemd em Raspberry
Pi / PCs antigos), então /proc e /sys/class/thermal estão sempre disponíveis;
qualquer falha de leitura é isolada e degrada para None em vez de derrubar
/api/activity."""
import os
import time
from collections import deque
from glob import glob

SAMPLE_INTERVAL = 5.0
HISTORY_POINTS = 60  # ~5 min de histórico a 5s/amostra

_prev_cpu: list[tuple[int, int]] | None = None
_latest: dict | None = None
_history: deque = deque(maxlen=HISTORY_POINTS)
_temp_zone_path: str | None = None
_temp_zone_resolved = False


def parse_cpu_stat(text: str) -> list[tuple[int, int]]:
    """Extrai (idle, total) de cada linha 'cpu*' de /proc/stat.
    Índice 0 = agregado, demais = por núcleo, na ordem do arquivo."""
    out = []
    for line in text.splitlines():
        if not line.startswith("cpu"):
            continue
        parts = line.split()
        label = parts[0]
        if label == "cpu" or (label[3:].isdigit()):
            fields = [int(x) for x in parts[1:]]
            if len(fields) < 5:
                continue
            idle = fields[3] + fields[4]  # idle + iowait
            total = sum(fields)
            out.append((idle, total))
    return out


def parse_meminfo(text: str) -> dict:
    """Extrai campos relevantes de /proc/meminfo, convertidos de kB para bytes."""
    vals: dict[str, int] = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        key = parts[0].rstrip(":")
        if key in ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree"):
            try:
                vals[key] = int(parts[1]) * 1024
            except ValueError:
                pass
    return vals


def _read_cpu_stat() -> list[tuple[int, int]] | None:
    try:
        with open("/proc/stat") as f:
            return parse_cpu_stat(f.read())
    except OSError:
        return None


def _read_meminfo() -> dict | None:
    try:
        with open("/proc/meminfo") as f:
            return parse_meminfo(f.read())
    except OSError:
        return None


def _read_uptime() -> float | None:
    try:
        with open("/proc/uptime") as f:
            return float(f.read().split()[0])
    except (OSError, ValueError, IndexError):
        return None


def _read_loadavg() -> tuple[float, float, float] | None:
    try:
        return os.getloadavg()
    except OSError:
        return None


def _resolve_temp_zone() -> str | None:
    global _temp_zone_path, _temp_zone_resolved
    if _temp_zone_resolved:
        return _temp_zone_path
    _temp_zone_resolved = True
    candidates = sorted(glob("/sys/class/thermal/thermal_zone*"))
    for zone in candidates:
        try:
            with open(f"{zone}/type") as f:
                ztype = f.read().strip().lower()
        except OSError:
            continue
        if any(k in ztype for k in ("cpu", "soc", "x86_pkg_temp")):
            _temp_zone_path = zone
            return zone
    _temp_zone_path = candidates[0] if candidates else None
    return _temp_zone_path


def _read_temp_c() -> float | None:
    zone = _resolve_temp_zone()
    if not zone:
        return None
    try:
        with open(f"{zone}/temp") as f:
            return int(f.read().strip()) / 1000.0
    except (OSError, ValueError):
        return None


def sample() -> None:
    """Lê /proc e /sys, calcula % de CPU pelo delta contra a amostra anterior,
    e atualiza _latest/_history. Chamado a cada SAMPLE_INTERVAL por uma task
    asyncio; sem custo de I/O bloqueante relevante (leitura de arquivos do
    kernel, sub-milissegundo)."""
    global _prev_cpu, _latest

    cpu_now = _read_cpu_stat()
    meminfo = _read_meminfo()
    uptime = _read_uptime()
    load = _read_loadavg()
    temp_c = _read_temp_c()

    cpu_pct = None
    cpu_per_core: list[float] = []
    if cpu_now and _prev_cpu and len(cpu_now) == len(_prev_cpu):
        pcts = []
        for (idle_now, total_now), (idle_prev, total_prev) in zip(cpu_now, _prev_cpu):
            d_total = total_now - total_prev
            d_idle = idle_now - idle_prev
            pcts.append(0.0 if d_total <= 0 else max(0.0, min(100.0, 100.0 * (1 - d_idle / d_total))))
        cpu_pct = pcts[0]
        cpu_per_core = pcts[1:]
    if cpu_now:
        _prev_cpu = cpu_now

    mem_pct = None
    mem_total = mem_used = swap_total = swap_used = None
    if meminfo and "MemTotal" in meminfo and "MemAvailable" in meminfo:
        mem_total = meminfo["MemTotal"]
        mem_used = max(0, mem_total - meminfo["MemAvailable"])
        mem_pct = 0.0 if mem_total <= 0 else 100.0 * mem_used / mem_total
        swap_total = meminfo.get("SwapTotal")
        if swap_total is not None:
            swap_used = max(0, swap_total - meminfo.get("SwapFree", swap_total))

    _latest = {
        "cpu_pct": cpu_pct,
        "cpu_per_core": cpu_per_core,
        "cpu_count": (len(cpu_now) - 1) if cpu_now else 0,
        "load_1": load[0] if load else None,
        "load_5": load[1] if load else None,
        "load_15": load[2] if load else None,
        "mem_total_bytes": mem_total,
        "mem_used_bytes": mem_used,
        "mem_pct": mem_pct,
        "swap_total_bytes": swap_total,
        "swap_used_bytes": swap_used,
        "swap_pct": (100.0 * swap_used / swap_total) if swap_total else 0.0,
        "temp_c": temp_c,
        "uptime_seconds": int(uptime) if uptime is not None else None,
    }
    if cpu_pct is not None and mem_pct is not None:
        _history.append({"t": int(time.time() * 1000), "cpu": round(cpu_pct, 1), "mem": round(mem_pct, 1)})


def snapshot() -> dict | None:
    """Devolve a última amostra + histórico, sem I/O. None se ainda não houve
    amostra válida (cold start) ou se o host não expõe /proc."""
    if _latest is None:
        return None
    return {**_latest, "history": list(_history)}


def reset() -> None:
    """Zera estado do módulo — usado no boot do lifespan e entre testes."""
    global _prev_cpu, _latest, _temp_zone_path, _temp_zone_resolved
    _prev_cpu = None
    _latest = None
    _history.clear()
    _temp_zone_path = None
    _temp_zone_resolved = False
