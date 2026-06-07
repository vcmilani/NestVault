import time as _time

_activity_cache: dict = {"data": None, "dirty": True, "ts": 0.0}
_ACTIVITY_TTL_ACTIVE = 2.0   # segundos — quando há backup/job rodando
_ACTIVITY_TTL_IDLE   = 30.0  # segundos — quando nada está rodando


def invalidate_activity() -> None:
    _activity_cache["dirty"] = True
