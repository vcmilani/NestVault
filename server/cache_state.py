import time as _time
import threading

_activity_cache: dict = {"data": None, "dirty": True, "ts": 0.0}
_ACTIVITY_TTL_ACTIVE = 2.0   # segundos — quando há backup/job rodando
_ACTIVITY_TTL_IDLE   = 15.0  # segundos — quando nada está rodando
_activity_wake = threading.Event()

# Flag que força refresh do bloco histórico (recent_versions, recent_jobs, maintenance_jobs)
# quando invalidate_activity() é chamado — evita esperar o TTL de 30s.
_historical_stale: dict = {"v": True}


def invalidate_activity() -> None:
    _activity_cache["dirty"] = True
    _historical_stale["v"] = True
    _activity_wake.set()  # acorda o refresh loop imediatamente
