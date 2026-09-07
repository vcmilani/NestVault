import threading

_activity_wake = threading.Event()

# Contador incrementado a cada invalidação. Serve para caches que precisam
# reagir a escritas mas não podem importar quem invalida (main.py importa
# cache_state, não o contrário) — em vez de um callback registrado, o dono do
# cache guarda a geração junto do valor e descarta quando ela muda.
_activity_gen = 0
_gen_lock = threading.Lock()


def invalidate_activity() -> None:
    global _activity_gen
    with _gen_lock:
        _activity_gen += 1
    _activity_wake.set()


def activity_generation() -> int:
    return _activity_gen
