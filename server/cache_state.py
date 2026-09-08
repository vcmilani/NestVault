import threading
import time

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


# Marca de atividade real de backup (relógio monotônico). Atualizada a cada
# arquivo recebido/registrado e ao fim de cada versão, tanto pelos uploads HTTP
# quanto pelos jobs rclone. Vive aqui, e não em main.py, porque rclone_runner
# também precisa marcá-la e não pode importar main (ciclo de import).
#
# O gate de ociosidade do SSD cache usa esta marca em vez de
# max(BackupVersion.finished_at): aquele valor é global e só se move quando uma
# versão *termina*, então um backup longo em curso não o atualizava e, do outro
# lado, ele ficava eternamente fresco em servidores com backups frequentes.
_last_backup_activity = 0.0


def mark_backup_activity() -> None:
    global _last_backup_activity
    _last_backup_activity = time.monotonic()


def seconds_since_backup_activity() -> "float | None":
    """Segundos desde a última atividade de backup, ou None se não houve
    nenhuma desde o boot (nesse caso não há o que esperar)."""
    if _last_backup_activity == 0.0:
        return None
    return time.monotonic() - _last_backup_activity
