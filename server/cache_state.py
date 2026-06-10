import threading

_activity_wake = threading.Event()


def invalidate_activity() -> None:
    _activity_wake.set()
