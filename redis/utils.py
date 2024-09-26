import time


def get_current_timestamp() -> float:
    return time.time_ns() / 1e6
