import random
import time
from collections.abc import Callable


def monotonic_ms() -> int:
    return int(time.monotonic() * 1000)


def randomized_timeout_ms(base: int, jitter: int) -> int:
    return base + random.randint(0, jitter)


def sleep_ms(ms: int, *, sleep_fn: Callable[[float], None] = time.sleep) -> None:
    sleep_fn(ms / 1000.0)
