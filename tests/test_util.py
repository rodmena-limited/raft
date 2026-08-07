from __future__ import annotations

import logging

from raft.util import get_logger, monotonic_ms, randomized_timeout_ms, setup_logging, sleep_ms


def test_randomized_timeout_within_range():
    for _ in range(200):
        v = randomized_timeout_ms(100, 50)
        assert 100 <= v <= 150


def test_randomized_timeout_zero_jitter():
    assert randomized_timeout_ms(42, 0) == 42


def test_randomized_timeout_variety():
    seen = {randomized_timeout_ms(1000, 100) for _ in range(100)}
    assert len(seen) > 1


def test_monotonic_ms_advances():
    a = monotonic_ms()
    import time

    time.sleep(0.01)
    b = monotonic_ms()
    assert b >= a


def test_sleep_ms_uses_supplied_fn():
    calls = []

    def fake(seconds: float) -> None:
        calls.append(seconds)

    sleep_ms(250, sleep_fn=fake)
    assert calls == [0.25]


def test_get_logger_returns_logger():
    logger = get_logger("raft.test")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "raft.test"


def test_setup_logging_is_idempotent():
    setup_logging(logging.DEBUG)
    root = logging.getLogger()
    handlers_before = len(root.handlers)
    setup_logging(logging.INFO)
    assert len(root.handlers) == handlers_before


def test_setup_logging_sets_level():
    setup_logging(logging.WARNING)
    assert logging.getLogger().level == logging.WARNING
