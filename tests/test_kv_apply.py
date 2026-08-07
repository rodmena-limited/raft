from __future__ import annotations

import pickle

import pytest

from raft.sm import KeyValueStateMachine


def test_kv_put_get():
    sm = KeyValueStateMachine()
    sm.apply(pickle.dumps(("put", "a", b"1")))
    out = sm.apply(pickle.dumps(("get", "a")))
    assert out == b"1"


def test_kv_get_missing_returns_none():
    sm = KeyValueStateMachine()
    assert sm.apply(pickle.dumps(("get", "nope"))) is None


def test_kv_put_overwrites():
    sm = KeyValueStateMachine()
    sm.apply(pickle.dumps(("put", "a", b"1")))
    sm.apply(pickle.dumps(("put", "a", b"2")))
    assert sm.apply(pickle.dumps(("get", "a"))) == b"2"


def test_kv_unknown_op_raises():
    sm = KeyValueStateMachine()
    with pytest.raises(ValueError):
        sm.apply(pickle.dumps(("delete", "a")))


def test_kv_snapshot_restore_roundtrip():
    sm = KeyValueStateMachine()
    sm.apply(pickle.dumps(("put", "a", b"1")))
    sm.apply(pickle.dumps(("put", "b", b"2")))
    data = sm.snapshot()
    fresh = KeyValueStateMachine()
    fresh.restore(data)
    assert fresh.store == {"a": b"1", "b": b"2"}
    assert fresh.apply(pickle.dumps(("get", "a"))) == b"1"


def test_kv_snapshot_is_deterministic():
    sm = KeyValueStateMachine()
    sm.apply(pickle.dumps(("put", "a", b"1")))
    assert sm.snapshot() == sm.snapshot()


def test_kv_put_value_bytes_types():
    sm = KeyValueStateMachine()
    sm.apply(pickle.dumps(("put", "k", b"\x00\x01\xff")))
    assert sm.store["k"] == b"\x00\x01\xff"
