import pickle

from raft.sm import KeyValueStateMachine


def test_kv_put_get():
    sm = KeyValueStateMachine()
    sm.apply(pickle.dumps(("put", "a", b"1")))
    out = sm.apply(pickle.dumps(("get", "a")))
    assert out == b"1"
