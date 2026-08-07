from __future__ import annotations

import pickle

from raft.storage import StateMachine


class KeyValueStateMachine(StateMachine):
    def __init__(self):
        self.store: dict[str, bytes] = {}

    def apply(self, data: bytes) -> bytes | None:
        op = pickle.loads(data)
        if op[0] == "put":
            _, key, value = op
            self.store[key] = value
            return None
        if op[0] == "get":
            _, key = op
            return self.store.get(key)
        raise ValueError("unknown op")

    def snapshot(self) -> bytes:
        return pickle.dumps(self.store)

    def restore(self, data: bytes) -> None:
        self.store = pickle.loads(data)
