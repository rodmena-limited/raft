from __future__ import annotations

import tempfile
from contextlib import asynccontextmanager

from raft.core.state import RaftState
from raft.sm import KeyValueStateMachine
from raft.storage import (
    FsLogStorage,
    FsSnapshotStore,
    LogEntryRecord,
    SnapshotMetadata,
    SnapshotStore,
)


def kv_sm(state: RaftState) -> KeyValueStateMachine:
    assert isinstance(state.sm, KeyValueStateMachine)
    return state.sm


def load_snapshot(store: SnapshotStore) -> tuple[SnapshotMetadata, bytes]:
    snap = store.load_snapshot()
    assert snap is not None
    return snap


def make_state(
    *,
    node_id: str = "n1",
    peers: list[str] | None = None,
    election_min_ms: int = 0,
    election_jitter_ms: int = 0,
    heartbeat_ms: int = 10,
    entries: list[LogEntryRecord] | None = None,
    term: int = 0,
    base_dir: str | None = None,
) -> tuple[RaftState, str]:
    peers = peers if peers is not None else []
    d = base_dir or tempfile.mkdtemp()
    storage = FsLogStorage(d)
    if entries:
        storage.append_entries(entries)
    state = RaftState(
        node_id=node_id,
        peers=peers,
        storage=storage,
        snapshots=FsSnapshotStore(d),
        state_machine=KeyValueStateMachine(),
        election_min_ms=election_min_ms,
        election_jitter_ms=election_jitter_ms,
        heartbeat_ms=heartbeat_ms,
    )
    if term:
        state.current_term = term
        state.persist_metadata()
    return state, d


class FakeStub:
    """Configurable fake gRPC stub used by the dummy client factories."""

    def __init__(
        self,
        *,
        vote_granted: bool = True,
        vote_term: int = 0,
        ae_success: bool = True,
        ae_match_index: int | None = None,
        ae_term: int = 0,
        snap_accepted: bool = True,
        snap_term: int = 0,
        fail_rpc: bool = False,
    ):
        self.vote_granted = vote_granted
        self.vote_term = vote_term
        self.ae_success = ae_success
        self.ae_match_index = ae_match_index
        self.ae_term = ae_term
        self.snap_accepted = snap_accepted
        self.snap_term = snap_term
        self.fail_rpc = fail_rpc
        self.append_calls: list = []
        self.vote_calls: list = []
        self.snap_calls: list = []

    async def RequestVote(self, req):
        self.vote_calls.append(req)
        if self.fail_rpc:
            raise ConnectionError("rpc failed")
        from raft.rpc.proto import raft_pb2

        return raft_pb2.RequestVoteResponse(term=self.vote_term, vote_granted=self.vote_granted)

    async def AppendEntries(self, req):
        self.append_calls.append(req)
        if self.fail_rpc:
            raise ConnectionError("rpc failed")
        from raft.rpc.proto import raft_pb2

        match = self.ae_match_index
        if match is None and self.ae_success:
            match = req.entries[-1].index if req.entries else req.prev_log_index
        return raft_pb2.AppendEntriesResponse(
            term=self.ae_term, success=self.ae_success, match_index=match if match else 0
        )

    async def InstallSnapshot(self, req):
        self.snap_calls.append(req)
        if self.fail_rpc:
            raise ConnectionError("rpc failed")
        from raft.rpc.proto import raft_pb2

        return raft_pb2.InstallSnapshotResponse(term=self.snap_term, accepted=self.snap_accepted)


def make_client_factory(stubs):
    """stubs: a single FakeStub used for every peer, or dict[peer, FakeStub]."""

    @asynccontextmanager
    async def factory(peer: str):
        if isinstance(stubs, dict):
            yield stubs[peer]
        else:
            yield stubs

    return factory


def kv_put(key: str, value: bytes) -> bytes:
    import pickle

    return pickle.dumps(("put", key, value))


def kv_get(key: str) -> bytes:
    import pickle

    return pickle.dumps(("get", key))


def kv_entry(index: int, term: int, key: str | None = None) -> LogEntryRecord:
    """A log entry whose payload is a valid KV put op, so it can be committed
    and applied by the KeyValueStateMachine."""
    key = key or f"k{index}"
    return LogEntryRecord(index=index, term=term, data=kv_put(key, b"v"))
