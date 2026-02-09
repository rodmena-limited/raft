import asyncio
import tempfile
from contextlib import asynccontextmanager

from raft.core.logic import RaftCore
from raft.core.state import RaftState, Role
from raft.sm import KeyValueStateMachine
from raft.storage import FsLogStorage, FsSnapshotStore


@asynccontextmanager
async def dummy_client_factory(peer: str):
    class Dummy:
        async def AppendEntries(self, req):
            return req  # not used

    client = Dummy()
    yield client


def test_client_write_rejected_if_not_leader():
    with tempfile.TemporaryDirectory() as d:
        state = RaftState(
            node_id="n1",
            peers=[],
            storage=FsLogStorage(d),
            snapshots=FsSnapshotStore(d),
            state_machine=KeyValueStateMachine(),
            election_min_ms=0,
            election_jitter_ms=0,
            heartbeat_ms=10,
        )
        core = RaftCore(state, lambda peer: dummy_client_factory(peer))
        accepted, _, _ = asyncio.run(core.client_write(b"cmd"))
        assert accepted is False


def test_client_write_accepts_when_leader():
    with tempfile.TemporaryDirectory() as d:
        state = RaftState(
            node_id="n1",
            peers=[],
            storage=FsLogStorage(d),
            snapshots=FsSnapshotStore(d),
            state_machine=KeyValueStateMachine(),
            election_min_ms=0,
            election_jitter_ms=0,
            heartbeat_ms=10,
        )
        core = RaftCore(state, lambda peer: dummy_client_factory(peer))
        state.role = Role.LEADER
        accepted, idx, term = asyncio.run(core.client_write(b"cmd"))
        assert accepted is True
        assert idx == 1
        assert term == state.current_term
