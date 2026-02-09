import asyncio
import tempfile
from contextlib import asynccontextmanager

from raft.core.logic import RaftCore
from raft.core.state import RaftState
from raft.rpc.proto import raft_pb2
from raft.sm import KeyValueStateMachine
from raft.storage import FsLogStorage, FsSnapshotStore, LogEntryRecord


@asynccontextmanager
async def dummy_client_factory(peer: str):
    class Dummy:
        async def AppendEntries(self, req):
            return raft_pb2.AppendEntriesResponse(
                term=req.term, success=True, match_index=req.prev_log_index
            )

    client = Dummy()
    yield client


def test_conflicting_append_entries_overwrites():
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
        state.append_log_entries(
            [LogEntryRecord(index=1, term=1, data=b"a"), LogEntryRecord(index=2, term=1, data=b"b")]
        )
        core = RaftCore(state, lambda peer: dummy_client_factory(peer))
        req = raft_pb2.AppendEntriesRequest(
            term=2,
            leader_id="n2",
            prev_log_index=1,
            prev_log_term=1,
            entries=[raft_pb2.LogEntry(index=2, term=2, data=b"c")],
            leader_commit=0,
        )
        resp = asyncio.run(core.handle_append_entries(req))
        assert resp.success
        entries = state.read_entries(1)
        assert [e.data for e in entries] == [b"a", b"c"]
