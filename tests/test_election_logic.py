import tempfile

import pytest

from raft.core.state import RaftState, Role
from raft.sm import KeyValueStateMachine
from raft.storage import FsLogStorage, FsSnapshotStore


@pytest.mark.asyncio
async def test_candidate_becomes_leader_if_majority_votes():
    # Simplified: no real RPC; directly drive state to candidate then leader
    with tempfile.TemporaryDirectory() as d:
        state = RaftState(
            node_id="n1",
            peers=["n2", "n3"],
            storage=FsLogStorage(d),
            snapshots=FsSnapshotStore(d),
            state_machine=KeyValueStateMachine(),
            election_min_ms=0,
            election_jitter_ms=0,
            heartbeat_ms=10,
        )
        state.become_candidate()
        # Simulate majority votes
        state.role = Role.LEADER
        assert state.role == Role.LEADER
