from __future__ import annotations

import asyncio

from helpers import FakeStub, kv_entry, kv_sm, make_client_factory, make_state

from raft.core.logic import RaftCore
from raft.core.state import Role
from raft.storage import LogEntryRecord, SnapshotMetadata


async def test_leader_replicates_entries_and_advances_progress():
    state, _ = make_state(
        node_id="n1",
        peers=["n2"],
        term=2,
        entries=[LogEntryRecord(1, 2, b"a"), LogEntryRecord(2, 2, b"b")],
    )
    state.role = Role.LEADER
    state.become_leader()  # resets progress.next_index to 3
    stub = FakeStub(ae_success=True)
    core = RaftCore(state, make_client_factory(stub))
    await core.broadcast_append_entries()
    assert state.progress["n2"].match_index == 2
    assert state.progress["n2"].next_index == 3
    assert stub.append_calls[0].prev_log_index == 2
    assert stub.append_calls[0].prev_log_term == 2
    assert [e.index for e in stub.append_calls[0].entries] == []


async def test_leader_sends_pending_entries_to_follower():
    state, _ = make_state(
        node_id="n1",
        peers=["n2"],
        term=2,
        entries=[LogEntryRecord(1, 2, b"a"), LogEntryRecord(2, 2, b"b")],
    )
    state.role = Role.LEADER
    state.progress["n2"].next_index = 2
    stub = FakeStub(ae_success=True)
    core = RaftCore(state, make_client_factory(stub))
    await core.broadcast_append_entries()
    sent = stub.append_calls[0]
    assert sent.prev_log_index == 1
    assert sent.prev_log_term == 2
    assert [e.index for e in sent.entries] == [2]


async def test_follower_rejection_backtracks_next_index():
    state, _ = make_state(
        node_id="n1",
        peers=["n2"],
        term=2,
        entries=[LogEntryRecord(1, 2, b"a"), LogEntryRecord(2, 2, b"b")],
    )
    state.role = Role.LEADER
    state.progress["n2"].next_index = 3
    stub = FakeStub(ae_success=False, ae_match_index=1)
    core = RaftCore(state, make_client_factory(stub))
    await core.broadcast_append_entries()
    assert state.progress["n2"].next_index == 2
    assert state.progress["n2"].match_index == 0


async def test_higher_term_in_response_steps_down():
    state, _ = make_state(node_id="n1", peers=["n2"], term=2)
    state.role = Role.LEADER
    state.current_term = 2
    stub = FakeStub(ae_success=False, ae_term=5)
    core = RaftCore(state, make_client_factory(stub))
    await core.broadcast_append_entries()
    assert state.role == Role.FOLLOWER
    assert state.current_term == 5


async def test_commit_advances_when_majority_replicated():
    state, _ = make_state(
        node_id="n1",
        peers=["n2", "n3"],
        term=2,
        entries=[kv_entry(1, 2, "k"), kv_entry(2, 2), kv_entry(3, 2)],
    )
    state.role = Role.LEADER
    state.become_leader()
    state.progress["n2"].next_index = 4
    state.progress["n3"].next_index = 4
    core = RaftCore(state, make_client_factory(FakeStub(ae_success=True)))
    await core.broadcast_append_entries()
    assert state.commit_index == 3
    assert state.last_applied == 3
    assert kv_sm(state).store["k"] == b"v"


async def test_snapshot_sent_to_lagging_follower():
    state, _ = make_state(node_id="n1", peers=["n2"], term=2)
    state.role = Role.LEADER
    state.storage.compact_prefix(5, 2)
    state.snapshots.store_snapshot(SnapshotMetadata(5, 2), b"snapdata")
    state.progress["n2"].next_index = 3  # follower behind the compaction base
    stub = FakeStub(snap_accepted=True)
    core = RaftCore(state, make_client_factory(stub))
    await core.broadcast_append_entries()
    assert len(stub.snap_calls) == 1
    assert stub.snap_calls[0].last_included_index == 5
    assert stub.snap_calls[0].data == b"snapdata"
    assert state.progress["n2"].match_index == 5
    assert state.progress["n2"].next_index == 6


async def test_snapshot_skipped_when_no_snapshot_available():
    state, _ = make_state(node_id="n1", peers=["n2"], term=2)
    state.role = Role.LEADER
    state.storage.compact_prefix(5, 2)  # compacted but no snapshot blob
    state.progress["n2"].next_index = 3
    stub = FakeStub()
    core = RaftCore(state, make_client_factory(stub))
    await core.broadcast_append_entries()
    assert stub.snap_calls == []
    assert state.progress["n2"].next_index == 6  # moved to first log index


async def test_normal_ae_when_next_index_within_log():
    state, _ = make_state(
        node_id="n1",
        peers=["n2"],
        term=2,
        entries=[LogEntryRecord(3, 2, b"c"), LogEntryRecord(4, 2, b"d")],
    )
    state.role = Role.LEADER
    state.storage.compact_prefix(2, 1)
    state.progress["n2"].next_index = 3
    stub = FakeStub(ae_success=True)
    core = RaftCore(state, make_client_factory(stub))
    await core.broadcast_append_entries()
    sent = stub.append_calls[0]
    assert sent.prev_log_index == 2
    assert sent.prev_log_term == 1  # compaction base term
    assert [e.index for e in sent.entries] == [3, 4]


async def test_rpc_failure_leaves_progress_unchanged():
    state, _ = make_state(node_id="n1", peers=["n2"], term=2, entries=[LogEntryRecord(1, 2, b"a")])
    state.role = Role.LEADER
    state.become_leader()
    core = RaftCore(state, make_client_factory(FakeStub(fail_rpc=True)))
    await core.broadcast_append_entries()
    assert state.progress["n2"].next_index == 2
    assert state.progress["n2"].match_index == 0


async def test_snapshot_rpc_failure_keeps_progress():
    state, _ = make_state(node_id="n1", peers=["n2"], term=2)
    state.role = Role.LEADER
    state.storage.compact_prefix(5, 2)
    state.snapshots.store_snapshot(SnapshotMetadata(5, 2), b"snapdata")
    state.progress["n2"].next_index = 3
    core = RaftCore(state, make_client_factory(FakeStub(fail_rpc=True)))
    await core.broadcast_append_entries()
    assert state.progress["n2"].next_index == 3


async def test_start_heartbeat_loop_restarts_existing_task():
    state, _ = make_state(node_id="n1", peers=["n2"], term=2)
    state.role = Role.LEADER
    core = RaftCore(state, make_client_factory(FakeStub(ae_success=True)))
    await core.start_heartbeat_loop()
    first = core.heartbeat_task
    assert first is not None and not first.done()
    await core.start_heartbeat_loop()
    second = core.heartbeat_task
    assert second is not None and second is not first
    await asyncio.sleep(0.05)
    assert second is not None and not second.done()
    state.role = Role.FOLLOWER
    await asyncio.sleep(0.05)
    assert second is not None and second.done()
