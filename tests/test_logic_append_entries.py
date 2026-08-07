from __future__ import annotations

from helpers import kv_entry, kv_put, kv_sm, make_client_factory, make_state

from raft.core.logic import RaftCore
from raft.core.state import Role
from raft.rpc.proto import raft_pb2
from raft.storage import LogEntryRecord


def _ae(
    term=1,
    leader="n2",
    prev_log_index=0,
    prev_log_term=0,
    entries=None,
    leader_commit=0,
):
    return raft_pb2.AppendEntriesRequest(
        term=term,
        leader_id=leader,
        prev_log_index=prev_log_index,
        prev_log_term=prev_log_term,
        entries=entries or [],
        leader_commit=leader_commit,
    )


def _e(index, term, data=b"x"):
    return raft_pb2.LogEntry(index=index, term=term, data=data)


async def test_rejects_lower_term():
    state, _ = make_state(term=5)
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_append_entries(_ae(term=4))
    assert not resp.success
    assert resp.term == 5


async def test_accepts_heartbeat():
    state, _ = make_state(term=1, entries=[LogEntryRecord(1, 1, b"a")])
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_append_entries(_ae(term=1, prev_log_index=1, prev_log_term=1))
    assert resp.success
    assert resp.match_index == 1


async def test_resets_election_deadline_on_append():
    state, _ = make_state(term=1, election_min_ms=10000, election_jitter_ms=0)
    deadline_before = state.election_deadline_ms
    core = RaftCore(state, make_client_factory(None))
    await core.handle_append_entries(_ae(term=1))
    assert state.election_deadline_ms >= deadline_before


async def test_higher_term_steps_down_to_follower():
    state, _ = make_state(term=1, peers=["n2"])
    state.role = Role.LEADER
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_append_entries(_ae(term=2, leader="n2"))
    assert resp.success
    assert state.role == Role.FOLLOWER
    assert state.current_term == 2


async def test_rejects_when_prev_index_beyond_log():
    state, _ = make_state(term=1, entries=[LogEntryRecord(1, 1, b"a")])
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_append_entries(_ae(term=1, prev_log_index=5, prev_log_term=1))
    assert not resp.success
    assert resp.match_index == 1


async def test_rejects_when_prev_term_mismatch():
    state, _ = make_state(term=1, entries=[LogEntryRecord(1, 1, b"a"), LogEntryRecord(2, 1, b"b")])
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_append_entries(_ae(term=1, prev_log_index=2, prev_log_term=9))
    assert not resp.success
    assert resp.match_index == 1


async def test_conflicting_entries_overwritten():
    state, _ = make_state(
        term=2,
        entries=[
            LogEntryRecord(1, 1, b"a"),
            LogEntryRecord(2, 1, b"b"),
            LogEntryRecord(3, 1, b"c"),
        ],
    )
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_append_entries(
        _ae(term=2, prev_log_index=1, prev_log_term=1, entries=[_e(2, 2, b"B"), _e(3, 2, b"C")])
    )
    assert resp.success
    assert [(e.index, e.term, e.data) for e in state.read_entries(1)] == [
        (1, 1, b"a"),
        (2, 2, b"B"),
        (3, 2, b"C"),
    ]


async def test_appends_new_entries_after_matching_prev():
    state, _ = make_state(term=1, entries=[LogEntryRecord(1, 1, b"a")])
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_append_entries(
        _ae(term=1, prev_log_index=1, prev_log_term=1, entries=[_e(2, 1, b"b"), _e(3, 1, b"c")])
    )
    assert resp.success
    assert resp.match_index == 3
    assert [e.data for e in state.read_entries(1)] == [b"a", b"b", b"c"]


async def test_leader_commit_advances_commit_and_applies():
    state, _ = make_state(term=1, entries=[LogEntryRecord(1, 1, kv_put("k", b"v"))])
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_append_entries(_ae(term=1, prev_log_index=0, leader_commit=1))
    assert resp.success
    assert state.commit_index == 1
    assert state.last_applied == 1
    assert kv_sm(state).store == {"k": b"v"}


async def test_leader_commit_capped_at_log_length():
    state, _ = make_state(term=1, entries=[kv_entry(1, 1)])
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_append_entries(
        _ae(term=1, prev_log_index=1, prev_log_term=1, leader_commit=99)
    )
    assert resp.success
    assert state.commit_index == 1


async def test_leader_commit_does_not_move_backwards():
    state, _ = make_state(term=1, entries=[LogEntryRecord(1, 1, b"a"), LogEntryRecord(2, 1, b"b")])
    state.commit_index = 2
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_append_entries(
        _ae(term=1, prev_log_index=2, prev_log_term=1, leader_commit=1)
    )
    assert resp.success
    assert state.commit_index == 2


async def test_prev_matching_compaction_base_is_accepted():
    state, _ = make_state(
        term=1,
        entries=[
            LogEntryRecord(1, 1, b"a"),
            LogEntryRecord(2, 1, b"b"),
            LogEntryRecord(3, 1, b"c"),
        ],
    )
    state.storage.compact_prefix(2, 1)
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_append_entries(
        _ae(term=1, prev_log_index=2, prev_log_term=1, entries=[_e(3, 1, b"c"), _e(4, 1, b"d")])
    )
    assert resp.success
    assert [e.data for e in state.read_entries(1)] == [b"c", b"d"]
    assert state.storage.compaction_base() == (2, 1)


async def test_prev_below_compaction_base_is_rejected():
    state, _ = make_state(
        term=1,
        entries=[
            LogEntryRecord(1, 1, b"a"),
            LogEntryRecord(2, 1, b"b"),
            LogEntryRecord(3, 1, b"c"),
        ],
    )
    state.storage.compact_prefix(2, 1)
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_append_entries(_ae(term=1, prev_log_index=1, prev_log_term=1))
    assert not resp.success


async def test_returns_match_index_of_log_after_append():
    state, _ = make_state(term=1, entries=[LogEntryRecord(1, 1, b"a")])
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_append_entries(
        _ae(term=1, prev_log_index=1, prev_log_term=1, entries=[_e(2, 1, b"b")])
    )
    assert resp.match_index == 2
