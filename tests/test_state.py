from __future__ import annotations

import pickle

from helpers import kv_put, kv_sm, load_snapshot, make_state

from raft.core.state import RaftState, Role
from raft.storage import FsLogStorage, FsSnapshotStore, LogEntryRecord, SnapshotMetadata, sm_restore


def test_initial_role_is_follower():
    state, _ = make_state()
    assert state.role == Role.FOLLOWER
    assert state.current_term == 0
    assert state.voted_for is None
    assert state.commit_index == 0
    assert state.last_applied == 0


def test_initial_election_deadline_is_in_future():
    state, _ = make_state(election_min_ms=1000, election_jitter_ms=0)
    from raft.util import monotonic_ms

    assert state.election_deadline_ms > monotonic_ms()


def test_become_candidate_increments_term_and_votes_self():
    state, _ = make_state(term=3)
    state.become_candidate()
    assert state.role == Role.CANDIDATE
    assert state.current_term == 4
    assert state.voted_for == state.node_id


def test_become_follower_higher_term_resets_vote_and_persists():
    state, _ = make_state(term=3)
    state.become_candidate()
    state.become_follower(5)
    assert state.role == Role.FOLLOWER
    assert state.current_term == 5
    assert state.voted_for is None
    meta = state.storage.load_metadata()
    assert meta.term == 5
    assert meta.voted_for is None


def test_become_follower_equal_term_keeps_vote():
    state, _ = make_state(term=3)
    state.become_candidate()
    state.become_follower(4)
    assert state.voted_for == state.node_id
    assert state.current_term == 4


def test_become_follower_resets_election_deadline():
    state, _ = make_state(election_min_ms=10000, election_jitter_ms=0)
    deadline_before = state.election_deadline_ms
    state.become_follower(1)
    assert state.election_deadline_ms >= deadline_before


def test_become_leader_sets_peer_progress_from_log():
    state, _ = make_state(
        peers=["n2", "n3"], entries=[LogEntryRecord(1, 1, b"a"), LogEntryRecord(2, 1, b"b")]
    )
    state.become_leader()
    assert state.role == Role.LEADER
    assert state.progress["n2"].next_index == 3
    assert state.progress["n3"].next_index == 3
    assert state.progress["n2"].match_index == 0


def test_become_leader_resets_election_deadline():
    state, _ = make_state(election_min_ms=10000, election_jitter_ms=0)
    deadline_before = state.election_deadline_ms
    state.become_leader()
    assert state.election_deadline_ms >= deadline_before


def test_persist_metadata_roundtrip():
    state, d = make_state(term=2)
    state.voted_for = "n9"
    state.commit_index = 4
    state.last_applied = 3
    state.persist_metadata()
    meta = FsLogStorage(d).load_metadata()
    assert meta.term == 2
    assert meta.voted_for == "n9"
    assert meta.commit_index == 4
    assert meta.last_applied == 3


def test_last_log_index_term():
    state, _ = make_state(entries=[LogEntryRecord(1, 1, b"a"), LogEntryRecord(2, 3, b"b")])
    assert state.last_log_index_term() == (2, 3)
    empty, _ = make_state()
    assert empty.last_log_index_term() == (0, 0)


def test_apply_entries_updates_state_machine_and_last_applied():
    state, _ = make_state(entries=[LogEntryRecord(1, 1, kv_put("k", b"v"))])
    state.commit_index = 1
    state.apply_entries()
    assert state.last_applied == 1
    assert kv_sm(state).store == {"k": b"v"}
    assert state.storage.load_metadata().last_applied == 1


def test_apply_entries_idempotent():
    state, _ = make_state(entries=[LogEntryRecord(1, 1, kv_put("k", b"v"))])
    state.commit_index = 1
    state.apply_entries()
    state.apply_entries()
    assert state.last_applied == 1
    assert kv_sm(state).store == {"k": b"v"}


def test_metadata_loaded_at_startup():
    state, d = make_state(term=5)
    state.voted_for = "n2"
    state.commit_index = 3
    state.last_applied = 2
    state.persist_metadata()
    reopened = RaftState(
        node_id="n1",
        peers=[],
        storage=FsLogStorage(d),
        snapshots=FsSnapshotStore(d),
        state_machine=type("SM", (), {"apply": lambda self, d: None})(),
        election_min_ms=0,
        election_jitter_ms=0,
        heartbeat_ms=10,
    )
    assert reopened.current_term == 5
    assert reopened.voted_for == "n2"
    assert reopened.commit_index == 3
    assert reopened.last_applied == 2


def test_startup_restores_state_machine_from_snapshot():
    from raft.sm import KeyValueStateMachine

    state, d = make_state(
        entries=[LogEntryRecord(1, 1, kv_put("a", b"1")), LogEntryRecord(2, 1, kv_put("b", b"2"))]
    )
    state.commit_index = 2
    state.apply_entries()
    state.snapshots.store_snapshot(SnapshotMetadata(2, 1), kv_sm(state).snapshot())
    reopened_sm = KeyValueStateMachine()
    snap = load_snapshot(FsSnapshotStore(d))
    sm_restore(reopened_sm, snap[1])
    assert reopened_sm.store == {"a": b"1", "b": b"2"}


def test_startup_restores_sm_and_bumps_applied_to_snapshot_base():
    state, d = make_state(
        entries=[LogEntryRecord(1, 1, kv_put("a", b"1")), LogEntryRecord(2, 1, kv_put("b", b"2"))]
    )
    state.commit_index = 2
    state.apply_entries()
    state.snapshots.store_snapshot(SnapshotMetadata(2, 1), kv_sm(state).snapshot())
    state.storage.compact_prefix(2, 1)
    state.last_applied = 0
    state.commit_index = 0
    state.persist_metadata()
    reopened = RaftState(
        node_id="n1",
        peers=[],
        storage=FsLogStorage(d),
        snapshots=FsSnapshotStore(d),
        state_machine=state.sm,
        election_min_ms=0,
        election_jitter_ms=0,
        heartbeat_ms=10,
    )
    assert reopened.last_applied == 2
    assert reopened.commit_index == 2
    assert kv_sm(reopened).store == {"a": b"1", "b": b"2"}


def test_maybe_snapshot_below_threshold_is_noop():
    state, _ = make_state(entries=[LogEntryRecord(1, 1, kv_put("a", b"1"))])
    state.maybe_snapshot(1000, 10)
    assert state.snapshots.load_snapshot() is None
    assert state.storage.compaction_base() == (0, 0)


def test_maybe_snapshot_compacts_and_stores_sm_snapshot():
    state, _ = make_state(
        entries=[LogEntryRecord(i, 1, kv_put(f"k{i}", b"v")) for i in range(1, 21)]
    )
    state.commit_index = 20
    state.apply_entries()
    state.maybe_snapshot(10, 5)
    snap = state.snapshots.load_snapshot()
    assert snap is not None
    meta, data = snap
    assert meta.last_included_index >= 10
    assert state.storage.compaction_base()[0] == meta.last_included_index
    # log only contains entries after the snapshot base
    assert all(e.index > meta.last_included_index for e in state.read_entries(1))
    # snapshot data is the SM's own snapshot (pickled store)
    assert pickle.loads(data) == kv_sm(state).store


def test_maybe_snapshot_does_not_exceed_last_applied():
    state, _ = make_state(
        entries=[LogEntryRecord(i, 1, kv_put(f"k{i}", b"v")) for i in range(1, 21)]
    )
    state.commit_index = 12
    state.apply_entries()
    state.maybe_snapshot(10, 5)
    meta, _ = load_snapshot(state.snapshots)
    assert meta.last_included_index <= 12


def test_maybe_snapshot_does_not_repeat_below_base():
    state, _ = make_state(
        entries=[LogEntryRecord(i, 1, kv_put(f"k{i}", b"v")) for i in range(1, 21)]
    )
    state.commit_index = 20
    state.apply_entries()
    state.maybe_snapshot(10, 5)
    first_meta, _ = load_snapshot(state.snapshots)
    # enough new entries arrive to exceed the threshold again
    state.append_log_entries([LogEntryRecord(i, 1, kv_put(f"k{i}", b"v")) for i in range(21, 41)])
    state.commit_index = 40
    state.apply_entries()
    state.maybe_snapshot(10, 5)
    second_meta, _ = load_snapshot(state.snapshots)
    assert second_meta.last_included_index > first_meta.last_included_index
