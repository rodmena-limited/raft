from __future__ import annotations

import pickle

from helpers import kv_put, kv_sm, load_snapshot, make_state

from raft.core.logic import RaftCore
from raft.core.state import Role
from raft.rpc.proto import raft_pb2
from raft.storage import LogEntryRecord

_SNAP_DATA = pickle.dumps({"a": b"1"})


def _snap(term=1, leader="n2", last_included_index=5, last_included_term=2, data=_SNAP_DATA):
    return raft_pb2.InstallSnapshotRequest(
        term=term,
        leader_id=leader,
        last_included_index=last_included_index,
        last_included_term=last_included_term,
        data=data,
    )


async def test_install_snapshot_rejects_lower_term():
    state, _ = make_state(term=5)
    core = RaftCore(state, lambda peer: None)
    resp = await core.handle_install_snapshot(_snap(term=4))
    assert not resp.accepted
    assert resp.term == 5


async def test_install_snapshot_stores_and_restores_sm():
    state, _ = make_state(term=1, entries=[LogEntryRecord(1, 1, kv_put("old", b"x"))])
    state.commit_index = 1
    state.apply_entries()
    core = RaftCore(state, lambda peer: None)
    data = kv_sm(state).snapshot()  # {'old': b'x'}
    resp = await core.handle_install_snapshot(
        _snap(term=1, data=data, last_included_index=3, last_included_term=1)
    )
    assert resp.accepted
    assert state.role == Role.FOLLOWER
    assert kv_sm(state).store == {"old": b"x"}
    assert state.commit_index == 3
    assert state.last_applied == 3


async def test_install_snapshot_compacts_log():
    state, _ = make_state(
        term=1,
        entries=[LogEntryRecord(1, 1, kv_put("a", b"1")), LogEntryRecord(2, 1, kv_put("b", b"2"))],
    )
    core = RaftCore(state, lambda peer: None)
    await core.handle_install_snapshot(
        _snap(term=1, data=pickle.dumps({}), last_included_index=4, last_included_term=2)
    )
    assert state.storage.compaction_base() == (4, 2)
    assert state.read_entries(1) == []
    assert state.last_log_index_term() == (4, 2)


async def test_install_snapshot_persists_metadata():
    state, _ = make_state(term=1)
    core = RaftCore(state, lambda peer: None)
    await core.handle_install_snapshot(
        _snap(term=1, data=pickle.dumps({}), last_included_index=4, last_included_term=2)
    )
    meta = state.storage.load_metadata()
    assert meta.commit_index == 4
    assert meta.last_applied == 4


async def test_install_snapshot_higher_term_steps_down():
    state, _ = make_state(term=1)
    state.role = Role.LEADER
    state.current_term = 2
    core = RaftCore(state, lambda peer: None)
    await core.handle_install_snapshot(_snap(term=3, data=pickle.dumps({})))
    assert state.role == Role.FOLLOWER
    assert state.current_term == 3


async def test_follower_catches_up_via_snapshot_then_entries():
    follower, _ = make_state(
        term=2,
        entries=[LogEntryRecord(1, 1, kv_put("a", b"1")), LogEntryRecord(2, 1, kv_put("b", b"2"))],
    )
    follower_core = RaftCore(follower, lambda peer: None)
    snap_data = pickle.dumps({"a": b"1", "b": b"2"})
    resp = await follower_core.handle_install_snapshot(
        _snap(term=2, data=snap_data, last_included_index=5, last_included_term=2)
    )
    assert resp.accepted
    assert follower.last_log_index_term() == (5, 2)
    # leader continues with entries 6..7
    from raft.rpc.proto import raft_pb2

    ae = raft_pb2.AppendEntriesRequest(
        term=2,
        leader_id="n1",
        prev_log_index=5,
        prev_log_term=2,
        entries=[
            raft_pb2.LogEntry(index=6, term=2, data=kv_put("f", b"v")),
            raft_pb2.LogEntry(index=7, term=2, data=kv_put("g", b"v")),
        ],
        leader_commit=7,
    )
    ae_resp = await follower_core.handle_append_entries(ae)
    assert ae_resp.success
    assert [e.data for e in follower.read_entries(1)] == [kv_put("f", b"v"), kv_put("g", b"v")]
    assert follower.commit_index == 7
    assert kv_sm(follower).store == {"a": b"1", "b": b"2", "f": b"v", "g": b"v"}


async def test_maybe_snapshot_then_install_restores_sm_state():
    leader_state, d = make_state(
        entries=[LogEntryRecord(i, 1, kv_put(f"k{i}", b"v")) for i in range(1, 21)]
    )
    leader_state.commit_index = 20
    leader_state.apply_entries()
    leader_state.maybe_snapshot(10, 5)
    meta, data = load_snapshot(leader_state.snapshots)
    assert meta is not None

    fresh, d2 = make_state(term=1)
    fresh_core = RaftCore(fresh, lambda peer: None)
    resp = await fresh_core.handle_install_snapshot(
        _snap(
            term=1,
            data=data,
            last_included_index=meta.last_included_index,
            last_included_term=meta.last_included_term,
        )
    )
    assert resp.accepted
    assert kv_sm(fresh).store == kv_sm(leader_state).store
    assert kv_sm(fresh).store == {f"k{i}": b"v" for i in range(1, 21)}
