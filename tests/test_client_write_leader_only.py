from __future__ import annotations

from helpers import FakeStub, kv_put, kv_sm, make_client_factory, make_state

from raft.core.logic import RaftCore
from raft.core.state import Role
from raft.storage import LogEntryRecord


async def test_client_write_rejected_if_not_leader():
    state, _ = make_state(term=2)
    core = RaftCore(state, make_client_factory(None))
    accepted, index, term = await core.client_write(kv_put("k", b"v"))
    assert accepted is False
    assert index == 0
    assert term == 2


async def test_client_write_accepts_when_leader():
    state, _ = make_state(term=1)
    state.role = Role.LEADER
    core = RaftCore(state, make_client_factory(None))
    accepted, idx, term = await core.client_write(kv_put("k", b"v"))
    assert accepted is True
    assert idx == 1
    assert term == state.current_term


async def test_client_write_appends_to_log():
    state, _ = make_state(term=1)
    state.role = Role.LEADER
    core = RaftCore(state, make_client_factory(None))
    await core.client_write(kv_put("k", b"v"))
    assert state.last_log_index_term() == (1, 1)
    assert state.read_entries(1)[0].data == kv_put("k", b"v")


async def test_client_write_replicates_to_peers():
    state, _ = make_state(node_id="n1", peers=["n2"], term=1)
    state.role = Role.LEADER
    state.become_leader()
    stub = FakeStub(ae_success=True)
    core = RaftCore(state, make_client_factory(stub))
    accepted, idx, _ = await core.client_write(kv_put("k", b"v"))
    assert accepted and idx == 1
    assert stub.append_calls and stub.append_calls[-1].entries[-1].index == 1


async def test_single_node_leader_commits_own_write():
    state, _ = make_state(term=1)
    state.role = Role.LEADER
    core = RaftCore(state, make_client_factory(None))
    await core.client_write(kv_put("k", b"v"))
    assert state.commit_index == 1
    assert state.last_applied == 1
    assert kv_sm(state).store == {"k": b"v"}


async def test_client_write_index_advances():
    state, _ = make_state(
        term=1,
        entries=[LogEntryRecord(1, 1, kv_put("a", b"1")), LogEntryRecord(2, 1, kv_put("b", b"2"))],
    )
    state.role = Role.LEADER
    core = RaftCore(state, make_client_factory(None))
    accepted, idx, _ = await core.client_write(kv_put("c", b"3"))
    assert accepted and idx == 3
