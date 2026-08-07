from __future__ import annotations

from helpers import kv_entry, make_client_factory, make_state

from raft.core.logic import RaftCore
from raft.core.state import Role


def _leader_state():
    """3-node cluster: n1 (leader, term 2), peers n2/n3."""
    state, _ = make_state(
        node_id="n1",
        peers=["n2", "n3"],
        term=2,
        entries=[kv_entry(1, 1), kv_entry(2, 2), kv_entry(3, 2)],
    )
    state.role = Role.LEADER
    return state


async def test_does_not_commit_previous_term_entry_at_majority():
    """Figure 8 regression: an entry from a previous term replicated to a
    majority must NOT be committed even when the leader's last entry is from
    the current term. Committing it would let a later leader overwrite it."""
    state = _leader_state()
    # followers have replicated up to index 1 (term 1); the leader's own
    # current-term entry at index 3 makes majority_match land on index 1.
    state.progress["n2"].match_index = 1
    state.progress["n3"].match_index = 1
    core = RaftCore(state, make_client_factory(None))
    await core.maybe_advance_commit_index()
    assert state.commit_index == 0


async def test_commits_current_term_entry_when_on_majority():
    state = _leader_state()
    # majority (n2 + self) has the current-term entry at index 3
    state.progress["n2"].match_index = 3
    state.progress["n3"].match_index = 0
    core = RaftCore(state, make_client_factory(None))
    await core.maybe_advance_commit_index()
    assert state.commit_index == 3
    assert state.last_applied == 3


async def test_commits_up_to_median_majority_match():
    state = _leader_state()
    state.progress["n2"].match_index = 2
    state.progress["n3"].match_index = 3
    core = RaftCore(state, make_client_factory(None))
    await core.maybe_advance_commit_index()
    # sorted match indexes [2,3,3]; median index 1 -> 3, whose term is current
    assert state.commit_index == 3


async def test_no_commit_when_majority_match_is_zero():
    state = _leader_state()
    core = RaftCore(state, make_client_factory(None))
    await core.maybe_advance_commit_index()
    assert state.commit_index == 0


async def test_no_commit_when_entry_at_majority_missing_from_log():
    state = _leader_state()
    state.progress["n2"].match_index = 10
    state.progress["n3"].match_index = 10
    core = RaftCore(state, make_client_factory(None))
    await core.maybe_advance_commit_index()
    # majority_match=10 but the log only reaches index 3 -> conservative no-op
    assert state.commit_index == 0


async def test_commit_after_newer_current_term_entry_replicates():
    """Once a current-term entry is committed at the tail, earlier entries are
    committed transitively because commit_index jumps over them."""
    state = _leader_state()
    state.progress["n2"].match_index = 2
    state.progress["n3"].match_index = 1
    core = RaftCore(state, make_client_factory(None))
    await core.maybe_advance_commit_index()
    assert state.commit_index == 2
