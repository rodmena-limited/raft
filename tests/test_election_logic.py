from __future__ import annotations

from helpers import FakeStub, make_client_factory, make_state

from raft.core.logic import RaftCore
from raft.core.state import Role
from raft.util import monotonic_ms


async def test_no_election_before_deadline():
    state, _ = make_state(election_min_ms=60000, election_jitter_ms=0)
    core = RaftCore(state, make_client_factory(FakeStub(vote_granted=True)))
    await core.maybe_start_election()
    assert state.role == Role.FOLLOWER
    assert state.current_term == 0


async def test_wins_election_with_majority():
    state, _ = make_state(node_id="n1", peers=["n2", "n3"], election_min_ms=0, election_jitter_ms=0)
    state.reset_election_deadline()  # force deadline to now

    state.election_deadline_ms = monotonic_ms() - 1
    core = RaftCore(state, make_client_factory(FakeStub(vote_granted=True)))
    await core.maybe_start_election()
    assert state.role == Role.LEADER
    assert state.current_term == 1
    assert state.voted_for == "n1"


async def test_loses_election_without_majority():
    state, _ = make_state(node_id="n1", peers=["n2", "n3"], election_min_ms=0, election_jitter_ms=0)
    state.election_deadline_ms = monotonic_ms() - 1
    core = RaftCore(state, make_client_factory(FakeStub(vote_granted=False)))
    await core.maybe_start_election()
    assert state.role == Role.CANDIDATE
    assert state.current_term == 1


async def test_leader_does_not_start_new_election():
    state, _ = make_state(node_id="n1", peers=["n2"], election_min_ms=0, election_jitter_ms=0)
    state.election_deadline_ms = monotonic_ms() - 1
    state.role = Role.LEADER
    state.current_term = 3
    core = RaftCore(state, make_client_factory(FakeStub(vote_granted=True)))
    await core.maybe_start_election()
    assert state.role == Role.LEADER
    assert state.current_term == 3


async def test_steps_down_when_peer_reports_higher_term():
    state, _ = make_state(node_id="n1", peers=["n2"], election_min_ms=0, election_jitter_ms=0)
    state.election_deadline_ms = monotonic_ms() - 1
    core = RaftCore(state, make_client_factory(FakeStub(vote_granted=True, vote_term=9)))
    await core.maybe_start_election()
    assert state.role == Role.FOLLOWER
    assert state.current_term == 9


async def test_does_not_become_leader_after_stepping_down_mid_election():
    """A candidate that learned of a higher term mid-gather must not count
    votes collected before the step-down and become leader."""
    state, _ = make_state(node_id="n1", peers=["n2", "n3"], election_min_ms=0, election_jitter_ms=0)
    state.election_deadline_ms = monotonic_ms() - 1
    stubs = {
        "n2": FakeStub(vote_granted=True),  # grants for the old term
        "n3": FakeStub(vote_granted=True, vote_term=7),  # forces step-down
    }
    core = RaftCore(state, make_client_factory(stubs))
    await core.maybe_start_election()
    assert state.role == Role.FOLLOWER
    assert state.current_term == 7
    assert state.role != Role.LEADER


async def test_rpc_failure_counts_as_denied_vote():
    state, _ = make_state(node_id="n1", peers=["n2", "n3"], election_min_ms=0, election_jitter_ms=0)
    state.election_deadline_ms = monotonic_ms() - 1
    core = RaftCore(state, make_client_factory(FakeStub(fail_rpc=True)))
    await core.maybe_start_election()
    assert state.role == Role.CANDIDATE


async def test_election_candidate_votes_for_self_and_persists():
    state, _ = make_state(node_id="n1", peers=["n2"], election_min_ms=0, election_jitter_ms=0)
    state.election_deadline_ms = monotonic_ms() - 1
    core = RaftCore(state, make_client_factory(FakeStub(vote_granted=False)))
    await core.maybe_start_election()
    assert state.voted_for == "n1"
    assert state.storage.load_metadata().voted_for == "n1"
