from __future__ import annotations

from helpers import make_client_factory, make_state

from raft.core.logic import RaftCore
from raft.core.state import Role
from raft.rpc.proto import raft_pb2
from raft.storage import LogEntryRecord


def _rv(term=1, candidate="n2", last_log_index=0, last_log_term=0):
    return raft_pb2.RequestVoteRequest(
        term=term,
        candidate_id=candidate,
        last_log_index=last_log_index,
        last_log_term=last_log_term,
    )


async def test_denies_lower_term():
    state, _ = make_state(term=3)
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_request_vote(_rv(term=2))
    assert not resp.vote_granted
    assert resp.term == 3


async def test_higher_term_steps_down_and_grants():
    state, _ = make_state(term=1)
    state.role = Role.CANDIDATE
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_request_vote(_rv(term=2, candidate="n2"))
    assert resp.vote_granted
    assert resp.term == 2
    assert state.role == Role.FOLLOWER
    assert state.current_term == 2
    assert state.voted_for == "n2"


async def test_denies_when_already_voted_for_other():
    state, _ = make_state(term=2)
    state.voted_for = "n9"
    state.persist_metadata()
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_request_vote(_rv(term=2, candidate="n2"))
    assert not resp.vote_granted


async def test_grants_again_for_same_candidate():
    state, _ = make_state(term=2)
    state.voted_for = "n2"
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_request_vote(_rv(term=2, candidate="n2"))
    assert resp.vote_granted


async def test_grants_when_log_more_up_to_date():
    state, _ = make_state(term=2, entries=[LogEntryRecord(1, 1, b"a"), LogEntryRecord(2, 2, b"b")])
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_request_vote(
        _rv(term=3, candidate="n5", last_log_index=3, last_log_term=2)
    )
    assert resp.vote_granted


async def test_denies_when_candidate_log_less_up_to_date():
    state, _ = make_state(term=2, entries=[LogEntryRecord(1, 1, b"a"), LogEntryRecord(2, 2, b"b")])
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_request_vote(
        _rv(term=3, candidate="n5", last_log_index=1, last_log_term=1)
    )
    assert not resp.vote_granted


async def test_grants_when_equal_term_higher_index():
    state, _ = make_state(term=2, entries=[LogEntryRecord(1, 1, b"a"), LogEntryRecord(2, 2, b"b")])
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_request_vote(
        _rv(term=3, candidate="n5", last_log_index=2, last_log_term=2)
    )
    assert resp.vote_granted


async def test_denies_when_equal_term_lower_index():
    state, _ = make_state(term=2, entries=[LogEntryRecord(1, 1, b"a"), LogEntryRecord(2, 2, b"b")])
    core = RaftCore(state, make_client_factory(None))
    resp = await core.handle_request_vote(
        _rv(term=3, candidate="n5", last_log_index=1, last_log_term=2)
    )
    assert not resp.vote_granted


async def test_grant_resets_election_deadline():
    state, _ = make_state(term=1, election_min_ms=10000, election_jitter_ms=0)
    deadline_before = state.election_deadline_ms
    core = RaftCore(state, make_client_factory(None))
    await core.handle_request_vote(_rv(term=1, candidate="n2"))
    assert state.election_deadline_ms >= deadline_before


async def test_grant_persists_voted_for():
    state, d = make_state(term=1)
    core = RaftCore(state, make_client_factory(None))
    await core.handle_request_vote(_rv(term=1, candidate="n2"))
    assert state.storage.load_metadata().voted_for == "n2"
