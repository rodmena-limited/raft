from __future__ import annotations

from raft.rpc.proto import raft_pb2


def _roundtrip(msg):
    data = msg.SerializeToString()
    return type(msg).FromString(data)


def test_request_vote_roundtrip():
    msg = raft_pb2.RequestVoteRequest(term=1, candidate_id="n1", last_log_index=5, last_log_term=2)
    parsed = _roundtrip(msg)
    assert parsed.term == 1
    assert parsed.candidate_id == "n1"
    assert parsed.last_log_index == 5
    assert parsed.last_log_term == 2


def test_request_vote_response_roundtrip():
    parsed = _roundtrip(raft_pb2.RequestVoteResponse(term=3, vote_granted=True))
    assert parsed.term == 3
    assert parsed.vote_granted is True


def test_append_entries_request_roundtrip():
    msg = raft_pb2.AppendEntriesRequest(
        term=2,
        leader_id="n2",
        prev_log_index=4,
        prev_log_term=1,
        entries=[
            raft_pb2.LogEntry(index=5, term=2, data=b"x"),
            raft_pb2.LogEntry(index=6, term=2, data=b"y"),
        ],
        leader_commit=4,
    )
    parsed = _roundtrip(msg)
    assert parsed.term == 2
    assert parsed.leader_id == "n2"
    assert parsed.prev_log_index == 4
    assert parsed.prev_log_term == 1
    assert [(e.index, e.term, e.data) for e in parsed.entries] == [(5, 2, b"x"), (6, 2, b"y")]
    assert parsed.leader_commit == 4


def test_append_entries_response_roundtrip():
    parsed = _roundtrip(raft_pb2.AppendEntriesResponse(term=2, success=True, match_index=6))
    assert parsed.term == 2
    assert parsed.success is True
    assert parsed.match_index == 6


def test_install_snapshot_roundtrip():
    msg = raft_pb2.InstallSnapshotRequest(
        term=3, leader_id="n1", last_included_index=10, last_included_term=2, data=b"\x00blob"
    )
    parsed = _roundtrip(msg)
    assert parsed.term == 3
    assert parsed.leader_id == "n1"
    assert parsed.last_included_index == 10
    assert parsed.last_included_term == 2
    assert parsed.data == b"\x00blob"


def test_install_snapshot_response_roundtrip():
    parsed = _roundtrip(raft_pb2.InstallSnapshotResponse(term=3, accepted=True))
    assert parsed.term == 3
    assert parsed.accepted is True


def test_client_write_roundtrip():
    parsed = _roundtrip(raft_pb2.ClientWriteRequest(data=b"cmd"))
    assert parsed.data == b"cmd"
    resp = _roundtrip(raft_pb2.ClientWriteResponse(accepted=True, index=7, term=2))
    assert resp.accepted is True
    assert resp.index == 7
    assert resp.term == 2


def test_membership_change_roundtrip():
    msg = raft_pb2.MembershipChangeRequest(node_id="n4", add=True)
    parsed = _roundtrip(msg)
    assert parsed.node_id == "n4"
    assert parsed.add is True
    resp = _roundtrip(raft_pb2.MembershipChangeResponse(accepted=False, message="not implemented"))
    assert resp.accepted is False
    assert resp.message == "not implemented"


def test_log_entry_roundtrip():
    parsed = _roundtrip(raft_pb2.LogEntry(index=3, term=1, data=b"payload"))
    assert parsed.index == 3
    assert parsed.term == 1
    assert parsed.data == b"payload"


def test_empty_append_entries_roundtrip():
    parsed = _roundtrip(raft_pb2.AppendEntriesRequest(term=1, leader_id="n1"))
    assert parsed.entries == []
