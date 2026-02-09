from raft.rpc.proto import raft_pb2


def test_proto_roundtrip():
    msg = raft_pb2.RequestVoteRequest(term=1, candidate_id="n1", last_log_index=5, last_log_term=2)
    data = msg.SerializeToString()
    parsed = raft_pb2.RequestVoteRequest.FromString(data)
    assert parsed.term == 1
    assert parsed.candidate_id == "n1"
    assert parsed.last_log_index == 5
    assert parsed.last_log_term == 2
