from __future__ import annotations
import asyncio
from raft.rpc.proto import raft_pb2
from raft.storage import LogEntryRecord
from raft.util import get_logger
from .state import RaftState, Role

class RaftCore:
    def __init__(self, state: RaftState, rpc_client_factory):
        self.state = state
        self.logger = get_logger(f"raft.core.{state.node_id}")
        self.rpc_client_factory = rpc_client_factory
        self.heartbeat_task: asyncio.Task | None = None

    async def handle_request_vote(
        self, req: raft_pb2.RequestVoteRequest
    ) -> raft_pb2.RequestVoteResponse:
        if req.term < self.state.current_term:
            return raft_pb2.RequestVoteResponse(term=self.state.current_term, vote_granted=False)

        if req.term > self.state.current_term:
            self.state.become_follower(req.term)

        up_to_date = (req.last_log_term > self.state.last_log_index_term()[1]) or (
            req.last_log_term == self.state.last_log_index_term()[1]
            and req.last_log_index >= self.state.last_log_index_term()[0]
        )

        if (self.state.voted_for in (None, req.candidate_id)) and up_to_date:
            self.state.voted_for = req.candidate_id
            self.state.persist_metadata()
            self.state.reset_election_deadline()
            return raft_pb2.RequestVoteResponse(term=self.state.current_term, vote_granted=True)

        return raft_pb2.RequestVoteResponse(term=self.state.current_term, vote_granted=False)
