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

    async def handle_append_entries(
        self, req: raft_pb2.AppendEntriesRequest
    ) -> raft_pb2.AppendEntriesResponse:
        if req.term < self.state.current_term:
            return raft_pb2.AppendEntriesResponse(
                term=self.state.current_term, success=False, match_index=0
            )

        self.state.reset_election_deadline()
        if req.term > self.state.current_term or self.state.role != Role.FOLLOWER:
            self.state.become_follower(req.term, leader_hint=req.leader_id)

        prev_index = req.prev_log_index
        prev_term = req.prev_log_term
        last_index, _ = self.state.last_log_index_term()
        if prev_index > last_index:
            return raft_pb2.AppendEntriesResponse(
                term=self.state.current_term, success=False, match_index=last_index
            )

        if prev_index > 0:
            entries = self.state.read_entries(prev_index, prev_index + 1)
            if not entries or entries[0].term != prev_term:
                return raft_pb2.AppendEntriesResponse(
                    term=self.state.current_term, success=False, match_index=prev_index - 1
                )

        # append new entries, overwriting conflicts
        if req.entries:
            # Remove any conflicting entries starting at first new entry index
            first_new_index = req.entries[0].index
            self.state.truncate_suffix(first_new_index - 1)
            new_records = [
                LogEntryRecord(index=e.index, term=e.term, data=e.data) for e in req.entries
            ]
            self.state.append_log_entries(new_records)

        if req.leader_commit > self.state.commit_index:
            self.state.commit_index = min(req.leader_commit, self.state.last_log_index_term()[0])
            self.state.apply_entries()

        match_index = self.state.last_log_index_term()[0]
        return raft_pb2.AppendEntriesResponse(
            term=self.state.current_term, success=True, match_index=match_index
        )
