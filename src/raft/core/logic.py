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

    async def handle_install_snapshot(
        self, req: raft_pb2.InstallSnapshotRequest
    ) -> raft_pb2.InstallSnapshotResponse:
        if req.term < self.state.current_term:
            return raft_pb2.InstallSnapshotResponse(term=self.state.current_term, accepted=False)

        self.state.become_follower(req.term, leader_hint=req.leader_id)
        # For simplicity, directly store snapshot and reset log
        from raft.storage import SnapshotMetadata

        meta = SnapshotMetadata(
            last_included_index=req.last_included_index, last_included_term=req.last_included_term
        )
        self.state.snapshots.store_snapshot(meta, req.data)
        self.state.truncate_suffix(req.last_included_index)
        self.state.commit_index = max(self.state.commit_index, req.last_included_index)
        self.state.apply_entries()
        return raft_pb2.InstallSnapshotResponse(term=self.state.current_term, accepted=True)

    async def maybe_start_election(self) -> None:
        if monotonic_ms() < self.state.election_deadline_ms:
            return
        self.state.become_candidate()
        votes = 1  # vote for self
        needed = (len(self.state.peers) + 1) // 2 + 1

        last_index, last_term = self.state.last_log_index_term()
        req = raft_pb2.RequestVoteRequest(
            term=self.state.current_term,
            candidate_id=self.state.node_id,
            last_log_index=last_index,
            last_log_term=last_term,
        )

        async def ask_peer(peer: str) -> bool:
            try:
                async with self.rpc_client_factory(peer) as client:
                    resp = await client.RequestVote(req)
                if resp.term > self.state.current_term:
                    self.state.become_follower(resp.term)
                    return False
                return resp.vote_granted
            except Exception as e:  # noqa: BLE001
                self.logger.warning("vote request to %s failed: %s", peer, e)
                return False

        results = await asyncio.gather(*(ask_peer(p) for p in self.state.peers))
        votes += sum(1 for r in results if r)
        if votes >= needed:
            self.state.become_leader()
            await self.start_heartbeat_loop()

    async def start_heartbeat_loop(self) -> None:
        if self.heartbeat_task:
            self.heartbeat_task.cancel()

        async def loop():
            while self.state.role == Role.LEADER:
                await self.broadcast_append_entries()
                await asyncio.sleep(self.state.heartbeat_ms / 1000.0)

        self.heartbeat_task = asyncio.create_task(loop())

    async def broadcast_append_entries(self) -> None:
        last_index, last_term = self.state.last_log_index_term()

        async def send(peer: str) -> None:
            progress = self.state.progress[peer]
            prev_index = progress.next_index - 1
            entries = self.state.read_entries(progress.next_index)
            prev_term = 0
            if prev_index > 0:
                prev_entries = self.state.read_entries(prev_index, prev_index + 1)
                if prev_entries:
                    prev_term = prev_entries[0].term
            req = raft_pb2.AppendEntriesRequest(
                term=self.state.current_term,
                leader_id=self.state.node_id,
                prev_log_index=prev_index,
                prev_log_term=prev_term,
                entries=[
                    raft_pb2.LogEntry(index=e.index, term=e.term, data=e.data) for e in entries
                ],
                leader_commit=self.state.commit_index,
            )
            try:
                async with self.rpc_client_factory(peer) as client:
                    resp = await client.AppendEntries(req)
                if resp.term > self.state.current_term:
                    self.state.become_follower(resp.term)
                    return
                if resp.success:
                    if entries:
                        progress.match_index = entries[-1].index
                        progress.next_index = progress.match_index + 1
                    else:
                        progress.match_index = prev_index
                        progress.next_index = progress.match_index + 1
                    await self.maybe_advance_commit_index()
                else:
                    progress.next_index = max(1, progress.next_index - 1)
            except Exception as e:  # noqa: BLE001
                self.logger.warning("append to %s failed: %s", peer, e)

        await asyncio.gather(*(send(p) for p in self.state.peers))

    async def maybe_advance_commit_index(self) -> None:
        match_indexes = [self.state.progress[p].match_index for p in self.state.peers]
        match_indexes.append(self.state.last_log_index_term()[0])
        match_indexes.sort()
        majority_match = match_indexes[len(match_indexes) // 2]
        _, term_at_idx = self.state.last_log_index_term()
        if majority_match > self.state.commit_index and term_at_idx == self.state.current_term:
            self.state.commit_index = majority_match
            self.state.apply_entries()

    async def client_write(self, data: bytes) -> tuple[bool, int, int]:
        if self.state.role != Role.LEADER:
            return False, 0, self.state.current_term
        last_index, _ = self.state.last_log_index_term()
        entry = LogEntryRecord(index=last_index + 1, term=self.state.current_term, data=data)
        self.state.append_log_entries([entry])
        await self.broadcast_append_entries()
        return True, entry.index, entry.term
