from __future__ import annotations

import enum
from dataclasses import dataclass

from raft.storage import (
    LogEntryRecord,
    LogMetadata,
    LogStorage,
    SnapshotMetadata,
    SnapshotStore,
    StateMachine,
)
from raft.util import get_logger, monotonic_ms, randomized_timeout_ms


class Role(enum.Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class PeerProgress:
    match_index: int = 0
    next_index: int = 1


class RaftState:
    def __init__(
        self,
        node_id: str,
        peers: list[str],
        storage: LogStorage,
        snapshots: SnapshotStore,
        state_machine: StateMachine,
        election_min_ms: int,
        election_jitter_ms: int,
        heartbeat_ms: int,
    ) -> None:
        self.node_id = node_id
        self.peers = peers
        self.storage = storage
        self.snapshots = snapshots
        self.sm = state_machine

        self.role: Role = Role.FOLLOWER
        self.current_term = 0
        self.voted_for: str | None = None
        self.commit_index = 0
        self.last_applied = 0

        meta = self.storage.load_metadata()
        self.current_term = meta.term
        self.voted_for = meta.voted_for
        self.commit_index = meta.commit_index
        self.last_applied = meta.last_applied

        self.heartbeat_ms = heartbeat_ms
        self.election_min_ms = election_min_ms
        self.election_jitter_ms = election_jitter_ms

        self.reset_election_deadline()

        self.progress: dict[str, PeerProgress] = {p: PeerProgress() for p in peers}
        self.logger = get_logger(f"raft.{node_id}")

    def reset_election_deadline(self) -> None:
        self.election_deadline_ms = monotonic_ms() + randomized_timeout_ms(
            self.election_min_ms, self.election_jitter_ms
        )

    def persist_metadata(self) -> None:
        self.storage.store_metadata(
            LogMetadata(
                term=self.current_term,
                voted_for=self.voted_for,
                commit_index=self.commit_index,
                last_applied=self.last_applied,
            )
        )

    # Log helpers
    def last_log_index_term(self) -> tuple[int, int]:
        return self.storage.last_index_term()

    def append_log_entries(self, entries: list[LogEntryRecord]) -> None:
        self.storage.append_entries(entries)

    def truncate_suffix(self, index: int) -> None:
        self.storage.truncate_suffix(index)

    def read_entries(self, start: int, end: int | None = None) -> list[LogEntryRecord]:
        return self.storage.read_entries(start, end)

    # Snapshot helpers
    def maybe_snapshot(self, snapshot_threshold: int, trailing: int) -> None:
        last_index, _ = self.last_log_index_term()
        first_index = self.storage.first_index()
        if last_index - first_index < snapshot_threshold:
            return
        snapshot_index = last_index - trailing
        entries = self.read_entries(first_index, snapshot_index + 1)
        data = b"".join(e.data for e in entries)  # simplistic; state machine could snapshot itself
        if entries:
            snap_meta = SnapshotMetadata(
                last_included_index=entries[-1].index, last_included_term=entries[-1].term
            )
            self.snapshots.store_snapshot(snap_meta, data)
            self.storage.truncate_suffix(snap_meta.last_included_index)

    # Apply committed entries to state machine
    def apply_entries(self) -> None:
        entries = self.read_entries(self.last_applied + 1, self.commit_index + 1)
        for entry in entries:
            self.sm.apply(entry.data)
            self.last_applied = entry.index
        self.persist_metadata()

    # Role transitions
    def become_follower(self, term: int, leader_hint: str | None = None) -> None:
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.persist_metadata()
        self.role = Role.FOLLOWER
        self.reset_election_deadline()
        self.logger.info("become follower term=%s leader=%s", self.current_term, leader_hint)

    def become_candidate(self) -> None:
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.persist_metadata()
        self.reset_election_deadline()
        self.logger.info("become candidate term=%s", self.current_term)

    def become_leader(self) -> None:
        self.role = Role.LEADER
        last_index, _ = self.last_log_index_term()
        for peer in self.peers:
            self.progress[peer] = PeerProgress(match_index=0, next_index=last_index + 1)
        self.logger.info("become leader term=%s", self.current_term)
