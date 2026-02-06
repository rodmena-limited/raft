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
    FOLLOWER = 'follower'
    CANDIDATE = 'candidate'
    LEADER = 'leader'

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

    def last_log_index_term(self) -> tuple[int, int]:
        return self.storage.last_index_term()

    def append_log_entries(self, entries: list[LogEntryRecord]) -> None:
        self.storage.append_entries(entries)

    def truncate_suffix(self, index: int) -> None:
        self.storage.truncate_suffix(index)
