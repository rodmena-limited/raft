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
