from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Timeouts:
    election_min_ms: int = 150
    election_jitter_ms: int = 150
    heartbeat_ms: int = 50


@dataclass
class Snapshotting:
    snapshot_threshold: int = 1000  # entries after last snapshot
    snapshot_trailing: int = 100  # keep trailing entries after snapshot point


@dataclass
class RaftConfig:
    node_id: str
    peers: list[str]
    timeouts: Timeouts = Timeouts()
    snapshotting: Snapshotting = Snapshotting()


DEFAULT_CONFIG = RaftConfig(node_id="", peers=[])
