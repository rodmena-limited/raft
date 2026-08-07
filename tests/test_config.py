from __future__ import annotations

from raft.config import DEFAULT_CONFIG, RaftConfig, Snapshotting, Timeouts


def test_timeouts_defaults():
    t = Timeouts()
    assert t.election_min_ms == 150
    assert t.election_jitter_ms == 150
    assert t.heartbeat_ms == 50


def test_snapshotting_defaults():
    s = Snapshotting()
    assert s.snapshot_threshold == 1000
    assert s.snapshot_trailing == 100


def test_config_defaults():
    c = RaftConfig(node_id="n1", peers=["n2", "n3"])
    assert c.node_id == "n1"
    assert c.peers == ["n2", "n3"]
    assert c.timeouts == Timeouts()
    assert c.snapshotting == Snapshotting()


def test_default_config():
    assert DEFAULT_CONFIG.node_id == ""
    assert DEFAULT_CONFIG.peers == []


def test_config_override():
    c = RaftConfig(
        node_id="n1",
        peers=[],
        timeouts=Timeouts(election_min_ms=200, election_jitter_ms=100, heartbeat_ms=25),
        snapshotting=Snapshotting(snapshot_threshold=500, snapshot_trailing=50),
    )
    assert c.timeouts.election_min_ms == 200
    assert c.snapshotting.snapshot_threshold == 500
