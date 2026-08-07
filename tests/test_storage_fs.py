from __future__ import annotations

import os
import tempfile

from helpers import load_snapshot

from raft.storage import (
    FsLogStorage,
    FsSnapshotStore,
    LogEntryRecord,
    LogMetadata,
    SnapshotMetadata,
)


def _entries(*pairs: tuple[int, int, bytes]) -> list[LogEntryRecord]:
    return [LogEntryRecord(index=i, term=t, data=d) for i, t, d in pairs]


def test_log_append_read_roundtrip():
    with tempfile.TemporaryDirectory() as d:
        log = FsLogStorage(d)
        log.append_entries(_entries((1, 1, b"a"), (2, 1, b"b"), (3, 2, b"c")))
        assert [e.data for e in log.read_entries(1)] == [b"a", b"b", b"c"]
        assert [e.data for e in log.read_entries(2)] == [b"b", b"c"]
        assert [e.data for e in log.read_entries(1, 3)] == [b"a", b"b"]
        assert [e.data for e in log.read_entries(1, 2)] == [b"a"]
        assert log.read_entries(4) == []
        assert log.read_entries(99, 100) == []


def test_log_empty_state():
    with tempfile.TemporaryDirectory() as d:
        log = FsLogStorage(d)
        assert log.last_index_term() == (0, 0)
        assert log.first_index() == 1
        assert log.read_entries(1) == []


def test_log_last_index_term():
    with tempfile.TemporaryDirectory() as d:
        log = FsLogStorage(d)
        log.append_entries(_entries((1, 1, b"a"), (2, 1, b"b")))
        assert log.last_index_term() == (2, 1)


def test_truncate_suffix():
    with tempfile.TemporaryDirectory() as d:
        log = FsLogStorage(d)
        log.append_entries(_entries((1, 1, b"a"), (2, 1, b"b"), (3, 2, b"c")))
        log.truncate_suffix(1)
        assert [e.index for e in log.read_entries(1)] == [1]
        log.truncate_suffix(0)
        assert log.read_entries(1) == []
        assert log.last_index_term() == (0, 0)


def test_compact_prefix():
    with tempfile.TemporaryDirectory() as d:
        log = FsLogStorage(d)
        log.append_entries(_entries((1, 1, b"a"), (2, 1, b"b"), (3, 2, b"c"), (4, 2, b"d")))
        log.compact_prefix(2, 1)
        assert log.compaction_base() == (2, 1)
        assert [e.index for e in log.read_entries(1)] == [3, 4]
        assert log.last_index_term() == (4, 2)
        assert log.first_index() == 3


def test_compact_prefix_ignores_lower_base():
    with tempfile.TemporaryDirectory() as d:
        log = FsLogStorage(d)
        log.append_entries(_entries((3, 2, b"c"), (4, 2, b"d")))
        log.compact_prefix(2, 1)
        assert log.compaction_base() == (2, 1)
        log.compact_prefix(1, 1)
        assert log.compaction_base() == (2, 1)
        assert [e.index for e in log.read_entries(1)] == [3, 4]


def test_compact_prefix_empty_log_last_index_uses_base():
    with tempfile.TemporaryDirectory() as d:
        log = FsLogStorage(d)
        log.append_entries(_entries((1, 1, b"a"), (2, 1, b"b")))
        log.compact_prefix(5, 3)
        assert log.read_entries(1) == []
        assert log.last_index_term() == (5, 3)
        assert log.first_index() == 6


def test_append_after_compact():
    with tempfile.TemporaryDirectory() as d:
        log = FsLogStorage(d)
        log.append_entries(_entries((1, 1, b"a"), (2, 1, b"b"), (3, 2, b"c")))
        log.compact_prefix(2, 1)
        log.append_entries(_entries((4, 2, b"d")))
        assert [e.index for e in log.read_entries(1)] == [3, 4]
        assert log.last_index_term() == (4, 2)


def test_metadata_persist_reload():
    with tempfile.TemporaryDirectory() as d:
        log = FsLogStorage(d)
        log.store_metadata(LogMetadata(term=7, voted_for="n2", commit_index=5, last_applied=4))
        reopened = FsLogStorage(d)
        meta = reopened.load_metadata()
        assert meta.term == 7
        assert meta.voted_for == "n2"
        assert meta.commit_index == 5
        assert meta.last_applied == 4


def test_default_metadata_on_fresh_dir():
    with tempfile.TemporaryDirectory() as d:
        meta = FsLogStorage(d).load_metadata()
        assert meta == LogMetadata(term=0, voted_for=None, commit_index=0, last_applied=0)


def test_log_durable_across_reopen():
    with tempfile.TemporaryDirectory() as d:
        log = FsLogStorage(d)
        log.append_entries(_entries((1, 1, b"a"), (2, 1, b"b"), (3, 1, b"c")))
        log.compact_prefix(2, 1)
        log.append_entries(_entries((4, 1, b"d")))
        reopened = FsLogStorage(d)
        assert [e.data for e in reopened.read_entries(1)] == [b"c", b"d"]
        assert reopened.compaction_base() == (2, 1)
        assert reopened.last_index_term() == (4, 1)


def test_append_persists_to_disk_immediately():
    with tempfile.TemporaryDirectory() as d:
        log = FsLogStorage(d)
        log.append_entries(_entries((1, 1, b"a")))
        assert os.path.exists(os.path.join(d, "log.pkl"))
        with open(os.path.join(d, "log.pkl"), "rb") as f:
            import pickle

            state = pickle.load(f)
        assert len(state["entries"]) == 1


def test_snapshot_store_roundtrip():
    with tempfile.TemporaryDirectory() as d:
        snaps = FsSnapshotStore(d)
        meta = SnapshotMetadata(last_included_index=5, last_included_term=2)
        data = b"snapshot"
        snaps.store_snapshot(meta, data)
        loaded = snaps.load_snapshot()
        assert loaded is not None
        meta2, data2 = loaded
        assert meta2 == meta
        assert data2 == data


def test_snapshot_store_none_when_missing():
    with tempfile.TemporaryDirectory() as d:
        snaps = FsSnapshotStore(d)
        assert snaps.load_snapshot() is None


def test_snapshot_overwrite():
    with tempfile.TemporaryDirectory() as d:
        snaps = FsSnapshotStore(d)
        snaps.store_snapshot(SnapshotMetadata(1, 1), b"one")
        snaps.store_snapshot(SnapshotMetadata(2, 1), b"two")
        meta, data = load_snapshot(snaps)
        assert meta.last_included_index == 2
        assert data == b"two"


def test_snapshot_durable_across_reopen():
    with tempfile.TemporaryDirectory() as d:
        snaps = FsSnapshotStore(d)
        snaps.store_snapshot(SnapshotMetadata(3, 2), b"data")
        reopened = FsSnapshotStore(d)
        meta, data = load_snapshot(reopened)
        assert (meta.last_included_index, meta.last_included_term) == (3, 2)
        assert data == b"data"


def test_append_empty_batch_is_noop():
    with tempfile.TemporaryDirectory() as d:
        log = FsLogStorage(d)
        log.append_entries([])
        assert log.last_index_term() == (0, 0)
