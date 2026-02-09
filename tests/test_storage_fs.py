import tempfile

from raft.storage import FsLogStorage, FsSnapshotStore, LogEntryRecord, SnapshotMetadata


def test_log_append_truncate_roundtrip():
    with tempfile.TemporaryDirectory() as d:
        log = FsLogStorage(d)
        log.append_entries(
            [LogEntryRecord(index=1, term=1, data=b"a"), LogEntryRecord(index=2, term=1, data=b"b")]
        )
        assert [e.data for e in log.read_entries(1)] == [b"a", b"b"]
        log.truncate_suffix(1)
        assert [e.data for e in log.read_entries(1)] == [b"a"]


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
