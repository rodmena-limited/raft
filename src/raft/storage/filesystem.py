from __future__ import annotations

import os
import pickle
from collections.abc import Iterable
from pathlib import Path

from .interfaces import LogEntryRecord, LogMetadata, LogStorage, SnapshotMetadata, SnapshotStore


class FsLogStorage(LogStorage):
    def __init__(self, base_dir: str):
        self.base = Path(base_dir)
        self.base.mkdir(parents=True, exist_ok=True)
        self.meta_path = self.base / "meta.pkl"
        self.log_path = self.base / "log.pkl"

        if not self.meta_path.exists():
            self.store_metadata(LogMetadata(term=0, voted_for=None, commit_index=0, last_applied=0))
        if not self.log_path.exists():
            self._write_log({"base_index": 0, "base_term": 0, "entries": []})

    def _read_log(self) -> dict:
        with self.log_path.open("rb") as f:
            return pickle.load(f)

    def _write_log(self, state: dict) -> None:
        with self.log_path.open("wb") as f:
            pickle.dump(state, f)
            f.flush()
            os.fsync(f.fileno())

    def _entries(self) -> list[LogEntryRecord]:
        return self._read_log()["entries"]

    def load_metadata(self) -> LogMetadata:
        with self.meta_path.open("rb") as f:
            return pickle.load(f)

    def store_metadata(self, meta: LogMetadata) -> None:
        with self.meta_path.open("wb") as f:
            pickle.dump(meta, f)
            f.flush()
            os.fsync(f.fileno())

    def append_entries(self, entries: Iterable[LogEntryRecord]) -> None:
        state = self._read_log()
        state["entries"].extend(entries)
        self._write_log(state)

    def read_entries(self, start: int, end: int | None = None) -> list[LogEntryRecord]:
        log = self._entries()
        if end is None:
            return [e for e in log if e.index >= start]
        return [e for e in log if start <= e.index < end]

    def truncate_suffix(self, index: int) -> None:
        state = self._read_log()
        state["entries"] = [e for e in state["entries"] if e.index <= index]
        self._write_log(state)

    def compact_prefix(self, index: int, term: int) -> None:
        state = self._read_log()
        if index < state["base_index"]:
            return
        state["entries"] = [e for e in state["entries"] if e.index > index]
        state["base_index"] = index
        state["base_term"] = term
        self._write_log(state)

    def compaction_base(self) -> tuple[int, int]:
        state = self._read_log()
        return state["base_index"], state["base_term"]

    def last_index_term(self) -> tuple[int, int]:
        state = self._read_log()
        if state["entries"]:
            last = state["entries"][-1]
            return last.index, last.term
        return state["base_index"], state["base_term"]

    def first_index(self) -> int:
        state = self._read_log()
        if state["entries"]:
            return state["entries"][0].index
        return state["base_index"] + 1


class FsSnapshotStore(SnapshotStore):
    def __init__(self, base_dir: str):
        self.base = Path(base_dir)
        self.base.mkdir(parents=True, exist_ok=True)
        self.snap_meta_path = self.base / "snapshot.meta"
        self.snap_data_path = self.base / "snapshot.bin"

    def load_snapshot(self) -> tuple[SnapshotMetadata, bytes] | None:
        if not self.snap_meta_path.exists() or not self.snap_data_path.exists():
            return None
        with self.snap_meta_path.open("rb") as f:
            meta = pickle.load(f)
        data = self.snap_data_path.read_bytes()
        return meta, data

    def store_snapshot(self, meta: SnapshotMetadata, data: bytes) -> None:
        with self.snap_meta_path.open("wb") as f:
            pickle.dump(meta, f)
            f.flush()
            os.fsync(f.fileno())
        with self.snap_data_path.open("wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
