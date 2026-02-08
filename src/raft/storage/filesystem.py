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
            self._write_log([])

    def _read_log(self) -> list[LogEntryRecord]:
        with self.log_path.open("rb") as f:
            return pickle.load(f)

    def _write_log(self, entries: list[LogEntryRecord]) -> None:
        with self.log_path.open("wb") as f:
            pickle.dump(entries, f)
            f.flush()
            os.fsync(f.fileno())

    def load_metadata(self) -> LogMetadata:
        with self.meta_path.open("rb") as f:
            return pickle.load(f)

    def store_metadata(self, meta: LogMetadata) -> None:
        with self.meta_path.open("wb") as f:
            pickle.dump(meta, f)
            f.flush()
            os.fsync(f.fileno())

    def append_entries(self, entries: Iterable[LogEntryRecord]) -> None:
        current = self._read_log()
        current.extend(entries)
        self._write_log(current)

    def read_entries(self, start: int, end: int | None = None) -> list[LogEntryRecord]:
        log = self._read_log()
        if end is None:
            return [e for e in log if e.index >= start]
        return [e for e in log if start <= e.index < end]

    def truncate_suffix(self, index: int) -> None:
        log = self._read_log()
        truncated = [e for e in log if e.index <= index]
        self._write_log(truncated)

    def last_index_term(self) -> tuple[int, int]:
        log = self._read_log()
        if not log:
            return 0, 0
        last = log[-1]
        return last.index, last.term

    def first_index(self) -> int:
        log = self._read_log()
        return log[0].index if log else 0
