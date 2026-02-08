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
