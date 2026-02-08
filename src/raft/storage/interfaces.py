from __future__ import annotations
import abc
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol

@dataclass
class LogMetadata:
    term: int
    voted_for: str | None
    commit_index: int
    last_applied: int

@dataclass
class SnapshotMetadata:
    last_included_index: int
    last_included_term: int

@dataclass
class LogEntryRecord:
    index: int
    term: int
    data: bytes

class LogStorage(abc.ABC):

    def load_metadata(self) -> LogMetadata: ...

    def store_metadata(self, meta: LogMetadata) -> None: ...

    def append_entries(self, entries: Iterable[LogEntryRecord]) -> None: ...

    def read_entries(self, start: int, end: int | None = None) -> list[LogEntryRecord]: ...

    def truncate_suffix(self, index: int) -> None: ...

    def last_index_term(self) -> tuple[int, int]: ...

    def first_index(self) -> int: ...

class SnapshotStore(abc.ABC):

    def load_snapshot(self) -> tuple[SnapshotMetadata, bytes] | None: ...

    def store_snapshot(self, meta: SnapshotMetadata, data: bytes) -> None: ...
