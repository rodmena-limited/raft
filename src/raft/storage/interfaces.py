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
    @abc.abstractmethod
    def load_metadata(self) -> LogMetadata: ...

    @abc.abstractmethod
    def store_metadata(self, meta: LogMetadata) -> None: ...

    @abc.abstractmethod
    def append_entries(self, entries: Iterable[LogEntryRecord]) -> None: ...

    @abc.abstractmethod
    def read_entries(self, start: int, end: int | None = None) -> list[LogEntryRecord]: ...

    @abc.abstractmethod
    def truncate_suffix(self, index: int) -> None: ...

    @abc.abstractmethod
    def last_index_term(self) -> tuple[int, int]: ...

    @abc.abstractmethod
    def first_index(self) -> int: ...


class SnapshotStore(abc.ABC):
    @abc.abstractmethod
    def load_snapshot(self) -> tuple[SnapshotMetadata, bytes] | None: ...

    @abc.abstractmethod
    def store_snapshot(self, meta: SnapshotMetadata, data: bytes) -> None: ...


class StateMachine(Protocol):
    def apply(self, data: bytes) -> bytes | None: ...
