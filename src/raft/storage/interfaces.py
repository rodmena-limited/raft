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
    def compact_prefix(self, index: int, term: int) -> None:
        """Drop all entries with index <= ``index`` and record the compaction
        base ``(index, term)``. Used when a snapshot covers those entries."""

    @abc.abstractmethod
    def compaction_base(self) -> tuple[int, int]:
        """Return (base_index, base_term): the snapshot-covered prefix of the
        log that has been compacted away. (0, 0) when nothing was compacted."""

    @abc.abstractmethod
    def last_index_term(self) -> tuple[int, int]: ...

    @abc.abstractmethod
    def first_index(self) -> int: ...


def sm_snapshot(state_machine: object) -> bytes | None:
    """Return the state machine's own snapshot blob, or None if the state
    machine does not support self-snapshotting."""
    fn = getattr(state_machine, "snapshot", None)
    return fn() if callable(fn) else None


def sm_restore(state_machine: object, data: bytes) -> bool:
    """Restore a state machine from a snapshot blob. Returns True when the
    state machine implements restore, False otherwise."""
    fn = getattr(state_machine, "restore", None)
    if callable(fn):
        fn(data)
        return True
    return False


class SnapshotStore(abc.ABC):
    @abc.abstractmethod
    def load_snapshot(self) -> tuple[SnapshotMetadata, bytes] | None: ...

    @abc.abstractmethod
    def store_snapshot(self, meta: SnapshotMetadata, data: bytes) -> None: ...


class StateMachine(Protocol):
    def apply(self, data: bytes) -> bytes | None: ...
