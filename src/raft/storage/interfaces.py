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
    pass
