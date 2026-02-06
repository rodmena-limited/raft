from __future__ import annotations
import asyncio
from raft.rpc.proto import raft_pb2
from raft.storage import LogEntryRecord
from raft.util import get_logger
from .state import RaftState, Role

class RaftCore:
    def __init__(self, state: RaftState, rpc_client_factory):
        self.state = state
        self.logger = get_logger(f"raft.core.{state.node_id}")
        self.rpc_client_factory = rpc_client_factory
        self.heartbeat_task: asyncio.Task | None = None
