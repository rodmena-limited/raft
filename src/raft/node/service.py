from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
import grpc
from raft.core.logic import RaftCore
from raft.core.state import RaftState
from raft.rpc.proto import raft_pb2, raft_pb2_grpc
from raft.storage import LogStorage, SnapshotStore, StateMachine
from raft.util import get_logger

class RaftServicer(raft_pb2_grpc.RaftServicer):
    def __init__(self, core: RaftCore):
        self.core = core

    async def RequestVote(self, request, context):
        return await self.core.handle_request_vote(request)

    async def AppendEntries(self, request, context):
        return await self.core.handle_append_entries(request)
