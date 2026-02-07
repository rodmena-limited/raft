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

    async def InstallSnapshot(self, request, context):
        return await self.core.handle_install_snapshot(request)

    async def ClientWrite(self, request, context):
        accepted, index, term = await self.core.client_write(request.data)
        return raft_pb2.ClientWriteResponse(accepted=accepted, index=index, term=term)

    async def ChangeMembership(self, request, context):
        # Stub; real joint consensus not fully implemented in this simplified version
        return raft_pb2.MembershipChangeResponse(accepted=False, message="not implemented")


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: list[str],
        storage: LogStorage,
        snapshots: SnapshotStore,
        state_machine: StateMachine,
        *,
        bind: str,
        election_min_ms: int,
        election_jitter_ms: int,
        heartbeat_ms: int,
    ) -> None:
        self.state = RaftState(
            node_id=node_id,
            peers=peers,
            storage=storage,
            snapshots=snapshots,
            state_machine=state_machine,
            election_min_ms=election_min_ms,
            election_jitter_ms=election_jitter_ms,
            heartbeat_ms=heartbeat_ms,
        )
        self.logger = get_logger(f"raft.node.{node_id}")
        self.bind = bind

        @asynccontextmanager
        async def client_factory(target: str):
            async with grpc.aio.insecure_channel(target) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)
                yield stub

        self.core = RaftCore(self.state, client_factory)
        self.server = grpc.aio.server(options=[("grpc.so_reuseport", 0)])
        raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(self.core), self.server)
        self.server.add_insecure_port(bind)

    async def start(self) -> None:
        await self.server.start()
        self.logger.info("node started on %s", self.bind)
        asyncio.create_task(self._run_election_timer())

    async def _run_election_timer(self) -> None:
        while True:
            await asyncio.sleep(0.01)
            await self.core.maybe_start_election()

    async def stop(self) -> None:
        await self.server.stop(grace=None)
        if self.core.heartbeat_task:
            self.core.heartbeat_task.cancel()
        self.logger.info("node stopped")
