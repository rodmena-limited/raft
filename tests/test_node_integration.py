from __future__ import annotations

import asyncio
import pickle
import socket
import tempfile

import grpc
import pytest
from helpers import kv_sm

from raft.core.state import Role
from raft.node import RaftNode
from raft.rpc.proto import raft_pb2, raft_pb2_grpc
from raft.sm import KeyValueStateMachine
from raft.storage import FsLogStorage, FsSnapshotStore

ELECTION_MIN_MS = 150
ELECTION_JITTER_MS = 150
HEARTBEAT_MS = 50


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_node(node_id: str, peers: list[str], bind: str) -> RaftNode:
    d = tempfile.mkdtemp(prefix=f"raft-{node_id}-")
    return RaftNode(
        node_id=node_id,
        peers=peers,
        storage=FsLogStorage(d),
        snapshots=FsSnapshotStore(d),
        state_machine=KeyValueStateMachine(),
        bind=bind,
        election_min_ms=ELECTION_MIN_MS,
        election_jitter_ms=ELECTION_JITTER_MS,
        heartbeat_ms=HEARTBEAT_MS,
    )


async def _wait_for(predicate, timeout: float = 10.0, interval: float = 0.02) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


def _rpc_client(target: str):
    channel = grpc.aio.insecure_channel(target)
    return raft_pb2_grpc.RaftStub(channel)


@pytest.fixture
async def cluster():
    ports = [_free_port() for _ in range(3)]
    addrs = [f"127.0.0.1:{p}" for p in ports]
    nodes = [
        _make_node(f"n{i}", [a for j, a in enumerate(addrs) if j != i], addrs[i]) for i in range(3)
    ]
    for n in nodes:
        await n.start()
    try:
        yield nodes, addrs
    finally:
        for n in nodes:
            await n.stop()


async def _leader_of(nodes):
    for n in nodes:
        if n.state.role == Role.LEADER:
            return n
    return None


@pytest.mark.asyncio
async def test_cluster_elects_single_stable_leader(cluster):
    nodes, _ = cluster
    ok = await _wait_for(lambda: sum(1 for n in nodes if n.state.role == Role.LEADER) == 1)
    assert ok, "no single leader elected"
    leader = await _leader_of(nodes)
    assert leader is not None
    term0 = leader.state.current_term
    # steady state: leader must not start new elections (term must not grow)
    await asyncio.sleep(0.8)
    leader_now = await _leader_of(nodes)
    assert leader_now is leader
    assert leader.state.current_term == term0, "leader self-deposed (term grew)"


@pytest.mark.asyncio
async def test_client_write_replicates_to_all_nodes(cluster):
    nodes, addrs = cluster
    ok = await _wait_for(lambda: sum(1 for n in nodes if n.state.role == Role.LEADER) == 1)
    assert ok
    leader = await _leader_of(nodes)
    op = pickle.dumps(("put", "k", b"v"))
    stub = _rpc_client(f"127.0.0.1:{leader.port}")
    resp = await stub.ClientWrite(raft_pb2.ClientWriteRequest(data=op))
    assert resp.accepted
    assert resp.index >= 1
    ok = await _wait_for(lambda: all(kv_sm(n.state).store.get("k") == b"v" for n in nodes))
    assert ok, "write not applied on all nodes"
    assert all(n.state.commit_index == resp.index for n in nodes)


@pytest.mark.asyncio
async def test_follower_rejects_client_write(cluster):
    nodes, addrs = cluster
    ok = await _wait_for(lambda: sum(1 for n in nodes if n.state.role == Role.LEADER) == 1)
    assert ok
    leader = await _leader_of(nodes)
    follower = next(n for n in nodes if n is not leader)
    stub = _rpc_client(f"127.0.0.1:{follower.port}")
    resp = await stub.ClientWrite(
        raft_pb2.ClientWriteRequest(data=pickle.dumps(("put", "k", b"v")))
    )
    assert not resp.accepted


@pytest.mark.asyncio
async def test_leader_failover_elects_new_leader_with_higher_term(cluster):
    nodes, addrs = cluster
    ok = await _wait_for(lambda: sum(1 for n in nodes if n.state.role == Role.LEADER) == 1)
    assert ok
    leader = await _leader_of(nodes)
    old_term = leader.state.current_term
    await leader.stop()
    remaining = [n for n in nodes if n is not leader]
    ok = await _wait_for(lambda: sum(1 for n in remaining if n.state.role == Role.LEADER) == 1)
    assert ok, "no new leader after failover"
    new_leader = await _leader_of(remaining)
    assert new_leader.state.current_term > old_term
    # new leader accepts writes
    stub = _rpc_client(f"127.0.0.1:{new_leader.port}")
    resp = await stub.ClientWrite(
        raft_pb2.ClientWriteRequest(data=pickle.dumps(("put", "k2", b"v2")))
    )
    assert resp.accepted


@pytest.mark.asyncio
async def test_cluster_survives_majority_loss_and_recovers(cluster):
    """Killing 2 of 3 nodes stops commits; the survivor still rejects writes."""
    nodes, addrs = cluster
    ok = await _wait_for(lambda: sum(1 for n in nodes if n.state.role == Role.LEADER) == 1)
    assert ok
    leader = await _leader_of(nodes)
    survivors = [n for n in nodes if n is not leader]
    await leader.stop()
    await survivors[0].stop()
    # only one node left: it must not become leader on its own and must reject writes
    alone = survivors[1]
    await asyncio.sleep(0.5)
    assert alone.state.role != Role.LEADER
    stub = _rpc_client(f"127.0.0.1:{alone.port}")
    resp = await stub.ClientWrite(
        raft_pb2.ClientWriteRequest(data=pickle.dumps(("put", "k", b"v")))
    )
    assert not resp.accepted
