"""Shared helpers for the raft audit probes.

Every probe drives the node through its own public interface -- the gRPC
service defined in proto/raft.proto -- rather than reaching into internal
state to assert correctness. Where a probe must observe internal state (for
example to show that a follower's log was truncated), it says so explicitly
in its evidence output.

Configuration comes from the environment so the same probe runs against a
scratch directory, a staging cluster, or the next deployment unchanged:

    RAFT_PROBE_DIR   base directory for node data (default: a temp dir)
    RAFT_PROBE_HOST  interface to bind probe nodes to (default: 127.0.0.1)
"""

from __future__ import annotations

import asyncio
import os
import pickle
import shutil
import socket
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

import grpc  # noqa: E402

from raft.node import RaftNode  # noqa: E402
from raft.rpc.proto import raft_pb2, raft_pb2_grpc  # noqa: E402
from raft.sm import KeyValueStateMachine  # noqa: E402
from raft.storage import FsLogStorage, FsSnapshotStore  # noqa: E402

HOST = os.environ.get("RAFT_PROBE_HOST", "127.0.0.1")

ELECTION_MIN_MS = 150
ELECTION_JITTER_MS = 150
HEARTBEAT_MS = 50


def probe_dir(name: str) -> str:
    base = os.environ.get("RAFT_PROBE_DIR")
    if base:
        d = Path(base) / name
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)
        return str(d)
    return tempfile.mkdtemp(prefix=f"raft-probe-{name}-")


def free_port() -> int:
    with socket.socket() as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


def make_node(
    node_id: str,
    peers: list[str],
    bind: str,
    data_dir: str,
    *,
    election_min_ms: int = ELECTION_MIN_MS,
    election_jitter_ms: int = ELECTION_JITTER_MS,
    heartbeat_ms: int = HEARTBEAT_MS,
    state_machine=None,
) -> RaftNode:
    """Build a node exactly the way node/service.py expects to be used."""
    return RaftNode(
        node_id=node_id,
        peers=peers,
        storage=FsLogStorage(data_dir),
        snapshots=FsSnapshotStore(data_dir),
        state_machine=state_machine or KeyValueStateMachine(),
        bind=bind,
        election_min_ms=election_min_ms,
        election_jitter_ms=election_jitter_ms,
        heartbeat_ms=heartbeat_ms,
    )


def rpc_client(target: str):
    channel = grpc.aio.insecure_channel(target)
    return raft_pb2_grpc.RaftStub(channel), channel


async def wait_for(predicate, timeout: float = 10.0, interval: float = 0.02) -> bool:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


def kv_put(key: str, value: bytes) -> bytes:
    return pickle.dumps(("put", key, value))


async def client_write(target: str, data: bytes, timeout: float = 5.0):
    """Submit a write through the real ClientWrite RPC."""
    stub, channel = rpc_client(target)
    try:
        return await asyncio.wait_for(
            stub.ClientWrite(raft_pb2.ClientWriteRequest(data=data)), timeout
        )
    finally:
        await channel.close()


class Probe:
    """Collects evidence and renders a PASS/FAIL verdict.

    PASS means the safety/liveness property under test HELD.
    FAIL means the property was violated -- i.e. the defect reproduced.
    """

    def __init__(self, name: str, claim: str):
        self.name = name
        self.claim = claim
        self.evidence: list[str] = []
        self.failures: list[str] = []

    def observe(self, msg: str) -> None:
        self.evidence.append(msg)
        print(f"    | {msg}", flush=True)

    def expect(self, condition: bool, msg: str) -> bool:
        if condition:
            self.observe(f"OK   {msg}")
        else:
            self.failures.append(msg)
            self.observe(f"VIOL {msg}")
        return condition

    def report(self) -> int:
        print()
        if self.failures:
            print(f"FAIL  {self.name}")
            print(f"      claim: {self.claim}")
            for f in self.failures:
                print(f"      violated: {f}")
            return 1
        print(f"PASS  {self.name}")
        print(f"      claim held: {self.claim}")
        return 0


def run_probe(name: str, claim: str, coro_fn) -> None:
    print(f"==> {name}")
    print(f"    claim under test: {claim}")
    probe = Probe(name, claim)
    try:
        asyncio.run(coro_fn(probe))
    except Exception as exc:  # noqa: BLE001
        import traceback

        probe.failures.append(f"probe raised {type(exc).__name__}: {exc}")
        probe.observe(f"EXC  {type(exc).__name__}: {exc}")
        traceback.print_exc()
    sys.exit(probe.report())
