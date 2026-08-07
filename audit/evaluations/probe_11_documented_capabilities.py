"""Claim: the capabilities advertised in README.md actually work.

README.md describes the project as "Production-grade Raft consensus" and lists
"Joint consensus membership changes" as a feature. A claim in the README is a
claim an operator will deploy against, so it belongs in the claim inventory.

This probe drives the advertised surface through the real gRPC service.

BLAST RADIUS: local only -- one node on loopback, temp dir.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _harness import (  # noqa: E402
    HOST,
    free_port,
    make_node,
    probe_dir,
    rpc_client,
    run_probe,
    wait_for,
)
from raft.rpc.proto import raft_pb2  # noqa: E402


async def main(probe) -> None:
    addr = f"{HOST}:{free_port()}"
    node = make_node("n1", [], addr, probe_dir("documented"))
    await node.start()
    elected = await wait_for(lambda: node.state.role.value == "leader", timeout=5.0)
    probe.expect(elected, "node is leader")

    stub, channel = rpc_client(addr)
    try:
        resp = await stub.ChangeMembership(
            raft_pb2.MembershipChangeRequest(node_id="n2", add=True)
        )
        probe.observe(
            f"ChangeMembership(add n2) -> accepted={resp.accepted} message={resp.message!r}"
        )
        probe.expect(
            resp.accepted,
            'README lists "Joint consensus membership changes" as a feature; '
            "the ChangeMembership RPC accepts a membership change",
        )
    finally:
        await channel.close()

    # Is there any way for a client to READ committed state?
    rpc_names = [m for m in dir(raft_pb2_grpc_stub_methods()) if not m.startswith("_")]
    probe.observe(f"RPCs exposed by the service: {', '.join(rpc_names)}")
    probe.expect(
        any("read" in m.lower() or "get" in m.lower() for m in rpc_names),
        "the service exposes some way for a client to read committed state",
    )
    probe.observe(
        "KeyValueStateMachine.apply() returns a value for a 'get' op, but "
        "RaftState.apply_entries() discards every return value, so even a "
        "read submitted through the log could not be answered"
    )

    await node.stop()


def raft_pb2_grpc_stub_methods():
    from raft.rpc.proto import raft_pb2_grpc

    class _Probe:
        pass

    inst = _Probe()
    for name in dir(raft_pb2_grpc.RaftServicer):
        if not name.startswith("_"):
            setattr(inst, name, None)
    return inst


run_probe(
    "documented-capabilities",
    "the capabilities advertised in README.md work through the real service",
    main,
)
