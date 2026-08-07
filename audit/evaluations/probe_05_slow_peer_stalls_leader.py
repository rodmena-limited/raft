"""Claim: one unresponsive peer does not stop the leader serving the others.

A peer that is reachable but wedged -- GC pause, disk stall, deadlocked event
loop, network blackhole -- is the most common real failure in a soft-realtime
cluster, and it is strictly harder than a crash. Raft tolerates it: the leader
should keep heartbeating the healthy majority.

broadcast_append_entries() awaits asyncio.gather() over ALL peers and no gRPC
call anywhere in the codebase carries a timeout or deadline. The probe stands
up a real gRPC Raft server whose handlers never return, makes it one of three
members, and measures whether the healthy follower keeps receiving heartbeats.

BLAST RADIUS: local only -- binds loopback ports and writes to temp dirs.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import grpc  # noqa: E402

from _harness import (  # noqa: E402
    HEARTBEAT_MS,
    HOST,
    free_port,
    make_node,
    probe_dir,
    run_probe,
    wait_for,
)
from raft.rpc.proto import raft_pb2_grpc  # noqa: E402


class WedgedRaftServicer(raft_pb2_grpc.RaftServicer):
    """A peer that completes the TCP+HTTP/2 handshake and then never answers.

    This is a live, wedged process -- not a mock of one. Every RPC hangs.
    """

    def __init__(self):
        self.received = 0

    async def _hang(self, request, context):
        self.received += 1
        await asyncio.Event().wait()  # never set

    RequestVote = _hang
    AppendEntries = _hang
    InstallSnapshot = _hang
    ClientWrite = _hang
    ChangeMembership = _hang


async def main(probe) -> None:
    wedged_port = free_port()
    wedged_addr = f"{HOST}:{wedged_port}"
    wedged = WedgedRaftServicer()
    wedged_server = grpc.aio.server(options=[("grpc.so_reuseport", 0)])
    raft_pb2_grpc.add_RaftServicer_to_server(wedged, wedged_server)
    wedged_server.add_insecure_port(wedged_addr)
    await wedged_server.start()
    probe.observe(f"wedged peer listening on {wedged_addr}: answers the handshake, never replies")

    ports = [free_port() for _ in range(2)]
    addrs = [f"{HOST}:{p}" for p in ports]
    members = addrs + [wedged_addr]
    nodes = [
        make_node(
            f"n{i}",
            [a for a in members if a != addrs[i]],
            addrs[i],
            probe_dir(f"slow_peer_n{i}"),
        )
        for i in range(2)
    ]
    for n in nodes:
        await n.start()
    probe.observe(
        f"3-member cluster: 2 healthy nodes + 1 wedged peer. "
        f"A majority (2 of 3) is alive and healthy, so the cluster must stay available."
    )

    elected = await wait_for(
        lambda: sum(1 for n in nodes if n.state.role.value == "leader") == 1, timeout=15.0
    )
    probe.expect(elected, "healthy majority elects a leader despite the wedged peer")
    if not elected:
        for n in nodes:
            probe.observe(
                f"  node {n.state.node_id}: role={n.state.role.value} "
                f"term={n.state.current_term} voted_for={n.state.voted_for}"
            )
        probe.observe(
            f"wedged peer absorbed {wedged.received} RequestVote/AppendEntries calls "
            f"that never returned"
        )
        probe.observe(
            "the election never completes: maybe_start_election() awaits asyncio.gather() "
            "over every peer with no RPC deadline, so a single unresponsive member blocks "
            "the vote count indefinitely and the node stays CANDIDATE forever"
        )
        for n in nodes:
            await n.stop()
        await wedged_server.stop(grace=None)
        return

    leader = next(n for n in nodes if n.state.role.value == "leader")
    follower = next(n for n in nodes if n is not leader)
    probe.observe(f"leader={leader.state.node_id} term={leader.state.current_term}")

    # Measure heartbeat liveness from the FOLLOWER's point of view: every
    # heartbeat resets its election deadline, so a moving deadline means
    # heartbeats are arriving.
    samples = []
    last_deadline = follower.state.election_deadline_ms
    last_change = time.monotonic()
    max_gap = 0.0
    start = time.monotonic()
    while time.monotonic() - start < 6.0:
        await asyncio.sleep(0.02)
        if follower.state.election_deadline_ms != last_deadline:
            gap = time.monotonic() - last_change
            samples.append(gap)
            max_gap = max(max_gap, gap)
            last_deadline = follower.state.election_deadline_ms
            last_change = time.monotonic()
    if not samples:
        max_gap = time.monotonic() - last_change

    probe.observe(
        f"over 6s the follower received {len(samples)} heartbeats "
        f"(heartbeat interval is configured at {HEARTBEAT_MS}ms, so ~120 were due)"
    )
    probe.observe(f"largest observed gap between heartbeats: {max_gap:.2f}s")
    probe.observe(f"wedged peer absorbed {wedged.received} RPCs that never completed")
    probe.observe(
        f"final state: leader={leader.state.node_id} role={leader.state.role.value} "
        f"term={leader.state.current_term}; follower role={follower.state.role.value} "
        f"term={follower.state.current_term}"
    )

    # Allow generous slack: 20x the configured heartbeat interval.
    tolerable = (HEARTBEAT_MS / 1000.0) * 20
    probe.expect(
        max_gap < tolerable,
        f"healthy follower keeps receiving heartbeats while one peer is wedged "
        f"(largest gap {max_gap:.2f}s, tolerance {tolerable:.2f}s)",
    )
    probe.expect(
        len(samples) > 20,
        f"leader sustained heartbeating to the healthy majority "
        f"(only {len(samples)} heartbeats in 6s)",
    )

    for n in nodes:
        await n.stop()
    await wedged_server.stop(grace=None)


run_probe(
    "slow-peer-stalls-leader",
    "one wedged peer does not stop the leader serving the healthy majority",
    main,
)
