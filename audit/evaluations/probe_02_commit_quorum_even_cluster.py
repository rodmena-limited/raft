"""Claim: an entry is only committed once a MAJORITY of the cluster stores it.

Raft's commit rule requires strictly more than half the cluster. This probe
builds a 4-node cluster (majority = 3), elects a leader, then stops two
followers so that at most 2 of 4 nodes -- a minority -- can hold any new
entry. It then writes through the real ClientWrite RPC and asks whether the
leader advanced its commit index anyway.

Committing on a minority breaks Leader Completeness: the two stopped nodes
plus one survivor form a quorum that can elect a leader which has never seen
the entry, and that leader will overwrite it. Acknowledged data is lost.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _harness import (  # noqa: E402
    HOST,
    client_write,
    free_port,
    kv_put,
    make_node,
    probe_dir,
    run_probe,
    wait_for,
)

CLUSTER_SIZE = 4


async def main(probe) -> None:
    ports = [free_port() for _ in range(CLUSTER_SIZE)]
    addrs = [f"{HOST}:{p}" for p in ports]
    nodes = [
        make_node(
            f"n{i}",
            [a for j, a in enumerate(addrs) if j != i],
            addrs[i],
            probe_dir(f"quorum_even_n{i}"),
        )
        for i in range(CLUSTER_SIZE)
    ]
    for n in nodes:
        await n.start()

    majority = CLUSTER_SIZE // 2 + 1
    probe.observe(f"cluster of {CLUSTER_SIZE} nodes started; a majority is {majority} nodes")

    elected = await wait_for(
        lambda: sum(1 for n in nodes if n.state.role.value == "leader") == 1, timeout=10.0
    )
    probe.expect(elected, "cluster elects exactly one leader")
    leader = next(n for n in nodes if n.state.role.value == "leader")
    followers = [n for n in nodes if n is not leader]
    probe.observe(f"leader is {leader.state.node_id} at term {leader.state.current_term}")

    # Stop two followers. Now only the leader + 1 follower can store anything:
    # 2 of 4 nodes, which is NOT a majority.
    for f in followers[:2]:
        await f.stop()
    probe.observe(
        f"stopped followers {followers[0].state.node_id} and {followers[1].state.node_id}; "
        f"at most 2 of {CLUSTER_SIZE} nodes can now hold a new entry"
    )
    await asyncio.sleep(0.3)

    commit_before = leader.state.commit_index
    resp = await client_write(leader.bind, kv_put("minority-write", b"should-not-commit"))
    probe.observe(f"ClientWrite -> accepted={resp.accepted} index={resp.index}")

    await asyncio.sleep(0.5)
    commit_after = leader.state.commit_index
    probe.observe(f"leader commit_index: {commit_before} -> {commit_after}")

    # Count how many live nodes actually hold the entry at that index.
    holders = 0
    for n in nodes:
        entries = n.state.storage.read_entries(resp.index, resp.index + 1)
        has = bool(entries)
        if has:
            holders += 1
        probe.observe(
            f"  node {n.state.node_id}: holds index {resp.index}? {has} "
            f"(inspecting durable log directly -- forensic read, not a correctness claim)"
        )
    probe.observe(f"{holders} of {CLUSTER_SIZE} nodes hold index {resp.index}; majority is {majority}")

    committed = commit_after >= resp.index
    probe.expect(
        not (committed and holders < majority),
        f"entry {resp.index} is not committed while only {holders}/{CLUSTER_SIZE} nodes "
        f"hold it (majority {majority} required)",
    )

    for n in nodes:
        if n.state.role.value == "leader" or n in followers[2:]:
            await n.stop()


run_probe(
    "commit-quorum-even-cluster",
    "an entry is committed only after a majority of the cluster stores it",
    main,
)
