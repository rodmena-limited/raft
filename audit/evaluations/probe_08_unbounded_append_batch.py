"""Claim: a follower that has fallen behind can always catch up.

broadcast_append_entries() calls read_entries(progress.next_index) with no end
bound, so a lagging follower is sent its ENTIRE backlog in a single
AppendEntries message. gRPC enforces a 4 MiB default receive limit, so once the
backlog crosses that threshold every attempt is rejected with
RESOURCE_EXHAUSTED. The exception is swallowed by a broad `except Exception`
that only logs, next_index never advances, and the leader retries the same
oversized message forever.

That is a permanent, self-inflicted wedge: the follower can never rejoin, and
because it never rejoins the cluster runs at reduced redundancy indefinitely
while reporting no error to any client.

BLAST RADIUS: local only -- loopback ports and temp dirs, ~6 MiB written.
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

ENTRY_BYTES = 128 * 1024
ENTRY_COUNT = 48  # ~6 MiB, comfortably past gRPC's 4 MiB default


async def main(probe) -> None:
    ports = [free_port() for _ in range(3)]
    addrs = [f"{HOST}:{p}" for p in ports]
    dirs = [probe_dir(f"big_batch_n{i}") for i in range(3)]
    nodes = [
        make_node(f"n{i}", [a for j, a in enumerate(addrs) if j != i], addrs[i], dirs[i])
        for i in range(3)
    ]
    for n in nodes:
        await n.start()

    elected = await wait_for(
        lambda: sum(1 for n in nodes if n.state.role.value == "leader") == 1, timeout=10.0
    )
    probe.expect(elected, "cluster elects exactly one leader")
    leader = next(n for n in nodes if n.state.role.value == "leader")
    followers = [n for n in nodes if n is not leader]
    lagging, healthy = followers[0], followers[1]
    lag_idx = nodes.index(lagging)

    await lagging.stop()
    probe.observe(
        f"stopped follower {lagging.state.node_id}; it will now miss "
        f"{ENTRY_COUNT} entries of {ENTRY_BYTES // 1024} KiB"
    )
    await asyncio.sleep(0.3)

    payload = b"z" * ENTRY_BYTES
    for i in range(ENTRY_COUNT):
        await client_write(leader.bind, kv_put(f"bulk{i}", payload), timeout=20.0)
    last_index = leader.state.last_log_index_term()[0]
    backlog_mib = (ENTRY_COUNT * ENTRY_BYTES) / (1024 * 1024)
    probe.observe(
        f"leader log now reaches index {last_index}; backlog for the stopped follower "
        f"is ~{backlog_mib:.1f} MiB (gRPC default receive limit is 4 MiB)"
    )
    probe.observe(
        f"healthy follower {healthy.state.node_id} is at commit_index="
        f"{healthy.state.commit_index}"
    )

    # Bring the follower back on the same address and data directory, exactly
    # as a restarted pod would return.
    revived = make_node(
        f"n{lag_idx}",
        [a for j, a in enumerate(addrs) if j != lag_idx],
        addrs[lag_idx],
        dirs[lag_idx],
    )
    await revived.start()
    probe.observe(f"restarted {revived.state.node_id} on {addrs[lag_idx]}; waiting 20s to catch up")

    caught_up = await wait_for(
        lambda: revived.state.last_log_index_term()[0] >= last_index, timeout=20.0
    )
    got = revived.state.last_log_index_term()[0]
    probe.observe(
        f"after 20s the restarted follower's log reaches index {got} of {last_index} "
        f"(commit_index={revived.state.commit_index})"
    )

    probe.expect(
        caught_up,
        f"a follower with a >4 MiB backlog catches up (reached index {got}/{last_index})",
    )

    await revived.stop()
    for n in nodes:
        if n is not lagging:
            await n.stop()


run_probe(
    "unbounded-append-batch",
    "a follower with a large backlog can still catch up with the leader",
    main,
)
