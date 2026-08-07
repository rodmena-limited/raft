"""Claim: ClientWrite returns accepted=True only for writes that are committed.

`accepted` is the only success signal the ClientWrite RPC gives a caller. If it
returns true for an entry that was merely appended to the leader's own log, the
caller believes durable consensus was reached when in fact a single un-replicated
copy exists -- and the next leader is entitled to overwrite it.

The probe elects a leader in a 3-node cluster, stops both followers so no
quorum is reachable, then writes. A correct implementation must not report
success: the entry cannot possibly be committed.
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


async def main(probe) -> None:
    ports = [free_port() for _ in range(3)]
    addrs = [f"{HOST}:{p}" for p in ports]
    nodes = [
        make_node(
            f"n{i}",
            [a for j, a in enumerate(addrs) if j != i],
            addrs[i],
            probe_dir(f"ack_uncommitted_n{i}"),
        )
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

    for f in followers:
        await f.stop()
    probe.observe(
        f"stopped both followers; leader {leader.state.node_id} is alone and "
        f"cannot reach a quorum of 2/3"
    )
    await asyncio.sleep(0.3)

    commit_before = leader.state.commit_index
    resp = await client_write(leader.bind, kv_put("no-quorum", b"data"))
    probe.observe(f"ClientWrite -> accepted={resp.accepted} index={resp.index} term={resp.term}")

    await asyncio.sleep(0.5)
    probe.observe(
        f"leader commit_index {commit_before} -> {leader.state.commit_index}; "
        f"entry index {resp.index} committed? {leader.state.commit_index >= resp.index}"
    )
    probe.observe(
        f"leader is still role={leader.state.role.value} despite having lost quorum "
        f"(no quorum-loss step-down)"
    )

    entry_committed = leader.state.commit_index >= resp.index
    probe.expect(
        not (resp.accepted and not entry_committed),
        f"ClientWrite did not report accepted=True for an entry that was never committed "
        f"(accepted={resp.accepted}, committed={entry_committed})",
    )

    await leader.stop()


run_probe(
    "client-write-acks-uncommitted",
    "ClientWrite reports accepted=True only once the entry is committed",
    main,
)
