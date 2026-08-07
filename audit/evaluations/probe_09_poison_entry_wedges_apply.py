"""Claim: one bad command cannot stop the cluster applying later commands.

RaftState.apply_entries() calls sm.apply() with no error handling, and
KeyValueStateMachine.apply() raises ValueError on an unrecognised op. The
raising entry is already committed and replicated, so every node re-reads it
from last_applied+1 on every subsequent apply attempt and raises again.
last_applied can never advance past it.

The result is a poison pill: a single unauthenticated ClientWrite of a few
dozen bytes permanently stops every node in the cluster from applying anything
ever again. The log keeps growing and entries keep committing, so the cluster
looks healthy from the outside while no write is ever observable.

BLAST RADIUS: local only -- loopback ports and temp dirs.
"""

from __future__ import annotations

import asyncio
import pickle
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
            probe_dir(f"poison_n{i}"),
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

    # 1. Baseline: a normal write is applied everywhere.
    await client_write(leader.bind, kv_put("before", b"ok"))
    ok = await wait_for(
        lambda: all(n.state.sm.store.get("before") == b"ok" for n in nodes), timeout=5.0
    )
    probe.expect(ok, "a normal write is applied on all three nodes (baseline)")
    probe.observe(f"last_applied per node: {[n.state.last_applied for n in nodes]}")

    # 2. The poison pill: valid pickle, unrecognised op. No credentials used.
    poison = pickle.dumps(("delete", "before"))
    probe.observe(f"submitting an anonymous {len(poison)}-byte payload with an unknown op")
    try:
        resp = await client_write(leader.bind, poison)
        probe.observe(f"ClientWrite -> accepted={resp.accepted} index={resp.index}")
    except Exception as exc:  # noqa: BLE001
        probe.observe(f"ClientWrite raised {type(exc).__name__} (entry is already in the log)")

    await asyncio.sleep(0.5)
    probe.observe(f"after poison, last_applied per node: {[n.state.last_applied for n in nodes]}")
    probe.observe(
        f"after poison, commit_index per node: {[n.state.commit_index for n in nodes]} "
        f"(log indices on leader: {[e.index for e in leader.state.storage.read_entries(1)]})"
    )

    # 3. A perfectly valid write submitted afterwards.
    leader_now = next((n for n in nodes if n.state.role.value == "leader"), leader)
    try:
        resp = await client_write(leader_now.bind, kv_put("after", b"should-be-visible"))
        probe.observe(f"post-poison ClientWrite -> accepted={resp.accepted} index={resp.index}")
    except Exception as exc:  # noqa: BLE001
        probe.observe(f"post-poison ClientWrite raised {type(exc).__name__}")

    recovered = await wait_for(
        lambda: any(n.state.sm.store.get("after") == b"should-be-visible" for n in nodes),
        timeout=5.0,
    )
    probe.observe(
        f"nodes holding the post-poison value: "
        f"{[n.state.node_id for n in nodes if n.state.sm.store.get('after')]}"
    )
    probe.observe(f"final last_applied per node: {[n.state.last_applied for n in nodes]}")

    probe.expect(
        recovered,
        "a valid write submitted after a malformed one is still applied somewhere "
        "(cluster is not permanently wedged by one bad command)",
    )

    for n in nodes:
        await n.stop()


run_probe(
    "poison-entry-wedges-apply",
    "one malformed command does not permanently stop the cluster applying later commands",
    main,
)
