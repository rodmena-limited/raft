"""Claim: a write acknowledged by the cluster survives a process restart.

This is the single most basic promise a consensus service makes. The probe
writes through the real ClientWrite RPC, waits for the entry to be committed
and applied, shuts the node down, then brings a fresh process-equivalent node
up on the SAME data directory and asks whether the value is still there.

The read-back deliberately goes through the state machine that the node
itself restored during construction -- the same object the service would use
to answer a client read -- not through the log file on disk.
"""

from __future__ import annotations

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
    data_dir = probe_dir("restart_replay")
    port = free_port()
    addr = f"{HOST}:{port}"

    # A single-node cluster is a legitimate Raft configuration: the node is its
    # own majority. It isolates durability from replication.
    node = make_node("n1", [], addr, data_dir)
    await node.start()
    probe.observe(f"node n1 started on {addr}, data dir {data_dir}")

    elected = await wait_for(lambda: node.state.role.value == "leader", timeout=5.0)
    probe.expect(elected, "single-node cluster elects itself leader")

    resp = await client_write(addr, kv_put("account-42", b"balance-1000"))
    probe.observe(f"ClientWrite -> accepted={resp.accepted} index={resp.index} term={resp.term}")
    probe.expect(resp.accepted, "leader accepts the client write")

    applied = await wait_for(
        lambda: node.state.sm.store.get("account-42") == b"balance-1000", timeout=5.0
    )
    probe.expect(applied, "write is applied to the state machine before restart")
    probe.observe(
        f"pre-restart: commit_index={node.state.commit_index} "
        f"last_applied={node.state.last_applied} "
        f"store={dict(node.state.sm.store)}"
    )

    await node.stop()
    probe.observe("node stopped (clean shutdown, no crash, no data dir change)")

    # Restart: a brand-new node object over the same durable directory. This is
    # exactly what happens when the service process is restarted or rescheduled.
    node2 = make_node("n1", [], f"{HOST}:{free_port()}", data_dir)
    probe.observe(
        f"post-restart: commit_index={node2.state.commit_index} "
        f"last_applied={node2.state.last_applied} "
        f"store={dict(node2.state.sm.store)}"
    )
    log_entries = node2.state.storage.read_entries(1)
    probe.observe(
        f"post-restart durable log holds {len(log_entries)} entries "
        f"(indices {[e.index for e in log_entries]}) -- the data IS on disk"
    )

    value = node2.state.sm.store.get("account-42")
    probe.expect(
        value == b"balance-1000",
        f"acknowledged write is readable after restart (got {value!r}, expected b'balance-1000')",
    )
    await node2.stop()


run_probe(
    "restart-replay",
    "a write acknowledged and applied before restart is still present after restart",
    main,
)
