"""Claim: a follower never deletes a COMMITTED entry from its log.

Raft (section 5.3) says a follower removes existing entries only when they
CONFLICT with the incoming ones -- same index, different term. handle_append_entries
instead truncates unconditionally at the first incoming index, so any
AppendEntries carrying an older, shorter batch deletes everything after it,
committed or not.

This needs no attacker. The leader itself emits overlapping AppendEntries
concurrently: broadcast_append_entries() is invoked both by the heartbeat loop
and by client_write() with no mutual exclusion, and gRPC gives no ordering
guarantee between two in-flight calls on separate channels. A duplicated or
reordered retransmission is an ordinary network event.

This probe reproduces that event deterministically by replaying an earlier,
smaller AppendEntries to a follower through the real RPC interface. The term
and log positions are read from the live leader so the replayed message is
byte-for-byte something the leader genuinely sent.
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
    rpc_client,
    run_probe,
    wait_for,
)
from raft.rpc.proto import raft_pb2  # noqa: E402


async def main(probe) -> None:
    ports = [free_port() for _ in range(3)]
    addrs = [f"{HOST}:{p}" for p in ports]
    nodes = [
        make_node(
            f"n{i}",
            [a for j, a in enumerate(addrs) if j != i],
            addrs[i],
            probe_dir(f"stale_append_n{i}"),
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
    follower = next(n for n in nodes if n is not leader)

    for i in range(1, 4):
        resp = await client_write(leader.bind, kv_put(f"key{i}", f"value{i}".encode()))
        probe.observe(f"ClientWrite key{i} -> accepted={resp.accepted} index={resp.index}")

    committed = await wait_for(lambda: follower.state.commit_index >= 3, timeout=5.0)
    probe.expect(committed, "follower reports all 3 entries committed")
    probe.observe(
        f"follower {follower.state.node_id}: commit_index={follower.state.commit_index} "
        f"log indices={[e.index for e in follower.state.storage.read_entries(1)]}"
    )

    # Rebuild an AppendEntries the leader genuinely sent earlier in this term:
    # the one that carried entry 2 with entry 1 as its predecessor. Term and
    # entry contents are taken from the live leader so nothing is fabricated.
    term = leader.state.current_term
    entry1 = leader.state.storage.read_entries(1, 2)[0]
    entry2 = leader.state.storage.read_entries(2, 3)[0]

    # Stop the leader BEFORE delivering the duplicate. Otherwise the next
    # heartbeat (50ms away) silently re-replicates whatever was truncated and
    # the damage is invisible -- which is exactly why this defect survived the
    # existing test suite. Modelling the leader crashing while a duplicate is
    # still in flight removes the repair path and shows the durable damage.
    await leader.stop()
    probe.observe(
        "leader stopped before delivering the duplicate, so nothing can re-replicate "
        "a truncated entry (models a leader crash with an RPC still in flight)"
    )
    replay = raft_pb2.AppendEntriesRequest(
        term=term,
        leader_id=leader.state.node_id,
        prev_log_index=1,
        prev_log_term=entry1.term,
        entries=[raft_pb2.LogEntry(index=entry2.index, term=entry2.term, data=entry2.data)],
        leader_commit=1,
    )
    probe.observe(
        f"replaying an earlier AppendEntries (term={term}, prev_log_index=1, entries=[index 2]) "
        f"-- a duplicate of a message this leader already sent"
    )

    stub, channel = rpc_client(follower.bind)
    try:
        resp = await asyncio.wait_for(stub.AppendEntries(replay), 5.0)
        probe.observe(f"follower response: success={resp.success} match_index={resp.match_index}")
    finally:
        await channel.close()

    surviving = [e.index for e in follower.state.storage.read_entries(1)]
    probe.observe(
        f"follower log immediately after replay: indices={surviving} "
        f"(commit_index is still {follower.state.commit_index})"
    )

    probe.expect(
        3 in surviving,
        f"committed entry 3 still present in the follower's log after a duplicate "
        f"AppendEntries (log now holds {surviving})",
    )
    probe.expect(
        follower.state.commit_index <= max(surviving, default=0),
        f"follower does not claim commit_index={follower.state.commit_index} while its "
        f"log only reaches index {max(surviving, default=0)}",
    )

    for n in nodes:
        if n is not leader:
            await n.stop()


run_probe(
    "stale-append-truncates-committed",
    "a follower never deletes a committed entry when handling a duplicated AppendEntries",
    main,
)
