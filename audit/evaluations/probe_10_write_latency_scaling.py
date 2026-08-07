"""Claim: write latency stays bounded as the log grows (soft-realtime fitness).

FsLogStorage keeps the whole log in one pickle file and rewrites ALL of it on
every append, truncate and compaction; every read (read_entries, first_index,
last_index_term, compaction_base) unpickles the whole file again. A single
client write therefore costs O(log size) in CPU and disk, making total cost
quadratic in the number of writes.

Two aggravating factors:

  * All of this file I/O -- including os.fsync -- runs synchronously on the
    asyncio event loop. While a write is being serialised, the node cannot
    send or answer heartbeats, so replication stalls in lockstep with it.
  * RaftState.maybe_snapshot() has no caller anywhere in src/. Nothing ever
    compacts the log, so it grows without bound for the life of the cluster
    and this cost only ever increases.

This probe measures end-to-end ClientWrite latency through the real RPC at
increasing log sizes and reports the trend.

BLAST RADIUS: local only -- one node, temp dir, a few thousand small writes.
"""

from __future__ import annotations

import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _harness import (  # noqa: E402
    HOST,
    REPO_ROOT,
    client_write,
    free_port,
    kv_put,
    make_node,
    probe_dir,
    run_probe,
    wait_for,
)

CHECKPOINTS = [100, 500, 1000, 2000, 4000]
SAMPLE = 25
# A soft-realtime consensus service should keep a single small write far below
# this; it is deliberately generous.
LATENCY_BUDGET_MS = 50.0


async def main(probe) -> None:
    # Static fact worth stating alongside the measurement.
    grep = subprocess.run(
        ["grep", "-rn", "maybe_snapshot", str(REPO_ROOT / "src")],
        capture_output=True,
        text=True,
    )
    callers = [ln for ln in grep.stdout.splitlines() if "def maybe_snapshot" not in ln]
    probe.observe(
        f"callers of maybe_snapshot() in src/: {len(callers)} "
        f"-- nothing in the library ever compacts the log"
    )

    data_dir = probe_dir("latency_scaling")
    addr = f"{HOST}:{free_port()}"
    node = make_node("n1", [], addr, data_dir)
    await node.start()
    elected = await wait_for(lambda: node.state.role.value == "leader", timeout=5.0)
    probe.expect(elected, "node is leader and accepting writes")

    results: list[tuple[int, float, float]] = []
    written = 0
    payload = kv_put("k", b"v" * 64)

    for target in CHECKPOINTS:
        while written < target:
            await client_write(addr, payload, timeout=60.0)
            written += 1

        samples = []
        for _ in range(SAMPLE):
            t0 = time.perf_counter()
            await client_write(addr, payload, timeout=60.0)
            samples.append((time.perf_counter() - t0) * 1000)
            written += 1
        median = statistics.median(samples)
        worst = max(samples)
        results.append((target, median, worst))
        probe.observe(
            f"log ~{target:>5} entries: median write {median:7.2f} ms, worst {worst:7.2f} ms"
        )

    log_bytes = (Path(data_dir) / "log.pkl").stat().st_size
    probe.observe(
        f"log.pkl is {log_bytes / 1024:.0f} KiB after {written} writes and is "
        f"fully rewritten on every single append"
    )

    first_median = results[0][1]
    last_median = results[-1][1]
    growth = last_median / first_median if first_median else float("inf")
    probe.observe(
        f"median latency grew {growth:.1f}x between {CHECKPOINTS[0]} and "
        f"{CHECKPOINTS[-1]} entries"
    )

    probe.expect(
        last_median < LATENCY_BUDGET_MS,
        f"median write latency stays under {LATENCY_BUDGET_MS:.0f} ms at "
        f"{CHECKPOINTS[-1]} entries (measured {last_median:.2f} ms)",
    )
    probe.expect(
        growth < 3.0,
        f"write latency does not scale with log size "
        f"(grew {growth:.1f}x over a {CHECKPOINTS[-1] // CHECKPOINTS[0]}x larger log)",
    )

    await node.stop()


run_probe(
    "write-latency-scaling",
    "write latency stays bounded and roughly flat as the log grows",
    main,
)
