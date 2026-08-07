"""Claim: a node whose process is killed mid-write can still start afterwards.

FsLogStorage._write_log and store_metadata both open the destination with
mode "wb", which truncates the real file in place, and only then serialise
into it. There is no write-to-temp + fsync + atomic rename. Between the open
and the fsync the on-disk file is a partial pickle, so a crash in that window
leaves the log or the metadata permanently unreadable.

fsync makes the write DURABLE but not ATOMIC -- those are different
properties, and only the second one protects against a torn write. This probe
kills a real process with SIGKILL while it is appending, then asks the storage
layer to load what is on disk.

The metadata file matters even more than the log: it holds current_term and
voted_for. A node that loses them can vote twice in the same term, which
breaks Election Safety and can produce two leaders.

BLAST RADIUS: local only -- spawns child processes writing to a temp dir.
"""

from __future__ import annotations

import os
import random
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _harness import REPO_ROOT, probe_dir, run_probe  # noqa: E402

from raft.storage import FsLogStorage  # noqa: E402

WRITER = r"""
import sys, os
sys.path.insert(0, %r)
from raft.storage import FsLogStorage, LogEntryRecord, LogMetadata
d = sys.argv[1]
s = FsLogStorage(d)
# Build a log big enough that each rewrite takes real time, widening the
# window in which the file on disk is a partial pickle.
batch = [LogEntryRecord(index=i, term=1, data=b"x" * 512) for i in range(1, 4001)]
s.append_entries(batch)
i = 4001
sys.stdout.write("ready\n"); sys.stdout.flush()
while True:
    s.append_entries([LogEntryRecord(index=i, term=1, data=b"y" * 512)])
    s.store_metadata(LogMetadata(term=1, voted_for="n1", commit_index=i, last_applied=i))
    i += 1
"""


async def main(probe) -> None:
    if os.environ.get("AUDIT_ALLOW_DESTRUCTIVE") != "1":
        print(
            "    | SKIPPED: this probe sends SIGKILL to processes it spawns.\n"
            "    |          Blast radius: its own child processes and a temp dir; it\n"
            "    |          never touches an existing cluster. Set AUDIT_ALLOW_DESTRUCTIVE=1\n"
            "    |          to run it."
        )
        sys.exit(0)

    attempts = 12
    corrupt_log = 0
    corrupt_meta = 0
    survived = 0
    examples: list[str] = []

    src = str(REPO_ROOT / "src")
    for attempt in range(attempts):
        d = probe_dir(f"crash_write_{attempt}")
        proc = subprocess.Popen(
            [sys.executable, "-c", WRITER % src, d],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        # Wait until the writer is past its initial bulk load and looping.
        if proc.stdout is not None:
            proc.stdout.readline()
        time.sleep(random.uniform(0.05, 0.35))
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()

        # Now do exactly what a restarting node does: construct the storage
        # layer over the surviving directory and read it.
        log_ok, meta_ok = True, True
        try:
            storage = FsLogStorage(d)
            storage.read_entries(1)
            storage.last_index_term()
        except Exception as exc:  # noqa: BLE001
            log_ok = False
            if len(examples) < 3:
                examples.append(f"log load failed: {type(exc).__name__}: {exc}")
        try:
            FsLogStorage(d).load_metadata()
        except Exception as exc:  # noqa: BLE001
            meta_ok = False
            if len(examples) < 3:
                examples.append(f"metadata load failed: {type(exc).__name__}: {exc}")

        if not log_ok:
            corrupt_log += 1
        if not meta_ok:
            corrupt_meta += 1
        if log_ok and meta_ok:
            survived += 1

    probe.observe(f"{attempts} processes killed with SIGKILL while appending")
    probe.observe(f"  restarted cleanly:            {survived}")
    probe.observe(f"  log.pkl unreadable:           {corrupt_log}")
    probe.observe(f"  meta.pkl unreadable:          {corrupt_meta}")
    for e in examples:
        probe.observe(f"  example -> {e}")

    probe.expect(
        corrupt_log == 0,
        f"log survives a mid-write SIGKILL ({corrupt_log}/{attempts} left an unreadable log)",
    )
    probe.expect(
        corrupt_meta == 0,
        f"persisted term/vote survives a mid-write SIGKILL "
        f"({corrupt_meta}/{attempts} left unreadable metadata)",
    )


run_probe(
    "crash-during-log-write",
    "a node killed mid-write can still load its log and its persisted term/vote",
    main,
)
