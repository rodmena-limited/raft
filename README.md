# raft — consensus for Rodmena

This repository holds two things:

1. **`deploy/`** — the live consensus service at
   <https://consensus.rodmena.co.uk>, a 3-member **etcd 3.5.17** cluster behind a
   TLS edge. This is what applications should use.
2. **`src/` and `audit/`** — `raft-py`, an in-house Raft implementation, and the
   mission-critical audit that concluded it must not carry traffic.

## Use the service

- **Integration guide (humans):** <https://consensus.rodmena.co.uk/docs>
- **Integration guide (agents/LLMs):** <https://consensus.rodmena.co.uk/llms.txt>
- **Health:** <https://consensus.rodmena.co.uk/health>

Two interfaces on one origin — the etcd v3 HTTP/JSON API under `/v3/`, and the
native etcd gRPC API on `:443`. Credentials are issued per application by
farshid@rodmena.co.uk. Deployment and operations: [`deploy/README.md`](deploy/README.md).

## Why the in-house implementation is not the provider

`raft-py` passed **134 tests at 96.45% coverage** with a green quality gate. An
audit then tested 11 safety, durability, availability and security claims against
a running cluster through its own gRPC interface. **All 11 failed**, ten of them
reproduced live:

| | Finding | Severity |
|---|---|---|
| 1 | Every restart silently discards all committed state — on a clean shutdown | critical |
| 2 | A 32-byte anonymous request permanently wedges the whole cluster's apply loop | critical |
| 3 | Unauthenticated remote code execution via `pickle.loads` on network payloads | critical |
| 4 | One unresponsive peer makes the cluster permanently leaderless | critical |
| 5 | A hard crash destroyed the log in 7 of 12 trials | critical |
| 6 | Even-sized clusters commit entries a majority never stored | critical |
| 7 | A duplicated AppendEntries deletes committed entries | critical |
| 8 | `ClientWrite` reports success for writes that were never committed | high |
| 9 | A follower >4 MiB behind can never catch up | high |
| 10 | Write latency scales with log size; nothing ever compacts | high |
| 11 | README advertised features that were stubs; no read path existed | medium |

Full report with live reproductions and smallest-correct fixes:
[`audit/AUDIT-2026-08-07.md`](audit/AUDIT-2026-08-07.md).

The lesson is kept deliberately: a green coverage gate measured the
implementation against its own assumptions. One bug even *passed* its first
probe because the live leader repaired the damage within one heartbeat — the
masking is in the write-up.

### Probe harnesses

Both are runnable. A `FAIL` is a finding, not a broken probe.

```bash
./audit/evaluations/run_all.sh          # falsifies raft-py's claims (all fail)
python deploy/tests/live_probe.py       # verifies the live etcd service (all pass)
```

The raft-py harness was validated by applying the minimal fix for finding 1 and
confirming its probe flips to PASS, then reverting — a probe that cannot go green
cannot go red.

## raft-py status

**Not maintained, not deployed, do not use.** It stays in-tree as the evidence
behind the decision and as a working example of falsification-driven auditing.
The defects are documented rather than fixed; anyone wanting to revive it should
start from the audit's smallest-correct-fix list, not from the current code.

```
src/raft/        the implementation as audited
tests/           134 passing tests that miss every defect above
audit/           the audit report and its probe harness
SPECS/           EARS specs for the audit and the deployment
```
