# SPEC 1 — Mission-critical audit of the raft implementation

- **Ticket:** issuedb #1
- **Date:** 2026-08-07
- **Context:** `raft-py` is intended to back `consensus.rodmena.co.uk` as a Raft
  provider for critical soft-realtime systems.
- **Report:** `audit/AUDIT-2026-08-07.md`
- **Harness:** `audit/evaluations/` (`run_all.sh`)

## EARS requirements — the audit process

- The auditor shall compile every guarantee stated by the code, the README and
  the test suite into a falsifiable claim, and shall attempt to disprove each one.
- When a candidate defect is identified by code reading, the auditor shall
  reproduce it live through the product's own gRPC interface before reporting it
  as CONFIRMED; unreproduced candidates shall be reported as SUSPECTED.
- The auditor shall persist every live reproduction as a probe under
  `audit/evaluations/` with a `run_all.sh` runner.
- Before a probe is used as evidence of a defect, the auditor shall demonstrate
  that the probe can report PASS on a known-good case; a probe that cannot go
  green shall not be used to claim a defect.
- The audit report shall rank findings by customer harm, shall state what was
  exercised, what was not tested, and what remains uncertain, and shall not claim
  production readiness.

## EARS requirements — the properties under test

These are the claims the probes assert. Each is currently VIOLATED; each maps to
one probe, which is the regression test for its fix.

- **Durability.** When a node restarts, the raft node shall expose every command
  that was committed before the restart. *(probe_01 — VIOLATED)*
- **Commit quorum.** The raft node shall advance `commit_index` to an index only
  once a strict majority of cluster members stores an entry at that index.
  *(probe_02 — VIOLATED)*
- **Log matching.** If an AppendEntries request is duplicated or reordered, then
  the receiving node shall not delete any entry that does not conflict with the
  incoming entries, and shall never hold a `commit_index` greater than its own
  last log index. *(probe_03 — VIOLATED)*
- **Acknowledgement semantics.** The raft node shall report `accepted=true` from
  ClientWrite only for commands that have been committed; while a leader cannot
  reach a quorum, it shall not report success. *(probe_04 — VIOLATED)*
- **Partial-failure liveness.** While one cluster member is reachable but
  unresponsive, the raft cluster shall continue to elect a leader and to
  replicate to the healthy majority. *(probe_05 — VIOLATED)*
- **Crash atomicity.** If a node's process is killed during a durable write, then
  on restart the node shall load its log and its persisted term/vote without
  error. *(probe_06 — VIOLATED)*
- **Transport security.** The raft node shall authenticate every RPC peer and
  client, and shall not execute code contained in a command payload.
  *(probe_07 — VIOLATED)*
- **Catch-up.** When a follower's backlog exceeds the transport message limit,
  the leader shall still bring that follower up to date. *(probe_08 — VIOLATED)*
- **Apply isolation.** If a committed command raises when applied, then the raft
  node shall isolate that failure and continue applying subsequent commands.
  *(probe_09 — VIOLATED)*
- **Bounded latency.** The raft node shall keep command latency bounded and
  independent of accumulated log size, and shall compact its log.
  *(probe_10 — VIOLATED)*
- **Honest documentation.** The raft node shall implement every capability its
  README advertises, and shall expose a way to read committed state.
  *(probe_11 — VIOLATED)*
