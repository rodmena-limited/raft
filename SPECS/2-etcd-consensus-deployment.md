# SPEC 2 — etcd consensus service at consensus.rodmena.co.uk

- **Ticket:** issuedb #2
- **Date:** 2026-08-07
- **Session:** unattended autonomous (operator out of office)
- **Supersedes:** the plan to serve `consensus.rodmena.co.uk` from `raft-py`
  (see SPEC 1 / ticket #1 — 11 of 11 claims violated)
- **Deployment:** `deploy/` · **Probes:** `deploy/tests/live_probe.py`

## EARS requirements

Each requirement maps to checks in `deploy/tests/live_probe.py`. All are
currently satisfied — 66/66 checks pass against the live service.

### Provision and reachability
- The consensus service shall be provided by etcd, reachable at
  `https://consensus.rodmena.co.uk`.
- The consensus service shall serve a publicly trusted TLS certificate covering
  `consensus.rodmena.co.uk`.
- If a client connects over plain HTTP, then the service shall redirect it to
  HTTPS.
- The etcd members shall not be reachable on the public IP; only the TLS edge
  shall be.

### Interfaces
- The consensus service shall expose the etcd v3 HTTP/JSON API under `/v3/`.
- The consensus service shall expose the native etcd gRPC API on port 443.
- A value written through one interface shall be readable through the other.

### Authentication
- While authentication is enabled, the consensus service shall reject any
  unauthenticated request to read or write a key.
- If a client presents an incorrect password, then the service shall refuse to
  issue a token.
- When a client presents valid credentials, the service shall issue a token that
  remains valid across a restart of any or all cluster members.
- Where an application holds an expired token, the service shall reject it so the
  application can re-authenticate and retry.

### Data operations
- The consensus service shall support put, range, prefix range, delete,
  transaction, lease and watch operations.
- When a transaction's compare clause fails, the service shall not apply the
  success clause.
- When a lease expires, the service shall delete every key attached to it.
- While an application sends lease keepalives, the service shall retain the keys
  attached to that lease beyond its TTL.
- When a key changes, the service shall deliver an event to every established
  watch on that key.

### Safety under failure
- While one of three members is unavailable, the consensus service shall continue
  to serve reads and writes.
- If a quorum of members is unavailable, then the consensus service shall refuse
  writes rather than accept them.
- When quorum is restored, the consensus service shall resume serving writes
  without operator intervention.
- Data committed before a member failure shall be readable after recovery.

### Edge protection
- If a request targets a full-database snapshot, a defragment, a cluster
  membership change, or disabling authentication, then the edge shall refuse it
  with 403, over both HTTP and gRPC, regardless of the credential presented.

### Documentation
- The consensus service shall serve `/llms.txt` describing how an application
  integrates with it.
- The consensus service shall serve `/docs` as human-readable documentation.
- The published documentation shall not contain any credential.
- The published documentation shall state that all members run on a single host
  and that the service does not survive loss of that host.

### Verification
- The deployment shall be verified through its own public interface, not by
  inspecting containers, volumes or configuration files.
- Every guard shall be exercised in both directions — blocking when it should and
  releasing when it should.
- Every assertion shall be demonstrated to pass on a known-positive case before
  being trusted to report a negative.

## Decisions recorded (made autonomously)

1. **etcd 3.5.17 over hardening raft-py.** Operator's instruction; the audit
   supports it. Reversible: raft-py remains in-tree.
2. **Three members on one host.** Gives a real quorum and tolerates member loss
   and rolling upgrades. Does not tolerate host loss — documented publicly rather
   than implied away. Reversible: move members to separate hosts and update
   `--initial-cluster`.
3. **JWT auth tokens (`--auth-token=jwt`, RS256, 30 min TTL).** The default
   `simple` tokens are per-member and in-memory, so a member restart 401s every
   client. Found by fault injection during deployment. Reversible: drop the flag
   and remove `/etc/consensus/jwt`.
4. **Destructive operations blocked at the edge** even for root, over HTTP and
   gRPC. Reversible: delete the `location` blocks in the nginx conf.
5. **Two users only — `root` (admin) and `app` (readwrite, no admin).**
   Per-application prefix-scoped users are the documented path for onboarding.
   Reversible: `etcdctl user`/`role` commands.
6. **Deployment lives in this repo** rather than a new one, because the domain
   and the concern are the same. Reversible: move `deploy/` out.

## Not covered by this spec

No off-host backup schedule. No monitoring or alerting integration. No sustained
load testing. No multi-tenant prefix isolation beyond the documented pattern.
