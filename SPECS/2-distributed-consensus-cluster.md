# SPEC 2 — Distribute the etcd consensus cluster across real failure domains

Ticket: issuedb #2 (repo: raft) · Delivered 2026-08-21

## EARS requirements

- The consensus service shall run three etcd members on three physically distinct hosts.
- While any one member host is unavailable, the consensus service shall continue to accept reads and writes.
- When etcd peer traffic crosses a host boundary, the consensus service shall encrypt it with mutual TLS.
- When etcd client traffic crosses a host boundary, the consensus service shall encrypt it with TLS.
- If a peer certificate is not signed by the cluster CA, then the etcd member shall refuse the connection.
- The consensus service shall expose client ports only to the loopback interface and to the other cluster members, never to the public internet.
- Where a member host is in a different datacentre, the consensus service shall use heartbeat and election timeouts that tolerate the measured inter-host latency.
- The consensus deployment shall carry a runbook covering bootstrap, member replacement, backup, restore, and certificate renewal.
- When a member is replaced, the runbook shall describe the procedure without requiring cluster downtime.
- The consensus service shall retain etcd authentication (JWT RS256) and per-user roles.
- If quorum is lost, then the runbook shall describe recovery from an etcd snapshot.

## Verification

| requirement | how verified |
|---|---|
| three distinct hosts | vm-2 (London), pg-nano-02 (Gravelines), pg-nano-04 (Limburg) — `member list` |
| survives one member loss | **killed the leader**; cluster served reads AND writes on 2 members, elected a new leader; recovered member reconciled the missed write, read back from its own endpoint with a nonexistent-key control |
| peer mTLS | `peer-transport-security.client-cert-auth: true` with cluster CA |
| client TLS | TLS on 2379; see deviation below |
| ports not public | `pf` on all three: 2379/2380 only from the two peer IPs; verified in the loaded ruleset |
| latency-tolerant timings | measured RTT 4.5/10/19 ms vs 100 ms heartbeat, 1000 ms election |
| runbook | `deploy/RUNBOOK.md` |
| member replacement | documented, no-downtime procedure (quorum 2) |
| RBAC retained | `root` + `app`; `app` **refused** admin ops, anonymous access refused |
| snapshot restore | documented with `etcdutl snapshot restore` per member |

## Deviations from the spec, with reasons

**Client port does NOT use `client-cert-auth`.** etcd's HTTP/JSON gateway cannot
accept a client certificate — it returns `HTTP 400: CommonName of client sending
a request against gateway will be ignored and not used as expected`, having no
way to map a cert CN to an etcd user. Requiring client certs would make the
public JSON API unusable, which is the service's entire purpose. Compensating
controls: pf restricts 2379 to peers and loopback; nginx is the sole public
ingress and blocks operator endpoints; etcd RBAC requires credentials per
operation; traffic remains TLS-encrypted. **Peer** mTLS is unchanged and
non-negotiable.

**Related trap:** supplying a client `trusted-ca-file` *implies* client-cert-auth
regardless of the flag — the server keeps advertising "Acceptable client
certificate CA names" and refuses certless clients. It must be omitted from the
client section. Setting the flag alone was silently ineffective.

## Notes

- vm-1 (workstation) deliberately excluded: it is a development machine and the
  least suitable consensus member. Operator's call, and correct.
- The public nginx edge also moved to vm-2, so no part of the service depends on
  the workstation.
- `pkg search '^etcd'` on FreeBSD returns `etcd-1.0.1_3`, an **ncurses CD player**
  (`audio/etcd`). The real package is `coreos-etcd35`.
- The FreeBSD package ships binaries only; the rc.d service is ours
  (`deploy/freebsd/etcd.rc`), following the ledger/futex `daemon(8)` pattern.
