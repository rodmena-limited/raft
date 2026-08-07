# consensus.rodmena.co.uk — deployment

The consensus service is a **3-member etcd 3.5.17 cluster** behind an nginx TLS
edge on this host.

## Why etcd and not the raft-py library in this repo

`raft-py` (in `src/`) was written to be the provider. A mission-critical audit —
`audit/AUDIT-2026-08-07.md`, ticket #1 — tested 11 safety, durability,
availability and security claims against it and **all 11 failed**, despite the
library passing 134 tests at 96% coverage. Four defects each caused silent,
unrecoverable data loss. Rather than harden an unproven implementation into
existence, the service runs etcd, whose Raft implementation is widely deployed
and independently verified.

`src/` and `audit/` stay in the repository as the record of that decision and as
a runnable demonstration of why "green test suite" and "correct consensus" are
different things.

## Layout

```
deploy/
  docker-compose.yml                      3 etcd members, loopback-bound
  nginx/consensus.rodmena.co.uk.conf      TLS edge, routing, edge blocks
  scripts/bootstrap-auth.sh               RBAC bootstrap (idempotent)
  www/llms.txt                            machine-readable integration guide
  www/docs/index.html                     human integration guide
  tests/live_probe.py                     live acceptance probes
```

Reference copies live here; the installed copies are at
`/etc/nginx/conf.d/` and `/var/www/consensus/`. Change the repo copy, then copy
it out — never edit the installed file directly.

## Architecture

```
internet ──TLS──> nginx :443 ──┬── /v3/…            → etcd HTTP/JSON gateway
                               ├── /etcdserverpb.…  → etcd gRPC
                               ├── /llms.txt, /docs → static
                               └── blocked paths    → 403

                  etcd1 :2379   etcd2 :2479   etcd3 :2579   (127.0.0.1 only)
                        └───────── raft peers, docker network ─────────┘
```

Members publish only to `127.0.0.1`. Peer traffic never leaves the Docker
network. nginx is the sole public route, verified by the probe suite attempting
to reach every etcd port on the public IP.

## Two design decisions worth knowing

**JWT auth tokens, not the default.** etcd's default `--auth-token=simple` keeps
tokens in the issuing member's memory, so restarting a member returns 401 to
every client holding one — a rolling upgrade would log out every application at
once. This was found by fault injection during deployment, not by reading docs.
The cluster runs `--auth-token=jwt` with an RS256 keypair at
`/etc/consensus/jwt/`, so any member can verify any token.
`probe_token_survives_restart` is the regression test.

**Destructive operations are blocked at the edge.** `snapshot`, `defragment`,
cluster membership changes and `auth disable` return 403 over both HTTP and gRPC
even with valid root credentials. A full-database snapshot over the public
internet is total data exfiltration in a single request. Run those on the host.

## Operating

```bash
# status
docker compose ps
docker exec consensus-etcd1 etcdctl --endpoints=http://127.0.0.1:2379 \
  --user root:$ETCD_ROOT_PASSWORD endpoint status --cluster -w table

# restart a single member (the cluster keeps serving)
docker restart consensus-etcd1

# backup -- blocked over the public API by design, run it here
docker exec consensus-etcd1 etcdctl --endpoints=http://127.0.0.1:2379 \
  --user root:$ETCD_ROOT_PASSWORD snapshot save /etcd-data/backup.db

# apply a config change
docker compose up -d --force-recreate

# edge changes
sudo cp nginx/consensus.rodmena.co.uk.conf /etc/nginx/conf.d/
sudo nginx -t && sudo systemctl reload nginx

# docs changes
sudo cp www/llms.txt /var/www/consensus/llms.txt
sudo cp www/docs/index.html /var/www/consensus/docs/index.html
sudo chown -R www-data:www-data /var/www/consensus
```

Credentials live in `/etc/consensus/credentials.env` (root-only, 0600) and are
generated on first run of `scripts/bootstrap-auth.sh`. They are not in this
repository and not in the published docs.

## Testing

The probe suite drives the **public** interface only — HTTPS, gRPC, DNS, TLS. It
never inspects a container or data directory to decide whether something works.

```bash
source /etc/consensus/credentials.env
CONSENSUS_USER=app CONSENSUS_PASSWORD=$ETCD_APP_PASSWORD \
  python deploy/tests/live_probe.py

# adds member stop/start fault injection; restores the cluster afterwards
ALLOW_DESTRUCTIVE=1 ... python deploy/tests/live_probe.py
```

Two rules the suite holds itself to, both learned the hard way in this repo's
audit:

- **Every guard is tested in both directions.** Quorum loss must refuse writes
  *and* the cluster must resume when quorum returns. A lease must expire *and*
  keepalive must hold it open.
- **Every assertion is shown to pass on a known-positive first.** A check that
  cannot go green cannot go red — it reports absence with full confidence.

## Known limitations

**All three members run on one host.** The cluster tolerates losing any single
member and keeps serving reads and writes; it does not survive loss of the host.
This is stated plainly in the public docs rather than implied away. Making it
host-redundant means moving members onto separate machines and re-pointing
`--initial-cluster` — the compose file is the only thing that changes.

There are no automated off-host backups. `snapshot save` is available on the
host; scheduling it somewhere durable is not yet done.
