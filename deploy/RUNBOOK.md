# consensus.rodmena.co.uk — operations runbook

**Distributed etcd 3.5.28 cluster, three members, three datacentres.**
Rebuilt 2026-08-21 (ticket #2). Replaced a three-container single-host cluster.

## Topology

| member | host | address | site | role |
|---|---|---|---|---|
| `etcd-vm2`   | vm-2 / bikeroom | 93.89.141.253  | London  | member + **public edge (nginx)** |
| `etcd-nano2` | pg-nano-02      | 51.91.248.208  | Gravelines FR | member |
| `etcd-nano4` | pg-nano-04      | 57.131.136.207 | Limburg DE | member |

Three genuine failure domains. Quorum is 2, so the cluster survives losing any
one member *including its entire site*.

**vm-1 (the workstation) is deliberately NOT a member.** It is a development
machine — containers churn, it is rebooted and reconfigured routinely. The old
deployment ran all three members there, which meant it survived a container
restart but not the thing it existed to survive.

Measured RTT: vm2↔nano2 4.5ms, vm2↔nano4 19ms, nano2↔nano4 10ms. Heartbeat is
100ms and election timeout 1000ms — over 5x the worst case, with the 10:1 ratio
etcd recommends.

## Layout on each member

```
/usr/local/etc/etcd/etcd.yml          member config (declarative)
/usr/local/etc/etcd/pki/              ca.crt, member.crt/key, etcd-client.crt/key
/usr/local/etc/etcd/credentials.env   root + app passwords (0600, vm-2 only)
/usr/local/etc/rc.d/etcd              service, daemon(8) supervised
/var/db/etcd/                         data directory (0700, user etcd)
```

Repo: `deploy/freebsd/` holds `etcd.yml.tmpl`, `etcd.rc`, `nginx-consensus.conf`,
`bootstrap-auth.sh`. `deploy/pki/make-certs.sh` regenerates the PKI.

## Everyday operations

```sh
service etcd status|start|stop|restart      # on any member
tail -f /var/log/etcd/*.log
```

`etcdctl` needs the CA and a credential. On vm-2:

```sh
. /usr/local/etc/etcd/credentials.env
export ETCDCTL_API=3
alias e='etcdctl --cacert=/usr/local/etc/etcd/pki/ca.crt \
  --endpoints=https://93.89.141.253:2379,https://51.91.248.208:2379,https://57.131.136.207:2379 \
  --user root:$ETCD_ROOT_PASSWORD'
e endpoint health --write-out=table
e endpoint status --write-out=table     # shows which member is leader
e member list --write-out=table
```

## Security model — and why each layer exists

1. **Peer port 2380: mutual TLS, `client-cert-auth: true`.** Non-negotiable. A
   Raft peer can propose and commit entries, so an unauthenticated peer port is
   a write path into the cluster.
2. **Client port 2379: TLS, `client-cert-auth: FALSE`.** This is deliberate, not
   a relaxation. etcd's HTTP/JSON gateway *cannot* accept a client certificate —
   it returns `HTTP 400: CommonName of client sending a request against gateway
   will be ignored and not used as expected`, because it has no way to map a
   cert CN to an etcd user. Requiring client certs there makes the public JSON
   API — the point of the service — unusable.
   **Trap:** supplying a client `trusted-ca-file` *implies* client-cert-auth
   regardless of the flag. The server keeps advertising "Acceptable client
   certificate CA names" and refuses certless clients. It must be omitted from
   the client section. The peer section keeps its `trusted-ca-file`.
3. **pf on every member:** 2379 and 2380 reachable only from the other two
   member IPs. Rules live at the end of `/etc/pf.conf`.
4. **nginx on vm-2 is the sole public ingress**, blocking operator-only
   endpoints (snapshot, defragment, member add/remove, auth disable) at the edge.
5. **etcd RBAC:** `root` (admin, operations only) and `app` (readwrite, no
   admin). Verified: `app` is refused admin operations.

## Replacing a member (no downtime)

Quorum is 2, so one member may be replaced while the other two serve.

```sh
# 1. remove the old member (from a SURVIVING member)
e member list                       # note the ID
e member remove <ID>

# 2. on the replacement host: install, deploy certs+config, then
#    set initial-cluster-state: existing  in /usr/local/etc/etcd/etcd.yml
#    and list ALL members including the new one
e member add etcd-<name> --peer-urls=https://<ip>:2380

# 3. start it
service etcd start
e endpoint health --write-out=table
```

Issue its certificate first with `deploy/pki/make-certs.sh` after adding the
host to the `MEMBERS` list, and add pf rules on all members for the new IP.

## Backup and restore

```sh
# snapshot (run ON a member; blocked at the public edge on purpose)
. /usr/local/etc/etcd/credentials.env
etcdctl --cacert=/usr/local/etc/etcd/pki/ca.crt --endpoints=https://127.0.0.1:2379 \
  --user root:$ETCD_ROOT_PASSWORD snapshot save /var/backups/etcd-$(date -u +%Y%m%dT%H%M%SZ).db

# verify it -- a snapshot nobody has restored is a guess
etcdutl snapshot status /var/backups/etcd-*.db --write-out=table
```

**Restore (quorum permanently lost).** Restore is a *cluster rebuild*: every
member is restored from the same snapshot with a NEW cluster token.

```sh
service etcd stop                      # on all three
etcdutl snapshot restore /path/snap.db \
  --name etcd-vm2 \
  --initial-cluster etcd-vm2=https://93.89.141.253:2380,etcd-nano2=https://51.91.248.208:2380,etcd-nano4=https://57.131.136.207:2380 \
  --initial-advertise-peer-urls https://93.89.141.253:2380 \
  --data-dir /var/db/etcd
# repeat per member with its own --name and --initial-advertise-peer-urls
# bump initial-cluster-token in etcd.yml, then start all three
```

## Certificate renewal

Member certs expire **825 days** from 2026-08-21. The CA expires in 10 years.

```sh
cd ~/develop/raft/deploy/pki
./make-certs.sh -f            # -f reissues; without it existing certs are kept
# copy member.crt/key + ca.crt to each host, then rolling restart one at a time,
# checking `e endpoint health` between each so quorum is never lost.
```

The public TLS cert for consensus.rodmena.co.uk is certbot/webroot on vm-2
(`/usr/local/www/acme`), renewed by the certbot timer there.

## Failure drill — how this was verified, and how to re-verify

```sh
e put /failtest/before v3members
service etcd stop                       # ON THE LEADER
e get /failtest/before                  # must still read
e put /failtest/during v2members        # must still WRITE -- quorum survived
e endpoint status --write-out=table     # a NEW leader must be elected
service etcd start                      # bring it back
# then read /failtest/during FROM THE RECOVERED MEMBER's own endpoint,
# and read a key that does not exist as a control
```

Done 2026-08-21: killed the leader, cluster kept serving reads and writes,
elected a new leader, and the recovered member reconciled the missed write.

## Auth tokens are JWTs — and this is load-bearing

`auth-token: jwt,...,sign-method=RS256,ttl=30m` in `etcd.yml`, with the keypair
at `/usr/local/etc/etcd/jwt/`.

**Do not omit it.** etcd silently falls back to *simple* tokens: a random prefix
plus a global counter, held in the **issuing member's memory**, unsigned, and
not verifiable by any other member. On a single-host cluster that is merely
fragile. Across three members behind an nginx failover it is a functional break
— a token minted by one member is rejected the moment a client is routed to
another, and every token dies when its issuer restarts.

This regression was introduced on the first distributed deploy (the JWT flags
were not carried over from the old compose file) and caught by an independent
tester, not by us. Verify after any config change:

```sh
# a JWT has THREE dot-separated segments; a simple token has two
TOK=$(curl -s -X POST https://consensus.rodmena.co.uk/v3/auth/authenticate \
  -H 'Content-Type: application/json' -d '{"name":"app","password":"..."}' \
  | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
echo "$TOK" | awk -F. '{print NF}'          # must be 3
# and the discriminating test: mint at one member, USE AT ANOTHER
```

## Gotchas that cost time

- `pkg search '^etcd'` returns **`etcd-1.0.1_3` — an ncurses CD player**
  (`audio/etcd`). The real package is `coreos-etcd35`.
- The FreeBSD package ships **binaries only, no rc.d script**. Ours is in
  `deploy/freebsd/etcd.rc`.
- Client `trusted-ca-file` silently overrides `client-cert-auth: false` (above).
- `HEAD /health` returns 405 from etcd — its handler is GET-only. nginx sets
  `proxy_method GET` on that location so HEAD gets the same status and headers
  with no body, which is what load balancers and uptime probes require.
- **Check DNS before believing an API failure.** A stale local resolver cache
  pointed at the OLD cluster produced "invalid user ID or password" while the
  same credential worked via etcdctl. Nothing was wrong with the deployment.

## Issuing a credential to a consuming service

Do **not** hand out the shared `app` user. It holds readwrite over the entire
keyspace (`[ , <open ended>`), so every service that gets it can read and
overwrite every other service's keys. Create a named, prefix-scoped user per
consumer instead.

    . /usr/local/etc/etcd/credentials.env
    PW=$(openssl rand -base64 30 | tr -d '/+=' | head -c 32)
    ctl role add <svc>
    ctl role grant-permission <svc> --prefix=true readwrite /<svc>/
    ctl user add <svc> --new-user-password="$PW"
    ctl user grant-role <svc> <svc>
    printf 'ETCD_<SVC>_PASSWORD=%s\n' "$PW" >> /usr/local/etc/etcd/credentials.env

Then refresh `/etc/secrets.enc` and the gist — the credentials file is inside
the bundle, and a password that exists only on one member is a password you lose
with that member.

**Consumers need no client certificate.** The public edge holds
`etcd-client.crt` and presents it upstream; over `https://consensus.rodmena.co.uk`
a service needs only its username and password. POST them to
`/v3/auth/authenticate` and send the returned token in `Authorization` — raw,
with no `Bearer ` prefix. Tokens are RS256 JWTs with a 30-minute TTL.

### Verifying a new credential

Scope is a claim until you try to break it, and both directions matter. Test
from **outside** the cluster, over the public edge — a check run on a member
with the client cert in hand proves nothing about what the consumer can do.

| check | expected |
|---|---|
| authenticate | 200, token issued |
| PUT + GET inside the prefix | 200, value round-trips |
| PUT `/app/secret`, `/consensus/config`, `/` | 403 |
| PUT `/<svc>X/...` (boundary, not string-prefix) | 403 |
| RANGE `/` → `\0` | 403 |
| any request with no token | 400 |
| authenticate with a wrong password | 400 |

The first two rows are not decoration. A credential that is simply broken
returns 403 to everything and would satisfy every refusal row on its own; the
positive rows are what make the negatives mean "scoped" rather than "dead".

### Issued

| user | prefix | consumer | issued |
|---|---|---|---|
| `app` | **whole keyspace** — legacy, do not issue again | shared | 2026-08-21 |
| `uptime` | `/uptime.systems/` | uptime.systems | 2026-08-22 |
| `uptime-staging` | `/uptime.systems-staging/` | uptime.systems (staging) | 2026-08-22 |

### Separate an environment's prefix, do not nest it

`/svc-staging/` beside `/svc/`, never `/svc/staging/`. Nesting puts staging
*inside* the production grant, so a staging bug writes production keys and the
separation buys nothing. Raised by uptime-service, and they were right.

The separation is a byte-range property, so check it rather than trusting the
names. etcd turns a prefix into `[prefix, prefix_with_last_byte+1)`:

    /uptime.systems/          -> [/uptime.systems/,         /uptime.systems0)
    /uptime.systems-staging/  -> [/uptime.systems-staging/, /uptime.systems-staging0)

`-` is 0x2D and `/` is 0x2F, so the staging range ends below where production
starts. Disjoint. Then **prove it in both directions** — each credential 403s on
the other's prefix for both read and write, and each 200s on its own.

Include a DELETE that returns `deleted=1`. A PUT that wrote nothing also returns
200, and against that, every 403 above proves only that the credential is dead.

uptime-service put the general form better than I did, so in their words: **an
operation that succeeds without changing anything is indistinguishable from one
that worked.** A search-and-replace matching nothing, a migration that no-ops, a
revocation against an empty list, a diagnostic that runs clean and answers a
different question than the one you asked — all return success. Assert the
EFFECT, not the return code.

### Client trap: integers come back as JSON STRINGS

`header.revision`, `mod_revision`, `create_revision`, lease ids and the
`deleted` count are int64, and protobuf-JSON encodes int64 as a **string**:

    r["header"]["revision"]      -> '26'   (str, not int)

Python raises `TypeError` on `'26' > 5`, which is loud and safe. **A language
that coerces gets it silently wrong** — in JavaScript `"9" > "26"` is `true`,
so a revision comparison can go backwards without erroring.

**Cast at the boundary, not at the point of use.** Casting before each
comparison puts the burden on every call site and fails the moment someone adds
one; parsing the response once, at the edge, makes a raw string unable to reach
a comparison at all. uptime-service's refinement, and it is the stronger rule.
Reported by them, confirmed here.

### A true check on a proxy is not a check on the thing

Found the hard way on 2026-08-22, by two agents independently, on the same
finding.

`git log --all -- .env` came back positive on a repo: a `.env` had been
committed in 2021 and was still reachable on `origin/main`. Five checks were run
on it — not tracked at HEAD, added at commit X, removed at commit Y, ancestor of
origin/main, repo private — and every one was **true**. It was escalated as a
credential leak.

Nobody read the file. It contained `AUTH_SECRET=123`.

The checks established *"a path named .env is in history"* and were then treated
as establishing *"a secret is in history"*. The filename was doing all the work.
This is worse than a check that cannot go red, because it goes red **accurately,
on a proxy** — and a true positive on the proxy feels exactly like confirmation
of the real thing.

    git show <sha>:<path>        # one command; read what you found

And when you add the control that would have caught this: **the control must
exercise the same code path as the real query, not merely a similar one.**
uptime-service's refinement, from the companion failure the same night — a
liveness scan reported a clean "not found" while its control returned 0, because
the two paths hashed the value differently (one let `cut` append a newline the
other had stripped). No hash could ever match, so "not found" was structurally
guaranteed regardless of the truth. A control built alongside the query rather
than through it proves nothing about the query.

Before escalating anything found by pattern, name, or path, open it. The same
applies to a grep for `password` in logs, a scan for key-shaped strings, or a
secret-scanner hit: **the finder tells you where to look, never what you found.**

### If a real secret is in history, rotate before you rewrite

uptime-service's rule, kept because it is right in general even though the case
above dissolved. A history rewrite does **not** rotate anything, and anyone who
cloned before it keeps the blob forever. Rewriting first buys the appearance of
a fix while the credential stays valid. Rotate, then decide whether the rewrite
is worth breaking every clone and fork — usually it is not.

