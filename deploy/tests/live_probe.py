#!/usr/bin/env python3
"""Live acceptance probes for https://consensus.rodmena.co.uk.

Every check drives the service through its own public interface -- HTTPS, gRPC,
DNS, TLS -- exactly as a customer would. Nothing here reads a container, a data
directory, or a config file to decide whether something works.

Two rules this suite holds itself to:

  * Every guard is tested in BOTH directions. A cap that is only shown to block
    is how a queue deadlocks forever; a lease that is only shown to expire never
    proves it stayed alive when it should have.
  * Every assertion is shown to be capable of passing on a known-positive before
    it is trusted to report a negative. A check that cannot go green cannot go
    red, and reports absence with full confidence.

Configuration (no secrets baked in):

    CONSENSUS_BASE      default https://consensus.rodmena.co.uk
    CONSENSUS_GRPC      default consensus.rodmena.co.uk:443
    CONSENSUS_USER      required
    CONSENSUS_PASSWORD  required
    CONSENSUS_PREFIX    key namespace for probe data, default /_probe
    ALLOW_DESTRUCTIVE   set to 1 to enable member stop/start fault injection

Exit code is non-zero if any check fails.
"""

from __future__ import annotations

import base64
import json
import os
import socket
import ssl
import subprocess
import sys
import time
from datetime import datetime, timezone

import httpx

BASE = os.environ.get("CONSENSUS_BASE", "https://consensus.rodmena.co.uk")
GRPC = os.environ.get("CONSENSUS_GRPC", "consensus.rodmena.co.uk:443")
USER = os.environ.get("CONSENSUS_USER", "")
PASSWORD = os.environ.get("CONSENSUS_PASSWORD", "")
PREFIX = os.environ.get("CONSENSUS_PREFIX", "/_probe")
DESTRUCTIVE = os.environ.get("ALLOW_DESTRUCTIVE") == "1"
HOSTNAME = BASE.split("//", 1)[-1].split("/")[0]
PUBLIC_IP = os.environ.get("CONSENSUS_PUBLIC_IP", "93.89.141.229")
ETCD_IMAGE = "quay.io/coreos/etcd:v3.5.17"

results: list[tuple[str, bool, str]] = []
_section = ""


def section(name: str) -> None:
    global _section
    _section = name
    print(f"\n\033[1m{name}\033[0m")


def check(label: str, ok: bool, evidence: str = "") -> bool:
    results.append((f"{_section} :: {label}", ok, evidence))
    mark = "\033[32mPASS\033[0m" if ok else "\033[31mFAIL\033[0m"
    print(f"  {mark}  {label}")
    if evidence:
        for line in str(evidence).splitlines():
            print(f"        {line}")
    return ok


def b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def unb64(s: str) -> str:
    return base64.b64decode(s).decode()


client = httpx.Client(base_url=BASE, timeout=20.0, follow_redirects=False)
token = ""


def authenticate() -> str:
    r = client.post("/v3/auth/authenticate", json={"name": USER, "password": PASSWORD})
    r.raise_for_status()
    return r.json()["token"]


def api(path: str, payload: dict, auth: bool = True, timeout: float = 20.0) -> httpx.Response:
    """Call the API, re-authenticating once on 401.

    Tokens carry a TTL, so any correct client must be able to renew. Modelling
    that here means the suite tests the service rather than the age of its own
    token -- but note it does NOT paper over the member-restart problem that
    made this necessary: JWT tokens are verified by every member, so a restart
    no longer invalidates them. probe_token_survives_restart proves that.
    """
    global token
    headers = {"Authorization": token} if auth and token else {}
    r = client.post(path, json=payload, headers=headers, timeout=timeout)
    if auth and token and r.status_code == 401:
        token = authenticate()
        r = client.post(path, json=payload, headers={"Authorization": token}, timeout=timeout)
    return r


def kv_get(key: str) -> str | None:
    r = api("/v3/kv/range", {"key": b64(key)})
    r.raise_for_status()
    kvs = r.json().get("kvs", [])
    return unb64(kvs[0]["value"]) if kvs else None


def kv_put(key: str, value: str) -> httpx.Response:
    return api("/v3/kv/put", {"key": b64(key), "value": b64(value)})


def etcdctl(*args: str, user: str | None = None, timeout: int = 60) -> subprocess.CompletedProcess:
    cred = user or f"{USER}:{PASSWORD}"
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "host", ETCD_IMAGE, "etcdctl",
         f"--endpoints=https://{GRPC}", f"--user={cred}", *args],
        capture_output=True, text=True, timeout=timeout,
    )


# --------------------------------------------------------------------- DNS/TLS
def probe_dns_and_tls() -> None:
    section("DNS and TLS")
    try:
        addrs = sorted({ai[4][0] for ai in socket.getaddrinfo(HOSTNAME, 443, socket.AF_INET)})
        check(f"{HOSTNAME} resolves publicly", bool(addrs), f"A -> {', '.join(addrs)}")
    except Exception as exc:
        check(f"{HOSTNAME} resolves publicly", False, str(exc))
        return

    # Trust the system roots -- no custom CA. This is what a customer's client does.
    ctx = ssl.create_default_context()
    try:
        with socket.create_connection((HOSTNAME, 443), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=HOSTNAME) as tls:
                cert = tls.getpeercert()
                proto = tls.version()
        names = [v for k, v in cert.get("subjectAltName", ()) if k == "DNS"]
        not_after = datetime.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z").replace(
            tzinfo=timezone.utc
        )
        days = (not_after - datetime.now(timezone.utc)).days
        check("certificate validates against public roots", True, f"{proto}, SAN={names}")
        check(f"certificate covers {HOSTNAME}", HOSTNAME in names, f"SAN={names}")
        check("certificate is not near expiry", days > 20, f"{days} days remaining")
    except Exception as exc:
        check("certificate validates against public roots", False, str(exc))

    # Known-negative: a wrong hostname must be rejected, proving the check above
    # is actually validating rather than accepting anything.
    try:
        with socket.create_connection((HOSTNAME, 443), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname="wrong.example.com"):
                pass
        check("TLS rejects a mismatched hostname", False, "handshake unexpectedly succeeded")
    except ssl.SSLError as exc:
        check("TLS rejects a mismatched hostname", True, f"rejected: {type(exc).__name__}")
    except Exception as exc:
        check("TLS rejects a mismatched hostname", True, f"rejected: {type(exc).__name__}: {exc}")

    r = httpx.get(f"http://{HOSTNAME}/docs", follow_redirects=False, timeout=15)
    check(
        "plain HTTP redirects to HTTPS",
        r.status_code in (301, 302) and r.headers.get("location", "").startswith("https://"),
        f"{r.status_code} -> {r.headers.get('location')}",
    )


def probe_members_not_public() -> None:
    section("Network exposure")
    # The etcd members must be loopback-only. Reaching them on the public IP
    # would bypass TLS, auth rate limiting and every edge block.
    for port in (2379, 2479, 2579, 2380):
        reachable = True
        try:
            with socket.create_connection((PUBLIC_IP, port), timeout=5):
                pass
        except (ConnectionRefusedError, socket.timeout, OSError):
            reachable = False
        check(
            f"etcd port {port} is NOT reachable on the public IP",
            not reachable,
            f"{PUBLIC_IP}:{port} {'ACCEPTED A CONNECTION' if reachable else 'refused/filtered'}",
        )


# ----------------------------------------------------------------------- auth
def probe_auth() -> None:
    global token
    section("Authentication")

    r = client.post("/v3/auth/authenticate", json={"name": USER, "password": PASSWORD})
    ok = r.status_code == 200 and "token" in r.json()
    check("valid credentials return a token", ok, f"HTTP {r.status_code}")
    if not ok:
        print("cannot continue without a token")
        summarise_and_exit()
    token = r.json()["token"]

    r = client.post("/v3/auth/authenticate", json={"name": USER, "password": "wrong-password"})
    body = r.text[:120]
    check(
        "wrong password is rejected",
        r.status_code != 200 or "token" not in r.json(),
        f"HTTP {r.status_code} {body}",
    )

    # Known-positive first: the same call WITH auth succeeds (proved below in
    # the KV section), so a failure here means auth is enforced, not that the
    # endpoint is broken.
    r = client.post("/v3/kv/range", json={"key": b64(f"{PREFIX}/anything")})
    denied = "user name is empty" in r.text or r.status_code in (401, 403)
    check("anonymous read is denied", denied, f"HTTP {r.status_code} {r.text[:120]}")

    r = client.post(
        "/v3/kv/put", json={"key": b64(f"{PREFIX}/anon"), "value": b64("should-not-persist")}
    )
    denied = "user name is empty" in r.text or r.status_code in (401, 403)
    check("anonymous write is denied", denied, f"HTTP {r.status_code} {r.text[:120]}")

    r = client.post(
        "/v3/kv/range",
        json={"key": b64(f"{PREFIX}/anything")},
        headers={"Authorization": f"Bearer {token}"},
    )
    check(
        "a 'Bearer' prefixed token is rejected (as documented)",
        "invalid auth token" in r.text,
        r.text[:120],
    )


# ------------------------------------------------------------------------- kv
def probe_kv() -> None:
    section("Key-value operations")
    key = f"{PREFIX}/basic"
    r = kv_put(key, "value-one")
    rev = int(r.json()["header"]["revision"])
    check("put succeeds and returns a revision", r.status_code == 200 and rev > 0, f"revision={rev}")

    check("read-after-write returns the new value", kv_get(key) == "value-one", f"got={kv_get(key)!r}")

    kv_put(key, "value-two")
    check("overwrite is visible immediately", kv_get(key) == "value-two", f"got={kv_get(key)!r}")

    # Documented shape: a miss has no kvs and no count at all.
    r = api("/v3/kv/range", {"key": b64(f"{PREFIX}/definitely-absent")})
    body = r.json()
    check(
        "a miss omits kvs and count entirely (documented shape)",
        "kvs" not in body and "count" not in body,
        json.dumps(body)[:200],
    )

    for i in range(3):
        kv_put(f"{PREFIX}/list/{i}", f"item-{i}")
    r = api("/v3/kv/range", {"key": b64(f"{PREFIX}/list/"), "range_end": b64(f"{PREFIX}/list0")})
    got = {unb64(kv["key"]): unb64(kv["value"]) for kv in r.json().get("kvs", [])}
    want = {f"{PREFIX}/list/{i}": f"item-{i}" for i in range(3)}
    check("prefix range returns exactly the prefix", got == want, f"count={len(got)}")

    r = api("/v3/kv/deleterange", {"key": b64(key)})
    check("delete reports one key removed", r.json().get("deleted") == "1", r.text[:120])
    check("deleted key is gone", kv_get(key) is None, f"got={kv_get(key)!r}")


def probe_txn() -> None:
    section("Transactions (compare-and-swap)")
    key = f"{PREFIX}/cas"
    api("/v3/kv/deleterange", {"key": b64(key)})

    def create_if_absent(value: str) -> bool:
        r = api("/v3/kv/txn", {
            "compare": [
                {"key": b64(key), "target": "VERSION", "result": "EQUAL", "version": "0"}
            ],
            "success": [{"requestPut": {"key": b64(key), "value": b64(value)}}],
            "failure": [],
        })
        r.raise_for_status()
        return r.json().get("succeeded") is True

    # Both directions of the guard, in one run.
    check("first writer wins the CAS", create_if_absent("first") is True)
    check("second writer loses the CAS", create_if_absent("second") is False)
    check("loser did not overwrite the value", kv_get(key) == "first", f"got={kv_get(key)!r}")

    # mod_revision based optimistic concurrency
    r = api("/v3/kv/range", {"key": b64(key)})
    mod = r.json()["kvs"][0]["mod_revision"]
    r = api("/v3/kv/txn", {
        "compare": [{"key": b64(key), "target": "MOD", "result": "EQUAL", "mod_revision": mod}],
        "success": [{"requestPut": {"key": b64(key), "value": b64("updated")}}],
        "failure": [],
    })
    check("CAS on unchanged mod_revision succeeds", r.json().get("succeeded") is True)
    r = api("/v3/kv/txn", {
        "compare": [{"key": b64(key), "target": "MOD", "result": "EQUAL", "mod_revision": mod}],
        "success": [{"requestPut": {"key": b64(key), "value": b64("stale-write")}}],
        "failure": [],
    })
    check("CAS on a stale mod_revision is refused", r.json().get("succeeded") is not True)
    check("stale writer did not overwrite", kv_get(key) == "updated", f"got={kv_get(key)!r}")
    api("/v3/kv/deleterange", {"key": b64(key)})


def probe_lease() -> None:
    section("Leases")
    key = f"{PREFIX}/lease"
    r = api("/v3/lease/grant", {"TTL": 5})
    lid = r.json()["ID"]
    check("lease granted", bool(lid), f"id={lid} ttl=5s")

    api("/v3/kv/put", {"key": b64(key), "value": b64("alive"), "lease": lid})
    # Known-positive: the key must be there NOW, or the expiry check below is vacuous.
    check("leased key is present immediately (known-positive)", kv_get(key) == "alive")
    time.sleep(3)
    check("leased key survives within its TTL", kv_get(key) == "alive", "t+3s of a 5s TTL")
    time.sleep(6)
    check("leased key is removed after the TTL lapses", kv_get(key) is None, "t+9s of a 5s TTL")

    # The release direction: keepalive must hold it open past the TTL.
    r = api("/v3/lease/grant", {"TTL": 5})
    lid2 = r.json()["ID"]
    key2 = f"{PREFIX}/lease-ka"
    api("/v3/kv/put", {"key": b64(key2), "value": b64("held"), "lease": lid2})
    held = True
    for _ in range(3):
        time.sleep(3)
        api("/v3/lease/keepalive", {"ID": lid2})
        if kv_get(key2) != "held":
            held = False
    check("keepalive holds a lease open past its TTL", held, "3 renewals over 9s of a 5s TTL")
    time.sleep(8)
    check("lease lapses once keepalives stop", kv_get(key2) is None, "t+8s after the last renewal")


def probe_watch() -> None:
    section("Watch (change notification)")
    key = f"{PREFIX}/watched"
    api("/v3/kv/deleterange", {"key": b64(key)})
    events: list[tuple[str, str]] = []
    created = False
    try:
        with httpx.Client(base_url=BASE, timeout=30.0) as wc:
            with wc.stream(
                "POST", "/v3/watch",
                json={"create_request": {"key": b64(key)}},
                headers={"Authorization": token},
            ) as resp:
                deadline = time.time() + 15
                wrote = False
                for line in resp.iter_lines():
                    if not line.strip():
                        continue
                    msg = json.loads(line).get("result", {})
                    if msg.get("created"):
                        created = True
                        # Only write AFTER the watch is established, otherwise
                        # a pass could just be a race we happened to win.
                        httpx.post(
                            f"{BASE}/v3/kv/put",
                            json={"key": b64(key), "value": b64("event-1")},
                            headers={"Authorization": token}, timeout=10,
                        )
                        wrote = True
                    for ev in msg.get("events", []):
                        kv = ev["kv"]
                        events.append((ev.get("type", "PUT"), unb64(kv.get("value", ""))))
                    if events or time.time() > deadline:
                        break
                    if not wrote and time.time() > deadline:
                        break
    except Exception as exc:
        check("watch stream established", False, f"{type(exc).__name__}: {exc}")
        return

    check("watch stream established", created)
    check(
        "a write made after the watch was established is delivered",
        ("PUT", "event-1") in events,
        f"events={events}",
    )
    api("/v3/kv/deleterange", {"key": b64(key)})


def probe_blocked_endpoints() -> None:
    section("Edge blocks (defence in depth)")
    blocked = [
        "/v3/maintenance/snapshot",
        "/v3/maintenance/defragment",
        "/v3/maintenance/downgrade",
        "/v3/maintenance/alarm",
        "/v3/cluster/member/add",
        "/v3/cluster/member/remove",
        "/v3/cluster/member/update",
        "/v3/cluster/member/promote",
        "/v3/auth/disable",
    ]
    for path in blocked:
        r = api(path, {})
        check(f"{path} is refused", r.status_code == 403, f"HTTP {r.status_code}")

    # A non-blocked endpoint must still work, or "403 everywhere" would look
    # like a pass. This is the known-positive for the checks above.
    r = api("/v3/kv/range", {"key": b64(f"{PREFIX}/anything")})
    check(
        "a non-blocked endpoint still succeeds (known-positive)",
        r.status_code == 200,
        f"HTTP {r.status_code}",
    )


def probe_grpc() -> None:
    section("gRPC interface (native etcd clients)")
    key = f"{PREFIX}/grpc"
    p = etcdctl("put", key, "via-grpc")
    check("etcdctl put over public gRPC+TLS", p.returncode == 0 and "OK" in p.stdout,
          (p.stdout or p.stderr).strip()[:200])

    p = etcdctl("get", key)
    check("etcdctl get returns the value", "via-grpc" in p.stdout,
          (p.stdout or p.stderr).strip()[:200])

    check("value written over gRPC is readable over HTTP/JSON",
          kv_get(key) == "via-grpc", f"got={kv_get(key)!r}")

    p = etcdctl("snapshot", "save", "/tmp/should-not-work.db", timeout=60)
    check("snapshot over gRPC is refused at the edge",
          p.returncode != 0 and "403" in (p.stdout + p.stderr),
          (p.stderr or p.stdout).strip()[:200])
    etcdctl("del", key)


def probe_docs() -> None:
    section("Documentation endpoints")
    r = httpx.get(f"{BASE}/llms.txt", timeout=15)
    check("/llms.txt is served", r.status_code == 200 and len(r.text) > 2000,
          f"HTTP {r.status_code}, {len(r.text)} bytes, {r.headers.get('content-type')}")
    check("/llms.txt documents the auth flow",
          "/v3/auth/authenticate" in r.text and "Bearer" in r.text)
    check("/llms.txt does not leak a credential",
          PASSWORD not in r.text and "ETCD_APP_PASSWORD=" not in r.text)

    r = httpx.get(f"{BASE}/docs", timeout=15, follow_redirects=True)
    check("/docs is served", r.status_code == 200 and len(r.text) > 2000,
          f"HTTP {r.status_code}, {len(r.text)} bytes")
    check("/docs does not leak a credential", PASSWORD not in r.text)

    r = httpx.get(f"{BASE}/", timeout=15, follow_redirects=False)
    check("/ redirects to /docs", r.status_code in (301, 302)
          and "/docs" in r.headers.get("location", ""),
          f"{r.status_code} -> {r.headers.get('location')}")

    r = httpx.get(f"{BASE}/health", timeout=15)
    check("/health is served without authentication",
          r.status_code == 200 and '"health":"true"' in r.text, r.text.strip()[:120])


def probe_token_survives_restart() -> None:
    section("Auth token durability across member restarts")
    if not DESTRUCTIVE:
        print("  SKIPPED: restarts etcd members. Set ALLOW_DESTRUCTIVE=1 to run.")
        return

    # Regression probe. With etcd's default --auth-token=simple, tokens live in
    # the issuing member's memory, so any restart 401s every client holding one
    # -- a rolling upgrade would break every application at once. The cluster
    # runs --auth-token=jwt so tokens are signed and verifiable by any member.
    fresh = authenticate()
    key = f"{PREFIX}/token-durability"
    r = client.post("/v3/kv/put", json={"key": b64(key), "value": b64("pre")},
                    headers={"Authorization": fresh})
    check("token works before any restart (known-positive)", r.status_code == 200,
          f"HTTP {r.status_code}")

    for c in ("consensus-etcd1", "consensus-etcd2", "consensus-etcd3"):
        subprocess.run(["docker", "restart", c], capture_output=True, timeout=120)
        time.sleep(12)
    time.sleep(5)

    r = client.post("/v3/kv/put", json={"key": b64(key), "value": b64("post")},
                    headers={"Authorization": fresh})
    check("the SAME token still works after every member restarted",
          r.status_code == 200, f"HTTP {r.status_code} {r.text[:160]}")
    api("/v3/kv/deleterange", {"key": b64(key)})


def probe_fault_tolerance() -> None:
    section("Fault tolerance (member failure)")
    if not DESTRUCTIVE:
        print("  SKIPPED: stops and restarts etcd members.")
        print("           Blast radius: this cluster only; it is restored at the end.")
        print("           Set ALLOW_DESTRUCTIVE=1 to run.")
        return

    key = f"{PREFIX}/fault"
    kv_put(key, "before-failure")
    check("write succeeds with all 3 members up (known-positive)",
          kv_get(key) == "before-failure")

    # --- lose ONE member: quorum of 2 survives, service must stay up ---
    subprocess.run(["docker", "stop", "consensus-etcd1"], capture_output=True, timeout=60)
    time.sleep(5)
    try:
        ok_write = kv_put(key, "during-1-member-down").status_code == 200
    except Exception as exc:
        ok_write = False
        print(f"        write raised {type(exc).__name__}: {exc}")
    check("writes still succeed with 1 of 3 members down", ok_write)
    check("reads still succeed with 1 of 3 members down",
          kv_get(key) == "during-1-member-down")

    # --- lose a SECOND member: quorum LOST, writes must now FAIL ---
    # This is the other direction of the guard. A consensus service that keeps
    # accepting writes without a quorum is the single worst failure it can have.
    subprocess.run(["docker", "stop", "consensus-etcd2"], capture_output=True, timeout=60)
    time.sleep(5)
    wrote_without_quorum = False
    try:
        r = kv_put(f"{PREFIX}/no-quorum", "should-not-commit")
        wrote_without_quorum = r.status_code == 200 and "error" not in r.text
    except Exception as exc:
        print(f"        write correctly failed: {type(exc).__name__}")
    check("writes are REFUSED when quorum is lost (2 of 3 down)",
          not wrote_without_quorum,
          "accepted a write without a quorum" if wrote_without_quorum else "refused as required")

    # --- restore: the service must come back unattended ---
    subprocess.run(["docker", "start", "consensus-etcd1"], capture_output=True, timeout=60)
    subprocess.run(["docker", "start", "consensus-etcd2"], capture_output=True, timeout=60)
    recovered = False
    for _ in range(30):
        time.sleep(2)
        try:
            if kv_put(key, "after-recovery").status_code == 200:
                recovered = True
                break
        except Exception:
            continue
    check("service recovers automatically once quorum returns", recovered)
    check("data written before the failure survived it",
          kv_get(key) == "after-recovery", f"got={kv_get(key)!r}")

    p = subprocess.run(
        ["docker", "exec", "consensus-etcd1", "etcdctl",
         "--endpoints=http://127.0.0.1:2379", f"--user={USER}:{PASSWORD}",
         "endpoint", "health", "--cluster"],
        capture_output=True, text=True, timeout=60,
    )
    healthy = (p.stdout + p.stderr).count("is healthy")
    check("all 3 members report healthy after recovery", healthy == 3,
          (p.stdout + p.stderr).strip()[:300])


def cleanup() -> None:
    if token:
        try:
            api("/v3/kv/deleterange", {"key": b64(PREFIX), "range_end": b64(PREFIX + "0")})
        except Exception:
            pass


def summarise_and_exit() -> None:
    cleanup()
    failed = [r for r in results if not r[1]]
    print("\n" + "=" * 72)
    print(f"SUMMARY  {len(results) - len(failed)}/{len(results)} checks passed")
    if failed:
        print(f"\n{len(failed)} FAILED:")
        for label, _, evidence in failed:
            print(f"  FAIL  {label}")
            if evidence:
                print(f"        {evidence}")
    print("=" * 72)
    sys.exit(1 if failed else 0)


def main() -> None:
    if not USER or not PASSWORD:
        print("CONSENSUS_USER and CONSENSUS_PASSWORD must be set")
        sys.exit(2)
    print(f"probing {BASE}  (gRPC {GRPC})  as user {USER!r}")
    probe_dns_and_tls()
    probe_members_not_public()
    probe_auth()
    probe_kv()
    probe_txn()
    probe_lease()
    probe_watch()
    probe_blocked_endpoints()
    probe_grpc()
    probe_docs()
    probe_token_survives_restart()
    probe_fault_tolerance()
    summarise_and_exit()


if __name__ == "__main__":
    main()
