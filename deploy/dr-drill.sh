#!/bin/sh
# etcd disaster-recovery drill.
#
# WHY THIS EXISTS: deploy/RUNBOOK.md documents a snapshot restore that nobody
# had ever executed. A restore runbook nobody has run is not a runbook, it is a
# hypothesis. mail-api discovered theirs was non-functional in three independent
# ways by attempting it; ours had the same standing and the same excuse.
#
# PHASE 1 (this script, no arguments) touches NO production data, needs NO
# credential, and contacts NO live member. It builds a synthetic etcd, snapshots
# it, and demands three answers:
#
#     a good snapshot restores and reconciles      -> the drill can say YES
#     a corrupted snapshot is REFUSED              -> the drill can say NO
#     a truncated snapshot is REFUSED              -> not just checksum luck
#
# A drill that has only ever been run against a good snapshot is the same
# hypothesis in nicer clothing. If it cannot say no, its yes means nothing.
#
# PHASE 2 -- restoring a real snapshot from the live cluster -- is deliberately
# NOT here. Phase 1 establishes that the MECHANISM works. It says nothing about
# whether a production snapshot restores, which is a different claim.
#
# Ports 2479/2480 and a scratch data dir, so the live member on 2379/2380 is
# never touched. Processes are killed BY PID, never by name.
set -u

ETCD=${ETCD:-/usr/local/bin/etcd}
ETCDCTL=${ETCDCTL:-/usr/local/bin/etcdctl}
ETCDUTL=${ETCDUTL:-/usr/local/bin/etcdutl}
WORK=$(mktemp -d /tmp/etcd-drill.XXXXXX)
CPORT=2479
PPORT=2480
PID=""
pass=0; fail=0

ok(){ printf '  PASS  %s\n' "$1"; pass=$((pass+1)); }
no(){ printf '  FAIL  %s -- %s\n' "$1" "$2"; fail=$((fail+1)); }

cleanup(){
    [ -n "$PID" ] && kill "$PID" 2>/dev/null   # BY PID. never pkill.
    sleep 1
    rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

ctl(){ "$ETCDCTL" --endpoints=http://127.0.0.1:$CPORT "$@"; }

start_etcd(){   # $1 = data-dir, $2 = name
    "$ETCD" --name "$2" --data-dir "$1" \
        --listen-client-urls http://127.0.0.1:$CPORT \
        --advertise-client-urls http://127.0.0.1:$CPORT \
        --listen-peer-urls http://127.0.0.1:$PPORT \
        --initial-advertise-peer-urls http://127.0.0.1:$PPORT \
        --initial-cluster "$2=http://127.0.0.1:$PPORT" \
        --initial-cluster-token drill-$$ \
        --log-level error >"$WORK/etcd.log" 2>&1 &
    PID=$!
    n=0
    while [ $n -lt 40 ]; do
        ctl endpoint health >/dev/null 2>&1 && return 0
        n=$((n+1)); sleep 0.25
    done
    return 1
}
stop_etcd(){ [ -n "$PID" ] && kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; PID=""; }

echo "== etcd DR drill, phase 1 (synthetic; no production data, no credential) =="
echo "   work=$WORK"
echo

# ---------------------------------------------------- PREFLIGHT: drift + skew
# Two things this script can be WRONG about while every leg still passes.
#
# 1. RUNBOOK DRIFT. Leg 4 exercises the three-member command "as documented".
#    If RUNBOOK.md is edited and this script is not, Leg 4 keeps passing against
#    a form nobody documents any more, and the drill quietly validates a
#    procedure no operator will ever copy. mail-api's runbook had drifted to a
#    glob that matched none of their backups; ours can drift the same way.
#    So: assert the runbook still contains the flags this script actually runs,
#    and FAIL rather than skip if it does not.
#
# 2. VERSION SKEW. A green drill on one etcd version says nothing about a
#    cluster running another. mail-api spent an hour on exactly this -- a
#    pg_dump 17 file against a PostgreSQL 15 server -- and their drill detected
#    it without DIAGNOSING it, so the failure read as a bad backup. Print the
#    versions, and refuse to run if the three binaries disagree with each other.
RUNBOOK=${RUNBOOK:-$(dirname "$0")/RUNBOOK.md}
if [ -f "$RUNBOOK" ]; then
    missing=""
    for tok in "etcdutl snapshot restore" "--initial-cluster" "--initial-advertise-peer-urls" "--data-dir" "--name"; do
        grep -q -- "$tok" "$RUNBOOK" || missing="$missing '$tok'"
    done
    [ -z "$missing" ] \
        && ok "PREFLIGHT -- RUNBOOK.md still documents the form Leg 4 exercises" \
        || no "PREFLIGHT drift" "RUNBOOK.md no longer contains:$missing -- Leg 4 is testing a procedure nobody documents"
else
    no "PREFLIGHT drift" "RUNBOOK.md not found at $RUNBOOK -- cannot confirm the drill matches the documentation"
fi

EV=$("$ETCD" --version 2>/dev/null | sed -n 's/^etcd Version: //p' | head -1)
UV=$("$ETCDUTL" version 2>/dev/null | sed -n 's/^etcdutl version: //p' | head -1)
CV=$("$ETCDCTL" version 2>/dev/null | sed -n 's/^etcdctl version: //p' | head -1)
printf '  etcd=%s  etcdutl=%s  etcdctl=%s\n' "${EV:-?}" "${UV:-?}" "${CV:-?}"
if [ "$EV" = "$UV" ] && [ "$UV" = "$CV" ] && [ -n "$EV" ]; then
    ok "PREFLIGHT -- all three binaries are $EV; this run is evidence ONLY for that version"
else
    no "PREFLIGHT skew" "etcd=$EV etcdutl=$UV etcdctl=$CV -- a mixed-version run diagnoses nothing"
fi
echo



# ---------------------------------------------------------------- build
if start_etcd "$WORK/orig" drill-orig; then
    ok "synthetic single-node etcd is up on 127.0.0.1:$CPORT"
else
    no "synthetic etcd" "did not become healthy; see $WORK/etcd.log"; exit 1
fi

N=137
i=0
while [ $i -lt $N ]; do ctl put "/drill/key-$i" "value-$i" >/dev/null 2>&1; i=$((i+1)); done
COUNT_BEFORE=$(ctl get /drill/ --prefix --keys-only 2>/dev/null | grep -c '^/drill/')
[ "$COUNT_BEFORE" -eq "$N" ] \
    && ok "wrote $N keys and read back $COUNT_BEFORE (the positive control for everything below)" \
    || no "seed" "wrote $N, read back $COUNT_BEFORE"

ctl snapshot save "$WORK/good.db" >/dev/null 2>&1
[ -s "$WORK/good.db" ] && ok "snapshot saved ($(wc -c <"$WORK/good.db" | tr -d ' ') bytes)" \
                       || { no "snapshot save" "no file produced"; exit 1; }

"$ETCDUTL" snapshot status "$WORK/good.db" >/dev/null 2>&1 \
    && ok "etcdutl accepts the good snapshot" \
    || no "snapshot status" "etcdutl rejected a snapshot it had just written"

stop_etcd

# ------------------------------------------------- LEG 1: can it say YES?
"$ETCDUTL" snapshot restore "$WORK/good.db" --data-dir "$WORK/restored" >"$WORK/restore.log" 2>&1
if [ $? -eq 0 ] && [ -d "$WORK/restored" ]; then
    if start_etcd "$WORK/restored" drill-orig; then
        COUNT_AFTER=$(ctl get /drill/ --prefix --keys-only 2>/dev/null | grep -c '^/drill/')
        SPOT=$(ctl get /drill/key-42 --print-value-only 2>/dev/null)
        stop_etcd
        if [ "$COUNT_AFTER" -eq "$N" ] && [ "$SPOT" = "value-42" ]; then
            ok "LEG 1 -- a good snapshot restores and reconciles ($COUNT_AFTER/$N, spot value intact): THE DRILL CAN SAY YES"
        else
            no "LEG 1" "restored $COUNT_AFTER/$N keys, spot='$SPOT'"
        fi
    else
        no "LEG 1" "restored data dir would not start"
    fi
else
    no "LEG 1" "restore of a good snapshot failed: $(head -2 "$WORK/restore.log" | tr '\n' ' ')"
fi

# ------------------------------------------ LEG 2: can it say NO (corruption)?
cp "$WORK/good.db" "$WORK/corrupt.db"
# flip bytes in the middle of the payload, then PROVE the file actually changed --
# a negative test whose setup silently does nothing produces a false FAILURE,
# which is the mirror of the false pass this drill exists to prevent.
dd if=/dev/urandom of="$WORK/corrupt.db" bs=1 seek=4096 count=512 conv=notrunc 2>/dev/null
if cmp -s "$WORK/good.db" "$WORK/corrupt.db"; then
    no "LEG 2 setup" "corruption did not change the file -- this leg proves nothing"
else
    ok "LEG 2 setup -- the corrupted copy genuinely differs from the good one"
    if "$ETCDUTL" snapshot restore "$WORK/corrupt.db" --data-dir "$WORK/corrupt-out" >"$WORK/c.log" 2>&1; then
        no "LEG 2" "*** A CORRUPTED SNAPSHOT RESTORED -- the drill cannot say NO ***"
    else
        ok "LEG 2 -- a corrupted snapshot is REFUSED ($(head -1 "$WORK/c.log" | cut -c1-60)): THE DRILL CAN SAY NO"
    fi
fi

# ------------------------------------------- LEG 3: can it say NO (truncation)?
head -c $(( $(wc -c <"$WORK/good.db") / 2 )) "$WORK/good.db" > "$WORK/short.db"
if [ "$(wc -c <"$WORK/short.db")" -ge "$(wc -c <"$WORK/good.db")" ]; then
    no "LEG 3 setup" "truncation did not shorten the file"
else
    ok "LEG 3 setup -- the truncated copy is genuinely shorter"
    if "$ETCDUTL" snapshot restore "$WORK/short.db" --data-dir "$WORK/short-out" >"$WORK/s.log" 2>&1; then
        no "LEG 3" "*** A TRUNCATED SNAPSHOT RESTORED ***"
    else
        ok "LEG 3 -- a truncated snapshot is REFUSED: integrity is checked, not assumed"
    fi
fi

# ---------------------- LEG 4: does the RUNBOOK'S OWN COMMAND work? ----------
# Legs 1-3 used the simple restore form. RUNBOOK.md documents a three-member
# rebuild with --name / --initial-cluster / --initial-advertise-peer-urls --
# the form an operator copies at 3am. That is exactly the shape that turned out
# to be non-functional in mail-api's runbook, in three independent ways, none of
# which were visible by reading it.
#
# And "exits 0 and creates a directory" is a PROXY. The claim is that the
# restored member SERVES THE DATA, so this starts it with --force-new-cluster
# (a single restored member cannot reach quorum with absent peers) and counts
# what comes back.
RB_CLUSTER="etcd-vm2=https://93.89.141.253:2380,etcd-nano2=https://51.91.248.208:2380,etcd-nano4=https://57.131.136.207:2380"
rb_fail=0
for m in "etcd-vm2 93.89.141.253" "etcd-nano2 51.91.248.208" "etcd-nano4 57.131.136.207"; do
    name=${m%% *}; ip=${m##* }
    if "$ETCDUTL" snapshot restore "$WORK/good.db" \
        --name "$name" --initial-cluster "$RB_CLUSTER" \
        --initial-advertise-peer-urls "https://$ip:2380" \
        --data-dir "$WORK/rb-$name" >"$WORK/rb.log" 2>&1 && [ -f "$WORK/rb-$name/member/snap/db" ]; then
        :
    else
        no "LEG 4 ($name)" "$(tail -1 "$WORK/rb.log" | cut -c1-80)"; rb_fail=1
    fi
done
[ "$rb_fail" -eq 0 ] && ok "LEG 4a -- the RUNBOOK invocation completes for all three members"

if [ "$rb_fail" -eq 0 ]; then
    "$ETCD" --name etcd-vm2 --data-dir "$WORK/rb-etcd-vm2" --force-new-cluster \
        --listen-client-urls http://127.0.0.1:$CPORT --advertise-client-urls http://127.0.0.1:$CPORT \
        --listen-peer-urls http://127.0.0.1:$PPORT --initial-advertise-peer-urls http://127.0.0.1:$PPORT \
        --log-level error >"$WORK/rb-serve.log" 2>&1 &
    PID=$!
    n=0; while [ $n -lt 40 ]; do ctl endpoint health >/dev/null 2>&1 && break; n=$((n+1)); sleep 0.25; done
    RB_COUNT=$(ctl get /drill/ --prefix --keys-only 2>/dev/null | grep -c '^/drill/')
    RB_SPOT=$(ctl get /drill/key-42 --print-value-only 2>/dev/null)
    stop_etcd
    if [ "$RB_COUNT" -eq "$N" ] && [ "$RB_SPOT" = "value-42" ]; then
        ok "LEG 4b -- a member restored by the RUNBOOK command SERVES $RB_COUNT/$N keys: the documented procedure works"
    else
        no "LEG 4b" "served $RB_COUNT/$N keys, spot='$RB_SPOT' -- the documented procedure does NOT work"
    fi
fi

echo
echo "== RESULT: $pass passed, $fail failed =="
echo
echo "   WHAT THIS RUN ESTABLISHED: the restore mechanism works on this host, it"
echo "   REFUSES a corrupted or truncated snapshot, and the three-member command"
echo "   as written in RUNBOOK.md produces a member that actually serves the data."
echo
echo "   WHAT IT DID NOT: it never touched the live cluster. It says nothing about"
echo "   whether a PRODUCTION snapshot restores -- that snapshot is larger, older,"
echo "   written by a cluster under load, and encrypted at rest. It also does not"
echo "   test the three members forming quorum together, only that each restores."
echo "   Those are Phase 2 and they are different claims."
[ "$fail" -eq 0 ] || exit 1
