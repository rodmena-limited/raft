#!/usr/bin/env bash
# Generate the etcd cluster PKI.
#
# WHY mTLS AT ALL: when all three members ran on one host, peer traffic never left
# a Docker bridge and plain HTTP was defensible. Distributed across three
# datacentres, peer traffic crosses the public internet. Raft peers can propose
# and commit entries, so an unauthenticated peer port is a write path into the
# cluster.
#
# Peer certificates carry BOTH serverAuth and clientAuth: an etcd peer dials its
# neighbours AND accepts their connections, so a server-only cert fails in one
# direction and the failure looks like a network fault.
#
# Re-runnable: existing member certs are left alone unless -f is passed.
set -euo pipefail
cd "$(dirname "$0")"
DAYS_CA=3650
DAYS_CERT=825          # under the 825-day cap browsers/tools enforce
FORCE="${1:-}"

MEMBERS="etcd-vm2:93.89.141.253:vm-2.rodmena.co.uk
etcd-nano2:51.91.248.208:pg-nano-02.rodmena.co.uk
etcd-nano4:57.131.136.207:pg-nano-04.rodmena.co.uk"

# ---- CA -------------------------------------------------------------------
if [[ ! -f ca.key || "$FORCE" == "-f" ]]; then
    openssl ecparam -name prime256v1 -genkey -noout -out ca.key
    chmod 600 ca.key
    openssl req -x509 -new -key ca.key -sha256 -days $DAYS_CA -out ca.crt \
        -subj "/O=Rodmena/OU=consensus/CN=consensus-etcd-ca" \
        -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
        -addext "keyUsage=critical,keyCertSign,cRLSign"
    echo "  CA created"
else
    echo "  CA exists, reusing"
fi

gen() {  # gen <name> <eku> [san...]
    local name=$1 eku=$2; shift 2
    local san="$*"
    [[ -f "$name.crt" && "$FORCE" != "-f" ]] && { echo "  $name exists, skipping"; return; }
    openssl ecparam -name prime256v1 -genkey -noout -out "$name.key"
    chmod 600 "$name.key"
    openssl req -new -key "$name.key" -out "$name.csr" -subj "/O=Rodmena/OU=consensus/CN=$name"
    openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
        -out "$name.crt" -days $DAYS_CERT -sha256 \
        -extfile <(printf "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=%s\nsubjectAltName=%s\n" "$eku" "$san")
    rm -f "$name.csr"
    echo "  $name issued"
}

while IFS=: read -r name ip host; do
    [[ -z "$name" ]] && continue
    gen "$name" "serverAuth,clientAuth" "DNS:$host,DNS:$name,DNS:localhost,IP:$ip,IP:127.0.0.1"
done <<< "$MEMBERS"

# Client cert for etcdctl and the nginx edge. clientAuth only -- it never serves.
gen "etcd-client" "clientAuth" "DNS:etcd-client"

echo
echo "  --- verification ---"
for f in etcd-vm2 etcd-nano2 etcd-nano4 etcd-client; do
    printf "  %-12s " "$f"
    openssl verify -CAfile ca.crt "$f.crt" >/dev/null 2>&1 && printf "chains-to-CA " || printf "CHAIN-FAIL "
    openssl x509 -noout -ext extendedKeyUsage -in "$f.crt" 2>/dev/null | tail -1 | xargs
done
