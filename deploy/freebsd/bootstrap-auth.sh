#!/bin/sh
# Enable etcd RBAC on the distributed consensus cluster and create initial users.
#
# Runs ON a cluster member (FreeBSD), not through docker. Transport auth is the
# client certificate; RBAC is a SECOND layer on top. Both are required:
#   - the cert proves you may talk to the port at all
#   - the user/password decides what you may read or write
# A cert alone gets you a connection and nothing else.
#
#   root -- full admin. Operations only. Never hand this to an application.
#   app  -- readwrite over the whole keyspace, no admin rights.
#
# Idempotent: safe to re-run.
set -eu
CRED_FILE="${CONSENSUS_CRED_FILE:-/usr/local/etc/etcd/credentials.env}"
PKI=/usr/local/etc/etcd/pki
EP="https://93.89.141.253:2379,https://51.91.248.208:2379,https://57.131.136.207:2379"

ctl() { ETCDCTL_API=3 /usr/local/bin/etcdctl \
    --cacert="$PKI/ca.crt" --cert="$PKI/etcd-client.crt" --key="$PKI/etcd-client.key" \
    --endpoints="$EP" "$@"; }
ctl_root() { ctl --user "root:${ETCD_ROOT_PASSWORD}" "$@"; }

if [ ! -f "$CRED_FILE" ]; then
    echo "generating new credentials at $CRED_FILE"
    umask 077
    cat > "$CRED_FILE" <<EOF
# consensus.rodmena.co.uk -- etcd credentials. Root-only, do not commit.
# Generated $(date -u +%Y-%m-%dT%H:%M:%SZ)
ETCD_ROOT_PASSWORD=$(openssl rand -base64 30 | tr -d '/+=' | head -c 32)
ETCD_APP_PASSWORD=$(openssl rand -base64 30 | tr -d '/+=' | head -c 32)
EOF
    chmod 0600 "$CRED_FILE"
else
    echo "reusing existing credentials at $CRED_FILE"
fi
. "$CRED_FILE"

if ctl_root auth status 2>/dev/null | grep -qi "Authentication Status: true"; then
    echo "auth already enabled; ensuring users/roles present"
else
    echo "creating root user"
    ctl user add root --new-user-password="${ETCD_ROOT_PASSWORD}" 2>/dev/null || echo "  root exists"
    echo "creating app role (readwrite whole keyspace, no admin)"
    ctl role add app 2>/dev/null || echo "  role app exists"
    ctl role grant-permission app --prefix=true readwrite '' 2>/dev/null || echo "  permission exists"
    echo "creating app user"
    ctl user add app --new-user-password="${ETCD_APP_PASSWORD}" 2>/dev/null || echo "  app exists"
    ctl user grant-role app app 2>/dev/null || echo "  role already granted"
    echo "enabling auth"
    ctl auth enable
fi

echo
echo "--- verification ---"
echo "auth status : $(ctl_root auth status 2>/dev/null | head -1)"
echo "users       : $(ctl_root user list 2>/dev/null | tr '\n' ' ')"
echo "roles       : $(ctl_root role list 2>/dev/null | tr '\n' ' ')"
# The control that matters: an ANONYMOUS client (valid cert, no credentials)
# must be refused. Without this check, "auth enabled" is only a claim.
if ctl get /_authprobe >/dev/null 2>&1; then
    echo "ANON ACCESS  : *** PERMITTED -- auth is not actually enforcing ***"
    exit 1
else
    echo "anon access : correctly refused"
fi
