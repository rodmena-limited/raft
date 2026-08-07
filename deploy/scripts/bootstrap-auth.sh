#!/usr/bin/env bash
# Enable etcd RBAC on the consensus cluster and create the initial users.
#
# Idempotent: safe to re-run. Generates credentials on first run and stores them
# in a root-only file; later runs reuse them.
#
#   root  -- full admin. Operations only. Never hand this to an application.
#   app   -- readwrite over the whole keyspace, no admin rights. The general
#            purpose application credential.
#
# Per-application, prefix-scoped users are created with scripts/add-app-user.sh
# and are the recommended way to onboard a new application.

set -euo pipefail

CRED_FILE="${CONSENSUS_CRED_FILE:-/etc/consensus/credentials.env}"
E1=http://127.0.0.1:2379

ctl() { docker exec consensus-etcd1 etcdctl --endpoints=$E1 "$@"; }
ctl_root() {
  docker exec consensus-etcd1 etcdctl --endpoints=$E1 \
    --user "root:${ETCD_ROOT_PASSWORD}" "$@"
}

# ---------------------------------------------------------------- credentials
if [[ ! -f "$CRED_FILE" ]]; then
  echo "generating new credentials at $CRED_FILE"
  install -d -m 0750 "$(dirname "$CRED_FILE")"
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
# shellcheck disable=SC1090
source "$CRED_FILE"

# ------------------------------------------------------------ already enabled?
if ctl_root auth status 2>/dev/null | grep -qi "Authentication Status: true"; then
  echo "auth already enabled; ensuring users/roles are present"
  AUTH_ON=1
else
  AUTH_ON=0
fi

if [[ $AUTH_ON -eq 0 ]]; then
  echo "creating root user"
  ctl user add root --new-user-password="${ETCD_ROOT_PASSWORD}" 2>/dev/null \
    || echo "  root already exists"

  echo "creating app role (readwrite over the whole keyspace, no admin)"
  ctl role add app 2>/dev/null || echo "  role app already exists"
  # --prefix with an empty key means every key.
  ctl role grant-permission app --prefix=true readwrite '' 2>/dev/null \
    || echo "  permission already granted"

  echo "creating app user"
  ctl user add app --new-user-password="${ETCD_APP_PASSWORD}" 2>/dev/null \
    || echo "  app already exists"
  ctl user grant-role app app 2>/dev/null || echo "  role already granted"

  # The guest role governs unauthenticated requests. It must hold no
  # permissions, or the whole keyspace is world-readable once auth is on.
  ctl role revoke-permission guest --prefix=true '' 2>/dev/null \
    || echo "  guest holds no permissions (expected)"

  echo "enabling auth"
  ctl auth enable
fi

echo
echo "--- users ---"
ctl_root user list
echo "--- app role ---"
ctl_root role get app
echo
echo "auth bootstrap complete. Credentials in $CRED_FILE (root-only)."
