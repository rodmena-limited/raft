#!/usr/bin/env bash
# Runs the raft audit probe harness.
#
# Each probe drives a raft node through its own gRPC interface and asserts a
# named safety or liveness claim. Exit status per probe:
#
#   PASS  the claim HELD
#   FAIL  the claim was VIOLATED -- the defect reproduced
#
# So a FAIL here is a finding, not a broken probe. This runner exits non-zero
# if any probe fails.
#
# Every probe is self-contained: it starts its own nodes on loopback ports and
# writes to its own temp directories. None of them touches an existing cluster,
# so the default set is safe to run anywhere.
#
# Environment:
#   RAFT_PYTHON              interpreter to use (default: ../../.venv/bin/python, else python3)
#   RAFT_PROBE_DIR           where node data goes (default: system temp)
#   RAFT_PROBE_HOST          bind interface (default: 127.0.0.1)
#   AUDIT_ALLOW_DESTRUCTIVE  set to 1 to enable probes that SIGKILL their own
#                            child processes (probe 06). Off by default.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

if [[ -n "${RAFT_PYTHON:-}" ]]; then
  PY="$RAFT_PYTHON"
elif [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then
  PY="$REPO_ROOT/.venv/bin/python"
else
  PY="$(command -v python3)"
fi

echo "raft audit harness"
echo "  interpreter: $PY"
echo "  repo:        $REPO_ROOT"
if [[ "${AUDIT_ALLOW_DESTRUCTIVE:-0}" == "1" ]]; then
  echo "  destructive probes: ENABLED (they SIGKILL their own child processes)"
else
  echo "  destructive probes: disabled (set AUDIT_ALLOW_DESTRUCTIVE=1 to enable)"
fi
echo

passed=(); failed=(); skipped=()

for probe in "$HERE"/probe_*.py; do
  name="$(basename "$probe" .py)"
  out="$("$PY" "$probe" 2>&1)"
  status=$?
  # Filter gRPC's connection chatter, which is expected noise in probes that
  # deliberately stop peers.
  echo "$out" | grep -vE 'chttp2_transport|GOAWAY|debug_error_string|status = StatusCode|^\s+details = |^>$|AioRpcError of RPC'
  echo
  if echo "$out" | grep -q "SKIPPED"; then
    skipped+=("$name")
  elif [[ $status -eq 0 ]]; then
    passed+=("$name")
  else
    failed+=("$name")
  fi
done

echo "======================================================================"
echo "SUMMARY"
echo "  claims upheld (PASS):   ${#passed[@]}"
for p in "${passed[@]:-}";  do [[ -n "$p" ]] && echo "      PASS  $p"; done
echo "  claims violated (FAIL): ${#failed[@]}"
for f in "${failed[@]:-}";  do [[ -n "$f" ]] && echo "      FAIL  $f"; done
if [[ ${#skipped[@]} -gt 0 ]]; then
  echo "  skipped:                ${#skipped[@]}"
  for s in "${skipped[@]:-}"; do [[ -n "$s" ]] && echo "      SKIP  $s"; done
fi
echo "======================================================================"

[[ ${#failed[@]} -eq 0 ]]
