#!/usr/bin/env bash
# Regenerate the gRPC/protobuf stubs into src/raft/rpc/proto/.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -x .venv/bin/python ]; then
    PY=.venv/bin/python
else
    PY=python3
fi

OUT=src/raft/rpc/proto
"$PY" -m grpc_tools.protoc -I proto --python_out="$OUT" --grpc_python_out="$OUT" proto/raft.proto

# The grpc plugin emits `import raft_pb2` (top-level). The stubs live inside the
# raft.rpc.proto package, so the import must be relative.
sed -i 's/^import raft_pb2 as raft__pb2$/from . import raft_pb2 as raft__pb2/' "$OUT/raft_pb2_grpc.py"

echo "generated stubs in $OUT"
