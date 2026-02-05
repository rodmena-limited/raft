# raft-py

Production-grade Raft consensus in Python 3.12 using asyncio, gRPC transport, filesystem-backed log/snapshots, and a demo key-value state machine. Includes comprehensive tests, property checks, and coverage quality gate.

## Features
- Raft per Diego Ongaro & John Ousterhout: leader election, log replication, safety
- asyncio-based timers and RPCs over gRPC
- Filesystem WAL segments, durable metadata, and snapshot/compaction
- Joint consensus membership changes
- Demo key-value state machine (get/put) for integration tests
- Quality gate: pytest + hypothesis + coverage (fail-under 85%), ruff, mypy

## Layout
- `proto/`: Raft protobuf definitions
- `src/raft/`: implementation
  - `core/`: Raft state, roles, log replication, membership
  - `rpc/`: gRPC server/client wrappers, generated stubs under `rpc/proto/`
  - `storage/`: interfaces and filesystem-backed WAL/snapshots
  - `node/`: lifecycle wiring and orchestration
  - `sm/`: state machine interface and demo KV
  - `util/`: timing, logging, helpers
- `tests/`: unit, integration, property tests

## Development
Install dev deps:
```
pip install -e .[dev]
```
Generate protobufs (requires `grpcio-tools`):
```
python -m grpc_tools.protoc -I proto --python_out=src --grpc_python_out=src proto/raft.proto
```
Run tests with coverage gate:
```
pytest --cov --cov-report=term-missing
```
Lint/type-check:
```
ruff check src tests
mypy src tests
```
