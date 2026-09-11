# bip300-monitor

Rust tooling for observing BIP300/301 enforcers. The project is an early pilot.

## Overview

```text
                                                    ┌──► Postgres (record)
BIP300/301 enforcer ── gRPC ──► enforcer-extractor
                                                    └──► NATS ──► event-logger
                                                         (live fan-out)
```

The extractor:

- records an initial enforcer state snapshot;
- follows live block connections and disconnections;
- discovers active sidechain slots, including slots activated while it runs;
- stores normalized protobuf events in Postgres before publishing them to NATS;
- recovers missing block history after startup or downtime.

Postgres is authoritative. Core NATS is best-effort live delivery: historical
pages go directly to Postgres and do not flood live consumers.

The workspace contains:

- `shared`: event contract, Postgres store, NATS and lifecycle utilities;
- `extractors/enforcer`: gRPC client, conversion and extraction runtime;
- `tools/event-logger`: live event decoder and logger.

## Historical recovery

For every active slot, the first backfill walks from the current tip through the
slot's activation height. Later runs extend that proven range.

History is processed in bounded pages of 128 blocks by default. Every page is
checked for exact size, height and hash continuity, then its events and next
cursor are committed in one Postgres transaction. Interrupted work resumes from
that cursor. Oversized or timed-out requests reduce the page size automatically,
and a reorg can restart the affected slot from activation.

The historical block stream includes block headers, BMM commitments, deposits
and withdrawal-bundle outcomes. The current enforcer API does not expose past
CTIP, proposal or pending-bundle snapshots; those RPCs only return current
state. `history_coverage.stream = 'block'` therefore proves complete block
history, not complete historical state snapshots.

## Event contract

The monitor publishes a stable protobuf contract rather than forwarding raw
enforcer responses. See [event schema and semantics](proto/README.md) for event
variants, byte order, snapshot semantics, idempotency and reorg handling.

## Run locally

Start the extractor with automatic active-slot discovery:

```bash
cargo run -p enforcer-extractor -- \
  --enforcer-endpoint http://127.0.0.1:50051 \
  --nats-url nats://127.0.0.1:4222
```

Use `--sidechain 9,98` to monitor an explicit fixed set. All options also have
`BIP300_MONITOR_*` environment-variable equivalents shown by `--help`.

Inspect live events with:

```bash
cargo run -p event-logger -- --nats-url nats://127.0.0.1:4222
```

Add `--full-events` to print complete normalized JSON payloads.

## Build and test

```bash
cargo check --workspace --jobs 2
cargo test --workspace --jobs 2
```

Generating the gRPC client requires `protoc`. PostgreSQL and NATS integration
tests are feature-gated because they require those services:

```bash
BIP300_MONITOR_TEST_POSTGRES_URL='host=127.0.0.1 port=55432 user=postgres password=test dbname=postgres' \
  cargo test -p shared --features postgres_integration_tests

NATS_SERVER_BINARY=/path/to/nats-server \
  cargo test -p enforcer-extractor --features nats_integration_tests --test nats
```

## Images and deployment

CI publishes separate `linux/amd64` extractor and logger images. See
[container images](docs/container-images.md) for tags and reproducible pins.

The locked eCash Alphanet stack includes the node, enforcer, Postgres, Core
NATS, extractor and logger. See the
[eCash deployment runbook](deployments/ecash/README.md) for provisioning,
verification, history status, resource limits and recovery.

## Scope

The monitor is designed to produce reproducible evidence about BIP300 behavior
and enforcer conformance. Generic L1 observability is out of scope.

## License

MIT
