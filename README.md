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
- records every capture occurrence separately from its idempotent event fact;
- records tip transitions and snapshot-consistency windows durably;
- samples the live BMM auction, including successful empty states;
- recovers both per-slot block history and global BIP300/301 deltas after
  startup or downtime.

Postgres is authoritative. Core NATS is best-effort live delivery: historical
pages go directly to Postgres and do not flood live consumers.

The workspace contains:

- `shared`: event contract, Postgres store, NATS and lifecycle utilities;
- `extractors/enforcer`: gRPC client, conversion and extraction runtime;
- `tools/event-logger`: live event decoder and logger.

## Historical recovery

Two resumable streams have distinct coverage:

- `block`, per sidechain instance, walks through that instance's activation
  height and stores headers, BMM commitments, deposits and terminal withdrawal
  outcomes;
- `bip300_delta`, global even when no slot is active, walks from the network
  activation height and stores exact BIP300 coinbase scripts, resolved
  M1/M2/M3/M4/M7 effects, M5/M6 treasury transitions and confirmed M8
  requests.

Pages and cursors commit atomically, are bounded, reorg-aware and bypass NATS.
The unary state RPCs still expose only current CTIP/proposal/pending-bundle
state. Historical snapshots are therefore never fabricated; historical
transitions come from persisted enforcer diffs and unpruned Core blocks.

`GetBip300BlockDelta` lives in the reviewed enforcer observer fork recorded in
[`proto/upstream/README.md`](proto/upstream/README.md). The deployment lock pins
that exact commit and its immutable OCI image, so the global delta history is a
required part of deployment verification rather than an optional development
path.

## Event contract

The monitor publishes a versioned normalized protobuf contract rather than
forwarding raw enforcer responses. Event contract v4 adds normalized live BMM
request snapshots, and PostgreSQL schema v5 permits multiple auction states at
one parent block without duplicating repeated observations. Schema v4 binds
each fact and occurrence to a dataset and extractor run, preserves `A -> B ->
A` tip order, tracks sidechain instances and retains coverage revisions. See
[event schema and semantics](proto/README.md) for event variants, byte order,
snapshot semantics, idempotency and reorg handling.

## Run locally

Start the extractor with automatic active-slot discovery:

```bash
cargo run -p enforcer-extractor -- \
  --enforcer-endpoint http://127.0.0.1:50051 \
  --nats-url nats://127.0.0.1:4222
```

Use `--sidechain 9,98` to select an explicit fixed set. A configured slot may
be inactive at startup; the extractor keeps running and begins its stream and
full-history recovery when that slot becomes active. All options also have
`BIP300_MONITOR_*` environment-variable equivalents shown by `--help`.

Inspect live events with:

```bash
cargo run -p event-logger -- --nats-url nats://127.0.0.1:4222
```

Add `--full-events` to print complete normalized JSON payloads.

## Build and test

```bash
cargo check --workspace --jobs 2
cargo test --workspace --all-targets --jobs 2
deployments/ecash/scripts/static-check.sh
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
The gated HOSTKEY transition to Betanet is documented in the
[Betanet migration runbook](docs/betanet-migration.md).

## Scope

The monitor is designed to produce reproducible evidence about BIP300 behavior
and enforcer conformance. Generic L1 observability is out of scope.

## License

MIT
