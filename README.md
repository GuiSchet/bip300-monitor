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
- recovers per-slot official block history and independent raw node history.

Postgres is authoritative. Core NATS is best-effort live delivery: historical
pages go directly to Postgres and do not flood live consumers.

The workspace contains:

- `shared`: event contract, Postgres store, NATS and lifecycle utilities;
- `extractors/enforcer`: gRPC client, conversion and extraction runtime;
- `tools/event-logger`: live event decoder and logger.

## Official sources and recovery

Contract **8 / SQL schema 9** uses a fresh dataset. The enforcer is unmodified
LayerTwo-Labs upstream, pinned in `deployments/ecash/VERSIONS.lock`. No private
observer RPCs or local BIP300 consensus replay are required.

Two independently checkpointed history streams are captured:

- `block` from official GetBlockInfo, per active sidechain instance: headers,
  deposits, BMM commitments and withdrawal outcomes;
- `mainchain_block` from the node: verified raw blocks, parent links and absolute
  cumulative work, from network activation even without active slots.

Pages and cursors commit atomically. Reorg repair joins a certified ancestor and
preserves the earlier proof while the replacement completes. Conflicting facts
remain as evidence and prevent certification. The node worker continues if the
enforcer RPC is unavailable. Their tips are separate observations.

The global official SubscribeEvents(0) connection records live transitions with
zero active slots. Reconnects are recorded as gaps; missing offline disconnects,
server sequence numbers and historical protocol effects are not invented.

State RPC responses are unanchored observations with before/after tip reads.
`tip_matched` means equal observed tips, not an atomic snapshot or proof against
an A → B → A race. Revisions and mempool readiness are unavailable upstream.
Every five-second BMM sample is retained, including empty responses; empty does
not prove the mempool is ready. Optional confirmed-fee enrichment covers only
previously observed bids matched to official commitments and node transactions.
Missing prevouts remain unknown; money uses exact integer arithmetic.

PostgreSQL is authoritative. NATS subjects are `bip300.enforcer` and
`bip300.node`; the logger subscribes to `bip300.*`. The writer remains serialized;
transaction wait/duration metrics and growth should be monitored. No retention
of observations is silently introduced.

See [event semantics](proto/README.md), [candidate cutover](deployments/ecash/RELEASE_OFFICIAL.md),
[backup/rollback](deployments/ecash/BACKUP_V7.md), and
[quality SQL](deployments/ecash/scripts/quality-report.sql).

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
