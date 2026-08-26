# bip300-monitor

Rust tooling to monitor BIP300/301 enforcers.

## Status

This project is an early pilot. The enforcer extractor consumes the enforcer's
public read-only API, records normalized protobuf events in Postgres, and fans
them out to Core NATS for live consumers.

## Architecture

```text
                                                    ┌──► Postgres  (the record)
BIP300/301 enforcer ─ Connect/gRPC ─► enforcer-extractor
                                                    └──► NATS ──► event-logger
                                                         (live fan-out)
```

Postgres is authoritative. The extractor commits there first and publishes to
NATS afterwards, so a failed write is fatal while a failed publication is only a
warning: the row is already durable, and a live consumer missing a message costs
nothing but its own freshness.

- `shared` contains the event contract, the Postgres record, NATS, JSON
  rendering, diagnostics, and lifecycle infrastructure.
- `extractors/enforcer` contains the enforcer client and extraction runtime.
- `tools/event-logger` decodes the events received from NATS and logs them.

The enforcer extractor generates a standard gRPC client from a minimal vendored
copy of the enforcer's public validator API. It does not link to the enforcer
implementation.

## Event schema

The monitor publishes its own stable protobuf contract instead of forwarding
the enforcer API responses directly. The top-level event envelope, normalized
enforcer messages, byte-order rules, and snapshot semantics are documented in
[`proto/README.md`](proto/README.md).

Rust event types are generated in `shared`. Fallible conversions in the
enforcer extractor reject missing fields, malformed hex, and incorrectly sized
hashes before an event can be published.

## Continuous extraction

The executable records an initial state snapshot and then follows live block
events until it receives
`SIGINT` or `SIGTERM`. Whenever a block moves the mainchain tip it also re-reads
the state that the tip changes — sidechain proposals, active sidechains, the
CTIP of each slot, and the withdrawal bundles still being voted on — and
republishes only what actually changed. Sidechain slots must be configured
explicitly:

```bash
cargo run -p enforcer-extractor -- \
  --enforcer-endpoint http://127.0.0.1:50051 \
  --nats-url nats://127.0.0.1:4222 \
  --sidechain 9,98
```

Configuration can also be supplied with the `BIP300_MONITOR_*` environment
variables shown by `--help`. NATS supports anonymous or username/password
authentication. Logging defaults to `info`; use `--log-level` or `RUST_LOG` for
more detail.

Subscriptions are opened before collecting the initial snapshot, so live
events are buffered during startup. This avoids an unreported gap but can
produce duplicates; consumers should deduplicate block events by type,
sidechain slot, and block hash. Ordering is preserved within each slot, not
across slots.

HTTP/2 and TCP keepalives detect dead gRPC connections. A fatal stream,
conversion, or record error stops all slot workers. `SIGINT` and `SIGTERM`
trigger graceful shutdown; a second signal or the configured timeout forces
termination.

Every restart republishes the snapshot. That is idempotent in the record — one
observation of one kind, for one slot, at one block is a single row — and it
re-establishes current state for live consumers, which Core NATS cannot replay.
What a restart does **not** yet recover is the blocks that passed while the
extractor was down: `SubscribeEvents` starts at subscription time, so that gap
needs the backfill listed under [Next](#next). Detailed event semantics are
documented in [`proto/README.md`](proto/README.md).

## Inspecting events

The event logger proves the consumer side of the pipeline by subscribing to
`bip300.enforcer` and decoding the received protobuf envelopes:

```bash
cargo run -p event-logger -- --nats-url nats://127.0.0.1:4222
```

It prints one summary per event. Add `--full-events` to also print the complete
normalized payload as one-line JSON with byte fields in hexadecimal. An
individual undecodable, unknown, or invalid event is reported as a warning and
discarded; loss of the NATS subscription remains fatal.

## Container images

CI publishes separate public `linux/amd64` images for the extractor and logger.
See [container images](docs/container-images.md) for tags, reproducible pins,
Docker Hub setup, and local builds.

## Deployments

Reproducible infrastructure lives under `deployments/`. The eCash target
generates its node configuration and isolated runtime identity from a network
lock, so the same topology can move between network generations. Its current
lock targets Alphanet and includes a
pinned node, validator enforcer, Postgres record, Core NATS, enforcer extractor,
and event logger: [eCash deployment](deployments/ecash/README.md).

## Build

```bash
cargo check --workspace --jobs 2
cargo test --workspace --jobs 2
```

The record integration tests need a server:

```bash
docker run --rm -d -p 55432:5432 -e POSTGRES_PASSWORD=test \
    --name bip300-test-postgres postgres:18.2-alpine
BIP300_MONITOR_TEST_POSTGRES_URL='host=127.0.0.1 port=55432 user=postgres password=test dbname=postgres' \
    cargo test -p shared --features postgres_integration_tests
```

Generating the client currently requires `protoc` to be installed. On
Debian/Ubuntu it is provided by `protobuf-compiler`.

The feature-gated Core NATS integration test requires a `nats-server` binary.
Run it with:

```bash
NATS_SERVER_BINARY=/path/to/nats-server \
  cargo test --workspace --all-features --jobs 2
```

Network gate: confirm that an idle enforcer remains subscribed beyond
`--request-timeout-seconds 5`; that normal `SIGTERM` exits with code 0 before
the 15-second shutdown timeout; that stopping NATS leaves the extractor
recording and only warns; and that stopping Postgres terminates it.

To verify API compatibility against a running enforcer:

```bash
cargo run --example get_chain_info -- \
  http://127.0.0.1:50051
```

## Next

The monitor's purpose is to answer two questions with reproducible evidence:
what the BIP300 mechanism is actually doing on the target network, and whether
the enforcer implements it as specified. Generic L1 observability is
deliberately out of scope.

Planned work, in order:

1. **Gap backfill.** `GetTwoWayPegData` and `GetBlockInfo` can replay the blocks
   missed while the extractor was down. `SubscribeEvents` does not replay
   history, so a restart leaves a real hole that nothing else can close. The
   checkpoint is a query against the record, not a file, so it cannot claim an
   event that was never stored.
2. **Slot discovery.** Deriving the monitored slots from `GetSidechains`
   instead of requiring `--sidechain`, so a newly activated sidechain is not
   invisible until the next restart.
3. **An independent oracle.** The enforcer parses the BIP300 coinbase messages
   but only publishes aggregates: per-block M2, M4 and M7 votes never leave it,
   and BMM bid amounts appear in no API at all. Deriving that state from the raw
   block and comparing it against what the enforcer reports turns the monitor
   into a conformance check rather than a mirror.

## License

MIT
