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
republishes only what actually changed.

Sidechain slots can be configured explicitly, or discovered from the enforcer's
active sidechains when `--sidechain` is omitted. A deployment that pins what it
expects to observe should still set them: then a slot going missing is a failure
rather than a silently smaller set.

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

A restart also backfills. `SubscribeEvents` delivers from the moment of
subscription and its request carries no cursor, so the blocks that passed while
the extractor was down are a real hole. The record says where to resume: the
checkpoint is `max(height)` over the recorded blocks of a slot, and because a
row is committed before anything is published it can never name a block that was
not stored. Three cases are handled explicitly, because each has a way of going
wrong quietly:

- **Within the bound.** `GetTwoWayPegData` walks from the checkpoint, which is
  exclusive, up to the tip.
- **Nothing recorded yet.** Omitting the start makes the enforcer walk back to
  genesis, so a first sight takes a bounded window instead.
- **A gap past `--backfill-max-blocks`, or a checkpoint that is no longer an
  ancestor of the tip** — the shape a reorg past it leaves. Both fall back to a
  bounded window and **warn**, because skipped history that is only logged at
  debug reads later as "nothing happened".

Detailed event semantics are documented in
[`proto/README.md`](proto/README.md).

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

1. **Re-subscribing without a restart.** Slots are resolved once, at startup, so
   a sidechain that activates later is reported loudly but stays unobserved
   until the extractor is restarted. Activation takes tens of thousands of
   blocks of miner ACKs, so this is rare enough that spawning workers mid-flight
   was not worth the restart loop a slot set flapping through a reorg would
   cause — but it is still a gap.
2. **An independent oracle.** The enforcer parses the BIP300 coinbase messages
   but only publishes aggregates: per-block M2, M4 and M7 votes never leave it,
   and BMM bid amounts appear in no API at all. Deriving that state from the raw
   block and comparing it against what the enforcer reports turns the monitor
   into a conformance check rather than a mirror.

## License

MIT
