# bip300-monitor

Rust tooling to monitor BIP300/301 enforcers.

## Status

This project is an early pilot. The first extractor will consume the enforcer's
public read-only API and publish live protobuf events to Core NATS.

## Architecture

```text
BIP300/301 enforcer ── Connect/gRPC ──► enforcer-extractor ── protobuf ──► NATS
```

- `shared` contains common event, NATS, and metrics infrastructure.
- `extractors/enforcer` contains the enforcer client and extraction runtime.

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

The executable publishes an initial state snapshot to the `bip300.enforcer`
Core NATS subject and then follows live block events until it receives
`SIGINT` or `SIGTERM`. Sidechain slots must be configured explicitly:

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

HTTP/2 and TCP keepalives detect dead gRPC connections. Every live publication
uses a bounded NATS client flush, and a fatal stream, conversion, or publication
error stops all slot workers. `SIGINT` and `SIGTERM` trigger graceful shutdown;
a second signal or the configured timeout forces termination.

Core NATS delivery remains at-most-once and non-durable. A successful client
flush is not a server or consumer acknowledgement, and this pilot does not
backfill downtime gaps. Detailed event and delivery semantics are documented in
[`proto/README.md`](proto/README.md). Persistent NATS failures terminate the
extractor; deployments must restart it, and every restart republishes the
snapshot.

## Build

```bash
cargo check --workspace --jobs 2
cargo test --workspace --jobs 2
```

Generating the client currently requires `protoc` to be installed. On
Debian/Ubuntu it is provided by `protobuf-compiler`.

The feature-gated Core NATS integration test requires a `nats-server` binary.
Run it with:

```bash
NATS_SERVER_BINARY=/path/to/nats-server \
  cargo test --workspace --all-features --jobs 2
```

Drynet gate: confirm that an idle enforcer remains subscribed beyond
`--request-timeout-seconds 5`; that normal `SIGTERM` exits with code 0 before
the 15-second shutdown timeout; and that an in-flight publication with NATS
unavailable reports its flush error before that outer deadline.

To verify API compatibility against a running enforcer:

```bash
cargo run --example get_chain_info -- \
  http://127.0.0.1:50051
```

## Next

Planned work includes operational metrics, gRPC resubscription, and gap
backfill. Pending withdrawal vote counts require a future extension to the
enforcer's public API.

## License

MIT
