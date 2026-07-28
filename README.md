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

## Initial snapshot

The current executable publishes one initial state snapshot to the
`bip300.enforcer` Core NATS subject and exits. Sidechain slots must be
configured explicitly:

```bash
cargo run -p enforcer-extractor -- \
  --enforcer-endpoint http://127.0.0.1:50051 \
  --nats-url nats://127.0.0.1:4222 \
  --sidechain 9,98
```

Configuration can also be supplied with the `BIP300_MONITOR_*` environment
variables shown by `cargo run -p enforcer-extractor -- --help`. NATS supports
anonymous access or a username with either `--nats-password` or
`--nats-password-file`. Passwords are never logged.

The initial snapshot contains chain configuration, chain tip, pending
sidechain proposals, active sidechains, and one CTIP snapshot for each
configured slot. Continuous block streaming is the next implementation stage.

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

To verify API compatibility against a running enforcer:

```bash
cargo run --example get_chain_info -- \
  http://127.0.0.1:50051
```

## Planned pilot

The first pilot will:

- publish the enforcer's chain configuration and current consensus-state view;
- stream mainchain block connections and disconnections;
- extract BMM commitments, deposits, withdrawal transitions, sidechain
  proposals, active sidechains, and CTIPs;
- monitor explicitly configured sidechain slots;
- expose low-cardinality operational metrics.

Pending withdrawal vote counts are maintained internally by the enforcer but
are not exposed by its current public API. Extending that API is intentionally
deferred and will be documented before implementation.

## License

MIT
