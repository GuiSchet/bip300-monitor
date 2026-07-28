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

## Build

```bash
cargo check --workspace --jobs 2
cargo test --workspace --jobs 2
```

Generating the client currently requires `protoc` to be installed. On
Debian/Ubuntu it is provided by `protobuf-compiler`.

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
