# Monitor event schema

The files in this directory define the stable protobuf contract published by
`bip300-monitor`. They are deliberately separate from `proto/upstream`, which
contains the pinned enforcer API used as an input.

`event.proto` defines the top-level envelope. Like `peer-observer`, every
payload carries the time at which the monitor constructed it and a `oneof`
identifying the extractor. `enforcer_extractor.proto` contains normalized
events derived from the enforcer's read-only validator API.

## Semantics

- `Event.timestamp` is the observation time in Unix milliseconds. It is not a
  Bitcoin block timestamp.
- Hashes and transaction IDs are decoded from the enforcer's `ReverseHex`
  values and stored as 32 bytes in conventional display order.
- Fields documented as consensus-encoded preserve the byte order and any
  length prefix supplied by the enforcer's `ConsensusHex` value.
- Proposal, active-sidechain, and CTIP messages are snapshots, not deltas.
- Block events are scoped to the explicitly configured sidechain slot because
  the upstream subscription is also per slot.
- An absent `CtipSnapshot.ctip` means that the sidechain has no current CTIP.

Conversions reject missing required input fields, malformed hex, and hashes
that are not exactly 32 bytes. This prevents incomplete upstream responses from
being published as valid-looking zero values.

When evolving the schema, add new fields with new numbers. Never change the
meaning of an existing field number or reuse a removed number.
