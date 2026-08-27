# Monitor event schema

The files in this directory define the stable protobuf contract published by
`bip300-monitor`. They are deliberately separate from `proto/upstream`, which
contains the pinned enforcer API used as an input.

`event.proto` defines the top-level envelope. Like `peer-observer`, every
payload carries the time at which the monitor constructed it and a `oneof`
identifying the extractor. `enforcer_extractor.proto` contains normalized
events derived from the enforcer's read-only validator API.

All envelopes are published to the stable Core NATS subject
`bip300.enforcer`. Consumers select the concrete payload through the protobuf
`oneof`; the subject is intentionally not split by sidechain slot or event
variant in the first pilot.

## Semantics

- `Event.timestamp` is the observation time in Unix milliseconds. It is not a
  Bitcoin block timestamp.
- `Event.observed_at_block` is the mainchain block the observation is anchored
  to, and it is what makes an event joinable to a height. For a block event it
  is the block the event is about. For a state snapshot it is the enforcer's
  tip when the state was read — an anchor, **not** a claim that the state is
  exactly as of that height, because reading state is a poll and the enforcer
  can advance between one field and the next.
- `ObservedBlock.height` is absent when the source did not report one. A
  disconnect names only the block being disconnected, so its height has to be
  recovered from the connect that preceded it. An absent height is never
  published as zero.
- Hashes and transaction IDs are decoded from the enforcer's `ReverseHex`
  values and stored as 32 bytes in conventional display order.
- Fields documented as consensus-encoded preserve the byte order and any
  length prefix supplied by the enforcer's `ConsensusHex` value.
- Proposal, active-sidechain, CTIP, and withdrawal-bundle-proposal messages are
  snapshots, not deltas. They are published once at startup and then again
  whenever a connected or disconnected block changed their value, so a consumer
  sees the latest state without diffing. An unchanged snapshot is not
  republished, and `ChainInfo` and `ChainTip` are published only at startup.
- Because a refresh is a poll rather than a per-block query, a snapshot
  describes the state at the moment it was read. Under fast blocks two heights
  can coalesce into one refresh, so consecutive snapshots are consecutive
  observations, not consecutive blocks. This is a property of the enforcer API,
  not of the monitor: `GetCtip`, `GetSidechains`, `GetSidechainProposals` and
  `GetWithdrawalBundleProposals` only answer for the current tip, and no RPC
  answers "the state at block X", so a value that changed and reverted inside one
  coalesced window leaves no observation behind.
- A snapshot's `observed_at_block` is the tip it was read against, which under a
  moving chain can be a later block than the one whose arrival triggered the
  read — and can therefore be a block with no `BlockConnected` of its own yet.
- Reorgs are recorded, not repaired. `BlockDisconnected` says a block left the
  chain; the `BlockConnected` that preceded it stays in the record, unmarked,
  because the envelope is a log of observations rather than a view of the current
  chain. Reconstructing the surviving chain is the consumer's job. Two
  consequences follow: a block that is disconnected and then connected again
  produces no second `BlockConnected`, since it is the same observation of the
  same block; and the backfill checkpoint, being the highest recorded height,
  can name an orphan until a later block outgrows it.
- `WithdrawalBundleProposalsSnapshot` carries the bundles still being voted on.
  Its `vote_count` is read against
  `Bip300Constants.withdrawal_bundle_inclusion_threshold` and its
  `proposal_height` against `withdrawal_bundle_max_age`. The terminal outcome of
  a bundle stays where it was, in `BlockConnected.events`.
- Block events are scoped to the explicitly configured sidechain slot because
  the upstream subscription is also per slot.
- Block event order is preserved within a sidechain slot. A reorganization is
  represented by the enforcer's ordered disconnect and connect events.
- There is no global ordering guarantee across sidechain slots. The same
  mainchain block is published once per configured slot with that slot's
  filtered data.
- An absent `CtipSnapshot.ctip` means that the sidechain has no current CTIP.
- Startup can publish a block in both the snapshot and the buffered live
  stream. Consumers should treat block event type, sidechain slot, and block
  hash as an idempotency key.

Conversions reject missing required input fields, malformed hex, and hashes
that are not exactly 32 bytes. This prevents incomplete upstream responses from
being published as valid-looking zero values.

Core NATS transport is at-most-once and non-durable. A successful bounded client
flush confirms that its transport write buffer was emptied; it does not confirm
that the server processed the bytes or that any consumer received or persisted
the event. The protobuf schema therefore defines observation data, not an
exactly-once delivery protocol.

When evolving the schema, add new fields with new numbers. Never change the
meaning of an existing field number or reuse a removed number.
