# Monitor event contract 8

`event.proto` is the normalized monitor envelope; `enforcer_extractor.proto`
contains its payloads. `upstream/` is a verbatim pinned official API input,
checked by `.github/scripts/check-proto-vendor.sh`. Legacy private payload types
remain only for decoding archived datasets; this runtime does not produce them.
A fresh dataset and SQL schema 9 are required.

## Sources and identity

Enforcer events use NATS `bip300.enforcer`; node events use `bip300.node`.
Core NATS is at-most-once, non-durable fan-out. PostgreSQL is authoritative;
historical pages bypass NATS. Normalized protobuf envelopes are not original
RPC wire messages. Fields never exposed by upstream cannot be reconstructed.

Hashes are exactly 32 bytes in display order. Work is a 32-byte little-endian
integer: `block_work` is per-block work, `cumulative_work` is absolute chainwork.
Only node headers supply cumulative work; official enforcer headers leave it
empty. Raw node bytes are verified against their hash, header, Merkle root and
witness commitment. Description identity hashes the decoded description vector,
excluding its CompactSize prefix. Unknown enum numbers remain raw integers.
JSON u64 values require lossless parsing; Observatory emits decimal strings.

Immutable facts and capture occurrences have separate identities. Facts dedupe
by payload; conflicting block content is retained and stops certification.
Occurrences retain run, capture sequence, read window and capture method.
Sequence is local capture order, not a global consensus order.

## Observation guarantees

`Event.timestamp` is monitor construction time. Block payloads identify the
block they describe. State snapshots (active sidechains, proposals, CTIP,
pending bundles) have no block anchor. Their snapshot group records tip before,
tip after, read interval and attempts. `tip_matched` means equal tips were read;
it does not prove atomicity or rule out A → B → A. Revisions stay null.
Use `state_snapshot_tip_matched` for this limited guarantee. Legacy
`state_snapshot_stable` does not certify official-source contract-8 state.

Mutable state is captured on startup and refreshed on tip changes; changed
windows are retried. Identical payloads may have different occurrence quality.
Consumers must select the latest occurrence before checking its quality.
Several blocks can pass within one read; values between observations are unknown.
An absent CTIP is observed absence, not a zero balance.

GetSeenBmmRequests samples a specific parent every five seconds. Parent-moving
reads are retried. Every accepted occurrence is retained, including empty lists.
Matching parent tips do not establish mempool readiness or exhaustive bid
coverage. No session, generation or ready flag is fabricated.

Official SubscribeEvents(0) records global connects/disconnects even with no
active slots. Slot 0 is a transport selector. There is no server sequence or
atomic subscription baseline. Disconnect height may be absent. Reconnects and
stream failures are explicit gaps in observation_failure. Historical backfill
cannot reproduce unseen live transitions.

Per-slot GetBlockInfo supplies BMM commitments, deposits and bundle outcomes.
Independent node history supplies raw blocks/headers and absolute work. Both
are resumable and hash-linked; reorg repair preserves earlier coverage while
walking to a certified ancestor. Node and enforcer tips are not interchangeable.
The Observatory derives branch membership from parent links and enforcer tips;
node disagreement blocks joint certification. Unsupported historical M1–M8
resolved effects remain unknown; no private consensus rules are replayed.

Optional confirmed BMM fees require an observed bid, matching official
commitment and a transaction in the node block. Exact input-minus-output fee
comes from node getblock verbosity 3. Coverage is `observed_bids_only`;
unavailable prevouts produce an unknown reason and retries, never zero.

Add fields with new numbers. Never repurpose wire numbers or relabel old
contract evidence as new guarantees.
