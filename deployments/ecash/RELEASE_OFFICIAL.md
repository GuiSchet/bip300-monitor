# V7 revised: official sources (contract 8 / SQL 9)

The enforcer source is unmodified LayerTwo-Labs upstream at
`1753fc0c23863bcb39c681e1cfaea2705613516f`. Its protobuf is copied verbatim and
verified with `.github/scripts/check-proto-vendor.sh`. Packaging belongs here;
no observer RPC patch or upstream rules implementation is maintained.

The node keeps the existing Betanet pin. A separate RPC identity restricted to four read methods is mounted read-only
into the extractor; the node cookie and its permissions are unchanged. The independent node worker persists verified raw blocks,
headers and absolute chain work even while the enforcer RPC is unavailable.
Raw-block pages are capped at four blocks to bound transaction memory.

Official SubscribeEvents(0) supplies global connect/disconnect observations
with no active slots. Slot 0 is a transport selector, not an active instance.
There is no durable server sequence, atomic baseline or replay of missed
live transitions. Reconnects are explicit gaps. Node and enforcer tips remain
separate; divergent tips prevent joint certification.

GetBlockInfo(slot) supplies historical deposits, bundle events and BMM
commitments. Protocol effects not supplied by upstream remain unknown.
GetSeenBmmRequests retains every sample. Empty means no bids were returned;
mempool readiness and complete bid coverage are unknown. Confirmed fees only
cover previously observed bids whose commitment and transaction match a block;
missing prevouts remain unknown. Integer amounts are calculated exactly.

State snapshots are unanchored events with an observation window. Equal tip
reads are `tip_matched`, never atomic `stable`. Historical private deltas and
revision/readiness fields are not produced. Old protobuf types remain solely
for decoding archived records.

## Validated local candidate

[RELEASE_OFFICIAL.json](RELEASE_OFFICIAL.json) records exact source revisions,
OCI manifest/config digests and the paired Observatory image. Monitor image
source is `2f2574d46d1bc9d888932f1161a2ab18d3b9dee6`; Observatory image source is
`026d0c739c86eb4c18ba87be7de4ebbae25c91ad`. Both source commits are GPG-signed.
The fork archive commit `7a7b625ae5a46a085b3617fdedf37a92181ea048` retires its
publishing workflows; that fork branch is not part of the deployment.

Validation passed: 153 monitor tests, 14 Observatory units, Clippy, static
configuration, exact official protobuf comparison, dedicated RPC permissions,
real PostgreSQL importer/API/reorg/conflict checks, Chromium and paired restore.
All five CLI binaries were checked in local containers without network access.

The images are local OCI archives, not published registry tags. Keep
`RELEASE_STATUS=preparing` until registry digests have been checked. The final
reviewed promotion changes this marker to `ready` before an approved start.
The old RELEASE_V7.md and its OCI files are superseded archives, not this release.

## Future cutover

1. Freeze and export the old monitor record using the existing backup workflow.
   Verify a separate restore and the paired Observatory backup before finalizing.
2. Preserve the old enforcer directory. Use `enforcer-official-v8` for official
   upstream; never open the fork database with an official binary.
3. Start a fresh monitor database (contract 8) and fresh Observatory database
   (projection 7). Do not reuse an old dataset or migrate its interpretation.
4. Verify the node and enforcer activation hash and tips, global node history,
   per-slot hash-linked history, conflicts, snapshot quality and worker health.
5. Record the actual official source SHA and image digest. Compatibility is
   checked by schema/contract/capabilities, not a hardcoded consumer SHA.
6. Roll back by stopping both new consumers and restoring the paired old record,
   projection, locks and enforcer directory. Keep both archives. Never run an
   old binary against the new database.

No HOSTKEY changes have been applied by this implementation.
