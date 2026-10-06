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

## Future cutover

Do not merge/deploy until the full candidate checks and artifact hashes are
recorded. `RELEASE_STATUS=preparing` blocks enforcer-up and monitor-up.

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
