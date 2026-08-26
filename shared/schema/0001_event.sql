-- The authoritative record of observed monitor events.
--
-- `envelope` is the raw protobuf and the source of truth: any other store can
-- be rebuilt from it. `payload` is the same event decoded, with byte fields in
-- hexadecimal, and exists only so the record can be queried.
CREATE TABLE IF NOT EXISTS event (
    id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- When the monitor observed the event, not a block timestamp.
    observed_at timestamptz NOT NULL,
    -- Which extractor wrote the row.
    source      text        NOT NULL,
    kind        text        NOT NULL,
    -- NULL for events that are not scoped to a slot.
    sidechain   smallint,
    -- The block the observation is anchored to, in display order.
    block_hash  bytea,
    -- NULL when the source named a block without a height, as a disconnect
    -- does. Never zero: that would be a claim about the genesis block.
    height      integer,
    envelope    bytea       NOT NULL,
    payload     jsonb       NOT NULL,

    CONSTRAINT event_height_is_a_real_height CHECK (height IS NULL OR height >= 0),
    CONSTRAINT event_block_hash_is_32_bytes
        CHECK (block_hash IS NULL OR octet_length(block_hash) = 32)
);

-- One observation of one kind, for one slot, at one block. This is what makes
-- the snapshot republished on restart and the blocks replayed by a backfill
-- idempotent rather than duplicated.
--
-- A named constraint rather than a bare unique index, because the writer names
-- it in ON CONFLICT: an unnamed index would let a schema change silently move
-- the conflict target.
ALTER TABLE event
    DROP CONSTRAINT IF EXISTS event_identity;
ALTER TABLE event
    ADD CONSTRAINT event_identity
    UNIQUE (source, kind, sidechain, block_hash);

CREATE INDEX IF NOT EXISTS event_by_time ON event (observed_at DESC);
CREATE INDEX IF NOT EXISTS event_by_block ON event (height DESC)
    WHERE height IS NOT NULL;
CREATE INDEX IF NOT EXISTS event_by_kind ON event (kind, sidechain, height DESC);
