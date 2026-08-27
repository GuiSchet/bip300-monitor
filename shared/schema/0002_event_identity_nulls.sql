-- Make the identity constraint cover the events that are not scoped to a slot.
--
-- `0001_event.sql` declared `UNIQUE (source, kind, sidechain, block_hash)`, and
-- Postgres treats NULLs in a unique constraint as distinct by default. The four
-- kinds that carry no slot -- chain_info, chain_tip, sidechain_proposals,
-- active_sidechains -- all record `sidechain = NULL`, so `ON CONFLICT` never
-- matched them and every restart inserted a fresh row at the same block. That
-- is precisely what the constraint was written to prevent.
--
-- The rows that already duplicated are collapsed first, keeping the earliest
-- observation of each identity, because `ADD CONSTRAINT` would otherwise fail
-- on the record it is meant to repair.
DELETE FROM event WHERE id IN (
    SELECT id FROM (
        SELECT id, row_number() OVER (
            PARTITION BY source, kind, sidechain, block_hash ORDER BY id
        ) AS duplicate_rank
        FROM event
    ) ranked WHERE duplicate_rank > 1
);

-- `PARTITION BY` groups NULLs together, which is the same grouping the
-- hardened constraint below enforces, so the cleanup above covers a NULL
-- `block_hash` as well as a NULL `sidechain`.
ALTER TABLE event
    DROP CONSTRAINT IF EXISTS event_identity;
ALTER TABLE event
    ADD CONSTRAINT event_identity
    UNIQUE NULLS NOT DISTINCT (source, kind, sidechain, block_hash);
