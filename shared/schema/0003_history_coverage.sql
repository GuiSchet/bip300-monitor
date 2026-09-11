-- Durable progress for bounded historical imports.
--
-- Event rows alone cannot be used as a backfill cursor: live delivery can put
-- a newer block in `event` while an older hole is still being filled.  This
-- table therefore records the contiguous range that has actually been proven
-- and the exact older block the current backwards walk must request next.
CREATE TABLE IF NOT EXISTS history_coverage (
    source                text        NOT NULL,
    stream                text        NOT NULL,
    -- NULL is reserved for future global history streams.  Block history is
    -- scoped to one sidechain slot.
    sidechain             smallint,
    coverage_start_height integer     NOT NULL,
    covered_tip_hash      bytea,
    covered_tip_height    integer,
    target_tip_hash       bytea       NOT NULL,
    target_tip_height     integer     NOT NULL,
    -- Exclusive lower boundary for the current walk.  It is NULL only when
    -- genesis itself is included.  A hash is present when extending an
    -- existing contiguous range and is verified before completing the walk.
    floor_hash            bytea,
    floor_height          integer,
    -- Inclusive cursor for the next page.  Both are NULL once the target has
    -- been joined to the floor successfully.
    next_hash             bytea,
    next_height           integer,
    status                text        NOT NULL,
    rows_recorded         bigint      NOT NULL DEFAULT 0,
    effective_page_blocks integer     NOT NULL,
    last_error            text,
    started_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),
    completed_at          timestamptz,

    CONSTRAINT history_coverage_identity
        UNIQUE NULLS NOT DISTINCT (source, stream, sidechain),
    CONSTRAINT history_coverage_stream_not_empty CHECK (stream <> ''),
    CONSTRAINT history_coverage_sidechain_is_a_slot
        CHECK (sidechain IS NULL OR sidechain BETWEEN 0 AND 255),
    CONSTRAINT history_coverage_start_is_a_height
        CHECK (coverage_start_height >= 0),
    CONSTRAINT history_coverage_covered_tip_pair
        CHECK ((covered_tip_hash IS NULL) = (covered_tip_height IS NULL)),
    CONSTRAINT history_coverage_floor_pair
        CHECK ((floor_hash IS NULL) OR (floor_height IS NOT NULL)),
    CONSTRAINT history_coverage_next_pair
        CHECK ((next_hash IS NULL) = (next_height IS NULL)),
    CONSTRAINT history_coverage_hash_sizes CHECK (
        (covered_tip_hash IS NULL OR octet_length(covered_tip_hash) = 32)
        AND octet_length(target_tip_hash) = 32
        AND (floor_hash IS NULL OR octet_length(floor_hash) = 32)
        AND (next_hash IS NULL OR octet_length(next_hash) = 32)
    ),
    CONSTRAINT history_coverage_heights CHECK (
        (covered_tip_height IS NULL OR covered_tip_height >= 0)
        AND target_tip_height >= 0
        AND (floor_height IS NULL OR floor_height >= 0)
        AND (next_height IS NULL OR next_height >= 0)
    ),
    CONSTRAINT history_coverage_status
        CHECK (status IN ('running', 'complete', 'error')),
    CONSTRAINT history_coverage_rows_nonnegative CHECK (rows_recorded >= 0),
    CONSTRAINT history_coverage_page_positive CHECK (effective_page_blocks > 0),
    CONSTRAINT history_coverage_completion_shape CHECK (
        (status = 'complete' AND next_hash IS NULL AND completed_at IS NOT NULL)
        OR
        (status <> 'complete' AND next_hash IS NOT NULL AND completed_at IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS history_coverage_by_status
    ON history_coverage (source, stream, status, sidechain);
