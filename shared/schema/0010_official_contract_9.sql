-- Contract 9: no fork-only kinds remain. Snapshot quality applies to every
-- official-source contract, and an unknown history stream is an error instead
-- of silently selecting a fork-only event kind.
DROP VIEW state_snapshot_tip_matched;
CREATE VIEW state_snapshot_tip_matched AS
SELECT e.*, o.observation_id, o.run_id, o.capture_seq, o.snapshot_group_id,
       o.observed_at AS occurrence_observed_at, s.started_at, s.finished_at,
       s.tip_before_hash, s.tip_after_hash, false AS atomicity_proven
FROM event e JOIN event_observation o ON o.event_id=e.id
JOIN snapshot_group s USING(snapshot_group_id)
WHERE e.event_contract_version>=8 AND s.consistency='tip_matched'
AND o.dataset_id=e.dataset_id AND s.dataset_id=e.dataset_id AND s.run_id=o.run_id
AND s.tip_before_hash=s.tip_after_hash AND s.tip_before_height=s.tip_after_height
AND s.revision_before IS NULL AND s.revision_after IS NULL
AND e.kind IN ('sidechain_proposals','active_sidechains','ctip','withdrawal_bundle_proposals');

CREATE OR REPLACE FUNCTION history_stream_kind(stream text) RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    CASE stream
        WHEN 'block' THEN RETURN 'block_connected';
        WHEN 'mainchain_block' THEN RETURN 'mainchain_block';
        ELSE RAISE EXCEPTION 'unknown history stream %', stream;
    END CASE;
END $$;

CREATE OR REPLACE FUNCTION observe_block_fact() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.kind IN ('block_connected', 'mainchain_block') THEN
        INSERT INTO event_conflict(dataset_id, first_event_id, conflicting_event_id)
        SELECT NEW.dataset_id, e.id, NEW.id FROM event e
         WHERE e.dataset_id = NEW.dataset_id
           AND e.event_contract_version = NEW.event_contract_version
           AND e.source = NEW.source AND e.kind = NEW.kind
           AND e.sidechain IS NOT DISTINCT FROM NEW.sidechain
           AND e.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id
           AND e.block_hash = NEW.block_hash AND e.id < NEW.id
           AND e.fact_sha256 <> NEW.fact_sha256
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NEW;
END $$;

-- Certification looks up proven blocks by hash once per walked block. The
-- unique key cannot serve that lookup: its nullable slot/instance columns are
-- compared with IS NOT DISTINCT FROM, which a btree cannot match, so every
-- lookup scanned the scope's whole proof and certification grew quadratically.
CREATE INDEX history_certified_block_lookup ON history_certified_block
    (dataset_id, event_contract_version, source, stream, block_hash);

-- Certification runs in the transaction that just wrote the page, before
-- autovacuum has seen those rows. Stale statistics made the planner scan the
-- whole scope at every step of the walk (quadratic). Each step is a lookup by
-- hash, so sequential scans are never the right plan for this function.
CREATE OR REPLACE FUNCTION certify_history() RETURNS trigger LANGUAGE plpgsql
SET enable_seqscan = off AS $$
DECLARE expected bigint; actual bigint; start_hash bytea; stream_kind text;
BEGIN
    IF NEW.status <> 'complete' THEN RETURN NEW; END IF;
    stream_kind := history_stream_kind(NEW.stream);
    IF NEW.covered_tip_hash <> NEW.target_tip_hash OR NEW.covered_tip_height <> NEW.target_tip_height THEN
        RAISE EXCEPTION 'coverage target differs from proven tip';
    END IF;
    -- Independently walk hashes, stopping only at an already certified prefix.
    WITH RECURSIVE path AS (
        SELECT e.block_hash, e.previous_hash, e.height,
               EXISTS(SELECT 1 FROM history_certified_block c WHERE
                   (c.dataset_id,c.event_contract_version,c.source,c.stream) =
                   (NEW.dataset_id,NEW.event_contract_version,NEW.source,NEW.stream)
                   AND c.sidechain IS NOT DISTINCT FROM NEW.sidechain
                   AND c.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id
                   AND c.block_hash=e.block_hash) AS certified
          FROM event e WHERE e.dataset_id=NEW.dataset_id AND e.event_contract_version=NEW.event_contract_version
           AND e.source=NEW.source AND e.kind=stream_kind
           AND e.sidechain IS NOT DISTINCT FROM NEW.sidechain
           AND e.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id
           AND e.block_hash=NEW.target_tip_hash
        UNION ALL
        -- LATERAL pins each step to an index lookup of the parent by hash.
        SELECT e.block_hash,e.previous_hash,e.height,
               EXISTS(SELECT 1 FROM history_certified_block c WHERE
                   (c.dataset_id,c.event_contract_version,c.source,c.stream) =
                   (NEW.dataset_id,NEW.event_contract_version,NEW.source,NEW.stream)
                   AND c.sidechain IS NOT DISTINCT FROM NEW.sidechain
                   AND c.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id
                   AND c.block_hash=e.block_hash)
          FROM path p CROSS JOIN LATERAL (
               SELECT e.block_hash,e.previous_hash,e.height FROM event e
                WHERE e.dataset_id=NEW.dataset_id AND e.event_contract_version=NEW.event_contract_version
                  AND e.source=NEW.source AND e.kind=stream_kind
                  AND e.block_hash=p.previous_hash AND e.height=p.height-1
                  AND e.sidechain IS NOT DISTINCT FROM NEW.sidechain
                  AND e.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id) e
         WHERE NOT p.certified AND p.height>NEW.coverage_start_height
    ), checked AS (
        SELECT count(*) AS n,count(DISTINCT block_hash) AS unique_n,min(height) AS low,
               bool_or(certified) AS joined, max(height) AS high FROM path
    )
    SELECT n, CASE WHEN n=unique_n AND high=NEW.target_tip_height
                       AND (joined OR low=NEW.coverage_start_height)
                   THEN high-low+1 ELSE -1 END
      INTO actual, expected FROM checked;
    IF actual=0 OR actual<>expected THEN RAISE EXCEPTION 'history is missing, discontinuous or ambiguous'; END IF;
    -- Store the newly proved suffix: the same walk, which stops at the first
    -- certified block. The proof check stays a correlated subplan so each step
    -- is one index lookup; a NOT EXISTS join here was planned as a scan of the
    -- scope's entire proof per block.
    WITH RECURSIVE path AS (
        SELECT e.block_hash, e.previous_hash, e.height,
               EXISTS(SELECT 1 FROM history_certified_block c WHERE
                   (c.dataset_id,c.event_contract_version,c.source,c.stream) =
                   (NEW.dataset_id,NEW.event_contract_version,NEW.source,NEW.stream)
                   AND c.sidechain IS NOT DISTINCT FROM NEW.sidechain
                   AND c.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id
                   AND c.block_hash=e.block_hash) AS certified
          FROM event e WHERE e.dataset_id=NEW.dataset_id AND e.event_contract_version=NEW.event_contract_version
           AND e.source=NEW.source AND e.kind=stream_kind
           AND e.sidechain IS NOT DISTINCT FROM NEW.sidechain
           AND e.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id
           AND e.block_hash=NEW.target_tip_hash
        UNION ALL
        -- LATERAL pins each step to an index lookup of the parent by hash.
        SELECT e.block_hash,e.previous_hash,e.height,
               EXISTS(SELECT 1 FROM history_certified_block c WHERE
                   (c.dataset_id,c.event_contract_version,c.source,c.stream) =
                   (NEW.dataset_id,NEW.event_contract_version,NEW.source,NEW.stream)
                   AND c.sidechain IS NOT DISTINCT FROM NEW.sidechain
                   AND c.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id
                   AND c.block_hash=e.block_hash)
          FROM path p CROSS JOIN LATERAL (
               SELECT e.block_hash,e.previous_hash,e.height FROM event e
                WHERE e.dataset_id=NEW.dataset_id AND e.event_contract_version=NEW.event_contract_version
                  AND e.source=NEW.source AND e.kind=stream_kind
                  AND e.block_hash=p.previous_hash AND e.height=p.height-1
                  AND e.sidechain IS NOT DISTINCT FROM NEW.sidechain
                  AND e.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id) e
         WHERE NOT p.certified AND p.height>NEW.coverage_start_height
    ) INSERT INTO history_certified_block(dataset_id,event_contract_version,source,stream,
            sidechain,sidechain_instance_id,block_hash,previous_hash,height)
      SELECT NEW.dataset_id,NEW.event_contract_version,NEW.source,NEW.stream,
             NEW.sidechain,NEW.sidechain_instance_id,block_hash,previous_hash,height FROM path
       WHERE NOT certified
      ON CONFLICT DO NOTHING;
    RETURN NEW;
END $$;
