ALTER TABLE snapshot_group ADD COLUMN revision_before text;
ALTER TABLE snapshot_group ADD COLUMN revision_after text;
-- v7 stores evidence and certification separately. Conflicting immutable
-- payloads remain queryable; neither their presence nor an orphan's height
-- constitutes coverage of the selected branch.
ALTER TABLE event ADD COLUMN IF NOT EXISTS previous_hash bytea;
ALTER TABLE event ADD CONSTRAINT event_previous_hash_size
    CHECK (previous_hash IS NULL OR octet_length(previous_hash) = 32);
CREATE INDEX event_block_lookup ON event
    (dataset_id, event_contract_version, source, kind, block_hash)
    INCLUDE (height, previous_hash, sidechain, sidechain_instance_id);

CREATE TABLE event_conflict (
    conflict_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id uuid NOT NULL REFERENCES dataset_manifest(dataset_id),
    first_event_id bigint NOT NULL REFERENCES event(id),
    conflicting_event_id bigint NOT NULL REFERENCES event(id),
    detected_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (first_event_id, conflicting_event_id),
    CHECK (first_event_id < conflicting_event_id)
);

CREATE FUNCTION observe_block_fact() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.kind IN ('block_connected', 'bip300_block_delta') THEN
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
CREATE TRIGGER event_observe_conflicts AFTER INSERT ON event
    FOR EACH ROW EXECUTE FUNCTION observe_block_fact();

CREATE VIEW state_snapshot_stable AS
SELECT e.*, o.observation_id, o.run_id, o.capture_seq, o.snapshot_group_id,
       o.observed_at AS occurrence_observed_at, s.started_at, s.finished_at
  FROM event e JOIN event_observation o ON o.event_id = e.id
  JOIN snapshot_group s USING (snapshot_group_id)
 WHERE s.consistency = 'stable'
   AND o.dataset_id=e.dataset_id AND s.dataset_id=e.dataset_id AND s.run_id=o.run_id
   AND s.tip_before_hash=s.tip_after_hash AND s.tip_before_height=s.tip_after_height
   AND s.revision_before IS NOT NULL AND s.revision_before=s.revision_after
   AND e.block_hash=s.tip_before_hash AND e.height=s.tip_before_height AND e.kind IN
       ('sidechain_proposals', 'active_sidechains', 'ctip', 'withdrawal_bundle_proposals');

CREATE TABLE observation_failure (
    failure_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id uuid NOT NULL REFERENCES dataset_manifest(dataset_id),
    run_id uuid NOT NULL REFERENCES extractor_run(run_id),
    worker text NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT now(),
    error text NOT NULL
);
CREATE INDEX observation_failure_dataset ON observation_failure(dataset_id, failure_id);

-- Membership means that this block's entire prefix down to the scope's
-- activation was proved. Live facts alone never populate this table.
CREATE TABLE history_certified_block (
    dataset_id uuid NOT NULL REFERENCES dataset_manifest(dataset_id),
    event_contract_version integer NOT NULL,
    source text NOT NULL,
    stream text NOT NULL,
    sidechain smallint,
    sidechain_instance_id text,
    block_hash bytea NOT NULL CHECK (octet_length(block_hash) = 32),
    previous_hash bytea NOT NULL CHECK (octet_length(previous_hash) = 32),
    height integer NOT NULL CHECK (height >= 0),
    certified_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE NULLS NOT DISTINCT(dataset_id, event_contract_version, source,
        stream, sidechain, sidechain_instance_id, block_hash)
);

CREATE FUNCTION certify_history() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected bigint; actual bigint; start_hash bytea;
BEGIN
    IF NEW.status <> 'complete' THEN RETURN NEW; END IF;
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
           AND e.source=NEW.source AND e.kind=CASE NEW.stream WHEN 'block' THEN 'block_connected' ELSE 'bip300_block_delta' END
           AND e.sidechain IS NOT DISTINCT FROM NEW.sidechain
           AND e.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id
           AND e.block_hash=NEW.target_tip_hash
        UNION ALL
        SELECT e.block_hash,e.previous_hash,e.height,
               EXISTS(SELECT 1 FROM history_certified_block c WHERE
                   (c.dataset_id,c.event_contract_version,c.source,c.stream) =
                   (NEW.dataset_id,NEW.event_contract_version,NEW.source,NEW.stream)
                   AND c.sidechain IS NOT DISTINCT FROM NEW.sidechain
                   AND c.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id
                   AND c.block_hash=e.block_hash)
          FROM path p JOIN event e ON e.block_hash=p.previous_hash AND e.height=p.height-1
         WHERE NOT p.certified AND p.height>NEW.coverage_start_height
           AND e.dataset_id=NEW.dataset_id AND e.event_contract_version=NEW.event_contract_version
           AND e.source=NEW.source AND e.kind=CASE NEW.stream WHEN 'block' THEN 'block_connected' ELSE 'bip300_block_delta' END
           AND e.sidechain IS NOT DISTINCT FROM NEW.sidechain
           AND e.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id
    ), checked AS (
        SELECT count(*) AS n,count(DISTINCT block_hash) AS unique_n,min(height) AS low,
               bool_or(certified) AS joined, max(height) AS high FROM path
    )
    SELECT n, CASE WHEN n=unique_n AND high=NEW.target_tip_height
                       AND (joined OR low=NEW.coverage_start_height)
                   THEN high-low+1 ELSE -1 END
      INTO actual, expected FROM checked;
    IF actual=0 OR actual<>expected THEN RAISE EXCEPTION 'history is missing, discontinuous or ambiguous'; END IF;
    -- The application has already checked the activation hash and floor before
    -- advancing. The independent operator verifier checks the full lineage too.
    WITH RECURSIVE path AS (
        SELECT e.block_hash,e.previous_hash,e.height FROM event e
         WHERE e.dataset_id=NEW.dataset_id AND e.event_contract_version=NEW.event_contract_version
           AND e.source=NEW.source AND e.kind=CASE NEW.stream WHEN 'block' THEN 'block_connected' ELSE 'bip300_block_delta' END
           AND e.sidechain IS NOT DISTINCT FROM NEW.sidechain
           AND e.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id
           AND e.block_hash=NEW.target_tip_hash
        UNION ALL
        SELECT e.block_hash,e.previous_hash,e.height FROM path p JOIN event e
            ON e.block_hash=p.previous_hash AND e.height=p.height-1
         WHERE p.height>NEW.coverage_start_height
           AND NOT EXISTS(SELECT 1 FROM history_certified_block c WHERE
               (c.dataset_id,c.event_contract_version,c.source,c.stream)=
               (NEW.dataset_id,NEW.event_contract_version,NEW.source,NEW.stream)
               AND c.sidechain IS NOT DISTINCT FROM NEW.sidechain
               AND c.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id
               AND c.block_hash=p.block_hash)
           AND e.dataset_id=NEW.dataset_id AND e.event_contract_version=NEW.event_contract_version
           AND e.source=NEW.source AND e.kind=CASE NEW.stream WHEN 'block' THEN 'block_connected' ELSE 'bip300_block_delta' END
           AND e.sidechain IS NOT DISTINCT FROM NEW.sidechain
           AND e.sidechain_instance_id IS NOT DISTINCT FROM NEW.sidechain_instance_id
    ) INSERT INTO history_certified_block(dataset_id,event_contract_version,source,stream,
            sidechain,sidechain_instance_id,block_hash,previous_hash,height)
      SELECT NEW.dataset_id,NEW.event_contract_version,NEW.source,NEW.stream,
             NEW.sidechain,NEW.sidechain_instance_id,block_hash,previous_hash,height FROM path
      ON CONFLICT DO NOTHING;
    RETURN NEW;
END $$;
CREATE TRIGGER history_certify BEFORE INSERT OR UPDATE ON history_coverage
    FOR EACH ROW EXECUTE FUNCTION certify_history();

CREATE TABLE bmm_fee_job (
    dataset_id uuid NOT NULL REFERENCES dataset_manifest(dataset_id),
    event_contract_version integer NOT NULL,
    source text NOT NULL,
    source_event_id bigint NOT NULL REFERENCES event(id),
    last_observed_at timestamptz NOT NULL DEFAULT now(),
    next_retry_at timestamptz,
    PRIMARY KEY(dataset_id,event_contract_version,source,source_event_id)
);
CREATE INDEX bmm_fee_due ON bmm_fee_job(dataset_id,event_contract_version,source,next_retry_at)
    WHERE next_retry_at IS NOT NULL;
