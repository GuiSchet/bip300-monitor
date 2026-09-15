-- Dataset identity, extractor executions, and observation occurrences.
--
-- `event` remains the idempotent fact table.  `event_observation` records every
-- time a run captured one of those facts, so a replay stays queryable without
-- turning the fact table into an activity counter.  A fixed legacy dataset is
-- used only for rows written before this migration; new databases immediately
-- create a deployment-specific dataset in application code.

CREATE TABLE IF NOT EXISTS dataset_manifest (
    dataset_id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    network_id              text        NOT NULL,
    activation_height       integer     NOT NULL,
    activation_block_hash   text        NOT NULL,
    initial_node_commit     text        NOT NULL,
    initial_enforcer_commit text        NOT NULL,
    initial_monitor_commit  text        NOT NULL,
    initial_event_contract_version integer NOT NULL,
    capabilities            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    creation_reason         text        NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT dataset_network_not_empty CHECK (network_id <> ''),
    CONSTRAINT dataset_activation_height CHECK (activation_height >= 0),
    CONSTRAINT dataset_contract_version CHECK (initial_event_contract_version > 0),
    CONSTRAINT dataset_creation_reason_not_empty CHECK (creation_reason <> ''),
    CONSTRAINT dataset_identity UNIQUE
        (network_id, activation_height, activation_block_hash)
);

INSERT INTO dataset_manifest
    (dataset_id, network_id, activation_height, activation_block_hash,
     initial_node_commit, initial_enforcer_commit, initial_monitor_commit,
     initial_event_contract_version, capabilities, creation_reason)
VALUES
    ('00000000-0000-0000-0000-000000000001', 'legacy-unknown', 0, 'unknown',
     'unknown', 'unknown', 'unknown', 1, '[]'::jsonb,
     'Rows migrated from the pre-v4 schema; original provenance is unavailable')
ON CONFLICT (dataset_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS extractor_run (
    run_id                 uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id             uuid        NOT NULL REFERENCES dataset_manifest(dataset_id),
    source                 text        NOT NULL,
    node_commit            text        NOT NULL,
    enforcer_commit        text        NOT NULL,
    monitor_commit         text        NOT NULL,
    event_contract_version integer     NOT NULL,
    capabilities           jsonb       NOT NULL DEFAULT '[]'::jsonb,
    status                 text        NOT NULL DEFAULT 'running',
    last_capture_seq       bigint      NOT NULL DEFAULT 0,
    started_at             timestamptz NOT NULL DEFAULT now(),
    finished_at            timestamptz,
    finish_reason          text,

    CONSTRAINT extractor_run_source_not_empty CHECK (source <> ''),
    CONSTRAINT extractor_run_contract_version CHECK (event_contract_version > 0),
    CONSTRAINT extractor_run_sequence_nonnegative CHECK (last_capture_seq >= 0),
    CONSTRAINT extractor_run_status CHECK (status IN ('running', 'completed', 'failed')),
    CONSTRAINT extractor_run_finish_shape CHECK (
        (status = 'running' AND finished_at IS NULL)
        OR (status <> 'running' AND finished_at IS NOT NULL)
    )
);

ALTER TABLE event ADD COLUMN IF NOT EXISTS dataset_id uuid;
ALTER TABLE event ADD COLUMN IF NOT EXISTS event_contract_version integer;
ALTER TABLE event ADD COLUMN IF NOT EXISTS ingested_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE event ADD COLUMN IF NOT EXISTS envelope_sha256 bytea;
ALTER TABLE event ADD COLUMN IF NOT EXISTS sidechain_instance_id text;

UPDATE event
   SET dataset_id = '00000000-0000-0000-0000-000000000001'
 WHERE dataset_id IS NULL;
UPDATE event SET event_contract_version = 1 WHERE event_contract_version IS NULL;
ALTER TABLE event ALTER COLUMN dataset_id SET NOT NULL;
ALTER TABLE event ALTER COLUMN event_contract_version SET NOT NULL;

ALTER TABLE event DROP CONSTRAINT IF EXISTS event_dataset;
ALTER TABLE event ADD CONSTRAINT event_dataset
    FOREIGN KEY (dataset_id) REFERENCES dataset_manifest(dataset_id);
ALTER TABLE event DROP CONSTRAINT IF EXISTS event_envelope_sha256_size;
ALTER TABLE event ADD CONSTRAINT event_envelope_sha256_size
    CHECK (envelope_sha256 IS NULL OR octet_length(envelope_sha256) = 32);
ALTER TABLE event DROP CONSTRAINT IF EXISTS event_contract_version_positive;
ALTER TABLE event ADD CONSTRAINT event_contract_version_positive
    CHECK (event_contract_version > 0);

ALTER TABLE event DROP CONSTRAINT IF EXISTS event_identity;
ALTER TABLE event ADD CONSTRAINT event_identity
    UNIQUE NULLS NOT DISTINCT
        (dataset_id, event_contract_version, source, kind, sidechain,
         sidechain_instance_id, block_hash);

CREATE TABLE IF NOT EXISTS snapshot_group (
    snapshot_group_id uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id        uuid        NOT NULL REFERENCES dataset_manifest(dataset_id),
    run_id            uuid        NOT NULL REFERENCES extractor_run(run_id),
    capture_method    text        NOT NULL,
    started_at        timestamptz NOT NULL,
    finished_at       timestamptz NOT NULL,
    tip_before_hash   bytea       NOT NULL,
    tip_before_height integer     NOT NULL,
    tip_after_hash    bytea       NOT NULL,
    tip_after_height  integer     NOT NULL,
    consistency       text        NOT NULL,
    attempts          integer     NOT NULL,

    CONSTRAINT snapshot_group_capture_method CHECK
        (capture_method IN ('startup', 'live', 'backfill', 'poll', 'reconcile')),
    CONSTRAINT snapshot_group_consistency CHECK (consistency IN ('stable', 'changed')),
    CONSTRAINT snapshot_group_hash_sizes CHECK
        (octet_length(tip_before_hash) = 32 AND octet_length(tip_after_hash) = 32),
    CONSTRAINT snapshot_group_heights CHECK
        (tip_before_height >= 0 AND tip_after_height >= 0),
    CONSTRAINT snapshot_group_attempts_positive CHECK (attempts > 0),
    CONSTRAINT snapshot_group_time_order CHECK (finished_at >= started_at)
);

CREATE TABLE IF NOT EXISTS event_observation (
    observation_id   bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id       uuid        NOT NULL REFERENCES dataset_manifest(dataset_id),
    run_id           uuid        NOT NULL REFERENCES extractor_run(run_id),
    capture_seq      bigint      NOT NULL,
    capture_method   text        NOT NULL,
    event_id         bigint      NOT NULL REFERENCES event(id),
    snapshot_group_id uuid       REFERENCES snapshot_group(snapshot_group_id),
    observed_at      timestamptz NOT NULL,
    ingested_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT event_observation_sequence_positive CHECK (capture_seq > 0),
    CONSTRAINT event_observation_capture_method CHECK
        (capture_method IN ('startup', 'live', 'backfill', 'poll', 'reconcile')),
    CONSTRAINT event_observation_run_sequence UNIQUE (run_id, capture_seq)
);

CREATE INDEX IF NOT EXISTS event_observation_by_event
    ON event_observation (event_id, observation_id);
CREATE INDEX IF NOT EXISTS event_observation_by_dataset
    ON event_observation (dataset_id, observation_id);

CREATE TABLE IF NOT EXISTS tip_observation (
    tip_observation_id       bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id               uuid        NOT NULL REFERENCES dataset_manifest(dataset_id),
    run_id                   uuid        NOT NULL REFERENCES extractor_run(run_id),
    capture_seq              bigint      NOT NULL,
    capture_method           text        NOT NULL,
    tip_hash                 bytea       NOT NULL,
    tip_height               integer     NOT NULL,
    previous_observed_hash   bytea,
    previous_observed_height integer,
    observed_at              timestamptz NOT NULL,
    ingested_at              timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT tip_observation_sequence_positive CHECK (capture_seq > 0),
    CONSTRAINT tip_observation_capture_method CHECK
        (capture_method IN ('startup', 'live', 'backfill', 'poll', 'reconcile')),
    CONSTRAINT tip_observation_hash_sizes CHECK (
        octet_length(tip_hash) = 32
        AND (previous_observed_hash IS NULL OR octet_length(previous_observed_hash) = 32)
    ),
    CONSTRAINT tip_observation_height CHECK (tip_height >= 0),
    CONSTRAINT tip_observation_previous_pair CHECK
        ((previous_observed_hash IS NULL) = (previous_observed_height IS NULL)),
    CONSTRAINT tip_observation_run_sequence UNIQUE (run_id, capture_seq)
);

CREATE INDEX IF NOT EXISTS tip_observation_by_dataset
    ON tip_observation (dataset_id, tip_observation_id);

CREATE TABLE IF NOT EXISTS sidechain_instance (
    dataset_id            uuid        NOT NULL REFERENCES dataset_manifest(dataset_id),
    sidechain_instance_id text        NOT NULL,
    sidechain             smallint    NOT NULL,
    raw_description       bytea       NOT NULL,
    description_sha256d   bytea       NOT NULL,
    proposal_height       integer     NOT NULL,
    activation_height     integer     NOT NULL,
    first_seen_at         timestamptz NOT NULL DEFAULT now(),
    last_seen_at          timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT sidechain_instance_identity
        PRIMARY KEY (dataset_id, sidechain_instance_id),
    CONSTRAINT sidechain_instance_slot CHECK (sidechain BETWEEN 0 AND 255),
    CONSTRAINT sidechain_instance_description_hash
        CHECK (octet_length(description_sha256d) = 32),
    CONSTRAINT sidechain_instance_heights
        CHECK (proposal_height >= 0 AND activation_height >= proposal_height),
    CONSTRAINT sidechain_instance_fields UNIQUE
        (dataset_id, sidechain, description_sha256d, proposal_height, activation_height)
);

CREATE TABLE IF NOT EXISTS current_sidechain_instance (
    dataset_id            uuid        NOT NULL REFERENCES dataset_manifest(dataset_id),
    sidechain             smallint    NOT NULL,
    sidechain_instance_id text        NOT NULL,
    observed_at           timestamptz NOT NULL,

    CONSTRAINT current_sidechain_instance_identity PRIMARY KEY (dataset_id, sidechain),
    CONSTRAINT current_sidechain_instance_reference
        FOREIGN KEY (dataset_id, sidechain_instance_id)
        REFERENCES sidechain_instance(dataset_id, sidechain_instance_id),
    CONSTRAINT current_sidechain_instance_slot CHECK (sidechain BETWEEN 0 AND 255)
);

ALTER TABLE event DROP CONSTRAINT IF EXISTS event_sidechain_instance;
ALTER TABLE event ADD CONSTRAINT event_sidechain_instance
    FOREIGN KEY (dataset_id, sidechain_instance_id)
    REFERENCES sidechain_instance(dataset_id, sidechain_instance_id);

CREATE TABLE IF NOT EXISTS extractor_status (
    dataset_id            uuid        NOT NULL REFERENCES dataset_manifest(dataset_id),
    source                text        NOT NULL,
    run_id                uuid        NOT NULL REFERENCES extractor_run(run_id),
    last_tip_hash         bytea,
    last_tip_height       integer,
    last_error            text,
    updated_at            timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT extractor_status_identity PRIMARY KEY (dataset_id, source),
    CONSTRAINT extractor_status_tip_pair CHECK
        ((last_tip_hash IS NULL) = (last_tip_height IS NULL)),
    CONSTRAINT extractor_status_hash_size CHECK
        (last_tip_hash IS NULL OR octet_length(last_tip_hash) = 32),
    CONSTRAINT extractor_status_height CHECK
        (last_tip_height IS NULL OR last_tip_height >= 0)
);

ALTER TABLE history_coverage ADD COLUMN IF NOT EXISTS dataset_id uuid;
ALTER TABLE history_coverage ADD COLUMN IF NOT EXISTS sidechain_instance_id text;
ALTER TABLE history_coverage ADD COLUMN IF NOT EXISTS event_contract_version integer;
UPDATE history_coverage
   SET dataset_id = '00000000-0000-0000-0000-000000000001'
 WHERE dataset_id IS NULL;
-- Pre-v4 facts belong to normalized contract v1. Their sidechain-instance
-- provenance cannot be reconstructed safely, so it deliberately remains NULL
-- in the isolated legacy dataset. A real dataset starts fresh coverage under
-- its explicit contract version and instance identity.
UPDATE history_coverage
   SET event_contract_version = 1
 WHERE event_contract_version IS NULL;
ALTER TABLE history_coverage ALTER COLUMN dataset_id SET NOT NULL;
ALTER TABLE history_coverage ALTER COLUMN event_contract_version SET NOT NULL;
ALTER TABLE history_coverage DROP CONSTRAINT IF EXISTS history_coverage_dataset;
ALTER TABLE history_coverage ADD CONSTRAINT history_coverage_dataset
    FOREIGN KEY (dataset_id) REFERENCES dataset_manifest(dataset_id);
ALTER TABLE history_coverage DROP CONSTRAINT IF EXISTS history_coverage_contract_version_positive;
ALTER TABLE history_coverage ADD CONSTRAINT history_coverage_contract_version_positive
    CHECK (event_contract_version > 0);
ALTER TABLE history_coverage DROP CONSTRAINT IF EXISTS history_coverage_status;
ALTER TABLE history_coverage ADD CONSTRAINT history_coverage_status
    CHECK (status IN ('running', 'complete', 'error', 'superseded'));
ALTER TABLE history_coverage DROP CONSTRAINT IF EXISTS history_coverage_identity;
ALTER TABLE history_coverage ADD CONSTRAINT history_coverage_identity
    UNIQUE NULLS NOT DISTINCT
        (dataset_id, event_contract_version, source, stream, sidechain,
         sidechain_instance_id);
ALTER TABLE history_coverage DROP CONSTRAINT IF EXISTS history_coverage_sidechain_instance;
ALTER TABLE history_coverage ADD CONSTRAINT history_coverage_sidechain_instance
    FOREIGN KEY (dataset_id, sidechain_instance_id)
    REFERENCES sidechain_instance(dataset_id, sidechain_instance_id);

CREATE TABLE IF NOT EXISTS history_coverage_revision (
    revision_id bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id  uuid        NOT NULL REFERENCES dataset_manifest(dataset_id),
    event_contract_version integer NOT NULL,
    source      text        NOT NULL,
    stream      text        NOT NULL,
    sidechain   smallint,
    sidechain_instance_id text,
    operation   text        NOT NULL,
    row_data    jsonb       NOT NULL,
    changed_at  timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT history_coverage_revision_operation CHECK (operation IN ('INSERT', 'UPDATE'))
);

CREATE OR REPLACE FUNCTION record_history_coverage_revision()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO history_coverage_revision
        (dataset_id, event_contract_version, source, stream, sidechain,
         sidechain_instance_id, operation, row_data)
    VALUES
        (NEW.dataset_id, NEW.event_contract_version, NEW.source, NEW.stream,
         NEW.sidechain, NEW.sidechain_instance_id, TG_OP, to_jsonb(NEW));
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS history_coverage_revision_trigger ON history_coverage;
CREATE TRIGGER history_coverage_revision_trigger
AFTER INSERT OR UPDATE ON history_coverage
FOR EACH ROW EXECUTE FUNCTION record_history_coverage_revision();
