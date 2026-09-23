-- Permit multiple distinct point-in-time facts of one kind at the same block.
--
-- The live BMM auction can change several times while the parent block stays
-- fixed. Identity therefore includes a hash of the normalized monitor payload
-- (excluding the observation timestamp). Contract v3 rows predate that hash;
-- their envelope hash is a safe migration value because contract version is
-- already part of the identity.

ALTER TABLE event ADD COLUMN IF NOT EXISTS fact_sha256 bytea;

UPDATE event
   SET fact_sha256 = COALESCE(envelope_sha256, sha256(envelope))
 WHERE fact_sha256 IS NULL;

ALTER TABLE event ALTER COLUMN fact_sha256 SET NOT NULL;
ALTER TABLE event DROP CONSTRAINT IF EXISTS event_fact_sha256_size;
ALTER TABLE event ADD CONSTRAINT event_fact_sha256_size
    CHECK (octet_length(fact_sha256) = 32);

ALTER TABLE event DROP CONSTRAINT IF EXISTS event_identity;
ALTER TABLE event ADD CONSTRAINT event_identity
    UNIQUE NULLS NOT DISTINCT
        (dataset_id, event_contract_version, source, kind, sidechain,
         sidechain_instance_id, block_hash, fact_sha256);

CREATE INDEX IF NOT EXISTS event_bmm_requests_by_observation
    ON event (dataset_id, observed_at DESC)
    WHERE kind = 'bmm_requests';
