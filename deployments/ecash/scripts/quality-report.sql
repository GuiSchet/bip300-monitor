-- Operational counters; run read-only daily and retain the output for trends.
-- Scope is explicit: psql -v dataset=UUID -v contract=8 -f quality-report.sql.
\set ON_ERROR_STOP on
SELECT now() AS sampled_at,pg_database_size(current_database()) AS database_bytes;
SELECT kind,count(*) AS facts,sum(pg_column_size(envelope)) AS envelope_bytes,
       sum(pg_column_size(payload)) AS payload_bytes
  FROM event WHERE dataset_id=:'dataset'::uuid AND event_contract_version=:'contract'::integer
 GROUP BY kind ORDER BY kind;
SELECT consistency,count(*) AS groups FROM snapshot_group
 WHERE dataset_id=:'dataset'::uuid AND started_at>now()-interval '24 hours'
 GROUP BY consistency;
WITH samples AS (
    SELECT o.run_id,o.observed_at,
        lag(o.observed_at) OVER(PARTITION BY o.run_id ORDER BY o.capture_seq) AS prior
    FROM event_observation o JOIN event e ON e.id=o.event_id
    WHERE o.dataset_id=:'dataset'::uuid AND e.event_contract_version=:'contract'::integer
      AND e.kind='bmm_requests' AND o.observed_at>now()-interval '24 hours'
) SELECT count(*) AS samples,count(*) FILTER(WHERE observed_at-prior>interval '30 seconds') AS gaps_over_30s,
    max(observed_at-prior) AS maximum_gap FROM samples;
SELECT worker,count(*) AS failures,max(observed_at) AS latest_failure FROM observation_failure
 WHERE dataset_id=:'dataset'::uuid AND observed_at>now()-interval '24 hours' GROUP BY worker;
SELECT count(*) AS immutable_conflicts FROM event_conflict WHERE dataset_id=:'dataset'::uuid;
SELECT stream,sidechain,status,covered_tip_height,target_tip_height,last_error FROM history_coverage
 WHERE dataset_id=:'dataset'::uuid AND event_contract_version=:'contract'::integer;
