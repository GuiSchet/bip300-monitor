-- Operational counters; run read-only daily and retain the output for trends.
-- Scope is explicit: psql -v dataset=UUID -v contract=9 -f quality-report.sql.
\set ON_ERROR_STOP on
SELECT now() AS sampled_at,pg_database_size(current_database()) AS database_bytes;
SELECT kind,count(*) AS facts,sum(pg_column_size(envelope)) AS envelope_bytes,
       sum(pg_column_size(payload)) AS payload_bytes
  FROM event WHERE dataset_id=:'dataset'::uuid AND event_contract_version=:'contract'::integer
 GROUP BY kind ORDER BY kind;
SELECT consistency,count(*) AS groups FROM snapshot_group
 WHERE dataset_id=:'dataset'::uuid AND started_at>now()-interval '24 hours'
 GROUP BY consistency;
-- Gaps are measured across runs: a restart storm is exactly where samples stop.
WITH samples AS (
    SELECT o.observed_at,g.consistency,
        lag(o.observed_at) OVER(ORDER BY o.observed_at,o.observation_id) AS prior
    FROM event_observation o JOIN event e ON e.id=o.event_id
    LEFT JOIN snapshot_group g ON g.snapshot_group_id=o.snapshot_group_id
    WHERE o.dataset_id=:'dataset'::uuid AND e.event_contract_version=:'contract'::integer
      AND e.kind='bmm_requests' AND o.observed_at>now()-interval '24 hours'
) SELECT count(*) AS samples,count(*) FILTER(WHERE consistency IS DISTINCT FROM 'tip_matched') AS untrusted_samples,
    count(*) FILTER(WHERE observed_at-prior>interval '30 seconds') AS gaps_over_30s,
    max(observed_at-prior) AS maximum_gap FROM samples;
-- Each boundary closes an interval whose global transitions are unknown.
SELECT count(*) AS subscription_boundaries,
       count(*) FILTER(WHERE payload #> '{monitor_event,Enforcer,event,MainchainTransition,gap_start}' IS NOT NULL) AS bounded_gaps
  FROM event WHERE dataset_id=:'dataset'::uuid AND event_contract_version=:'contract'::integer
   AND kind='mainchain_transition'
   AND (payload #>> '{monitor_event,Enforcer,event,MainchainTransition,action}')::integer=3
   AND observed_at>now()-interval '24 hours';
SELECT worker,count(*) AS failures,max(observed_at) AS latest_failure FROM observation_failure
 WHERE dataset_id=:'dataset'::uuid AND observed_at>now()-interval '24 hours' GROUP BY worker;
SELECT count(*) AS immutable_conflicts FROM event_conflict WHERE dataset_id=:'dataset'::uuid;
SELECT stream,sidechain,status,covered_tip_height,target_tip_height,last_error FROM history_coverage
 WHERE dataset_id=:'dataset'::uuid AND event_contract_version=:'contract'::integer;
