-- At most one process may claim to be the active writer for a dataset/source.
-- Keep the newest legacy row active so the index can be installed on records
-- that contain runs abandoned by older releases. Application startup closes
-- that remaining row transactionally before creating its replacement.

WITH ranked AS (
    SELECT run_id,
           row_number() OVER (
               PARTITION BY dataset_id, source
               ORDER BY started_at DESC, run_id DESC
           ) AS position
      FROM extractor_run
     WHERE status = 'running'
)
UPDATE extractor_run run
   SET status = 'failed',
       finished_at = now(),
       finish_reason = 'superseded while installing the single-active-run invariant'
  FROM ranked
 WHERE run.run_id = ranked.run_id
   AND ranked.position > 1;

CREATE UNIQUE INDEX IF NOT EXISTS extractor_run_one_running
    ON extractor_run (dataset_id, source)
    WHERE status = 'running';
