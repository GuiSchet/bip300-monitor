-- Durable health belongs to the worker that produced it.  The aggregate
-- extractor_status.last_error remains for compatibility, but is derived by
-- application transactions from these independent rows.

CREATE TABLE IF NOT EXISTS extractor_worker_status (
    run_id               uuid        NOT NULL REFERENCES extractor_run(run_id),
    worker               text        NOT NULL,
    consecutive_failures integer     NOT NULL DEFAULT 0,
    last_error           text,
    last_success_at      timestamptz,
    last_failure_at      timestamptz,
    updated_at           timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT extractor_worker_status_identity PRIMARY KEY (run_id, worker),
    CONSTRAINT extractor_worker_name_not_empty CHECK (worker <> ''),
    CONSTRAINT extractor_worker_failures_nonnegative CHECK (consecutive_failures >= 0)
);

-- Facts are immutable and may recur.  Their first observed_at value cannot
-- answer "what was observed most recently"; occurrence order lives here.
DROP INDEX IF EXISTS event_bmm_requests_by_observation;
CREATE INDEX IF NOT EXISTS event_observation_latest_by_dataset
    ON event_observation (dataset_id, observed_at DESC, observation_id DESC)
    INCLUDE (event_id, run_id, capture_method);
