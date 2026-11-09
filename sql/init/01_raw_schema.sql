-- Raw layer: one row per job posting per daily snapshot, stored exactly as the API returned it.
-- Views / applications / reposts change every day and cannot be recovered later,
-- so we keep every snapshot and clean it in later steps.
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.mcf_job_snapshots (
    snapshot_date  DATE        NOT NULL,
    job_uuid       TEXT        NOT NULL,
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload        JSONB       NOT NULL,
    PRIMARY KEY (snapshot_date, job_uuid)   -- re-running a day overwrites, never duplicates
);

-- One row per DAG run, for the performance section of the report.
CREATE TABLE IF NOT EXISTS raw.ingest_runs (
    snapshot_date  DATE        PRIMARY KEY,
    pages_fetched  INTEGER     NOT NULL,
    jobs_fetched   INTEGER     NOT NULL,
    api_total      INTEGER,
    seconds_taken  NUMERIC     NOT NULL,
    finished_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Added 8 Nov 2026, for the Pipeline health tab (idempotent; on an existing warehouse apply this file once by hand).
-- NULL = not recorded: days before 8 Nov come from ops/ingest_retries.py, which reads the Airflow task logs.
ALTER TABLE raw.ingest_runs ADD COLUMN IF NOT EXISTS fetch_attempts SMALLINT;  -- try of fetch_snapshot that wrote the day
ALTER TABLE raw.ingest_runs ADD COLUMN IF NOT EXISTS retries_429    INTEGER;   -- HTTP 429 retries in that try
