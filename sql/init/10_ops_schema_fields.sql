-- Schema-drift history: which JSON field paths each day's MyCareersFuture snapshot had, written by the ingest
-- DAG's check_schema_drift task (quality/snapshot_checks.py). The day's verdict, new / missing / retyped paths
-- against the last stored day, is the 'raw_schema_drift' row in clean.dq_results. Design: docs/alerts-and-monitoring.md.
--
-- Grain: ONE ROW = ONE FIELD PATH SEEN IN ONE DAY'S SNAPSHOT. Rewritten per (source, snapshot_date)
-- (delete-then-insert), so rerunning a day replaces its rows. About 150 rows a day.
--
-- This file is idempotent (IF NOT EXISTS). It runs on first container start after 01 to 09 (it needs
-- clean.dq_results from 02). On an existing warehouse apply it by hand; until then the task is skipped.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.schema_fields (
    source         TEXT    NOT NULL,       -- 'mcf_jobs': the daily MyCareersFuture snapshot
    snapshot_date  DATE    NOT NULL,
    field_path     TEXT    NOT NULL,       -- 'metadata.totalNumberOfView'; '[]' = inside a list: 'skills[].skill'
    postings       INTEGER NOT NULL,       -- postings that day with this path (present, even if null)
    sample_size    INTEGER NOT NULL,       -- postings read that day (all of them by default)
    json_types     TEXT[]  NOT NULL,       -- JSON types seen, e.g. {null,number}
    checked_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, snapshot_date, field_path)
);
