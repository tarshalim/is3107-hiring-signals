-- Operations schema: how the pipeline itself is doing, kept apart from the data it produces.
-- ops.alert_log gets one row per task failure, task retry or failed DAG run, written by the shared Airflow
-- callback in dags/common/alerts.py (wired into every DAG's default_args). Design and deploy: docs/alerts-and-monitoring.md.
--
-- Why a table and not only the Airflow log (wk11 [133]: store results in a separate database): the dashboard's
-- Pipeline health tab and the report read it, and it survives Airflow's own log cleanup.
--
-- This file is idempotent (IF NOT EXISTS). It runs on first container start after 01 to 08. On an existing
-- warehouse apply it by hand (docs/alerts-and-monitoring.md, "Deploy"). Until it is applied the callback only
-- logs a warning and the DAGs run exactly as before.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.alert_log (
    alert_id     BIGSERIAL   PRIMARY KEY,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    event        TEXT        NOT NULL CHECK (event IN ('task_failed', 'task_retry', 'dag_run_failed')),
    dag_id       TEXT        NOT NULL,
    task_id      TEXT,                    -- NULL for a run-level event (dag_run_failed)
    run_id       TEXT,
    try_number   INTEGER,                 -- the try that failed; NULL for a run-level event
    exception    TEXT,                    -- 'ExceptionType: message', first 2,000 characters
    log_url      TEXT,                    -- link to the task log in the Airflow UI
    notified     BOOLEAN     NOT NULL DEFAULT FALSE,   -- a Telegram message was sent
    -- One row per event: a callback that runs twice for the same try writes once (Postgres 15+ syntax).
    CONSTRAINT alert_log_event_key UNIQUE NULLS NOT DISTINCT (dag_id, task_id, run_id, try_number, event)
);
-- The dashboard reads the latest events; the report counts them per DAG.
CREATE INDEX IF NOT EXISTS alert_log_created_idx ON ops.alert_log (created_at DESC);
CREATE INDEX IF NOT EXISTS alert_log_dag_idx ON ops.alert_log (dag_id, created_at DESC);
