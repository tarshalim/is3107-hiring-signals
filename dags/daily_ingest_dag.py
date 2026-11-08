"""Daily MyCareersFuture snapshot.

fetch_snapshot -> check_snapshot -+-> load_raw
                                  +-> check_schema_drift   (warn only; beside load_raw, never in its way)

Runs once a day (Singapore time). Each run saves the API's postings to
data/raw/mcf/<date>.jsonl.gz and loads them into raw.mcf_job_snapshots.
Cleaning, skill standardisation and weekly metrics are separate steps
that read from the raw table.
"""
import json
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

try:  # Airflow 3
    from airflow.sdk import get_current_context
except ImportError:  # Airflow 2
    from airflow.operators.python import get_current_context

from common.alerts import TASK_ALERTS, on_dag_failure   # dags/common: failure and retry alerts
from ingestion.mcf_client import read_snapshot, read_snapshot_batches, snapshot_to_file
from quality.snapshot_checks import (DRIFT_SAMPLE, drift_tables_ready, field_stats, record_schema_drift,
                                     snapshot_problem)

RAW_DIR = Path("/opt/airflow/data/raw/mcf")
MIN_JOBS = 100          # fewer than this means something is wrong with the API call
MIN_COVERAGE = 0.9      # we should get at least 90% of what the API says exists
BATCH_SIZE = 1000       # rows per INSERT when loading into Postgres


@dag(
    dag_id="daily_mcf_ingest",
    schedule="0 10 * * *",  # 10:00 every day
    start_date=pendulum.datetime(2026, 10, 21, tz="Asia/Singapore"),
    catchup=False,         # past days can't be re-fetched, so never backfill
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=10), **TASK_ALERTS},
    on_failure_callback=on_dag_failure,   # run-level failures, e.g. a timed-out run (dags/common/alerts.py)
    tags=["ingestion", "mcf"],
)
def daily_mcf_ingest():

    @task
    def fetch_snapshot() -> dict:
        # Label the snapshot with the day it was actually fetched, in Singapore time.
        # (Airflow's `ds` is the start of the data interval in UTC, which would be 2 days behind.)
        snapshot_date = pendulum.now("Asia/Singapore").to_date_string()
        max_pages = int(os.environ.get("MCF_MAX_PAGES", "0"))
        stats = snapshot_to_file(RAW_DIR / f"{snapshot_date}.jsonl.gz", max_pages=max_pages)
        stats["snapshot_date"] = snapshot_date
        # which try of this task wrote the day (1 = no retry), for the Pipeline health tab's reliability numbers
        stats["fetch_attempts"] = get_current_context()["ti"].try_number
        print(json.dumps(stats))
        return stats

    @task
    def check_snapshot(stats: dict) -> dict:
        trial_run = int(os.environ.get("MCF_MAX_PAGES", "0")) > 0
        problem = snapshot_problem(stats, trial_run, MIN_JOBS, MIN_COVERAGE)
        if problem:
            raise AirflowFailException(problem)
        return stats

    @task
    def load_raw(stats: dict) -> int:
        ds = stats["snapshot_date"]
        hook = PostgresHook(postgres_conn_id="warehouse")
        conn = hook.get_conn()
        loaded = 0
        try:
            with conn, conn.cursor() as cur:  # `with conn` commits once at the end
                # Insert in batches so the whole day's postings never sit in memory at once.
                for batch in read_snapshot_batches(Path(stats["path"]), BATCH_SIZE):
                    rows = [(ds, job["uuid"], json.dumps(job)) for job in batch]
                    execute_values(
                        cur,
                        """
                        INSERT INTO raw.mcf_job_snapshots (snapshot_date, job_uuid, payload)
                        VALUES %s
                        ON CONFLICT (snapshot_date, job_uuid)
                        DO UPDATE SET payload = EXCLUDED.payload, fetched_at = now()
                        """,
                        rows,
                        page_size=BATCH_SIZE,
                    )
                    loaded += len(rows)
                cur.execute(
                    """
                    INSERT INTO raw.ingest_runs (snapshot_date, pages_fetched, jobs_fetched, api_total, seconds_taken,
                                                 fetch_attempts, retries_429)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (snapshot_date) DO UPDATE SET
                        pages_fetched = EXCLUDED.pages_fetched, jobs_fetched = EXCLUDED.jobs_fetched,
                        api_total = EXCLUDED.api_total, seconds_taken = EXCLUDED.seconds_taken,
                        fetch_attempts = EXCLUDED.fetch_attempts, retries_429 = EXCLUDED.retries_429,
                        finished_at = now()
                    """,
                    (ds, stats["pages_fetched"], stats["jobs_fetched"], stats["api_total"], stats["seconds_taken"],
                     stats.get("fetch_attempts"), stats.get("retries_429")),
                )
        finally:
            conn.close()
        return loaded

    @task
    def check_schema_drift(stats: dict) -> dict:
        """Compare the file's JSON field paths with the last stored day (quality/snapshot_checks.py) and record
        the verdict as raw_schema_drift in clean.dq_results. Warn only, and a separate task beside load_raw, so a
        changed schema or a failure here can never stop the day's load. Reads the gz file, not the table, so it
        does not wait for load_raw. About 22 s for 90,000 postings."""
        if int(os.environ.get("MCF_MAX_PAGES", "0")) > 0:
            raise AirflowSkipException("trial run: a few pages would make a misleading baseline")
        conn = PostgresHook(postgres_conn_id="warehouse").get_conn()
        try:
            if not drift_tables_ready(conn):
                raise AirflowSkipException("ops.schema_fields is missing: apply sql/init/11_ops_schema_fields.sql")
            fields, read = field_stats(read_snapshot(Path(stats["path"])), limit=DRIFT_SAMPLE)
            out = record_schema_drift(conn, stats["snapshot_date"], fields, read)
        finally:
            conn.close()
        print(json.dumps(out))
        return out

    checked = check_snapshot(fetch_snapshot())
    load_raw(checked)
    check_schema_drift(checked)


daily_mcf_ingest()
