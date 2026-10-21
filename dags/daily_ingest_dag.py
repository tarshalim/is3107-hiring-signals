"""Daily MyCareersFuture snapshot.

fetch_snapshot -> check_snapshot -> load_raw

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
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from ingestion.mcf_client import read_snapshot, snapshot_to_file

RAW_DIR = Path("/opt/airflow/data/raw/mcf")
MIN_JOBS = 100          # fewer than this means something is wrong with the API call
MIN_COVERAGE = 0.9      # we should get at least 90% of what the API says exists


@dag(
    dag_id="daily_mcf_ingest",
    schedule="0 6 * * *",  # 06:00 every day
    start_date=pendulum.datetime(2026, 10, 21, tz="Asia/Singapore"),
    catchup=False,         # past days can't be re-fetched, so never backfill
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=10)},
    tags=["ingestion", "mcf"],
)
def daily_mcf_ingest():

    @task
    def fetch_snapshot(ds=None) -> dict:
        max_pages = int(os.environ.get("MCF_MAX_PAGES", "0"))
        stats = snapshot_to_file(RAW_DIR / f"{ds}.jsonl.gz", max_pages=max_pages)
        stats["snapshot_date"] = ds
        print(json.dumps(stats))
        return stats

    @task
    def check_snapshot(stats: dict) -> dict:
        n, total = stats["jobs_fetched"], stats["api_total"]
        if n < MIN_JOBS and not int(os.environ.get("MCF_MAX_PAGES", "0")):
            raise AirflowFailException(f"Only {n} postings fetched; expected at least {MIN_JOBS}")
        if total and n < MIN_COVERAGE * total and not int(os.environ.get("MCF_MAX_PAGES", "0")):
            raise AirflowFailException(f"Fetched {n} of {total} postings ({n / total:.0%})")
        return stats

    @task
    def load_raw(stats: dict) -> int:
        ds = stats["snapshot_date"]
        rows = [(ds, job["uuid"], json.dumps(job)) for job in read_snapshot(Path(stats["path"]))]
        hook = PostgresHook(postgres_conn_id="warehouse")
        with hook.get_conn() as conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO raw.mcf_job_snapshots (snapshot_date, job_uuid, payload)
                VALUES %s
                ON CONFLICT (snapshot_date, job_uuid)
                DO UPDATE SET payload = EXCLUDED.payload, fetched_at = now()
                """,
                rows,
                page_size=1000,
            )
            cur.execute(
                """
                INSERT INTO raw.ingest_runs (snapshot_date, pages_fetched, jobs_fetched, api_total, seconds_taken)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_date) DO UPDATE SET
                    pages_fetched = EXCLUDED.pages_fetched, jobs_fetched = EXCLUDED.jobs_fetched,
                    api_total = EXCLUDED.api_total, seconds_taken = EXCLUDED.seconds_taken,
                    finished_at = now()
                """,
                (ds, stats["pages_fetched"], stats["jobs_fetched"], stats["api_total"], stats["seconds_taken"]),
            )
        return len(rows)

    load_raw(check_snapshot(fetch_snapshot()))


daily_mcf_ingest()
