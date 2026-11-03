"""Near-real-time MyCareersFuture feed, every 30 minutes.

fetch_newest -> load_poll -> check_poll

Each run reads the first few pages (MCF_LIVE_PAGES, default 3, so 300 postings) of the same public
newest-first list the daily crawl walks, writes slim rows to data/raw/mcf_live/<date>/<time>.jsonl.gz and
appends them to raw.mcf_live_poll. The metrics.live_* views turn the rows into first-24-hour curves:
views and applications by hours since posting, which a once-a-day snapshot cannot give.

Design (L7, and the "justify it, do not over-build it" point from the streaming lectures):
  - micro-batch, not a stream: new postings arrive at about 3 a minute at midday and a posting's counters
    move over hours, so a 30-minute poll loses nothing the analysis needs. Why not Kafka: docs/live-feed.md.
  - polite: 3 calls a poll (144 a day, about 15% of one daily crawl), 2 s between pages, the same retry and
    backoff session as the daily crawl. Only open postings, only the list, no job-ID lookups.
  - idempotent: the key is (poll_ts, job_uuid) and rows are never updated, so a retried load_poll inserts
    nothing new. poll_ts is fixed in fetch_newest and passed on, so retries reuse it.
  - small atomic tasks: a failed load or check retries without calling the API again (the gz file is the
    hand-over, as in the daily DAG, so no data goes through XCom).
  - catchup=False: a missed poll cannot be fetched later (the counters have moved on). max_active_runs=1 and
    dagrun_timeout=25 min, so a slow run (API backing off) never overlaps the next one.
  - quality: check_poll stores its verdicts in raw.mcf_live_poll_dq and fails the run on a 'fail' check.
"""
import json
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

from ingestion.mcf_live import DEFAULT_PAGES, DEFAULT_PAUSE, load_poll, poll_file_path, poll_newest, \
    read_poll_file, write_poll_file
from quality.live_poll_checks import failed_checks, run_live_checks, summarise

SGT = "Asia/Singapore"
LIVE_DIR = Path("/opt/airflow/data/raw/mcf_live")


def _warehouse_conn():
    return PostgresHook(postgres_conn_id="warehouse").get_conn()


@dag(
    dag_id="mcf_live_poll",
    schedule="*/30 * * * *",       # every 30 minutes
    start_date=pendulum.datetime(2026, 11, 3, tz=SGT),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=25),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["ingestion", "mcf", "live"],
    doc_md=__doc__,
)
def mcf_live_poll():

    @task(execution_timeout=timedelta(minutes=15))
    def fetch_newest() -> dict:
        max_pages = int(os.environ.get("MCF_LIVE_PAGES", DEFAULT_PAGES))
        rows, stats = poll_newest(max_pages=max_pages, pause=DEFAULT_PAUSE)
        stats["path"] = str(write_poll_file(poll_file_path(LIVE_DIR, stats["poll_ts"]), rows))
        print(json.dumps(stats))
        return stats

    @task(task_id="load_poll")
    def load_poll_task(stats: dict) -> dict:
        rows = read_poll_file(Path(stats["path"]))
        conn = _warehouse_conn()
        try:
            stored = load_poll(conn, stats, rows)
        finally:
            conn.close()
        print(json.dumps({"poll_ts": stats["poll_ts"], "rows_in_file": len(rows), "rows_in_table": stored}))
        return stats

    @task
    def check_poll(stats: dict) -> dict:
        conn = _warehouse_conn()
        try:
            results = run_live_checks(conn, stats["poll_ts"])
        finally:
            conn.close()
        for r in results:
            print(f"{r.status.upper():4} {r.category:13} {r.name:32} observed={r.observed} threshold={r.threshold}")
        summary = summarise(results)
        if failed_checks(results):
            raise AirflowFailException(f"Live poll {stats['poll_ts']} failed checks: {summary['failed']}")
        return summary

    check_poll(load_poll_task(fetch_newest()))


mcf_live_poll()
