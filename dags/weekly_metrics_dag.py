"""Weekly dashboard metrics.

refresh_mom_stats -> compare_mcf_with_mom

Runs every Tuesday at 14:00 Singapore time, after that day's ingest (10:00) and transform (10:30, whose
sensor gives up by 13:30 at the latest), so it normally sees Tuesday's snapshot. If the transform is
very late, the comparison uses Monday's snapshot instead; it never reads a half-loaded day, because the
transform loads each day in one transaction.
Only the MOM comparison is here for now; the other dashboard metrics will be added as more tasks.

Why weekly and not daily: MOM publishes one new quarter every three months, and occupation shares in a
~90k-posting snapshot move very little from one day to the next. Weekly gives one history point per week
in metrics.mcf_vs_mom_share, which is plenty for the dashboard and the report.

Design:
  - one week per run: each run covers its data interval (Tuesday 14:00 to the next Tuesday 14:00 SGT)
    and uses the LATEST snapshot of that week (transform/snapshot_week.py), never max(snapshot_date) of
    the whole table. A week with no snapshot fails the run with a message naming the week. One snapshot
    is enough, so a missed day changes nothing.
  - catchup=True from 20 Oct: our first snapshot is 24 Oct, so the first run is the week ending Tue
    27 Oct and Airflow backfills one run per week since then. max_active_runs=1 runs them in order.
    Every MCF-reading task added later must take the week from the run's data interval in the same way,
    or all backfilled runs compute the same thing.
  - MOM: refresh_mom_stats downloads the whole MOM series (about 450 rows, a few seconds) every run
    instead of checking for a new quarter: simpler, and MOM sometimes revises past quarters. Every run,
    backfilled ones included, compares with the latest MOM quarter available.
  - idempotent: the MOM tables are upserts keyed on quarter + occupation; the comparison deletes and
    rewrites the rows of its week's snapshot dates in one transaction. So a retry or a cleared run
    replaces its rows and never duplicates them.
  - no sensor on the transform DAG: the 14:00 slot is after the transform's sensor timeout, and
    backfilled weeks are long finished.
"""
import json
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

from transform import mom_vacancies, snapshot_week

SGT = "Asia/Singapore"


def _warehouse_conn():
    return PostgresHook(postgres_conn_id="warehouse").get_conn()


@dag(
    dag_id="weekly_metrics",
    schedule="0 14 * * 2",   # Tuesdays 14:00 SGT
    start_date=pendulum.datetime(2026, 10, 20, 14, tz=SGT),   # first run covers the week ending 27 Oct
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["metrics", "mom"],
    doc_md=__doc__,
)
def weekly_metrics():

    @task
    def refresh_mom_stats() -> dict:
        """Download MOM vacancies by occupation from data.gov.sg into raw and clean tables."""
        conn = _warehouse_conn()
        try:
            stats = mom_vacancies.refresh_mom_stats(conn)
        finally:
            conn.close()
        print(json.dumps(stats, default=str))
        if stats["split_total_mismatches"]:
            print("WARNING: Split A and Split B totals differ in", stats["split_total_mismatches"])
        return stats

    @task
    def compare_mcf_with_mom(mom_stats: dict, data_interval_start=None, data_interval_end=None) -> dict:
        """MCF share of postings vs MOM share of vacancies, per occupation group, for this run's week.
        Airflow fills in data_interval_start/end from the run."""
        after, up_to = snapshot_week.week_bounds(data_interval_start, data_interval_end)
        conn = _warehouse_conn()
        try:
            stats = mom_vacancies.build_comparison(conn, after, up_to)
        except snapshot_week.NoSnapshotInWeek as e:
            # Fail without retries: waiting will not create a snapshot for a past week.
            raise AirflowFailException(str(e)) from e
        finally:
            conn.close()
        print(json.dumps(stats, default=str))
        return stats

    compare_mcf_with_mom(refresh_mom_stats())


weekly_metrics()
