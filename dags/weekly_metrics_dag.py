"""Weekly dashboard metrics: the MOM comparison and the hiring signals, one week per run.

refresh_mom_stats -> compare_mcf_with_mom

                                       +-> build_barrier_skill -------------+
                                       +-> build_shortage_skill ------------+
pick_snapshot -> build_posting_week ---+-> build_unrealistic_requirements --+-> check_and_log
                                       +-> build_competitiveness -----------+
                                       +-> build_skill_demand --------------+
                                       +-> build_skill_salary --------------+

Runs every Tuesday at 14:00 Singapore time, after that day's ingest (10:00) and transform (10:30, whose
sensor gives up by 13:30 at the latest), so it normally sees Tuesday's snapshot. If the transform is
very late, the comparison and the signals use Monday's snapshot instead; they never read a half-loaded
day, because the transform loads each day in one transaction.

The two branches are independent: the MOM comparison writes metrics.mcf_vs_mom_share
(sql/init/03_mom_vacancies.sql), the signal tasks write the tables of sql/init/05_metrics_signals.sql
(docs/weekly-metrics.md). A failed MOM download does not stop the signals, and the other way round, but
either failure fails the run. Both read the same week and the same snapshot, so their numbers line up.

Why weekly and not daily: MOM publishes one new quarter every three months, and occupation shares in a
~90k-posting snapshot move very little from one day to the next. Weekly gives one history point per week
in metrics.mcf_vs_mom_share and in every signal table, which is plenty for the dashboard and the report.

Design:
  - one week per run: each run covers its data interval (Tuesday 14:00 to the next Tuesday 14:00 SGT)
    and uses the LATEST snapshot of that week (transform/snapshot_week.py), never max(snapshot_date) of
    the whole table. A week with no snapshot fails the run with a message naming the week. One snapshot
    is enough, so a missed day changes nothing.
  - catchup=True from 20 Oct: our first snapshot is 24 Oct, so the first run is the week ending Tue
    27 Oct and Airflow backfills one run per week since then. max_active_runs=1 runs them in order.
    Every MCF-reading task (compare_mcf_with_mom, pick_snapshot) takes the week from the run's data
    interval in the same way, or all backfilled runs would compute the same thing.
  - MOM: refresh_mom_stats downloads the whole MOM series (about 450 rows, a few seconds) every run
    instead of checking for a new quarter: simpler, and MOM sometimes revises past quarters. Every run,
    backfilled ones included, compares with the latest MOM quarter available.
  - signals (metrics/): pick_snapshot resolves the week and its snapshot once and records them in
    metrics.run_log (week_end_date = the interval end's Singapore date, snapshot_date = the snapshot
    used). build_posting_week stores the open postings of the week end, then the six signal tables build
    in parallel: they read only metrics.posting_week and clean, never each other, so a failure reruns
    only that table. check_and_log runs range checks on the week's rows and fails the run on a 'fail'
    check (the rows stay for inspection, like the transform's data-quality step).
  - idempotent: the MOM tables are upserts keyed on quarter + occupation; the comparison deletes and
    rewrites the rows of its week's snapshot dates in one transaction; every signal table deletes and
    rewrites its week's rows (week_end_date) in one transaction. So a retry or a cleared run replaces
    its rows and never duplicates them.
  - no sensor on the transform DAG: the 14:00 slot is after the transform's sensor timeout, and
    backfilled weeks are long finished.
"""
import json
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

from metrics.build_week import (SIGNAL_TABLES, build_table, finish_run_log, resolve_week, run_metric_checks,
                                start_run_log)
from quality.clean_checks import summarise
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

    @task
    def pick_snapshot(data_interval_start=None, data_interval_end=None, run_id=None) -> dict:
        """This run's week and its latest snapshot (the same rule as compare_mcf_with_mom), written to
        metrics.run_log before any signal table is built. Airflow fills in the data interval and run_id."""
        after, up_to = snapshot_week.week_bounds(data_interval_start, data_interval_end)
        conn = _warehouse_conn()
        try:
            week = resolve_week(conn, after, up_to)
            start_run_log(conn, week, run_id)
        except snapshot_week.NoSnapshotInWeek as e:
            # Fail without retries, as compare_mcf_with_mom: nothing is built for a week without a snapshot.
            raise AirflowFailException(str(e)) from e
        finally:
            conn.close()
        print(json.dumps({k: v for k, v in week.items() if k != "thresholds"}, default=str))
        return week

    @task
    def build_posting_week(week: dict) -> dict:
        """The open postings at the week end, with the posting-level flags every signal reads."""
        conn = _warehouse_conn()
        try:
            return build_table(conn, "posting_week", week)
        finally:
            conn.close()

    @task
    def build_signal(week: dict, table: str) -> dict:
        """One signal table for the week (task id build_<table>)."""
        conn = _warehouse_conn()
        try:
            return build_table(conn, table, week)
        finally:
            conn.close()

    @task
    def check_and_log(week: dict, posting: dict, signals: list) -> dict:
        """Range checks on the week's signal rows, then rows, seconds and verdicts into metrics.run_log."""
        conn = _warehouse_conn()
        try:
            results = run_metric_checks(conn, week["week_end_date"])
            status = finish_run_log(conn, week, [posting, *signals], results)
        finally:
            conn.close()
        for r in results:
            print(f"{r.status.upper():4} {r.category:13} {r.name:34} observed={r.observed} threshold={r.threshold}")
        summary = summarise(results)
        if status != "ok":
            raise AirflowFailException(f"Metrics checks failed for week ending {week['week_end_date']}: {summary['failed']}")
        return summary

    compare_mcf_with_mom(refresh_mom_stats())

    week = pick_snapshot()
    posting = build_posting_week(week)
    signals = []
    for table in SIGNAL_TABLES:
        built = build_signal.override(task_id=f"build_{table}")(week, table)
        posting >> built
        signals.append(built)
    check_and_log(week, posting, signals)


weekly_metrics()
