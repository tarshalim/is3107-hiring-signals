"""Weekly Parquet lake export of the clean fact rows (docs/lake-and-spark.md).

list_week_days -> export_day (one mapped task per snapshot day) --+-> summarise
export_dim_skill -------------------------------------------------+

Tuesdays 21:00 Singapore time, after the transform's latest possible finish (its sensor gives up at about 20:00),
so every day of the week is final. Each run covers the same week as weekly_metrics: the snapshot days after the
data interval's start date up to its end date (Wednesday to Tuesday, transform/snapshot_week.py).

Design:
  - a DAG of its own, not a task in weekly_metrics or the daily transform: the lake is an analytics copy for
    batch jobs (Spark) and the report's size numbers, not something the dashboard or the signals need. A failed
    export must not turn the metrics run red or hold up the daily path the collector depends on, and the team can
    pause it on a laptop short of memory without touching anything else.
  - weekly, not daily: nothing reads the lake within the week, and one run writes seven small files (about 7 MB
    each). A single day can be refreshed at any time with `python -m transform.lake_export --date ...`.
  - one mapped task per day (dynamic task mapping): small, atomic, and a failure retries only that day. At most two
    run at once (max_active_tis_per_dag), so the export never takes more than two worker slots or much memory.
  - idempotent: each day overwrites its own partition file in one rename (transform/lake_export.py), so a retry, a
    cleared run or a backfill gives the same files.
  - catchup=True from 20 Oct: unpausing it backfills one run per week since the first snapshot (24 Oct), in order
    (max_active_runs=1), which builds the whole lake. It is paused when first deployed.
"""
import json
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.alerts import TASK_ALERTS, on_dag_failure   # dags/common: failure and retry alerts
from transform import lake_export, snapshot_week

SGT = "Asia/Singapore"


def _warehouse_conn():
    return PostgresHook(postgres_conn_id="warehouse").get_conn()


@dag(
    dag_id="weekly_lake_export",
    schedule="0 21 * * 2",    # Tuesdays 21:00 SGT
    start_date=pendulum.datetime(2026, 10, 20, 21, tz=SGT),   # first run covers the week ending 27 Oct
    catchup=True,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), **TASK_ALERTS},
    on_failure_callback=on_dag_failure,   # run-level failures, e.g. a timed-out run (dags/common/alerts.py)
    tags=["lake", "parquet", "warehouse"],
    doc_md=__doc__,
)
def weekly_lake_export():

    @task
    def list_week_days(data_interval_start=None, data_interval_end=None) -> list:
        """The snapshot days of this run's week that are in clean.fact_job_day (an empty week maps to nothing)."""
        after, up_to = snapshot_week.week_bounds(data_interval_start, data_interval_end)
        conn = _warehouse_conn()
        try:
            days = lake_export.days_between(conn, after, up_to)
        finally:
            conn.close()
        print(json.dumps({"after": str(after), "up_to": str(up_to), "days": days}))
        return days

    @task(max_active_tis_per_dag=2)
    def export_day(snapshot_date: str) -> dict:
        conn = _warehouse_conn()
        try:
            out = lake_export.export_day(conn, snapshot_date)
        finally:
            conn.close()
        print(json.dumps(out))
        return out

    @task
    def export_dim_skill() -> dict:
        """Rewritten every run: small (about 1 MB), and it carries skill remaps to every past partition."""
        conn = _warehouse_conn()
        try:
            out = lake_export.export_dim_skill(conn)
        finally:
            conn.close()
        print(json.dumps(out))
        return out

    @task(trigger_rule="none_failed")
    def summarise(days: list, skills: dict) -> dict:
        days = list(days or [])
        out = {"days": len(days), "rows": sum(d["rows"] for d in days), "bytes": sum(d["bytes"] for d in days),
               "seconds": round(sum(d["seconds"] for d in days), 1), "dim_skill_rows": skills["rows"]}
        print(json.dumps(out))
        return out

    exported = export_day.expand(snapshot_date=list_week_days())
    summarise(exported, export_dim_skill())


weekly_lake_export()
