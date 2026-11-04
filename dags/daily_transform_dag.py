"""Daily clean-and-warehouse load (role 2).

wait_for_raw ─┐
              ├─> transform_snapshot -> run_quality_checks -> log_run_stats
load_reference┘

Runs every day at 10:30 Singapore time, after daily_mcf_ingest (10:00, about 24 minutes).
Same TaskFlow style as dags/daily_ingest_dag.py (works on the team's Airflow 2.10 image and on the
course's Airflow 3.0.1 docker-compose: the decorators, PostgresHook and PokeReturnValue exist in both).

Design, following L7:
  - small atomic tasks with explicit dependencies: a failure in the quality checks reruns only that
    task, not the 1 minute parse. wait_for_raw and load_reference have no dependency on each other,
    so Airflow runs them in parallel (the lecturer's "separate blocks" point).
  - idempotent: transform_snapshot is delete-then-insert per snapshot_date plus ON CONFLICT upserts
    (transform/transform_snapshot.py), so a retry or a manual rerun gives the same tables.
  - retries with a delay for temporary errors (database restarting), and a sensor timeout at about
    20:00 SGT for the permanent case (the raw day never arrives) so the run fails clearly instead of
    hanging. A sensor timeout is not retried.
  - which day: the Singapore date of the run's data_interval_end (transform/run_context.py), never
    Airflow's UTC-based `ds` (the start of the data interval, a day behind) and never the wall clock:
    clearing an old run must reload that run's day, not today's. (The ingest DAG does use the wall
    clock, on purpose: the API only serves today's postings, so a past day cannot be re-fetched.)
  - catchup=False: a past day whose raw snapshot was never collected cannot be fetched later, so
    backfilling makes no sense. An old day that IS in raw can be reloaded by clearing its run, or by
    triggering the DAG with {"snapshot_date": "2026-10-24"} in the run config.

Why a sensor on the warehouse and not a Dataset/Asset dependency on the ingest DAG:
  1. The contract is "the day's rows are in raw.ingest_runs", whoever put them there: the ingest DAG,
     or a teammate replaying a saved file with ingestion/load_snapshot_file.py (the lecturer said a
     mock replay is fine, and it is how this layer was built and verified without hitting the API).
  2. It needs no change to the collector's DAG file (Datasets need `outlets=[...]` on load_raw and
     both DAGs deployed together).
  3. The same code runs on Airflow 2.10 (Dataset) and 3.0 (renamed Asset). The switch is three lines
     if the team prefers it later: outlets=[Dataset("postgres://warehouse/raw.mcf_job_snapshots")] on
     load_raw, and schedule=[that dataset] here instead of the cron plus sensor.
  mode="reschedule" frees the worker slot between pokes, so the sensor does not hold a slot for hours.
"""
import json
import time
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import PokeReturnValue

try:  # Airflow 3
    from airflow.sdk import get_current_context
except ImportError:  # Airflow 2
    from airflow.operators.python import get_current_context

from quality.clean_checks import failed_checks, run_checks, summarise
from transform.load_reference import load_reference
from transform.run_context import run_timings, snapshot_date_for_run
from transform.skill_mapping import match_rate_report
from transform.transform_snapshot import transform_snapshot, write_run_summary

SGT = "Asia/Singapore"
POKE_INTERVAL = 5 * 60          # check for the raw day every 5 minutes
# Give up at about 20:00 SGT (9.5 hours after the 10:30 start). Ingest normally ends by 10:45, but on
# 27 and 28 Oct it ran until 14:49 and 15:31, and the old 3 hour timeout (13:30) missed both days.
SENSOR_TIMEOUT = int(9.5 * 60 * 60)


def _warehouse_conn():
    return PostgresHook(postgres_conn_id="warehouse").get_conn()


def _snapshot_date_from_context() -> str:
    """Run config {"snapshot_date": "YYYY-MM-DD"} wins; otherwise the Singapore date of data_interval_end."""
    ctx = get_current_context()
    override = (ctx.get("params") or {}).get("snapshot_date")
    return snapshot_date_for_run(override, ctx["data_interval_end"])


@dag(
    dag_id="daily_mcf_transform",
    schedule="30 10 * * *",   # 10:30 SGT, half an hour after daily_mcf_ingest starts
    start_date=pendulum.datetime(2026, 10, 24, tz=SGT),
    catchup=False,
    max_active_runs=1,        # SCD2 versioning must see days in order
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    params={"snapshot_date": ""},
    tags=["transform", "warehouse", "mcf"],
    doc_md=__doc__,
)
def daily_mcf_transform():

    @task.sensor(poke_interval=POKE_INTERVAL, timeout=SENSOR_TIMEOUT, mode="reschedule")
    def wait_for_raw() -> PokeReturnValue:
        """Done when raw.ingest_runs has today's row (the ingest DAG writes it last, after load_raw)
        AND the raw rows are there. Times out -> the run fails with a clear message."""
        snapshot_date = _snapshot_date_from_context()
        conn = _warehouse_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT jobs_fetched FROM raw.ingest_runs WHERE snapshot_date = %s", (snapshot_date,))
                run = cur.fetchone()
                cur.execute("SELECT count(*) FROM raw.mcf_job_snapshots WHERE snapshot_date = %s", (snapshot_date,))
                n_raw = cur.fetchone()[0]
        finally:
            conn.close()
        ready = run is not None and n_raw > 0 and n_raw >= run[0]
        print(f"snapshot_date={snapshot_date} ingest_runs={'yes' if run else 'no'} raw_rows={n_raw} ready={ready}")
        return PokeReturnValue(is_done=ready, xcom_value=snapshot_date)

    @task
    def load_reference_tables() -> dict:
        """Idempotent upsert of the SkillsFuture and SSOC reference files (seconds). Running it every
        day means a refreshed file in data/reference/ is picked up without a separate DAG."""
        conn = _warehouse_conn()
        try:
            stats = load_reference(conn)
        finally:
            conn.close()
        print(json.dumps(stats))
        return stats

    @task
    def transform_snapshot_task(snapshot_date: str, reference_stats: dict) -> dict:
        ctx = get_current_context()
        conn = _warehouse_conn()
        try:
            stats = transform_snapshot(conn, snapshot_date, run_id=ctx["run_id"])
        finally:
            conn.close()
        print(json.dumps(stats, default=str))
        return stats

    @task
    def run_quality_checks(snapshot_date: str) -> dict:
        """Writes every verdict to clean.dq_results, then fails the run on any 'fail' severity check."""
        conn = _warehouse_conn()
        try:
            results = run_checks(conn, snapshot_date)
        finally:
            conn.close()
        for r in results:
            print(f"{r.status.upper():4} {r.category:13} {r.name:40} observed={r.observed} threshold={r.threshold}")
        summary = summarise(results)
        if failed_checks(results):
            raise AirflowFailException(f"Data quality failed for {snapshot_date}: {summary['failed']}")
        return summary

    @task
    def log_run_stats(snapshot_date: str, stats: dict, dq_summary: dict) -> dict:
        """One 'dag_run' row in clean.load_log plus the skill match rate, for the performance section.
        seconds = the transform's own run time; the sensor wait and wall time go in details (run_context.py)."""
        ctx = get_current_context()
        dag_run = ctx["dag_run"]
        sensor = dag_run.get_task_instance("wait_for_raw")
        times = run_timings(stats["elapsed_seconds"], dag_run.start_date, sensor.end_date if sensor else None,
                            pendulum.now("UTC"))
        conn = _warehouse_conn()
        try:
            rates = match_rate_report(conn, snapshot_date)
            details = {**stats, "skill_match": rates, "sensor_wait_seconds": times["sensor_wait_seconds"],
                       "run_wall_seconds": times["run_wall_seconds"]}
            write_run_summary(conn, ctx["run_id"], snapshot_date, details, dq_summary, times["seconds"])
        finally:
            conn.close()
        print(json.dumps({"snapshot_date": snapshot_date, "dq": dq_summary, "skill_match": rates}, default=str))
        return rates

    snapshot_date = wait_for_raw()
    reference = load_reference_tables()
    stats = transform_snapshot_task(snapshot_date, reference)
    dq = run_quality_checks(snapshot_date)
    stats >> dq
    log_run_stats(snapshot_date, stats, dq)


daily_mcf_transform()
