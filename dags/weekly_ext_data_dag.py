"""Weekly refresh of the extra data in the `ext` schema (docs/extra-data.md).

refresh_wage_tables ─────────────────────────────┐
load_kaggle_2019 -> borrow_ssoc_for_2019_titles ──┼─> summary
build_posting_history ───────────────────────────┘

Mondays 12:00 Singapore time, after that morning's ingest (10:00) and transform (10:30).

  - refresh_wage_tables: downloads the two MOM wage tables through the data.gov.sg API and reloads them
    (delete-then-insert in one transaction, so a retry is safe). They change once a year, so weekly is plenty.
  - load_kaggle_2019: the 2019 MCF crawl is a static file that a person downloads once (no Kaggle login
    needed, see the module docstring). Loaded only when the file's checksum differs from the one recorded
    in ext.source_registry. Skipped, not failed, when the file is not there.
  - borrow_ssoc_for_2019_titles: 2019 postings have no SSOC code, so they borrow one from this week's
    postings with the same title. Re-run weekly because the clean layer grows.
  - build_posting_history: rebuilds the weekly history from the posting dates in clean.dim_job.

The three branches do not depend on each other, so Airflow runs them in parallel (L7: small atomic tasks).
catchup=False: every task rebuilds from the current state, so running old weeks would only repeat work.
"""
import json
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.alerts import TASK_ALERTS, on_dag_failure   # dags/common: failure and retry alerts
from ingestion.datagov_wages import DATASET_IDS, LOADERS, download_dataset
from ingestion.ext_sources import EXT_DATA_DIR, KAGGLE_MCF_2019, sha256_of
from ingestion.kaggle_mcf2019 import load_kaggle_mcf2019, map_titles_to_ssoc
from transform.posting_history import build_posting_history

KAGGLE_DIR = EXT_DATA_DIR / "kaggle_mcf2019"


def _warehouse_conn():
    return PostgresHook(postgres_conn_id="warehouse").get_conn()


@dag(
    dag_id="weekly_ext_data",
    schedule="0 12 * * 1",     # Monday 12:00 SGT
    start_date=pendulum.datetime(2026, 11, 2, tz="Asia/Singapore"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), **TASK_ALERTS},
    on_failure_callback=on_dag_failure,   # run-level failures, e.g. a timed-out run (dags/common/alerts.py)
    tags=["ext", "warehouse"],
    doc_md=__doc__,
)
def weekly_ext_data():

    @task
    def refresh_wage_tables() -> dict:
        out = {}
        conn = _warehouse_conn()
        try:
            for source_id, (_source, loader) in LOADERS.items():
                path = download_dataset(DATASET_IDS[source_id])
                out[source_id] = loader(conn, path)
        finally:
            conn.close()
        print(json.dumps(out, default=str))
        return {k: v.get("rows") for k, v in out.items()}

    @task
    def load_kaggle_2019() -> dict:
        files = sorted(KAGGLE_DIR.glob("*.zip")) + sorted(KAGGLE_DIR.glob("*.json"))
        if not files:
            raise AirflowSkipException(f"no Kaggle file in {KAGGLE_DIR}: see docs/extra-data.md to download it")
        path = files[0]
        conn = _warehouse_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT file_sha256 FROM ext.source_registry WHERE source_id = %s",
                            (KAGGLE_MCF_2019.source_id,))
                row = cur.fetchone()
            if row and row[0] == sha256_of(path):
                return {"loaded": False, "reason": "same file already loaded"}
            stats = load_kaggle_mcf2019(conn, path)
        finally:
            conn.close()
        print(json.dumps(stats, default=str))
        return {"loaded": True, "rows": stats["loaded"]}

    @task(trigger_rule="none_failed")
    def borrow_ssoc_for_2019_titles(_kaggle: dict | None = None) -> dict:
        conn = _warehouse_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM ext.mcf2019_posting")
                if cur.fetchone()[0] == 0:
                    raise AirflowSkipException("ext.mcf2019_posting is empty")
            return map_titles_to_ssoc(conn)
        finally:
            conn.close()

    @task
    def build_posting_history_task() -> dict:
        conn = _warehouse_conn()
        try:
            stats = build_posting_history(conn)
        finally:
            conn.close()
        print(json.dumps(stats, default=str))
        return stats

    @task(trigger_rule="none_failed")
    def summary(wages: dict | None = None, ssoc: dict | None = None, history: dict | None = None) -> None:
        print(json.dumps({"wages": wages, "ssoc_2019": ssoc, "history": history}, default=str))

    wages = refresh_wage_tables()
    ssoc = borrow_ssoc_for_2019_titles(load_kaggle_2019())
    history = build_posting_history_task()
    summary(wages, ssoc, history)


weekly_ext_data()
