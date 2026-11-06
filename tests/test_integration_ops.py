"""The ops pieces against a real Postgres (skipped unless WAREHOUSE_TEST_DSN is set).

    WAREHOUSE_TEST_DSN=postgresql://jobs:<pw>@localhost:5433/jobs pytest tests/test_integration_ops.py -v

Creates its own scratch database `is3107_test_ops` (never the warehouse itself), applies every sql/init file in
order and the new ones twice (rerunnable), then checks the alert log written by dags/common/alerts.py.
"""
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from common import alerts  # noqa: E402

DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="set WAREHOUSE_TEST_DSN to run against a Postgres warehouse")
REPO = Path(__file__).resolve().parent.parent
TEST_DB = "is3107_test_ops"
NEW_FILES = ["09_ops_alerts.sql"]


def _test_dsn():
    return DSN.rpartition("/")[0] + "/" + TEST_DB


@pytest.fixture(scope="module")
def conn():
    admin = psycopg2.connect(DSN)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        cur.execute(f"CREATE DATABASE {TEST_DB}")
    admin.close()
    c = psycopg2.connect(_test_dsn())
    with c, c.cursor() as cur:
        init = sorted((REPO / "sql" / "init").glob("*.sql"))          # the order the warehouse container uses
        for f in init + [REPO / "sql" / "init" / n for n in NEW_FILES]:
            cur.execute(f.read_text(encoding="utf-8"))
    yield c
    c.close()


def _connect():
    return psycopg2.connect(_test_dsn())


def _count(conn, sql, *params):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        n = cur.fetchone()[0]
    conn.rollback()
    return n


def _ctx(try_number, exc):
    ti = SimpleNamespace(dag_id="daily_mcf_ingest", task_id="fetch_snapshot", run_id="manual__2026-11-06",
                         try_number=try_number, log_url="http://localhost:8080/log")
    return {"ti": ti, "run_id": ti.run_id, "exception": exc}


def test_alert_rows_are_written_once_per_event(conn):
    retry = alerts.task_alert_row(_ctx(1, RuntimeError("429")), "task_retry")
    fail = alerts.task_alert_row(_ctx(2, RuntimeError("429 again")), "task_failed")
    assert alerts.handle(retry, connect=_connect, env={})["logged"]
    assert alerts.handle(fail, connect=_connect, env={})["logged"]
    again = alerts.handle(fail, connect=_connect, env={})                # the same event twice: one row
    assert again == {"logged": False, "duplicate": True, "notified": False}
    run_level = alerts.dag_alert_row({"dag_run": SimpleNamespace(dag_id="mcf_live_poll", run_id="r1"),
                                      "reason": "timed_out"})
    assert alerts.handle(run_level, connect=_connect, env={})["logged"]
    assert alerts.handle(run_level, connect=_connect, env={})["duplicate"]   # NULL task_id still deduplicates
    assert _count(conn, "SELECT count(*) FROM ops.alert_log") == 3
    assert _count(conn, "SELECT count(*) FROM ops.alert_log WHERE event = 'task_retry' AND try_number = 1") == 1
    assert _count(conn, "SELECT count(*) FROM ops.alert_log WHERE notified") == 0


def test_unknown_event_is_rejected_by_the_table_not_by_the_task(conn):
    bad = alerts.task_alert_row(_ctx(3, None), "something_else")
    assert alerts.handle(bad, connect=_connect, env={}) == {"logged": False, "duplicate": False, "notified": False}


def test_dashboard_query_reads_the_log(conn, monkeypatch):
    pytest.importorskip("pandas")
    pytest.importorskip("plotly")
    pytest.importorskip("streamlit")                 # the tab module needs both at import time
    monkeypatch.setenv("WAREHOUSE_DSN", _test_dsn())
    from dashboard.tabs import pipeline
    frame = pipeline.alert_log()
    assert len(frame) == 3 and set(frame["event"]) == {"task_retry", "task_failed", "dag_run_failed"}
