"""Offline tests for the shared failure and retry callback (dags/common/alerts.py). No Airflow, no database."""
import ast
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from common import alerts

REPO = Path(__file__).resolve().parent.parent
DAG_FILES = sorted((REPO / "dags").glob("*_dag.py"))
TOKEN = "123456:SECRET-token"


def _ti(**kw):
    base = dict(dag_id="daily_mcf_ingest", task_id="fetch_snapshot", run_id="scheduled__2026-11-06T02:00:00+00:00",
                try_number=2, log_url="http://localhost:8080/log?x=1")
    base.update(kw)
    return SimpleNamespace(**base)


def _context(exc=RuntimeError("HTTP 429 from the MCF API"), **ti_kw):
    ti = _ti(**ti_kw)
    return {"task_instance": ti, "ti": ti, "run_id": ti.run_id, "exception": exc}


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((" ".join(sql.split()), params))

    def fetchone(self):
        return (1,) if self.conn.new else None


class FakeConn:
    def __init__(self, new=True):
        self.new, self.executed, self.closed = new, [], False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


class FakePost:
    def __init__(self, status=200, raises=None):
        self.calls, self.status, self.raises = [], status, raises

    def __call__(self, url, data=None, timeout=None):
        self.calls.append((url, data, timeout))
        if self.raises:
            raise self.raises
        return SimpleNamespace(status_code=self.status, text="ok")


TELEGRAM_ENV = {"TELEGRAM_BOT_TOKEN": TOKEN, "TELEGRAM_CHAT_ID": "-1001"}


def test_task_row_has_every_column():
    row = alerts.task_alert_row(_context(), "task_failed")
    assert row == {"event": "task_failed", "dag_id": "daily_mcf_ingest", "task_id": "fetch_snapshot",
                   "run_id": "scheduled__2026-11-06T02:00:00+00:00", "try_number": 2,
                   "exception": "RuntimeError: HTTP 429 from the MCF API", "log_url": "http://localhost:8080/log?x=1"}


def test_long_exception_text_is_truncated():
    row = alerts.task_alert_row(_context(exc=ValueError("x" * 10_000)), "task_retry")
    assert len(row["exception"]) == alerts.EXCEPTION_MAX and row["exception"].endswith("[truncated]")


def test_log_url_that_raises_does_not_stop_the_row():
    class BadTI(SimpleNamespace):
        @property
        def log_url(self):
            raise RuntimeError("no webserver config")
    ti = BadTI(dag_id="d", task_id="t", run_id="r", try_number=1)
    row = alerts.task_alert_row({"ti": ti, "exception": None}, "task_failed")
    assert row["log_url"] is None and row["exception"] is None and row["dag_id"] == "d"


def test_dag_level_failure_from_a_task_is_left_to_the_task_callback():
    run = SimpleNamespace(dag_id="mcf_live_poll", run_id="scheduled__x")
    assert alerts.dag_alert_row({"dag_run": run, "reason": "task_failure"}) is None
    row = alerts.dag_alert_row({"dag_run": run, "reason": "timed_out"})
    assert row["event"] == "dag_run_failed" and row["task_id"] is None and row["try_number"] is None
    assert row["exception"] == "DAG run failed: timed_out" and row["run_id"] == "scheduled__x"


def test_new_event_is_logged_and_sent_then_marked_notified():
    conn, post = FakeConn(new=True), FakePost()
    out = alerts.handle(alerts.task_alert_row(_context(), "task_failed"), connect=lambda: conn,
                        env=TELEGRAM_ENV, post=post)
    assert out == {"logged": True, "duplicate": False, "notified": True}
    sqls = [s for s, _ in conn.executed]
    assert sqls[0].startswith("INSERT INTO ops.alert_log") and "ON CONFLICT" in sqls[0]
    assert sqls[1].startswith("UPDATE ops.alert_log SET notified = TRUE")
    assert conn.closed
    url, data, timeout = post.calls[0]
    assert url == f"https://api.telegram.org/bot{TOKEN}/sendMessage" and data["chat_id"] == "-1001"
    assert "FAILED: daily_mcf_ingest.fetch_snapshot" in data["text"] and "HTTP 429" in data["text"]
    assert timeout == alerts.TELEGRAM_TIMEOUT


def test_duplicate_event_sends_nothing():
    post = FakePost()
    out = alerts.handle(alerts.task_alert_row(_context(), "task_retry"), connect=lambda: FakeConn(new=False),
                        env=TELEGRAM_ENV, post=post)
    assert out == {"logged": False, "duplicate": True, "notified": False} and post.calls == []


def test_without_telegram_settings_only_the_table_is_written():
    post = FakePost()
    out = alerts.handle(alerts.task_alert_row(_context(), "task_failed"), connect=lambda: FakeConn(),
                        env={"TELEGRAM_BOT_TOKEN": TOKEN}, post=post)      # chat id missing
    assert out["logged"] and not out["notified"] and post.calls == []
    assert alerts.telegram_settings({}) is None
    assert alerts.telegram_settings({"TELEGRAM_BOT_TOKEN": " ", "TELEGRAM_CHAT_ID": "1"}) is None


def test_warehouse_down_still_sends_telegram_and_never_raises():
    def down():
        raise ConnectionError("could not connect to server: Connection refused")
    post = FakePost()
    out = alerts.handle(alerts.task_alert_row(_context(), "task_failed"), connect=down, env=TELEGRAM_ENV, post=post)
    assert out == {"logged": False, "duplicate": False, "notified": True}
    assert "not written to ops.alert_log" in post.calls[0][1]["text"]


def test_telegram_error_never_raises_and_never_logs_the_token(caplog):
    caplog.set_level(logging.WARNING)
    err = ConnectionError(f"Max retries exceeded with url: /bot{TOKEN}/sendMessage")
    assert alerts.send_telegram("hi", TOKEN, "-1001", post=FakePost(raises=err)) is False
    assert alerts.send_telegram("hi", TOKEN, "-1001", post=FakePost(status=401)) is False
    assert TOKEN not in caplog.text and "***" in caplog.text


def test_callbacks_swallow_anything(monkeypatch):
    monkeypatch.setattr(alerts, "_warehouse_conn", lambda: (_ for _ in ()).throw(RuntimeError("no airflow here")))
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    alerts.on_task_failure(_context())
    alerts.on_task_retry(_context())
    alerts.on_task_failure(None)                      # broken context: still no exception
    alerts.on_dag_failure({"dag_run": None, "reason": "timed_out"})


def test_message_is_plain_text_and_bounded():
    row = alerts.task_alert_row(_context(exc=ValueError("y" * 5000)), "task_retry")
    text = alerts.format_message(row)
    assert text.startswith("[IS3107 pipeline] RETRY: daily_mcf_ingest.fetch_snapshot")
    assert len(text) <= alerts.MESSAGE_MAX


def _dag_kwargs(path):
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and getattr(dec.func, "id", None) == "dag":
                    return {kw.arg: kw.value for kw in dec.keywords}
    raise AssertionError(f"no @dag(...) in {path.name}")


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda p: p.name)
def test_every_dag_wires_the_callbacks(path):
    kwargs = _dag_kwargs(path)
    default_args = kwargs["default_args"]
    spreads = [ast.unparse(v) for k, v in zip(default_args.keys, default_args.values) if k is None]
    assert "TASK_ALERTS" in spreads, f"{path.name}: default_args needs **TASK_ALERTS"
    assert ast.unparse(kwargs["on_failure_callback"]) == "on_dag_failure"
    assert "from common.alerts import TASK_ALERTS, on_dag_failure" in path.read_text(encoding="utf-8")


def test_task_alerts_cover_failure_and_retry():
    assert alerts.TASK_ALERTS == {"on_failure_callback": alerts.on_task_failure,
                                  "on_retry_callback": alerts.on_task_retry}


def test_helpers_are_not_parsed_as_dags():
    lines = [line.split("#")[0].strip() for line in (REPO / "dags" / ".airflowignore").read_text().splitlines()]
    assert "^common/" in lines


def test_sql_file_is_rerunnable():
    sql = (REPO / "sql" / "init" / "09_ops_alerts.sql").read_text(encoding="utf-8")
    code = "\n".join(line.split("--")[0] for line in sql.splitlines())
    statements = [s.strip() for s in code.split(";") if s.strip()]
    assert statements and all(s.startswith(("CREATE SCHEMA IF NOT EXISTS", "CREATE TABLE IF NOT EXISTS",
                                             "CREATE INDEX IF NOT EXISTS")) for s in statements)
    assert "UNIQUE NULLS NOT DISTINCT (dag_id, task_id, run_id, try_number, event)" in code
    assert "ON CONFLICT (dag_id, task_id, run_id, try_number, event) DO NOTHING" in alerts.INSERT_SQL
