"""Failure and retry alerts for every DAG: one row per event in ops.alert_log, plus an optional Telegram message.

Every DAG file adds the task callbacks through default_args and the run-level one on the DAG:

    from common.alerts import TASK_ALERTS, on_dag_failure
    @dag(..., default_args={"retries": 2, ..., **TASK_ALERTS}, on_failure_callback=on_dag_failure)

Why (wk7 [074], wk11 [104]): a red run in the Airflow UI only helps if someone is looking at it. The alert log is
a table, so the dashboard's Pipeline health tab and the report can show every failure and retry with its error,
and a Telegram message reaches the team without anyone opening Airflow.

Events:
    task_retry      a task failed and Airflow will try it again (on_retry_callback)
    task_failed     a task failed for good: retries used up, or AirflowFailException (on_failure_callback)
    dag_run_failed  the run failed for a reason no task reported, e.g. dagrun_timeout. The scheduler then marks
                    the unfinished tasks skipped, so no task callback fires. Runs that failed because a task
                    failed are skipped here: that task's own row already says why.

Rules:
    - never raise. A callback that raises is logged by Airflow and the failure it was reporting would get lost
      in the noise, so every step is wrapped and only logged.
    - idempotent: the key (dag_id, task_id, run_id, try_number, event) means a callback that runs twice for the
      same event writes one row and sends one message.
    - the warehouse may be the thing that is down, so a failed insert still sends the Telegram message.
    - Telegram only when TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are set (in .env, never in git). The token is
      part of the API URL, so it is removed from any error text before that text is logged.

Airflow imports stay inside functions, so the pure parts are unit-tested without Airflow (tests/test_alerts.py).
"""
import logging
import os

log = logging.getLogger(__name__)

EXCEPTION_MAX = 2000        # characters of the error kept in ops.alert_log
MESSAGE_MAX = 1500          # characters of a Telegram message (the API limit is 4096)
CONNECT_TIMEOUT = 5         # seconds; a down warehouse must not hold the task slot
TELEGRAM_TIMEOUT = (5, 10)  # connect, read seconds
TELEGRAM_URL = "https://api.telegram.org/bot{token}/sendMessage"

INSERT_SQL = """
    INSERT INTO ops.alert_log (event, dag_id, task_id, run_id, try_number, exception, log_url)
    VALUES (%(event)s, %(dag_id)s, %(task_id)s, %(run_id)s, %(try_number)s, %(exception)s, %(log_url)s)
    ON CONFLICT (dag_id, task_id, run_id, try_number, event) DO NOTHING
    RETURNING alert_id
"""


def _truncate(text, limit: int):
    if text is None:
        return None
    text = str(text)
    return text if len(text) <= limit else text[: limit - 15] + " ...[truncated]"


def _exception_text(exc):
    if exc is None:
        return None
    if isinstance(exc, BaseException):
        message = str(exc)
        return f"{type(exc).__name__}: {message}" if message else type(exc).__name__
    return str(exc)


def task_alert_row(context: dict, event: str) -> dict:
    """The ops.alert_log row for a task callback's context (pure: reads attributes only)."""
    ti = context.get("task_instance") or context.get("ti")
    try:
        log_url = ti.log_url if ti is not None else None
    except Exception:  # log_url reads the webserver config; never let that stop the alert
        log_url = None
    return {
        "event": event,
        "dag_id": getattr(ti, "dag_id", None) or getattr(context.get("dag"), "dag_id", None) or "unknown",
        "task_id": getattr(ti, "task_id", None),
        "run_id": context.get("run_id") or getattr(ti, "run_id", None),
        "try_number": getattr(ti, "try_number", None),
        "exception": _truncate(_exception_text(context.get("exception")), EXCEPTION_MAX),
        "log_url": log_url,
    }


def dag_alert_row(context: dict):
    """The row for a DAG-level failure callback, or None when a task's own callback already covered it."""
    reason = context.get("reason")
    if reason == "task_failure":
        return None
    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", None) or getattr(context.get("dag"), "dag_id", None) or "unknown"
    return {
        "event": "dag_run_failed",
        "dag_id": dag_id,
        "task_id": None,
        "run_id": getattr(dag_run, "run_id", None) or context.get("run_id"),
        "try_number": None,
        "exception": _truncate(f"DAG run failed: {reason or 'no reason given'}", EXCEPTION_MAX),
        "log_url": None,
    }


def format_message(row: dict, logged: bool = True) -> str:
    """Plain-text Telegram message (no Markdown, so error text needs no escaping)."""
    headline = {"task_failed": "FAILED", "task_retry": "RETRY", "dag_run_failed": "RUN FAILED"}.get(row["event"],
                                                                                                    row["event"])
    where = row["dag_id"] + (f".{row['task_id']}" if row.get("task_id") else "")
    lines = [f"[IS3107 pipeline] {headline}: {where}"]
    if row.get("try_number") is not None:
        lines.append(f"try {row['try_number']}, run {row.get('run_id')}")
    elif row.get("run_id"):
        lines.append(f"run {row['run_id']}")
    if row.get("exception"):
        lines.append(row["exception"])
    if row.get("log_url"):
        lines.append(row["log_url"])
    if not logged:
        lines.append("(not written to ops.alert_log: the warehouse insert failed)")
    return _truncate("\n".join(lines), MESSAGE_MAX)


def record_alert(conn, row: dict) -> bool:
    """Insert the row; True if it is new, False if this exact event was already logged."""
    with conn, conn.cursor() as cur:
        cur.execute(INSERT_SQL, row)
        return cur.fetchone() is not None


def mark_notified(conn, row: dict) -> None:
    with conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE ops.alert_log SET notified = TRUE
            WHERE dag_id = %(dag_id)s AND task_id IS NOT DISTINCT FROM %(task_id)s
              AND run_id IS NOT DISTINCT FROM %(run_id)s AND try_number IS NOT DISTINCT FROM %(try_number)s
              AND event = %(event)s
        """, row)


def _redact(text: str, secret: str | None) -> str:
    return text.replace(secret, "***") if secret else text


def send_telegram(text: str, token: str, chat_id: str, post=None) -> bool:
    """Send one message. Returns True on HTTP 200; never raises and never logs the token."""
    try:
        if post is None:
            import requests
            post = requests.post
        resp = post(TELEGRAM_URL.format(token=token), data={"chat_id": chat_id, "text": text},
                    timeout=TELEGRAM_TIMEOUT)
        if resp.status_code == 200:
            return True
        log.warning("Telegram alert not sent: HTTP %s %s", resp.status_code,
                    _redact(str(getattr(resp, "text", ""))[:200], token))
    except Exception as exc:
        log.warning("Telegram alert not sent: %s", _redact(f"{type(exc).__name__}: {exc}", token))
    return False


def telegram_settings(env=None):
    """(token, chat_id) when both are set, else None."""
    env = os.environ if env is None else env
    token, chat_id = env.get("TELEGRAM_BOT_TOKEN", "").strip(), env.get("TELEGRAM_CHAT_ID", "").strip()
    return (token, chat_id) if token and chat_id else None


def _warehouse_conn():
    """psycopg2 connection from the Airflow 'warehouse' connection, with a short connect timeout."""
    import psycopg2
    from airflow.hooks.base import BaseHook
    c = BaseHook.get_connection("warehouse")
    return psycopg2.connect(host=c.host, port=c.port or 5432, user=c.login, password=c.password,
                            dbname=c.schema, connect_timeout=CONNECT_TIMEOUT)


def handle(row, connect=None, env=None, post=None) -> dict:
    """Log the row, then send Telegram if configured and the event is new. Never raises.
    Returns what happened, for the task log and the tests."""
    outcome = {"logged": False, "duplicate": False, "notified": False}
    if row is None:
        return outcome
    conn = None
    try:
        conn = (connect or _warehouse_conn)()
        new = record_alert(conn, row)
        outcome["logged"], outcome["duplicate"] = new, not new
    except Exception as exc:
        log.warning("Could not write ops.alert_log (%s): %s", type(exc).__name__, exc)
    try:
        settings = telegram_settings(env)
        if settings and not outcome["duplicate"]:
            outcome["notified"] = send_telegram(format_message(row, logged=outcome["logged"]), *settings, post=post)
            if outcome["notified"] and outcome["logged"] and conn is not None:
                mark_notified(conn, row)
    except Exception as exc:
        log.warning("Alert notification step failed (%s): %s", type(exc).__name__, exc)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    log.info("alert %s %s.%s: %s", row["event"], row["dag_id"], row.get("task_id"), outcome)
    return outcome


def _safe(build, context, *args):
    try:
        return handle(build(context, *args))
    except Exception as exc:  # last line of defence: an alert must never mask the failure it reports
        log.warning("Alert callback failed (%s): %s", type(exc).__name__, exc)
        return None


def on_task_failure(context):
    """on_failure_callback for tasks."""
    _safe(task_alert_row, context, "task_failed")


def on_task_retry(context):
    """on_retry_callback for tasks."""
    _safe(task_alert_row, context, "task_retry")


def on_dag_failure(context):
    """on_failure_callback for the DAG itself (run-level failures such as dagrun_timeout)."""
    _safe(dag_alert_row, context)


# Spread into every DAG's default_args.
TASK_ALERTS = {"on_failure_callback": on_task_failure, "on_retry_callback": on_task_retry}
