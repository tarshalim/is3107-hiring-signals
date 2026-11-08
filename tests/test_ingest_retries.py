"""Offline tests for ops/ingest_retries.py (attempts and 429s of past crawls, from the Airflow task logs) and for the
ingest DAG recording them itself from 8 Nov."""
from datetime import date
from pathlib import Path

from ops.ingest_retries import RETRIES_LOGGED_FROM, backfill, parse_task_logs

REPO = Path(__file__).resolve().parent.parent

STATS = ('[2026-10-29T02:31:00.000+0000] {logging_mixin.py:190} INFO - {"path": "/opt/***/data/raw/mcf/%s.jsonl.gz", '
         '"pages_fetched": 916, "jobs_fetched": %d, "api_total": 91159, "seconds_taken": 1700.1, '
         '"snapshot_date": "%s"}')
RETRY = "[2026-10-29T02:11:32.166+0000] {mcf_client.py:31} WARNING - MCF retry: /v2/jobs?page=449 (reason: %s)"


def _log(root, run, attempt, lines):
    d = root / "dag_id=daily_mcf_ingest" / f"run_id={run}" / "task_id=fetch_snapshot"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"attempt={attempt}.log").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _logs(tmp_path):
    # 28 Oct: three failed tries (connection errors), the fourth fetched the day with no 429
    for attempt in (1, 2, 3):
        _log(tmp_path, "scheduled__2026-10-27T020000+0000", attempt, [RETRY % "ConnectionError()"])
    _log(tmp_path, "scheduled__2026-10-27T020000+0000", 4, [STATS % ("2026-10-28", 91579, "2026-10-28")])
    # 30 Oct: first try, two 429s and one 504
    _log(tmp_path, "scheduled__2026-10-29T020000+0000", 1,
         [RETRY % 429, RETRY % 504, RETRY % 429, STATS % ("2026-10-30", 90079, "2026-10-30")])
    # 24 Oct: a 2-page trial and the real crawl both printed stats; retries were not logged yet
    _log(tmp_path, "scheduled__2026-10-22T220000+0000", 1, [STATS % ("2026-10-24", 148, "2026-10-24")])
    _log(tmp_path, "scheduled__2026-10-23T020000+0000", 3, [STATS % ("2026-10-24", 91825, "2026-10-24")])
    return parse_task_logs(tmp_path)


def test_parse_task_logs_reads_the_attempt_that_fetched_each_day(tmp_path):
    by_jobs = {a["jobs_fetched"]: a for a in _logs(tmp_path)}
    assert set(by_jobs) == {91579, 90079, 148, 91825}                 # failed attempts printed no stats
    assert (by_jobs[91579]["fetch_attempts"], by_jobs[91579]["retries_429"]) == (4, 0)
    assert (by_jobs[90079]["fetch_attempts"], by_jobs[90079]["retries_429"]) == (1, 2)   # the 504 is not a 429
    assert by_jobs[91825]["fetch_attempts"] == 3 and by_jobs[91825]["snapshot_date"] == date(2026, 10, 24)
    assert by_jobs[91825]["retries_429"] is None                     # before RETRIES_LOGGED_FROM: unknown, not 0
    assert RETRIES_LOGGED_FROM == date(2026, 10, 28)


class FakeCursor:
    """raw.ingest_runs as {(day, jobs_fetched): fetch_attempts}; records the UPDATEs."""

    def __init__(self, rows):
        self.rows, self.updates, self._one = rows, [], None

    def execute(self, sql, params):
        if sql.lstrip().startswith("SELECT"):
            day, jobs = params
            self._one = (1,) if (day, jobs) in self.rows and self.rows[(day, jobs)] is None else None
        else:
            self.updates.append(params)

    def fetchone(self):
        return self._one

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, rows):
        self.cur = FakeCursor(rows)

    def cursor(self):
        return self.cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_backfill_matches_the_stored_crawl_and_never_overwrites(tmp_path):
    attempts = _logs(tmp_path)
    # stored: the real 24 Oct crawl, 28 Oct, and 30 Oct already filled in by the DAG
    rows = {(date(2026, 10, 24), 91825): None, (date(2026, 10, 28), 91579): None, (date(2026, 10, 30), 90079): 1}
    dry = FakeConn(rows)
    matched = backfill(dry, attempts)
    assert sorted(m["jobs_fetched"] for m in matched) == [91579, 91825]   # not the 2-page trial, not 30 Oct
    assert dry.cur.updates == []                                         # dry run by default
    real = FakeConn(rows)
    backfill(real, attempts, write=True)
    assert sorted(real.cur.updates) == [(3, None, date(2026, 10, 24), 91825), (4, 0, date(2026, 10, 28), 91579)]


def test_the_ingest_dag_records_attempts_and_429s_itself():
    dag = (REPO / "dags" / "daily_ingest_dag.py").read_text(encoding="utf-8")
    assert 'stats["fetch_attempts"] = get_current_context()["ti"].try_number' in dag
    assert "fetch_attempts = EXCLUDED.fetch_attempts, retries_429 = EXCLUDED.retries_429" in dag
    schema = (REPO / "sql" / "init" / "01_raw_schema.sql").read_text(encoding="utf-8")
    assert "ALTER TABLE raw.ingest_runs ADD COLUMN IF NOT EXISTS fetch_attempts SMALLINT" in schema
    assert "ALTER TABLE raw.ingest_runs ADD COLUMN IF NOT EXISTS retries_429    INTEGER" in schema
