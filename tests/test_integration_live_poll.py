"""End-to-end test of the near-real-time feed against a real Postgres (skipped unless WAREHOUSE_TEST_DSN is set).

    WAREHOUSE_TEST_DSN=postgresql://jobs:<pw>@localhost:5433/jobs pytest tests/test_integration_live_poll.py -v

Creates its own scratch database `is3107_test_live` (not `is3107_test`, so it never collides with
tests/test_integration_warehouse.py), applies sql/init/01, 02, 03 and 06 (06 twice: rerunnable), then runs
five fake polls through the real poll -> load -> check code and checks the metrics.live_* views by hand-worked
numbers. No network: the API is a fake session.
"""
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from ingestion import mcf_client, mcf_live  # noqa: E402
from quality.live_poll_checks import LIVE_CHECKS, run_live_checks  # noqa: E402

DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="set WAREHOUSE_TEST_DSN to run against a Postgres warehouse")
REPO = Path(__file__).resolve().parent.parent
TEST_DB = "is3107_test_live"
UTC = timezone.utc
T0 = datetime(2026, 11, 3, 2, 15, tzinfo=UTC)                     # 10:15 SGT
POLLS = [T0, T0 + timedelta(minutes=30), T0 + timedelta(minutes=60), T0 + timedelta(minutes=90),
         T0 + timedelta(minutes=110),                             # the API's cache: same pages as 20 minutes before
         datetime(2026, 11, 3, 8, 5, tzinfo=UTC)]                 # the last one comes after a 4-hour gap


@pytest.fixture(scope="module")
def conn():
    admin = psycopg2.connect(DSN)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        cur.execute(f"CREATE DATABASE {TEST_DB}")
    admin.close()
    c = psycopg2.connect(DSN.rpartition("/")[0] + "/" + TEST_DB)
    with c, c.cursor() as cur:
        for f in ("01_raw_schema.sql", "02_clean_schema.sql", "03_mom_vacancies.sql", "06_live_poll.sql",
                  "06_live_poll.sql"):
            cur.execute((REPO / "sql" / "init" / f).read_text(encoding="utf-8"))
    yield c
    c.close()


class FakeResp:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class FakeSession:
    def __init__(self, jobs):
        self.jobs = jobs

    def get(self, url, params, timeout):
        return FakeResp({"results": self.jobs if params["page"] == 0 else [], "total": len(self.jobs)})


def _job(uuid, created, views, apps, category, ssoc, repost=0):
    day = created[:10]
    return {"uuid": uuid, "title": f"Job {uuid[:1]}", "ssocCode": ssoc, "categories": [{"category": category}],
            "status": {"jobStatus": "Re-open" if repost else "Open"},
            "metadata": {"createdAt": created, "updatedAt": created, "newPostingDate": day,
                         "originalPostingDate": "2026-10-20" if repost else day, "repostCount": repost,
                         "editCount": 0, "totalNumberOfView": views, "totalNumberJobApplication": apps}}


A, B, C, D = "a" * 32, "b" * 32, "c" * 32, "d" * 32
# (views, applications) per poll; None = not on the polled pages any more.
SERIES = {
    A: [(1, 0), (5, 0), (9, 1), (14, 2), (14, 2), (30, 3)],      # created 10:00 SGT, IT, a professional
    B: [(0, 0), (2, 0), (3, 0), (6, 1), (6, 1), None],           # created 10:10 SGT, F&B, a service worker
    C: [(50, 4), (50, 4), (50, 4), (49, 4), (49, 4), (49, 4)],   # created the night before: too old; views fall once
    D: [(90, 9), (91, 9), (92, 9), (93, 9), (93, 9), None],      # a repost: not tracked
}
META = {A: ("2026-11-03T02:00:00.000Z", "Information Technology", "25121", 0),
        B: ("2026-11-03T02:10:00.000Z", "F&B", "51202", 0),
        C: ("2026-11-02T22:00:00.000Z", "Engineering", "21499", 0),
        D: ("2026-11-03T02:05:00.000Z", "Sales / Retail", "52201", 1)}


def _poll(conn, i, monkeypatch):
    monkeypatch.setattr(mcf_client.time, "sleep", lambda s: None)
    jobs = []
    for uuid in (B, D, A, C):                           # newest first, as the API lists them
        point = SERIES[uuid][i]
        if point is not None:
            created, category, ssoc, repost = META[uuid]
            jobs.append(_job(uuid, created, *point, category, ssoc, repost))
    rows, stats = mcf_live.poll_newest(max_pages=3, session=FakeSession(jobs), now=POLLS[i])
    return rows, stats


def _one(conn, sql, *params):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def _all(conn, sql, *params):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def test_polls_load_idempotently_and_pass_checks(conn, monkeypatch):
    for i in range(len(POLLS)):
        rows, stats = _poll(conn, i, monkeypatch)
        assert mcf_live.load_poll(conn, stats, rows) == len(rows)
        assert mcf_live.load_poll(conn, stats, rows) == len(rows)          # a retried load adds nothing
        results = {r.name: r for r in run_live_checks(conn, stats["poll_ts"])}
        assert not [r.name for r in results.values() if r.status == "fail"], i
        statuses = {name: r.status for name, r in results.items()}
        if i == 0:
            assert statuses["minutes_since_prev_poll"] == "warn"            # first poll: nothing to compare with
        elif i == 5:
            assert statuses["minutes_since_prev_poll"] == "warn"            # the 4-hour gap is flagged
            assert results["minutes_since_prev_poll"].observed == 240
        else:
            assert statuses["minutes_since_prev_poll"] == "pass"
        assert statuses["counters_fell_since_prev_poll"] == ("warn" if i == 3 else "pass")   # C's views fell
        assert statuses["list_newest_first"] == "pass" and statuses["poll_duplicate_keys"] == "pass"
        assert results["pages_unchanged_share"].observed == (1.0 if i == 4 else 0.0)   # poll 4 is a cached copy
    assert _one(conn, "SELECT count(*) FROM raw.mcf_live_poll")[0] == 4 * 5 + 2      # raw keeps the cached copy too
    assert _one(conn, "SELECT count(*) FROM raw.mcf_live_poll_runs")[0] == 6
    assert _one(conn, "SELECT count(*) FROM raw.mcf_live_poll_dq")[0] == 6 * len(LIVE_CHECKS)


def test_only_brand_new_postings_are_tracked(conn):
    rows = _all(conn, "SELECT job_uuid, minutes_to_first_seen, ssoc_major_group, category "
                      "FROM metrics.live_tracked_postings ORDER BY job_uuid")
    assert rows == [(A, 15.0, "2 Professionals", "Information Technology"),
                    (B, 5.0, "5 Service and sales workers", "F&B")]


def test_daily_crawl_extends_the_curve_to_24_hours(conn):
    # The 4 Nov crawl started at 10:00 SGT (finished_at 10:05 minus 300 s of fetching) and saw A and B.
    with conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO raw.ingest_runs (snapshot_date, pages_fetched, jobs_fetched, api_total, seconds_taken, finished_at)
                       VALUES ('2026-11-04', 900, 90000, 90000, 300, '2026-11-04T02:05:00Z')""")
        cur.execute("""INSERT INTO clean.dim_date (date_key, date, year, month, day, day_of_week, day_name, is_weekend, week_start, iso_week)
                       VALUES (20261104, '2026-11-04', 2026, 11, 4, 3, 'Wednesday', false, '2026-11-02', 45)""")
        for uuid, views, apps in ((A, 80, 6), (B, 40, 3)):
            cur.execute("""INSERT INTO clean.dim_job (job_uuid, title, attr_hash, valid_from, last_seen_date)
                           VALUES (%s, 'x', 'h', '2026-11-04', '2026-11-04') RETURNING job_key""", (uuid,))
            key = cur.fetchone()[0]
            cur.execute("""INSERT INTO clean.fact_job_day (snapshot_date, job_uuid, job_key, date_key, views_total,
                               applications_total, repost_count, edit_count, is_first_snapshot)
                           VALUES ('2026-11-04', %s, %s, 20261104, %s, %s, 0, 0, true)""", (uuid, key, views, apps))
    points = _all(conn, "SELECT job_uuid, source, hours_since_posting, applications FROM metrics.live_curve_points "
                        "ORDER BY job_uuid, observed_at")
    # No point at 2.08 h for A: the poll at 12:05 SGT was a cached copy of the 11:45 one, so it is left out.
    assert [(s, float(h), a) for u, s, h, a in points if u == A] == [
        ("live", 0.25, 0), ("live", 0.75, 0), ("live", 1.25, 1), ("live", 1.75, 2), ("live", 6.08, 3), ("daily", 24.0, 6)]
    assert [(s, float(h)) for u, s, h, a in points if u == B][-1] == ("daily", 23.83)


def test_curve_and_summary_numbers(conn):
    curve = {row[0]: row[1:] for row in _all(conn, """
        SELECT hours_since_posting, postings, median_applications, pct_with_application
        FROM metrics.live_first24h_curve WHERE group_type = 'all'""")}
    assert curve[0] == (2, 0.0, 0.0)            # hour 0: A at 0.75 h and B at 0.58 h, no applications yet
    assert curve[1] == (2, 1.5, 100.0)          # hour 1: A has 2, B has 1
    assert curve[6] == (1, 3.0, 100.0)          # only A was still on the polled pages (the night-time case)
    assert curve[23] == (1, 3.0, 100.0)         # B's daily point at 23.8 h; A's (24.0 h) is past the curve
    summary = {(row[0], row[1], row[2]): row[3:] for row in _all(conn, """
        SELECT group_type, group_name, hours_since_posting, postings, median_applications FROM metrics.live_first24h_summary""")}
    assert summary[("all", "all postings", 1)] == (2, 0.0)       # closest to 1 h, earlier one on a tie
    assert summary[("all", "all postings", 6)] == (1, 3.0)
    assert ("all", "all postings", 12) not in summary            # nobody measured near 12 h: no row, not a fake 0
    assert summary[("all", "all postings", 24)] == (2, 4.5)
    assert summary[("category", "F&B", 24)] == (1, 3.0)
    assert summary[("ssoc_major_group", "2 Professionals", 24)] == (1, 6.0)


def test_status_view_for_the_demo(conn):
    rows = _all(conn, """SELECT postings_seen, new_since_last_poll, minutes_since_newest_posting, checks_failed, checks_warned
                         FROM metrics.live_poll_status ORDER BY poll_time_sgt""")
    assert rows[0] == (4, 4, 5.0, 0, 1)         # first poll: everything is new; newest (B) went live 5 min before
    assert rows[1][:2] == (4, 0)
    assert rows[5][:2] == (2, 0)
    assert [r[0] for r in _all(conn, "SELECT pages_unchanged FROM metrics.live_poll_status ORDER BY poll_time_sgt")]         == [0, 0, 0, 0, 1, 0]
    assert _one(conn, "SELECT poll_time_sgt FROM metrics.live_poll_status ORDER BY 1 LIMIT 1")[0] == \
        datetime(2026, 11, 3, 10, 15)           # shown in Singapore time
