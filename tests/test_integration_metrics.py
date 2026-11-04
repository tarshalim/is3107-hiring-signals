"""End-to-end test of the weekly metrics against a real Postgres (skipped unless WAREHOUSE_TEST_DSN is set).

    WAREHOUSE_TEST_DSN=postgresql://jobs:<pw>@localhost:5433/jobs pytest tests/test_integration_metrics.py -v

It creates a scratch database `is3107_test_metrics` (not the one tests/test_integration_warehouse.py uses),
applies every sql/init file in name order as the warehouse container does, then all of them again
(idempotent), loads a 7-posting fixture for three days (Sat 24 to Mon 26 Oct) through the real transform,
builds signal weeks and checks, against numbers worked out by hand:
  - the open-posting rule: a posting the crawl skipped on the snapshot day is carried forward, a closed one is not
  - the 24-hour normalisation of the flow numbers (intervals of 16.8, 24 and 40.8 hours)
  - the three posting flags and the barrier, shortage and unrealistic tables built on them
  - two MCF tags mapped to one SkillsFuture skill count once per posting
  - the first scheduled week (Wed 21 to Tue 27 Oct) uses its latest snapshot, Mon 26 Oct, and says why
  - a week without a snapshot fails and writes nothing
  - the MOM comparison (transform/mom_vacancies.py) of the same week uses the same snapshot and the same
    occupation counts (made-up MOM numbers, scratch database only)
  - a rerun of a week gives identical rows and leaves other weeks alone
"""
import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2.extras import execute_values  # noqa: E402

from metrics.build_week import SIGNAL_TABLES, run_week  # noqa: E402
from transform import mom_vacancies  # noqa: E402
from transform.snapshot_week import NoSnapshotInWeek  # noqa: E402
from transform.load_reference import load_reference  # noqa: E402
from transform.transform_snapshot import transform_snapshot  # noqa: E402

DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="set WAREHOUSE_TEST_DSN to run against a Postgres warehouse")
REPO = Path(__file__).resolve().parent.parent
TEST_DB = "is3107_test_metrics"
D1, D2, D3 = date(2026, 10, 24), date(2026, 10, 25), date(2026, 10, 26)
TUE_27 = date(2026, 10, 27)          # end of the first scheduled week (data interval 20 Oct 14:00 to 27 Oct 14:00)
FETCHED = {D1: datetime(2026, 10, 24, 9, 35, tzinfo=timezone.utc),    # 17:35 SGT
           D2: datetime(2026, 10, 25, 2, 23, tzinfo=timezone.utc),    # 16.8 hours later
           D3: datetime(2026, 10, 26, 2, 23, tzinfo=timezone.utc)}    # 24 hours later
# a 7-posting fixture cannot meet the real minimum sample sizes
SMALL = {"skill_min_postings": 1, "group_min_postings": 1, "salary_min_postings": 1,
         "demand_min_postings": 1, "shortage_min_flagged": 1, "min_companies": 1}
METRIC_TABLES = ("posting_week", *SIGNAL_TABLES)


def _test_dsn():
    base, _, _db = DSN.rpartition("/")
    return f"{base}/{TEST_DB}"


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
        files = sorted((REPO / "sql" / "init").glob("*.sql"))
        assert "05_metrics_signals.sql" in [f.name for f in files]
        for f in files + files:                                      # twice: idempotent
            cur.execute(f.read_text(encoding="utf-8"))
    load_reference(c, REPO / "data" / "reference")
    yield c
    c.close()


def _fixture_days(make_payload):
    def p(letter, views, apps, extra=None):
        return make_payload(uuid=letter * 32, **{"metadata.totalNumberOfView": views,
                                                 "metadata.totalNumberJobApplication": apps, **(extra or {})})

    teamwork = {"skill": "Teamwork", "uuid": "bbbb0000bbbb0000bbbb0000bbbb0000", "confidence": None, "isKeySkill": False}
    forklift = [{"skill": "Forklift Operation", "uuid": "f" * 32, "confidence": None, "isKeySkill": True}]
    a = {"skills": make_payload()["skills"] + [teamwork]}      # Team Player AND Teamwork: both alias to Collaboration
    b = {"positionLevels": [{"id": 1, "position": "Fresh/entry level"}], "minimumYearsExperience": 3}
    c = {"skills": forklift, "ssocCode": "83441", "metadata.repostCount": 2}   # reposted twice, 1 application, many views
    d = {"metadata.repostCount": 1}                            # reposted although it has the most applications
    e = {"positionLevels": [{"id": 2, "position": "Junior Executive"}], "minimumYearsExperience": 1}
    g = {"metadata.originalPostingDate": "2026-10-26", "metadata.newPostingDate": "2026-10-26",
         "salary.minimum": 12, "salary.maximum": 15}           # an hourly rate typed as monthly: not a usable salary
    return {
        D1: [p("a", 100, 5, a), p("b", 50, 1, b), p("c", 400, 1, c), p("d", 1000, 300, d), p("e", 10, 0, e),
             p("f", 80, 3)],
        # e skipped by the crawl on D2 (comes back on D3), f closed after D1
        D2: [p("a", 130, 6, a), p("b", 60, 1, b), p("c", 450, 1, c), p("d", 1100, 310, d)],
        D3: [p("a", 160, 8, a), p("b", 70, 2, b), p("c", 500, 1, c), p("d", 1200, 320, d), p("e", 30, 2, e),
             p("g", 120, 7, g)],
    }


def _load_raw(conn, snapshot_date, payloads, fetched_at):
    with conn, conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO raw.mcf_job_snapshots (snapshot_date, job_uuid, fetched_at, payload) VALUES %s
            ON CONFLICT (snapshot_date, job_uuid) DO UPDATE SET payload = EXCLUDED.payload, fetched_at = EXCLUDED.fetched_at
        """, [(snapshot_date, x["uuid"], fetched_at, json.dumps(x)) for x in payloads])
        cur.execute("""
            INSERT INTO raw.ingest_runs (snapshot_date, pages_fetched, jobs_fetched, api_total, seconds_taken, finished_at)
            VALUES (%s, 1, %s, %s, 1.0, %s) ON CONFLICT (snapshot_date) DO UPDATE SET finished_at = EXCLUDED.finished_at
        """, (snapshot_date, len(payloads), len(payloads), fetched_at))


def _one(conn, sql, *params):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def _all(conn, sql, *params):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def _run(conn, week_ending, run_id, **kw):
    """The signal build for the 7 snapshot days ending on week_ending, as the DAG and the CLI call it."""
    return run_week(conn, week_ending - timedelta(days=7), week_ending, run_id=run_id, thresholds=SMALL, **kw)


def _week_rows(conn, week_end):
    return {t: _all(conn, f"SELECT x::text FROM metrics.{t} x WHERE week_end_date = %s ORDER BY 1", week_end)
            for t in METRIC_TABLES}


def test_weekly_metrics_end_to_end(conn, make_payload):
    for day, payloads in _fixture_days(make_payload).items():
        _load_raw(conn, day, payloads, FETCHED[day])
        transform_snapshot(conn, day, run_id=f"test-{day}")

    # ---- week ending D3: the snapshot is the week-end day itself -----------------------------------
    out = _run(conn, D3, "test-w3")
    assert out["status"] == "ok" and out["week"]["snapshot_date"] == str(D3)
    pool = _all(conn, """SELECT left(job_uuid, 1), seen_on_snapshot, is_shortage, is_reposted_many_apps,
                                is_entry_2plus, is_junior_2plus
                         FROM metrics.posting_week WHERE week_end_date = %s ORDER BY 1""", D3)
    assert [r[0] for r in pool] == list("abcdeg")                       # f closed after D1
    flags = {r[0]: r[2:] for r in pool}
    assert flags["c"] == (True, False, False, False)                    # shortage
    # applications per day on D3: a 8/2, b 2/2, c 1/2, d 320/2, e 2/2, g 7/1 (posted that day: at least 1 day)
    # -> P90 (percentile_disc) of 0.5, 1, 1, 4, 7, 160 is 160, and d is the only reposted posting at or above it
    assert flags["d"] == (False, True, False, False)                    # reposted with the P90 of applications per day
    assert flags["b"] == (False, False, True, False)                    # entry level asking 3 years
    assert flags["e"] == (False, False, False, False)                   # junior asking 1 year: neither flag
    assert sum(sum(v) for v in flags.values()) == 3

    # competitiveness, overall. Stock per posting (applications / days since posting, at least 1 day):
    # a 8/2, b 2/2, c 1/2, d 320/2, e 2/2, g 7/1 -> median of 0.5, 1, 1, 4, 7, 160 = 2.5.
    # Flow: 26 applications over 4 x 16.8 h + 4 x 24 h + 40.8 h (e skipped D2) = 204 h -> 24 * 26 / 204.
    row = _one(conn, """SELECT postings, median_apps_per_day_since_posting, flow_postings, flow_posting_days,
                               flow_applications, apps_per_posting_day_24h
                        FROM metrics.competitiveness WHERE week_end_date = %s AND group_type = 'overall'""", D3)
    assert row == (6, Decimal("2.5000"), 5, Decimal("8.5"), 26, Decimal("3.0588"))
    # a carries Team Player and Teamwork, both mapped to Collaboration: counted once
    assert _one(conn, """SELECT postings FROM metrics.competitiveness WHERE week_end_date = %s
                         AND group_type = 'skill' AND group_label = 'Collaboration'""", D3) == (5,)

    # barrier: Forklift postings get 500 views (above the 346.7 average) and 1 application
    barrier = _all(conn, "SELECT skill_name, rate_vs_overall FROM metrics.barrier_skill WHERE week_end_date = %s AND is_barrier", D3)
    assert len(barrier) == 1 and "Forklift" in barrier[0][0] and barrier[0][1] < Decimal("0.1")
    # shortage: c is the only flagged posting (1 of 6), so its skill has lift 6
    assert _all(conn, """SELECT shortage_postings, lift, median_repost_count, median_applications
                         FROM metrics.shortage_skill WHERE week_end_date = %s""", D3) \
        == [(1, Decimal("6.000"), Decimal("2.0"), Decimal("1.0"))]           # medians of c, the shortage posting
    unreal = {r[0]: r[1:] for r in _all(conn, """SELECT flag, base_postings, flagged_postings, flagged_share, rule
                 FROM metrics.unrealistic_requirements WHERE week_end_date = %s AND group_type = 'overall'""", D3)}
    assert unreal["reposted_many_apps"][:3] == (6, 1, Decimal("0.1667"))
    assert "160 or more applications per day" in unreal["reposted_many_apps"][3]
    assert unreal["entry_level_2plus_years"][:3] == (1, 1, Decimal("1.0000"))
    assert unreal["junior_2plus_years"][:3] == (1, 0, Decimal("0.0000"))      # e: Junior Executive asking 1 year

    # skill demand by posting week: a to f were first posted on 24 Oct (week of 19 Oct), g on 26 Oct
    demand = {(r[0], r[1]): r[2:] for r in _all(conn, """
        SELECT skill_name, posting_week, new_postings, week_total_postings, share_of_week, week_fully_observed
        FROM metrics.skill_demand WHERE week_end_date = %s AND skill_name = 'Collaboration'""", D3)}
    assert demand[("Collaboration", date(2026, 10, 19))] == (5, 6, Decimal("0.83333"), False)   # f counts: seen, now closed
    assert demand[("Collaboration", date(2026, 10, 26))] == (1, 1, Decimal("1.00000"), False)
    assert _one(conn, "SELECT min(salary_mid_median), max(salary_mid_median) FROM metrics.skill_salary WHERE week_end_date = %s",
                D3) == (Decimal("3850.00"), Decimal("3850.00"))
    # g's $12 to $15 "monthly" salary is left out: Collaboration has 5 open postings but 4 usable salaries
    assert _one(conn, "SELECT postings FROM metrics.skill_salary WHERE week_end_date = %s AND skill_name = 'Collaboration'",
                D3) == (4,)
    assert _one(conn, "SELECT salary_usable FROM metrics.posting_week WHERE week_end_date = %s AND job_uuid = %s",
                D3, "g" * 32) == (False,)

    # MOM occupation groups of the open postings (the competitiveness breakdown by mom_group reads them)
    assert _all(conn, """SELECT mom_group, count(*) FROM metrics.posting_week WHERE week_end_date = %s
                         GROUP BY 1 ORDER BY 1""", D3) == [("PMET", 5), ("production_transport", 1)]

    log = _one(conn, "SELECT snapshot_date, snapshot_is_week_end, pool_postings, carried_forward, status, thresholds, steps "
                     "FROM metrics.run_log WHERE week_end_date = %s", D3)
    assert log[:5] == (D3, True, 6, 0, "ok")
    assert log[5]["skill_min_postings"] == 1 and set(log[6]) == set(METRIC_TABLES)

    # ---- week ending D2: e was skipped by the crawl on D2, so it stays open with its D1 numbers ------
    _run(conn, D2, "test-w2")
    pool2 = {r[0]: r[1:] for r in _all(conn, """SELECT left(job_uuid, 1), seen_on_snapshot, last_seen_date, views_total
                                               FROM metrics.posting_week WHERE week_end_date = %s""", D2)}
    assert sorted(pool2) == list("abcde") and pool2["e"] == (False, D1, 10)
    assert _one(conn, "SELECT carried_forward FROM metrics.run_log WHERE week_end_date = %s", D2) == (1,)
    # D2 deltas only: 11 applications over 4 x 16.8 h
    assert _one(conn, """SELECT apps_per_posting_day_24h FROM metrics.competitiveness
                         WHERE week_end_date = %s AND group_type = 'overall'""", D2) == (Decimal("3.9286"),)

    # ---- week ending Tue 27 Oct (the first scheduled run): no snapshot that Tuesday, so the latest one in
    # the week, Mon 26 Oct, is used and the reason recorded --------------------------------------------
    out = _run(conn, TUE_27, "test-fallback")
    assert out["status"] == "ok" and out["week"]["snapshot_date"] == str(D3)
    fb = _one(conn, """SELECT week_start_date, snapshot_date, snapshot_is_week_end, fallback_reason, snapshots_in_week
                       FROM metrics.run_log WHERE week_end_date = %s""", TUE_27)
    assert fb[:3] == (date(2026, 10, 21), D3, False) and "2026-10-26" in fb[3] and fb[4] == [D1, D2, D3]
    assert {r.name: r.status for r in out["checks"]}["snapshot_is_week_end"] == "warn"
    assert _one(conn, "SELECT count(DISTINCT snapshot_date) FROM metrics.posting_week WHERE week_end_date = %s",
                TUE_27) == (1,)

    # ---- the MOM comparison of the same week reads the same snapshot and the same occupation counts ---
    with conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO clean.mom_vacancy_quarterly (quarter, quarter_start, year, quarter_num, split,
                           mom_group, group_title, job_vacancy, covers_public_sector)
                       VALUES ('2026-Q2', '2026-04-01', 2026, 2, 'A', 'PMET', 'PMET', 600, TRUE),
                              ('2026-Q2', '2026-04-01', 2026, 2, 'A', 'clerical_sales_services', 'CSS', 100, TRUE),
                              ('2026-Q2', '2026-04-01', 2026, 2, 'A', 'production_transport', 'PT', 300, TRUE)""")
    try:
        cmp = mom_vacancies.build_comparison(conn, TUE_27 - timedelta(days=7), TUE_27)
        assert cmp["mcf_snapshot_date"] == str(D3)
        mom_counts = dict(_all(conn, """SELECT mom_group, mcf_postings FROM metrics.mcf_vs_mom_share
                                        WHERE mcf_snapshot_date = %s AND split = 'A' AND mom_group <> 'unknown'""", D3))
        ours = dict(_all(conn, """SELECT group_key, postings FROM metrics.competitiveness
                                  WHERE week_end_date = %s AND group_type = 'mom_group'""", TUE_27))
        assert mom_counts == {"PMET": 5, "clerical_sales_services": 0, "production_transport": 1}
        assert ours == {g: n for g, n in mom_counts.items() if n}
    finally:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM metrics.mcf_vs_mom_share")
            cur.execute("DELETE FROM clean.mom_vacancy_quarterly")

    # ---- a week without a snapshot fails before writing anything (as the MOM comparison does) ----------
    with pytest.raises(NoSnapshotInWeek, match="2026-10-14 to 2026-10-20"):
        _run(conn, date(2026, 10, 20), "test-empty")
    assert _one(conn, "SELECT count(*) FROM metrics.run_log WHERE week_end_date = %s", date(2026, 10, 20)) == (0,)
    assert all(rows == [] for rows in _week_rows(conn, date(2026, 10, 20)).values())

    # ---- idempotency: rerunning D3 gives identical rows and does not touch other weeks ---------------
    before_d3, before_d2 = _week_rows(conn, D3), _week_rows(conn, D2)
    _run(conn, D3, "test-w3-rerun")
    assert _week_rows(conn, D3) == before_d3
    assert _week_rows(conn, D2) == before_d2
    assert _one(conn, "SELECT count(*) FROM metrics.run_log") == (3,)
