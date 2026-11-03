"""End-to-end test against a real Postgres (skipped unless WAREHOUSE_TEST_DSN is set).

    WAREHOUSE_TEST_DSN=postgresql://jobs:<pw>@localhost:5433/jobs pytest tests/test_integration_warehouse.py -v

It creates a scratch database `is3107_test` on that server, applies sql/init/01, 02 and 03 (which also
proves the init scripts are idempotent), loads a 4-posting fixture for two days, runs the transform
twice for the second day and checks: row counts, SCD2 versions, deltas and hours, rejected rows,
data-quality verdicts, and that the rerun changed nothing. Docker-free runs of `pytest` skip it.
"""
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2.extras import execute_values  # noqa: E402

from quality.clean_checks import failed_checks, run_checks  # noqa: E402
from transform.load_reference import load_reference  # noqa: E402
from transform.skill_mapping import match_rate_report  # noqa: E402
from transform.transform_snapshot import transform_snapshot  # noqa: E402

DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="set WAREHOUSE_TEST_DSN to run against a Postgres warehouse")
REPO = Path(__file__).resolve().parent.parent
TEST_DB = "is3107_test"
D1, D2 = date(2026, 10, 24), date(2026, 10, 25)
T1 = datetime(2026, 10, 24, 9, 35, tzinfo=timezone.utc)
T2 = datetime(2026, 10, 25, 2, 23, tzinfo=timezone.utc)
# A 4-posting fixture trips the two scale checks on purpose (under 100 raw rows, and 1 of 3 postings
# out of range is above 1%). Those two failing is the expected verdict here; anything else is a bug.
SCALE_CHECKS = {"raw_rows_present", "experience_out_of_range_share"}


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
        for f in ("01_raw_schema.sql", "02_clean_schema.sql", "02_clean_schema.sql",   # twice: idempotent
                  "03_mom_vacancies.sql", "03_mom_vacancies.sql"):
            cur.execute((REPO / "sql" / "init" / f).read_text(encoding="utf-8"))
    load_reference(c, REPO / "data" / "reference")
    yield c
    c.close()


def _load_raw(conn, snapshot_date, payloads, fetched_at, jobs_fetched=None, api_total=None):
    with conn, conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO raw.mcf_job_snapshots (snapshot_date, job_uuid, fetched_at, payload) VALUES %s
            ON CONFLICT (snapshot_date, job_uuid) DO UPDATE SET payload = EXCLUDED.payload, fetched_at = EXCLUDED.fetched_at
        """, [(snapshot_date, p["uuid"], fetched_at, json.dumps(p)) for p in payloads])
        cur.execute("""
            INSERT INTO raw.ingest_runs (snapshot_date, pages_fetched, jobs_fetched, api_total, seconds_taken, finished_at)
            VALUES (%s, 1, %s, %s, 1.0, %s)
            ON CONFLICT (snapshot_date) DO UPDATE SET jobs_fetched = EXCLUDED.jobs_fetched, api_total = EXCLUDED.api_total,
                finished_at = EXCLUDED.finished_at
        """, (snapshot_date, jobs_fetched or len(payloads), api_total or len(payloads), fetched_at))


def _count(conn, sql, *params):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def _snapshot_of_tables(conn):
    out = {}
    with conn.cursor() as cur:
        for table, order in (("clean.dim_job", "job_uuid, valid_from"), ("clean.fact_job_day", "snapshot_date, job_uuid"),
                             ("clean.bridge_job_skill", "job_key, skill_key"), ("clean.dim_skill", "mcf_skill_uuid"),
                             ("clean.rejected_rows", "snapshot_date, job_uuid, reason")):
            cols = "*" if table != "clean.dim_job" else \
                "job_uuid, title, salary_max, attr_hash, valid_from, valid_to, is_current, last_seen_date"
            if table == "clean.rejected_rows":
                cols = "snapshot_date, job_uuid, reason"
            if table == "clean.fact_job_day":
                cols = "snapshot_date, job_uuid, job_key, views_total, applications_total, views_delta, applications_delta, " \
                       "hours_since_prev, is_first_snapshot, reposted_since_prev, prev_snapshot_date"
            cur.execute(f"SELECT {cols} FROM {table} ORDER BY {order}")
            out[table] = cur.fetchall()
    return out


def test_two_days_then_rerun(conn, make_payload):
    a = make_payload(uuid="a" * 32, **{"metadata.totalNumberOfView": 100, "metadata.totalNumberJobApplication": 5})
    b = make_payload(uuid="b" * 32, title="Data Engineer", minimumYearsExperience=88,
                     **{"metadata.totalNumberOfView": 10, "metadata.totalNumberJobApplication": 0})
    c = make_payload(uuid="c" * 32, title="Cleaner")
    bad = make_payload(uuid="d" * 32, **{"metadata.totalNumberOfView": None})
    _load_raw(conn, D1, [a, b, c, bad], T1)

    s1 = transform_snapshot(conn, D1, run_id="test-d1")
    assert s1["raw_rows"] == 4 and s1["fact_rows"] == 3 and s1["rejected"] == 1
    assert s1["versions_inserted"] == 3 and s1["fact_rows_with_previous"] == 0
    assert _count(conn, "SELECT count(*) FROM clean.dim_skill") == 3          # Nursing, Perform Ecg, Team Player
    assert _count(conn, "SELECT count(*) FROM clean.bridge_job_skill") == 9
    assert _count(conn, "SELECT min_years_exp FROM clean.dim_job WHERE job_uuid = %s", "b" * 32) == 40
    assert _count(conn, "SELECT count(*) FROM clean.dim_job WHERE exp_out_of_range") == 1
    assert _count(conn, "SELECT match_method FROM clean.dim_skill WHERE mcf_skill_name = 'Team Player'") == "alias"

    # Day 2: a gains views, b is edited (salary up) and reposted, c disappears, e is new.
    a2 = make_payload(uuid="a" * 32, **{"metadata.totalNumberOfView": 130, "metadata.totalNumberJobApplication": 6})
    b2 = make_payload(uuid="b" * 32, title="Data Engineer", minimumYearsExperience=88,
                      **{"salary.maximum": 6000, "metadata.editCount": 1, "metadata.repostCount": 1,
                         "metadata.newPostingDate": "2026-10-25", "metadata.totalNumberOfView": 10,
                         "metadata.totalNumberJobApplication": 0})
    e = make_payload(uuid="e" * 32, title="New Posting")
    _load_raw(conn, D2, [a2, b2, e], T2, jobs_fetched=3, api_total=3)

    s2 = transform_snapshot(conn, D2, run_id="test-d2")
    assert s2["versions_inserted"] == 2 and s2["versions_closed"] == 1 and s2["versions_touched"] == 1
    assert _count(conn, "SELECT count(*) FROM clean.dim_job") == 5
    assert _count(conn, "SELECT count(*) FROM clean.dim_job WHERE is_current") == 4
    with conn.cursor() as cur:
        cur.execute("SELECT valid_from, valid_to, is_current, salary_max FROM clean.dim_job WHERE job_uuid = %s ORDER BY valid_from", ("b" * 32,))
        versions = cur.fetchall()
    assert [(v[0], v[1], v[2]) for v in versions] == [(D1, D1, False), (D2, None, True)]
    assert versions[1][3] == 6000
    with conn.cursor() as cur:
        cur.execute("SELECT views_delta, applications_delta, hours_since_prev, reposted_since_prev, is_first_snapshot "
                    "FROM clean.fact_job_day WHERE snapshot_date = %s AND job_uuid = %s", (D2, "a" * 32))
        v_delta, a_delta, hours, reposted, first = cur.fetchone()
        assert (v_delta, a_delta, float(hours), reposted, first) == (30, 1, 16.8, False, False)
        cur.execute("SELECT reposted_since_prev, edited_since_prev FROM clean.fact_job_day WHERE snapshot_date = %s AND job_uuid = %s", (D2, "b" * 32))
        assert cur.fetchone() == (True, True)
        cur.execute("SELECT is_first_snapshot, views_delta FROM clean.fact_job_day WHERE snapshot_date = %s AND job_uuid = %s", (D2, "e" * 32))
        assert cur.fetchone() == (True, None)
    # the fact row for b on day 2 points at the NEW version, day 1 at the old one
    assert _count(conn, "SELECT count(DISTINCT job_key) FROM clean.fact_job_day WHERE job_uuid = %s", "b" * 32) == 2

    results = run_checks(conn, D2)
    assert {r.name for r in failed_checks(results)} == SCALE_CHECKS
    by_name = {r.name: r for r in results}
    assert by_name["experience_out_of_range"].status == "warn" and by_name["experience_out_of_range"].observed == 1
    assert by_name["coverage_ge_90pct"].status == "pass"
    assert by_name["fact_plus_rejected_equals_raw"].status == "pass"
    rates = match_rate_report(conn, D2)
    assert rates["mentions"] == 9 and 0 < rates["mapped_share_by_mentions"] <= 1

    # Idempotency: rerun day 2, nothing changes.
    before = _snapshot_of_tables(conn)
    s3 = transform_snapshot(conn, D2, run_id="test-d2-rerun")
    assert s3["versions_inserted"] == 0 and s3["versions_closed"] == 0 and s3["versions_touched"] == 3
    assert _snapshot_of_tables(conn) == before
    assert {r.name for r in failed_checks(run_checks(conn, D2))} == SCALE_CHECKS

    # Reloading the OLDER day re-derives day 2's deltas against it (no change here, but no crash and no drift).
    s4 = transform_snapshot(conn, D1, run_id="test-d1-rerun")
    assert s4["next_snapshot_rows_recomputed"] == 2        # a and b have a day-2 row
    assert _snapshot_of_tables(conn) == before
    assert _count(conn, "SELECT count(*) FROM clean.load_log WHERE step = 'parse_raw'") == 4
