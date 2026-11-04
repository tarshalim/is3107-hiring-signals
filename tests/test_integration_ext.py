"""End-to-end test of the ext layer against a real Postgres (skipped unless WAREHOUSE_TEST_DSN is set).

    WAREHOUSE_TEST_DSN=postgresql://jobs:<pw>@localhost:5433/jobs pytest tests/test_integration_ext.py -v

Creates a scratch database `is3107_ext_test`, applies every sql/init file in name order as the warehouse
container does, then all of them again (idempotent), runs the real daily transform on a 5-posting fixture
over three days, then loads a tiny Kaggle-shaped file and two data.gov.sg-shaped CSVs, builds the posting
history, and checks every table and view. Reruns must change nothing.
"""
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2.extras import execute_values  # noqa: E402

from ingestion.datagov_wages import load_lfs_income, load_ows  # noqa: E402
from ingestion.kaggle_mcf2019 import load_kaggle_mcf2019, map_titles_to_ssoc  # noqa: E402
from transform.posting_history import build_posting_history  # noqa: E402
from transform.load_reference import load_reference  # noqa: E402
from transform.transform_snapshot import transform_snapshot  # noqa: E402

DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="set WAREHOUSE_TEST_DSN to run against a Postgres warehouse")
REPO = Path(__file__).resolve().parent.parent
TEST_DB = "is3107_ext_test"
D1, D2, D3 = date(2026, 10, 24), date(2026, 10, 25), date(2026, 10, 28)
FETCHED = {D1: datetime(2026, 10, 24, 9, 35, tzinfo=timezone.utc), D2: datetime(2026, 10, 25, 2, 23, tzinfo=timezone.utc),
           D3: datetime(2026, 10, 28, 2, 30, tzinfo=timezone.utc)}

KAGGLE_JOBS = {"jobs": [
    {"company_name": "EXAMPLE CLINIC", "job_title": "Staff Nurse", "employment_type": "Full Time",
     "seniority": "Executive", "job_category": ["Healthcare ", " Pharmaceutical"], "location": "x",
     "salary": "$3,000to$4,000Monthly", "min_experience": "2 years  exp",
     "skills_required": ["Nursing", "Team Player", "Patient Care"], "requirements_and_role": "x",
     "job_requirements": "x", "company_info": "x", "posting_date": "10 May 2019", "expiry_date": "09 Jun 2019",
     "no_of_applications": "3", "job_id": "JOB-2019-0000001"},
    {"company_name": "EXAMPLE SEARCH", "job_title": "Data Engineer", "employment_type": "Full Time",
     "seniority": "Professional", "job_category": "['Information Technology']", "location": "x",
     "salary": "Salary undisclosed", "min_experience": "3 years  exp", "skills_required": "['Python', 'SQL']",
     "requirements_and_role": "x", "job_requirements": "x", "company_info": "x", "posting_date": "11 May 2019",
     "expiry_date": "10 Jun 2019", "no_of_applications": "0", "job_id": "Recruiter"},
    {k: "not_available" for k in ("company_name", "job_title", "salary", "job_id", "skills_required")},
]}
OWS_CSV = ("year,occ_desc,mthly_gross_wage_75_pctile,mthly_gross_wage_50_pctile,mthly_gross_wage_25_pctile,"
           "mthly_basic_wage_75_pctile,mthly_basic_wage_50_pctile,mthly_basic_wage_25_pctile\n"
           '2024,"registered nurse and other nursing professional (e.g. clinical nurse, nurse educator, excluding '
           'enrolled nurse)",5500,4500,3800,5000,4200,3500\n')
LFS_CSV = ("DataSeries,2024,2019\nProfessionals - Male,9156,7805\nProfessionals - Female,7722,6580\n"
           "Clerical Support Workers - Male,3358,2779\n")


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
        assert "04_ext_schema.sql" in [f.name for f in files]
        for f in files + files:                                      # twice: idempotent
            cur.execute(f.read_text(encoding="utf-8"))
    load_reference(c, REPO / "data" / "reference")
    yield c
    c.close()


def _load_raw(conn, snapshot_date, payloads):
    with conn, conn.cursor() as cur:
        execute_values(cur, "INSERT INTO raw.mcf_job_snapshots (snapshot_date, job_uuid, fetched_at, payload) VALUES %s",
                       [(snapshot_date, p["uuid"], FETCHED[snapshot_date], json.dumps(p)) for p in payloads])
        cur.execute("INSERT INTO raw.ingest_runs (snapshot_date, pages_fetched, jobs_fetched, api_total, seconds_taken, "
                    "finished_at) VALUES (%s, 1, %s, %s, 1.0, %s)",
                    (snapshot_date, len(payloads), len(payloads), FETCHED[snapshot_date]))


def _one(conn, sql, *params):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def _posting(make_payload, uuid, original, new, expiry, repost=0, title="Staff Nurse (Health Screening)", **kw):
    return make_payload(uuid=uuid * 32, title=title, **{"metadata.originalPostingDate": original,
                        "metadata.newPostingDate": new, "metadata.expiryDate": expiry,
                        "metadata.repostCount": repost}, **kw)


def test_ext_layer_end_to_end(conn, make_payload, tmp_path):
    a = _posting(make_payload, "a", "2026-10-24", "2026-10-24", "2026-11-23")              # 30-day period
    b = _posting(make_payload, "b", "2026-08-03", "2026-10-10", "2026-11-09", 1, title="Data Engineer",
                 ssocCode="25121")                                                            # old job, reposted
    c = _posting(make_payload, "c", "2026-10-14", "2026-10-14", "2026-10-28")              # closes early after D2
    e = _posting(make_payload, "e", "2026-10-19", "2026-10-19", "2026-10-26")              # 7 days, expires normally
    b3 = _posting(make_payload, "b", "2026-08-03", "2026-10-28", "2026-11-27", 2, title="Data Engineer",
                  ssocCode="25121")                                                           # reposted again
    d = _posting(make_payload, "d", "2026-10-26", "2026-10-26", "2026-11-25", title="Cleaner", ssocCode="91131")
    for day, payloads in ((D1, [a, b, c, e]), (D2, [a, b, c, e]), (D3, [a, b3, d])):
        _load_raw(conn, day, payloads)
        transform_snapshot(conn, day, run_id=f"test-{day}")

    h = build_posting_history(conn)
    assert (h["first_snapshot"], h["last_snapshot"], h["jobs"], h["posting_periods"]) == ("2026-10-24", "2026-10-28", 5, 6)
    assert h["duration_mix"] == {7: 0.5, 30: 0.5}                          # a and e started 18 to 24 Oct
    assert h["early_close_rate_per_day"] == pytest.approx(1 / 8)           # c gone after 25 Oct with expiry ahead
    weeks = {}
    with conn.cursor() as cur:
        cur.execute("SELECT week_start, first_postings, periods_started, reposts_started, coverage_class, est_coverage "
                    "FROM ext.posting_history_week ORDER BY 1")
        for w, fp, ps, rp, cls, cov in cur.fetchall():
            weeks[w] = (fp, ps, rp, cls, cov)
    assert weeks[date(2026, 8, 3)][:4] == (1, 0, 0, "survivors_only")      # b's first posting
    assert weeks[date(2026, 10, 5)][:4] == (0, 1, 1, "partial")             # b's first repost
    assert weeks[date(2026, 10, 12)][:4] == (1, 1, 0, "partial")            # c
    assert weeks[date(2026, 10, 19)][:4] == (2, 2, 0, "near_complete")      # a and e
    assert weeks[date(2026, 10, 26)][:4] == (1, 2, 1, "in_progress")        # d, and b's second repost
    assert float(weeks[date(2026, 10, 12)][4]) == pytest.approx(0.5 * (7 / 8) ** 10, abs=1e-3)
    assert _one(conn, "SELECT sum(first_postings) FROM ext.posting_history_ssoc_week")[0] == 5
    assert _one(conn, "SELECT first_postings, periods_started FROM ext.posting_history_ssoc_week "
                      "WHERE week_start = '2026-10-19' AND ssoc_major_group = 2") == (2, 2)
    team_player = _one(conn, "SELECT sum(first_postings) FROM ext.v_skill_week_share WHERE mcf_skill_name = 'Team Player'")
    assert team_player[0] == 5                                             # every fixture posting lists it

    kaggle = tmp_path / "mycareersfuture.json"
    kaggle.write_text(json.dumps(KAGGLE_JOBS), encoding="utf-8")
    k = load_kaggle_mcf2019(conn, kaggle)
    assert (k["records"], k["rejected"], k["loaded"], k["distinct_tags"]) == (3, 1, 2, 5)
    assert k["tag_match_methods"]["alias"] >= 1                            # 'Team Player' via the curated alias
    m = map_titles_to_ssoc(conn)
    assert m["postings_matched"] == 1                                      # 'Staff Nurse' borrows 22200 (a, c, e)
    assert _one(conn, "SELECT ssoc_code, ssoc_match_share FROM ext.mcf2019_posting WHERE job_id = 'JOB-2019-0000001'") \
        == ("22200", 1)

    ows_csv, lfs_csv = tmp_path / "ows.csv", tmp_path / "lfs.csv"
    ows_csv.write_text(OWS_CSV, encoding="utf-8")
    lfs_csv.write_text(LFS_CSV, encoding="utf-8")
    assert load_ows(conn, ows_csv)["ssoc_matched"] == 1
    assert load_lfs_income(conn, lfs_csv)["rows"] == 6

    skill = _one(conn, "SELECT postings_2019, postings_now, rank_now FROM ext.v_skill_2019_vs_now WHERE tag_key = 'team player'")
    assert skill == (1, 3, 1)                                              # D3 has a, b, d
    assert _one(conn, "SELECT postings_2019, median_salary_2019, postings_now FROM ext.v_salary_2019_vs_now "
                      "WHERE category = 'all'") == (1, 3500, 3)
    assert _one(conn, "SELECT postings_2019 FROM ext.v_salary_2019_vs_now WHERE category = 'Healthcare / Pharmaceutical'")[0] == 1
    pay = _one(conn, "SELECT postings_2019, advertised_median_2019, resident_income_change_pct FROM "
                     "ext.v_pay_change_by_ssoc_group WHERE ssoc_major_group = 2")
    assert pay[:2] == (1, 3500) and float(pay[2]) == pytest.approx(round(100 * (8439 / 7192.5 - 1), 1))
    postings, p50, ratio = _one(conn, "SELECT postings, gross_wage_p50, ratio_mid_to_median "
                                      "FROM ext.v_advertised_vs_actual_wage WHERE ssoc_code = '22200'")
    assert (postings, float(p50)) == (1, 4500.0)                           # only a is a nurse on D3
    assert float(ratio) == pytest.approx(3850 / 4500, abs=1e-3)
    assert _one(conn, "SELECT count(*) FROM ext.source_registry")[0] == 3

    # Reruns change nothing (timestamps aside).
    tables = {
        "posting_period": "*", "posting_history_skill_week": "*", "posting_history_ssoc_week": "*",
        "posting_history_week": "week_start, first_postings, periods_started, reposts_started, coverage_class, "
                                "est_coverage, periods_started_est, early_close_rate",
        "mcf2019_posting": "posting_key, title, categories, salary_monthly_mid, ssoc_code",
        "mcf2019_posting_skill": "*", "mcf2019_skill": "*",
        "mom_occupational_wage": "year, occupation, ssoc_code, gross_wage_p50",
        "mom_income_by_occupation": "year, ssoc_major_group, sex, median_income",
    }

    def state():
        out = {}
        with conn.cursor() as cur:
            for t, cols in tables.items():
                cur.execute(f"SELECT md5(string_agg(x::text, ',' ORDER BY x::text)) FROM (SELECT {cols} FROM ext.{t}) x")
                out[t] = cur.fetchone()[0]
        return out
    before = state()
    build_posting_history(conn)
    load_kaggle_mcf2019(conn, kaggle)
    map_titles_to_ssoc(conn)
    load_ows(conn, ows_csv)
    load_lfs_income(conn, lfs_csv)
    assert state() == before
