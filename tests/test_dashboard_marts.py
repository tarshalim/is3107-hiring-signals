"""Dashboard marts and queries against a real Postgres (skipped unless WAREHOUSE_TEST_DSN is set).

    WAREHOUSE_TEST_DSN=postgresql://jobs:<pw>@localhost:5433/jobs pytest tests/test_dashboard_marts.py -v

Builds a scratch database `is3107_marts_test`, applies sql/init/01 and 02, inserts a small hand-made star
(3 postings over 14 days, one of them starting on day 8) and checks the numbers the dashboard would show.
"""
import os
from datetime import date, timedelta
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")
pd = pytest.importorskip("pandas")
st = pytest.importorskip("streamlit")

from transform.refresh_marts import MARTS, rebuild_marts  # noqa: E402

DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="set WAREHOUSE_TEST_DSN to run against a Postgres warehouse")
REPO = Path(__file__).resolve().parent.parent
TEST_DB = "is3107_marts_test"
FIRST = date(2026, 10, 1)
DAYS = 14
LAST = FIRST + timedelta(days=DAYS - 1)


def _test_dsn():
    base, _, _db = DSN.rpartition("/")
    return f"{base}/{TEST_DB}"


def _seed(cur):
    cur.execute("""
        INSERT INTO clean.ref_ssg_skill (skill_id, skill_title, norm_title, skill_type, is_emerging) VALUES
          ('ssg-python', 'Python Programming', 'python programming', 'tsc', TRUE),
          ('ssg-sql', 'SQL Database', 'sql database', 'tsc', FALSE)
    """)
    cur.execute("""
        INSERT INTO clean.dim_skill (skill_key, mcf_skill_uuid, mcf_skill_name, norm_name, ssg_skill_id,
                                     ssg_skill_title, ssg_skill_type, is_emerging, is_casl, match_method,
                                     match_confidence, first_seen_date, last_seen_date) VALUES
          (1, 'u-py',   'Python', 'python', 'ssg-python', 'Python Programming', 'tsc', TRUE,  FALSE, 'exact',     1.0, %(f)s, %(l)s),
          (2, 'u-sql',  'SQL',    'sql',    'ssg-sql',    'SQL Database',       'tsc', FALSE, FALSE, 'exact',     1.0, %(f)s, %(l)s),
          (3, 'u-mysql','MySQL',  'mysql',  'ssg-sql',    'SQL Database',       'tsc', FALSE, FALSE, 'alias',     0.9, %(f)s, %(l)s),
          (4, 'u-excel','Excel',  'excel',  NULL,         NULL,                 NULL,  NULL,  NULL,  'unmatched', NULL, %(f)s, %(l)s)
    """, {"f": FIRST, "l": LAST})

    jobs = [  # job_key, uuid, ssoc, major, mom, category, salary_min, salary_max, hidden, first day index,
              # employer, industry, position level, years of experience asked
        (1, "j1", "25122", 2, "PMET", "Information Technology", 4000, 6000, False, 0,
         "Acme", "Software", "Executive", 3),
        (2, "j2", "41101", 4, "clerical_sales_services", "Admin", 2000, 2400, False, 0,
         "Bolt", "Retail", "Non-executive", 0),
        (3, "j3", "25122", 2, "PMET", "Information Technology", 0, 0, True, 7,
         "Acme", "Software", "Executive", 5),
    ]
    for key, uuid, ssoc, major, mom, cat, smin, smax, hidden, start, company, industry, level, years in jobs:
        cur.execute("""
            INSERT INTO clean.dim_job (job_key, job_uuid, title, ssoc_code, ssoc_title, ssoc_major_group, mom_group,
                                       category, salary_min, salary_max, salary_is_hidden, vacancies, attr_hash,
                                       valid_from, last_seen_date, company_name, company_ssic_desc, position_level,
                                       min_years_exp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s, %s, %s, %s, %s, %s)
        """, (key, uuid, f"title {uuid}", ssoc, f"occupation {ssoc}", major, mom, cat, smin, smax, hidden,
              f"hash{key}", FIRST + timedelta(days=start), LAST, company, industry, level, years))
    cur.execute("""
        INSERT INTO clean.bridge_job_skill (job_key, skill_key) VALUES (1, 1), (1, 2), (1, 3), (2, 4), (3, 1)
    """)
    cur.execute("SELECT setval('clean.dim_job_job_key_seq', 3), setval('clean.dim_skill_skill_key_seq', 4)")

    for i in range(DAYS):
        day = FIRST + timedelta(days=i)
        cur.execute("""
            INSERT INTO clean.dim_date (date_key, date, year, month, day, day_of_week, day_name, is_weekend,
                                        week_start, iso_week)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (int(day.strftime("%Y%m%d")), day, day.year, day.month, day.day, day.isoweekday(),
              day.strftime("%A"), day.isoweekday() >= 6, day - timedelta(days=day.weekday()), day.isocalendar()[1]))
        for key, uuid, *_rest in jobs:
            start = _rest[-5]
            if i < start:
                continue
            first = i == start
            cur.execute("""
                INSERT INTO clean.fact_job_day (snapshot_date, job_uuid, job_key, date_key, views_total,
                    applications_total, repost_count, edit_count, status, days_since_original_posting,
                    prev_snapshot_date, hours_since_prev, is_first_snapshot, views_delta, applications_delta,
                    reposted_since_prev, edited_since_prev)
                VALUES (%s, %s, %s, %s, %s, %s, 0, 0, 'Open', %s, %s, %s, %s, %s, %s, %s, FALSE)
            """, (day, uuid, key, int(day.strftime("%Y%m%d")), 10 * (i - start + 1), 2 * (i - start + 1),
                  i - start, None if first else day - timedelta(days=1), None if first else 24, first,
                  None if first else 10, None if first else 2, uuid == "j2" and i == DAYS - 1))


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
        for f in ("01_raw_schema.sql", "02_clean_schema.sql", "08_salary_model.sql"):
            cur.execute((REPO / "sql" / "init" / f).read_text(encoding="utf-8"))
        _seed(cur)
    yield c
    c.close()


@pytest.fixture(scope="module")
def marts(conn):
    return rebuild_marts(conn)


@pytest.fixture(scope="module")
def queries(conn, marts):
    patch = pytest.MonkeyPatch()
    patch.setenv("WAREHOUSE_DSN", _test_dsn())
    from dashboard import queries as q
    st.cache_data.clear()
    yield q
    patch.undo()


def _scalar(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def test_rebuild_reports_every_mart_and_is_idempotent(conn, marts):
    assert set(marts["rows"]) == set(MARTS)
    assert all(n > 0 for n in marts["rows"].values())
    assert rebuild_marts(conn)["rows"] == marts["rows"]


def test_the_sql_file_applies_to_empty_tables_as_on_first_container_start():
    admin = psycopg2.connect(DSN)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS is3107_marts_empty")
        cur.execute("CREATE DATABASE is3107_marts_empty")
    admin.close()
    base, _, _db = DSN.rpartition("/")
    c = psycopg2.connect(f"{base}/is3107_marts_empty")
    try:
        with c, c.cursor() as cur:
            for f in ("01_raw_schema.sql", "02_clean_schema.sql", "07_dashboard_marts.sql", "08_salary_model.sql"):
                cur.execute((REPO / "sql" / "init" / f).read_text(encoding="utf-8"))
    finally:
        c.close()


def test_a_posting_with_two_tags_of_one_skill_counts_once(conn, marts):
    assert _scalar(conn, "SELECT count(*) FROM mart.job_skill") == 4      # 5 bridge rows, j1's two SQL tags merge
    sql_postings = _scalar(conn, """
        SELECT postings FROM mart.skill_slice_day WHERE skill_id = 'ssg-sql' AND snapshot_date = %s
    """, (LAST,))
    assert sql_postings == 1


def test_slice_day_counts_and_normalises_rates_per_day(conn, marts):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT mom_group, postings, rated_postings, applications_per_day, views_per_day
            FROM mart.slice_day WHERE snapshot_date = %s ORDER BY mom_group
        """, (LAST,))
        rows = {r[0]: r[1:] for r in cur.fetchall()}
    assert rows["PMET"] == (2, 2, 4, 20)
    assert rows["clerical_sales_services"] == (1, 1, 2, 10)
    first_day = _scalar(conn, "SELECT sum(rated_postings) FROM mart.slice_day WHERE snapshot_date = %s", (FIRST,))
    assert first_day == 0                                   # first snapshots have no delta, so no rate


def test_reposts_are_counted_on_the_day_they_happen(conn, marts):
    assert _scalar(conn, "SELECT sum(reposts) FROM mart.slice_day") == 1


def test_salary_percentiles_ignore_hidden_salaries(conn, marts):
    row = _scalar(conn, "SELECT row(salary_postings, salary_p50) FROM mart.skill_salary_latest WHERE skill_id = 'ssg-python'")
    assert row == "(1,5000)"                                 # j3 hides its salary, so only j1 counts
    assert _scalar(conn, "SELECT salary_p50 FROM mart.occupation_latest WHERE ssoc_code = '41101'") == 2200


def test_skill_pairs_are_ordered_and_unique(conn, marts):
    with conn.cursor() as cur:
        cur.execute("SELECT skill_a, skill_b, postings, salary_p50 FROM mart.skill_pair_latest")
        assert cur.fetchall() == [("ssg-python", "ssg-sql", 1, 5000)]


def test_employer_and_industry_marts(conn, marts):
    with conn.cursor() as cur:
        cur.execute("SELECT employer, mom_group, postings, industry FROM mart.company_latest ORDER BY employer")
        assert cur.fetchall() == [("Acme", "PMET", 2, "Software"), ("Bolt", "clerical_sales_services", 1, "Retail")]
        cur.execute("SELECT employer, skill_id, postings FROM mart.company_skill_latest")
        assert cur.fetchall() == [("Acme", "ssg-python", 2)]          # skills held by a single posting are dropped
        cur.execute("SELECT industry, postings, salary_postings, salary_p50 FROM mart.industry_latest ORDER BY 1")
        assert cur.fetchall() == [("Retail", 1, 1, 2200), ("Software", 2, 1, 5000)]


def test_salary_drivers_bucket_experience_level_and_skill_count(conn, marts):
    with conn.cursor() as cur:
        cur.execute("SELECT driver, bucket, salary_postings, salary_p50 FROM mart.salary_driver_latest ORDER BY 1, 2")
        rows = {(d, b): (n, p) for d, b, n, p in cur.fetchall()}
    assert rows[("experience", "3-5 years")] == (1, 5000)
    assert rows[("experience", "None")] == (1, 2200)
    assert rows[("position_level", "Executive")] == (1, 5000)
    assert rows[("skill_count", "1-2 skills")] == (2, 3600)             # j1 has 2 distinct skills, j2 has 1


def test_postings_without_an_employer_are_not_ranked(conn, marts):
    assert _scalar(conn, "SELECT count(*) FROM mart.company_latest WHERE employer = 'Unknown employer'") == 0


def test_training_is_skipped_when_there_is_too_little_data(conn, marts):
    from transform.salary_model import load_model, train
    assert "skipped" in train(conn)
    assert load_model(conn) is None
    conn.rollback()


def test_trending_share_and_week_over_week_change(queries):
    from dashboard.filters import Filters
    top = queries.trending_skills(Filters(), limit=20, min_mentions=1, min_prev_mentions=1).set_index("skill_label")
    python = top.loc["Python Programming"]
    assert python["mentions"] == 21
    assert python["share"] == pytest.approx(21 / 35)
    assert python["wow_pct"] == pytest.approx(100 / 3)      # share 50% -> 66.7%
    assert top.loc["SQL Database", "mentions"] == 14        # j1 only, not three tags


def test_trending_filters_narrow_the_slice(queries):
    from dashboard.filters import Filters
    only_clerical = queries.trending_skills(Filters(mom_groups=("clerical_sales_services",)),
                                            min_mentions=1, min_prev_mentions=1)
    assert only_clerical["skill_label"].tolist() == ["Excel"]
    assert queries.trending_skills(Filters(skill_types=("unmapped",)), min_mentions=1)["skill_label"].tolist() == ["Excel"]
    emerging = queries.trending_skills(Filters(emerging_only=True), min_mentions=1)
    assert emerging["skill_label"].tolist() == ["Python Programming"]
    assert queries.trending_skills(Filters(major_groups=(4,)), min_mentions=1)["skill_label"].tolist() == ["Excel"]
    assert queries.trending_skills(Filters(categories=("Admin",)), min_mentions=1)["skill_label"].tolist() == ["Excel"]


def test_min_mentions_hides_rare_skills(queries):
    from dashboard.filters import Filters
    labels = queries.trending_skills(Filters(), min_mentions=20)["skill_label"].tolist()
    assert labels == ["Python Programming"]


def test_window_limits_the_days_counted(queries):
    from dashboard.filters import Filters
    top = queries.trending_skills(Filters(window_days=7), min_mentions=1, min_prev_mentions=1).set_index("skill_label")
    assert top.loc["Python Programming", "mentions"] == 14


def test_every_dashboard_query_runs(queries):
    from dashboard.filters import Filters
    f = Filters()
    assert queries.coverage().iloc[0]["days"] == DAYS
    options = queries.filter_options()
    assert "PMET" in options["mom_groups"]["mom_group"].tolist()
    daily = queries.skill_share_by_day(f, ("ssg-python",))
    assert len(daily) == DAYS
    market = queries.market_by_day(f)
    assert market.iloc[-1]["applications_per_posting"] == pytest.approx(2.0)
    assert len(queries.skill_salaries(f, 1)) >= 2
    assert len(queries.skill_pair_salaries(f, 1)) == 1
    assert set(queries.occupation_salaries(f, 1)["ssoc_title"]) == {"occupation 25122", "occupation 41101"}
    assert len(queries.occupation_difficulty(f, 1)) == 2
    assert not queries.skill_difficulty(f, 1).empty
    occupations, skills = queries.occupation_skill_profiles()
    assert list(occupations.columns) == ["ssoc_code", "ssoc_title", "mom_group", "postings"]
    assert "skill_postings" in skills.columns
    industries = queries.industry_overview(f)
    assert industries["industry"].tolist() == ["Software", "Retail"]
    assert industries.iloc[0]["posting_days"] == 21
    assert industries.iloc[0]["share"] == pytest.approx(21 / 35)
    heat = queries.industry_skill_heatmap(f)
    assert heat.to_dict("records") == [{"industry": "Software", "skill_label": "Python Programming", "share": 1.0}]
    assert queries.industry_top_skills(f, "Software")["skill_label"].tolist() == ["Python Programming"]
    employers = queries.top_employers(f)
    assert employers["employer"].tolist() == ["Acme", "Bolt"]
    assert employers.iloc[0]["total_postings"] == 3
    assert queries.top_employers(Filters(mom_groups=("PMET",)))["employer"].tolist() == ["Acme"]
    assert queries.employer_top_skills(f, "Acme").iloc[0]["share"] == 1.0
    assert len(queries.salary_drivers(1)) == 5
    assert queries.industry_salaries(1)["industry"].tolist() == ["Software", "Retail"]
    assert len(queries.estimator_occupations()) == 2
    assert queries.salary_model() is None
    assert queries.freshness().iloc[0]["mart_latest"] == LAST
    queries.skill_match_quality()
    queries.ingest_runs()
    queries.load_steps()
    queries.dq_results()
