"""Weekly signal tabs against a real Postgres (skipped unless WAREHOUSE_TEST_DSN is set).

    WAREHOUSE_TEST_DSN=postgresql://jobs:<pw>@localhost:5433/jobs pytest tests/test_integration_dashboard_signals.py -v

Creates a scratch database `is3107_signals_test`, applies every sql/init file in name order twice (as the
warehouse container does, and idempotent), writes three hand-made weeks straight into the metrics tables (the
middle one failed its checks) and checks:
  - metrics.signal_weeks lists only finished weeks, and the week before skips the failed one
  - metrics.skill_signals lines up every signal per skill, with last week's flags NULL in the first week
  - skill demand uses only fully observed posting weeks, and 0 for a skill nobody asked for that week
  - every dashboard/signal_queries.py function returns those rows
  - the whole app renders every tab without an exception (Streamlit AppTest), with the daily marts empty
"""
import json
import os
from datetime import date
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")
pd = pytest.importorskip("pandas")
st = pytest.importorskip("streamlit")

DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="set WAREHOUSE_TEST_DSN to run against a Postgres warehouse")
REPO = Path(__file__).resolve().parent.parent
TEST_DB = "is3107_signals_test"
W1, W2, W3 = date(2026, 10, 27), date(2026, 11, 3), date(2026, 11, 10)     # W2 failed its checks
D_OLD, D_PREV, D_LAST, D_OPEN = date(2026, 10, 19), date(2026, 10, 26), date(2026, 11, 2), date(2026, 11, 9)


def _test_dsn():
    base, _, _db = DSN.rpartition("/")
    return f"{base}/{TEST_DB}"


def _seed(cur):
    for week, status in ((W1, "ok"), (W2, "failed_checks"), (W3, "ok")):
        cur.execute("""
            INSERT INTO metrics.run_log (week_end_date, week_start_date, snapshot_date, snapshot_is_week_end,
                snapshots_in_week, pool_from_date, pool_to_date, thresholds, pool_postings, carried_forward, status)
            VALUES (%(w)s, %(w)s - 6, %(w)s, TRUE, ARRAY[%(w)s], %(w)s - 2, %(w)s + 1, %(t)s, 1000, 10, %(s)s)
        """, {"w": week, "t": json.dumps({"barrier_min_lift": 1.5}), "s": status})

    # shortage: QS listed in W1 and W3; GL listed only in the failed W2 (must not count as "last week")
    for week, ref, name, lift in ((W1, "qs", "Quantity Surveying", 2.0), (W2, "gl", "General Ledger", 1.9),
                                  (W3, "qs", "Quantity Surveying", 2.5)):
        cur.execute("""
            INSERT INTO metrics.shortage_skill (week_end_date, snapshot_date, skill_ref, skill_name, is_mapped, is_casl,
                postings, shortage_postings, shortage_companies, top_company_share, shortage_share,
                overall_shortage_share, lift)
            VALUES (%s, %s, %s, %s, FALSE, FALSE, 1333, 86, 51, 0.13, 0.0645, 0.0254, %s)
        """, (week, week, ref, name, lift))

    # barrier: GL flagged in W3, present but not flagged in W1; QS present, not flagged in W3
    for week, ref, name, lift, flagged in ((W1, "gl", "General Ledger", 1.2, False),
                                           (W3, "gl", "General Ledger", 3.6, True),
                                           (W3, "qs", "Quantity Surveying", 1.3, False)):
        cur.execute("""
            INSERT INTO metrics.barrier_skill (week_end_date, snapshot_date, skill_ref, skill_name, is_mapped, is_casl,
                postings, companies, barrier_postings, barrier_companies, top_company_share, barrier_share,
                expected_share, lift, views, applications, is_barrier)
            VALUES (%s, %s, %s, %s, FALSE, FALSE, 115, 40, 31, 25, 0.13, 0.27, 0.075, %s, 1000, 50, %s)
        """, (week, week, ref, name, lift, flagged))

    cur.execute("""
        INSERT INTO metrics.skill_salary (week_end_date, snapshot_date, skill_ref, skill_name, is_mapped, postings,
            salary_mid_p25, salary_mid_median, salary_mid_p75, mid_vs_overall)
        VALUES (%(w)s, %(w)s, 'sm', 'Self Management', TRUE, 20000, 3000, 4000, 5500, 0.99),
               (%(w)s, %(w)s, 'qs', 'Quantity Surveying', FALSE, 1332, 3900, 4500, 5750, 1.111)
    """, {"w": W3})

    rows = [(W3, "overall", "all", "All postings", 88610, 0.35), (W3, "skill", "qs", "Quantity Surveying", 1333, 0.11),
            (W3, "skill", "gl", "General Ledger", 115, 1.57), (W3, "skill", "sm", "Self Management", 22128, 0.40),
            (W3, "mom_group", "PMET", "PMET", 65070, 0.36), (W3, "ssoc", "21413", "Quantity surveyor", 1269, 0.021),
            (W1, "skill", "qs", "Quantity Surveying", 1300, 0.06), (W1, "ssoc", "21413", "Quantity surveyor", 1250, 0.02)]
    for week, gtype, key, label, postings, apps in rows:
        cur.execute("""
            INSERT INTO metrics.competitiveness (week_end_date, snapshot_date, week_start_date, group_type, group_key,
                group_label, postings, apps_per_posting_day_24h)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (week, week, week, gtype, key, label, postings, apps))

    # demand in W3: two fully observed posting weeks and one still open (ignored); GL had no new posting in
    # the latest one. W1 has no fully observed week at all.
    demand = [(W3, "qs", "Quantity Surveying", False, None, D_PREV, True, 274, 0.012),
              (W3, "qs", "Quantity Surveying", False, None, D_LAST, True, 300, 0.015),
              (W3, "qs", "Quantity Surveying", False, None, D_OPEN, False, 50, 0.5),
              (W3, "gl", "General Ledger", False, None, D_PREV, True, 23, 0.001),
              (W3, "sm", "Self Management", True, "ccs", D_LAST, True, 5000, 0.25),
              (W1, "qs", "Quantity Surveying", False, None, D_OLD, False, 200, 0.01)]
    for week, ref, name, mapped, stype, pweek, full, n, share in demand:
        cur.execute("""
            INSERT INTO metrics.skill_demand (week_end_date, snapshot_date, skill_ref, skill_name, is_mapped, skill_type,
                posting_week, week_fully_observed, new_postings, week_total_postings, share_of_week)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 20000, %s)
        """, (week, week, ref, name, mapped, stype, pweek, full, n, share))

    for week, share in ((W1, 0.46), (W3, 0.4657)):
        cur.execute("""
            INSERT INTO metrics.unrealistic_requirements (week_end_date, snapshot_date, flag, group_type, group_key,
                group_label, base_postings, flagged_postings, flagged_share, flagged_salary_mid_median,
                other_salary_mid_median, rule)
            VALUES (%s, %s, 'junior_2plus_years', 'overall', 'all', 'All', 9413, 4384, %s, 3800, 3150, 'rule')
        """, (week, week, share))

    for week, gap in ((W1, 16.68), (W3, 17.19)):
        cur.execute("""
            INSERT INTO metrics.mcf_vs_mom_share (mcf_snapshot_date, split, mom_group, group_title, mcf_postings,
                mcf_vacancies, mcf_share_pct, mom_share_pct, share_gap_pp, disagrees, disagree_threshold_pp)
            VALUES (%s, 'A', 'PMET', 'Professionals, managers, executives & technicians', 64177, 193520,
                    73.41, 56.22, %s, TRUE, 10),
                   (%s, 'A', 'unknown', 'Missing or invalid ssocCode', 0, 0, NULL, NULL, NULL, FALSE, 10)
        """, (week, gap, week))


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
        assert "10_dashboard_signals.sql" in [f.name for f in files]
        for f in files + files:                                      # twice: idempotent
            cur.execute(f.read_text(encoding="utf-8"))
        _seed(cur)
    yield c
    c.close()


@pytest.fixture(scope="module")
def sq(conn):
    patch = pytest.MonkeyPatch()
    patch.setenv("WAREHOUSE_DSN", _test_dsn())
    from dashboard import signal_queries
    st.cache_data.clear()
    yield signal_queries
    patch.undo()


def _signals(conn, week):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM metrics.skill_signals WHERE week_end_date = %s", (week,))
        cols = [c.name for c in cur.description]
        return {r[cols.index("skill_ref")]: dict(zip(cols, r)) for r in cur.fetchall()}


def test_signal_weeks_skip_the_failed_week(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT week_end_date, prev_week_end_date FROM metrics.signal_weeks ORDER BY 1")
        assert cur.fetchall() == [(W1, None), (W3, W1)]


def test_skill_signals_line_up_every_signal(conn):
    rows = _signals(conn, W3)
    assert set(rows) == {"qs", "gl", "sm"}
    qs = rows["qs"]
    assert (qs["skill_name"], qs["open_postings"], qs["snapshot_date"]) == ("Quantity Surveying", 1333, W3)
    assert qs["shortage_listed"] and float(qs["shortage_lift"]) == 2.5
    assert qs["shortage_listed_prev_week"] is True and float(qs["shortage_lift_prev_week"]) == 2.0
    assert qs["is_barrier"] is False and float(qs["barrier_lift"]) == 1.3 and qs["is_barrier_prev_week"] is False
    assert float(qs["salary_mid_median"]) == 4500 and float(qs["salary_vs_overall"]) == pytest.approx(1.111)
    assert float(qs["apps_per_posting_day_24h"]) == pytest.approx(0.11)
    assert float(qs["overall_apps_per_posting_day_24h"]) == pytest.approx(0.35)
    assert float(qs["apps_per_posting_day_24h_prev_week"]) == pytest.approx(0.06)


def test_last_week_means_the_last_finished_week(conn):
    gl = _signals(conn, W3)["gl"]
    assert gl["is_barrier"] is True and gl["is_barrier_prev_week"] is False      # W1: present, not flagged
    assert gl["shortage_listed"] is False
    assert gl["shortage_listed_prev_week"] is False      # listed only in W2, which failed its checks


def test_first_week_has_nothing_to_compare(conn):
    first = _signals(conn, W1)
    assert set(first) == {"qs", "gl"}
    for row in first.values():
        assert row["prev_week_end_date"] is None
        assert row["shortage_listed_prev_week"] is None and row["is_barrier_prev_week"] is None
        assert row["demand_week"] is None and row["demand_share"] is None   # no fully observed posting week
    assert _signals(conn, W2) == {}                                          # failed week: not shown


def test_demand_uses_only_fully_observed_posting_weeks(conn):
    rows = _signals(conn, W3)
    qs, gl, sm = rows["qs"], rows["gl"], rows["sm"]
    assert (qs["demand_week"], qs["demand_week_before"]) == (D_LAST, D_PREV)       # D_OPEN is still filling
    assert float(qs["demand_share"]) == pytest.approx(0.015) and qs["demand_new_postings"] == 300
    assert float(qs["demand_share_week_before"]) == pytest.approx(0.012)
    assert float(gl["demand_share"]) == 0 and gl["demand_new_postings"] == 0     # asked for by nobody that week
    assert float(gl["demand_share_week_before"]) == pytest.approx(0.001)
    assert sm["skill_type"] == "ccs" and sm["is_mapped"] is True and float(sm["demand_share"]) == 0.25


def test_signal_queries_return_the_rows(sq):
    weeks = sq.weeks()
    assert weeks["week_end_date"].tolist() == [W3, W1]
    assert weeks.iloc[0]["thresholds"] == {"barrier_min_lift": 1.5}
    skills = sq.skill_signals(W3)
    assert skills["skill_ref"].tolist() == ["sm", "qs", "gl"]                     # most open postings first
    junior = sq.unrealistic(W3).iloc[0]
    assert junior["flagged_share"] == pytest.approx(0.4657) and junior["flagged_share_prev_week"] == pytest.approx(0.46)
    comp = sq.competitiveness(W3)
    assert set(comp["group_type"]) == {"overall", "mom_group", "ssoc"}           # 'skill' rows are not asked for
    surveyor = comp[comp["group_key"] == "21413"].iloc[0]
    assert surveyor["apps_per_posting_day_24h_prev_week"] == pytest.approx(0.02)
    market = sq.market_share(W3)
    pmet = market[market["mom_group"] == "PMET"].iloc[0]
    assert pmet["share_gap_pp"] == pytest.approx(17.19) and pmet["share_gap_pp_prev_week"] == pytest.approx(16.68)
    assert sq.market_share(W2).empty


def test_every_tab_renders_with_empty_daily_marts(sq):
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file(str(REPO / "dashboard" / "app.py"), default_timeout=60).run()
    assert not at.exception, [e.value for e in at.exception]
    assert not at.error, [e.value for e in at.error]
    assert [t.label for t in at.tabs][:3] == ["Hiring signals", "Skill lookup", "Market check"]
    labels = {m.label for m in at.metric}
    assert {"Shortage", "Barrier", "Unrealistic ask", "Demand", "Pay", "Competition"} <= labels
    assert any("daily marts are empty" in i.value for i in at.info)
    assert at.sidebar.selectbox[0].value == 0                                    # newest week by default
