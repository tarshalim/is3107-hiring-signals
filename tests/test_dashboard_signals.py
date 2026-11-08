"""Weekly signal tabs: pure-pandas logic (dashboard/signals.py) and offline checks on their SQL
(dashboard/signal_queries.py, sql/init/10_dashboard_signals.sql). No database needed; the SQL itself runs
against Postgres in tests/test_integration_dashboard_signals.py."""
import re
from datetime import date
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")

from dashboard import signals as logic  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
SQL_DIR = REPO / "sql" / "init"
VIEWS_SQL = (SQL_DIR / "10_dashboard_signals.sql").read_text(encoding="utf-8")
QUERIES_SRC = (REPO / "dashboard" / "signal_queries.py").read_text(encoding="utf-8")
NAN = float("nan")


def _skills():
    """Four skills as metrics.skill_signals returns them for one week (week before exists)."""
    return pd.DataFrame([
        # ref, name, mapped, postings, listed, lift, prev listed, shortage n, firms, is_barrier, barrier lift, prev barrier
        ("qs", "Quantity Surveying", False, 1333, True, 2.5, True, 86, 51, False, 1.3, False),
        ("rev", "Autodesk Revit", False, 369, True, 3.1, False, 29, 14, False, NAN, None),
        ("pm", "Project Management", True, 4653, True, 0.5, True, 198, 48, False, 0.9, False),
        ("gl", "General Ledger", False, 115, False, NAN, False, NAN, NAN, True, 3.6, False),
        ("qb", "QuickBooks", False, 147, False, NAN, False, NAN, NAN, True, 3.2, True),
    ], columns=["skill_ref", "skill_name", "is_mapped", "open_postings", "shortage_listed", "shortage_lift",
                "shortage_listed_prev_week", "shortage_postings", "shortage_companies", "is_barrier", "barrier_lift",
                "is_barrier_prev_week"]).assign(barrier_postings=[148, 0, 10, 31, 36])


def _row(**values):
    base = {"skill_name": "Quantity Surveying", "shortage_listed": True, "shortage_lift": 2.535,
            "shortage_postings": 86, "shortage_companies": 51, "shortage_listed_prev_week": True,
            "is_barrier": False, "barrier_lift": 1.318, "barrier_postings": 148, "barrier_companies": 122,
            "is_barrier_prev_week": False, "apps_per_posting_day_24h": 0.1145,
            "overall_apps_per_posting_day_24h": 0.3451, "apps_per_posting_day_24h_prev_week": 0.0603,
            "salary_mid_median": 4500.0, "salary_mid_p25": 3900.0, "salary_mid_p75": 5750.0,
            "salary_vs_overall": 1.111, "demand_week": date(2026, 10, 26), "demand_share": 0.01187,
            "demand_new_postings": 274, "demand_share_week_before": None}
    return pd.Series({**base, **values})


T = logic.DEFAULT_THRESHOLDS


# --- formatting and weeks -------------------------------------------------------------------------

def test_number_formats_and_missing_values():
    assert logic.times(2.535) == "2.5x" and logic.times(None) == "n/a" and logic.times(NAN) == "n/a"
    assert logic.pct(0.0626) == "6.3%" and logic.pct(0.4657, 0) == "47%"
    assert logic.money(3445) == "$3,445"
    assert logic.points(0.0062) == "+0.6 pts" and logic.points(-0.002) == "-0.2 pts" and logic.points(NAN) is None
    assert logic.pay_gap(3800, 3150) == pytest.approx(0.2063, abs=1e-4)
    assert logic.pay_gap(3800, None) is None and logic.pay_gap(3800, 0) is None


def test_long_labels_are_cut_or_wrapped():
    assert logic.short_label("Beautician") == "Beautician"
    cut = logic.short_label("Other administrative and related associate professionals n.e.c.", 30)
    assert len(cut) == 30 and cut.endswith("...")
    title = "Production & transport operators, cleaners & labourers"
    lines = logic.wrap_label(title, 28).split("<br>")
    assert len(lines) > 1 and all(len(line) <= 28 for line in lines) and " ".join(lines) == title


def test_week_label_and_caption_name_the_snapshot():
    week = pd.Series({"week_end_date": date(2026, 11, 3), "week_start_date": date(2026, 10, 28),
                      "snapshot_date": date(2026, 11, 3), "snapshot_is_week_end": True, "pool_postings": 88610})
    assert logic.week_label(week) == "Week ending Tue 3 Nov 2026"
    caption = logic.week_caption(week)
    assert "Wed 28 Oct to Tue 3 Nov" in caption and "3 Nov snapshot" in caption and "88,610" in caption
    assert "missing" not in caption
    fallback = week.copy()
    fallback["snapshot_is_week_end"] = False
    assert "missing" in logic.week_caption(fallback)


def test_a_partial_week_is_marked_wherever_it_is_shown():
    """8 Nov check: the 27 Oct week starts on Sat 24 Oct (4 snapshots), and nothing said so."""
    first = pd.Series({"week_end_date": date(2026, 10, 27), "week_start_date": date(2026, 10, 21),
                       "snapshot_date": date(2026, 10, 27), "snapshot_is_week_end": True, "pool_postings": 90655,
                       "snapshots": 4, "first_snapshot_date": date(2026, 10, 24), "prev_week_end_date": None,
                       "prev_snapshots": None, "prev_first_snapshot_date": None})
    assert logic.partial_week(first) == "data from Sat 24 Oct, 4 snapshots"
    assert logic.week_label(first) == "Week ending Tue 27 Oct 2026 (partial)"
    assert "**Partial week** (data from Sat 24 Oct, 4 snapshots)" in logic.week_caption(first)
    assert logic.last_week_name(first) == "Last week"
    second = pd.Series({"week_end_date": date(2026, 11, 3), "week_start_date": date(2026, 10, 28),
                        "snapshot_date": date(2026, 11, 3), "snapshot_is_week_end": True, "pool_postings": 88667,
                        "snapshots": 7, "first_snapshot_date": date(2026, 10, 28),
                        "prev_week_end_date": date(2026, 10, 27), "prev_snapshots": 4,
                        "prev_first_snapshot_date": date(2026, 10, 24)})
    assert logic.partial_week(second) is None and logic.week_label(second) == "Week ending Tue 3 Nov 2026"
    caption = logic.week_caption(second)
    assert "Last week (ending 27 Oct) was a **partial week** (data from Sat 24 Oct, 4 snapshots)" in caption
    assert logic.last_week_name(second) == "Last week (partial: data from Sat 24 Oct, 4 snapshots)"
    # a run from before 8 Nov (no snapshot counts) is not called partial
    assert logic.partial_week(pd.Series({"snapshots": None, "first_snapshot_date": None})) is None


def test_thresholds_come_from_the_run_with_defaults_for_older_runs():
    week = pd.Series({"thresholds": {"barrier_min_lift": 2.0}, "prev_week_end_date": None})
    t = logic.thresholds(week)
    assert t["barrier_min_lift"] == 2.0 and t["min_companies"] == 10
    assert logic.thresholds(pd.Series({"thresholds": None})) == logic.DEFAULT_THRESHOLDS
    assert not logic.has_previous(week)
    assert logic.has_previous(pd.Series({"prev_week_end_date": date(2026, 10, 27)}))


# --- shortage and barrier lists -------------------------------------------------------------------

def test_stability_label_needs_an_earlier_week():
    assert logic.stability(True) == logic.STABLE
    assert logic.stability(False) == logic.NEW
    assert logic.stability(None) == logic.FIRST and logic.stability(NAN) == logic.FIRST


def test_shortage_list_is_every_listed_skill_by_lift_with_stability():
    rows = logic.shortage_skills(_skills())
    assert rows["skill_ref"].tolist() == ["rev", "qs", "pm"]        # listed only, highest lift first
    assert rows["stability"].tolist() == [logic.NEW, logic.STABLE, logic.STABLE]
    assert logic.carried_over(rows) == (2, 3)


def test_barrier_list_is_only_skills_the_run_flagged():
    rows = logic.barrier_skills(_skills())
    assert rows["skill_ref"].tolist() == ["gl", "qb"]
    assert logic.carried_over(rows) == (1, 2)


def test_technical_only_hides_soft_skills_unless_switched_off():
    rows = logic.shortage_skills(_skills().assign(is_technical=[True, True, False, True, True]))
    assert logic.technical_only(rows)["skill_ref"].tolist() == ["rev", "qs"]
    assert logic.technical_only(rows, on=False)["skill_ref"].tolist() == ["rev", "qs", "pm"]
    old_week = logic.shortage_skills(_skills())                         # built before 8 Nov: no is_technical
    assert len(logic.technical_only(old_week)) == 3
    assert len(logic.technical_only(old_week.assign(is_technical=None))) == 3
    assert "is_technical" in VIEWS_SQL.split("CREATE OR REPLACE VIEW metrics.skill_signals")[1]


def test_shortage_chart_shows_the_direct_employer_base_of_the_lift():
    """8 Nov check: the chart showed all open postings (Spa 494) while the lift rests on direct employers' (329)."""
    view = VIEWS_SQL.split("CREATE OR REPLACE VIEW metrics.skill_signals")[1]
    assert "sh.postings          AS shortage_base_postings" in view
    tab = (REPO / "dashboard" / "tabs" / "signals.py").read_text(encoding="utf-8")
    shortage = tab.split("def _shortage(")[1].split("def _barrier(")[0]
    assert "shortage_base_postings" in shortage and '"open_postings"' not in shortage
    assert "the average posting" not in shortage and "average direct employer's posting" in shortage


def test_carried_over_is_unknown_in_the_first_week():
    first = _skills().assign(shortage_listed_prev_week=None)
    assert logic.carried_over(logic.shortage_skills(first)) is None


def test_skill_options_rank_by_postings_and_tell_twin_names_apart():
    skills = pd.DataFrame({"skill_ref": ["a", "b", "c"], "skill_name": ["Teamwork", "Python", "Teamwork"],
                           "is_mapped": [True, False, False], "open_postings": [50, 900, 10]})
    options = logic.skill_options(skills)
    assert list(options) == ["b", "a", "c"]
    assert options["a"] == "Teamwork (SkillsFuture skill)" and options["c"] == "Teamwork (MCF tag)"
    assert options["b"] == "Python"


def test_lookup_opens_on_quantity_surveying_when_the_week_has_it():
    """8 Nov check: it opened on Collaboration (1.4x), the commission-sales artefact."""
    assert logic.DEFAULT_LOOKUP_SKILL == "Quantity Surveying"
    assert logic.default_skill(_skills()) == "qs"
    assert logic.default_skill(_skills().assign(shortage_listed=False)) == "qs"   # listed or not
    without = _skills()[_skills()["skill_ref"] != "qs"]
    assert logic.default_skill(without) == "pm"                          # most common skill on the shortage list
    assert logic.default_skill(without, shown=1) == "rev"                # only Revit shown
    assert logic.default_skill(without.assign(shortage_listed=False)) == "pm"   # the most common skill
    assert logic.default_skill(_skills().iloc[0:0]) is None


# --- competitiveness and market check -------------------------------------------------------------

def test_spread_ignores_groups_below_the_minimum():
    groups = pd.DataFrame({"group_label": ["Clerical", "Managers", "Craftsmen", "Agricultural"],
                           "postings": [8124, 14296, 3162, 22],
                           "apps_per_posting_day_24h": [0.6303, 0.5536, 0.0785, 0.01]})
    s = logic.spread(groups, 30)
    assert s["top"]["group_label"] == "Clerical" and s["bottom"]["group_label"] == "Craftsmen"
    assert s["ratio"] == pytest.approx(0.6303 / 0.0785)
    assert logic.spread(groups, 10000) is None


def _market():
    return pd.DataFrame([
        ("A", "PMET", "Professionals, managers, executives & technicians", 73.41, 56.22, 17.19),
        ("A", "clerical_sales_services", "Clerical, sales & services workers", 17.50, 18.31, -0.81),
        ("A", "production_transport", "Production & transport operators, cleaners & labourers", 9.09, 25.46, -16.37),
        ("A", "unknown", "Missing or invalid ssocCode", None, None, None),
        ("B", "PME", "Professionals, managers & executives", 49.42, 32.62, 16.80),
        ("B", "non_PME", "Non-PME", 50.58, 67.38, -16.80),
    ], columns=["split", "mom_group", "group_title", "mcf_share_pct", "mom_share_pct", "share_gap_pp"])


def test_market_rows_leave_out_the_unknown_bucket():
    assert logic.market_rows(_market(), "A")["mom_group"].tolist() == ["PMET", "clerical_sales_services",
                                                                       "production_transport"]


def test_market_gap_names_the_over_and_under_represented_groups():
    gap = logic.market_gap(_market(), "A")
    assert gap["line"].startswith("PMET jobs are 73% of MyCareersFuture postings but 56% of MOM vacancies (+17 points)")
    assert "production, transport and labouring jobs are 9% against 25%" in gap["line"]
    assert gap["meaning"].startswith("Read the weekly signals as mostly about PMET jobs")
    assert logic.market_gap(_market(), "B")["over"] == "PME jobs"
    assert logic.market_gap(_market().iloc[0:0], "A") is None


# --- skill lookup verdicts ------------------------------------------------------------------------

def test_shortage_verdict():
    v = logic.shortage_verdict(_row(), T)
    assert v["value"] == "2.5x" and v["flagged"] and "86 postings, 51 employers" in v["caption"]
    assert "Flagged last week too" in v["caption"]
    low = logic.shortage_verdict(_row(shortage_lift=0.49), T)
    assert not low["flagged"] and "Below 1.5x" in low["caption"]
    # Collaboration on 3 Nov, listed before the 1.5x minimum: not called a shortage signal
    collab = _row(skill_name="Collaboration", shortage_lift=1.432)
    assert not logic.shortage_verdict(collab, T)["flagged"]
    assert "no shortage or barrier signal" in logic.lookup_summary(collab, T)
    assert logic.shortage_verdict(_row(shortage_lift=1.5), T)["flagged"]
    none = logic.shortage_verdict(_row(shortage_listed=False), T)
    assert none["value"] == "No" and "needs 1.5x" in none["caption"] and "25+ such postings" in none["caption"]
    first = logic.shortage_verdict(_row(shortage_listed_prev_week=None), T)
    assert "last week" not in first["caption"]


def test_barrier_verdict():
    not_flagged = logic.barrier_verdict(_row(), T)
    assert not_flagged["value"] == "No" and "1.3x" in not_flagged["caption"] and "1.5x" in not_flagged["caption"]
    flagged = logic.barrier_verdict(_row(is_barrier=True, barrier_lift=3.554, barrier_postings=31,
                                         barrier_companies=25), T)
    assert flagged["value"] == "3.6x" and flagged["flagged"] and "31 postings, 25 employers" in flagged["caption"]
    assert "Not enough postings" in logic.barrier_verdict(_row(barrier_lift=None), T)["caption"]
    # General Ledger on 3 Nov: lift 3.6 but 1.43 times the market's applications per view, so not a barrier
    gl = logic.barrier_verdict(_row(barrier_lift=3.554, barrier_rate_vs_overall=1.43), T)
    assert not gl["flagged"] and "1.4x the average posting's applications per view" in gl["caption"]
    assert "applications per view" not in not_flagged["caption"]       # lift too low: the lift is the reason


def test_experience_asks_lead_with_entry_level_and_read_through_pay():
    assert logic.EXPERIENCE_FLAGS == ("entry_level_2plus_years", "junior_2plus_years")
    assert "reposted_many_apps" not in logic.EXPERIENCE_FLAGS          # shown apart, as a minor signal
    # 3 Nov: entry level $3,445 against $3,400 (+1%), Junior Executive $3,800 against $3,150 (+21%)
    assert "clearest unrealistic ask" in logic.experience_ask_reading(logic.pay_gap(3445, 3400))
    assert "labelled junior" in logic.experience_ask_reading(logic.pay_gap(3800, 3150))
    assert logic.experience_ask_reading(0.07) == "" and logic.experience_ask_reading(None) == ""


def test_harder_score_points_the_same_way_for_every_column():
    """log2 times the market, turned so that above 0 always means harder to fill, capped at +-2."""
    assert logic.harder_score(0.036, 0.018, True) == pytest.approx(1.0)          # twice the shortage share
    assert logic.harder_score(0.17, 0.34, False) == pytest.approx(1.0)           # half the applicants: harder
    assert logic.harder_score(0.80, 0.20, False) == pytest.approx(-2.0)          # 4x the applicants
    assert logic.harder_score(1.0, 0.01, True) == logic.GRID_CAP                 # capped
    assert logic.harder_score(0, 0.018, True) == -logic.GRID_CAP                 # no flagged postings at all
    assert logic.harder_score(0, 0.34, False) == logic.GRID_CAP                  # no applications at all
    assert all(map(lambda v: v != v, (logic.harder_score(None, 0.1, True), logic.harder_score(0.1, NAN, True))))


def test_summary_grid_rows_fewest_applicants_first():
    overall = pd.Series({"apps_per_posting_day_24h": 0.3451, "shortage_share": 0.018, "barrier_share": 0.0834,
                         "entry_2plus_share": 0.0626})
    groups = pd.DataFrame({
        "group_label": ["Accounting", "Personal Care / Beauty", "Building and Construction"],
        "apps_per_posting_day_24h": [0.8135, 0.042, 0.148], "shortage_share": [0.0005, 0.0749, 0.024],
        "barrier_share": [0.1235, 0.0718, 0.0955], "entry_2plus_share": [0.097, 0.119, 0.2935],
        "postings": [3845, 849, 9596], "direct_postings": [2086, 374, 5961], "entry_postings": [268, 42, 293]})
    grid = logic.summary_grid(groups, overall)
    assert grid["rows"]["group_label"].tolist() == ["Personal Care / Beauty", "Building and Construction", "Accounting"]
    assert grid["text"][0] == ["0.04/day", "7.5%", "7.2%", "11.9%"]
    assert grid["bases"][0] == [849, 374, 849, 42]
    # 42 entry-level postings: below GRID_MIN_BASE, so greyed out (no colour), the rest of the row coloured
    assert logic.GRID_MIN_BASE == 100 and grid["small"][0] == [False, False, False, True]
    beauty = grid["z"][0]
    assert beauty[0] == logic.GRID_CAP and beauty[1] == pytest.approx(2.0, abs=0.06) and beauty[3] != beauty[3]
    assert grid["z"][2][0] < 0                                       # accounting: more applicants than the market
    assert grid["headers"][1] == "Shortage<br>(all: 1.8%)"


def test_competition_verdict_compares_with_the_average_posting():
    fewer = logic.competition_verdict(_row())
    assert fewer["value"] == "0.11" and "3.0 times fewer" in fewer["caption"] and fewer["delta"] == "+0.05 vs last week"
    more = logic.competition_verdict(_row(apps_per_posting_day_24h=1.566, apps_per_posting_day_24h_prev_week=None))
    assert "4.5 times the average" in more["caption"] and more["delta"] is None
    assert logic.competition_verdict(_row(apps_per_posting_day_24h=None))["value"] == "n/a"


def test_demand_and_pay_verdicts():
    d = logic.demand_verdict(_row())
    assert d["value"] == "1.2%" and "26 Oct" in d["caption"] and "274 postings" in d["caption"] and d["delta"] is None
    assert logic.demand_verdict(_row(demand_share_week_before=0.0100))["delta"] == "+0.2 pts"
    assert logic.demand_verdict(_row(demand_week=None))["value"] == "n/a"
    p = logic.pay_verdict(_row(), T)
    assert p["value"] == "$4,500" and "$3,900 to $5,750" in p["caption"] and "1.1x" in p["caption"]
    none = logic.pay_verdict(_row(salary_mid_median=None), T)
    assert none["value"] == "n/a" and "more than 50% of them" in none["caption"]


def test_lookup_summary_names_the_causes():
    assert logic.lookup_summary(_row(), T).startswith("Quantity Surveying shows a shortage signal (2.5x).")
    assert "fewer applicants than average" in logic.lookup_summary(_row(), T)
    both = logic.lookup_summary(_row(is_barrier=True, barrier_lift=2.0), T)
    assert "a shortage signal (2.5x) and a barrier signal (2.0x)" in both
    neither = logic.lookup_summary(_row(shortage_listed=False), T)
    assert "no shortage or barrier signal" in neither


# --- the SQL, offline -----------------------------------------------------------------------------

def test_views_file_runs_after_the_tables_it_reads_and_creates_only_views():
    names = sorted(p.name for p in SQL_DIR.glob("*.sql"))
    assert names.index("10_dashboard_signals.sql") > names.index("05_metrics_signals.sql") > \
        names.index("03_mom_vacancies.sql")
    sql = re.sub(r"--[^\n]*", "", VIEWS_SQL)
    assert "CREATE TABLE" not in sql and "DROP" not in sql and "DELETE" not in sql   # adds views, changes no data
    created = re.findall(r"CREATE OR REPLACE VIEW (metrics\.\w+)", sql)
    assert created == ["metrics.signal_weeks", "metrics.skill_signals"]


def _tables_read(sql: str) -> set:
    return {m.group(1) for m in re.finditer(r"\b(?:FROM|JOIN)\s+([a-z_]+\.[a-z_]+)", sql)}


def test_weekly_tabs_read_only_the_metrics_schema():
    """Dr Lek: results are read from the store they were loaded into, never recomputed from raw or clean."""
    sql = re.sub(r"--[^\n]*", "", VIEWS_SQL)
    for source in (sql, QUERIES_SRC):
        tables = _tables_read(source)
        assert tables, "no table found"
        assert all(t.startswith("metrics.") for t in tables), tables


def test_weekly_queries_are_parameterised():
    queries = re.findall(r'run_query\("""(.*?)"""', QUERIES_SRC, re.S)
    assert len(queries) == 6
    for sql in queries:
        assert "{" not in sql                                            # no f-string values in the SQL
        assert re.sub(r"%\(\w+\)s", "", sql).count("%") == 0             # no stray % for psycopg2
    assert sum("%(week)s" in sql for sql in queries) == 5                # every query but the week list
    assert QUERIES_SRC.count("@st.cache_data(") == 6                     # and every one cached
