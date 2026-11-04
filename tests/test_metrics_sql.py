"""Offline checks on the signal SQL and its orchestration (metrics/build_week.py, sql/init/05_metrics_signals.sql).

The SQL itself runs against Postgres in tests/test_integration_metrics.py. These tests catch the mistakes
that do not need a database: a builder writing to the wrong table, a week-unscoped statement, a stray '%'
that psycopg2 would misread, a table without the week key, the delete-then-insert order, and a week whose
snapshot is not picked the way the MOM comparison picks it (transform/snapshot_week.py).
"""
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from metrics import build_week
from metrics.build_week import (BUILDERS, METRIC_CHECKS, SIGNAL_TABLES, THRESHOLDS, build_table, resolve_week,
                                sql_params)
from transform import snapshot_week as sw
from transform.parse import EXP_MAX

REPO = Path(__file__).resolve().parent.parent
SQL_DIR = REPO / "sql" / "init"
SCHEMA = (SQL_DIR / "05_metrics_signals.sql").read_text(encoding="utf-8")

INSERT_SQL = {
    "posting_week": build_week.POSTING_WEEK_SQL,
    "barrier_skill": build_week.BARRIER_SQL,
    "shortage_skill": build_week.SHORTAGE_SQL,
    "unrealistic_requirements": build_week.UNREALISTIC_SQL,
    "competitiveness": build_week.COMPETITIVENESS_SQL,
    "skill_demand": build_week.SKILL_DEMAND_SQL,
    "skill_salary": build_week.SKILL_SALARY_SQL,
}

# The week ending Tue 3 Nov (Wed 28 Oct to Tue 3 Nov), from Tuesday's snapshot
WEEK = {
    "week_end_date": "2026-11-03", "week_start_date": "2026-10-28", "snapshot_date": "2026-11-03",
    "snapshot_is_week_end": True, "fallback_reason": None, "snapshots_in_week": [],
    "pool_from_date": "2026-11-01", "pool_to_date": "2026-11-03", "first_snapshot_date": "2026-10-24",
    "snapshot_dq_failures": 0, "thresholds": THRESHOLDS,
}
# Snapshot days in clean.fact_job_day, as on the warehouse on 3 Nov 2026 (24 Oct to 2 Nov)
LOADED = [date(2026, 10, 24) + timedelta(days=i) for i in range(10)]


def _table_blocks(sql):
    """{table: body} for every CREATE TABLE in the schema file."""
    return {m.group(1): m.group(2) for m in
            re.finditer(r"CREATE TABLE IF NOT EXISTS metrics\.(\w+) \((.*?)\n\);", sql, re.S)}


def test_each_builder_writes_only_its_own_table_and_only_for_the_week():
    for name, sql in INSERT_SQL.items():
        assert re.findall(r"INSERT INTO metrics\.(\w+)", sql) == [name], name
        assert "%(e)s" in sql, f"{name} is not scoped to the week"
        assert "DELETE" not in sql.upper().split("INSERT")[0], name     # deletes happen in build_table only
    assert list(BUILDERS) == ["posting_week", *SIGNAL_TABLES]          # posting_week first, the rest read it


def test_no_stray_percent_signs_for_psycopg2():
    """psycopg2 treats every % as a placeholder: only %(name)s may appear in parameterised SQL."""
    for name, sql in {**INSERT_SQL, **{c.name: c.sql for c in METRIC_CHECKS}}.items():
        assert re.sub(r"%\(\w+\)s", "", sql).count("%") == 0, name


def test_every_parameter_the_sql_uses_is_provided():
    params = sql_params(WEEK)
    params.update({"rule_reposted": "", "rule_entry": "", "rule_junior": "", "many": 1})
    for name, sql in INSERT_SQL.items():
        missing = set(re.findall(r"%\((\w+)\)s", sql)) - set(params)
        assert not missing, f"{name} uses {missing}"


def test_every_table_is_keyed_by_week_and_records_the_snapshot_used():
    blocks = _table_blocks(SCHEMA)
    assert set(blocks) == {"run_log", "posting_week", *SIGNAL_TABLES}
    for table, body in blocks.items():
        assert re.search(r"^\s+snapshot_date\s+DATE\s+NOT NULL", body, re.M), table
        if table == "run_log":
            assert re.search(r"week_end_date\s+DATE PRIMARY KEY", body)
        else:
            assert re.search(r"PRIMARY KEY \(week_end_date[,)]", body), table
    assert "REFERENCES" not in SCHEMA            # no foreign keys into clean: metrics survive a clean rebuild
    assert SCHEMA.count("IF NOT EXISTS") >= len(blocks) + 1                # idempotent file (schema + tables)


def test_signal_schema_runs_after_the_mom_and_ext_files_and_leaves_their_tables_alone():
    """The init scripts run in name order: 05 must come after 03_mom_vacancies.sql (which creates the metrics
    schema and the MOM validation table) and never redefine a table of another file."""
    names = sorted(p.name for p in SQL_DIR.glob("*.sql"))
    assert "03_mom_vacancies.sql" in names
    assert all(n < "05_metrics_signals.sql" for n in names if n.startswith(("01", "02", "03", "04")))
    assert "mcf_vs_mom_share" not in _table_blocks(SCHEMA) and "mom_validation" not in SCHEMA
    others = "".join((SQL_DIR / n).read_text(encoding="utf-8") for n in names if n != "05_metrics_signals.sql")
    for table in _table_blocks(SCHEMA):
        assert not re.search(rf"CREATE (TABLE|VIEW|OR REPLACE VIEW)[^;(]*metrics\.{table}\b", others), table


def test_thresholds_are_sane():
    t = THRESHOLDS
    assert 0 < t["barrier_max_rate_ratio"] < 1
    assert 0 < t["many_apps_percentile"] < 1
    assert t["shortage_min_reposts"] >= 1 and t["shortage_max_applications"] >= 0
    assert t["entry_min_years"] == 2 and t["exp_max_valid"] == EXP_MAX == 30   # one cap for clean and metrics
    assert (t["unrealistic_min_reposts"], t["shortage_min_reposts"]) == (1, 2)
    assert "Fresh/entry level" in t["entry_levels"] and "Junior Executive" in t["junior_levels"]
    assert not set(t["entry_levels"]) & set(t["junior_levels"])           # one posting, one flag
    assert (t["salary_floor"], t["salary_ceiling"]) == (300, 100000)      # monthly; hourly rates and annual pay left out
    assert t["min_companies"] == 10                                       # barrier and shortage skills (review #14)
    for k in ("skill_min_postings", "group_min_postings", "salary_min_postings", "demand_min_postings",
              "shortage_min_flagged", "demand_weeks", "max_gap_snapshots", "min_companies"):
        assert t[k] >= 1, k


def test_no_signal_threshold_is_hard_coded_in_the_sql():
    """Review 4 Nov #13: every number a flag uses comes from THRESHOLDS (and so lands in run_log.thresholds)."""
    src = (REPO / "metrics" / "build_week.py").read_text(encoding="utf-8")
    assert not re.search(r"repost_count\s*>=\s*\d", src)
    assert not re.search(r"applications_total\s*<=\s*\d", src)


def test_entry_level_and_junior_are_separate_posting_flags():
    """Review 4 Nov #9: one column mixed them (907 entry + 4,303 junior on 3 Nov)."""
    cols = _table_blocks(SCHEMA)["posting_week"]
    assert "is_entry_2plus" in cols and "is_junior_2plus" in cols and "is_entry_level_2plus" not in cols
    assert "is_entry_level_2plus" not in build_week.POSTING_WEEK_SQL + build_week.UNREALISTIC_SQL
    # an existing warehouse gets the same columns from the upgrade block at the end of 05
    assert "ADD COLUMN IF NOT EXISTS is_junior_2plus" in SCHEMA
    assert "DROP COLUMN IF EXISTS is_entry_level_2plus" in SCHEMA


def test_shortage_medians_describe_the_shortage_postings():
    """Review 4 Nov #8: over all postings with the skill, every top skill showed 0.0."""
    sql = " ".join(build_week.SHORTAGE_SQL.split())
    assert "(ORDER BY pw.repost_count) FILTER (WHERE pw.is_shortage)" in sql
    assert "(ORDER BY pw.applications_total) FILTER (WHERE pw.is_shortage)" in sql


def test_ext_views_use_the_same_usable_salary_rule_as_the_metrics():
    ext = (SQL_DIR / "04_ext_schema.sql").read_text(encoding="utf-8")
    floor, ceiling = THRESHOLDS["salary_floor"], THRESHOLDS["salary_ceiling"]
    rule = f"(j.salary_min >= {floor} AND j.salary_max >= j.salary_min AND j.salary_max <= {ceiling}) IS TRUE AS salary_usable"
    assert rule in ext
    assert ext.count(f"salary_monthly_min >= {floor}") == 2 and ext.count(f"salary_monthly_max <= {ceiling}") == 2
    # every view reading current salaries filters on the rule
    assert len(re.findall(r"NOT n?\.?salary_is_hidden(?! AND n?\.?salary_usable)", ext)) == 0


def test_sql_params_dates():
    p = sql_params(WEEK)
    assert p["e"] == date(2026, 11, 3) and p["s"] == date(2026, 11, 3) and p["ws"] == date(2026, 10, 28)
    assert p["demand_from"] == date(2026, 9, 28)          # Monday of 3 Nov's week, 5 weeks back: 6 posting weeks
    assert p["demand_from"].weekday() == 0


class FakeCursor:
    """Answers the snapshot_week query and the loaded-days query from a list of dates, the DQ query with 0,
    every other fetch with 7. Records every statement."""

    def __init__(self, dates=LOADED):
        self.dates, self.executed, self.rowcount, self._one, self._all = dates, [], 3, None, []

    def execute(self, sql, params=None):
        flat = " ".join(sql.split())
        self.executed.append(flat)
        if "max(snapshot_date)" in flat:
            after, up_to = params
            in_week = [d for d in self.dates if after < d <= up_to]
            self._one = (max(in_week) if in_week else None,)
        elif "FROM clean.dim_date" in flat:
            self._all = [(d,) for d in sorted(self.dates)]
        elif "clean.dq_results" in flat:
            self._one = (0,)
        else:
            self._one = (7,)

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, dates=LOADED):
        self.cur = FakeCursor(dates)
        self.transactions = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cur

    def rollback(self):
        self.rollbacks += 1

    def __enter__(self):
        self.transactions += 1
        return self

    def __exit__(self, *a):
        return False


def _run_week(end_utc, dates=LOADED):
    """resolve_week for the run whose data interval ends at end_utc, as pick_snapshot calls it."""
    after, up_to = sw.week_bounds(end_utc - timedelta(days=7), end_utc)
    conn = FakeConn(dates)
    return resolve_week(conn, after, up_to), conn


def test_resolve_week_uses_the_latest_snapshot_of_the_run_week_like_the_mom_comparison():
    first, _ = _run_week(datetime(2026, 10, 27, 6, tzinfo=timezone.utc))     # backfilled run, week ending Tue 27 Oct
    second, conn = _run_week(datetime(2026, 11, 3, 6, tzinfo=timezone.utc))  # Tuesday 3 Nov not loaded yet
    assert (first["week_end_date"], first["snapshot_date"]) == ("2026-10-27", "2026-10-27")
    assert (second["week_end_date"], second["snapshot_date"]) == ("2026-11-03", "2026-11-02")
    assert second["snapshot_is_week_end"] is False and second["week_start_date"] == "2026-10-28"
    assert conn.cur.executed[0] == ("SELECT max(snapshot_date) FROM clean.fact_job_day "
                                    "WHERE snapshot_date > %s AND snapshot_date <= %s")
    assert conn.rollbacks == 1 and conn.transactions == 0                    # read-only, nothing written


def test_a_week_without_snapshot_fails_before_anything_is_written():
    with pytest.raises(sw.NoSnapshotInWeek, match="2026-10-14 to 2026-10-20"):
        _run_week(datetime(2026, 10, 20, 6, tzinfo=timezone.utc))            # before our first snapshot (24 Oct)
    conn = FakeConn()
    with pytest.raises(sw.NoSnapshotInWeek):
        resolve_week(conn, date(2026, 10, 13), date(2026, 10, 20))
    assert len(conn.cur.executed) == 1 and conn.rollbacks == 1


def test_build_table_deletes_the_week_then_inserts_in_one_transaction():
    conn = FakeConn()
    stats = build_table(conn, "barrier_skill", WEEK)
    first, second, third = conn.cur.executed
    assert first.startswith("DELETE FROM metrics.barrier_skill WHERE week_end_date")
    assert second.startswith("INSERT INTO metrics.barrier_skill")
    assert third.startswith("SELECT count(*) FROM metrics.barrier_skill")
    assert conn.transactions == 1
    assert stats["table"] == "barrier_skill" and stats["rows"] == 7 and stats["deleted_before_insert"] == 3


def test_unknown_table_is_refused():
    with pytest.raises(ValueError):
        build_table(FakeConn(), "clean.fact_job_day; DROP", WEEK)
    with pytest.raises(ValueError):
        build_table(FakeConn(), "mom_validation", WEEK)       # dropped: metrics.mcf_vs_mom_share is the MOM validation


def test_metric_checks_are_well_formed():
    names = [c.name for c in METRIC_CHECKS]
    assert len(names) == len(set(names))
    for c in METRIC_CHECKS:
        assert c.severity in {"fail", "warn", "info"} and c.op in {"<=", ">=", "=="}
        assert c.sql.lstrip().upper().startswith("SELECT") and "%(e)s" in c.sql
        assert "mom_validation" not in c.sql
    assert any(c.severity == "fail" for c in METRIC_CHECKS)
