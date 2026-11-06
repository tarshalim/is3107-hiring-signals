"""Offline tests for the snapshot checks run before loading."""
from quality.snapshot_checks import snapshot_problem


def check(jobs_fetched, api_total, trial_run=False):
    stats = {"jobs_fetched": jobs_fetched, "api_total": api_total}
    return snapshot_problem(stats, trial_run, min_jobs=100, min_coverage=0.9)


def test_full_snapshot_passes():
    assert check(95_000, 96_000) is None


def test_too_few_jobs_fails():
    assert "Only 50" in check(50, None)


def test_low_coverage_fails():
    assert "50%" in check(48_000, 96_000)


def test_trial_run_skips_checks():
    assert check(200, 96_000, trial_run=True) is None


# ---------------- schema drift ----------------
import ast  # noqa: E402
from pathlib import Path  # noqa: E402

from quality.snapshot_checks import (DETAILS_MAX, compare_fields, drift_result, field_stats,  # noqa: E402
                                     posting_paths)

INGEST_DAG = Path(__file__).resolve().parent.parent / "dags" / "daily_ingest_dag.py"


def test_paths_cover_nested_objects_lists_and_nulls(payload):
    paths = posting_paths(payload)
    assert paths["metadata.totalNumberOfView"] == {"number"}
    assert paths["skills"] == {"array"} and paths["skills[]"] == {"object"}
    assert paths["skills[].skill"] == {"string"}
    assert paths["skills[].confidence"] == {"null"}            # present but null still counts
    assert paths["hiringCompany"] == {"null"} and paths["metadata.isHideSalary"] == {"boolean"}
    assert "salary.type.salaryType" in paths
    assert posting_paths({"tags": ["a", 1, None]})["tags[]"] == {"string", "number", "null"}


def test_field_stats_count_each_posting_once(make_payload):
    postings = [make_payload(), make_payload(hiringCompany={"name": "X"}), make_payload()]
    stats, n = field_stats(postings)
    assert n == 3 and stats["skills[].skill"]["postings"] == 3     # 3 skills per posting, counted once
    assert stats["hiringCompany.name"]["postings"] == 1
    assert stats["hiringCompany"]["types"] == {"null", "object"}
    assert field_stats(postings, limit=2)[1] == 2


def test_compare_finds_new_missing_and_retyped_paths(make_payload):
    before, _ = field_stats([make_payload()])
    changed = make_payload(**{"salary.minimum": "3500", "metadata.newCounter": 5})
    del changed["ssecEqa"]
    after, _ = field_stats([changed])
    diff = compare_fields(before, after)
    assert diff["added"] == ["metadata.newCounter"] and diff["removed"] == ["ssecEqa"]
    assert diff["retyped"] == [("salary.minimum", ["number"], ["string"])]


def test_a_field_null_all_day_is_not_a_type_change(make_payload):
    before, _ = field_stats([make_payload()])
    after, _ = field_stats([make_payload(**{"salary.minimum": None})])
    assert compare_fields(before, after)["retyped"] == []


def test_drift_verdicts(make_payload):
    today, _ = field_stats([make_payload()])
    first = drift_result(None, today)
    assert first.status == "warn" and first.observed is None and "baseline" in first.details
    same = drift_result(today, today, "2026-11-05")
    assert same.status == "pass" and same.observed == 0 and same.severity == "warn" and same.category == "schema"
    new, _ = field_stats([make_payload(**{"metadata.newCounter": 5})])
    changed = drift_result(today, new, "2026-11-05")
    assert changed.status == "warn" and changed.observed == 1
    assert changed.details.startswith("compared with 2026-11-05") and "1 new: metadata.newCounter (1 postings)" in changed.details


def test_drift_details_are_bounded(make_payload):
    today, _ = field_stats([make_payload()])
    wide, _ = field_stats([make_payload(**{f"extra.field_{i:03d}_with_a_long_name": i for i in range(200)})])
    result = drift_result(today, wide, "2026-11-05")
    assert result.observed == 201 and len(result.details) <= DETAILS_MAX and "and 193 more" in result.details


def test_ingest_dag_runs_the_drift_check_beside_load_raw():
    src = INGEST_DAG.read_text(encoding="utf-8")
    tasks = [n.name for n in ast.walk(ast.parse(src)) if isinstance(n, ast.FunctionDef)
             and any(ast.unparse(d).startswith("task") for d in n.decorator_list)]
    assert tasks == ["fetch_snapshot", "check_snapshot", "load_raw", "check_schema_drift"]
    assert "checked = check_snapshot(fetch_snapshot())" in src
    assert "load_raw(checked)" in src and "check_schema_drift(checked)" in src     # siblings, not a chain
    assert "drift_tables_ready(conn)" in src and "AirflowSkipException" in src     # old warehouse: skipped
