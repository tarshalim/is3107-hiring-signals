"""Offline tests for the scale, cost and restore report's pure parts (ops/scale_report.py)."""
from ops import scale_report as sr

INGEST = [
    {"snapshot_date": "2026-10-24", "jobs_fetched": "91825", "api_total": "92060", "seconds_taken": "1420.1"},
    {"snapshot_date": "2026-10-25", "jobs_fetched": "88916", "api_total": "88962", "seconds_taken": "1393.5"},
    {"snapshot_date": "2026-10-27", "jobs_fetched": "89898", "api_total": "91266", "seconds_taken": "4818.0"},
]


def test_ingest_summary():
    s = sr.ingest_summary(INGEST)
    assert s["days"] == 3 and (s["first"], s["last"]) == ("2026-10-24", "2026-10-27")
    assert s["jobs_median"] == 89898 and s["jobs_total"] == 91825 + 88916 + 89898
    assert round(s["crawl_minutes_max"]) == 80 and round(s["coverage_min"], 4) == 0.985
    assert sr.ingest_summary([]) == {}


def test_load_log_uses_the_last_run_of_each_day():
    rows = [{"run_id": "old", "snapshot_date": "2026-10-24", "step": "parse_raw", "seconds": "99"},
            {"run_id": "new", "snapshot_date": "2026-10-24", "step": "parse_raw", "seconds": "25"},
            {"run_id": "new2", "snapshot_date": "2026-10-25", "step": "parse_raw", "seconds": "23"}]
    assert sr.load_log_summary(rows) == {"parse_raw": {"days": 2, "median": 24.0, "max": 25.0, "left_out": 0}}
    stale = rows + [{"run_id": "new2", "snapshot_date": "2026-10-25", "step": "dag_run", "seconds": "348055.6"},
                    {"run_id": "new", "snapshot_date": "2026-10-24", "step": "dag_run", "seconds": "110.5"}]
    assert sr.load_log_summary(stale)["dag_run"] == {"days": 1, "median": 110.5, "max": 110.5, "left_out": 1}


def test_task_summary_counts_retried_and_failed_runs():
    rows = [{"dag_id": "d", "task_id": "fetch", "state": "success", "try_number": "1", "duration_s": "10"},
            {"dag_id": "d", "task_id": "fetch", "state": "failed", "try_number": "1", "duration_s": "5"},
            {"dag_id": "d", "task_id": "fetch", "state": "success", "try_number": "2", "duration_s": "30"}]
    (t,) = sr.task_summary(rows)
    assert (t["runs"], t["successes"], t["median_s"], t["max_s"], t["retried_runs"], t["failed_runs"]) == \
        (3, 2, 20, 30, 1, 1)


def test_projection_is_linear_per_snapshot_day():
    per_day = sr.per_day_bytes({"raw.mcf_job_snapshots": 2_000, "clean.fact_job_day": 200, "clean.dim_job": 100}, 2)
    assert per_day["Raw JSONB (raw.mcf_job_snapshots)"] == 1_000
    assert per_day["Clean star (fact, dim_job, bridge, dim_skill)"] == 150
    p = sr.project({"x": 10.0, "missing": None})
    assert p == {"x": {30: 300.0, 90: 900.0, 365: 3650.0}}


def test_files_are_priced_as_object_storage_and_tables_as_block_storage():
    cost = sr.monthly_storage_cost({"Raw gz files (data/raw/mcf)": sr.GIB, "Raw JSONB (raw.mcf_job_snapshots)": sr.GIB,
                                    "Parquet lake (data/lake/fact_job_day)": sr.GIB})
    assert cost["Raw gz files (data/raw/mcf)"] == sr.PRICES["s3_standard_gb_month"]
    assert cost["Parquet lake (data/lake/fact_job_day)"] == sr.PRICES["s3_standard_gb_month"]
    assert cost["Raw JSONB (raw.mcf_job_snapshots)"] == sr.PRICES["ebs_gp3_gb_month"]


def test_only_the_generated_block_is_rewritten():
    doc = f"# Title\n\nhand-written\n\n{sr.BEGIN}\nold numbers\n{sr.END}\n\nmore hand-written\n"
    new = sr.replace_generated(doc, f"{sr.BEGIN}\nnew numbers\n{sr.END}")
    assert "old numbers" not in new and "new numbers" in new
    assert new.startswith("# Title\n\nhand-written") and new.endswith("more hand-written\n")
    assert sr.replace_generated("# Doc\n", "BLOCK").endswith("BLOCK\n")


def test_render_has_every_section():
    report = {"generated_at": "2026-11-06 23:00 +0800", "warehouse": "localhost:5433/jobs", "days_loaded": 2,
              "note": "real data", "ingest": sr.ingest_summary(INGEST), "ingest_source": "ingest_runs.csv",
              "load_log": {"parse_raw": {"days": 2, "median": 24.0, "max": 25.0, "left_out": 0}},
              "load_log_source": "clean.load_log",
              "tasks": [], "tables": [{"table": "raw.mcf_job_snapshots", "kind": "r", "bytes": 10 * sr.GIB,
                                       "rows_estimate": 180_000}],
              "schema_bytes": {"raw": 10 * sr.GIB}, "database_bytes": 11 * sr.GIB,
              "per_day": {"Raw JSONB (raw.mcf_job_snapshots)": 5 * sr.GIB},
              "projection": sr.project({"Raw JSONB (raw.mcf_job_snapshots)": 5 * sr.GIB}),
              "cost": {"Raw JSONB (raw.mcf_job_snapshots)": 175.2}, "rows_per_day_summary": "90,000 raw rows a day",
              "restore": {"file": "2026-10-25.jsonl.gz", "file_bytes": 145_000_000, "postings_in_file": 88916,
                          "read_file_seconds": 6.0, "restore_raw_seconds": 30.2, "rows_restored": 88916,
                          "restored_table_bytes": 470_000_000, "verified": True}}
    md = sr.render(report)
    assert md.startswith(sr.BEGIN) and md.endswith(sr.END)
    for heading in ("### Collection", "### Transform time per day", "### Warehouse size now",
                    "### Growth per snapshot day and projection", "### Restore test: 2026-10-25.jsonl.gz"):
        assert heading in md
    assert "| Raw JSONB (raw.mcf_job_snapshots) | 5.0 GB | 150.0 GB | 450.0 GB | 1,825.0 GB | $175.20 |" in md
    assert "| File | 138.3 MB, 88,916 postings |" in md
    assert "**30.2 s**" in md and "passed" in md
