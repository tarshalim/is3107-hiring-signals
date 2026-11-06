"""Offline tests for the Parquet lake export (transform/lake_export.py) and its DAG. No database."""
import ast
from datetime import date
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from transform import lake_export as lx  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
DAG_FILE = REPO / "dags" / "weekly_lake_export_dag.py"


def _fact_row(uuid="a" * 32, skills=(3, 1)):
    values = {"job_uuid": uuid, "job_key": 7, "views_total": 120, "applications_total": 7, "views_delta": None,
              "applications_delta": None, "repost_count": 0, "edit_count": 0, "status": "Open",
              "days_since_new_posting": 0, "days_since_original_posting": 0, "days_to_expiry": 14,
              "prev_snapshot_date": None, "hours_since_prev": None, "is_first_snapshot": True,
              "reposted_since_prev": None, "edited_since_prev": None, "title": "Staff Nurse",
              "company_name": "HRNET", "company_ssic_code": "78104", "ssoc_code": "22200", "ssoc_major_group": 2,
              "mom_group": "PMET", "category": "Healthcare / Pharmaceutical", "position_level": "Non-executive",
              "min_years_exp": 1, "exp_out_of_range": False, "salary_min": 3500.0, "salary_max": 4200.0,
              "salary_mid": 3850.0, "vacancies": 4, "original_posting_date": date(2026, 10, 24),
              "new_posting_date": date(2026, 10, 24), "expiry_date": date(2026, 11, 7), "skill_keys": list(skills)}
    return tuple(values[name] for name, _, _ in lx.FACT_COLUMNS)


def test_explicit_schema_matches_the_sql():
    schema = lx.arrow_schema(lx.FACT_COLUMNS)
    assert schema.field("skill_keys").type == pa.list_(pa.int32())
    assert schema.field("prev_snapshot_date").type == pa.date32() and schema.field("job_key").type == pa.int64()
    assert "snapshot_date" not in schema.names            # it is the partition folder, not a column in the file
    assert lx.FACT_SQL.count(",") >= len(lx.FACT_COLUMNS) - 1 and "WHERE f.snapshot_date = %s" in lx.FACT_SQL
    assert "clean.bridge_job_skill" in lx.FACT_SQL and "ORDER BY f.job_uuid" in lx.FACT_SQL


def test_rows_to_table_keeps_types_even_for_all_null_columns():
    table = lx.rows_to_table([_fact_row(), _fact_row("b" * 32, ())], lx.FACT_COLUMNS)
    assert table.num_rows == 2 and table.schema == lx.arrow_schema(lx.FACT_COLUMNS)
    assert table.column("views_delta").null_count == 2 and table.column("skill_keys").to_pylist() == [[3, 1], []]
    assert lx.rows_to_table([], lx.FACT_COLUMNS).num_rows == 0


def test_write_atomic_overwrites_and_leaves_one_file(tmp_path):
    folder = lx.partition_dir(tmp_path, "2026-10-24")
    schema = lx.arrow_schema(lx.FACT_COLUMNS)
    folder.mkdir(parents=True)
    (folder / "part-old.parquet").write_bytes(b"stale")
    path, rows = lx.write_atomic([lx.rows_to_table([_fact_row()], lx.FACT_COLUMNS)], folder, schema)
    first = path.read_bytes()
    path2, rows2 = lx.write_atomic([lx.rows_to_table([_fact_row()], lx.FACT_COLUMNS)], folder, schema)
    assert (path, rows) == (path2, rows2) == (folder / "part-0.parquet", 1)
    assert path.read_bytes() == first                                   # rerun: byte-identical
    assert sorted(p.name for p in folder.iterdir()) == ["part-0.parquet"]
    assert folder.name == "snapshot_date=2026-10-24"


def test_batches_become_row_groups_of_one_file(tmp_path):
    batches = [lx.rows_to_table([_fact_row(f"{i:032d}")], lx.FACT_COLUMNS) for i in range(3)]
    path, rows = lx.write_atomic(batches, tmp_path / "d", lx.arrow_schema(lx.FACT_COLUMNS))
    assert rows == 3 and pq.ParquetFile(path).metadata.num_row_groups == 3
    assert pq.ParquetFile(path).metadata.row_group(0).column(0).compression == "ZSTD"


def test_a_day_with_no_rows_removes_its_partition(tmp_path):
    folder = lx.partition_dir(tmp_path, "2026-10-24")
    lx.write_atomic([lx.rows_to_table([_fact_row()], lx.FACT_COLUMNS)], folder, lx.arrow_schema(lx.FACT_COLUMNS))
    assert lx.write_atomic([], folder, lx.arrow_schema(lx.FACT_COLUMNS)) == (None, 0)
    assert not folder.exists()


def test_partitions_read_back_as_one_dataset(tmp_path):
    import pyarrow.dataset as ds
    schema = lx.arrow_schema(lx.FACT_COLUMNS)
    for d, n in (("2026-10-24", 2), ("2026-10-25", 1)):
        rows = [_fact_row(f"{d}{i}".ljust(32, "x")) for i in range(n)]
        lx.write_atomic([lx.rows_to_table(rows, lx.FACT_COLUMNS)], lx.partition_dir(tmp_path, d), schema)
    table = ds.dataset(tmp_path / "fact_job_day", format="parquet", partitioning="hive").to_table()
    assert table.num_rows == 3 and sorted(set(table.column("snapshot_date").to_pylist())) == ["2026-10-24", "2026-10-25"]


def test_default_lake_dir(monkeypatch, tmp_path):
    monkeypatch.delenv("LAKE_DIR", raising=False)
    assert lx.default_lake_dir() == REPO / "data" / "lake"
    monkeypatch.setenv("LAKE_DIR", str(tmp_path))
    assert lx.default_lake_dir() == tmp_path


def _dag_kwargs():
    for node in ast.walk(ast.parse(DAG_FILE.read_text(encoding="utf-8"))):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and getattr(dec.func, "id", None) == "dag":
                    return {kw.arg: kw.value for kw in dec.keywords}
    raise AssertionError("no @dag(...)")


def test_dag_settings():
    kw = _dag_kwargs()
    assert ast.literal_eval(kw["dag_id"]) == "weekly_lake_export"
    assert ast.literal_eval(kw["schedule"]) == "0 21 * * 2"            # after the transform's 20:00 deadline
    assert ast.literal_eval(kw["catchup"]) is True and ast.literal_eval(kw["max_active_runs"]) == 1
    assert ast.literal_eval(kw["is_paused_upon_creation"]) is True     # nothing runs until someone unpauses it
    assert ast.unparse(kw["start_date"]) == "pendulum.datetime(2026, 10, 20, 21, tz=SGT)"


def test_dag_maps_one_task_per_day_and_reuses_the_weekly_week():
    src = DAG_FILE.read_text(encoding="utf-8")
    assert "export_day.expand(snapshot_date=list_week_days())" in src
    assert "@task(max_active_tis_per_dag=2)" in src
    assert "snapshot_week.week_bounds(data_interval_start, data_interval_end)" in src
    assert "lake_export.days_between(conn, after, up_to)" in src
