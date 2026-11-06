"""Columnar lake: the clean fact rows as Parquet files, one partition per snapshot day (docs/lake-and-spark.md).

    data/lake/fact_job_day/snapshot_date=2026-10-24/part-0.parquet    one file per day (Hive-style partition)
    data/lake/dim_skill/part-0.parquet                                 small, rewritten every run

Each fact file is one day of clean.fact_job_day (grain: one posting on one snapshot day) with the attributes of the
dim_job version live that day and the posting's skill keys as a list column. So a partition is self-contained: a
reader needs only it and dim_skill, and a remapped skill (load_reference --remap) reaches every past day through
dim_skill without rewriting old partitions.

Why Parquet (wk2 [1:14:17], wk3 [2:32:00], wk5 [51:03] "IO is always the bottleneck"): columnar, so a question about
two columns reads two columns, not 90,000 whole JSON documents; typed and compressed per column (zstd); and the
format Spark, pandas and DuckDB all read. Postgres stays the system of record and the dashboard's source.

Idempotent (wk7 [056]): a partition is written to a hidden temporary file in its folder and renamed over
part-0.parquet in one step (os.replace), so a rerun overwrites the day and a reader never sees half a file. Files
starting with '.' or '_' are ignored by Spark and pyarrow. A day with no rows in clean has its partition removed,
so the lake mirrors clean. The export log goes to ops.lake_exports (sql/init/11_ops_lake_exports.sql).

    python -m transform.lake_export --date 2026-10-24 --date 2026-10-25     # chosen days
    python -m transform.lake_export --all                                     # every day in clean.fact_job_day
"""
import argparse
import json
import os
import shutil
import sys
import time
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
FACT_DATASET = "fact_job_day"
SKILL_DATASET = "dim_skill"
PART_FILE = "part-0.parquet"
COMPRESSION = "zstd"
BATCH_ROWS = 50_000       # rows per fetch and per Parquet row group

# (column, pyarrow type name, SQL expression). Numeric columns are cast to float8 so psycopg2 returns floats.
FACT_COLUMNS = [
    ("job_uuid", "string", "f.job_uuid"),
    ("job_key", "int64", "f.job_key"),
    ("views_total", "int32", "f.views_total"),
    ("applications_total", "int32", "f.applications_total"),
    ("views_delta", "int32", "f.views_delta"),
    ("applications_delta", "int32", "f.applications_delta"),
    ("repost_count", "int16", "f.repost_count"),
    ("edit_count", "int16", "f.edit_count"),
    ("status", "string", "f.status"),
    ("days_since_new_posting", "int16", "f.days_since_new_posting"),
    ("days_since_original_posting", "int16", "f.days_since_original_posting"),
    ("days_to_expiry", "int16", "f.days_to_expiry"),
    ("prev_snapshot_date", "date32", "f.prev_snapshot_date"),
    ("hours_since_prev", "float64", "f.hours_since_prev::float8"),
    ("is_first_snapshot", "bool", "f.is_first_snapshot"),
    ("reposted_since_prev", "bool", "f.reposted_since_prev"),
    ("edited_since_prev", "bool", "f.edited_since_prev"),
    ("title", "string", "j.title"),
    ("company_name", "string", "j.company_name"),
    ("company_ssic_code", "string", "j.company_ssic_code"),
    ("ssoc_code", "string", "j.ssoc_code"),
    ("ssoc_major_group", "int16", "j.ssoc_major_group"),
    ("mom_group", "string", "j.mom_group"),
    ("category", "string", "j.category"),
    ("position_level", "string", "j.position_level"),
    ("min_years_exp", "int16", "j.min_years_exp"),
    ("exp_out_of_range", "bool", "j.exp_out_of_range"),
    ("salary_min", "float64", "j.salary_min::float8"),
    ("salary_max", "float64", "j.salary_max::float8"),
    ("salary_mid", "float64", "j.salary_mid::float8"),
    ("vacancies", "int32", "j.vacancies"),
    ("original_posting_date", "date32", "j.original_posting_date"),
    ("new_posting_date", "date32", "j.new_posting_date"),
    ("expiry_date", "date32", "j.expiry_date"),
    ("skill_keys", "list<int32>",
     "COALESCE(ARRAY(SELECT b.skill_key FROM clean.bridge_job_skill b WHERE b.job_key = f.job_key "
     "ORDER BY b.skill_key), '{}')"),
]

FACT_SQL = ("SELECT " + ", ".join(sql for _, _, sql in FACT_COLUMNS) + """
FROM clean.fact_job_day f
JOIN clean.dim_job j ON j.job_key = f.job_key
WHERE f.snapshot_date = %s
ORDER BY f.job_uuid""")

SKILL_COLUMNS = [
    ("skill_key", "int32", "skill_key"),
    ("skill_ref", "string", "COALESCE(ssg_skill_id, mcf_skill_uuid)"),     # the metrics' skill identity
    ("skill_name", "string", "COALESCE(ssg_skill_title, mcf_skill_name)"),
    ("is_mapped", "bool", "ssg_skill_id IS NOT NULL"),
    ("mcf_skill_name", "string", "mcf_skill_name"),
    ("ssg_skill_id", "string", "ssg_skill_id"),
    ("ssg_skill_type", "string", "ssg_skill_type"),
    ("is_emerging", "bool", "is_emerging"),
    ("match_method", "string", "match_method"),
]

SKILL_SQL = "SELECT " + ", ".join(sql for _, _, sql in SKILL_COLUMNS) + " FROM clean.dim_skill ORDER BY skill_key"


def default_lake_dir() -> Path:
    """LAKE_DIR, else data/lake next to the code (/opt/airflow/data/lake inside the Airflow containers)."""
    return Path(os.environ.get("LAKE_DIR") or REPO_ROOT / "data" / "lake")


def _arrow_type(name: str):
    import pyarrow as pa
    if name.startswith("list<"):
        return pa.list_(_arrow_type(name[5:-1]))
    return {"string": pa.string(), "int64": pa.int64(), "int32": pa.int32(), "int16": pa.int16(),
            "float64": pa.float64(), "bool": pa.bool_(), "date32": pa.date32()}[name]


def arrow_schema(columns):
    import pyarrow as pa
    return pa.schema([(name, _arrow_type(kind)) for name, kind, _ in columns])


def rows_to_table(rows, columns):
    """psycopg2 rows -> pyarrow Table with the explicit schema (never inferred, so every day has the same types)."""
    import pyarrow as pa
    schema = arrow_schema(columns)
    by_column = list(zip(*rows)) if rows else [[] for _ in columns]
    return pa.Table.from_arrays([pa.array(list(values), type=field.type) for values, field in zip(by_column, schema)],
                                schema=schema)


def partition_dir(lake_dir: Path, snapshot_date) -> Path:
    return Path(lake_dir) / FACT_DATASET / f"snapshot_date={snapshot_date}"


def write_atomic(tables, folder: Path, schema):
    """Write the tables (an iterable of batches) as folder/part-0.parquet through a hidden temp file and one
    rename, then drop any other data file there. Returns (path, rows), or (None, 0) if there were no rows (the
    folder is then removed, so the lake mirrors clean)."""
    import pyarrow.parquet as pq
    folder.mkdir(parents=True, exist_ok=True)
    final = folder / PART_FILE
    tmp = folder / f".{PART_FILE}.tmp-{os.getpid()}"
    rows, writer = 0, None
    try:
        for table in tables:
            if table.num_rows == 0:
                continue
            if writer is None:
                writer = pq.ParquetWriter(tmp, schema, compression=COMPRESSION)
            writer.write_table(table)
            rows += table.num_rows
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        shutil.rmtree(folder, ignore_errors=True)
        return None, 0
    os.replace(tmp, final)
    for other in folder.iterdir():
        if other.is_file() and other.name != PART_FILE and not other.name.startswith("."):
            other.unlink()           # e.g. a file from an older naming scheme: the folder holds exactly one day
    return final, rows


def _batches(conn, sql, params, columns, name):
    """Stream the query through a server-side cursor, BATCH_ROWS at a time, as pyarrow tables: a day is about
    90,000 rows, and holding them all as Python objects would cost a few hundred MB on a 6 GB laptop."""
    with conn.cursor(name=name) as cur:
        cur.itersize = BATCH_ROWS
        cur.execute(sql, params)
        while True:
            rows = cur.fetchmany(BATCH_ROWS)
            if not rows:
                break
            yield rows_to_table(rows, columns)
    conn.rollback()                          # end the read-only transaction


def _log(conn, dataset, partition_key, rows, nbytes, seconds, path):
    with conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('ops.lake_exports') IS NOT NULL")
        if not cur.fetchone()[0]:
            return False                 # 11_ops_lake_exports.sql not applied: the files are still written
        cur.execute("""
            INSERT INTO ops.lake_exports (dataset, partition_key, rows, bytes, seconds, path)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (dataset, partition_key) DO UPDATE SET rows = EXCLUDED.rows, bytes = EXCLUDED.bytes,
                seconds = EXCLUDED.seconds, path = EXCLUDED.path, exported_at = now()
        """, (dataset, partition_key, rows, nbytes, round(seconds, 3), path))
    return True


def export_day(conn, snapshot_date, lake_dir: Path | None = None) -> dict:
    """Overwrite one day's partition with that day's clean fact rows. Returns rows, bytes and seconds."""
    lake_dir = Path(lake_dir or default_lake_dir())
    snapshot_date = date.fromisoformat(str(snapshot_date))
    t0 = time.perf_counter()
    path, rows = write_atomic(_batches(conn, FACT_SQL, (snapshot_date,), FACT_COLUMNS, f"lake_{snapshot_date:%Y%m%d}"),
                              partition_dir(lake_dir, snapshot_date), arrow_schema(FACT_COLUMNS))
    seconds = time.perf_counter() - t0
    if path is None:
        return {"dataset": FACT_DATASET, "snapshot_date": str(snapshot_date), "rows": 0, "bytes": 0,
                "seconds": round(seconds, 3), "removed": True}
    nbytes, rel = path.stat().st_size, path.relative_to(lake_dir).as_posix()
    logged = _log(conn, FACT_DATASET, str(snapshot_date), rows, nbytes, seconds, rel)
    return {"dataset": FACT_DATASET, "snapshot_date": str(snapshot_date), "rows": rows, "bytes": nbytes,
            "seconds": round(seconds, 3), "path": rel, "logged": logged}


def export_dim_skill(conn, lake_dir: Path | None = None) -> dict:
    lake_dir = Path(lake_dir or default_lake_dir())
    t0 = time.perf_counter()
    path, rows = write_atomic(_batches(conn, SKILL_SQL, None, SKILL_COLUMNS, "lake_dim_skill"),
                              lake_dir / SKILL_DATASET, arrow_schema(SKILL_COLUMNS))
    seconds = time.perf_counter() - t0
    if path is None:
        return {"dataset": SKILL_DATASET, "rows": 0, "bytes": 0, "seconds": round(seconds, 3), "removed": True}
    nbytes, rel = path.stat().st_size, path.relative_to(lake_dir).as_posix()
    logged = _log(conn, SKILL_DATASET, "all", rows, nbytes, seconds, rel)
    return {"dataset": SKILL_DATASET, "rows": rows, "bytes": nbytes, "seconds": round(seconds, 3),
            "path": rel, "logged": logged}


def days_between(conn, after, up_to) -> list[str]:
    """Snapshot days in clean.fact_job_day with after < snapshot_date <= up_to (a weekly run's week)."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT snapshot_date FROM clean.fact_job_day WHERE snapshot_date > %s "
                    "AND snapshot_date <= %s ORDER BY 1", (after, up_to))
        days = [str(r[0]) for r in cur.fetchall()]
    conn.rollback()
    return days


def all_days(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT snapshot_date FROM clean.fact_job_day ORDER BY 1")
        days = [str(r[0]) for r in cur.fetchall()]
    conn.rollback()
    return days


def main(argv=None):
    from transform.db import connect
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", action="append", default=[], help="YYYY-MM-DD; repeat for several days")
    ap.add_argument("--all", action="store_true", help="every day in clean.fact_job_day")
    ap.add_argument("--lake", help="lake root (default: LAKE_DIR or data/lake)")
    args = ap.parse_args(argv)
    lake = Path(args.lake) if args.lake else default_lake_dir()
    conn = connect()
    try:
        days = all_days(conn) if args.all else args.date
        if not days:
            ap.error("give --date or --all")
        for d in days:
            print(json.dumps(export_day(conn, d, lake)))
        print(json.dumps(export_dim_skill(conn, lake)))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
