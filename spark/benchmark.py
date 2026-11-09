"""Measure the lake against the warehouse: sizes, scan times, and Spark against Postgres (docs/lake-and-spark.md).

Run on a laptop with the warehouse up, after the lake export and the Spark job:

    python -m transform.lake_export --all
    docker compose --profile spark run --rm spark
    python spark/benchmark.py --raw-dir data/raw/mcf            # WAREHOUSE_* from .env (port 5433)

Read-only on the warehouse. Writes data/lake/_bench/benchmark.json and benchmark.md (git-ignored), and prints the
markdown. Every timing is repeated (--repeat, default 3): the first run is cold-ish (files and pages not yet
cached), the median of the rest is warm. Totals from each method are printed so the methods can be seen to agree.

  1. Size of one day in each format: JSONL.gz (the raw file), JSONB (raw.mcf_job_snapshots), the clean star
     tables, the full raw payload as Parquet (converted here once, not kept in the lake), the lake's clean
     fact Parquet.
  2. One question, five ways: postings, views and applications per category on one day, from the gz file
     (Python json, and pyarrow's C++ JSON reader), from JSONB, from the clean star, and from the Parquet lake.
  3. Weekly skill demand: SKILL_DEMAND_SQL on Postgres against spark/skill_demand.py's output and timing,
     compared row for row.
"""
import argparse
import gzip
import json
import os
import statistics
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from transform.db import connect  # noqa: E402

# The Postgres version of spark/skill_demand.py. Same definitions: a posting-day counts once per skill_ref
# (COALESCE(ssg_skill_id, mcf_skill_uuid)), weeks start on Monday, the name is the lowest skill_key's.
SKILL_DEMAND_SQL = """
WITH names AS (
    SELECT DISTINCT ON (COALESCE(ssg_skill_id, mcf_skill_uuid))
           COALESCE(ssg_skill_id, mcf_skill_uuid) AS skill_ref, COALESCE(ssg_skill_title, mcf_skill_name) AS skill_name
    FROM clean.dim_skill
    ORDER BY COALESCE(ssg_skill_id, mcf_skill_uuid), skill_key
),
rows AS (
    SELECT DISTINCT f.snapshot_date, f.job_uuid, COALESCE(f.applications_delta, 0) AS apps,
           COALESCE(s.ssg_skill_id, s.mcf_skill_uuid) AS skill_ref
    FROM clean.fact_job_day f
    JOIN clean.bridge_job_skill b ON b.job_key = f.job_key
    JOIN clean.dim_skill s ON s.skill_key = b.skill_key
),
per AS (
    SELECT date_trunc('week', snapshot_date)::date AS week_start, skill_ref,
           count(DISTINCT job_uuid) AS postings, count(*) AS posting_days, sum(apps) AS new_applications
    FROM rows GROUP BY 1, 2
),
totals AS (
    SELECT date_trunc('week', snapshot_date)::date AS week_start, count(DISTINCT job_uuid) AS week_postings
    FROM clean.fact_job_day GROUP BY 1
)
SELECT p.week_start, p.skill_ref, n.skill_name, p.postings, p.posting_days, p.new_applications, t.week_postings,
       p.postings::float8 / t.week_postings AS share_of_postings
FROM per p JOIN totals t USING (week_start) JOIN names n USING (skill_ref)
"""

JSONB_SCAN_SQL = """
SELECT payload->'categories'->0->>'category' AS category, count(*),
       sum((payload->'metadata'->>'totalNumberOfView')::bigint),
       sum((payload->'metadata'->>'totalNumberJobApplication')::bigint)
FROM raw.mcf_job_snapshots WHERE snapshot_date = %s GROUP BY 1
"""

CLEAN_SCAN_SQL = """
SELECT j.category, count(*), sum(f.views_total), sum(f.applications_total)
FROM clean.fact_job_day f JOIN clean.dim_job j ON j.job_key = f.job_key
WHERE f.snapshot_date = %s GROUP BY 1
"""


def timed(fn, repeat):
    """(result of the last run, [seconds per run])."""
    times, result = [], None
    for _ in range(max(1, repeat)):
        t = time.perf_counter()
        result = fn()
        times.append(round(time.perf_counter() - t, 3))
    return result, times


def summary(times):
    warm = times[1:] or times
    return {"first": times[0], "warm_median": round(statistics.median(warm), 3), "runs": times}


def totals(rows):
    """[(category, postings, views, applications)] -> (postings, views, applications) over all categories."""
    return tuple(int(sum(r[i] or 0 for r in rows)) for i in (1, 2, 3))


def query(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    conn.rollback()
    return rows


# ---------------- 1. sizes ----------------

def sizes(conn, lake, raw_dir, days, raw_parquet_dir):
    import pyarrow as pa
    import pyarrow.json as pj
    import pyarrow.parquet as pq

    per_day = {str(d): {"rows": int(n), "jsonb_bytes": int(b)} for d, n, b in query(conn, """
        SELECT snapshot_date, count(*), sum(pg_column_size(payload)) FROM raw.mcf_job_snapshots GROUP BY 1""")}
    n_days = len(per_day) or 1
    (raw_total,), = query(conn, "SELECT pg_total_relation_size('raw.mcf_job_snapshots')")
    (star_total,), = query(conn, """SELECT pg_total_relation_size('clean.fact_job_day')
        + pg_total_relation_size('clean.dim_job') + pg_total_relation_size('clean.bridge_job_skill')
        + pg_total_relation_size('clean.dim_skill')""")
    (fact_total,), = query(conn, "SELECT pg_total_relation_size('clean.fact_job_day')")
    out = {"days_in_warehouse": n_days, "raw_table_total_bytes": int(raw_total), "star_total_bytes": int(star_total),
           "fact_table_total_bytes": int(fact_total), "days": {}}
    for d in days:
        row = dict(per_day.get(d, {}))
        gz = Path(raw_dir) / f"{d}.jsonl.gz" if raw_dir else None
        row["jsonl_gz_bytes"] = gz.stat().st_size if gz and gz.exists() else None
        part = Path(lake) / "fact_job_day" / f"snapshot_date={d}" / "part-0.parquet"
        row["parquet_fact_bytes"] = part.stat().st_size if part.exists() else None
        out["days"][d] = row
    # The full raw payload as Parquet, for a like-for-like size with JSONL.gz and JSONB. One day is enough.
    first = next((d for d in days if out["days"][d]["jsonl_gz_bytes"]), None)
    if first:
        raw_parquet_dir.mkdir(parents=True, exist_ok=True)
        target = raw_parquet_dir / f"raw_{first}.parquet"
        t = time.perf_counter()
        table = pj.read_json(pa.input_stream(str(Path(raw_dir) / f"{first}.jsonl.gz"), compression="gzip"),
                             read_options=pj.ReadOptions(block_size=64 << 20))
        pq.write_table(table, target, compression="zstd")
        out["raw_parquet"] = {"day": first, "bytes": target.stat().st_size, "columns": table.num_columns,
                              "seconds_to_convert": round(time.perf_counter() - t, 2)}
        slim = raw_parquet_dir / f"raw_{first}_no_description.parquet"
        pq.write_table(table.drop_columns(["description"]), slim, compression="zstd")
        out["raw_parquet"]["bytes_without_description"] = slim.stat().st_size
        del table
    return out


# ---------------- 2. one question, five ways ----------------

def scan_jsonl_python(path):
    agg = {}
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            job = json.loads(line)
            cats = job.get("categories") or []
            cat = cats[0].get("category") if cats else None
            meta = job.get("metadata") or {}
            a = agg.setdefault(cat, [0, 0, 0])
            a[0] += 1
            a[1] += meta.get("totalNumberOfView") or 0
            a[2] += meta.get("totalNumberJobApplication") or 0
    return [(k, *v) for k, v in agg.items()]


def scan_jsonl_pyarrow(path):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.json as pj
    t = pj.read_json(pa.input_stream(str(path), compression="gzip"), read_options=pj.ReadOptions(block_size=64 << 20))
    meta = t.column("metadata")
    first =pc.struct_field(pc.list_element(t.column("categories"), 0), "category")
    small = pa.table({"category": first, "views": pc.struct_field(meta, "totalNumberOfView"),
                      "apps": pc.struct_field(meta, "totalNumberJobApplication")})
    g = small.group_by("category").aggregate([("views", "count"), ("views", "sum"), ("apps", "sum")])
    return list(zip(g["category"].to_pylist(), g["views_count"].to_pylist(), g["views_sum"].to_pylist(),
                    g["apps_sum"].to_pylist()))


def scan_parquet(path):
    import pyarrow.parquet as pq
    t = pq.read_table(path, columns=["category", "views_total", "applications_total"])
    g = t.group_by("category").aggregate([("views_total", "count"), ("views_total", "sum"),
                                          ("applications_total", "sum")])
    return list(zip(g["category"].to_pylist(), g["views_total_count"].to_pylist(), g["views_total_sum"].to_pylist(),
                    g["applications_total_sum"].to_pylist()))


def scans(conn, lake, raw_dir, day, repeat):
    out = {"day": day}
    gz = Path(raw_dir) / f"{day}.jsonl.gz" if raw_dir else None
    part = Path(lake) / "fact_job_day" / f"snapshot_date={day}" / "part-0.parquet"
    methods = []
    if gz and gz.exists():
        methods += [("jsonl_gz_python_json", lambda: scan_jsonl_python(gz), 1),
                    ("jsonl_gz_pyarrow", lambda: scan_jsonl_pyarrow(gz), repeat)]
    methods += [("postgres_jsonb", lambda: query(conn, JSONB_SCAN_SQL, (day,)), repeat),
                ("postgres_clean_star", lambda: query(conn, CLEAN_SCAN_SQL, (day,)), repeat)]
    if part.exists():
        methods.append(("parquet_lake_pyarrow", lambda: scan_parquet(part), repeat))
    for name, fn, n in methods:
        rows, times = timed(fn, n)
        out[name] = {**summary(times), "totals_postings_views_applications": totals(rows), "groups": len(rows)}
    return out


# ---------------- 3. Spark against Postgres ----------------

def spark_vs_postgres(conn, lake, repeat):
    import pyarrow.parquet as pq
    rows, times = timed(lambda: query(conn, SKILL_DEMAND_SQL), repeat)
    out = {"postgres": {**summary(times), "rows": len(rows)}}
    timing_file = Path(lake) / "_bench" / "spark_timing.json"
    result_dir = Path(lake) / "_spark" / "skill_demand_weekly"
    if not timing_file.exists() or not result_dir.exists():
        out["spark"] = "not run: docker compose --profile spark run --rm spark"
        return out
    out["spark"] = json.loads(timing_file.read_text(encoding="utf-8"))
    files = [p for p in result_dir.glob("*.parquet")]
    spark_rows = pq.ParquetDataset(files).read().to_pylist() if files else []
    pg = {(str(r[0]), r[1]): (int(r[3]), int(r[4]), int(r[5]), int(r[6]), float(r[7])) for r in rows}
    sp = {(str(r["week_start"]), r["skill_ref"]): (int(r["postings"]), int(r["posting_days"]),
                                                   int(r["new_applications"]), int(r["week_postings"]),
                                                   float(r["share_of_postings"])) for r in spark_rows}
    mismatched = [k for k in pg.keys() & sp.keys()
                  if pg[k][:4] != sp[k][:4] or abs(pg[k][4] - sp[k][4]) > 1e-12]
    out["comparison"] = {"postgres_rows": len(pg), "spark_rows": len(sp), "only_postgres": len(pg.keys() - sp.keys()),
                         "only_spark": len(sp.keys() - pg.keys()), "value_mismatches": len(mismatched),
                         "identical": len(pg) == len(sp) and not mismatched and pg.keys() == sp.keys()}
    return out


# ---------------- report ----------------

def mb(n):
    return "n/a" if n is None else f"{n / 2**20:,.1f} MB"


def to_markdown(res):
    s, lines = res["sizes"], []
    lines += ["### Size of one day", "", "| Day | Postings | JSONL.gz file | JSONB in Postgres (payload column) | "
              "Parquet, clean fact (lake) |", "|---|---|---|---|---|"]
    for d, r in s["days"].items():
        lines.append(f"| {d} | {r.get('rows', 'n/a'):,} | {mb(r.get('jsonl_gz_bytes'))} | {mb(r.get('jsonb_bytes'))} | "
                     f"{mb(r.get('parquet_fact_bytes'))} |")
    n = s["days_in_warehouse"]
    lines += ["", f"Whole tables over {n} day(s), with indexes and TOAST: raw.mcf_job_snapshots "
              f"{mb(s['raw_table_total_bytes'])} ({mb(s['raw_table_total_bytes'] / n)} a day), clean star "
              f"{mb(s['star_total_bytes'])} ({mb(s['star_total_bytes'] / n)} a day; fact table alone "
              f"{mb(s['fact_table_total_bytes'] / n)} a day)."]
    if "raw_parquet" in s:
        rp = s["raw_parquet"]
        lines.append(f"Full raw payload of {rp['day']} as Parquet (zstd, {rp['columns']} top-level columns, schema "
                     f"inferred by pyarrow): {mb(rp['bytes'])}, {mb(rp['bytes_without_description'])} without the "
                     f"HTML description; converted in {rp['seconds_to_convert']} s.")
    sc = res["scans"]
    lines += ["", f"### One question, five ways: postings, views and applications per category on {sc['day']}", "",
              "| Method | First run | Warm median | Totals (postings, views, applications) |", "|---|---|---|---|"]
    for name, v in sc.items():
        if isinstance(v, dict):
            lines.append(f"| {name} | {v['first']} s | {v['warm_median']} s | {v['totals_postings_views_applications']} |")
    sv = res["spark_vs_postgres"]
    lines += ["", "### Weekly skill demand: Postgres SQL against PySpark on the lake", "",
              f"- Postgres: first {sv['postgres']['first']} s, warm median {sv['postgres']['warm_median']} s, "
              f"{sv['postgres']['rows']:,} rows."]
    if isinstance(sv.get("spark"), dict):
        sp = sv["spark"]
        lines.append(f"- Spark {sp['spark_version']} ({sp['master']}): job runs {sp['job_runs_seconds']} s "
                     f"(first {sp['first_run_seconds']} s), whole spark-submit process {sp['total_process_seconds']} s, "
                     f"{sp['result_rows']:,} rows from {sp['fact_rows']:,} fact rows.")
        lines.append(f"- Same answer: {sv['comparison']}")
    else:
        lines.append(f"- Spark: {sv.get('spark')}")
    return "\n".join(lines) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lake", default=str(REPO / "data" / "lake"))
    ap.add_argument("--raw-dir", help="folder with <date>.jsonl.gz files (default: data/raw/mcf)",
                    default=str(REPO / "data" / "raw" / "mcf"))
    ap.add_argument("--days", nargs="*", help="days to size (default: every day in the lake)")
    ap.add_argument("--scan-day", help="day for the five-way scan (default: the first of --days)")
    ap.add_argument("--repeat", type=int, default=3)
    args = ap.parse_args(argv)
    lake = Path(args.lake)
    days = args.days or sorted(p.name.split("=", 1)[1] for p in (lake / "fact_job_day").glob("snapshot_date=*"))
    if not days:
        ap.error("no days: export the lake first (python -m transform.lake_export --all) or pass --days")
    bench = lake / "_bench"
    conn = connect()
    try:
        res = {"sizes": sizes(conn, lake, args.raw_dir, days, bench),
               "scans": scans(conn, lake, args.raw_dir, args.scan_day or days[0], args.repeat),
               "spark_vs_postgres": spark_vs_postgres(conn, lake, args.repeat)}
    finally:
        conn.close()
    bench.mkdir(parents=True, exist_ok=True)
    (bench / "benchmark.json").write_text(json.dumps(res, indent=2, default=str), encoding="utf-8")
    md = to_markdown(res)
    (bench / "benchmark.md").write_text(md, encoding="utf-8")
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
