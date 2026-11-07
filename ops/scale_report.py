"""Scale, cost and restore report: measures the warehouse, projects a year of growth, times a restore of one day.

    python -m ops.scale_report                                   # warehouse from .env (port 5433), writes the doc
    python -m ops.scale_report --raw-dir data/raw/mcf --restore-file data/raw/mcf/2026-11-06.jsonl.gz --with-transform
    python -m ops.scale_report --ingest-runs ingest_runs.csv --load-log transform_load_log.csv \
        --task-runtimes airflow_task_runtimes.csv                 # run stats exported from the collector

It rewrites only the part of docs/scale-and-cost.md between the GENERATED markers; the hand-written text stays.

Read-only on the warehouse's own tables. The restore test creates a scratch database `is3107_restore_test` on the same
server, loads one gz file into it with the same code as the ingest DAG's load_raw (ingestion/load_snapshot_file.py),
checks every posting arrived unchanged, optionally rebuilds the clean tables for that day, and drops the database.

Projection model (stated in the report): the tables that hold one row per posting per day (raw snapshots, fact rows,
check verdicts) grow linearly with snapshot days; dim_job and the bridge grow with new postings and new versions,
so dividing their size by the days loaded and projecting linearly is an upper bound. Everything else (reference
tables, marts, metrics) is small and bounded.
"""
import argparse
import csv
import gzip
import json
import statistics
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DOC = REPO / "docs" / "scale-and-cost.md"
BEGIN = "<!-- BEGIN GENERATED: ops/scale_report.py -->"
END = "<!-- END GENERATED -->"
SCHEMAS = ("raw", "clean", "metrics", "mart", "ext", "ml", "ops")
HORIZONS = (30, 90, 365)
RESTORE_DB = "is3107_restore_test"
GIB = 2 ** 30

# Stores that grow with every snapshot day: label -> tables whose total size is divided by the days loaded.
DAILY_STORES = {
    "Raw JSONB (raw.mcf_job_snapshots)": ["raw.mcf_job_snapshots"],
    "Clean star (fact, dim_job, bridge, dim_skill)": ["clean.fact_job_day", "clean.dim_job", "clean.bridge_job_skill",
                                                      "clean.dim_skill"],
}

# Monthly list prices used for the cost table. AWS on-demand, Singapore (ap-southeast-1), as published in 2025;
# confirm on calculator.aws before quoting them in the report.
PRICES = {
    "ebs_gp3_gb_month": 0.096,        # block storage for a self-managed Postgres on a VM
    "s3_standard_gb_month": 0.025,    # object storage for gz files and Parquet
    "vm_4gb_month": 0.0528 * 730,     # t3.medium (2 vCPU, 4 GiB), on demand, 730 hours
}


# ---------------- inputs ----------------

def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _query(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    conn.rollback()
    return rows


def ingest_rows_from_db(conn):
    rows = _query(conn, "SELECT snapshot_date, pages_fetched, jobs_fetched, api_total, seconds_taken, finished_at "
                        "FROM raw.ingest_runs ORDER BY snapshot_date")
    keys = ("snapshot_date", "pages_fetched", "jobs_fetched", "api_total", "seconds_taken", "finished_at")
    return [dict(zip(keys, (str(v) if v is not None else None for v in r))) for r in rows]


def load_log_rows_from_db(conn):
    rows = _query(conn, "SELECT run_id, snapshot_date, step, seconds FROM clean.load_log ORDER BY load_id")
    return [{"run_id": r[0], "snapshot_date": str(r[1]), "step": r[2], "seconds": str(r[3])} for r in rows]


# ---------------- summaries (pure) ----------------

def ingest_summary(rows) -> dict:
    """The daily crawl from raw.ingest_runs (or its CSV export)."""
    if not rows:
        return {}
    jobs = [_num(r["jobs_fetched"]) for r in rows]
    secs = [_num(r["seconds_taken"]) for r in rows]
    cover = [_num(r["jobs_fetched"]) / _num(r["api_total"]) for r in rows if _num(r.get("api_total"))]
    return {"days": len(rows), "first": rows[0]["snapshot_date"][:10], "last": rows[-1]["snapshot_date"][:10],
            "jobs_median": statistics.median(jobs), "jobs_min": min(jobs), "jobs_max": max(jobs),
            "jobs_total": sum(jobs), "crawl_minutes_median": statistics.median(secs) / 60,
            "crawl_minutes_max": max(secs) / 60, "coverage_min": min(cover) if cover else None,
            "coverage_median": statistics.median(cover) if cover else None}


MAX_STEP_SECONDS = 86_400   # a logged step longer than a day is not a run time (see load_log_summary)


def load_log_summary(rows) -> dict:
    """Seconds per transform step, from the last run of each day (clean.load_log or its CSV export). Values over a
    day are left out and counted: runs logged before the 30 Oct fix stored the time since the run's logical date in
    the 'dag_run' row (e.g. 348,056 s for 24 Oct, transformed on 27 Oct), not the transform's own time."""
    last_run = {}
    for r in rows:
        last_run[r["snapshot_date"][:10]] = r["run_id"]
    by_step, dropped = {}, {}
    for r in rows:
        if last_run.get(r["snapshot_date"][:10]) == r["run_id"]:
            v = _num(r["seconds"])
            if v is None or v > MAX_STEP_SECONDS:
                dropped[r["step"]] = dropped.get(r["step"], 0) + 1
            else:
                by_step.setdefault(r["step"], []).append(v)
    return {step: {"days": len(v), "median": statistics.median(v), "max": max(v), "left_out": dropped.get(step, 0)}
            for step, v in by_step.items()}


def task_summary(rows) -> list:
    """Airflow's task_instance export has one row per task per DAG run, holding that run's last try. So: runs,
    runs that ended in success, median and max seconds of the successful ones that have a duration, runs that
    needed more than one try, runs that ended failed. A success with no duration (e.g. the four manual transform
    runs in the 6 Nov export, which have no start_date) still counts as a success; timed_successes is how many the
    median and max are over."""
    out = {}
    for r in rows:
        key = (r["dag_id"], r["task_id"])
        s = out.setdefault(key, {"tries": 0, "successes": 0, "timed": [], "retried": 0, "failed": 0})
        s["tries"] += 1
        if r.get("state") == "success":
            s["successes"] += 1
            if _num(r.get("duration_s")) is not None:
                s["timed"].append(_num(r["duration_s"]))
        if (_num(r.get("try_number")) or 1) > 1:
            s["retried"] += 1
        if r.get("state") == "failed":
            s["failed"] += 1
    return [{"dag_id": d, "task_id": t, "runs": s["tries"], "successes": s["successes"],
             "timed_successes": len(s["timed"]),
             "median_s": statistics.median(s["timed"]) if s["timed"] else None,
             "max_s": max(s["timed"]) if s["timed"] else None, "retried_runs": s["retried"],
             "failed_runs": s["failed"]}
            for (d, t), s in sorted(out.items())]


def per_day_bytes(sizes: dict, days_loaded: int) -> dict:
    """Bytes per snapshot day of each growing store, from {"schema.table": bytes}."""
    days_loaded = max(days_loaded, 1)
    return {label: sum(sizes.get(t, 0) for t in tables) / days_loaded for label, tables in DAILY_STORES.items()}


def project(per_day: dict, horizons=HORIZONS) -> dict:
    """Linear projection: {store: {days: bytes}}."""
    return {store: {h: b * h for h in horizons} for store, b in per_day.items() if b is not None}


def monthly_storage_cost(year_bytes: dict) -> dict:
    """$ per month to keep one year of each store: Postgres stores on block storage, files on object storage."""
    out = {}
    for store, b in year_bytes.items():
        price = PRICES["s3_standard_gb_month"] if ("file" in store.lower() or "parquet" in store.lower()) \
            else PRICES["ebs_gp3_gb_month"]
        out[store] = b / GIB * price
    return out


# ---------------- warehouse measurements ----------------

def table_sizes(conn) -> list:
    return [{"table": f"{s}.{t}", "kind": k, "bytes": int(b), "rows_estimate": int(max(n, 0))}
            for s, t, k, b, n in _query(conn, """
        SELECT n.nspname, c.relname, c.relkind, pg_total_relation_size(c.oid), c.reltuples
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'm', 'p') AND n.nspname = ANY(%s)
        ORDER BY 4 DESC""", (list(SCHEMAS),))]


def rows_per_day(conn) -> list:
    return [{"snapshot_date": str(d), "raw_rows": int(r or 0), "fact_rows": int(f or 0)} for d, r, f in _query(conn, """
        SELECT coalesce(r.snapshot_date, f.snapshot_date), r.n, f.n
        FROM (SELECT snapshot_date, count(*) AS n FROM raw.mcf_job_snapshots GROUP BY 1) r
        FULL JOIN (SELECT snapshot_date, count(*) AS n FROM clean.fact_job_day GROUP BY 1) f USING (snapshot_date)
        ORDER BY 1""")]


def lake_bytes_per_day(conn, lake_dir: Path):
    if _query(conn, "SELECT to_regclass('ops.lake_exports') IS NOT NULL")[0][0]:
        (avg,), = _query(conn, "SELECT avg(bytes) FROM ops.lake_exports WHERE dataset = 'fact_job_day'")
        if avg:
            return float(avg)
    files = list((lake_dir / "fact_job_day").glob("snapshot_date=*/part-0.parquet"))
    return sum(f.stat().st_size for f in files) / len(files) if files else None


def gz_bytes_per_day(raw_dir):
    files = sorted(Path(raw_dir).glob("*.jsonl.gz")) if raw_dir and Path(raw_dir).exists() else []
    return (sum(f.stat().st_size for f in files) / len(files), len(files)) if files else (None, 0)


# ---------------- restore test ----------------

def restore_test(dsn: str, gz_path: Path, with_transform: bool = False) -> dict:
    """Rebuild one day from its gz file in a scratch database, check it, drop the database."""
    import psycopg2

    from ingestion.load_snapshot_file import load_snapshot_file
    snapshot_date = date.fromisoformat(gz_path.name[:10])
    base = dsn.rpartition("/")[0]
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    out = {"file": gz_path.name, "file_bytes": gz_path.stat().st_size, "snapshot_date": str(snapshot_date)}
    try:
        with admin.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {RESTORE_DB}")
            cur.execute(f"CREATE DATABASE {RESTORE_DB}")
        conn = psycopg2.connect(f"{base}/{RESTORE_DB}")
        try:
            files = ["01_raw_schema.sql"] + (["02_clean_schema.sql"] if with_transform else [])
            with conn, conn.cursor() as cur:
                for f in files:
                    cur.execute((REPO / "sql" / "init" / f).read_text(encoding="utf-8"))
            t = time.perf_counter()
            with gzip.open(gz_path, "rt", encoding="utf-8") as f:
                first_line, lines = None, 0
                for line in f:
                    first_line = first_line or line
                    lines += 1
            out["read_file_seconds"] = round(time.perf_counter() - t, 1)
            loaded = load_snapshot_file(conn, gz_path, snapshot_date)
            out["restore_raw_seconds"] = loaded["seconds"]
            first = json.loads(first_line)
            rows = _query(conn, "SELECT count(*), count(DISTINCT job_uuid) FROM raw.mcf_job_snapshots")[0]
            payload = _query(conn, "SELECT payload FROM raw.mcf_job_snapshots WHERE job_uuid = %s", (first["uuid"],))
            out.update({"postings_in_file": lines, "rows_restored": int(rows[0]), "distinct_postings": int(rows[1]),
                        "first_posting_identical": bool(payload) and payload[0][0] == first})
            out["verified"] = out["rows_restored"] == out["distinct_postings"] == lines and out["first_posting_identical"]
            (size,), = _query(conn, "SELECT pg_total_relation_size('raw.mcf_job_snapshots')")
            out["restored_table_bytes"] = int(size)
            if with_transform:
                from transform.load_reference import load_reference
                from transform.transform_snapshot import transform_snapshot
                t = time.perf_counter()
                load_reference(conn)
                out["load_reference_seconds"] = round(time.perf_counter() - t, 1)
                t = time.perf_counter()
                stats = transform_snapshot(conn, snapshot_date, run_id="restore-test")
                out["transform_seconds"] = round(time.perf_counter() - t, 1)
                out["fact_rows"] = stats["fact_rows"]
        finally:
            conn.close()
    finally:
        with admin.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {RESTORE_DB}")
        admin.close()
    return out


# ---------------- report ----------------

def _gb(b):
    if b is None:
        return "n/a"
    return f"{b / GIB:,.1f} GB" if b >= GIB else f"{b / 2**20:,.1f} MB"


def render(r: dict) -> str:
    L = [BEGIN, "", f"_Generated {r['generated_at']} by `python -m ops.scale_report` from warehouse "
         f"`{r['warehouse']}` ({r['days_loaded']} snapshot days loaded). {r['note']}_", ""]
    ing = r.get("ingest") or {}
    if ing:
        cov = "n/a" if ing["coverage_min"] is None else f"{ing['coverage_median']:.1%} median, {ing['coverage_min']:.1%} lowest"
        L += [f"### Collection ({r['ingest_source']})", "", "| | |", "|---|---|",
              f"| Days collected | {ing['days']} ({ing['first']} to {ing['last']}) |",
              f"| Postings per day | {ing['jobs_median']:,.0f} median ({ing['jobs_min']:,.0f} to {ing['jobs_max']:,.0f}) |",
              f"| Posting-days so far | {ing['jobs_total']:,.0f} |",
              f"| Crawl time | {ing['crawl_minutes_median']:.0f} min median, {ing['crawl_minutes_max']:.0f} min longest |",
              f"| Coverage (fetched / API total) | {cov} |", ""]
    steps = r.get("load_log") or {}
    if steps:
        L += [f"### Transform time per day ({r['load_log_source']})", "", "| Step | Days | Median s | Max s |",
              "|---|---|---|---|"]
        for step, s in sorted(steps.items(), key=lambda kv: -kv[1]["median"]):
            L.append(f"| {step} | {s['days']} | {s['median']:.1f} | {s['max']:.1f} |")
        left_out = {k: v["left_out"] for k, v in steps.items() if v.get("left_out")}
        if left_out:
            L += ["", f"Left out as not run times (over a day, logged before the 30 Oct fix): {left_out}."]
        L.append("")
    tasks = r.get("tasks") or []
    if tasks:
        L += ["### Airflow task runs (from Airflow's task_instance table)", "",
              "| DAG | Task | Runs | Succeeded | Median s | Max s | Needed a retry | Ended failed |",
              "|---|---|---|---|---|---|---|---|"]
        for t in tasks:
            med = "n/a" if t["median_s"] is None else f"{t['median_s']:.1f}"
            mx = "n/a" if t["max_s"] is None else f"{t['max_s']:.1f}"
            ok = (f"{t['successes']}" if t["timed_successes"] == t["successes"]
                  else f"{t['successes']} ({t['timed_successes']} timed)")       # the median and max are over these
            L.append(f"| {t['dag_id']} | {t['task_id']} | {t['runs']} | {ok} | {med} | {mx} | "
                     f"{t['retried_runs']} | {t['failed_runs']} |")
        L.append("")
    L += ["### Warehouse size now", "", "| Schema | Size |", "|---|---|"]
    for schema, b in sorted(r["schema_bytes"].items(), key=lambda kv: -kv[1]):
        L.append(f"| {schema} | {_gb(b)} |")
    L += [f"| **database total** | **{_gb(r['database_bytes'])}** |", "", "Largest tables:", "",
          "| Table | Size | Rows (estimate) |", "|---|---|---|"]
    for t in r["tables"][:8]:
        L.append(f"| {t['table']} | {_gb(t['bytes'])} | {t['rows_estimate']:,} |")
    L += ["", "### Growth per snapshot day and projection", "",
          "| Store | Per day | 30 days | 90 days | 1 year (365 days) | $ a month to keep 1 year |",
          "|---|---|---|---|---|---|"]
    for store, p in r["projection"].items():
        cost = r["cost"].get(store)
        L.append(f"| {store} | {_gb(r['per_day'][store])} | {_gb(p[30])} | {_gb(p[90])} | {_gb(p[365])} | "
                 f"{'n/a' if cost is None else f'${cost:,.2f}'} |")
    L += ["", f"Rows per day: {r['rows_per_day_summary']}. Prices: block storage ${PRICES['ebs_gp3_gb_month']}/GB-month "
          f"(Postgres on a VM disk), object storage ${PRICES['s3_standard_gb_month']}/GB-month (files), a 4 GB VM "
          f"${PRICES['vm_4gb_month']:,.2f}/month; AWS Singapore list prices, check before quoting.", ""]
    rt = r.get("restore")
    if rt:
        L += [f"### Restore test: {rt['file']}", "", "| | |", "|---|---|",
              f"| File | {_gb(rt['file_bytes'])}, {rt['postings_in_file']:,} postings |",
              f"| Read the file only | {rt['read_file_seconds']} s |",
              f"| **Restore into raw (same code as load_raw)** | **{rt['restore_raw_seconds']} s**, "
              f"{rt['rows_restored']:,} rows, {_gb(rt['restored_table_bytes'])} in Postgres |",
              f"| Checks | rows = distinct postings = lines in file, first posting's JSON identical: "
              f"{'passed' if rt['verified'] else 'FAILED'} |"]
        if "transform_seconds" in rt:
            L.append(f"| Rebuild clean for the day | {rt['transform_seconds']} s ({rt['fact_rows']:,} fact rows), plus "
                     f"{rt['load_reference_seconds']} s to load the reference files once |")
        L.append("")
    L.append(END)
    return "\n".join(L)


def replace_generated(text: str, block: str) -> str:
    if BEGIN in text and END in text:
        return text[:text.index(BEGIN)] + block + text[text.index(END) + len(END):]
    return text.rstrip("\n") + "\n\n" + block + "\n"


def build_report(conn, args) -> dict:
    from transform.db import warehouse_dsn
    sizes = table_sizes(conn)
    by_table = {t["table"]: t["bytes"] for t in sizes}
    schema_bytes = {}
    for t in sizes:
        schema_bytes[t["table"].split(".")[0]] = schema_bytes.get(t["table"].split(".")[0], 0) + t["bytes"]
    (db_bytes,), = _query(conn, "SELECT pg_database_size(current_database())")
    days = rows_per_day(conn)
    days_loaded = sum(1 for d in days if d["raw_rows"]) or 1
    per_day = per_day_bytes(by_table, days_loaded)
    gz, n_gz = gz_bytes_per_day(args.raw_dir)
    per_day["Raw gz files (data/raw/mcf)"] = gz
    per_day["Parquet lake (data/lake/fact_job_day)"] = lake_bytes_per_day(conn, Path(args.lake))
    projection = project(per_day)
    cost = monthly_storage_cost({s: p[365] for s, p in projection.items()})
    raw_rows = [d["raw_rows"] for d in days if d["raw_rows"]]
    ingest_rows = read_csv(args.ingest_runs) if args.ingest_runs else ingest_rows_from_db(conn)
    log_rows = read_csv(args.load_log) if args.load_log else load_log_rows_from_db(conn)
    dsn = warehouse_dsn()
    return {
        "generated_at": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M %z"),
        "warehouse": dsn.rsplit("@", 1)[-1], "days_loaded": days_loaded, "note": args.note or "",
        "ingest": ingest_summary(ingest_rows), "ingest_source": args.ingest_runs and Path(args.ingest_runs).name or "raw.ingest_runs",
        "load_log": load_log_summary(log_rows), "load_log_source": args.load_log and Path(args.load_log).name or "clean.load_log",
        "tasks": task_summary(read_csv(args.task_runtimes)) if args.task_runtimes else [],
        "tables": sizes, "schema_bytes": schema_bytes, "database_bytes": int(db_bytes),
        "per_day": per_day, "projection": projection, "cost": cost, "gz_files_measured": n_gz,
        "rows_per_day_summary": (f"{statistics.median(raw_rows):,.0f} raw rows a day (median of {len(raw_rows)} days)"
                                 if raw_rows else "no raw rows"),
        "restore": restore_test(dsn, Path(args.restore_file), args.with_transform) if args.restore_file else None,
    }


def main(argv=None):
    from transform.db import connect
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw-dir", default=str(REPO / "data" / "raw" / "mcf"), help="folder of <date>.jsonl.gz files")
    ap.add_argument("--lake", default=str(REPO / "data" / "lake"))
    ap.add_argument("--ingest-runs", help="CSV export of raw.ingest_runs (default: read the table)")
    ap.add_argument("--load-log", help="CSV export of clean.load_log (default: read the table)")
    ap.add_argument("--task-runtimes", help="CSV export of Airflow task tries (dag_id, task_id, state, try_number, duration_s)")
    ap.add_argument("--restore-file", help="a <date>.jsonl.gz to restore into a scratch database, timed")
    ap.add_argument("--with-transform", action="store_true", help="also rebuild the clean tables for that day")
    ap.add_argument("--note", help="one line printed under the heading, e.g. which data this is")
    ap.add_argument("--doc", default=str(DOC), help="markdown file whose GENERATED block is rewritten")
    ap.add_argument("--json", help="also write the raw numbers here")
    args = ap.parse_args(argv)
    conn = connect()
    try:
        report = build_report(conn, args)
    finally:
        conn.close()
    block = render(report)
    doc = Path(args.doc)
    doc.write_text(replace_generated(doc.read_text(encoding="utf-8") if doc.exists() else "", block), encoding="utf-8")
    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(block)
    return 0


if __name__ == "__main__":
    sys.exit(main())
