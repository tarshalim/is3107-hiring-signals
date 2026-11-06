"""Checks on a daily snapshot before it is loaded into the warehouse.

snapshot_problem: is the crawl complete enough to load (the ingest DAG's check_snapshot)?
schema drift:     did the API's JSON shape change since the last stored day (the ingest DAG's check_schema_drift)?
"""
from datetime import date

from psycopg2.extras import execute_values

from quality.clean_checks import Result


def snapshot_problem(stats: dict, trial_run: bool, min_jobs: int, min_coverage: float):
    """Return a message describing what is wrong with the snapshot, or None if it looks fine.

    Trial runs (MCF_MAX_PAGES > 0) fetch only a few pages on purpose, so they skip the checks.
    """
    if trial_run:
        return None
    n, total = stats["jobs_fetched"], stats["api_total"]
    if n < min_jobs:
        return f"Only {n} postings fetched; expected at least {min_jobs}"
    if total and n < min_coverage * total:
        return f"Fetched {n} of {total} postings ({n / total:.0%})"
    return None


# ---------------------------------------------------------------------------------------------
# Schema drift (wk2 [53:04]: when a source changes its schema, "in real life, you wouldn't know").
#
# Every field path of every posting in the day's file, with the JSON types seen, is compared with the last
# stored day in ops.schema_fields. A new path, a missing path or a path whose type changed (e.g. salary.minimum
# from number to string) becomes one 'warn' row, raw_schema_drift, in clean.dq_results next to the transform's
# checks. Warn, not fail: a new field breaks nothing (parse.py reads named fields), and a missing or retyped field
# that matters also shows up in the transform's own checks (missing core attributes, rejected rows), so the day
# still loads and the team gets told what changed.
#
# Whole file, not a sample: the rarest path on 24 Oct (schemes[].subScheme.*) is in 0.7% of postings, and reading
# all ~90,000 postings takes about 22 s, so a sample would only add the risk of a rare path looking "missing".
# ---------------------------------------------------------------------------------------------
DRIFT_SOURCE = "mcf_jobs"
DRIFT_CHECK = "raw_schema_drift"
DRIFT_SAMPLE = 0             # postings to read per day; 0 = all of them
DETAILS_MAX = 1000           # characters of the dq_results details text
LIST_SHOWN = 8               # paths named per kind of change in the details text


def _json_type(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):          # before int: bool is an int subclass in Python
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return type(value).__name__


def posting_paths(value, prefix: str = "", out: dict | None = None) -> dict:
    """{field path: set of JSON types} for one posting. Keys are joined with '.', list items add '[]', so all
    skills share one path: 'skills[].skill'. A key that is present with null still counts as present."""
    out = {} if out is None else out
    if isinstance(value, dict):
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            out.setdefault(path, set()).add(_json_type(item))
            posting_paths(item, path, out)
    elif isinstance(value, list):
        path = prefix + "[]"
        for item in value:
            out.setdefault(path, set()).add(_json_type(item))
            posting_paths(item, path, out)
    return out


def field_stats(postings, limit: int = DRIFT_SAMPLE):
    """({path: {"postings": n, "types": set}}, postings read) over an iterable of postings."""
    stats, n = {}, 0
    for posting in postings:
        for path, types in posting_paths(posting).items():
            entry = stats.setdefault(path, {"postings": 0, "types": set()})
            entry["postings"] += 1
            entry["types"] |= types
        n += 1
        if limit and n >= limit:
            break
    return stats, n


def compare_fields(previous: dict, current: dict) -> dict:
    """What changed between two days' field stats. Types are compared without 'null' (a field that happens to be
    null all day is not a type change), and only when both days saw a non-null value."""
    retyped = []
    for path in sorted(previous.keys() & current.keys()):
        before, after = set(previous[path]["types"]) - {"null"}, set(current[path]["types"]) - {"null"}
        if before and after and before != after:
            retyped.append((path, sorted(before), sorted(after)))
    return {"added": sorted(current.keys() - previous.keys()),
            "removed": sorted(previous.keys() - current.keys()),
            "retyped": retyped}


def _named(paths, counts) -> str:
    shown = [f"{p} ({counts[p]['postings']} postings)" for p in paths[:LIST_SHOWN]]
    more = len(paths) - LIST_SHOWN
    return ", ".join(shown) + (f" and {more} more" if more > 0 else "")


def drift_result(previous: dict | None, current: dict, previous_date=None) -> Result:
    """The raw_schema_drift verdict for clean.dq_results (pure)."""
    base = dict(name=DRIFT_CHECK, category="schema", severity="warn", threshold=0.0)
    if previous is None:
        return Result(**base, status="warn", observed=None,
                      details=f"no earlier day in ops.schema_fields to compare with; stored {len(current)} paths "
                              "as the baseline")
    diff = compare_fields(previous, current)
    changes = len(diff["added"]) + len(diff["removed"]) + len(diff["retyped"])
    if not changes:
        return Result(**base, status="pass", observed=0.0, details=None)
    parts = [f"compared with {previous_date} ({len(previous)} paths, today {len(current)})"]
    if diff["added"]:
        parts.append(f"{len(diff['added'])} new: " + _named(diff["added"], current))
    if diff["removed"]:
        parts.append(f"{len(diff['removed'])} missing: " + _named(diff["removed"], previous))
    if diff["retyped"]:
        shown = [f"{p} {'|'.join(a)} -> {'|'.join(b)}" for p, a, b in diff["retyped"][:LIST_SHOWN]]
        parts.append(f"{len(diff['retyped'])} type changed: " + ", ".join(shown))
    details = "; ".join(parts)
    if len(details) > DETAILS_MAX:
        details = details[: DETAILS_MAX - 15] + " ...[truncated]"
    return Result(**base, status="warn", observed=float(changes), details=details)


def drift_tables_ready(conn) -> bool:
    """False on a warehouse where sql/init/10_ops_schema_fields.sql has not been applied yet."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('ops.schema_fields') IS NOT NULL "
                    "AND to_regclass('clean.dq_results') IS NOT NULL")
        ready = bool(cur.fetchone()[0])
    conn.rollback()
    return ready


def record_schema_drift(conn, snapshot_date, current: dict, sample_size: int, source: str = DRIFT_SOURCE) -> dict:
    """Compare with the latest stored day before snapshot_date, store today's paths (delete-then-insert, so a
    rerun replaces them) and upsert the raw_schema_drift row in clean.dq_results. One transaction."""
    snapshot_date = date.fromisoformat(str(snapshot_date))
    with conn, conn.cursor() as cur:
        cur.execute("SELECT max(snapshot_date) FROM ops.schema_fields WHERE source = %s AND snapshot_date < %s",
                    (source, snapshot_date))
        previous_date = cur.fetchone()[0]
        previous = None
        if previous_date is not None:
            cur.execute("SELECT field_path, postings, json_types FROM ops.schema_fields "
                        "WHERE source = %s AND snapshot_date = %s", (source, previous_date))
            previous = {path: {"postings": n, "types": set(types)} for path, n, types in cur.fetchall()}
        result = drift_result(previous, current, previous_date)
        cur.execute("DELETE FROM ops.schema_fields WHERE source = %s AND snapshot_date = %s", (source, snapshot_date))
        execute_values(cur, """
            INSERT INTO ops.schema_fields (source, snapshot_date, field_path, postings, sample_size, json_types)
            VALUES %s
        """, [(source, snapshot_date, path, s["postings"], sample_size, sorted(s["types"]))
              for path, s in sorted(current.items())])
        cur.execute("""
            INSERT INTO clean.dq_results (snapshot_date, check_name, category, severity, status, observed, threshold, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_date, check_name) DO UPDATE SET
                category = EXCLUDED.category, severity = EXCLUDED.severity, status = EXCLUDED.status,
                observed = EXCLUDED.observed, threshold = EXCLUDED.threshold, details = EXCLUDED.details,
                checked_at = now()
        """, (snapshot_date, result.name, result.category, result.severity, result.status, result.observed,
              result.threshold, result.details))
    return {"snapshot_date": str(snapshot_date), "paths": len(current), "postings_read": sample_size,
            "previous_date": str(previous_date) if previous_date else None, "status": result.status,
            "changes": result.observed, "details": result.details}


def main(argv=None):
    """Build the drift history from saved files, oldest first (also how a missed day is filled in):
        python -m quality.snapshot_checks data/raw/mcf/2026-10-24.jsonl.gz data/raw/mcf/2026-10-25.jsonl.gz
        python -m quality.snapshot_checks "data/raw/mcf/*.jsonl.gz"     (quoted: expanded here, also in PowerShell)
    """
    import argparse
    import glob
    import json
    from pathlib import Path

    from ingestion.mcf_client import read_snapshot
    from transform.db import connect

    ap = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+", help="<date>.jsonl.gz files; the date comes from the file name")
    args = ap.parse_args(argv)
    conn = connect()
    try:
        if not drift_tables_ready(conn):
            raise SystemExit("ops.schema_fields is missing: apply sql/init/10_ops_schema_fields.sql first")
        files = {Path(f) for pattern in args.paths for f in (glob.glob(pattern) or [pattern])}
        for path in sorted(files, key=lambda p: p.name):
            stats, n = field_stats(read_snapshot(path))
            print(json.dumps(record_schema_drift(conn, path.name[:10], stats, n)))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
