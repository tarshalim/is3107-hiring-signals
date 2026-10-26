"""Run the daily transform outside Airflow: the same three steps the DAG runs, from the command line.

    python -m transform.run --date 2026-10-24            # transform + data-quality checks + run summary
    python -m transform.run --date 2026-10-24 --load-reference   # also (re)load the reference tables first

Exit code 1 when a 'fail' severity data-quality check fails, so a shell script or CI notices.
Used by docs/VERIFY-ME.md to reproduce every number without a running Airflow.
"""
import argparse
import json
import sys
import time
from datetime import date, datetime, timezone

from quality.clean_checks import failed_checks, run_checks, summarise
from transform.db import connect
from transform.load_reference import load_reference
from transform.skill_mapping import match_rate_report
from transform.transform_snapshot import transform_snapshot, write_run_summary


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", required=True, help="snapshot_date to load, YYYY-MM-DD")
    ap.add_argument("--load-reference", action="store_true", help="load data/reference/* into clean.ref_* first")
    ap.add_argument("--skip-dq", action="store_true")
    args = ap.parse_args(argv)
    snapshot_date = date.fromisoformat(args.date)
    run_id = f"cli-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"

    conn = connect()
    t0 = time.perf_counter()
    try:
        if args.load_reference:
            print("reference:", json.dumps(load_reference(conn), indent=2))
        stats = transform_snapshot(conn, snapshot_date, run_id=run_id)
        print("transform:", json.dumps(stats, indent=2, default=str))
        dq_summary = None
        if not args.skip_dq:
            results = run_checks(conn, snapshot_date)
            for r in results:
                print(f"  DQ {r.status.upper():4} {r.category:13} {r.name:40} observed={r.observed} threshold={r.threshold} {r.details or ''}")
            dq_summary = summarise(results)
            print("dq summary:", dq_summary)
        print("skill match rate:", json.dumps(match_rate_report(conn, snapshot_date), indent=2))
        write_run_summary(conn, run_id, snapshot_date, stats, dq_summary, time.perf_counter() - t0)
        if dq_summary and failed_checks(results):
            print("DATA QUALITY FAILED:", [r.name for r in failed_checks(results)])
            return 1
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
