"""Run the weekly hiring signals outside Airflow: the same steps as the DAG's signal tasks, from the command line.

    python -m metrics.run --week-ending 2026-11-03                 # one week (Wed 28 Oct to Tue 3 Nov)
    python -m metrics.run --week-ending 2026-10-27 2026-11-03      # several weeks, oldest first
    python -m metrics.run                                          # the week ending today (SGT)

Same week rule as `python -m transform.mom_vacancies --week-ending`: the 7 snapshot days ending on the given
day, using the latest snapshot among them (transform/snapshot_week.py). Scheduled runs end on a Tuesday. A
week with no snapshot stops with an error. Rerunning a week replaces its rows. Exit code 1 when a 'fail'
severity metrics check fails, so a shell script or CI notices.
"""
import argparse
import json
import sys
from datetime import date, datetime, timedelta

from metrics.build_week import run_week
from transform.db import connect
from transform.snapshot_week import SGT


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--week-ending", nargs="+", type=date.fromisoformat, default=[datetime.now(SGT).date()],
                    help="last day of each 7-day snapshot week, YYYY-MM-DD (default: today, SGT)")
    args = ap.parse_args(argv)

    conn = connect()
    failed = False
    try:
        for up_to in sorted(args.week_ending):
            out = run_week(conn, up_to - timedelta(days=7), up_to)
            for r in out["checks"]:
                print(f"  CHECK {r.status.upper():4} {r.category:13} {r.name:34} observed={r.observed} threshold={r.threshold}")
            print("summary:", json.dumps({
                "week_end_date": out["week"]["week_end_date"], "snapshot_date": out["week"]["snapshot_date"],
                "fallback_reason": out["week"]["fallback_reason"], "status": out["status"],
                "rows": {s["table"]: s["rows"] for s in out["steps"]},
                "seconds": round(sum(s["seconds"] for s in out["steps"]), 1),
            }, indent=2, default=str))
            failed = failed or out["status"] != "ok"
    finally:
        conn.close()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
