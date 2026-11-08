"""Fill raw.ingest_runs.fetch_attempts and retries_429 for days crawled before 8 Nov 2026, from the Airflow task logs.

    python -m ops.ingest_retries logs/            # print what it would write
    python -m ops.ingest_retries logs/ --write    # and write it (only into rows where fetch_attempts is NULL)

From 8 Nov the ingest DAG records both itself. For earlier days the only record is the fetch_snapshot task log:
    logs/dag_id=daily_mcf_ingest/run_id=<run>/task_id=fetch_snapshot/attempt=<n>.log
The attempt that fetched the day prints its stats as JSON (snapshot_date, jobs_fetched, ...). Several runs can fetch
the same day (on 24 Oct a 2-page trial, a manual run and the scheduled run), so a log is matched to the stored row
by snapshot_date AND jobs_fetched: the run whose crawl was loaded.

retries_429 counts the "MCF retry: ... (reason: 429)" lines in that attempt. mcf_client.py logs retries only since
commit 6ebe9de (28 Oct 00:59 SGT), so earlier days get NULL (unknown), not 0.
"""
import argparse
import json
import re
from datetime import date
from pathlib import Path

RETRIES_LOGGED_FROM = date(2026, 10, 28)       # first snapshot crawled with the retry log lines
STATS_LINE = re.compile(r"INFO - (\{\"path\".*\"snapshot_date\".*\})\s*$")
RETRY_429 = "(reason: 429)"


def parse_task_logs(logs_dir: Path) -> list[dict]:
    """One dict per fetch_snapshot attempt that printed its stats: snapshot_date, jobs_fetched, the attempt number
    (fetch_attempts), its 429 retries (None before RETRIES_LOGGED_FROM) and the run_id."""
    found = []
    for task_dir in sorted(Path(logs_dir).glob("dag_id=daily_mcf_ingest/run_id=*/task_id=fetch_snapshot")):
        for log in sorted(task_dir.glob("attempt=*.log")):
            stats, retries = None, 0
            for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
                if RETRY_429 in line and "MCF retry" in line:
                    retries += 1
                m = STATS_LINE.search(line)
                if m:
                    stats = json.loads(m.group(1))
            if stats is None:          # a failed attempt: it fetched nothing that was kept
                continue
            day = date.fromisoformat(stats["snapshot_date"])
            found.append({"snapshot_date": day, "jobs_fetched": stats["jobs_fetched"],
                          "fetch_attempts": int(log.stem.split("=")[1]),
                          "retries_429": retries if day >= RETRIES_LOGGED_FROM else None,
                          "run_id": task_dir.parent.name.split("=", 1)[1]})
    return found


def backfill(conn, attempts: list[dict], write: bool = False) -> list[dict]:
    """The attempts that match a raw.ingest_runs row without fetch_attempts (same day, same jobs_fetched); with
    write, store them. Rows the DAG already filled are never touched."""
    matched = []
    with conn:
        with conn.cursor() as cur:
            for a in attempts:
                cur.execute("""SELECT 1 FROM raw.ingest_runs
                               WHERE snapshot_date = %s AND jobs_fetched = %s AND fetch_attempts IS NULL""",
                            (a["snapshot_date"], a["jobs_fetched"]))
                if cur.fetchone() is None:
                    continue
                matched.append(a)
                if write:
                    cur.execute("""UPDATE raw.ingest_runs SET fetch_attempts = %s, retries_429 = %s
                                   WHERE snapshot_date = %s AND jobs_fetched = %s AND fetch_attempts IS NULL""",
                                (a["fetch_attempts"], a["retries_429"], a["snapshot_date"], a["jobs_fetched"]))
    return matched


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("logs_dir", type=Path, help="the Airflow logs folder (the one docker-compose mounts)")
    ap.add_argument("--write", action="store_true", help="write the values (default: only print them)")
    args = ap.parse_args(argv)

    from transform.db import connect
    conn = connect()
    try:
        rows = backfill(conn, parse_task_logs(args.logs_dir), write=args.write)
    finally:
        conn.close()
    for r in sorted(rows, key=lambda r: r["snapshot_date"]):
        print(f"{r['snapshot_date']}  attempts={r['fetch_attempts']}  429s={r['retries_429']}  ({r['run_id']})")
    print(f"{len(rows)} days {'written' if args.write else 'matched (dry run, add --write)'}")


if __name__ == "__main__":
    main()
