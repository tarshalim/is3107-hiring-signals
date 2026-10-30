"""Replay a saved snapshot file (data/raw/mcf/<date>.jsonl.gz) into raw.mcf_job_snapshots.

Does exactly what the ingest DAG's load_raw task does (same upsert, same batching) but from a file
on disk, with no API call. This is the "mock replay" the lecturer said is fine: it lets teammates
rebuild the warehouse from the shared gz files, and lets this layer be verified without the live API.

    python -m ingestion.load_snapshot_file ../Admin/2026-10-24.jsonl.gz --snapshot-date 2026-10-24 \
        --ingest-runs ../Admin/ingest_runs.csv

--ingest-runs loads the collector's own run stats (pages, jobs_fetched, api_total, seconds, finished_at)
into raw.ingest_runs, and stamps fetched_at with that run's finish time, so the warehouse looks the
same as the collector's and the coverage and time-zone checks can run.
"""
import argparse
import csv
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path

from psycopg2.extras import execute_values

from ingestion.mcf_client import read_snapshot_batches
from transform.db import connect

BATCH_SIZE = 1000


def read_ingest_runs(path: Path) -> dict:
    """snapshot_date -> row dict from the collector's ingest_runs.csv export."""
    runs = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            runs[row["snapshot_date"]] = row
    return runs


def load_snapshot_file(conn, path: Path, snapshot_date: date, run: dict | None = None,
                       batch_size: int = BATCH_SIZE) -> dict:
    t0 = time.perf_counter()
    fetched_at = datetime.fromisoformat(run["finished_at"]) if run and run.get("finished_at") else None
    loaded = 0
    with conn:
        with conn.cursor() as cur:
            for batch in read_snapshot_batches(path, batch_size):
                rows = [(snapshot_date, job["uuid"], fetched_at, json.dumps(job)) if fetched_at
                        else (snapshot_date, job["uuid"], json.dumps(job)) for job in batch]
                if fetched_at:
                    execute_values(cur, """
                        INSERT INTO raw.mcf_job_snapshots (snapshot_date, job_uuid, fetched_at, payload)
                        VALUES %s
                        ON CONFLICT (snapshot_date, job_uuid)
                        DO UPDATE SET payload = EXCLUDED.payload, fetched_at = EXCLUDED.fetched_at
                    """, rows, page_size=batch_size)
                else:
                    execute_values(cur, """
                        INSERT INTO raw.mcf_job_snapshots (snapshot_date, job_uuid, payload)
                        VALUES %s
                        ON CONFLICT (snapshot_date, job_uuid)
                        DO UPDATE SET payload = EXCLUDED.payload, fetched_at = now()
                    """, rows, page_size=batch_size)
                loaded += len(rows)
            if run:
                cur.execute("""
                    INSERT INTO raw.ingest_runs (snapshot_date, pages_fetched, jobs_fetched, api_total, seconds_taken, finished_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (snapshot_date) DO UPDATE SET
                        pages_fetched = EXCLUDED.pages_fetched, jobs_fetched = EXCLUDED.jobs_fetched,
                        api_total = EXCLUDED.api_total, seconds_taken = EXCLUDED.seconds_taken,
                        finished_at = EXCLUDED.finished_at
                """, (snapshot_date, int(run["pages_fetched"]), int(run["jobs_fetched"]),
                      int(run["api_total"]) if run.get("api_total") else None,
                      float(run["seconds_taken"]), fetched_at))
    return {"snapshot_date": str(snapshot_date), "rows_loaded": loaded,
            "seconds": round(time.perf_counter() - t0, 1), "ingest_runs_row": bool(run)}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", help="<date>.jsonl.gz written by the ingest DAG")
    ap.add_argument("--snapshot-date", help="YYYY-MM-DD; default: the file name")
    ap.add_argument("--ingest-runs", help="ingest_runs.csv exported from the collector's raw.ingest_runs")
    args = ap.parse_args(argv)
    path = Path(args.path)
    snapshot_date = date.fromisoformat(args.snapshot_date or path.name[:10])
    run = read_ingest_runs(Path(args.ingest_runs)).get(str(snapshot_date)) if args.ingest_runs else None
    conn = connect()
    try:
        print(json.dumps(load_snapshot_file(conn, path, snapshot_date, run)))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
