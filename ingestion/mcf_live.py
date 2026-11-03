"""Near-real-time feed: poll the first few pages of MyCareersFuture's newest-first list.

Same public endpoint, session (retries with backoff on 400/429/5xx) and page walk as the daily crawl in
ingestion/mcf_client.py. A poll only reads the first `max_pages` pages of OPEN postings in the API's
default order (newest first, by metadata.createdAt), with a pause between pages. It never looks up job IDs
one by one and never asks for closed postings.

Each posting becomes one slim row (counters plus the few fields the first-24-hour analysis groups by),
about 250 bytes instead of the 7.7 KB full JSON. dags/mcf_live_poll_dag.py runs a poll every 30 minutes:
fetch to a small gz file, append the rows to raw.mcf_live_poll, then check them.
"""
import gzip
import hashlib
import json
import time
from datetime import date, datetime, timezone
from pathlib import Path

from psycopg2.extras import execute_values

from ingestion.mcf_client import PAGE_SIZE, iter_jobs, make_session

DEFAULT_PAGES = 3        # 3 x 100 newest postings: about 1.5 to 2 hours of new postings at midday
DEFAULT_PAUSE = 2.0      # seconds between pages (the daily crawl uses 1 s)
TITLE_MAX = 200

# Column order of raw.mcf_live_poll after poll_ts (sql/init/06_live_poll.sql).
COLUMNS = ("job_uuid", "list_rank", "created_at", "updated_at", "new_posting_date", "original_posting_date",
           "repost_count", "edit_count", "views", "applications", "job_status", "ssoc_code", "category", "title")


def _iso_ts(value):
    """'2026-11-03T04:54:23.000Z' -> '2026-11-03T04:54:23+00:00'. Unreadable or missing -> None."""
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.isoformat()


def _iso_date(value):
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except ValueError:
        return None


def _int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text(value, limit=None):
    if value is None:
        return None
    text = " ".join(str(value).split())
    return (text[:limit] if limit else text) or None


def slim_posting(job: dict, rank: int) -> dict:
    """The fields of one posting that the live table keeps. `rank` is its position in the list (0 = newest)."""
    meta = job.get("metadata") or {}
    categories = job.get("categories") or []
    first_category = categories[0].get("category") if categories and isinstance(categories[0], dict) else None
    return {
        "job_uuid": job.get("uuid"),
        "list_rank": rank,
        "created_at": _iso_ts(meta.get("createdAt")),
        "updated_at": _iso_ts(meta.get("updatedAt")),
        "new_posting_date": _iso_date(meta.get("newPostingDate")),
        "original_posting_date": _iso_date(meta.get("originalPostingDate")),
        "repost_count": _int(meta.get("repostCount")),
        "edit_count": _int(meta.get("editCount")),
        "views": _int(meta.get("totalNumberOfView")),
        "applications": _int(meta.get("totalNumberJobApplication")),
        "job_status": _text((job.get("status") or {}).get("jobStatus")),
        "ssoc_code": _text(job.get("ssocCode")),
        "category": _text(first_category),
        "title": _text(job.get("title"), TITLE_MAX),
    }


def _totals(row: dict) -> tuple:
    return (row["views"] or 0, row["applications"] or 0)


def page_signature(jobs: list) -> str:
    """16 hex characters that change whenever the page's postings, their order or their counters change."""
    content = [(j.get("uuid"), (j.get("metadata") or {}).get("totalNumberOfView"),
                (j.get("metadata") or {}).get("totalNumberJobApplication")) for j in jobs]
    return hashlib.md5(json.dumps(content).encode("utf-8")).hexdigest()[:16]


def poll_newest(max_pages: int = DEFAULT_PAGES, pause: float = DEFAULT_PAUSE, session=None, now=None):
    """Fetch the first `max_pages` pages once. Returns (rows, stats).

    poll_ts is the time the poll started (UTC, whole seconds); every row of the poll carries it, so it
    is the poll's identity.

    The API caches each page (seen on 3 Nov 2026: page 0 unchanged for 53 minutes, and pages of different
    ages in one poll). So:
      - a posting on two pages is kept once, from the copy with the higher running totals (totals never
        fall, so that copy is the fresher one);
      - each page gets a short signature of what it held (uuids and counters, in order). A page whose
        signature equals an earlier poll's same page is a cached copy with no new information, and the
        metrics views leave its rows out of the curves.
    """
    session = session or make_session()
    poll_ts = (now or datetime.now(timezone.utc)).replace(microsecond=0)
    start = time.monotonic()
    rows, position, signatures = [], {}, []
    pages = duplicates = no_uuid = 0
    api_total = None
    for page, total, jobs in iter_jobs(session, max_pages=max_pages, pause=pause):
        pages += 1
        api_total = total
        signatures.append(page_signature(jobs))
        for i, job in enumerate(jobs):
            uuid = job.get("uuid")
            if not uuid:
                no_uuid += 1
                continue
            row = slim_posting(job, page * PAGE_SIZE + i)
            if uuid in position:
                duplicates += 1
                if _totals(row) > _totals(rows[position[uuid]]):
                    rows[position[uuid]] = row
                continue
            position[uuid] = len(rows)
            rows.append(row)
    created = [r["created_at"] for r in rows if r["created_at"]]
    stats = {
        "poll_ts": poll_ts.isoformat(),
        "pages_fetched": pages,
        "rows_fetched": len(rows),
        "duplicates_dropped": duplicates,
        "rows_without_uuid": no_uuid,
        "api_total": api_total,
        "newest_created_at": max(created, key=datetime.fromisoformat) if created else None,
        "oldest_created_at": min(created, key=datetime.fromisoformat) if created else None,
        "seconds_taken": round(time.monotonic() - start, 1),
        "page_signatures": signatures,
    }
    return rows, stats


def poll_file_path(base_dir: Path, poll_ts: str) -> Path:
    """data/raw/mcf_live/<UTC date>/<HHMMSS>.jsonl.gz, one small file per poll (for replay and backup)."""
    ts = datetime.fromisoformat(poll_ts).astimezone(timezone.utc)
    return Path(base_dir) / ts.strftime("%Y-%m-%d") / f"{ts.strftime('%H%M%S')}.jsonl.gz"


def write_poll_file(path: Path, rows: list) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return path


def read_poll_file(path: Path) -> list:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def load_poll(conn, stats: dict, rows: list) -> int:
    """Append one poll to raw.mcf_live_poll and record it in raw.mcf_live_poll_runs, in one transaction.

    Idempotent: the key is (poll_ts, job_uuid) and an observation never changes, so a retried load inserts
    nothing new (ON CONFLICT DO NOTHING). Returns how many rows the table holds for this poll afterwards.
    """
    poll_ts = stats["poll_ts"]
    values = [(poll_ts, *(row[c] for c in COLUMNS)) for row in rows]
    with conn:
        with conn.cursor() as cur:
            if values:
                execute_values(cur, f"""
                    INSERT INTO raw.mcf_live_poll (poll_ts, {", ".join(COLUMNS)})
                    VALUES %s
                    ON CONFLICT (poll_ts, job_uuid) DO NOTHING
                """, values, page_size=1000)
            cur.execute("""
                INSERT INTO raw.mcf_live_poll_runs (poll_ts, pages_fetched, rows_fetched, duplicates_dropped,
                                                    api_total, newest_created_at, oldest_created_at, seconds_taken,
                                                    page_signatures)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (poll_ts) DO UPDATE SET
                    pages_fetched = EXCLUDED.pages_fetched, rows_fetched = EXCLUDED.rows_fetched,
                    duplicates_dropped = EXCLUDED.duplicates_dropped, api_total = EXCLUDED.api_total,
                    newest_created_at = EXCLUDED.newest_created_at, oldest_created_at = EXCLUDED.oldest_created_at,
                    seconds_taken = EXCLUDED.seconds_taken, page_signatures = EXCLUDED.page_signatures,
                    finished_at = now()
            """, (poll_ts, stats["pages_fetched"], stats["rows_fetched"], stats["duplicates_dropped"],
                  stats["api_total"], stats["newest_created_at"], stats["oldest_created_at"], stats["seconds_taken"],
                  list(stats.get("page_signatures") or [])))
            cur.execute("SELECT count(*) FROM raw.mcf_live_poll WHERE poll_ts = %s", (poll_ts,))
            return cur.fetchone()[0]
