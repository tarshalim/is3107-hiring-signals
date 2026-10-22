"""Fetch job postings from the MyCareersFuture public API.

The API returns pages of postings. Each posting carries the fields the
dashboard needs (skills, salary, SSOC code, experience) plus the engagement
counters under `metadata` (views, applications, repost count) that we
snapshot every day.
"""
import gzip
import json
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.mycareersfuture.gov.sg/v2/jobs"
PAGE_SIZE = 100
USER_AGENT = "IS3107-student-project (NUS)"


def make_session() -> requests.Session:
    """A session that retries rate limits and server errors with backoff."""
    retry = Retry(
        total=5,
        backoff_factor=2,  # 2s, 4s, 8s, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = USER_AGENT
    return session


def fetch_page(session: requests.Session, page: int, limit: int = PAGE_SIZE) -> dict:
    resp = session.get(BASE_URL, params={"limit": limit, "page": page}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def iter_jobs(session: requests.Session, max_pages: int = 0, pause: float = 0.5):
    """Yield (page_number, api_total, jobs) for each page until the API runs out.

    max_pages=0 means fetch everything.
    """
    page = 0
    while True:
        body = fetch_page(session, page)
        jobs = body.get("results", [])
        total = body.get("total")
        if not jobs:
            return
        yield page, total, jobs
        page += 1
        if max_pages and page >= max_pages:
            return
        if total is not None and page * PAGE_SIZE >= total:
            return
        time.sleep(pause)  # be polite to a public API


def snapshot_to_file(out_path: Path, max_pages: int = 0, session=None) -> dict:
    """Write every posting as one JSON line to a gzipped file. Returns run stats."""
    session = session or make_session()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    pages = jobs_written = 0
    api_total = None
    seen = set()
    with gzip.open(out_path, "wt", encoding="utf-8") as f:
        for _, total, jobs in iter_jobs(session, max_pages=max_pages):
            pages += 1
            api_total = total
            for job in jobs:
                uuid = job.get("uuid")
                # Postings can shift between pages while we paginate; keep the first copy.
                if not uuid or uuid in seen:
                    continue
                seen.add(uuid)
                f.write(json.dumps(job, ensure_ascii=False) + "\n")
                jobs_written += 1
    return {
        "path": str(out_path),
        "pages_fetched": pages,
        "jobs_fetched": jobs_written,
        "api_total": api_total,
        "seconds_taken": round(time.monotonic() - start, 1),
    }


def read_snapshot(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


def read_snapshot_batches(path: Path, size: int):
    """Yield lists of up to `size` postings, so a large file is never fully in memory."""
    batch = []
    for job in read_snapshot(path):
        batch.append(job)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch
