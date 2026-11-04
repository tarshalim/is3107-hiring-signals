"""Fetch job postings from the MyCareersFuture public API.

The API returns pages of postings. Each posting carries the fields the
dashboard needs (skills, salary, SSOC code, experience) plus the engagement
counters under `metadata` (views, applications, repost count) that we
snapshot every day.
"""
import gzip
import json
import logging
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.mycareersfuture.gov.sg/v2/jobs"
PAGE_SIZE = 100
USER_AGENT = "IS3107-student-project (NUS)"

log = logging.getLogger(__name__)


class LoggedRetry(Retry):
    """Retry that writes one warning line per retry, so the Airflow task log
    shows why a run was slow (429 rate limit vs. server errors)."""

    def increment(self, method=None, url=None, response=None, error=None, *args, **kwargs):
        reason = response.status if response is not None else repr(error)
        log.warning("MCF retry: %s (reason: %s)", url, reason)
        return super().increment(method, url, response, error, *args, **kwargs)


def make_session() -> requests.Session:
    """A session that retries rate limits and server errors with backoff."""
    # The API rate-limits (429) part-way through a full run, so wait up to ~6 min
    # in total before giving up; a failed page means the whole run restarts.
    # 400 is retried too: on 27 Oct the API returned a one-off 400 for a page that
    # loaded fine moments later, and that single error restarted the whole run.
    retry = LoggedRetry(
        total=8,
        backoff_factor=5,  # 5s, 10s, 20s, 40s, 80s, then capped at backoff_max
        backoff_max=120,
        status_forcelist=[400, 429, 500, 502, 503, 504],
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


def iter_jobs(session: requests.Session, max_pages: int = 0, pause: float = 1.0):
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
