"""Fetch MOM job-vacancy statistics from the data.gov.sg API.

Dataset: "Number of Job Vacancy by Occupation - Quarterly" (MOM Labour Market Survey),
id d_86dffa3d28e4c3ee085c3f99abad6e9f. About 450 rows: one per quarter per occupation group,
1998-Q1 onwards, updated every quarter. Columns: quarter ("2026-Q2"), occupation (text),
job_vacancy (a number stored as text, rounded to the nearest 100).

The occupation column holds two different splits of the same total (see
data/reference/mom_occupation_labels.csv). Mapping to groups happens in transform/mom_vacancies.py;
this module only downloads the rows as the API returns them.

The API needs no key. It is paged with limit/offset, like the MCF API is paged with page numbers.
"""
import logging
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://data.gov.sg/api/action/datastore_search"
DATASET_ID = "d_86dffa3d28e4c3ee085c3f99abad6e9f"
PAGE_SIZE = 1000   # the whole dataset fits in one page today; the loop handles growth
USER_AGENT = "IS3107-student-project (NUS)"

log = logging.getLogger(__name__)


class LoggedRetry(Retry):
    """Retry that writes one warning line per retry, so the Airflow log shows why a run was slow."""

    def increment(self, method=None, url=None, response=None, error=None, *args, **kwargs):
        reason = response.status if response is not None else repr(error)
        log.warning("data.gov.sg retry: %s (reason: %s)", url, reason)
        return super().increment(method, url, response, error, *args, **kwargs)


def make_session() -> requests.Session:
    """A session that retries rate limits and server errors with backoff."""
    # data.gov.sg rate-limits requests without an API key (429), so back off and try again.
    # 400 is NOT retried here (unlike MCF): on this API it means a wrong dataset id, which won't fix itself.
    retry = LoggedRetry(
        total=6,
        backoff_factor=5,  # 5s, 10s, 20s, 40s, then capped at backoff_max
        backoff_max=60,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = USER_AGENT
    return session


def fetch_page(session, dataset_id: str, offset: int) -> dict:
    """One page of rows. Returns the API's `result` object: {"records": [...], "total": N, ...}."""
    params = {"resource_id": dataset_id, "limit": PAGE_SIZE, "offset": offset}
    resp = session.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    # The API answers 200 with success=false for some errors, so check the flag too.
    if not body.get("success"):
        raise RuntimeError(f"data.gov.sg returned an error for {dataset_id}: {body.get('error')}")
    return body["result"]


def fetch_all(session=None, dataset_id: str = DATASET_ID, pause: float = 1.0) -> dict:
    """Download every row of the dataset. Returns the rows plus run stats."""
    session = session or make_session()
    start = time.monotonic()
    rows, pages, api_total = [], 0, None
    while True:
        result = fetch_page(session, dataset_id, offset=len(rows))
        records = result.get("records", [])
        api_total = result.get("total")
        pages += 1
        rows.extend(records)
        if not records or (api_total is not None and len(rows) >= api_total):
            break
        time.sleep(pause)  # be polite to a public API
    return {
        "dataset_id": dataset_id,
        "rows": rows,
        "rows_fetched": len(rows),
        "api_total": api_total,
        "pages_fetched": pages,
        "seconds_taken": round(time.monotonic() - start, 1),
    }
