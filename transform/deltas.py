"""Day-over-day arithmetic for fact_job_day (pure, unit-tested in tests/test_deltas.py).

The API only gives running totals (views, applications). The increase since the previous snapshot of
the same posting is what the dashboard sums, so it is computed once here at load time and stored.
The previous snapshot is the latest earlier fact row of that posting, not always "yesterday": about
50 postings a run are missed while paging, and the first interval was 17 hours. hours_since_prev is
stored so rates can be expressed per 24 hours (delta * 24 / hours_since_prev).

Negative deltas are kept as they are (not clipped) so the data-quality check "counters never fall"
can see them.
"""
from datetime import datetime


def hours_between(later: datetime | None, earlier: datetime | None):
    if later is None or earlier is None:
        return None
    return round((later - earlier).total_seconds() / 3600.0, 2)


def compute_delta(cur: dict, prev: dict | None) -> dict:
    """cur and prev carry: snapshot_date, fetched_at, views_total, applications_total, repost_count,
    edit_count, new_posting_date. prev is None on the posting's first snapshot."""
    if prev is None:
        return {
            "prev_snapshot_date": None,
            "hours_since_prev": None,
            "is_first_snapshot": True,
            "views_delta": None,
            "applications_delta": None,
            "reposted_since_prev": None,
            "edited_since_prev": None,
        }
    moved = (
        cur.get("new_posting_date") is not None
        and prev.get("new_posting_date") is not None
        and cur["new_posting_date"] != prev["new_posting_date"]
    )
    return {
        "prev_snapshot_date": prev["snapshot_date"],
        "hours_since_prev": hours_between(cur.get("fetched_at"), prev.get("fetched_at")),
        "is_first_snapshot": False,
        "views_delta": cur["views_total"] - prev["views_total"],
        "applications_delta": cur["applications_total"] - prev["applications_total"],
        "reposted_since_prev": cur["repost_count"] > prev["repost_count"] or moved,
        "edited_since_prev": cur["edit_count"] > prev["edit_count"],
    }
