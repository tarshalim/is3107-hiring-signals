"""Offline tests for the day-over-day arithmetic (transform/deltas.py)."""
from datetime import date, datetime, timezone

from transform.deltas import compute_delta, hours_between

T1 = datetime(2026, 10, 24, 9, 35, tzinfo=timezone.utc)    # 17:35 SGT
T2 = datetime(2026, 10, 25, 2, 23, tzinfo=timezone.utc)    # 10:23 SGT, about 17 hours later


def row(d, fetched_at, views, apps, repost=0, edit=0, new_posting=date(2026, 10, 1)):
    return {"snapshot_date": d, "fetched_at": fetched_at, "views_total": views, "applications_total": apps,
            "repost_count": repost, "edit_count": edit, "new_posting_date": new_posting}


def test_first_snapshot_has_null_deltas():
    d = compute_delta(row(date(2026, 10, 24), T1, 10, 1), None)
    assert d["is_first_snapshot"] is True
    assert d["views_delta"] is None and d["applications_delta"] is None
    assert d["prev_snapshot_date"] is None and d["hours_since_prev"] is None


def test_delta_against_previous_snapshot_with_hours():
    prev = row(date(2026, 10, 24), T1, 100, 5)
    cur = row(date(2026, 10, 25), T2, 112, 7)
    d = compute_delta(cur, prev)
    assert d["is_first_snapshot"] is False
    assert d["views_delta"] == 12 and d["applications_delta"] == 2
    assert d["prev_snapshot_date"] == date(2026, 10, 24)
    assert d["hours_since_prev"] == 16.8          # the first interval was 17 h, not 24
    assert d["reposted_since_prev"] is False and d["edited_since_prev"] is False


def test_repost_detected_by_counter_or_by_posting_date_moving():
    prev = row(date(2026, 10, 24), T1, 100, 5, repost=0)
    assert compute_delta(row(date(2026, 10, 25), T2, 100, 5, repost=1), prev)["reposted_since_prev"] is True
    moved = row(date(2026, 10, 25), T2, 100, 5, repost=0, new_posting=date(2026, 10, 25))
    assert compute_delta(moved, prev)["reposted_since_prev"] is True
    assert compute_delta(row(date(2026, 10, 25), T2, 100, 5, edit=1), prev)["edited_since_prev"] is True


def test_negative_delta_is_kept_for_the_quality_check():
    d = compute_delta(row(date(2026, 10, 25), T2, 90, 5), row(date(2026, 10, 24), T1, 100, 5))
    assert d["views_delta"] == -10


def test_hours_between_handles_missing_timestamps():
    assert hours_between(None, T1) is None
    assert hours_between(T2, T1) == 16.8
