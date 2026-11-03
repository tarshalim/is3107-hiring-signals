"""Offline tests for the week and snapshot arithmetic of the weekly signals (metrics/weeks.py), which follows
transform/snapshot_week.py: a run covers its data interval and uses the latest snapshot in it."""
from datetime import date, datetime, timedelta, timezone

import pytest

from metrics.weeks import gap_window, week_context
from transform import snapshot_week as sw

UTC = timezone.utc
DAYS = [date(2026, 10, 24) + timedelta(days=i) for i in range(10)]     # 24 Oct to 2 Nov, as collected


def _week(end_utc):
    """(after, up_to) of the run whose data interval ends at end_utc, as the DAG computes it."""
    return sw.week_bounds(end_utc - timedelta(days=7), end_utc)


def test_week_end_date_is_the_singapore_date_of_the_interval_end():
    # Tue 3 Nov 14:00 SGT is 06:00 UTC: the week is Wed 28 Oct to Tue 3 Nov
    after, up_to = _week(datetime(2026, 11, 3, 6, tzinfo=UTC))
    ctx = week_context(DAYS, after, up_to, date(2026, 11, 2))
    assert ctx["week_end_date"] == "2026-11-03" and ctx["week_start_date"] == "2026-10-28"


def test_snapshot_is_the_tuesday_when_it_was_transformed():
    after, up_to = _week(datetime(2026, 10, 27, 6, tzinfo=UTC))       # first run, week ending Tue 27 Oct
    ctx = week_context(DAYS, after, up_to, date(2026, 10, 27))
    assert ctx["snapshot_date"] == "2026-10-27" and ctx["snapshot_is_week_end"] is True
    assert ctx["fallback_reason"] is None
    assert ctx["snapshots_in_week"] == [str(date(2026, 10, 24) + timedelta(days=i)) for i in range(4)]
    assert ctx["first_snapshot_date"] == "2026-10-24"


def test_an_earlier_day_of_the_week_is_recorded_with_the_reason():
    # Tuesday 3 Nov not loaded yet when the run started: snapshot_week picks Monday 2 Nov
    after, up_to = _week(datetime(2026, 11, 3, 6, tzinfo=UTC))
    ctx = week_context(DAYS, after, up_to, date(2026, 11, 2))
    assert ctx["snapshot_date"] == "2026-11-02" and ctx["snapshot_is_week_end"] is False
    assert "2026-11-02" in ctx["fallback_reason"] and "1 day(s) earlier" in ctx["fallback_reason"]
    assert ctx["snapshots_in_week"][0] == "2026-10-28" and ctx["snapshots_in_week"][-1] == "2026-11-02"


def test_a_snapshot_outside_the_week_is_refused():
    after, up_to = _week(datetime(2026, 11, 3, 6, tzinfo=UTC))
    with pytest.raises(ValueError):
        week_context(DAYS, after, up_to, date(2026, 10, 27))          # the Tuesday that STARTS the interval
    with pytest.raises(ValueError):
        week_context(DAYS, after, up_to, date(2026, 11, 4))


def test_gap_window_counts_snapshots_not_calendar_days():
    assert gap_window(DAYS, date(2026, 10, 28)) == (date(2026, 10, 26), date(2026, 10, 30))
    # newest day: nothing after it yet, so a posting skipped that day cannot be confirmed (provisional)
    assert gap_window(DAYS, date(2026, 11, 2)) == (date(2026, 10, 31), date(2026, 11, 2))
    assert gap_window(DAYS, date(2026, 10, 24)) == (date(2026, 10, 24), date(2026, 10, 26))
    no_27 = [d for d in DAYS if d != date(2026, 10, 27)]
    assert gap_window(no_27, date(2026, 10, 28)) == (date(2026, 10, 25), date(2026, 10, 30))
    assert gap_window(DAYS, date(2026, 10, 28), max_gap=1) == (date(2026, 10, 27), date(2026, 10, 29))


def test_a_backfilled_week_sees_the_later_days_for_the_crawl_skip_window():
    # catchup run for the week ending 27 Oct, executed on 3 Nov: 28 and 29 Oct can confirm a skip on 27 Oct
    after, up_to = _week(datetime(2026, 10, 27, 6, tzinfo=UTC))
    ctx = week_context(DAYS, after, up_to, date(2026, 10, 27))
    assert (ctx["pool_from_date"], ctx["pool_to_date"]) == ("2026-10-25", "2026-10-29")
