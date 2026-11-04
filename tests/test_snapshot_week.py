"""Offline tests for transform/snapshot_week.py: each weekly_metrics run must pick its own week's snapshot."""
from datetime import date, datetime, timedelta, timezone

import pytest

from transform import mom_vacancies as mv
from transform import snapshot_week as sw

UTC = timezone.utc
# Snapshot days in clean.fact_job_day, as on the warehouse on 3 Nov 2026 (24 Oct to 2 Nov).
LOADED = [date(2026, 10, 24) + timedelta(days=i) for i in range(10)]


class FakeCursor:
    """Answers the snapshot_week query from a list of dates; records every other statement."""

    def __init__(self, dates):
        self.dates, self.statements, self._result = dates, [], None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        if "max(snapshot_date)" in sql:
            after, up_to = params
            in_week = [d for d in self.dates if after < d <= up_to]
            self._result = (max(in_week) if in_week else None,)

    def fetchone(self):
        return self._result


def _interval(end_utc):
    """Airflow passes data intervals as UTC datetimes; each one is exactly a week long."""
    return end_utc - timedelta(days=7), end_utc


def _pick(end_utc, dates=LOADED):
    after, up_to = sw.week_bounds(*_interval(end_utc))
    return sw.latest_snapshot_in_week(FakeCursor(dates), after, up_to)


def test_week_bounds_use_singapore_dates():
    # Tue 27 Oct 14:00 SGT is 06:00 UTC; the week is Wed 21 Oct to Tue 27 Oct.
    assert sw.week_bounds(*_interval(datetime(2026, 10, 27, 6, tzinfo=UTC))) == (date(2026, 10, 20), date(2026, 10, 27))
    # 20:00 UTC on 26 Oct is already 27 Oct in Singapore.
    assert sw.week_bounds(*_interval(datetime(2026, 10, 26, 20, tzinfo=UTC)))[1] == date(2026, 10, 27)


def test_two_runs_pick_different_snapshots():
    first = _pick(datetime(2026, 10, 27, 6, tzinfo=UTC))    # backfilled run, week ending Tue 27 Oct
    second = _pick(datetime(2026, 11, 3, 6, tzinfo=UTC))    # week ending Tue 3 Nov, Tuesday not loaded yet
    assert first == date(2026, 10, 27)
    assert second == date(2026, 11, 2)                       # falls back to Monday, not to the table max
    assert first != second


def test_snapshot_of_the_starting_tuesday_belongs_to_the_previous_week():
    # Only 27 Oct is loaded: it is the last day of the week ending 27 Oct, not part of the week after.
    assert _pick(datetime(2026, 10, 27, 6, tzinfo=UTC), [date(2026, 10, 27)]) == date(2026, 10, 27)
    with pytest.raises(sw.NoSnapshotInWeek):
        _pick(datetime(2026, 11, 3, 6, tzinfo=UTC), [date(2026, 10, 27)])


def test_week_without_snapshot_fails_and_names_the_week():
    with pytest.raises(sw.NoSnapshotInWeek, match="2026-10-14 to 2026-10-20"):
        _pick(datetime(2026, 10, 20, 6, tzinfo=UTC))         # before our first snapshot (24 Oct)


class ReadyCursor:
    """Answers week_end_ready from the days with a 'dag_run' row and the days in fact_job_day."""

    def __init__(self, logged, loaded):
        self.logged, self.loaded = logged, loaded

    def execute(self, sql, params=None):
        day, _ = params
        self._result = (day in self.logged or any(d > day for d in self.loaded),)

    def fetchone(self):
        return self._result


def test_week_end_ready_waits_for_the_tuesday_run_to_finish():
    tue = date(2026, 11, 3)
    assert not sw.week_end_ready(ReadyCursor(logged=LOADED, loaded=LOADED), tue)    # Tuesday not loaded yet
    assert sw.week_end_ready(ReadyCursor(logged=[*LOADED, tue], loaded=[*LOADED, tue]), tue)


def test_week_end_ready_does_not_wait_for_a_tuesday_that_was_missed():
    # Tuesday 27 Oct never loaded, but 28 Oct is: waiting would never help
    logged = [d for d in LOADED if d != date(2026, 10, 27)]
    assert sw.week_end_ready(ReadyCursor(logged=logged, loaded=logged), date(2026, 10, 27))


def test_build_comparison_writes_nothing_for_a_missing_week():
    class FakeConn:
        def __init__(self):
            self.cur = FakeCursor(LOADED)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def cursor(self):
            return self.cur

    conn = FakeConn()
    with pytest.raises(sw.NoSnapshotInWeek):
        mv.build_comparison(conn, date(2026, 10, 13), date(2026, 10, 20))
    # Only the snapshot lookup ran: no DELETE and no INSERT on metrics.mcf_vs_mom_share.
    assert len(conn.cur.statements) == 1 and "max(snapshot_date)" in conn.cur.statements[0][0]
