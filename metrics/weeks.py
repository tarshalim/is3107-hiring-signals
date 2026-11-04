"""Which week, which snapshot, which postings: the date arithmetic of the weekly signals.

Pure functions (no database, no Airflow), unit-tested in tests/test_metrics_weeks.py.

The week and the snapshot are the weekly_metrics DAG's (transform/snapshot_week.py), so the signals and the
MOM comparison of one run always describe the same week from the same snapshot:
  - a run covers its data interval, Tuesday 14:00 SGT to the next Tuesday 14:00 SGT. As Singapore dates that
    is (after, up_to]: the seven snapshot days Wednesday to Tuesday.
  - the snapshot used is the LATEST one in that week (snapshot_week.latest_snapshot_in_week), normally the
    Tuesday itself. If Tuesday's transform was late, an earlier day of the week is used and the reason is
    recorded. A week with no snapshot raises snapshot_week.NoSnapshotInWeek and nothing is written.
  - the signal tables name the week by its last day, week_end_date = up_to, and record snapshot_date.

A posting the crawl skipped is not a closed posting (data check of 2 Nov: 570 to 1,030 open postings a
day were skipped on 27 to 29 Oct and came back one or two days later). So a posting missing on the
snapshot day still counts as open if it was seen in the MAX_GAP_SNAPSHOTS snapshots before AND in the
MAX_GAP_SNAPSHOTS snapshots after. Its numbers come from its latest sighting on or before the snapshot.
For the newest day no later snapshot exists yet, so that day is provisional: clearing the run after the
next day lands adds the skipped postings back.
"""
from datetime import date, datetime, timedelta

WEEK_DAYS = 7
MAX_GAP_SNAPSHOTS = 2       # a posting skipped by the crawl for up to this many snapshots in a row stays open


def to_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def gap_window(available, snapshot, max_gap: int = MAX_GAP_SNAPSHOTS):
    """(pool_from, pool_to): the snapshot max_gap places before and after `snapshot` in the list of loaded
    days (clipped at the ends). A posting missing on `snapshot` is still open if it was seen in
    [pool_from, snapshot) and in (snapshot, pool_to]."""
    days = sorted({to_date(d) for d in available})
    snapshot = to_date(snapshot)
    i = days.index(snapshot)
    return days[max(i - max_gap, 0)], days[min(i + max_gap, len(days) - 1)]


def week_context(available, after, up_to, snapshot, max_gap: int = MAX_GAP_SNAPSHOTS) -> dict:
    """Everything the builders need to know about one week, as JSON-friendly strings (it travels between
    Airflow tasks through XCom).

    after, up_to : the week as Singapore dates, snapshots after < d <= up_to (snapshot_week.week_bounds)
    snapshot     : the latest snapshot in the week (snapshot_week.latest_snapshot_in_week)
    available    : every snapshot day loaded in the clean layer (for the crawl-skip window)
    """
    after, up_to, snapshot = to_date(after), to_date(up_to), to_date(snapshot)
    if not after < snapshot <= up_to:
        raise ValueError(f"snapshot {snapshot} is not in the week {after + timedelta(days=1)} to {up_to}")
    days = sorted({to_date(d) for d in available} | {snapshot})
    start = after + timedelta(days=1)
    pool_from, pool_to = gap_window(days, snapshot, max_gap)
    reason = None
    if snapshot != up_to:
        reason = (f"no snapshot for {up_to} in the clean layer when the run started: used {snapshot}, "
                  f"the latest in the week ({(up_to - snapshot).days} day(s) earlier)")
    return {
        "week_end_date": str(up_to),
        "week_start_date": str(start),
        "snapshot_date": str(snapshot),
        "snapshot_is_week_end": snapshot == up_to,
        "fallback_reason": reason,
        "snapshots_in_week": [str(d) for d in days if start <= d <= snapshot],
        "pool_from_date": str(pool_from),
        "pool_to_date": str(pool_to),
        "first_snapshot_date": str(days[0]),
    }
