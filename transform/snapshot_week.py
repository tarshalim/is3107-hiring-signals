"""Which MCF snapshot a weekly_metrics run uses.

Each run covers one Airflow data interval, Tuesday 14:00 SGT to the next Tuesday 14:00 SGT, and uses the
LATEST snapshot taken in that week. It never takes max(snapshot_date) over the whole table: with catchup,
a backfilled run for an old week must compute that week's numbers, not today's.

Snapshots are labelled with the Singapore date and taken at about 10:00, so the snapshot of the Tuesday
that STARTS an interval is from before 14:00 and belongs to the previous week. A week is therefore the
snapshot dates after the start date, up to and including the end date (Wednesday to Tuesday).
"""
from datetime import timedelta, timezone

# Singapore has been UTC+8 all year since 1982 (no daylight saving), so a fixed offset is exact and needs
# no time zone database (Windows Python has none without the tzdata package).
SGT = timezone(timedelta(hours=8), "Asia/Singapore")


class NoSnapshotInWeek(ValueError):
    """The run's week has no MCF snapshot in clean.fact_job_day."""


def week_bounds(data_interval_start, data_interval_end):
    """(after, up_to) as Singapore dates. The week's snapshots are after < snapshot_date <= up_to."""
    return data_interval_start.astimezone(SGT).date(), data_interval_end.astimezone(SGT).date()


def latest_snapshot_in_week(cur, after, up_to):
    """Latest snapshot_date in the week (after, up_to]. Raises NoSnapshotInWeek if there is none."""
    cur.execute("SELECT max(snapshot_date) FROM clean.fact_job_day WHERE snapshot_date > %s AND snapshot_date <= %s",
                (after, up_to))
    snapshot_date = cur.fetchone()[0]
    if snapshot_date is None:
        raise NoSnapshotInWeek(f"No MCF snapshot in clean.fact_job_day for the week {after + timedelta(days=1)} "
                               f"to {up_to}: the daily ingest/transform did not load any day of that week.")
    return snapshot_date
