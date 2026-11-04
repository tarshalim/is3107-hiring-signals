"""transform/run_context.py: which day a daily_mcf_transform run loads (review 4 Nov, #4)."""
from datetime import datetime, timezone

from transform.run_context import run_timings, snapshot_date_for_run


def test_scheduled_run_loads_its_own_singapore_day():
    # the 10:30 SGT run of 4 Nov: data_interval_end is 02:30 UTC that day
    assert snapshot_date_for_run("", datetime(2026, 11, 4, 2, 30, tzinfo=timezone.utc)) == "2026-11-04"


def test_cleared_old_run_reloads_that_runs_day_not_today():
    assert snapshot_date_for_run(None, datetime(2026, 10, 27, 2, 30, tzinfo=timezone.utc)) == "2026-10-27"


def test_utc_evening_is_already_the_next_singapore_day():
    assert snapshot_date_for_run("", datetime(2026, 10, 27, 17, 0, tzinfo=timezone.utc)) == "2026-10-28"


def test_run_config_wins():
    end = datetime(2026, 11, 4, 2, 30, tzinfo=timezone.utc)
    assert snapshot_date_for_run("2026-10-24", end) == "2026-10-24"


def _utc(h, m=0):
    return datetime(2026, 10, 28, h, m, tzinfo=timezone.utc)


def test_run_seconds_are_the_transform_only_not_the_sensor_wait():
    # 28 Oct: run starts 10:30 SGT, raw arrives 15:31 SGT, the transform itself takes 62.4 s
    t = run_timings(62.4, _utc(2, 30), _utc(7, 31), _utc(7, 35))
    assert t["seconds"] == 62.4
    assert t["sensor_wait_seconds"] == 5 * 3600 + 60
    assert t["run_wall_seconds"] == 5 * 3600 + 5 * 60


def test_missing_times_give_none_not_an_error():
    t = run_timings(10, None, None, _utc(3))
    assert t == {"seconds": 10, "sensor_wait_seconds": None, "run_wall_seconds": None}
