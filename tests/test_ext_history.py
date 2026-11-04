"""Offline tests for the coverage logic in transform/posting_history.py (the SQL part runs in the integration test)."""
from datetime import date, timedelta

import pytest

from transform.posting_history import coverage_class, day_coverage, monday, survival_share, week_rows

FIRST, LAST = date(2026, 10, 24), date(2026, 11, 2)      # a Saturday and a Monday, like the real snapshots
MIX = {7: 5, 14: 10, 21: 2, 30: 83}                      # share of periods by length, close to the measured one


def test_survival_share_by_gap():
    assert survival_share(MIX, 0) == 1.0
    assert survival_share(MIX, 7) == 1.0                  # every period lasts at least 7 days
    assert survival_share(MIX, 8) == pytest.approx(0.95)
    assert survival_share(MIX, 22) == pytest.approx(0.83)
    assert survival_share(MIX, 31) == 0.0
    assert survival_share({}, 5) is None


def test_day_coverage_includes_early_closures():
    assert day_coverage(FIRST + timedelta(days=3), FIRST, MIX, 0.01) == 1.0   # seen by the daily crawl
    assert day_coverage(FIRST - timedelta(days=10), FIRST, MIX, 0.0) == pytest.approx(0.95)
    assert day_coverage(FIRST - timedelta(days=10), FIRST, MIX, 0.01) == pytest.approx(0.95 * 0.99 ** 10)
    assert day_coverage(FIRST - timedelta(days=40), FIRST, MIX) == 0.0


@pytest.mark.parametrize("week, expected", [
    (date(2026, 10, 26), "observed"),
    (date(2026, 10, 19), "near_complete"),       # starts 5 days before 24 Oct
    (date(2026, 10, 12), "partial"),
    (date(2026, 9, 21), "partial"),              # 27 Sep is within 30 days of 24 Oct
    (date(2026, 9, 14), "survivors_only"),
    (date(2026, 11, 2), "in_progress"),          # runs past the last snapshot
])
def test_coverage_class(week, expected):
    assert coverage_class(week, FIRST, LAST) == expected


def test_week_rows_estimate_and_labels():
    first_by_day = {date(2026, 6, 3): 4, date(2026, 10, 14): 10, date(2026, 10, 27): 20}
    periods_by_day = {date(2026, 10, 14): 19, date(2026, 10, 27): 20, date(2026, 11, 2): 3}
    reposts_by_day = {date(2026, 10, 14): 9}
    weeks = week_rows(first_by_day, periods_by_day, reposts_by_day, FIRST, LAST, MIX, 0.0)
    by = {w["week_start"]: w for w in weeks}
    assert weeks[0]["week_start"] == monday(date(2026, 6, 3)) and weeks[-1]["week_start"] == date(2026, 11, 2)
    assert len(weeks) == (date(2026, 11, 2) - monday(date(2026, 6, 3))).days // 7 + 1   # empty weeks kept
    old = by[monday(date(2026, 6, 3))]
    assert old["coverage_class"] == "survivors_only" and old["first_postings"] == 4 and old["est_coverage"] is None
    oct12 = by[date(2026, 10, 12)]                    # 14 Oct is 10 days before: 95% of periods survive
    assert oct12["coverage_class"] == "partial" and oct12["reposts_started"] == 9
    assert oct12["periods_started_est"] == pytest.approx(20.0) and oct12["est_coverage"] == pytest.approx(0.95)
    oct26 = by[date(2026, 10, 26)]
    assert oct26["coverage_class"] == "observed" and oct26["est_coverage"] == 1.0
    nov2 = by[date(2026, 11, 2)]
    assert nov2["coverage_class"] == "in_progress" and nov2["est_coverage"] is None and nov2["periods_started"] == 3


def test_week_with_unobservable_days_has_no_estimate():
    """21 to 23 Sep are more than 30 days before 24 Oct: nothing that started then could be seen, so a total
    for the week of 21 Sep would silently cover only 4 of its 7 days."""
    weeks = week_rows({}, {date(2026, 9, 24): 5}, {}, FIRST, LAST, MIX)
    sep21 = weeks[0]
    assert sep21["week_start"] == date(2026, 9, 21) and sep21["coverage_class"] == "partial"
    assert sep21["periods_started"] == 5 and sep21["est_coverage"] is None and sep21["periods_started_est"] is None


def test_week_rows_empty():
    assert week_rows({}, {}, {}, FIRST, LAST, MIX) == []
