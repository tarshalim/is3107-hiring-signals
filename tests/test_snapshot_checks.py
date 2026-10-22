"""Offline tests for the snapshot checks run before loading."""
from quality.snapshot_checks import snapshot_problem


def check(jobs_fetched, api_total, trial_run=False):
    stats = {"jobs_fetched": jobs_fetched, "api_total": api_total}
    return snapshot_problem(stats, trial_run, min_jobs=100, min_coverage=0.9)


def test_full_snapshot_passes():
    assert check(95_000, 96_000) is None


def test_too_few_jobs_fails():
    assert "Only 50" in check(50, None)


def test_low_coverage_fails():
    assert "50%" in check(48_000, 96_000)


def test_trial_run_skips_checks():
    assert check(200, 96_000, trial_run=True) is None
