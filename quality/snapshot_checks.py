"""Checks on a daily snapshot before it is loaded into the warehouse."""


def snapshot_problem(stats: dict, trial_run: bool, min_jobs: int, min_coverage: float):
    """Return a message describing what is wrong with the snapshot, or None if it looks fine.

    Trial runs (MCF_MAX_PAGES > 0) fetch only a few pages on purpose, so they skip the checks.
    """
    if trial_run:
        return None
    n, total = stats["jobs_fetched"], stats["api_total"]
    if n < min_jobs:
        return f"Only {n} postings fetched; expected at least {min_jobs}"
    if total and n < min_coverage * total:
        return f"Fetched {n} of {total} postings ({n / total:.0%})"
    return None
