"""Pipeline health tab (dashboard/tabs/pipeline.py): the daily-crawl numbers, offline. The tab's queries run against
Postgres in tests/test_dashboard_marts.py."""
from datetime import date, timedelta
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("streamlit")
pytest.importorskip("plotly")

from dashboard.tabs.pipeline import RELIABILITY_HELP, ingest_reliability  # noqa: E402

REPO = Path(__file__).resolve().parent.parent


def _runs():
    """24 Oct to 7 Nov as on the warehouse: every day there, 27 and 28 Oct retried, 429s logged from 28 Oct."""
    days = [date(2026, 10, 24) + timedelta(days=i) for i in range(15)]
    attempts = {date(2026, 10, 24): 3, date(2026, 10, 27): 2, date(2026, 10, 28): 4}
    retries = dict(zip(days[4:], [0, 8, 5, 10, 5, 9, 0, 5, 9, 0, 4]))
    return pd.DataFrame({"snapshot_date": days, "fetch_attempts": [attempts.get(d, 1) for d in days],
                         "retries_429": [retries.get(d) for d in days]})


def test_reliability_counts_retried_days_as_reliable_but_lists_them():
    r = ingest_reliability(_runs())
    assert r["reliability"] == 1.0 and r["missing"] == 0 and r["expected_days"] == 15
    assert r["retried"]["snapshot_date"].tolist() == [date(2026, 10, 24), date(2026, 10, 27), date(2026, 10, 28)]
    assert r["first_try"] == 12 and r["attempts_known"] == 15
    assert r["retries_429"] == 55 and r["days_429_logged"] == 11      # unknown days are not counted as 0
    assert "even if Airflow had to retry it" in RELIABILITY_HELP


def test_a_missing_day_and_unrecorded_attempts():
    runs = _runs().drop(index=5).assign(fetch_attempts=None)          # 29 Oct missing, attempts not recorded
    r = ingest_reliability(runs)
    assert r["missing"] == 1 and r["reliability"] == pytest.approx(14 / 15)
    assert r["retried"].empty and r["attempts_known"] == 0


def test_the_transform_chart_leaves_out_the_whole_dag_run():
    """8 Nov check: dag_run was stacked on top of the steps (double counting) and its sensor waits (89,000 to
    262,000 s) flattened the rest."""
    src = (REPO / "dashboard" / "queries.py").read_text(encoding="utf-8")
    assert "AND l.step <> 'dag_run'" in src.split("def load_steps(")[1].split("def dq_results(")[0]
