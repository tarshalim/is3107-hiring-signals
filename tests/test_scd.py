"""Offline tests for the SCD Type 2 planner (transform/scd.py), including idempotency."""
from datetime import date

from transform.scd import CurrentVersion, plan_versions

D1, D2, D3 = date(2026, 10, 24), date(2026, 10, 25), date(2026, 10, 26)


def apply_in_memory(plan, snapshot_date, staged, current, next_key):
    """Mimic what transform_snapshot does to the dim_job table, on a dict. Returns the new current map."""
    current = dict(current)
    for job_key, valid_to in plan.close:
        for u, v in list(current.items()):
            if v.job_key == job_key:
                del current[u]                     # closed versions are no longer current
    for u, job_key in plan.update_in_place:
        v = current[u]
        current[u] = CurrentVersion(job_key, staged[u], v.valid_from, snapshot_date)
    for job_key in plan.touch:
        for u, v in list(current.items()):
            if v.job_key == job_key:
                current[u] = CurrentVersion(job_key, v.attr_hash, v.valid_from, max(v.last_seen_date, snapshot_date))
    for u in plan.insert:
        current[u] = CurrentVersion(next_key, staged[u], snapshot_date, snapshot_date)
        next_key += 1
    return current, next_key


def test_first_day_inserts_everything():
    plan = plan_versions(D1, {"a": "h1", "b": "h2"}, {})
    assert plan.insert == ["a", "b"] and not plan.close and not plan.touch
    assert plan.counts()["inserted"] == 2


def test_unchanged_posting_is_only_touched():
    current = {"a": CurrentVersion(1, "h1", D1, D1)}
    plan = plan_versions(D2, {"a": "h1"}, current)
    assert plan.touch == [1] and not plan.insert and not plan.close


def test_changed_posting_closes_old_and_inserts_new():
    current = {"a": CurrentVersion(1, "h1", D1, D1)}
    plan = plan_versions(D2, {"a": "h1-edited"}, current)
    assert plan.close == [(1, D1)]          # old version valid to the last day it was seen
    assert plan.insert == ["a"]


def test_gap_day_closes_at_last_seen_not_yesterday():
    current = {"a": CurrentVersion(1, "h1", D1, D1)}   # missed on D2, back changed on D3
    plan = plan_versions(D3, {"a": "h2"}, current)
    assert plan.close == [(1, D1)]


def test_same_day_rerun_with_changed_data_updates_in_place():
    current = {"a": CurrentVersion(5, "h1", D2, D2)}
    plan = plan_versions(D2, {"a": "h1-reingested"}, current)
    assert plan.update_in_place == [("a", 5)] and not plan.insert and not plan.close


def test_out_of_order_day_is_skipped():
    current = {"a": CurrentVersion(5, "h2", D3, D3)}     # version started on D3, we are reloading D2
    plan = plan_versions(D2, {"a": "h1"}, current)
    assert plan.out_of_order == ["a"] and not plan.insert and not plan.close
    current = {"a": CurrentVersion(5, "h1", D1, D3)}     # seen on D3 already, D2 reloaded with new hash
    plan = plan_versions(D2, {"a": "h9"}, current)
    assert plan.out_of_order == ["a"]


def test_idempotent_rerun_is_a_no_op():
    staged_d1 = {"a": "h1", "b": "h2"}
    current, nk = apply_in_memory(plan_versions(D1, staged_d1, {}), D1, staged_d1, {}, 1)
    staged_d2 = {"a": "h1", "b": "h2-edited", "c": "h3"}
    plan_d2 = plan_versions(D2, staged_d2, current)
    assert plan_d2.counts() == {"inserted": 2, "closed": 1, "updated_in_place": 0, "touched": 1, "out_of_order": 0}
    current2, nk = apply_in_memory(plan_d2, D2, staged_d2, current, nk)
    # Re-running D2 on the resulting state must change nothing: every posting is only touched.
    rerun = plan_versions(D2, staged_d2, current2)
    assert rerun.counts() == {"inserted": 0, "closed": 0, "updated_in_place": 0, "touched": 3, "out_of_order": 0}
    current3, _ = apply_in_memory(rerun, D2, staged_d2, current2, nk)
    assert current3 == current2
