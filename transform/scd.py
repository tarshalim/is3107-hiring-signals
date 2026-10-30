"""Slowly changing dimension, Type 2, for dim_job: decide what to do with each posting seen today.

This is a pure function over two dictionaries (no database) so the versioning rules are unit-tested
offline, including the idempotency case: planning the same day twice must change nothing the
second time (L7: a task that gives the same result when run many times).

Vocabulary (see sql/init/02_clean_schema.sql, dim_job):
    attr_hash      hash of every tracked attribute of the posting (title, salary, skills ...)
    valid_from     first snapshot day a version was seen
    valid_to       last snapshot day it was seen, NULL while it is the current version
    last_seen_date last snapshot day the posting was seen with this version

Rules, for each posting in today's snapshot:
    no version yet                                    -> INSERT a version (valid_from = today)
    current version has the same hash                 -> TOUCH (last_seen_date = today, nothing else)
    hash differs, current version started today       -> UPDATE IN PLACE (a rerun of today with changed raw data)
    hash differs, current version started earlier     -> CLOSE it (valid_to = last day it was seen, capped at
                                                         yesterday) and INSERT a new version from today
    current version starts AFTER today                -> OUT OF ORDER: skip the dimension (facts still load
                                                         against whichever version covered that day)
"""
from dataclasses import dataclass, field
from datetime import date, timedelta


@dataclass(frozen=True)
class CurrentVersion:
    job_key: int
    attr_hash: str
    valid_from: date
    last_seen_date: date


@dataclass
class VersionPlan:
    insert: list = field(default_factory=list)           # job_uuids that need a brand-new version row
    close: list = field(default_factory=list)            # (job_key, valid_to) for versions superseded today
    update_in_place: list = field(default_factory=list)  # (job_uuid, job_key): same-day rerun, attributes changed
    touch: list = field(default_factory=list)            # job_keys unchanged today: only last_seen_date moves
    out_of_order: list = field(default_factory=list)     # job_uuids whose current version starts after today

    def counts(self) -> dict:
        return {
            "inserted": len(self.insert),
            "closed": len(self.close),
            "updated_in_place": len(self.update_in_place),
            "touched": len(self.touch),
            "out_of_order": len(self.out_of_order),
        }


def plan_versions(snapshot_date: date, staged_hashes: dict, current: dict) -> VersionPlan:
    """staged_hashes: job_uuid -> attr_hash of today's payload.
    current: job_uuid -> CurrentVersion (the is_current row), for postings that have one."""
    plan = VersionPlan()
    for job_uuid, new_hash in staged_hashes.items():
        cur = current.get(job_uuid)
        if cur is None:
            plan.insert.append(job_uuid)
        elif cur.valid_from > snapshot_date:
            plan.out_of_order.append(job_uuid)
        elif cur.attr_hash == new_hash:
            plan.touch.append(cur.job_key)
        elif cur.valid_from == snapshot_date:
            plan.update_in_place.append((job_uuid, cur.job_key))
        elif cur.last_seen_date > snapshot_date:
            # The current version was already seen on a later day than the one being (re)loaded.
            # Rewriting it would overlap versions, so leave the dimension alone.
            plan.out_of_order.append(job_uuid)
        else:
            valid_to = min(cur.last_seen_date, snapshot_date - timedelta(days=1))
            plan.close.append((cur.job_key, valid_to))
            plan.insert.append(job_uuid)
    return plan
