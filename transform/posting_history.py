"""Rebuild a weekly posting history from the posting dates inside our own snapshots.

We only collected 10 daily snapshots (24 Oct to 2 Nov 2026), but each posting carries its
original_posting_date, new_posting_date (moves forward on a repost) and expiry_date. The first snapshot
alone has original posting dates back to March 2026. This module turns that into:

    ext.posting_period              one row per posting period (first post or repost) seen in clean.dim_job
    ext.posting_history_week        new postings per week, with a coverage label
    ext.posting_history_skill_week  new postings per week per MCF skill tag (clean.dim_skill)
    ext.posting_history_ssoc_week   new postings per week per SSOC major group, with median advertised pay

    python -m transform.posting_history            # after the daily transform, rebuilds everything in seconds

SURVIVORSHIP, the caveat that must travel with every chart made from these tables: we can only see a
posting that was still open on the first snapshot (24 Oct) or later. A posting period lasts 7, 14, 21 or
30 days (expiry minus new posting date, measured), and employers also close postings early. So:

    observed        week starts on or after the first snapshot: every posting seen by the daily crawl
    near_complete   week starts at most 6 days before: every period was still open, minus early closures
    partial         some days within 30 days before: only the longer periods survived (est_coverage < 1)
    survivors_only  every day more than 30 days before: only jobs kept alive by reposting, i.e. the
                    hard-to-fill ones. Never read these weeks as demand.
    in_progress     the week runs past the last snapshot

est_coverage estimates the share of a week's posting periods that we can see: for a period starting g days
before the first snapshot, P(seen) = share of periods lasting at least g days (duration mix of periods that
started in the 6 days before the first snapshot, which are all still visible) x (1 - early close rate)^g.
The early close rate is measured from our own snapshots: postings that disappear for good while their
expiry date is still ahead. Weekly totals divide each day's count by its P(seen) (inverse probability
weighting), so periods_started_est is a rough "what the week really had". first_postings (by original
posting date) has at least this coverage, because reposting keeps some older jobs visible.

Rebuilt from scratch in one transaction (delete then insert), so a rerun gives the same tables.
"""
import argparse
import json
import sys
import time
from datetime import date, timedelta

MAX_PERIOD_DAYS = 30          # longest posting period seen (expiry - new posting date)
MIX_WINDOW_DAYS = 6           # periods that started this close to the first snapshot are all still visible
CLOSE_CONFIRM_DAYS = 2        # a posting must stay gone for 2 snapshots before it counts as closed


def monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def survival_share(mix: dict, gap: int):
    """Share of posting periods lasting at least `gap` days. mix = {duration_days: count}."""
    if gap <= 0:
        return 1.0
    total = sum(n for d, n in mix.items() if d is not None)
    if not total:
        return None
    return sum(n for d, n in mix.items() if d is not None and d >= gap) / total


def day_coverage(day: date, first_snapshot: date, mix: dict, early_close_rate: float = 0.0):
    """P(a period that started on `day` was still open on the first snapshot). 1.0 once the crawl runs daily."""
    gap = (first_snapshot - day).days
    if gap <= 0:
        return 1.0
    share = survival_share(mix, gap)
    if share is None:
        return None
    return share * (1.0 - early_close_rate) ** gap


def coverage_class(week_start: date, first_snapshot: date, last_snapshot: date) -> str:
    week_end = week_start + timedelta(days=6)
    if week_end > last_snapshot:
        return "in_progress"
    if week_start >= first_snapshot:
        return "observed"
    if (first_snapshot - week_start).days <= MIX_WINDOW_DAYS:
        return "near_complete"
    if (first_snapshot - week_end).days <= MAX_PERIOD_DAYS:
        return "partial"
    return "survivors_only"


def week_rows(first_by_day: dict, periods_by_day: dict, reposts_by_day: dict, first_snapshot: date,
              last_snapshot: date, mix: dict, early_close_rate: float = 0.0) -> list[dict]:
    """Daily counts -> one dict per week from the oldest date to the last snapshot's week."""
    days = set(first_by_day) | set(periods_by_day)
    if not days:
        return []
    out = []
    week = monday(min(days))
    while week <= monday(last_snapshot):
        span = [week + timedelta(days=i) for i in range(7)]
        periods = sum(periods_by_day.get(d, 0) for d in span)
        cls = coverage_class(week, first_snapshot, last_snapshot)
        est, estimable = 0.0, periods > 0 and cls != "in_progress"
        for d in span:
            p = day_coverage(d, first_snapshot, mix, early_close_rate)
            if not p:                 # a day nobody could have seen: the week total cannot be estimated
                estimable = False
                break
            est += periods_by_day.get(d, 0) / p
        out.append({
            "week_start": week,
            "first_postings": sum(first_by_day.get(d, 0) for d in span),
            "periods_started": periods,
            "reposts_started": sum(reposts_by_day.get(d, 0) for d in span),
            "coverage_class": cls,
            "est_coverage": round(periods / est, 3) if estimable and est else None,
            "periods_started_est": round(est, 1) if estimable else None,
        })
        week += timedelta(days=7)
    return out


# ---------------------------------------------------------------------------------------------
# SQL (ELT: the heavy grouping runs inside Postgres)
# ---------------------------------------------------------------------------------------------
INSERT_PERIODS = """
INSERT INTO ext.posting_period (job_uuid, new_posting_date, original_posting_date, is_repost, job_key,
    first_seen_date, last_seen_date, expiry_date, duration_days, ssoc_code, ssoc_major_group, mom_group, salary_mid)
SELECT DISTINCT ON (j.job_uuid, j.new_posting_date)
       j.job_uuid, j.new_posting_date,
       coalesce(j.original_posting_date, j.new_posting_date),
       j.new_posting_date > coalesce(j.original_posting_date, j.new_posting_date),
       j.job_key, s.first_seen, s.last_seen, j.expiry_date,
       (j.expiry_date - j.new_posting_date),
       j.ssoc_code, j.ssoc_major_group, j.mom_group, j.salary_mid
FROM clean.dim_job j
JOIN (SELECT job_uuid, new_posting_date, min(valid_from) AS first_seen, max(last_seen_date) AS last_seen
      FROM clean.dim_job WHERE new_posting_date IS NOT NULL GROUP BY 1, 2) s
  ON s.job_uuid = j.job_uuid AND s.new_posting_date = j.new_posting_date
ORDER BY j.job_uuid, j.new_posting_date, j.valid_from, j.job_key
"""

# The version a job was first seen with: its original posting date, skills and SSOC.
FIRST_VERSION = """
CREATE TEMP TABLE tmp_first_version ON COMMIT DROP AS
SELECT DISTINCT ON (job_uuid) job_uuid, job_key,
       coalesce(original_posting_date, new_posting_date) AS original_posting_date,
       ssoc_major_group, mom_group
FROM clean.dim_job
WHERE coalesce(original_posting_date, new_posting_date) IS NOT NULL
ORDER BY job_uuid, valid_from, job_key
"""

EARLY_CLOSE = """
WITH last AS (SELECT job_uuid, max(snapshot_date) AS last_seen FROM clean.fact_job_day GROUP BY 1),
latest AS (SELECT DISTINCT ON (job_uuid) job_uuid, expiry_date FROM clean.dim_job
           ORDER BY job_uuid, valid_from DESC, job_key DESC)
SELECT (SELECT count(*) FROM last JOIN latest USING (job_uuid)
        WHERE last.last_seen <= %(cut)s AND latest.expiry_date > last.last_seen + 1),
       (SELECT count(*) FROM clean.fact_job_day WHERE snapshot_date <= %(cut)s)
"""

INSERT_SKILL_WEEK = """
INSERT INTO ext.posting_history_skill_week (week_start, skill_key, first_postings, periods_started)
SELECT week_start, skill_key, sum(fp)::int, sum(ps)::int
FROM (
    SELECT date_trunc('week', fv.original_posting_date)::date AS week_start, b.skill_key, 1 AS fp, 0 AS ps
    FROM tmp_first_version fv JOIN clean.bridge_job_skill b ON b.job_key = fv.job_key
    UNION ALL
    SELECT date_trunc('week', p.new_posting_date)::date, b.skill_key, 0, 1
    FROM ext.posting_period p JOIN clean.bridge_job_skill b ON b.job_key = p.job_key
) x
GROUP BY 1, 2
"""

INSERT_SSOC_WEEK = """
INSERT INTO ext.posting_history_ssoc_week (week_start, ssoc_major_group, mom_group, first_postings,
    periods_started, median_salary_mid)
SELECT week_start, g, min(mom_group), sum(fp)::int, sum(ps)::int,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY sal) FILTER (WHERE ps = 1))::numeric, 2)
FROM (
    SELECT date_trunc('week', original_posting_date)::date AS week_start, coalesce(ssoc_major_group, 0) AS g,
           mom_group, 1 AS fp, 0 AS ps, NULL::numeric AS sal
    FROM tmp_first_version
    UNION ALL
    SELECT date_trunc('week', new_posting_date)::date, coalesce(ssoc_major_group, 0), mom_group, 0, 1, salary_mid
    FROM ext.posting_period
) x
GROUP BY 1, 2
"""


def build_posting_history(conn) -> dict:
    from psycopg2.extras import execute_values
    t0 = time.perf_counter()
    with conn, conn.cursor() as cur:
        cur.execute("SELECT min(snapshot_date), max(snapshot_date), count(DISTINCT snapshot_date) FROM clean.fact_job_day")
        first, last, n_days = cur.fetchone()
        if first is None:
            raise RuntimeError("clean.fact_job_day is empty: run the daily transform first")
        for table in ("posting_history_skill_week", "posting_history_ssoc_week", "posting_history_week",
                      "posting_period"):
            cur.execute(f"DELETE FROM ext.{table}")
        cur.execute(INSERT_PERIODS)
        periods = cur.rowcount
        cur.execute(FIRST_VERSION)

        cur.execute("""SELECT duration_days, count(*) FROM ext.posting_period
                       WHERE new_posting_date BETWEEN %s AND %s GROUP BY 1""",
                    (first - timedelta(days=MIX_WINDOW_DAYS), first))
        mix = {d: n for d, n in cur.fetchall() if d is not None}

        cut = last - timedelta(days=CLOSE_CONFIRM_DAYS)
        early_close_rate = None
        if cut > first:
            cur.execute(EARLY_CLOSE, {"cut": cut})
            closed, exposure = cur.fetchone()
            early_close_rate = closed / exposure if exposure else None

        cur.execute("SELECT original_posting_date, count(*) FROM tmp_first_version GROUP BY 1")
        first_by_day = dict(cur.fetchall())
        cur.execute("SELECT new_posting_date, count(*), count(*) FILTER (WHERE is_repost) FROM ext.posting_period GROUP BY 1")
        periods_by_day, reposts_by_day = {}, {}
        for d, n, r in cur.fetchall():
            periods_by_day[d], reposts_by_day[d] = n, r

        weeks = week_rows(first_by_day, periods_by_day, reposts_by_day, first, last, mix, early_close_rate or 0.0)
        execute_values(cur, """
            INSERT INTO ext.posting_history_week (week_start, first_postings, periods_started, reposts_started,
                coverage_class, est_coverage, periods_started_est, first_snapshot, last_snapshot, early_close_rate)
            VALUES %s
        """, [(w["week_start"], w["first_postings"], w["periods_started"], w["reposts_started"], w["coverage_class"],
               w["est_coverage"], w["periods_started_est"], first, last,
               round(early_close_rate, 5) if early_close_rate is not None else None) for w in weeks])
        cur.execute(INSERT_SKILL_WEEK)
        skill_rows = cur.rowcount
        cur.execute(INSERT_SSOC_WEEK)
        ssoc_rows = cur.rowcount
        cur.execute("SELECT count(*) FROM tmp_first_version")
        jobs = cur.fetchone()[0]
    mix_total = sum(mix.values())
    return {
        "first_snapshot": str(first), "last_snapshot": str(last), "snapshot_days": n_days,
        "jobs": jobs, "posting_periods": periods, "weeks": len(weeks),
        "oldest_week": str(weeks[0]["week_start"]) if weeks else None,
        "weeks_by_class": {c: sum(1 for w in weeks if w["coverage_class"] == c)
                           for c in ("observed", "near_complete", "partial", "survivors_only", "in_progress")},
        "duration_mix": {int(d): round(n / mix_total, 4) for d, n in sorted(mix.items())} if mix_total else {},
        "early_close_rate_per_day": round(early_close_rate, 5) if early_close_rate is not None else None,
        "skill_week_rows": skill_rows, "ssoc_week_rows": ssoc_rows,
        "seconds": round(time.perf_counter() - t0, 1),
    }


def main(argv=None):
    from transform.db import connect
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.parse_args(argv)
    conn = connect()
    try:
        print(json.dumps(build_posting_history(conn), indent=2, default=str))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
