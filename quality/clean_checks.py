"""Data-quality checks on the clean layer for one snapshot day.

L7 taught validation as three kinds: schema (the data has the shape we expect), range (values are
inside sensible limits) and business rules (the data behaves the way the real world does). Each check
below is tagged with one of those categories, runs one SQL statement that returns ONE number, and is
judged against a threshold. Results go to clean.dq_results (a proper store, not only the Airflow log),
so the dashboard can show which days passed and the report can quote the numbers.

Severity decides what the DAG does (quality/clean_checks.py is called by run_quality_checks):
    fail  : the day's data cannot be trusted (duplicate versions, counts that do not add up, coverage
            under 90%, more than 1% of counters falling). The task raises, the DAG run goes red, the
            team gets a clear signal. The data stays loaded so the problem can be inspected.
    warn  : a real-world oddity worth a look but not a reason to stop (a few counters fell by 1, a
            posting asks for 88 years, a replayed file was loaded on a later date). Recorded only.
    info  : a number we want on the record every day (skill match rate, how many postings are new).

Why not drop bad rows instead? The lecturer's point: clean data leaves "nothing to show". Flagging
keeps the evidence (exp_out_of_range, rejected_rows) and lets the dashboard decide what to exclude.

The pure parts (evaluate, summarise) are unit-tested offline; run_checks is tested with a fake
connection (tests/test_clean_checks.py) and for real in tests/test_integration_warehouse.py.
"""
from dataclasses import dataclass

from psycopg2.extras import execute_values


@dataclass(frozen=True)
class Check:
    name: str
    category: str      # 'schema' | 'range' | 'business_rule' | 'info'
    severity: str      # 'fail' | 'warn' | 'info'
    sql: str           # returns one numeric value; %(d)s is the snapshot_date
    op: str            # how observed must relate to threshold to PASS: '<=', '>=', '=='
    threshold: float
    description: str


@dataclass(frozen=True)
class Result:
    name: str
    category: str
    severity: str
    status: str        # 'pass' | 'warn' | 'fail'
    observed: float | None
    threshold: float
    details: str | None


# Shorthands used in several checks: the fact rows of the day joined to the dim_job version live that day.
_DAY_JOBS = """
    FROM clean.fact_job_day f JOIN clean.dim_job j ON j.job_key = f.job_key
    WHERE f.snapshot_date = %(d)s
"""

CHECKS = [
    # ---------------- schema: the data has the shape we expect ----------------
    Check("raw_rows_present", "schema", "fail",
          "SELECT count(*) FROM raw.mcf_job_snapshots WHERE snapshot_date = %(d)s",
          ">=", 100, "the raw layer has at least 100 postings for the day"),
    Check("fact_plus_rejected_equals_raw", "schema", "fail",
          """SELECT (SELECT count(*) FROM raw.mcf_job_snapshots WHERE snapshot_date = %(d)s)
                  - (SELECT count(*) FROM clean.fact_job_day WHERE snapshot_date = %(d)s)
                  - (SELECT count(*) FROM clean.rejected_rows WHERE snapshot_date = %(d)s)""",
          "==", 0, "every raw posting became exactly one fact row or one rejected row"),
    Check("rejected_rows", "schema", "warn",
          "SELECT count(*) FROM clean.rejected_rows WHERE snapshot_date = %(d)s",
          "<=", 0, "payloads that could not become a fact row (dead letter table)"),
    Check("dim_job_one_current_per_uuid", "schema", "fail",
          """SELECT count(*) FROM (SELECT job_uuid FROM clean.dim_job WHERE is_current
                                   GROUP BY job_uuid HAVING count(*) > 1) t""",
          "==", 0, "SCD2 invariant: at most one current version per posting"),
    Check("dim_job_overlapping_versions", "schema", "fail",
          """SELECT count(*) FROM clean.dim_job a JOIN clean.dim_job b
                ON a.job_uuid = b.job_uuid AND a.job_key <> b.job_key AND a.valid_from < b.valid_from
             WHERE a.valid_to IS NULL OR a.valid_to >= b.valid_from""",
          "==", 0, "SCD2 invariant: version date ranges of one posting never overlap"),
    Check("fact_version_covers_day", "schema", "fail",
          """SELECT count(*) """ + _DAY_JOBS + """
               AND NOT (j.valid_from <= f.snapshot_date AND (j.valid_to IS NULL OR j.valid_to >= f.snapshot_date))""",
          "==", 0, "each fact row points at the dim_job version that was live that day"),
    Check("dim_job_missing_core_attrs", "schema", "warn",
          """SELECT count(*) """ + _DAY_JOBS + """
               AND (j.ssoc_code IS NULL OR j.new_posting_date IS NULL OR j.expiry_date IS NULL OR j.position_level IS NULL)""",
          "<=", 0, "postings with no SSOC code, posting date, expiry date or position level"),
    Check("versions_without_skills", "schema", "warn",
          """SELECT count(*) FROM clean.dim_job j WHERE j.valid_from = %(d)s
               AND NOT EXISTS (SELECT 1 FROM clean.bridge_job_skill b WHERE b.job_key = j.job_key)""",
          "<=", 0, "today's new or changed versions that have no skill rows"),

    # ---------------- range: values inside sensible limits ----------------
    Check("counters_negative", "range", "fail",
          "SELECT count(*) FROM clean.fact_job_day WHERE snapshot_date = %(d)s AND (views_total < 0 OR applications_total < 0)",
          "==", 0, "views and applications are never negative"),
    Check("experience_out_of_range", "range", "warn",
          "SELECT count(*) " + _DAY_JOBS + " AND j.exp_out_of_range",
          "<=", 0, "years of experience outside 0..40 (capped at 40 and flagged, e.g. the posting asking 88 years)"),
    Check("experience_out_of_range_share", "range", "fail",
          "SELECT avg(CASE WHEN j.exp_out_of_range THEN 1.0 ELSE 0.0 END) " + _DAY_JOBS,
          "<=", 0.01, "more than 1% out of range would mean the field changed meaning"),
    Check("salary_min_gt_max", "range", "warn",
          "SELECT count(*) " + _DAY_JOBS + " AND j.salary_min > j.salary_max",
          "<=", 0, "salary minimum above maximum"),
    Check("salary_missing", "range", "warn",
          "SELECT count(*) " + _DAY_JOBS + " AND (j.salary_min IS NULL OR j.salary_max IS NULL)",
          "<=", 0, "postings with no salary range"),
    Check("dates_inconsistent", "range", "warn",
          """SELECT count(*) """ + _DAY_JOBS + """
               AND (j.expiry_date < j.new_posting_date OR j.new_posting_date > f.snapshot_date
                    OR j.original_posting_date > j.new_posting_date)""",
          "<=", 0, "expiry before posting, posting date in the future, or original after new posting date"),

    # ---------------- business rules: the data behaves like the real world ----------------
    Check("views_decreased_count", "business_rule", "warn",
          "SELECT count(*) FROM clean.fact_job_day WHERE snapshot_date = %(d)s AND views_delta < 0",
          "<=", 0, "running view totals should never fall day over day"),
    Check("views_decreased_share", "business_rule", "fail",
          """SELECT avg(CASE WHEN views_delta < 0 THEN 1.0 ELSE 0.0 END)
             FROM clean.fact_job_day WHERE snapshot_date = %(d)s AND NOT is_first_snapshot""",
          "<=", 0.01, "more than 1% falling would mean the counters were reset or the days are mislabelled"),
    Check("applications_decreased_count", "business_rule", "warn",
          "SELECT count(*) FROM clean.fact_job_day WHERE snapshot_date = %(d)s AND applications_delta < 0",
          "<=", 0, "running application totals should never fall day over day"),
    Check("applications_decreased_share", "business_rule", "fail",
          """SELECT avg(CASE WHEN applications_delta < 0 THEN 1.0 ELSE 0.0 END)
             FROM clean.fact_job_day WHERE snapshot_date = %(d)s AND NOT is_first_snapshot""",
          "<=", 0.01, "see views_decreased_share"),
    Check("repost_count_decreased", "business_rule", "warn",
          """SELECT count(*) FROM clean.fact_job_day f JOIN clean.fact_job_day p
                ON p.job_uuid = f.job_uuid AND p.snapshot_date = f.prev_snapshot_date
             WHERE f.snapshot_date = %(d)s AND f.repost_count < p.repost_count""",
          "<=", 0, "repost counters never fall"),
    Check("applications_gt_views", "business_rule", "warn",
          "SELECT count(*) FROM clean.fact_job_day WHERE snapshot_date = %(d)s AND applications_total > views_total",
          "<=", 0, "nobody applies without viewing"),
    Check("snapshot_date_matches_fetch_sgt", "business_rule", "warn",
          """SELECT count(*) FROM (
                 SELECT COALESCE((SELECT finished_at FROM raw.ingest_runs i WHERE i.snapshot_date = %(d)s),
                                 (SELECT max(fetched_at) FROM raw.mcf_job_snapshots r WHERE r.snapshot_date = %(d)s)) AS t
             ) x WHERE (x.t AT TIME ZONE 'Asia/Singapore')::date <> %(d)s""",
          "<=", 0, "the snapshot label equals the crawl's Singapore date (L7: UTC vs SGT; a replayed file loaded later only warns)"),
    Check("coverage_ge_90pct", "business_rule", "fail",
          "SELECT jobs_fetched::numeric / NULLIF(api_total, 0) FROM raw.ingest_runs WHERE snapshot_date = %(d)s",
          ">=", 0.90, "the crawl fetched at least 90% of what the API said existed (no ingest_runs row -> warn)"),
    Check("raw_rows_equal_jobs_fetched", "business_rule", "warn",
          """SELECT abs((SELECT count(*) FROM raw.mcf_job_snapshots WHERE snapshot_date = %(d)s)
                      - (SELECT jobs_fetched FROM raw.ingest_runs WHERE snapshot_date = %(d)s))""",
          "<=", 0, "nothing was lost between the crawl and the raw table"),
    Check("postings_missing_vs_previous_share", "business_rule", "fail",
          """SELECT 1.0 - count(f.job_uuid)::numeric / NULLIF(count(*), 0)
             FROM (SELECT DISTINCT job_uuid FROM clean.fact_job_day
                   WHERE snapshot_date = (SELECT max(snapshot_date) FROM clean.fact_job_day WHERE snapshot_date < %(d)s)) p
             LEFT JOIN clean.fact_job_day f ON f.job_uuid = p.job_uuid AND f.snapshot_date = %(d)s""",
          "<=", 0.50, "normal churn is about 4% a day; over half missing means the crawl was cut short (first day -> warn)"),

    # ---------------- info: numbers on the record every day ----------------
    Check("fact_rows", "info", "info",
          "SELECT count(*) FROM clean.fact_job_day WHERE snapshot_date = %(d)s", ">=", 0, "postings loaded"),
    Check("postings_first_snapshot", "info", "info",
          "SELECT count(*) FROM clean.fact_job_day WHERE snapshot_date = %(d)s AND is_first_snapshot", ">=", 0,
          "postings seen for the first time"),
    Check("reposted_since_prev", "info", "info",
          "SELECT count(*) FROM clean.fact_job_day WHERE snapshot_date = %(d)s AND reposted_since_prev", ">=", 0,
          "postings reposted since their previous snapshot"),
    Check("dim_job_versions_created", "info", "info",
          "SELECT count(*) FROM clean.dim_job WHERE valid_from = %(d)s", ">=", 0, "new or changed versions today"),
    Check("skill_mentions_mapped_share", "info", "info",
          """SELECT avg(CASE WHEN s.match_method <> 'unmatched' THEN 1.0 ELSE 0.0 END)
             FROM clean.fact_job_day f
             JOIN clean.bridge_job_skill b ON b.job_key = f.job_key
             JOIN clean.dim_skill s ON s.skill_key = b.skill_key
             WHERE f.snapshot_date = %(d)s""",
          ">=", 0, "share of skill mentions mapped to a SkillsFuture skill"),
    Check("skill_distinct_tags_mapped_share", "info", "info",
          """SELECT avg(CASE WHEN s.match_method <> 'unmatched' THEN 1.0 ELSE 0.0 END)
             FROM (SELECT DISTINCT b.skill_key FROM clean.fact_job_day f
                   JOIN clean.bridge_job_skill b ON b.job_key = f.job_key WHERE f.snapshot_date = %(d)s) t
             JOIN clean.dim_skill s ON s.skill_key = t.skill_key""",
          ">=", 0, "share of distinct skill tags mapped"),
]


def _compare(observed: float, op: str, threshold: float) -> bool:
    if op == "<=":
        return observed <= threshold
    if op == ">=":
        return observed >= threshold
    if op == "==":
        return observed == threshold
    raise ValueError(f"unknown op {op}")


def evaluate(check: Check, observed) -> Result:
    """Pure: turn one measured number into pass / warn / fail according to the check's severity."""
    if check.severity == "info":
        return Result(check.name, check.category, check.severity, "pass",
                      None if observed is None else float(observed), check.threshold, check.description)
    if observed is None:
        # Nothing to measure (e.g. no ingest_runs row, first day has no previous snapshot).
        return Result(check.name, check.category, check.severity, "warn", None, check.threshold,
                      "no data to evaluate: " + check.description)
    observed = float(observed)
    if _compare(observed, check.op, check.threshold):
        return Result(check.name, check.category, check.severity, "pass", observed, check.threshold, None)
    status = "fail" if check.severity == "fail" else "warn"
    return Result(check.name, check.category, check.severity, status, observed, check.threshold, check.description)


def run_checks(conn, snapshot_date, checks=CHECKS) -> list[Result]:
    """Execute every check for the day, store the verdicts in clean.dq_results, return them."""
    results = []
    with conn:
        with conn.cursor() as cur:
            for check in checks:
                cur.execute(check.sql, {"d": snapshot_date})
                row = cur.fetchone()
                observed = row[0] if row else None
                results.append(evaluate(check, observed))
            execute_values(cur, """
                INSERT INTO clean.dq_results (snapshot_date, check_name, category, severity, status, observed, threshold, details)
                VALUES %s
                ON CONFLICT (snapshot_date, check_name) DO UPDATE SET
                    category = EXCLUDED.category, severity = EXCLUDED.severity, status = EXCLUDED.status,
                    observed = EXCLUDED.observed, threshold = EXCLUDED.threshold, details = EXCLUDED.details,
                    checked_at = now()
            """, [(snapshot_date, r.name, r.category, r.severity, r.status, r.observed, r.threshold, r.details)
                  for r in results])
    return results


def failed_checks(results) -> list[Result]:
    return [r for r in results if r.status == "fail"]


def summarise(results) -> dict:
    return {
        "checks": len(results),
        "pass": sum(1 for r in results if r.status == "pass"),
        "warn": sum(1 for r in results if r.status == "warn"),
        "fail": sum(1 for r in results if r.status == "fail"),
        "failed": [r.name for r in results if r.status == "fail"],
        "warned": [r.name for r in results if r.status == "warn"],
    }
