"""Data-quality checks for one poll of the near-real-time feed (dags/mcf_live_poll_dag.py).

Same shape as quality/clean_checks.py: each check is one SQL statement returning one number, tagged
schema, range or business rule, judged against a threshold, with a severity. `fail` stops the DAG run
(the rows stay loaded so the problem can be seen), `warn` is recorded only, `info` puts a number on the
record (how many pages the API served from its cache). Verdicts go to
raw.mcf_live_poll_dq, one row per check per poll, so the status view and the report can quote them.

%(t)s is the poll's poll_ts. The first poll ever has no previous poll, so minutes_since_prev_poll has
nothing to measure and records a warning, like the daily checks on the first snapshot day.
"""
from psycopg2.extras import execute_values

from quality.clean_checks import Check, evaluate, failed_checks, summarise  # noqa: F401 (re-exported for the DAG)

_POLL = "FROM raw.mcf_live_poll WHERE poll_ts = %(t)s"
_PREV_POLL = "(SELECT max(poll_ts) FROM raw.mcf_live_poll WHERE poll_ts < %(t)s)"

LIVE_CHECKS = [
    # ---------------- schema ----------------
    Check("poll_rows_present", "schema", "fail",
          "SELECT count(*) " + _POLL,
          ">=", 1, "the poll stored at least one posting (0 means the API answered with an empty list)"),
    Check("poll_rows_equal_fetched", "schema", "fail",
          """SELECT abs((SELECT count(*) """ + _POLL + """)
                      - (SELECT rows_fetched FROM raw.mcf_live_poll_runs WHERE poll_ts = %(t)s))""",
          "==", 0, "every posting the poll fetched was stored, and nothing else"),
    Check("poll_duplicate_keys", "schema", "fail",
          "SELECT count(*) - count(DISTINCT job_uuid) " + _POLL,
          "==", 0, "one row per posting per poll (the key of the append-only table)"),
    # ---------------- range ----------------
    Check("counters_missing_or_negative", "range", "warn",
          "SELECT count(*) " + _POLL + " AND (views IS NULL OR applications IS NULL OR views < 0 OR applications < 0)",
          "<=", 0, "views and applications are present and never negative"),
    # ---------------- business rules ----------------
    Check("counters_fell_since_prev_poll", "business_rule", "warn",
          """SELECT count(*) FROM raw.mcf_live_poll c
             JOIN raw.mcf_live_poll p ON p.job_uuid = c.job_uuid AND p.poll_ts = """ + _PREV_POLL + """
             WHERE c.poll_ts = %(t)s AND (c.views < p.views OR c.applications < p.applications)""",
          "<=", 0, "running totals never fall between two polls"),
    Check("list_newest_first", "business_rule", "warn",
          """SELECT avg(CASE WHEN created_at > prev_created THEN 1.0 ELSE 0.0 END) FROM (
                 SELECT created_at, lag(created_at) OVER (ORDER BY list_rank) AS prev_created """ + _POLL + """
             ) x WHERE prev_created IS NOT NULL""",
          "<=", 0.05, "the API still lists newest first (the feed relies on it to catch new postings)"),
    Check("pages_out_of_step_share", "business_rule", "warn",
          """SELECT duplicates_dropped::numeric / NULLIF(rows_fetched + duplicates_dropped, 0)
             FROM raw.mcf_live_poll_runs WHERE poll_ts = %(t)s""",
          "<=", 0.05, "share of postings seen on two pages; above 5% means the API served pages of different "
                      "ages (a cached page 0), so this poll's newest postings are older than poll_ts suggests"),
    Check("minutes_since_prev_poll", "business_rule", "warn",
          "SELECT extract(epoch FROM %(t)s::timestamptz - " + _PREV_POLL + ") / 60",
          "<=", 45, "over 45 minutes since the previous poll means at least one poll was missed"),
    # ---------------- info: a number on the record every poll ----------------
    Check("pages_unchanged_share", "info", "info",
          "SELECT avg(CASE WHEN unchanged_from_earlier_poll THEN 1.0 ELSE 0.0 END) FROM metrics.live_poll_pages "
          "WHERE poll_ts = %(t)s",
          ">=", 0, "share of pages identical to an earlier poll's (served from the API's cache; 1.0 = nothing new)"),
]


def run_live_checks(conn, poll_ts, checks=LIVE_CHECKS) -> list:
    """Run every check for one poll, store the verdicts in raw.mcf_live_poll_dq, return them."""
    results = []
    with conn:
        with conn.cursor() as cur:
            for check in checks:
                cur.execute(check.sql, {"t": poll_ts})
                row = cur.fetchone()
                results.append(evaluate(check, row[0] if row else None))
            execute_values(cur, """
                INSERT INTO raw.mcf_live_poll_dq (poll_ts, check_name, category, severity, status, observed, threshold, details)
                VALUES %s
                ON CONFLICT (poll_ts, check_name) DO UPDATE SET
                    category = EXCLUDED.category, severity = EXCLUDED.severity, status = EXCLUDED.status,
                    observed = EXCLUDED.observed, threshold = EXCLUDED.threshold, details = EXCLUDED.details,
                    checked_at = now()
            """, [(poll_ts, r.name, r.category, r.severity, r.status, r.observed, r.threshold, r.details)
                  for r in results])
    return results
