"""All SQL the dashboard runs. Every function returns a DataFrame and is cached for an hour.

The heavy lifting is already done by the `mart` schema (sql/init/07_dashboard_marts.sql); these queries
only filter, window and rank those small tables. `Filters` is hashable, so it works as a cache key.
"""
import streamlit as st

from dashboard.db import run_query
from dashboard.filters import Filters

TTL = 3600
AS_OF = "(SELECT max(snapshot_date) FROM mart.slice_day)"


def _slice(alias: str = "") -> str:
    a = f"{alias}." if alias else ""
    return (
        f"(cardinality(%(mom)s::text[]) = 0 OR {a}mom_group = ANY(%(mom)s::text[])) "
        f"AND (cardinality(%(major)s::int[]) = 0 OR {a}ssoc_major_group = ANY(%(major)s::int[])) "
        f"AND (cardinality(%(cat)s::text[]) = 0 OR {a}category = ANY(%(cat)s::text[]))"
    )


def _occupation_slice(alias: str = "") -> str:
    """Occupation marts have no category and no major-group column; the major group is the SSOC code's first digit."""
    a = f"{alias}." if alias else ""
    major = f"(CASE WHEN {a}ssoc_code ~ '^[0-9]' THEN left({a}ssoc_code, 1)::int ELSE 0 END)"
    return (
        f"(cardinality(%(mom)s::text[]) = 0 OR {a}mom_group = ANY(%(mom)s::text[])) "
        f"AND (cardinality(%(major)s::int[]) = 0 OR {major} = ANY(%(major)s::int[]))"
    )


def _group_slice(alias: str = "") -> str:
    """Industry and employer marts carry the occupation-group columns but no job category."""
    a = f"{alias}." if alias else ""
    return (
        f"(cardinality(%(mom)s::text[]) = 0 OR {a}mom_group = ANY(%(mom)s::text[])) "
        f"AND (cardinality(%(major)s::int[]) = 0 OR {a}ssoc_major_group = ANY(%(major)s::int[]))"
    )


def _skill_filter(alias: str = "k") -> str:
    return (
        f"(cardinality(%(stype)s::text[]) = 0 OR {alias}.skill_type = ANY(%(stype)s::text[])) "
        f"AND (NOT %(emerging)s OR {alias}.is_emerging)"
    )


# --- sidebar / coverage -------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def coverage():
    return run_query(f"""
        SELECT min(snapshot_date) AS first_day, max(snapshot_date) AS last_day,
               count(DISTINCT snapshot_date) AS days, COALESCE(sum(postings), 0) AS posting_days
        FROM mart.slice_day
    """)


@st.cache_data(ttl=TTL, show_spinner=False)
def filter_options():
    groups = run_query("""
        SELECT DISTINCT mom_group FROM mart.slice_day ORDER BY 1
    """)
    majors = run_query("""
        SELECT DISTINCT major_group, major_group_title FROM clean.ref_ssoc ORDER BY 1
    """)
    cats = run_query("""
        SELECT category, sum(postings) AS postings FROM mart.slice_day GROUP BY 1 ORDER BY 2 DESC
    """)
    return {"mom_groups": groups, "major_groups": majors, "categories": cats}


# --- trending skills ----------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def trending_skills(f: Filters, limit: int = 20, min_mentions: int = 20, min_prev_mentions: int = 20):
    """Top skills in the window by posting-days, with share of all postings in the slice and the
    last-7-days vs previous-7-days change in that share."""
    sql = f"""
    WITH den AS (
        SELECT COALESCE(sum(postings) FILTER (WHERE snapshot_date > {AS_OF} - %(window)s), 0) AS win,
               COALESCE(sum(postings) FILTER (WHERE snapshot_date > {AS_OF} - 7), 0) AS last7,
               COALESCE(sum(postings) FILTER (WHERE snapshot_date > {AS_OF} - 14
                                                AND snapshot_date <= {AS_OF} - 7), 0) AS prev7
        FROM mart.slice_day
        WHERE snapshot_date > {AS_OF} - GREATEST(%(window)s, 14) AND {_slice()}
    ),
    sk AS (
        SELECT s.skill_id,
               COALESCE(sum(s.postings) FILTER (WHERE s.snapshot_date > {AS_OF} - %(window)s), 0) AS mentions,
               COALESCE(sum(s.postings) FILTER (WHERE s.snapshot_date > {AS_OF} - 7), 0) AS last7,
               COALESCE(sum(s.postings) FILTER (WHERE s.snapshot_date > {AS_OF} - 14
                                                  AND s.snapshot_date <= {AS_OF} - 7), 0) AS prev7
        FROM mart.skill_slice_day s
        JOIN mart.skill k ON k.skill_id = s.skill_id
        WHERE s.snapshot_date > {AS_OF} - GREATEST(%(window)s, 14)
          AND {_slice('s')} AND {_skill_filter('k')}
        GROUP BY s.skill_id
    )
    SELECT k.skill_id, k.skill_label, k.skill_type, k.is_emerging,
           sk.mentions,
           sk.mentions::float8 / NULLIF(den.win, 0)                      AS share,
           sk.last7::float8 / NULLIF(den.last7, 0)                       AS share_last7,
           sk.prev7::float8 / NULLIF(den.prev7, 0)                       AS share_prev7,
           sk.prev7                                                       AS prev7_mentions,
           CASE WHEN sk.prev7 >= %(min_prev)s AND den.prev7 > 0 AND den.last7 > 0
                THEN ((sk.last7::float8 / den.last7) / (sk.prev7::float8 / den.prev7) - 1) * 100 END AS wow_pct
    FROM sk CROSS JOIN den
    JOIN mart.skill k ON k.skill_id = sk.skill_id
    WHERE sk.mentions >= %(min_mentions)s
    ORDER BY sk.mentions DESC, k.skill_label
    LIMIT %(limit)s
    """
    return run_query(sql, {**f.params(), "limit": limit, "min_mentions": min_mentions,
                           "min_prev": min_prev_mentions})


@st.cache_data(ttl=TTL, show_spinner=False)
def skill_share_by_day(f: Filters, skill_ids: tuple):
    sql = f"""
    WITH den AS (
        SELECT snapshot_date, sum(postings) AS total
        FROM mart.slice_day
        WHERE snapshot_date > {AS_OF} - %(window)s AND {_slice()}
        GROUP BY snapshot_date
    )
    SELECT s.snapshot_date, k.skill_label, sum(s.postings)::float8 / den.total AS share
    FROM mart.skill_slice_day s
    JOIN den ON den.snapshot_date = s.snapshot_date
    JOIN mart.skill k ON k.skill_id = s.skill_id
    WHERE s.skill_id = ANY(%(skills)s::text[]) AND {_slice('s')}
    GROUP BY s.snapshot_date, k.skill_label, den.total
    ORDER BY s.snapshot_date, k.skill_label
    """
    return run_query(sql, {**f.params(), "skills": list(skill_ids)})


@st.cache_data(ttl=TTL, show_spinner=False)
def market_by_day(f: Filters):
    sql = f"""
    SELECT snapshot_date,
           sum(postings) AS postings,
           sum(applications_per_day) / NULLIF(sum(rated_postings), 0) AS applications_per_posting,
           sum(views_per_day) / NULLIF(sum(rated_postings), 0)        AS views_per_posting,
           sum(reposts)::float8 / NULLIF(sum(postings), 0)            AS repost_rate
    FROM mart.slice_day
    WHERE snapshot_date > {AS_OF} - %(window)s AND {_slice()}
    GROUP BY snapshot_date
    ORDER BY snapshot_date
    """
    return run_query(sql, f.params())


# --- salary -------------------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def skill_salaries(f: Filters, min_salary_postings: int = 20, limit: int = 40):
    sql = f"""
    SELECT k.skill_label, k.skill_type, k.is_emerging, s.postings, s.salary_postings,
           s.salary_p25, s.salary_p50, s.salary_p75
    FROM mart.skill_salary_latest s
    JOIN mart.skill k ON k.skill_id = s.skill_id
    WHERE s.salary_postings >= %(min_n)s AND {_skill_filter('k')}
    ORDER BY s.salary_p50 DESC
    LIMIT %(limit)s
    """
    return run_query(sql, {**f.params(), "min_n": min_salary_postings, "limit": limit})


@st.cache_data(ttl=TTL, show_spinner=False)
def skill_pair_salaries(f: Filters, min_salary_postings: int = 15, limit: int = 30):
    sql = f"""
    SELECT ka.skill_label AS skill_a, kb.skill_label AS skill_b, p.postings, p.salary_postings,
           p.salary_p25, p.salary_p50, p.salary_p75
    FROM mart.skill_pair_latest p
    JOIN mart.skill ka ON ka.skill_id = p.skill_a
    JOIN mart.skill kb ON kb.skill_id = p.skill_b
    WHERE p.salary_postings >= %(min_n)s AND {_skill_filter('ka')} AND {_skill_filter('kb')}
    ORDER BY p.salary_p50 DESC
    LIMIT %(limit)s
    """
    return run_query(sql, {**f.params(), "min_n": min_salary_postings, "limit": limit})


@st.cache_data(ttl=TTL, show_spinner=False)
def occupation_salaries(f: Filters, min_salary_postings: int = 20, limit: int = 40):
    sql = f"""
    SELECT ssoc_title, mom_group, postings, salary_postings, salary_p25, salary_p50, salary_p75
    FROM mart.occupation_latest
    WHERE salary_postings >= %(min_n)s AND {_occupation_slice()}
    ORDER BY salary_p50 DESC
    LIMIT %(limit)s
    """
    return run_query(sql, {**f.params(), "min_n": min_salary_postings, "limit": limit})


@st.cache_data(ttl=TTL, show_spinner=False)
def salary_drivers(min_salary_postings: int = 30):
    """Salary percentiles by years of experience asked, position level and number of skills listed."""
    return run_query("""
        SELECT driver, bucket, sort, salary_postings, salary_p25, salary_p50, salary_p75
        FROM mart.salary_driver_latest
        WHERE salary_postings >= %(min_n)s
        ORDER BY driver, sort, salary_p50
    """, {"min_n": min_salary_postings})


@st.cache_data(ttl=TTL, show_spinner=False)
def industry_salaries(min_salary_postings: int = 30):
    return run_query("""
        SELECT industry, postings, salary_postings, salary_p25, salary_p50, salary_p75
        FROM mart.industry_latest
        WHERE salary_postings >= %(min_n)s AND industry <> 'Unknown industry'
        ORDER BY salary_p50 DESC
        LIMIT 30
    """, {"min_n": min_salary_postings})


# --- industries and employers -------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def industry_overview(f: Filters, limit: int = 25):
    """Open postings per industry over the window, share of all postings, and the share change week over week."""
    sql = f"""
    WITH d AS (
        SELECT industry,
               sum(postings) AS posting_days,
               sum(postings) FILTER (WHERE snapshot_date > {AS_OF} - 7)                                   AS recent,
               sum(postings) FILTER (WHERE snapshot_date <= {AS_OF} - 7 AND snapshot_date > {AS_OF} - 14) AS prev,
               sum(applications_per_day) / NULLIF(sum(rated_postings), 0) AS applications_per_posting
        FROM mart.industry_slice_day
        WHERE snapshot_date > {AS_OF} - %(window)s AND {_group_slice()}
        GROUP BY industry
    ),
    days AS (
        SELECT count(DISTINCT snapshot_date) AS n FROM mart.industry_slice_day
        WHERE snapshot_date > {AS_OF} - %(window)s
    )
    SELECT industry, posting_days, posting_days::float8 / NULLIF(days.n, 0) AS avg_open_postings,
           posting_days::float8 / NULLIF(sum(posting_days) OVER (), 0) AS share,
           recent::float8 / NULLIF(sum(recent) OVER (), 0) AS recent_share,
           prev::float8 / NULLIF(sum(prev) OVER (), 0) AS prev_share,
           applications_per_posting
    FROM d CROSS JOIN days
    ORDER BY posting_days DESC
    LIMIT %(limit)s
    """
    return run_query(sql, {**f.params(), "limit": limit})


@st.cache_data(ttl=TTL, show_spinner=False)
def industry_skill_heatmap(f: Filters, n_industries: int = 12, n_skills: int = 15):
    """Share of each large industry's postings that ask for each of the most-requested skills (latest day)."""
    sql = f"""
    WITH ind AS (
        SELECT industry, postings FROM mart.industry_latest
        WHERE industry <> 'Unknown industry' ORDER BY postings DESC LIMIT %(n_ind)s
    ),
    top_skill AS (
        SELECT s.skill_id FROM mart.industry_skill_latest s
        JOIN ind USING (industry) JOIN mart.skill k USING (skill_id)
        WHERE {_skill_filter('k')}
        GROUP BY s.skill_id ORDER BY sum(s.postings) DESC LIMIT %(n_skills)s
    )
    SELECT i.industry, k.skill_label, s.postings::float8 / i.postings AS share
    FROM ind i
    JOIN mart.industry_skill_latest s ON s.industry = i.industry
    JOIN top_skill t ON t.skill_id = s.skill_id
    JOIN mart.skill k ON k.skill_id = s.skill_id
    """
    return run_query(sql, {**f.params(), "n_ind": n_industries, "n_skills": n_skills})


@st.cache_data(ttl=TTL, show_spinner=False)
def industry_top_skills(f: Filters, industry: str, limit: int = 15):
    sql = f"""
    SELECT k.skill_label, k.skill_type, s.postings,
           s.postings::float8 / NULLIF(i.postings, 0) AS share
    FROM mart.industry_skill_latest s
    JOIN mart.industry_latest i USING (industry)
    JOIN mart.skill k USING (skill_id)
    WHERE s.industry = %(industry)s AND {_skill_filter('k')}
    ORDER BY s.postings DESC
    LIMIT %(limit)s
    """
    return run_query(sql, {**f.params(), "industry": industry, "limit": limit})


@st.cache_data(ttl=TTL, show_spinner=False)
def top_employers(f: Filters, limit: int = 20):
    """Employers with the most postings open on the latest day. `total_postings` is the whole slice."""
    sql = f"""
    SELECT employer, min(industry) AS industry, sum(postings) AS postings, sum(vacancies) AS vacancies,
           sum(applications_per_day) / NULLIF(sum(rated_postings), 0) AS applications_per_posting,
           sum(sum(postings)) OVER () AS total_postings,
           count(*) OVER () AS employers
    FROM mart.company_latest
    WHERE {_group_slice()}
    GROUP BY employer
    ORDER BY postings DESC, employer
    LIMIT %(limit)s
    """
    return run_query(sql, {**f.params(), "limit": limit})


@st.cache_data(ttl=TTL, show_spinner=False)
def employer_top_skills(f: Filters, employer: str, limit: int = 15):
    sql = f"""
    SELECT k.skill_label, k.skill_type, s.postings,
           s.postings::float8 / NULLIF((SELECT sum(postings) FROM mart.company_latest WHERE employer = %(employer)s), 0)
               AS share
    FROM mart.company_skill_latest s
    JOIN mart.skill k USING (skill_id)
    WHERE s.employer = %(employer)s AND {_skill_filter('k')}
    ORDER BY s.postings DESC
    LIMIT %(limit)s
    """
    return run_query(sql, {**f.params(), "employer": employer, "limit": limit})


# --- salary estimator ---------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def estimator_occupations():
    return run_query("""
        SELECT ssoc_code, ssoc_title, postings
        FROM mart.occupation_latest
        WHERE ssoc_code ~ '^[0-9]{3}'
        ORDER BY ssoc_title
    """)


@st.cache_data(ttl=TTL, show_spinner=False)
def salary_model():
    """The most recently trained salary model (transform.salary_model), or None if there is not one yet."""
    from transform.db import connect
    from transform.salary_model import load_model
    conn = connect()
    try:
        conn.set_session(readonly=True, autocommit=True)
        return load_model(conn)
    finally:
        conn.close()


# --- hiring difficulty --------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def occupation_difficulty(f: Filters, min_posting_days: int = 100):
    sql = f"""
    SELECT ssoc_code, min(ssoc_title) AS ssoc_title, min(mom_group) AS mom_group,
           sum(postings) AS posting_days,
           sum(postings)::float8 / count(DISTINCT snapshot_date) AS avg_open_postings,
           sum(applications_per_day) / NULLIF(sum(rated_postings), 0) AS applications_per_posting,
           sum(views_per_day) / NULLIF(sum(rated_postings), 0)        AS views_per_posting,
           sum(reposts)::float8 / NULLIF(sum(postings), 0)            AS repost_rate,
           sum(avg_days_open * postings) / NULLIF(sum(postings), 0)   AS avg_days_open
    FROM mart.occupation_day
    WHERE snapshot_date > {AS_OF} - %(window)s AND {_occupation_slice()}
    GROUP BY ssoc_code
    HAVING sum(postings) >= %(min_n)s
    ORDER BY posting_days DESC
    """
    return run_query(sql, {**f.params(), "min_n": min_posting_days})


@st.cache_data(ttl=TTL, show_spinner=False)
def skill_difficulty(f: Filters, min_posting_days: int = 100, limit: int = 40):
    sql = f"""
    SELECT k.skill_label, k.skill_type, sum(s.postings) AS posting_days,
           sum(s.applications_per_day) / NULLIF(sum(s.rated_postings), 0) AS applications_per_posting,
           sum(s.reposts)::float8 / NULLIF(sum(s.postings), 0)            AS repost_rate
    FROM mart.skill_slice_day s
    JOIN mart.skill k ON k.skill_id = s.skill_id
    WHERE s.snapshot_date > {AS_OF} - %(window)s AND {_slice('s')} AND {_skill_filter('k')}
    GROUP BY k.skill_label, k.skill_type
    HAVING sum(s.postings) >= %(min_n)s
    ORDER BY posting_days DESC
    LIMIT %(limit)s
    """
    return run_query(sql, {**f.params(), "min_n": min_posting_days, "limit": limit})


# --- career transitions -------------------------------------------------------------------------

@st.cache_data(ttl=TTL, show_spinner=False)
def occupation_skill_profiles():
    """Skills each occupation asks for on the latest day: (ssoc_code, skill, postings) + occupation totals."""
    occupations = run_query("""
        SELECT ssoc_code, ssoc_title, mom_group, postings
        FROM mart.occupation_latest
        WHERE ssoc_code <> '?' AND postings >= 20
        ORDER BY postings DESC
    """)
    skills = run_query("""
        SELECT os.ssoc_code, os.skill_id, k.skill_label, k.skill_type, os.postings AS skill_postings
        FROM mart.occupation_skill_latest os
        JOIN mart.skill k USING (skill_id)
    """)
    return occupations, skills


# --- pipeline health ----------------------------------------------------------------------------

@st.cache_data(ttl=300, show_spinner=False)
def ingest_runs(days: int = 30):
    return run_query("""
        SELECT snapshot_date, pages_fetched, jobs_fetched, api_total, seconds_taken, finished_at
        FROM raw.ingest_runs
        ORDER BY snapshot_date DESC
        LIMIT %(days)s
    """, {"days": days})


@st.cache_data(ttl=300, show_spinner=False)
def load_steps(days: int = 14):
    return run_query("""
        WITH last_run AS (
            SELECT DISTINCT ON (snapshot_date) snapshot_date, run_id
            FROM clean.load_log
            ORDER BY snapshot_date, started_at DESC
        )
        SELECT l.snapshot_date, l.step, l.seconds, l.rows_in, l.rows_out
        FROM clean.load_log l
        JOIN last_run r ON r.snapshot_date = l.snapshot_date AND r.run_id = l.run_id
        WHERE l.snapshot_date > (SELECT max(snapshot_date) FROM clean.load_log) - %(days)s
        ORDER BY l.snapshot_date, l.started_at
    """, {"days": days})


@st.cache_data(ttl=300, show_spinner=False)
def dq_results(days: int = 14):
    return run_query("""
        SELECT snapshot_date, check_name, category, severity, status, observed, threshold, details
        FROM clean.dq_results
        WHERE snapshot_date > (SELECT max(snapshot_date) FROM clean.dq_results) - %(days)s
        ORDER BY snapshot_date DESC, status DESC, check_name
    """, {"days": days})


@st.cache_data(ttl=300, show_spinner=False)
def skill_match_quality():
    return run_query("""
        SELECT COALESCE(match_method, 'unmatched') AS match_method,
               count(*) AS skill_tags,
               avg(match_confidence)::float8 AS avg_confidence
        FROM clean.dim_skill
        GROUP BY 1
        ORDER BY 2 DESC
    """)


@st.cache_data(ttl=300, show_spinner=False)
def freshness():
    return run_query("""
        SELECT (SELECT max(snapshot_date) FROM raw.ingest_runs)      AS raw_latest,
               (SELECT max(snapshot_date) FROM clean.fact_job_day)   AS clean_latest,
               (SELECT max(snapshot_date) FROM mart.slice_day)       AS mart_latest
    """)
