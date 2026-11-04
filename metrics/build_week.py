"""Build one week of hiring signals from the clean layer into the metrics schema, idempotently.

    resolve_week        the week (after, up_to] and its latest snapshot (transform/snapshot_week.py, the same
                        rule as the MOM comparison), plus the crawl-skip window (metrics/weeks.py)
    start_run_log       metrics.run_log row: which snapshot was used and why, status 'running'
    build_table         one transaction per table: DELETE the week's rows, then INSERT them again
        posting_week              the open postings at the week end with posting-level flags (first)
        barrier_skill, shortage_skill, unrealistic_requirements, competitiveness,
        skill_demand, skill_salary          (independent of each other: parallel in the DAG)
    run_metric_checks   range and sanity checks on the week's rows (schema, range, business rule, info)
    finish_run_log      rows, seconds and check verdicts, status 'ok' or 'failed_checks'

run_week() runs them all in order (CLI and tests). dags/weekly_metrics_dag.py runs the same functions as
separate tasks, next to the MOM comparison (transform/mom_vacancies.py), which is the MOM validation.

Every number is computed inside Postgres (ELT: the warehouse's own compute, as in the transform). The
largest step reads about 100k postings and 1.4 million posting-skill rows and takes seconds.

Definitions shared by all signals:
  - open postings at the week end: metrics/weeks.py. A posting the crawl skipped on the snapshot day
    still counts if it was seen just before and just after (data check of 2 Nov).
  - running totals (views_total, applications_total) are semi-additive: only the latest value per
    posting is used, never a sum across days. Day-over-day increases are summed, and normalised to
    24 hours with hours_since_prev because runs were 17 to 28 hours apart.
  - skill_ref groups a skill by its SkillsFuture id when the MCF tag is mapped, else by the MCF tag,
    and a posting counts once per skill_ref.
  - minimum sample sizes come from THRESHOLDS and are stored with every run in metrics.run_log.
"""
import json
import time
from datetime import date, datetime, timedelta, timezone

from quality.clean_checks import Check, evaluate, summarise
from metrics.weeks import MAX_GAP_SNAPSHOTS, week_context
from transform.snapshot_week import latest_snapshot_in_week

THRESHOLDS = {
    # open postings (metrics/weeks.py): a crawl skip of up to this many snapshots in a row is not a closure
    "max_gap_snapshots": MAX_GAP_SNAPSHOTS,
    # minimum sample sizes, so one employer's bulk postings cannot create a signal out of nothing
    "skill_min_postings": 100,       # open postings carrying the skill (barrier, shortage, competitiveness)
    "group_min_postings": 30,        # postings in a category, SSOC occupation or position level
    "salary_min_postings": 30,       # open postings with the skill and a usable salary range
    "demand_min_postings": 30,       # postings with the skill across the whole demand window
    "demand_weeks": 6,               # posting weeks kept in skill_demand, counting back from the snapshot's week
    "min_companies": 10,             # employers behind a barrier skill, or behind a skill's shortage postings
                                     # (on 25 Oct "Aircraft Systems" had 40 shortage postings from ONE employer;
                                     # on 3 Nov, with 5, Metal Fabrication's 57 shortage postings came from 5 firms)
    # usable monthly salary: below 300 are hourly or daily rates typed into the monthly field (for example $12 to
    # $24 "per month" for a packing job, 1,395 postings below 300 on 25 Oct) and placeholders like $1, above
    # 100,000 are annual figures
    "salary_floor": 300,
    "salary_ceiling": 100000,
    # barrier skills: at least 20% fewer applications per view than the average open posting
    "barrier_max_rate_ratio": 0.8,
    # skill shortages: reposted at least twice (2 is the highest count seen) and at most 2 applications in total
    # (totals carry over a repost, so 2 in total means at most 2 since the repost: the conservative side)
    "shortage_min_reposts": 2,
    "shortage_max_applications": 2,
    "shortage_min_flagged": 25,      # flagged postings carrying the skill
    # unrealistic requirements
    "many_apps_percentile": 0.9,     # "many applications" = the P90 of open postings that week
    "entry_levels": ["Fresh/entry level"],
    "junior_levels": ["Junior Executive"],   # reported as its own flag: 46% ask 2+ years against 6.6% at entry level
    "entry_min_years": 2,
    "exp_max_valid": 30,             # 32, 50 and 88 years are typing errors (data check), left out of the flag
}

# The MOM validation is not built here: it is metrics.mcf_vs_mom_share, written by compare_mcf_with_mom in
# the same DAG run from the same snapshot (transform/mom_vacancies.py).
SIGNAL_TABLES = (
    "barrier_skill", "shortage_skill", "unrealistic_requirements", "competitiveness",
    "skill_demand", "skill_salary",
)

# ---------------------------------------------------------------------------------------------
# Shared SQL fragments (CTEs). %(e)s is the week_end_date, %(s)s the snapshot used.
# ---------------------------------------------------------------------------------------------
_POOL_SKILL = """
    pool_skill AS (   -- one row per open posting and skill_ref
        SELECT DISTINCT pw.job_uuid, COALESCE(s.ssg_skill_id, s.mcf_skill_uuid) AS skill_ref
        FROM metrics.posting_week pw
        JOIN clean.bridge_job_skill b ON b.job_key = pw.job_key
        JOIN clean.dim_skill s ON s.skill_key = b.skill_key
        WHERE pw.week_end_date = %(e)s
    )"""

_SKILL_NAMES = """
    skill_names AS (
        SELECT DISTINCT ON (COALESCE(ssg_skill_id, mcf_skill_uuid))
               COALESCE(ssg_skill_id, mcf_skill_uuid)    AS skill_ref,
               COALESCE(ssg_skill_title, mcf_skill_name) AS skill_name,
               ssg_skill_id IS NOT NULL                  AS is_mapped,
               COALESCE(is_casl, FALSE)                  AS is_casl
        FROM clean.dim_skill
        ORDER BY COALESCE(ssg_skill_id, mcf_skill_uuid), skill_key
    )"""

_MAJOR_TITLES = """
    major_titles AS (
        SELECT major_group, max(major_group_title) AS title FROM clean.ref_ssoc GROUP BY major_group
    )"""

# ---------------------------------------------------------------------------------------------
# posting_week: the open postings at the week end (periodic snapshot fact)
# ---------------------------------------------------------------------------------------------
POSTING_WEEK_SQL = """
INSERT INTO metrics.posting_week (
    week_end_date, snapshot_date, job_uuid, job_key, last_seen_date, seen_on_snapshot,
    title, company_name, ssoc_code, ssoc_title, ssoc_major_group, mom_group, category, position_level,
    min_years_exp, exp_out_of_range, salary_min, salary_max, salary_mid, salary_usable, vacancies,
    original_posting_date, new_posting_date, expiry_date,
    views_total, applications_total, repost_count, days_since_original_posting, apps_per_day_since_posting,
    is_shortage, is_reposted_many_apps, is_entry_level_2plus)
WITH latest AS (
    -- each posting's latest sighting on or before the snapshot, inside the gap window
    SELECT DISTINCT ON (f.job_uuid)
           f.job_uuid, f.job_key, f.snapshot_date, f.views_total, f.applications_total, f.repost_count,
           f.days_since_original_posting
    FROM clean.fact_job_day f
    WHERE f.snapshot_date BETWEEN %(pool_from)s AND %(s)s
    ORDER BY f.job_uuid, f.snapshot_date DESC
),
open_postings AS (
    -- seen on the snapshot day, or skipped by the crawl that day: seen just before AND again soon after
    SELECT l.* FROM latest l
    WHERE l.snapshot_date = %(s)s
       OR EXISTS (SELECT 1 FROM clean.fact_job_day n
                  WHERE n.job_uuid = l.job_uuid AND n.snapshot_date > %(s)s AND n.snapshot_date <= %(pool_to)s)
)
SELECT %(e)s, %(s)s, o.job_uuid, o.job_key, o.snapshot_date, o.snapshot_date = %(s)s,
       j.title, j.company_name, j.ssoc_code, j.ssoc_title, j.ssoc_major_group, j.mom_group, j.category,
       j.position_level, j.min_years_exp, j.exp_out_of_range, j.salary_min, j.salary_max, j.salary_mid,
       (j.salary_min >= %(salary_floor)s AND j.salary_max >= j.salary_min
        AND j.salary_max <= %(salary_ceiling)s) IS TRUE,
       j.vacancies, j.original_posting_date, j.new_posting_date, j.expiry_date,
       o.views_total, o.applications_total, o.repost_count, o.days_since_original_posting,
       CASE WHEN o.days_since_original_posting IS NOT NULL
            THEN round(o.applications_total::numeric / GREATEST(o.days_since_original_posting, 1), 4) END,
       (o.repost_count >= %(shortage_min_reposts)s AND o.applications_total <= %(shortage_max_applications)s),
       FALSE,      -- set below, once the week's P90 of applications is known
       ((j.position_level = ANY(%(entry_levels)s) OR j.position_level = ANY(%(junior_levels)s))
        AND NOT j.exp_out_of_range AND j.min_years_exp BETWEEN %(entry_min_years)s AND %(exp_max_valid)s) IS TRUE
FROM open_postings o
JOIN clean.dim_job j ON j.job_key = o.job_key
"""


def many_apps_threshold(cur, p) -> int:
    """'Many applications' for the week: the P90 of applications_total over the open postings, at least 1."""
    cur.execute("""SELECT percentile_disc(%(many_apps_percentile)s) WITHIN GROUP (ORDER BY applications_total)
                   FROM metrics.posting_week WHERE week_end_date = %(e)s""", p)
    value = cur.fetchone()[0]
    return max(int(value or 0), 1)


def _build_posting_week(cur, p) -> dict:
    cur.execute(POSTING_WEEK_SQL, p)
    many = many_apps_threshold(cur, p)
    cur.execute("""
        UPDATE metrics.posting_week SET is_reposted_many_apps = (repost_count >= 1 AND applications_total >= %(many)s)
        WHERE week_end_date = %(e)s
    """, {**p, "many": many})
    cur.execute("""
        SELECT count(*) FILTER (WHERE NOT seen_on_snapshot), count(*) FILTER (WHERE is_shortage),
               count(*) FILTER (WHERE is_reposted_many_apps), count(*) FILTER (WHERE is_entry_level_2plus)
        FROM metrics.posting_week WHERE week_end_date = %(e)s
    """, p)
    carried, shortage, reposted_many, entry = cur.fetchone()
    return {"carried_forward": carried, "flag_shortage": shortage, "flag_reposted_many_apps": reposted_many,
            "flag_entry_level_2plus": entry, "many_apps_threshold": many}


# ---------------------------------------------------------------------------------------------
# Signal 1: barrier skills
# ---------------------------------------------------------------------------------------------
BARRIER_SQL = f"""
INSERT INTO metrics.barrier_skill (week_end_date, snapshot_date, skill_ref, skill_name, is_mapped, is_casl,
    postings, companies, views, applications, views_per_posting, apply_rate, overall_apply_rate,
    rate_vs_overall, views_vs_overall, is_barrier)
WITH {_POOL_SKILL}, {_SKILL_NAMES},
overall AS (
    SELECT sum(views_total)::numeric / NULLIF(count(*), 0)               AS views_per_posting,
           sum(applications_total)::numeric / NULLIF(sum(views_total), 0) AS apply_rate
    FROM metrics.posting_week WHERE week_end_date = %(e)s
),
per_skill AS (
    SELECT ps.skill_ref, count(*) AS postings, count(DISTINCT pw.company_name) AS companies,
           sum(pw.views_total) AS views, sum(pw.applications_total) AS applications
    FROM pool_skill ps
    JOIN metrics.posting_week pw ON pw.week_end_date = %(e)s AND pw.job_uuid = ps.job_uuid
    GROUP BY ps.skill_ref
    HAVING count(*) >= %(skill_min_postings)s AND count(DISTINCT pw.company_name) >= %(min_companies)s
),
rated AS (
    SELECT p.*, p.views::numeric / p.postings AS vpp, p.applications::numeric / NULLIF(p.views, 0) AS rate
    FROM per_skill p
)
SELECT %(e)s, %(s)s, r.skill_ref, n.skill_name, n.is_mapped, n.is_casl,
       r.postings, r.companies, r.views, r.applications,
       round(r.vpp, 1), round(r.rate, 5), round(o.apply_rate, 5),
       round(r.rate / NULLIF(o.apply_rate, 0), 3),
       round(r.vpp / NULLIF(o.views_per_posting, 0), 3),
       COALESCE(r.vpp >= o.views_per_posting
                AND r.rate / NULLIF(o.apply_rate, 0) <= %(barrier_max_rate_ratio)s, FALSE)
FROM rated r
CROSS JOIN overall o
JOIN skill_names n ON n.skill_ref = r.skill_ref
"""

# ---------------------------------------------------------------------------------------------
# Signal 2: skill shortages
# ---------------------------------------------------------------------------------------------
SHORTAGE_SQL = f"""
INSERT INTO metrics.shortage_skill (week_end_date, snapshot_date, skill_ref, skill_name, is_mapped, is_casl,
    postings, shortage_postings, shortage_companies, shortage_share, overall_shortage_share, lift,
    median_repost_count, median_applications)
WITH {_POOL_SKILL}, {_SKILL_NAMES},
overall AS (
    SELECT count(*) FILTER (WHERE is_shortage)::numeric / NULLIF(count(*), 0) AS share
    FROM metrics.posting_week WHERE week_end_date = %(e)s
),
per_skill AS (
    SELECT ps.skill_ref, count(*) AS postings,
           count(*) FILTER (WHERE pw.is_shortage)                         AS shortage_postings,
           count(DISTINCT pw.company_name) FILTER (WHERE pw.is_shortage)  AS shortage_companies,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY pw.repost_count)       AS median_reposts,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY pw.applications_total) AS median_apps
    FROM pool_skill ps
    JOIN metrics.posting_week pw ON pw.week_end_date = %(e)s AND pw.job_uuid = ps.job_uuid
    GROUP BY ps.skill_ref
    HAVING count(*) >= %(skill_min_postings)s
       AND count(*) FILTER (WHERE pw.is_shortage) >= %(shortage_min_flagged)s
       AND count(DISTINCT pw.company_name) FILTER (WHERE pw.is_shortage) >= %(min_companies)s
)
SELECT %(e)s, %(s)s, p.skill_ref, n.skill_name, n.is_mapped, n.is_casl,
       p.postings, p.shortage_postings, p.shortage_companies,
       round(p.shortage_postings::numeric / p.postings, 4),
       round(o.share, 4),
       round((p.shortage_postings::numeric / p.postings) / NULLIF(o.share, 0), 3),
       round(p.median_reposts::numeric, 1), round(p.median_apps::numeric, 1)
FROM per_skill p
CROSS JOIN overall o
JOIN skill_names n ON n.skill_ref = p.skill_ref
"""

# ---------------------------------------------------------------------------------------------
# Signal 3: unrealistic requirements (two flags, long format)
# ---------------------------------------------------------------------------------------------
UNREALISTIC_SQL = f"""
INSERT INTO metrics.unrealistic_requirements (week_end_date, snapshot_date, flag, group_type, group_key,
    group_label, base_postings, flagged_postings, flagged_share, flagged_salary_mid_median,
    other_salary_mid_median, rule)
WITH {_MAJOR_TITLES},
base AS (
    SELECT 'reposted_many_apps' AS flag, pw.is_reposted_many_apps AS flagged,
           pw.category, pw.ssoc_major_group, pw.position_level,
           CASE WHEN pw.salary_usable THEN pw.salary_mid END AS salary_mid
    FROM metrics.posting_week pw
    WHERE pw.week_end_date = %(e)s
    UNION ALL
    -- entry level and junior are separate flags: their shares differ several times over
    SELECT CASE WHEN pw.position_level = ANY(%(entry_levels)s) THEN 'entry_level_2plus_years'
                ELSE 'junior_2plus_years' END,
           pw.is_entry_level_2plus, pw.category, pw.ssoc_major_group, pw.position_level,
           CASE WHEN pw.salary_usable THEN pw.salary_mid END
    FROM metrics.posting_week pw
    WHERE pw.week_end_date = %(e)s
      AND (pw.position_level = ANY(%(entry_levels)s) OR pw.position_level = ANY(%(junior_levels)s))
      AND NOT pw.exp_out_of_range AND pw.min_years_exp <= %(exp_max_valid)s
),
grouped AS (
    SELECT b.flag, g.group_type, g.group_key, max(g.group_label) AS group_label,
           count(*) AS base_postings, count(*) FILTER (WHERE b.flagged) AS flagged_postings,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY b.salary_mid) FILTER (WHERE b.flagged)     AS flagged_mid,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY b.salary_mid) FILTER (WHERE NOT b.flagged) AS other_mid
    FROM base b
    LEFT JOIN major_titles mt ON mt.major_group = b.ssoc_major_group
    CROSS JOIN LATERAL (VALUES
        ('overall', 'all', 'All'),
        ('category', b.category, b.category),
        ('ssoc_major_group', b.ssoc_major_group::text, COALESCE(mt.title, 'SSOC major group ' || b.ssoc_major_group)),
        ('position_level', b.position_level, b.position_level)
    ) AS g (group_type, group_key, group_label)
    WHERE g.group_key IS NOT NULL
    GROUP BY b.flag, g.group_type, g.group_key
)
SELECT %(e)s, %(s)s, flag, group_type, group_key, group_label, base_postings, flagged_postings,
       round(flagged_postings::numeric / base_postings, 4),
       round(flagged_mid::numeric, 2), round(other_mid::numeric, 2),
       CASE flag WHEN 'reposted_many_apps' THEN %(rule_reposted)s
                 WHEN 'entry_level_2plus_years' THEN %(rule_entry)s ELSE %(rule_junior)s END
FROM grouped
WHERE group_type = 'overall' OR base_postings >= %(group_min_postings)s
"""


def _build_unrealistic(cur, p) -> dict:
    many = many_apps_threshold(cur, p)
    pct = round(p["many_apps_percentile"] * 100)
    def experience_rule(levels):
        return (f"position level {' or '.join(levels)} asking {p['entry_min_years']} or more years of experience "
                f"(values above {p['exp_max_valid']} left out as typing errors). Base: open postings at that level "
                f"with a usable experience value")

    rules = {
        "rule_reposted": (f"reposted (repost_count 1 or more) with applications_total {many} or more, the P{pct} "
                          f"of open postings this week. Base: all open postings"),
        "rule_entry": experience_rule(p["entry_levels"]),
        "rule_junior": experience_rule(p["junior_levels"]),
    }
    cur.execute(UNREALISTIC_SQL, {**p, **rules})
    return {"many_apps_threshold": many}


# ---------------------------------------------------------------------------------------------
# Signal 4: competitiveness (stock per open posting, flow per 24 hours)
# ---------------------------------------------------------------------------------------------
COMPETITIVENESS_SQL = f"""
INSERT INTO metrics.competitiveness (week_end_date, snapshot_date, week_start_date, group_type, group_key,
    group_label, postings, median_apps_per_day_since_posting, mean_apps_per_day_since_posting,
    flow_postings, flow_posting_days, flow_applications, flow_views,
    apps_per_posting_day_24h, views_per_posting_day_24h, apply_rate_flow)
WITH {_POOL_SKILL}, {_SKILL_NAMES}, {_MAJOR_TITLES},
min_support (group_type, min_postings) AS (
    VALUES ('overall', 1), ('mom_group', 1), ('ssoc_major_group', 1),
           ('ssoc', %(group_min_postings)s), ('category', %(group_min_postings)s), ('skill', %(skill_min_postings)s)
),
stock_rows AS (
    SELECT g.group_type, g.group_key, g.group_label, pw.apps_per_day_since_posting AS apd
    FROM metrics.posting_week pw
    LEFT JOIN major_titles mt ON mt.major_group = pw.ssoc_major_group
    CROSS JOIN LATERAL (VALUES
        ('overall', 'all', 'All postings'),
        ('mom_group', pw.mom_group, pw.mom_group),
        ('ssoc_major_group', pw.ssoc_major_group::text, COALESCE(mt.title, 'SSOC major group ' || pw.ssoc_major_group)),
        ('ssoc', pw.ssoc_code, COALESCE(pw.ssoc_title, pw.ssoc_code)),
        ('category', pw.category, pw.category)
    ) AS g (group_type, group_key, group_label)
    WHERE pw.week_end_date = %(e)s AND g.group_key IS NOT NULL
    UNION ALL
    SELECT 'skill', ps.skill_ref, n.skill_name, pw.apps_per_day_since_posting
    FROM pool_skill ps
    JOIN metrics.posting_week pw ON pw.week_end_date = %(e)s AND pw.job_uuid = ps.job_uuid
    JOIN skill_names n ON n.skill_ref = ps.skill_ref
),
stock AS (
    SELECT group_type, group_key, max(group_label) AS group_label, count(*) AS postings,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY apd) AS median_apd, avg(apd) AS mean_apd
    FROM stock_rows
    GROUP BY group_type, group_key
),
flow_version AS (
    -- the week's day-over-day increases per posting version. hours_since_prev is the real interval
    -- (17 to 28 hours, about 48 after a skipped day), so the sums are normalised to 24 hours below
    SELECT f.job_key, f.job_uuid, sum(f.applications_delta) AS apps, sum(f.views_delta) AS views,
           sum(f.hours_since_prev) AS hours
    FROM clean.fact_job_day f
    WHERE f.snapshot_date BETWEEN %(ws)s AND %(s)s
      AND NOT f.is_first_snapshot AND f.hours_since_prev > 0
    GROUP BY f.job_key, f.job_uuid
),
version_skill AS (
    SELECT DISTINCT b.job_key, COALESCE(s.ssg_skill_id, s.mcf_skill_uuid) AS skill_ref
    FROM clean.bridge_job_skill b
    JOIN clean.dim_skill s ON s.skill_key = b.skill_key
    WHERE b.job_key IN (SELECT job_key FROM flow_version)
),
flow_rows AS (
    SELECT g.group_type, g.group_key, fv.job_uuid, fv.apps, fv.views, fv.hours
    FROM flow_version fv
    JOIN clean.dim_job j ON j.job_key = fv.job_key
    CROSS JOIN LATERAL (VALUES
        ('overall', 'all'), ('mom_group', j.mom_group), ('ssoc_major_group', j.ssoc_major_group::text),
        ('ssoc', j.ssoc_code), ('category', j.category)
    ) AS g (group_type, group_key)
    WHERE g.group_key IS NOT NULL
    UNION ALL
    SELECT 'skill', vs.skill_ref, fv.job_uuid, fv.apps, fv.views, fv.hours
    FROM flow_version fv
    JOIN version_skill vs ON vs.job_key = fv.job_key
),
flow AS (
    SELECT group_type, group_key, count(DISTINCT job_uuid) AS flow_postings, sum(hours) AS hours,
           sum(apps) AS apps, sum(views) AS views
    FROM flow_rows
    GROUP BY group_type, group_key
)
SELECT %(e)s, %(s)s, %(ws)s, st.group_type, st.group_key, st.group_label, st.postings,
       round(st.median_apd::numeric, 4), round(st.mean_apd, 4),
       fl.flow_postings, round(fl.hours / 24.0, 1), fl.apps, fl.views,
       round(24.0 * fl.apps / NULLIF(fl.hours, 0), 4),
       round(24.0 * fl.views / NULLIF(fl.hours, 0), 4),
       round(fl.apps::numeric / NULLIF(fl.views, 0), 5)
FROM stock st
JOIN min_support ms ON ms.group_type = st.group_type
LEFT JOIN flow fl ON fl.group_type = st.group_type AND fl.group_key = st.group_key
WHERE st.postings >= ms.min_postings
"""

# ---------------------------------------------------------------------------------------------
# Signal 5a: skill demand by posting week
# ---------------------------------------------------------------------------------------------
SKILL_DEMAND_SQL = f"""
INSERT INTO metrics.skill_demand (week_end_date, snapshot_date, skill_ref, skill_name, is_mapped, posting_week,
    week_fully_observed, new_postings, new_vacancies, week_total_postings, share_of_week, salary_mid_median)
WITH {_SKILL_NAMES},
seen AS (
    -- every posting seen up to the snapshot, in its latest version on or before it (open or since closed)
    SELECT DISTINCT ON (j.job_uuid) j.job_uuid, j.job_key, j.original_posting_date, j.vacancies,
           CASE WHEN j.salary_min >= %(salary_floor)s AND j.salary_max >= j.salary_min
                     AND j.salary_max <= %(salary_ceiling)s THEN j.salary_mid END AS salary_mid
    FROM clean.dim_job j
    WHERE j.valid_from <= %(s)s
    ORDER BY j.job_uuid, j.valid_from DESC
),
recent AS (
    SELECT s.*, date_trunc('week', s.original_posting_date::timestamp)::date AS posting_week
    FROM seen s
    WHERE s.original_posting_date BETWEEN %(demand_from)s AND %(s)s
),
week_totals AS (
    SELECT posting_week, count(*) AS total FROM recent GROUP BY posting_week
),
job_skill AS (
    SELECT DISTINCT r.job_uuid, r.posting_week, r.vacancies, r.salary_mid,
           COALESCE(sk.ssg_skill_id, sk.mcf_skill_uuid) AS skill_ref
    FROM recent r
    JOIN clean.bridge_job_skill b ON b.job_key = r.job_key
    JOIN clean.dim_skill sk ON sk.skill_key = b.skill_key
),
per AS (
    SELECT skill_ref, posting_week, count(*) AS new_postings, sum(vacancies) AS new_vacancies,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY salary_mid) AS mid,
           sum(count(*)) OVER (PARTITION BY skill_ref) AS skill_total
    FROM job_skill
    GROUP BY skill_ref, posting_week
)
SELECT %(e)s, %(s)s, p.skill_ref, n.skill_name, n.is_mapped, p.posting_week,
       (p.posting_week >= %(first_snapshot)s AND p.posting_week + 6 <= %(s)s),
       p.new_postings, p.new_vacancies, w.total, round(p.new_postings::numeric / w.total, 5),
       round(p.mid::numeric, 2)
FROM per p
JOIN week_totals w ON w.posting_week = p.posting_week
JOIN skill_names n ON n.skill_ref = p.skill_ref
WHERE p.skill_total >= %(demand_min_postings)s
"""

# ---------------------------------------------------------------------------------------------
# Signal 5b: salary per skill (open postings, monthly, as advertised)
# ---------------------------------------------------------------------------------------------
SKILL_SALARY_SQL = f"""
INSERT INTO metrics.skill_salary (week_end_date, snapshot_date, skill_ref, skill_name, is_mapped, postings,
    vacancies, salary_min_median, salary_max_median, salary_mid_p25, salary_mid_median, salary_mid_p75,
    mid_vs_overall)
WITH {_POOL_SKILL}, {_SKILL_NAMES},
paid AS (
    SELECT job_uuid, vacancies, salary_min, salary_max, salary_mid
    FROM metrics.posting_week
    WHERE week_end_date = %(e)s AND salary_usable
),
overall AS (
    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY salary_mid) AS mid FROM paid
),
per_skill AS (
    SELECT ps.skill_ref, count(*) AS postings, sum(p.vacancies) AS vacancies,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY p.salary_min)                 AS min_med,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY p.salary_max)                 AS max_med,
           percentile_cont(ARRAY[0.25, 0.5, 0.75]) WITHIN GROUP (ORDER BY p.salary_mid) AS mid_q
    FROM pool_skill ps
    JOIN paid p ON p.job_uuid = ps.job_uuid
    GROUP BY ps.skill_ref
    HAVING count(*) >= %(salary_min_postings)s
)
SELECT %(e)s, %(s)s, p.skill_ref, n.skill_name, n.is_mapped, p.postings, p.vacancies,
       round(p.min_med::numeric, 2), round(p.max_med::numeric, 2),
       round(p.mid_q[1]::numeric, 2), round(p.mid_q[2]::numeric, 2), round(p.mid_q[3]::numeric, 2),
       round(p.mid_q[2]::numeric / NULLIF(o.mid::numeric, 0), 3)
FROM per_skill p
CROSS JOIN overall o
JOIN skill_names n ON n.skill_ref = p.skill_ref
"""

def _sql_step(sql):
    def build(cur, p):
        cur.execute(sql, p)
        return {}
    return build


# Order matters only for posting_week (first). The signal builders read it and nothing else in metrics.
BUILDERS = {
    "posting_week": _build_posting_week,
    "barrier_skill": _sql_step(BARRIER_SQL),
    "shortage_skill": _sql_step(SHORTAGE_SQL),
    "unrealistic_requirements": _build_unrealistic,
    "competitiveness": _sql_step(COMPETITIVENESS_SQL),
    "skill_demand": _sql_step(SKILL_DEMAND_SQL),
    "skill_salary": _sql_step(SKILL_SALARY_SQL),
}

# ---------------------------------------------------------------------------------------------
# Checks on the week's rows (same Check / evaluate as quality/clean_checks.py, %(e)s = week_end_date)
# ---------------------------------------------------------------------------------------------
METRIC_CHECKS = [
    Check("posting_week_rows", "schema", "fail",
          "SELECT count(*) FROM metrics.posting_week WHERE week_end_date = %(e)s",
          ">=", 1, "the week has open postings"),
    Check("competitiveness_overall_row", "schema", "fail",
          "SELECT count(*) FROM metrics.competitiveness WHERE week_end_date = %(e)s AND group_type = 'overall'",
          "==", 1, "exactly one overall competitiveness row"),
    *[Check(f"{t}_rows", "schema", "warn", f"SELECT count(*) FROM metrics.{t} WHERE week_end_date = %(e)s",
            ">=", 1, f"{t} has rows (none means no skill reached the minimum sample size)")
      for t in ("barrier_skill", "shortage_skill", "skill_demand", "skill_salary", "unrealistic_requirements")],
    Check("shares_outside_0_1", "range", "fail",
          """SELECT (SELECT count(*) FROM metrics.shortage_skill WHERE week_end_date = %(e)s
                       AND (shortage_share NOT BETWEEN 0 AND 1 OR lift < 0))
                  + (SELECT count(*) FROM metrics.unrealistic_requirements WHERE week_end_date = %(e)s
                       AND (flagged_share NOT BETWEEN 0 AND 1 OR flagged_postings > base_postings))
                  + (SELECT count(*) FROM metrics.skill_demand WHERE week_end_date = %(e)s
                       AND share_of_week NOT BETWEEN 0 AND 1)""",
          "==", 0, "every share is between 0 and 1 and no group has more flagged than base postings"),
    Check("negative_rates", "range", "fail",
          """SELECT (SELECT count(*) FROM metrics.competitiveness WHERE week_end_date = %(e)s
                       AND (apps_per_posting_day_24h < 0 OR median_apps_per_day_since_posting < 0 OR apply_rate_flow < 0))
                  + (SELECT count(*) FROM metrics.barrier_skill WHERE week_end_date = %(e)s
                       AND (apply_rate < 0 OR rate_vs_overall < 0))""",
          "==", 0, "applications per day and per view are never negative"),
    Check("carried_forward_share", "business_rule", "warn",
          """SELECT avg(CASE WHEN seen_on_snapshot THEN 0.0 ELSE 1.0 END) FROM metrics.posting_week
             WHERE week_end_date = %(e)s""",
          "<=", 0.02, "more than 2% of open postings skipped by the crawl on the snapshot day"),
    Check("snapshot_is_week_end", "business_rule", "warn",
          "SELECT CASE WHEN snapshot_is_week_end THEN 1 ELSE 0 END FROM metrics.run_log WHERE week_end_date = %(e)s",
          "==", 1, "the week-end Tuesday was not in the clean layer yet, an earlier day of the week was used "
                   "(see fallback_reason)"),
    Check("snapshot_dq_failures", "business_rule", "warn",
          "SELECT snapshot_dq_failures FROM metrics.run_log WHERE week_end_date = %(e)s",
          "<=", 0, "the snapshot used failed a daily data-quality check (NULL: no checks were run on it)"),
    Check("flow_apps_per_posting_day_24h", "info", "info",
          """SELECT apps_per_posting_day_24h FROM metrics.competitiveness
             WHERE week_end_date = %(e)s AND group_type = 'overall'""",
          ">=", 0, "applications per open posting per 24 hours during the week"),
    Check("flow_days_per_posting", "info", "info",
          """SELECT flow_posting_days / NULLIF(flow_postings, 0) FROM metrics.competitiveness
             WHERE week_end_date = %(e)s AND group_type = 'overall'""",
          ">=", 0, "observed days per posting in the week (about 7 for a posting open all week)"),
]


# ---------------------------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------------------------
def loaded_snapshot_dates(cur) -> list[date]:
    """Snapshot days the daily transform has loaded into the clean layer (dim_date row plus fact rows)."""
    cur.execute("""
        SELECT d.date FROM clean.dim_date d
        WHERE EXISTS (SELECT 1 FROM clean.fact_job_day f WHERE f.snapshot_date = d.date)
        ORDER BY d.date
    """)
    return [r[0] for r in cur.fetchall()]


def resolve_week(conn, after, up_to, thresholds: dict | None = None) -> dict:
    """Read-only. The week (after, up_to] as Singapore dates (snapshot_week.week_bounds of the run's data
    interval), its latest snapshot (snapshot_week.latest_snapshot_in_week, the same rule as the MOM
    comparison), the crawl-skip window (metrics/weeks.py), the thresholds and the snapshot's daily DQ
    verdict. Raises snapshot_week.NoSnapshotInWeek when the week has no snapshot, before anything is written."""
    th = {**THRESHOLDS, **(thresholds or {})}
    try:
        with conn.cursor() as cur:
            snapshot = latest_snapshot_in_week(cur, after, up_to)
            week = week_context(loaded_snapshot_dates(cur), after, up_to, snapshot, th["max_gap_snapshots"])
            cur.execute("""
                SELECT CASE WHEN count(*) = 0 THEN NULL ELSE count(*) FILTER (WHERE status = 'fail') END
                FROM clean.dq_results WHERE snapshot_date = %s
            """, (week["snapshot_date"],))
            week["snapshot_dq_failures"] = cur.fetchone()[0]
    finally:
        conn.rollback()     # end the implicit read-only transaction
    week["thresholds"] = th
    return week


def sql_params(week: dict) -> dict:
    th = week["thresholds"]
    s = date.fromisoformat(week["snapshot_date"])
    monday = s - timedelta(days=s.weekday())
    return {
        **th,
        "e": date.fromisoformat(week["week_end_date"]),
        "ws": date.fromisoformat(week["week_start_date"]),
        "s": s,
        "pool_from": date.fromisoformat(week["pool_from_date"]),
        "pool_to": date.fromisoformat(week["pool_to_date"]),
        "first_snapshot": date.fromisoformat(week["first_snapshot_date"]),
        "demand_from": monday - timedelta(weeks=th["demand_weeks"] - 1),
    }


def start_run_log(conn, week: dict, run_id: str) -> None:
    """Record which snapshot is used for the week (and why) before anything is built."""
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO metrics.run_log (week_end_date, week_start_date, snapshot_date, snapshot_is_week_end,
                    fallback_reason, snapshots_in_week, pool_from_date, pool_to_date, snapshot_dq_failures,
                    thresholds, status, run_id, started_at)
                VALUES (%(week_end_date)s, %(week_start_date)s, %(snapshot_date)s, %(snapshot_is_week_end)s,
                    %(fallback_reason)s, %(snapshots_in_week)s::date[], %(pool_from_date)s, %(pool_to_date)s,
                    %(snapshot_dq_failures)s, %(thresholds_json)s, 'running', %(run_id)s, now())
                ON CONFLICT (week_end_date) DO UPDATE SET
                    week_start_date = EXCLUDED.week_start_date, snapshot_date = EXCLUDED.snapshot_date,
                    snapshot_is_week_end = EXCLUDED.snapshot_is_week_end, fallback_reason = EXCLUDED.fallback_reason,
                    snapshots_in_week = EXCLUDED.snapshots_in_week, pool_from_date = EXCLUDED.pool_from_date,
                    pool_to_date = EXCLUDED.pool_to_date, snapshot_dq_failures = EXCLUDED.snapshot_dq_failures,
                    thresholds = EXCLUDED.thresholds, status = 'running', run_id = EXCLUDED.run_id,
                    started_at = now(), pool_postings = NULL, carried_forward = NULL, steps = NULL,
                    checks = NULL, finished_at = NULL, seconds = NULL
            """, {**week, "thresholds_json": json.dumps(week["thresholds"]), "run_id": run_id})


def build_table(conn, name: str, week: dict) -> dict:
    """Rebuild one metrics table for one week in one transaction: delete the week's rows, insert them again."""
    if name not in BUILDERS:
        raise ValueError(f"unknown metrics table {name}")
    p = sql_params(week)
    t0 = time.perf_counter()
    with conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM metrics.{name} WHERE week_end_date = %(e)s", p)
            deleted = cur.rowcount
            details = BUILDERS[name](cur, p) or {}
            cur.execute(f"SELECT count(*) FROM metrics.{name} WHERE week_end_date = %(e)s", p)
            rows = cur.fetchone()[0]
    seconds = round(time.perf_counter() - t0, 3)
    stats = {"table": name, "rows": rows, "deleted_before_insert": deleted, "seconds": seconds, **details}
    print(f"[{name}] {seconds}s rows={rows} deleted_before_insert={deleted} {details or ''}")
    return stats


def run_metric_checks(conn, week_end, checks=METRIC_CHECKS) -> list:
    results = []
    with conn:
        with conn.cursor() as cur:
            for check in checks:
                cur.execute(check.sql, {"e": str(week_end)})
                row = cur.fetchone()
                results.append(evaluate(check, row[0] if row else None))
    return results


def finish_run_log(conn, week: dict, steps: list[dict], results) -> str:
    """Complete the week's run_log row. Returns the status written: 'ok' or 'failed_checks'."""
    status = "failed_checks" if any(r.status == "fail" for r in results) else "ok"
    pool = next((s for s in steps if s["table"] == "posting_week"), {})
    checks = [{"name": r.name, "category": r.category, "severity": r.severity, "status": r.status,
               "observed": r.observed, "threshold": r.threshold, "details": r.details} for r in results]
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE metrics.run_log SET pool_postings = %s, carried_forward = %s, steps = %s, checks = %s,
                    status = %s, finished_at = now(), seconds = %s
                WHERE week_end_date = %s
            """, (pool.get("rows"), pool.get("carried_forward"),
                  json.dumps({s["table"]: {k: v for k, v in s.items() if k != "table"} for s in steps}, default=str),
                  json.dumps(checks, default=str), status, round(sum(s["seconds"] for s in steps), 3),
                  week["week_end_date"]))
    return status


def run_week(conn, after, up_to, run_id: str | None = None, thresholds: dict | None = None) -> dict:
    """The whole signal build for the week (after, up_to] in one process: what the DAG's signal tasks do,
    task by task, for the CLI and tests."""
    run_id = run_id or f"cli-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    week = resolve_week(conn, after, up_to, thresholds)
    print(f"week ending {week['week_end_date']}: snapshot {week['snapshot_date']}"
          + (f" (fallback: {week['fallback_reason']})" if week["fallback_reason"] else ""))
    start_run_log(conn, week, run_id)
    steps = [build_table(conn, name, week) for name in BUILDERS]
    results = run_metric_checks(conn, week["week_end_date"])
    status = finish_run_log(conn, week, steps, results)
    return {"week": week, "steps": steps, "checks": results, "dq": summarise(results), "status": status}
