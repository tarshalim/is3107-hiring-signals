-- Dashboard marts: small pre-aggregated tables the Streamlit app reads (schema `mart`).
--
-- Why marts and not queries on clean.fact_job_day: the fact table grows by about 100k rows a day and
-- the main skill query joins it to the skill bridge (3 to 20 skills per posting), so one chart would scan
-- tens of millions of rows. Aggregating once a day (the transform DAG's last task) makes every dashboard
-- click a lookup on a few hundred thousand rows.
--
-- This file REBUILDS the whole schema (drop + create, in one transaction), so it is idempotent and a
-- changed definition needs no migration. It runs on first container start after 02_clean_schema.sql
-- (empty tables, instant) and daily via transform/refresh_marts.py. Readers block for the few seconds of
-- the rebuild transaction and never see a half-built schema.
--
-- Conventions
--   * "posting-day" = one row of fact_job_day. Counts are posting-days, so a 30-day sum divided by the
--     number of days is the average number of postings open on a day. Shares (skill posting-days over
--     all posting-days in the same slice) stay comparable when the number of snapshot days differs.
--   * A skill is a SkillsFuture skill when the MCF tag was mapped, otherwise the normalised MCF tag.
--     Several tags can map to one SkillsFuture skill, so job_skill de-duplicates (job, skill) first;
--     without that a posting listing two tags of one skill would count twice.
--   * Applications and views are the stored deltas normalised to 24 hours (delta * 24 / hours_since_prev),
--     because the interval between snapshots is not always a day. They are NULL on a posting's first
--     snapshot, so `rated_postings` is the right denominator for "per posting".
--   * Slice dimensions: mom_group (PMET etc.), ssoc_major_group (1 to 9, 0 = unknown), category (MCF).
--     Unknowns are kept as 'unknown' / 0 / 'Uncategorised' so filters never silently drop postings.

DROP SCHEMA IF EXISTS mart CASCADE;
CREATE SCHEMA mart;

-- ---------------------------------------------------------------------------------------------
-- Plain views (cheap, only used inside the materialised views below)
-- ---------------------------------------------------------------------------------------------

-- One row per (MCF skill tag, skill): maps a tag to the skill it is counted as.
CREATE VIEW mart.skill_map AS
SELECT skill_key,
       COALESCE(ssg_skill_id, 'mcf:' || norm_name) AS skill_id
FROM clean.dim_skill;

-- One row per skill: the lookup the dashboard joins for labels and filters.
CREATE VIEW mart.skill AS
SELECT COALESCE(ssg_skill_id, 'mcf:' || norm_name)       AS skill_id,
       min(COALESCE(ssg_skill_title, mcf_skill_name))    AS skill_label,
       min(COALESCE(ssg_skill_type, 'unmapped'))         AS skill_type,   -- 'tsc', 'ccs' or 'unmapped'
       bool_or(COALESCE(is_emerging, FALSE))             AS is_emerging,
       bool_or(COALESCE(is_casl, FALSE))                 AS is_casl,
       count(*)                                          AS mcf_tags
FROM clean.dim_skill
GROUP BY 1;

-- (posting version, skill) without duplicates.
CREATE VIEW mart.job_skill AS
SELECT DISTINCT b.job_key, m.skill_id
FROM clean.bridge_job_skill b
JOIN mart.skill_map m USING (skill_key);

-- One row per posting-day with the slice dimensions and the per-24h rates.
CREATE VIEW mart.job_day AS
SELECT f.snapshot_date,
       f.job_uuid,
       f.job_key,
       COALESCE(j.mom_group, 'unknown')                  AS mom_group,
       COALESCE(j.ssoc_major_group, 0)::smallint         AS ssoc_major_group,
       COALESCE(j.category, 'Uncategorised')             AS category,
       COALESCE(j.ssoc_code, '?')                        AS ssoc_code,
       j.ssoc_title,
       CASE WHEN f.applications_delta IS NOT NULL AND f.views_delta IS NOT NULL AND f.hours_since_prev > 0
            THEN f.applications_delta * 24.0 / f.hours_since_prev END AS applications_per_day,
       CASE WHEN f.applications_delta IS NOT NULL AND f.views_delta IS NOT NULL AND f.hours_since_prev > 0
            THEN f.views_delta * 24.0 / f.hours_since_prev END        AS views_per_day,
       COALESCE(f.reposted_since_prev, FALSE)            AS reposted_since_prev,
       f.days_since_original_posting,
       COALESCE(j.vacancies, 1)                          AS vacancies,
       -- salary_mid only when disclosed and plausible as a monthly figure (drops 0, 1 and 999999 typos)
       CASE WHEN NOT j.salary_is_hidden AND j.salary_min > 0 AND j.salary_max >= j.salary_min
                 AND j.salary_mid BETWEEN 300 AND 150000
            THEN j.salary_mid END                        AS salary_mid
FROM clean.fact_job_day f
JOIN clean.dim_job j USING (job_key);

-- ---------------------------------------------------------------------------------------------
-- Materialised views
-- ---------------------------------------------------------------------------------------------

-- Denominators and market-level activity per day and slice.
CREATE MATERIALIZED VIEW mart.slice_day AS
SELECT snapshot_date, mom_group, ssoc_major_group, category,
       count(*)                                          AS postings,
       count(applications_per_day)                       AS rated_postings,
       COALESCE(sum(applications_per_day), 0)            AS applications_per_day,
       COALESCE(sum(views_per_day), 0)                   AS views_per_day,
       count(*) FILTER (WHERE reposted_since_prev)       AS reposts
FROM mart.job_day
GROUP BY snapshot_date, mom_group, ssoc_major_group, category;
CREATE INDEX slice_day_date_idx ON mart.slice_day (snapshot_date);

-- Skill demand per day and slice: the source of the trending-skills tab and the difficulty-by-skill chart.
CREATE MATERIALIZED VIEW mart.skill_slice_day AS
SELECT jd.snapshot_date, js.skill_id, jd.mom_group, jd.ssoc_major_group, jd.category,
       count(*)                                          AS postings,
       count(jd.applications_per_day)                    AS rated_postings,
       COALESCE(sum(jd.applications_per_day), 0)         AS applications_per_day,
       COALESCE(sum(jd.views_per_day), 0)                AS views_per_day,
       count(*) FILTER (WHERE jd.reposted_since_prev)    AS reposts
FROM mart.job_day jd
JOIN mart.job_skill js USING (job_key)
GROUP BY jd.snapshot_date, js.skill_id, jd.mom_group, jd.ssoc_major_group, jd.category;
CREATE INDEX skill_slice_day_date_idx  ON mart.skill_slice_day (snapshot_date);
CREATE INDEX skill_slice_day_skill_idx ON mart.skill_slice_day (skill_id, snapshot_date);

-- Hiring difficulty per occupation (SSOC code) per day.
CREATE MATERIALIZED VIEW mart.occupation_day AS
SELECT snapshot_date, ssoc_code,
       min(ssoc_title)                                   AS ssoc_title,
       min(mom_group)                                    AS mom_group,
       count(*)                                          AS postings,
       sum(vacancies)                                    AS vacancies,
       count(applications_per_day)                       AS rated_postings,
       COALESCE(sum(applications_per_day), 0)            AS applications_per_day,
       COALESCE(sum(views_per_day), 0)                   AS views_per_day,
       count(*) FILTER (WHERE reposted_since_prev)       AS reposts,
       avg(days_since_original_posting)                  AS avg_days_open
FROM mart.job_day
GROUP BY snapshot_date, ssoc_code;
CREATE INDEX occupation_day_date_idx ON mart.occupation_day (snapshot_date);

-- ---------------------------------------------------------------------------------------------
-- Latest-snapshot marts: salaries and skill combinations describe the postings open today.
-- ---------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW mart.occupation_latest AS
WITH latest AS (SELECT max(snapshot_date) AS d FROM clean.fact_job_day)
SELECT jd.ssoc_code,
       min(jd.ssoc_title)                                AS ssoc_title,
       min(jd.mom_group)                                 AS mom_group,
       count(*)                                          AS postings,
       count(jd.salary_mid)                              AS salary_postings,
       percentile_cont(0.25) WITHIN GROUP (ORDER BY jd.salary_mid) AS salary_p25,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY jd.salary_mid) AS salary_p50,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY jd.salary_mid) AS salary_p75
FROM mart.job_day jd
JOIN latest ON jd.snapshot_date = latest.d
GROUP BY jd.ssoc_code;

-- Which skills each occupation asks for (skills in only one posting are dropped as noise).
CREATE MATERIALIZED VIEW mart.occupation_skill_latest AS
WITH latest AS (SELECT max(snapshot_date) AS d FROM clean.fact_job_day)
SELECT jd.ssoc_code, js.skill_id, count(*) AS postings
FROM mart.job_day jd
JOIN latest ON jd.snapshot_date = latest.d
JOIN mart.job_skill js USING (job_key)
GROUP BY jd.ssoc_code, js.skill_id
HAVING count(*) >= 2;
CREATE INDEX occupation_skill_latest_skill_idx ON mart.occupation_skill_latest (skill_id);

CREATE MATERIALIZED VIEW mart.skill_salary_latest AS
WITH latest AS (SELECT max(snapshot_date) AS d FROM clean.fact_job_day)
SELECT js.skill_id,
       count(*)                                          AS postings,
       count(jd.salary_mid)                              AS salary_postings,
       percentile_cont(0.25) WITHIN GROUP (ORDER BY jd.salary_mid) AS salary_p25,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY jd.salary_mid) AS salary_p50,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY jd.salary_mid) AS salary_p75
FROM mart.job_day jd
JOIN latest ON jd.snapshot_date = latest.d
JOIN mart.job_skill js USING (job_key)
GROUP BY js.skill_id;
CREATE INDEX skill_salary_latest_skill_idx ON mart.skill_salary_latest (skill_id);

-- Salary of skill pairs, restricted to the 40 most requested skills (all pairs would be millions of rows).
CREATE MATERIALIZED VIEW mart.skill_pair_latest AS
WITH latest AS (SELECT max(snapshot_date) AS d FROM clean.fact_job_day),
cur AS MATERIALIZED (
    SELECT jd.job_key, jd.salary_mid
    FROM mart.job_day jd JOIN latest ON jd.snapshot_date = latest.d
),
cur_skill AS MATERIALIZED (
    SELECT js.job_key, js.skill_id
    FROM mart.job_skill js
    WHERE js.job_key IN (SELECT job_key FROM cur)
),
top_skill AS MATERIALIZED (
    SELECT skill_id FROM cur_skill GROUP BY skill_id ORDER BY count(*) DESC, skill_id LIMIT 40
),
cur_top AS MATERIALIZED (
    SELECT cs.job_key, cs.skill_id FROM cur_skill cs JOIN top_skill USING (skill_id)
)
SELECT a.skill_id AS skill_a, b.skill_id AS skill_b,
       count(*)                                          AS postings,
       count(c.salary_mid)                               AS salary_postings,
       percentile_cont(0.25) WITHIN GROUP (ORDER BY c.salary_mid) AS salary_p25,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY c.salary_mid) AS salary_p50,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY c.salary_mid) AS salary_p75
FROM cur_top a
JOIN cur_top b ON a.job_key = b.job_key AND a.skill_id < b.skill_id
JOIN cur c ON c.job_key = a.job_key
GROUP BY a.skill_id, b.skill_id;
