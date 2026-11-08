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
       -- MCF fills a missing SSIC with the code '00000' as its description too: that is no industry either
       CASE WHEN COALESCE(j.company_ssic_desc, '') ~ '^[0-9 ]*$' THEN 'Unknown industry'
            ELSE j.company_ssic_desc END                 AS industry,
       -- an agency posting for a client counts for the client, which is who is actually hiring
       COALESCE(NULLIF(COALESCE(NULLIF(j.hiring_company_name, ''), j.company_name), ''), 'Unknown employer') AS employer,
       COALESCE(j.position_level, 'Unknown')             AS position_level,
       j.min_years_exp,
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

-- Postings per employer industry (SSIC description of the posting company) per day and slice.
-- No category column: industry here is the employer's, MCF's category is the job's.
CREATE MATERIALIZED VIEW mart.industry_slice_day AS
SELECT snapshot_date, industry, mom_group, ssoc_major_group,
       count(*)                                          AS postings,
       count(applications_per_day)                       AS rated_postings,
       COALESCE(sum(applications_per_day), 0)            AS applications_per_day
FROM mart.job_day
GROUP BY snapshot_date, industry, mom_group, ssoc_major_group;
CREATE INDEX industry_slice_day_date_idx ON mart.industry_slice_day (snapshot_date);

-- ---------------------------------------------------------------------------------------------
-- Latest-snapshot marts: salaries and skill combinations describe the postings open today.
-- ---------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW mart.industry_latest AS
WITH latest AS (SELECT max(snapshot_date) AS d FROM clean.fact_job_day)
SELECT jd.industry,
       count(*)                                          AS postings,
       sum(jd.vacancies)                                 AS vacancies,
       count(jd.salary_mid)                              AS salary_postings,
       percentile_cont(0.25) WITHIN GROUP (ORDER BY jd.salary_mid) AS salary_p25,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY jd.salary_mid) AS salary_p50,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY jd.salary_mid) AS salary_p75
FROM mart.job_day jd
JOIN latest ON jd.snapshot_date = latest.d
GROUP BY jd.industry;

-- Skills asked for by the 40 largest industries (a skill in a single posting is dropped as noise).
CREATE MATERIALIZED VIEW mart.industry_skill_latest AS
WITH latest AS (SELECT max(snapshot_date) AS d FROM clean.fact_job_day),
cur AS MATERIALIZED (
    SELECT jd.job_key, jd.industry FROM mart.job_day jd JOIN latest ON jd.snapshot_date = latest.d
),
top_industry AS MATERIALIZED (
    SELECT industry FROM cur GROUP BY industry ORDER BY count(*) DESC, industry LIMIT 40
)
SELECT c.industry, js.skill_id, count(*) AS postings
FROM cur c
JOIN top_industry USING (industry)
JOIN mart.job_skill js USING (job_key)
GROUP BY c.industry, js.skill_id
HAVING count(*) >= 2;

-- Employer leaderboard (postings open on the latest day). Grain keeps the occupation-group slice so the
-- dashboard filters still apply; counts add up across the slices, medians would not, so none are stored.
CREATE MATERIALIZED VIEW mart.company_latest AS
WITH latest AS (SELECT max(snapshot_date) AS d FROM clean.fact_job_day)
SELECT jd.employer, jd.mom_group, jd.ssoc_major_group,
       min(jd.industry)                                  AS industry,
       count(*)                                          AS postings,
       sum(jd.vacancies)                                 AS vacancies,
       count(jd.applications_per_day)                    AS rated_postings,
       COALESCE(sum(jd.applications_per_day), 0)         AS applications_per_day
FROM mart.job_day jd
JOIN latest ON jd.snapshot_date = latest.d
WHERE jd.employer <> 'Unknown employer'
GROUP BY jd.employer, jd.mom_group, jd.ssoc_major_group;
CREATE INDEX company_latest_employer_idx ON mart.company_latest (employer);

-- Skills asked for by the 100 biggest employers.
CREATE MATERIALIZED VIEW mart.company_skill_latest AS
WITH latest AS (SELECT max(snapshot_date) AS d FROM clean.fact_job_day),
cur AS MATERIALIZED (
    SELECT jd.job_key, jd.employer FROM mart.job_day jd JOIN latest ON jd.snapshot_date = latest.d
    WHERE jd.employer <> 'Unknown employer'
),
top_employer AS MATERIALIZED (
    SELECT employer FROM cur GROUP BY employer ORDER BY count(*) DESC, employer LIMIT 100
)
SELECT c.employer, js.skill_id, count(*) AS postings
FROM cur c
JOIN top_employer USING (employer)
JOIN mart.job_skill js USING (job_key)
GROUP BY c.employer, js.skill_id
HAVING count(*) >= 2;

-- What moves pay: salary percentiles by years of experience asked, position level and number of skills listed.
CREATE MATERIALIZED VIEW mart.salary_driver_latest AS
WITH latest AS (SELECT max(snapshot_date) AS d FROM clean.fact_job_day),
cur AS MATERIALIZED (
    SELECT jd.job_key, jd.salary_mid, jd.min_years_exp, jd.position_level
    FROM mart.job_day jd JOIN latest ON jd.snapshot_date = latest.d
    WHERE jd.salary_mid IS NOT NULL
),
nskills AS MATERIALIZED (
    SELECT job_key, count(*) AS n FROM mart.job_skill
    WHERE job_key IN (SELECT job_key FROM cur) GROUP BY job_key
),
bucketed AS (
    SELECT 'experience' AS driver, cur.salary_mid,
           CASE WHEN min_years_exp = 0 THEN 'None' WHEN min_years_exp <= 2 THEN '1-2 years'
                WHEN min_years_exp <= 5 THEN '3-5 years' WHEN min_years_exp <= 9 THEN '6-9 years'
                ELSE '10+ years' END AS bucket,
           CASE WHEN min_years_exp = 0 THEN 1 WHEN min_years_exp <= 2 THEN 2
                WHEN min_years_exp <= 5 THEN 3 WHEN min_years_exp <= 9 THEN 4 ELSE 5 END AS sort
    FROM cur WHERE min_years_exp IS NOT NULL
    UNION ALL
    SELECT 'position_level', salary_mid, position_level, 0 FROM cur
    UNION ALL
    SELECT 'skill_count', cur.salary_mid,
           CASE WHEN COALESCE(n.n, 0) = 0 THEN 'No skills' WHEN n.n <= 2 THEN '1-2 skills'
                WHEN n.n <= 5 THEN '3-5 skills' WHEN n.n <= 9 THEN '6-9 skills'
                WHEN n.n <= 14 THEN '10-14 skills' ELSE '15+ skills' END,
           CASE WHEN COALESCE(n.n, 0) = 0 THEN 0 WHEN n.n <= 2 THEN 1 WHEN n.n <= 5 THEN 2
                WHEN n.n <= 9 THEN 3 WHEN n.n <= 14 THEN 4 ELSE 5 END
    FROM cur LEFT JOIN nskills n USING (job_key)
)
SELECT driver, bucket, min(sort) AS sort,
       count(*)                                          AS salary_postings,
       percentile_cont(0.25) WITHIN GROUP (ORDER BY salary_mid) AS salary_p25,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY salary_mid) AS salary_p50,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY salary_mid) AS salary_p75
FROM bucketed
GROUP BY driver, bucket;

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
