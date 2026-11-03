-- ext layer: data that widens the 10 days of MCF snapshots, kept apart from raw and clean.
--
--   1. ext.mcf2019_*              a 2019 MyCareersFuture crawl (Kaggle), for "2019 vs now" on skills and pay
--   2. ext.mom_occupational_wage  MOM Occupational Wage Survey, June 2024 (data.gov.sg), actual wages per SSOC
--      ext.mom_income_by_occupation  MOM Labour Force Survey median income by occupation group, 2001 to 2024
--   3. ext.posting_period, ext.posting_history_*   weekly history rebuilt from the posting dates inside our
--      own snapshots (derived from clean, not external, but it serves the same purpose: more time range)
--
-- Why a separate schema: these sources have their own grain, refresh cycle and licence. Keeping them out of
-- clean means the daily transform never depends on them, and dropping ext loses nothing the pipeline needs.
-- Every external load writes a row to ext.source_registry (where it came from, licence, when, checksum), so
-- the report can cite each number's source (L7: lineage).
--
-- MOM job vacancies are NOT here on purpose: they are clean.mom_vacancy_quarterly (03_mom_vacancies.sql).
--
-- Loaders (all idempotent: delete-then-insert per dataset, one transaction):
--   python -m ingestion.kaggle_mcf2019 data/ext/kaggle_mcf2019/mycareersfuture.json
--   python -m ingestion.datagov_wages
--   python -m transform.posting_history
-- This file runs on first container start after 01 to 03, and is safe to run again (IF NOT EXISTS,
-- CREATE OR REPLACE VIEW). On an existing warehouse apply it once by hand (README, "Extra data"):
--   docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/04_ext_schema.sql
--
-- Nothing in raw or clean depends on ext. The history tables point at clean.dim_job and clean.dim_skill with
-- ON DELETE CASCADE, so rebuilding clean also empties them (rerun python -m transform.posting_history after).
-- The views read clean.dim_job, so drop them first (DROP VIEW ext.v_now_posting CASCADE) before changing a
-- dim_job column type, then rerun this file.

CREATE SCHEMA IF NOT EXISTS ext;

-- ---------------------------------------------------------------------------------------------
-- Lineage: one row per external dataset, rewritten by its loader.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ext.source_registry (
    source_id       TEXT PRIMARY KEY,          -- 'kaggle_mcf_2019', 'mom_ows_2024', 'mom_lfs_income_occupation'
    title           TEXT NOT NULL,
    publisher       TEXT NOT NULL,
    url             TEXT NOT NULL,             -- the page a reader can open
    licence         TEXT NOT NULL,
    retrieved_at    TIMESTAMPTZ NOT NULL,      -- when the file was downloaded
    file_name       TEXT,
    file_sha256     TEXT,                      -- proves which copy of the file the numbers came from
    rows_in_file    INTEGER,
    rows_loaded     INTEGER NOT NULL,
    coverage_start  DATE,
    coverage_end    DATE,
    notes           TEXT,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------------------------
-- 1. MyCareersFuture postings crawled in 2019 (Kaggle dataset asrsaiteja/mycareerfuture-job-postings).
--    Grain: one posting. Posting dates are mostly 1 May to 18 Jun 2019, so it is one snapshot of the
--    postings open in mid June 2019. It has skills, salary and the application count at crawl time,
--    but no SSOC code and no views.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ext.mcf2019_posting (
    posting_key        TEXT PRIMARY KEY,       -- the MCF job id (JOB-2019-0098255), or 'h' + hash of the whole record
                                               -- for the 1,148 recruiter rows whose id the scraper lost
    job_id             TEXT,                   -- NULL when the file says 'Recruiter' or 'via Recruiter'
    source_row         INTEGER NOT NULL,       -- position in the file (the first copy of a duplicate is kept)
    is_recruiter_post  BOOLEAN NOT NULL,
    company_name       TEXT,
    title              TEXT NOT NULL,
    title_key          TEXT NOT NULL,          -- normalised title, used to borrow an SSOC code from today's postings
    employment_types   TEXT,                   -- '|' separated
    position_level     TEXT,                   -- first level
    position_levels    TEXT,                   -- all levels, '|' separated
    categories         TEXT,                   -- MCF categories rebuilt from the scraper's split, '|' separated
    min_years_exp      SMALLINT,
    salary_disclosed   BOOLEAN NOT NULL,       -- false for 'Salary undisclosed' (14%)
    salary_period      TEXT,                   -- 'Monthly' or 'Annually' as advertised
    salary_min         NUMERIC(12,2),          -- as advertised
    salary_max         NUMERIC(12,2),
    salary_monthly_min NUMERIC(12,2),          -- annual salaries divided by 12
    salary_monthly_max NUMERIC(12,2),
    salary_monthly_mid NUMERIC(12,2) GENERATED ALWAYS AS ((salary_monthly_min + salary_monthly_max) / 2) STORED,
    applications       INTEGER,                -- applications when the page was crawled
    posting_date       DATE,
    expiry_date        DATE,
    n_skills           SMALLINT NOT NULL,      -- always 20 or fewer: MCF capped the list at 20 in 2019
    ssoc_code          TEXT,                   -- borrowed from today's postings with the same title (see ssoc_match)
    ssoc_major_group   SMALLINT,
    ssoc_match         TEXT,                   -- 'title' or NULL
    ssoc_match_share   NUMERIC(4,3),           -- share of today's postings with that title that carry this SSOC
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS mcf2019_posting_ssoc_idx ON ext.mcf2019_posting (ssoc_code);

-- One row per distinct 2019 skill tag. tag_key is the tag in lower case with single spaces, which is
-- the same key as lower(clean.dim_skill.mcf_skill_name), because both years use MCF's own tag list.
-- The SkillsFuture columns come from the same SkillMatcher the daily transform uses.
CREATE TABLE IF NOT EXISTS ext.mcf2019_skill (
    tag_key          TEXT PRIMARY KEY,
    skill_name       TEXT NOT NULL,            -- first spelling seen
    norm_name        TEXT NOT NULL,            -- transform.skill_mapping.normalise
    ssg_skill_id     TEXT,
    ssg_skill_title  TEXT,
    match_method     TEXT NOT NULL,            -- 'exact', 'alias', 'stem', 'fuzzy', 'unmatched'
    match_confidence NUMERIC(4,3)
);

CREATE TABLE IF NOT EXISTS ext.mcf2019_posting_skill (
    posting_key  TEXT NOT NULL REFERENCES ext.mcf2019_posting (posting_key) ON DELETE CASCADE,
    tag_key      TEXT NOT NULL REFERENCES ext.mcf2019_skill (tag_key),
    PRIMARY KEY (posting_key, tag_key)
);
CREATE INDEX IF NOT EXISTS mcf2019_posting_skill_tag_idx ON ext.mcf2019_posting_skill (tag_key);

-- ---------------------------------------------------------------------------------------------
-- 2a. MOM Occupational Wage Survey, June 2024 (data.gov.sg d_9917e751f7498502f70052a940a3f312).
--     Grain: one occupation in one survey year. Residents only, private establishments with 25+ staff.
--     Gross wage = basic + overtime + commissions + allowances, before employee CPF, without employer
--     CPF and bonuses. That is the closest official match to an advertised monthly salary.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ext.mom_occupational_wage (
    year              SMALLINT NOT NULL,
    occupation        TEXT NOT NULL,           -- as published, lower case
    ssoc_code         TEXT,                    -- 5-digit SSOC 2020, matched on the title (all 562 in 2024)
    ssoc_major_group  SMALLINT,
    gross_wage_p25    NUMERIC(10,2),
    gross_wage_p50    NUMERIC(10,2),
    gross_wage_p75    NUMERIC(10,2),
    basic_wage_p25    NUMERIC(10,2),
    basic_wage_p50    NUMERIC(10,2),
    basic_wage_p75    NUMERIC(10,2),
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (year, occupation)
);
CREATE INDEX IF NOT EXISTS mom_occupational_wage_ssoc_idx ON ext.mom_occupational_wage (ssoc_code);

-- 2b. Median gross monthly income from employment of full-time employed residents, by occupation group and
--     sex, June, 2001 to 2024 (MOM Labour Force Survey, data.gov.sg d_8f024ddf2553d81ee00ede55b1d9b0ff).
--     Grain: one year x SSOC major group x sex. INCLUDES employer CPF and 1/12 of bonuses, so use it for
--     growth over time, not for levels against advertised salaries. 2005 and 2006 are not published.
CREATE TABLE IF NOT EXISTS ext.mom_income_by_occupation (
    year                SMALLINT NOT NULL,
    ssoc_major_group    SMALLINT NOT NULL,     -- 1 to 9 (6, agricultural workers, is not published)
    occupation_group    TEXT NOT NULL,         -- as published
    sex                 TEXT NOT NULL,         -- 'male' or 'female'
    median_income       NUMERIC(10,2) NOT NULL,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (year, ssoc_major_group, sex)
);

-- ---------------------------------------------------------------------------------------------
-- 3. Posting history rebuilt from our own snapshots (python -m transform.posting_history).
--
-- Every snapshot row carries original_posting_date, new_posting_date (moves on a repost) and
-- expiry_date. So the 24 Oct snapshot already tells us when each open posting was first posted, back
-- to March 2026. The catch is SURVIVORSHIP: we only see postings still open on 24 Oct or later.
-- A posting period lasts 7, 14, 21 or 30 days (expiry - new posting date), so every period that
-- started in the 6 days before 24 Oct is there, older weeks keep only the longer periods, and weeks
-- more than 30 days back keep only jobs that were reposted (the hard-to-fill ones). coverage_class
-- and est_coverage say how much of each week is really there. Never read older weeks as demand.
-- ---------------------------------------------------------------------------------------------

-- One row per posting period: a job's first posting or one of its reposts.
CREATE TABLE IF NOT EXISTS ext.posting_period (
    job_uuid               TEXT NOT NULL,
    new_posting_date       DATE NOT NULL,      -- start of this period
    original_posting_date  DATE NOT NULL,      -- first time the job was posted
    is_repost              BOOLEAN NOT NULL,   -- new_posting_date later than original_posting_date
    job_key                BIGINT NOT NULL REFERENCES clean.dim_job (job_key) ON DELETE CASCADE,
                                               -- first version seen in this period: its skills, SSOC and pay
    first_seen_date        DATE NOT NULL,      -- first snapshot that showed this period
    last_seen_date         DATE NOT NULL,
    expiry_date            DATE,
    duration_days          SMALLINT,           -- expiry_date - new_posting_date: 7, 14, 21 or 30
    ssoc_code              TEXT,
    ssoc_major_group       SMALLINT,
    mom_group              TEXT,
    salary_mid             NUMERIC(12,2),
    PRIMARY KEY (job_uuid, new_posting_date)
);
CREATE INDEX IF NOT EXISTS posting_period_new_date_idx ON ext.posting_period (new_posting_date);

-- One row per calendar week (Monday start) from the oldest original posting date to the last snapshot.
CREATE TABLE IF NOT EXISTS ext.posting_history_week (
    week_start           DATE PRIMARY KEY,
    first_postings       INTEGER NOT NULL,     -- jobs FIRST posted that week (original_posting_date) and seen by us
    periods_started      INTEGER NOT NULL,     -- posting periods (first posts and reposts) that started that week
    reposts_started      INTEGER NOT NULL,
    coverage_class       TEXT NOT NULL,        -- 'observed', 'near_complete', 'partial', 'survivors_only', 'in_progress'
    est_coverage         NUMERIC(5,3),         -- estimated share of the week's periods still open on the first snapshot
    periods_started_est  NUMERIC(10,1),        -- periods_started / coverage, day by day (NULL when not estimable)
    first_snapshot       DATE NOT NULL,
    last_snapshot        DATE NOT NULL,
    early_close_rate     NUMERIC(6,5),         -- share of open postings closed before expiry per day (measured)
    built_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Weekly counts per MCF skill tag (clean.dim_skill), so "skill demand by posting week" is one join.
CREATE TABLE IF NOT EXISTS ext.posting_history_skill_week (
    week_start       DATE NOT NULL,
    skill_key        INTEGER NOT NULL REFERENCES clean.dim_skill (skill_key) ON DELETE CASCADE,
    first_postings   INTEGER NOT NULL,
    periods_started  INTEGER NOT NULL,
    PRIMARY KEY (week_start, skill_key)
);
CREATE INDEX IF NOT EXISTS posting_history_skill_week_skill_idx ON ext.posting_history_skill_week (skill_key);

-- Weekly counts per SSOC major group (0 = no SSOC code on the posting).
CREATE TABLE IF NOT EXISTS ext.posting_history_ssoc_week (
    week_start        DATE NOT NULL,
    ssoc_major_group  SMALLINT NOT NULL,
    mom_group         TEXT,
    first_postings    INTEGER NOT NULL,
    periods_started   INTEGER NOT NULL,
    median_salary_mid NUMERIC(12,2),           -- of the periods that started that week
    PRIMARY KEY (week_start, ssoc_major_group)
);

-- ---------------------------------------------------------------------------------------------
-- Views for the report and the dashboard. "Now" = the postings in the latest snapshot.
-- ---------------------------------------------------------------------------------------------

-- Postings open on the latest snapshot, with the attributes as they were that day.
CREATE OR REPLACE VIEW ext.v_now_posting AS
SELECT j.*
FROM clean.fact_job_day f
JOIN clean.dim_job j ON j.job_key = f.job_key
WHERE f.snapshot_date = (SELECT max(snapshot_date) FROM clean.fact_job_day);

-- Top skills, 2019 vs now. share = share of postings listing the tag. 2019 postings list exactly 20
-- tags (MCF's cap then) against 14.5 now, so compare RANKS first and shares second.
CREATE OR REPLACE VIEW ext.v_skill_2019_vs_now AS
WITH p19 AS (SELECT count(*)::numeric AS n FROM ext.mcf2019_posting),
s19 AS (
    SELECT ps.tag_key, count(*) AS postings
    FROM ext.mcf2019_posting_skill ps
    GROUP BY ps.tag_key
),
pnow AS (SELECT count(*)::numeric AS n FROM ext.v_now_posting),
snow AS (
    SELECT lower(s.mcf_skill_name) AS tag_key, min(s.mcf_skill_name) AS skill_name,
           min(s.ssg_skill_title) AS ssg_skill_title, count(DISTINCT n.job_uuid) AS postings
    FROM ext.v_now_posting n
    JOIN clean.bridge_job_skill b ON b.job_key = n.job_key
    JOIN clean.dim_skill s ON s.skill_key = b.skill_key
    GROUP BY lower(s.mcf_skill_name)
)
SELECT coalesce(k.skill_name, snow.skill_name)                       AS skill_name,
       coalesce(s19.tag_key, snow.tag_key)                           AS tag_key,
       coalesce(k.ssg_skill_title, snow.ssg_skill_title)             AS ssg_skill_title,
       coalesce(s19.postings, 0)                                     AS postings_2019,
       round(coalesce(s19.postings, 0) / NULLIF(p19.n, 0), 4)        AS share_2019,
       CASE WHEN s19.postings IS NOT NULL THEN rank() OVER (ORDER BY s19.postings DESC NULLS LAST) END AS rank_2019,
       coalesce(snow.postings, 0)                                    AS postings_now,
       round(coalesce(snow.postings, 0) / NULLIF(pnow.n, 0), 4)      AS share_now,
       CASE WHEN snow.postings IS NOT NULL THEN rank() OVER (ORDER BY snow.postings DESC NULLS LAST) END AS rank_now
FROM s19
FULL OUTER JOIN snow ON snow.tag_key = s19.tag_key
LEFT JOIN ext.mcf2019_skill k ON k.tag_key = coalesce(s19.tag_key, snow.tag_key)
CROSS JOIN p19 CROSS JOIN pnow;

-- Median advertised monthly salary, 2019 vs now, overall and per MCF category. A posting counts in every
-- category it lists. Only disclosed salaries (2019) and salaries not hidden by the employer (now).
CREATE OR REPLACE VIEW ext.v_salary_2019_vs_now AS
WITH y19 AS (
    SELECT 'all'::text AS category, salary_monthly_mid AS mid FROM ext.mcf2019_posting WHERE salary_disclosed
    UNION ALL
    SELECT c.category, p.salary_monthly_mid
    FROM ext.mcf2019_posting p
    CROSS JOIN LATERAL unnest(string_to_array(p.categories, '|')) AS c(category)
    WHERE p.salary_disclosed
),
ynow AS (
    SELECT 'all'::text AS category, salary_mid AS mid FROM ext.v_now_posting WHERE NOT salary_is_hidden
    UNION ALL
    SELECT c.category, n.salary_mid
    FROM ext.v_now_posting n
    CROSS JOIN LATERAL unnest(string_to_array(n.categories, '|')) AS c(category)
    WHERE NOT n.salary_is_hidden
),
a AS (SELECT category, count(mid) AS n, percentile_cont(0.5) WITHIN GROUP (ORDER BY mid) AS med FROM y19 GROUP BY 1),
b AS (SELECT category, count(mid) AS n, percentile_cont(0.5) WITHIN GROUP (ORDER BY mid) AS med FROM ynow GROUP BY 1)
SELECT coalesce(a.category, b.category)       AS category,
       a.n                                    AS postings_2019,
       round(a.med::numeric, 0)               AS median_salary_2019,
       b.n                                    AS postings_now,
       round(b.med::numeric, 0)               AS median_salary_now,
       round((100 * (b.med / NULLIF(a.med, 0) - 1))::numeric, 1) AS change_pct
FROM a FULL OUTER JOIN b ON b.category = a.category;

-- Advertised pay change 2019 -> now per SSOC major group, next to the change in actual resident income
-- 2019 -> 2024 from the Labour Force Survey (male and female medians averaged). 2019 postings get their
-- SSOC from today's postings with the same title, so only matched postings count on the 2019 side.
CREATE OR REPLACE VIEW ext.v_pay_change_by_ssoc_group AS
WITH adv19 AS (
    SELECT ssoc_major_group AS g, count(*) AS n,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY salary_monthly_mid) AS med
    FROM ext.mcf2019_posting WHERE salary_disclosed AND ssoc_major_group IS NOT NULL GROUP BY 1
),
advnow AS (
    SELECT ssoc_major_group AS g, count(*) AS n,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY salary_mid) AS med
    FROM ext.v_now_posting WHERE NOT salary_is_hidden AND ssoc_major_group IS NOT NULL GROUP BY 1
),
lfs AS (
    SELECT ssoc_major_group AS g, min(occupation_group) AS occupation_group,
           avg(median_income) FILTER (WHERE year = 2019) AS inc_2019,
           avg(median_income) FILTER (WHERE year = 2024) AS inc_2024
    FROM ext.mom_income_by_occupation GROUP BY 1
)
SELECT coalesce(advnow.g, adv19.g)                            AS ssoc_major_group,
       lfs.occupation_group,
       adv19.n                                                AS postings_2019,
       round(adv19.med::numeric, 0)                           AS advertised_median_2019,
       advnow.n                                               AS postings_now,
       round(advnow.med::numeric, 0)                          AS advertised_median_now,
       round((100 * (advnow.med / NULLIF(adv19.med, 0) - 1))::numeric, 1) AS advertised_change_pct,
       round(lfs.inc_2019, 0)                                 AS resident_income_2019,
       round(lfs.inc_2024, 0)                                 AS resident_income_2024,
       round(100 * (lfs.inc_2024 / NULLIF(lfs.inc_2019, 0) - 1), 1) AS resident_income_change_pct
FROM advnow
FULL OUTER JOIN adv19 ON adv19.g = advnow.g
LEFT JOIN lfs ON lfs.g = coalesce(advnow.g, adv19.g);

-- Advertised salary vs actual wage per 5-digit SSOC occupation (OWS June 2024). One row per occupation
-- with at least one posting on the latest snapshot. ratio_mid_to_median > 1 means postings advertise
-- more than residents in that job are actually paid (in mid 2024).
CREATE OR REPLACE VIEW ext.v_advertised_vs_actual_wage AS
WITH adv AS (
    SELECT ssoc_code, count(*) AS postings,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY salary_min) AS med_min,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY salary_max) AS med_max,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY salary_mid) AS med_mid
    FROM ext.v_now_posting
    WHERE NOT salary_is_hidden AND ssoc_code IS NOT NULL
    GROUP BY ssoc_code
),
ows AS (
    SELECT DISTINCT ON (ssoc_code) ssoc_code, occupation, year, gross_wage_p25, gross_wage_p50, gross_wage_p75
    FROM ext.mom_occupational_wage WHERE ssoc_code IS NOT NULL
    ORDER BY ssoc_code, year DESC
)
SELECT adv.ssoc_code,
       ows.occupation,
       left(adv.ssoc_code, 1)::smallint                       AS ssoc_major_group,
       adv.postings,
       round(adv.med_min::numeric, 0)                          AS advertised_min_median,
       round(adv.med_max::numeric, 0)                          AS advertised_max_median,
       round(adv.med_mid::numeric, 0)                          AS advertised_mid_median,
       ows.year                                                AS wage_year,
       ows.gross_wage_p25, ows.gross_wage_p50, ows.gross_wage_p75,
       round((adv.med_mid / NULLIF(ows.gross_wage_p50, 0))::numeric, 3) AS ratio_mid_to_median
FROM adv JOIN ows ON ows.ssoc_code = adv.ssoc_code;

-- Share of each week's new postings that ask for a skill, with the week's coverage label beside it.
-- For trend charts keep coverage_class IN ('observed', 'near_complete') or est_coverage >= 0.5.
CREATE OR REPLACE VIEW ext.v_skill_week_share AS
SELECT sw.week_start, s.skill_key, s.mcf_skill_name, s.ssg_skill_title,
       sw.first_postings,
       round(sw.first_postings::numeric / NULLIF(w.first_postings, 0), 4) AS share_of_first_postings,
       sw.periods_started,
       round(sw.periods_started::numeric / NULLIF(w.periods_started, 0), 4) AS share_of_periods,
       w.coverage_class, w.est_coverage
FROM ext.posting_history_skill_week sw
JOIN ext.posting_history_week w ON w.week_start = sw.week_start
JOIN clean.dim_skill s ON s.skill_key = sw.skill_key;
