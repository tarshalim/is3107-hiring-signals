-- Metrics layer, part 2: the weekly hiring signals the dashboard reads (built by the signal tasks of
-- dags/weekly_metrics_dag.py). Part 1 is sql/init/03_mom_vacancies.sql (the metrics schema itself and
-- metrics.mcf_vs_mom_share, the MOM validation), so this file runs after 03 and after 04 (ext).
--
-- raw (API answer) -> clean (star schema, one row per posting per day) -> metrics (03 and this file).
-- The dashboard reads only this schema (Dr Lek: results are loaded to a separate location and read
-- from there, not computed on the fly from the warehouse).
--
-- Week: the same as the MOM comparison (transform/snapshot_week.py). Each weekly_metrics run covers its
-- Airflow data interval, Tuesday 14:00 SGT to the next Tuesday 14:00 SGT, so the snapshot days Wednesday
-- to Tuesday. Keys: every table starts its primary key with week_end_date, the Singapore date of the
-- interval end (the Tuesday), and carries snapshot_date, the snapshot the numbers were computed from:
-- the latest one in the week, normally that Tuesday (the same snapshot metrics.mcf_vs_mom_share uses).
-- A week with no snapshot fails the run and writes nothing.
--
-- Idempotency (L7): each builder runs DELETE ... WHERE week_end_date = X and then INSERT, in one
-- transaction per table, so rerunning a week replaces that week's rows and never duplicates them.
-- Other weeks are never touched.
--
-- No foreign keys into clean: the clean layer can be rebuilt from raw (new surrogate keys), and then
-- the affected weeks are simply rerun. job_key here is only a pointer for joins at build time.
--
-- This file is idempotent (IF NOT EXISTS everywhere). It runs on first container start from
-- /docker-entrypoint-initdb.d after 01 to 04. On an existing warehouse volume, apply it once by hand
-- (README, "Weekly metrics"):
--   docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/05_metrics_signals.sql

CREATE SCHEMA IF NOT EXISTS metrics;

-- ---------------------------------------------------------------------------------------------
-- run_log: one row per metrics week (upserted, so a rerun overwrites it). Written at the start of a
-- run (which snapshot was picked and why) and completed at the end (rows, seconds, checks).
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metrics.run_log (
    week_end_date         DATE PRIMARY KEY,         -- Singapore date of the run's data_interval_end (a Tuesday)
    week_start_date       DATE NOT NULL,            -- the day after data_interval_start (a Wednesday): week_end_date - 6
    snapshot_date         DATE NOT NULL,            -- snapshot used: the latest one in the week (transform/snapshot_week.py)
    snapshot_is_week_end  BOOLEAN NOT NULL,         -- false = the week-end day was not in clean yet, an earlier day was used
    fallback_reason       TEXT,
    snapshots_in_week     DATE[] NOT NULL,          -- loaded snapshot days from week_start_date to snapshot_date
    pool_from_date        DATE NOT NULL,            -- open-posting rule: a posting the crawl skipped on snapshot_date
    pool_to_date          DATE NOT NULL,            --   stays open if seen in [pool_from_date, snapshot_date) and (snapshot_date, pool_to_date]
    snapshot_dq_failures  INTEGER,                  -- 'fail' verdicts in clean.dq_results for snapshot_date (NULL: no checks ran)
    thresholds            JSONB NOT NULL,           -- every threshold the run used
    pool_postings         INTEGER,                  -- open postings in metrics.posting_week
    carried_forward       INTEGER,                  -- of those, skipped by the crawl on snapshot_date
    steps                 JSONB,                    -- per table: rows, seconds, rows deleted before insert, extras
    checks                JSONB,                    -- verdict of each metrics check
    status                TEXT NOT NULL,            -- 'running', 'ok', 'failed_checks'
    run_id                TEXT,                     -- Airflow run_id or 'cli-<timestamp>'
    started_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at           TIMESTAMPTZ,
    seconds               NUMERIC(10,3)             -- sum of the build steps
);

-- ---------------------------------------------------------------------------------------------
-- posting_week: a periodic snapshot fact (Kimball). Grain: ONE OPEN POSTING AT THE END OF ONE WEEK.
-- Every signal table below is an aggregate of it, so the signals share one definition of "open" and
-- one set of posting-level flags. Attributes are copied from the dim_job version live on
-- last_seen_date (denormalised so the dashboard can list example postings without touching clean).
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metrics.posting_week (
    week_end_date          DATE     NOT NULL,
    snapshot_date          DATE     NOT NULL,
    job_uuid               TEXT     NOT NULL,
    job_key                BIGINT   NOT NULL,       -- clean.dim_job version live on last_seen_date
    last_seen_date         DATE     NOT NULL,       -- latest sighting on or before snapshot_date
    seen_on_snapshot       BOOLEAN  NOT NULL,       -- false = skipped by the crawl on snapshot_date, still open
    title                  TEXT,
    company_name           TEXT,
    ssoc_code              TEXT,
    ssoc_title             TEXT,
    ssoc_major_group       SMALLINT,
    mom_group              TEXT,
    category               TEXT,
    position_level         TEXT,
    min_years_exp          SMALLINT,
    exp_out_of_range       BOOLEAN  NOT NULL,
    salary_min             NUMERIC(12,2),
    salary_max             NUMERIC(12,2),
    salary_mid             NUMERIC(12,2),
    salary_usable          BOOLEAN  NOT NULL,       -- monthly range inside thresholds.salary_floor..salary_ceiling
                                                    -- (below: hourly or daily rates typed as monthly, above: annual)
    vacancies              INTEGER,
    original_posting_date  DATE,
    new_posting_date       DATE,
    expiry_date            DATE,
    views_total            INTEGER  NOT NULL,       -- running totals on last_seen_date
    applications_total     INTEGER  NOT NULL,
    repost_count           SMALLINT NOT NULL,
    days_since_original_posting SMALLINT,
    apps_per_day_since_posting  NUMERIC(12,4),      -- applications_total / max(days since original posting, 1)
    views_per_day_since_posting NUMERIC(12,4),      -- views_total / max(days since original posting, 1)
    is_shortage            BOOLEAN  NOT NULL,       -- reposted repeatedly and still few applications
    is_reposted_many_apps  BOOLEAN  NOT NULL,       -- reposted although it draws many applications per day (week's P90)
    barrier_group          TEXT,                    -- barrier comparison group: 2-digit SSOC (e.g. '24') when it has
                                                    -- thresholds.barrier_group_min_postings postings, else 1-digit ('2')
    is_barrier_posting     BOOLEAN  NOT NULL,       -- views per day at or above its barrier_group's P75, but applications
                                                    -- per view at most thresholds.barrier_max_rate_ratio x the group's
    is_entry_2plus         BOOLEAN  NOT NULL,       -- "Fresh/entry level" position asking 2+ years
    is_junior_2plus        BOOLEAN  NOT NULL,       -- "Junior Executive" position asking 2+ years (kept apart: a far higher share)
    posted_by_agency       BOOLEAN  NOT NULL,       -- poster's SSIC starts with thresholds.agency_ssic_prefix (78, employment
                                                    -- agencies); left out of the shortage signal
    PRIMARY KEY (week_end_date, job_uuid)
);

-- ---------------------------------------------------------------------------------------------
-- Skill tables use skill_ref = the SkillsFuture skill id when the MCF tag is mapped, else the MCF
-- skill uuid. Two MCF tags mapped to the same SkillsFuture skill are one row, and a posting carrying
-- both counts once.
-- ---------------------------------------------------------------------------------------------

-- Signal 1, barrier skills: which skills sit in postings people look at a lot and then do not apply to,
-- compared with similar jobs? A posting-level flag (posting_week.is_barrier_posting, judged within its
-- posting_week.barrier_group: 2-digit SSOC, or 1-digit when the 2-digit group is small) and a lift, like shortage_skill.
CREATE TABLE IF NOT EXISTS metrics.barrier_skill (
    week_end_date       DATE    NOT NULL,
    snapshot_date       DATE    NOT NULL,
    skill_ref           TEXT    NOT NULL,
    skill_name          TEXT    NOT NULL,
    is_mapped           BOOLEAN NOT NULL,
    is_casl             BOOLEAN NOT NULL,
    postings            INTEGER NOT NULL,           -- open postings with the skill (at least thresholds.skill_min_postings)
    companies           INTEGER NOT NULL,           -- distinct employers among them (at least thresholds.min_companies)
    barrier_postings    INTEGER NOT NULL,           -- of those, posting_week.is_barrier_posting
    barrier_companies   INTEGER NOT NULL,           -- employers behind the barrier postings
    top_company_share   NUMERIC(6,4),               -- share of the barrier postings from the single biggest employer
                                                    -- (above thresholds.max_top_company_share: not flagged; NULL: none)
    barrier_share       NUMERIC(8,4),               -- barrier_postings / postings
    expected_share      NUMERIC(8,4),               -- the share if each posting behaved like its barrier_group's average
    lift                NUMERIC(10,3),              -- barrier_share / expected_share. 2 = twice as common as its occupations predict
    views               BIGINT  NOT NULL,
    applications        BIGINT  NOT NULL,
    apply_rate          NUMERIC(10,5),              -- applications / views for postings with the skill (descriptive)
    overall_apply_rate  NUMERIC(10,5),              -- applications / views over all open postings
    rate_vs_overall     NUMERIC(10,3),              -- apply_rate / overall_apply_rate. 0.6 = 40% fewer applications per view
    is_barrier          BOOLEAN NOT NULL,           -- lift >= thresholds.barrier_min_lift, with barrier_min_flagged barrier
                                                    -- postings from min_companies employers, no one employer above
                                                    -- max_top_company_share of them, and rate_vs_overall below
                                                    -- barrier_max_rate_vs_overall (fewer applications per view than
                                                    -- the whole market, not only than similar jobs)
    skill_type          TEXT,                       -- SkillsFuture type 'tsc' / 'ccs'; NULL when not mapped
    is_technical        BOOLEAN NOT NULL,           -- 'tsc', or not mapped and not in thresholds.soft_skill_tags
                                                    -- (the dashboard's default "technical skills only" view)
    PRIMARY KEY (week_end_date, skill_ref)
);

-- Signal 2, skill shortages: which skills sit in postings reposted again and again with few applicants?
-- Direct employers only: postings by employment agencies (posting_week.posted_by_agency) are left out of every
-- count and share here, because agencies keep evergreen ads up and repost them.
CREATE TABLE IF NOT EXISTS metrics.shortage_skill (
    week_end_date          DATE    NOT NULL,
    snapshot_date          DATE    NOT NULL,
    skill_ref              TEXT    NOT NULL,
    skill_name             TEXT    NOT NULL,
    is_mapped              BOOLEAN NOT NULL,
    is_casl                BOOLEAN NOT NULL,        -- SSG's own "needed by growth jobs" list, an outside check
    postings               INTEGER NOT NULL,
    shortage_postings      INTEGER NOT NULL,        -- of those, posting_week.is_shortage
    shortage_companies     INTEGER NOT NULL,        -- employers behind the shortage postings (at least thresholds.min_companies)
    top_company_share      NUMERIC(6,4),            -- share of the shortage postings from the single biggest employer
                                                    -- (a skill above thresholds.max_top_company_share is not listed)
    shortage_share         NUMERIC(8,4),            -- shortage_postings / postings
    overall_shortage_share NUMERIC(8,4),            -- the same over all open postings by direct employers
    lift                   NUMERIC(10,3),           -- shortage_share / overall_shortage_share. 2 = twice as common as usual
    median_repost_count    NUMERIC(8,1),            -- over the skill's shortage postings only
    median_applications    NUMERIC(10,1),           -- over the skill's shortage postings only
    skill_type             TEXT,                    -- SkillsFuture type 'tsc' / 'ccs'; NULL when not mapped
    is_technical           BOOLEAN NOT NULL,        -- as barrier_skill.is_technical
    PRIMARY KEY (week_end_date, skill_ref)
);

-- Signal 3, unrealistic requirements, three flags reported apart because they mean different things:
--   'reposted_many_apps'      : reposted although it draws many applications per day since first posted
--                               (the week's P90 or more; per day so older postings are not favoured)
--   'entry_level_2plus_years' : "Fresh/entry level" position asking for 2+ years of experience
--   'junior_2plus_years'      : "Junior Executive" position asking for 2+ years (a much higher share, so kept apart)
-- Long format: one row per flag and group (overall, category, SSOC major group, position level).
-- Salary medians use usable monthly salaries only (posting_week.salary_usable).
CREATE TABLE IF NOT EXISTS metrics.unrealistic_requirements (
    week_end_date             DATE    NOT NULL,
    snapshot_date             DATE    NOT NULL,
    flag                      TEXT    NOT NULL,
    group_type                TEXT    NOT NULL,     -- 'overall', 'category', 'ssoc_major_group', 'position_level'
    group_key                 TEXT    NOT NULL,
    group_label               TEXT,
    base_postings             INTEGER NOT NULL,     -- postings the flag could apply to (all open postings, or the entry/junior ones)
    flagged_postings          INTEGER NOT NULL,
    flagged_share             NUMERIC(8,4),
    flagged_salary_mid_median NUMERIC(12,2),
    other_salary_mid_median   NUMERIC(12,2),
    rule                      TEXT    NOT NULL,     -- the rule in words, with this week's numbers (e.g. the P90 value)
    PRIMARY KEY (week_end_date, flag, group_type, group_key)
);

-- Signal 4, competitiveness: which roles and skills get the most applicants per day?
--   stock : applications so far / days since the original posting date, per open posting (median and mean)
--   flow  : applications gained during the week, normalised to 24 hours with hours_since_prev, because
--           runs were 17 to 28 hours apart: 24 * sum(applications_delta) / sum(hours_since_prev)
CREATE TABLE IF NOT EXISTS metrics.competitiveness (
    week_end_date             DATE    NOT NULL,
    snapshot_date             DATE    NOT NULL,
    week_start_date           DATE    NOT NULL,
    group_type                TEXT    NOT NULL,     -- 'overall', 'mom_group', 'ssoc_major_group', 'ssoc', 'category', 'skill'
    group_key                 TEXT    NOT NULL,
    group_label               TEXT,
    postings                  INTEGER NOT NULL,     -- open postings in the group
    median_apps_per_day_since_posting NUMERIC(12,4),
    mean_apps_per_day_since_posting   NUMERIC(12,4),
    flow_postings             INTEGER,              -- postings with at least one day-over-day delta in the week
    flow_posting_days         NUMERIC(14,1),        -- sum(hours_since_prev) / 24
    flow_applications         BIGINT,
    flow_views                BIGINT,
    apps_per_posting_day_24h  NUMERIC(12,4),        -- 24 * flow_applications / sum(hours_since_prev)
    views_per_posting_day_24h NUMERIC(12,4),
    apply_rate_flow           NUMERIC(10,5),        -- flow_applications / flow_views
    PRIMARY KEY (week_end_date, group_type, group_key)
);

-- Signal 5a, skill demand: postings per skill by POSTING week (Monday of original_posting_date), from
-- every posting seen up to snapshot_date. Weeks before collection began only contain postings that
-- were still open when collection started, so week_fully_observed marks the weeks that are complete.
CREATE TABLE IF NOT EXISTS metrics.skill_demand (
    week_end_date        DATE    NOT NULL,
    snapshot_date        DATE    NOT NULL,
    skill_ref            TEXT    NOT NULL,
    skill_name           TEXT    NOT NULL,
    is_mapped            BOOLEAN NOT NULL,
    skill_type           TEXT,                      -- SkillsFuture type: 'tsc' (technical) or 'ccs' (critical core);
                                                    -- NULL when the MCF tag is not mapped (is_mapped false)
    posting_week         DATE    NOT NULL,          -- Monday of original_posting_date
    week_fully_observed  BOOLEAN NOT NULL,          -- collection covered the whole posting week
    new_postings         INTEGER NOT NULL,
    new_vacancies        INTEGER,
    week_total_postings  INTEGER NOT NULL,          -- all postings first posted that week
    share_of_week        NUMERIC(8,5),              -- new_postings / week_total_postings (comparable across weeks)
    salary_mid_median    NUMERIC(12,2),
    PRIMARY KEY (week_end_date, skill_ref, posting_week)
);

-- Signal 5b, skill salary: what do open postings asking for each skill pay (monthly, as advertised)?
-- Employer-weighted (8 Nov): every median and percentile is taken over the employers' own medians, so one
-- employer counts once however many postings it has.
CREATE TABLE IF NOT EXISTS metrics.skill_salary (
    week_end_date      DATE    NOT NULL,
    snapshot_date      DATE    NOT NULL,
    skill_ref          TEXT    NOT NULL,
    skill_name         TEXT    NOT NULL,
    is_mapped          BOOLEAN NOT NULL,
    postings           INTEGER NOT NULL,            -- open postings with the skill and a usable salary (posting_week.salary_usable)
    companies          INTEGER NOT NULL,            -- distinct employers among them (at least thresholds.min_companies)
    vacancies          INTEGER,
    salary_min_median  NUMERIC(12,2),               -- median across employers of each one's median advertised minimum
    salary_max_median  NUMERIC(12,2),               -- the same for the advertised maximum
    salary_mid_p25     NUMERIC(12,2),               -- P25, median and P75 across employers of each one's median midpoint
    salary_mid_median  NUMERIC(12,2),
    salary_mid_p75     NUMERIC(12,2),
    mid_vs_overall     NUMERIC(8,3),                -- salary_mid_median / the same employer-weighted median over all
                                                    -- open postings with a usable salary
    top_company_share  NUMERIC(6,4),                -- share of the postings from the single biggest employer
                                                    -- (a skill above thresholds.max_top_company_share is left out)
    PRIMARY KEY (week_end_date, skill_ref)
);

-- Summary by group: one row per job category and SSOC major group (and 'overall'), with the share of each
-- posting-level flag, so the dashboard can show which cause shows up where. Applications per day for the same
-- groups are in metrics.competitiveness (same group_type and group_key). Groups below
-- thresholds.group_min_postings postings are left out; a share whose base is below it is NULL.
CREATE TABLE IF NOT EXISTS metrics.group_signals (
    week_end_date          DATE    NOT NULL,
    snapshot_date          DATE    NOT NULL,
    group_type             TEXT    NOT NULL,        -- 'overall', 'category', 'ssoc_major_group'
    group_key              TEXT    NOT NULL,
    group_label            TEXT,
    postings               INTEGER NOT NULL,        -- open postings in the group
    agency_share           NUMERIC(8,4),            -- share posted by employment agencies (posted_by_agency)
    direct_postings        INTEGER NOT NULL,        -- open postings by direct employers: the shortage base
    shortage_postings      INTEGER NOT NULL,        -- of those, is_shortage
    shortage_share         NUMERIC(8,4),            -- shortage_postings / direct_postings
    barrier_postings       INTEGER NOT NULL,        -- is_barrier_posting (all posters)
    barrier_share          NUMERIC(8,4),            -- barrier_postings / postings
    entry_postings         INTEGER NOT NULL,        -- entry-level postings with a usable experience value
    entry_2plus_postings   INTEGER NOT NULL,        -- of those, is_entry_2plus
    entry_2plus_share      NUMERIC(8,4),            -- entry_2plus_postings / entry_postings
    salary_mid_median      NUMERIC(12,2),           -- median monthly midpoint, usable salaries only
    PRIMARY KEY (week_end_date, group_type, group_key)
);

-- The MOM validation is metrics.mcf_vs_mom_share (sql/init/03_mom_vacancies.sql), built by the
-- compare_mcf_with_mom task of the same DAG run from the same snapshot.

-- ---------------------------------------------------------------------------------------------
-- Upgrade of a warehouse created before 4 Nov 2026 (review fixes #6, #9, top_company_share). CREATE TABLE IF NOT
-- EXISTS above leaves an existing table as it is, so the new columns are added here. Every statement
-- is idempotent, and on a new warehouse they change nothing. Rows of old weeks get FALSE / NULL in the
-- new columns until those weeks are rerun (clear their weekly_metrics runs).
-- ---------------------------------------------------------------------------------------------
-- #9: one flag per position level instead of is_entry_level_2plus (entry level and junior mixed)
ALTER TABLE metrics.posting_week ADD COLUMN IF NOT EXISTS is_entry_2plus  BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE metrics.posting_week ADD COLUMN IF NOT EXISTS is_junior_2plus BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE metrics.posting_week ALTER COLUMN is_entry_2plus  DROP DEFAULT;
ALTER TABLE metrics.posting_week ALTER COLUMN is_junior_2plus DROP DEFAULT;
ALTER TABLE metrics.posting_week DROP COLUMN IF EXISTS is_entry_level_2plus;
-- #6: barrier skills on views per day, judged within the SSOC major group, with a lift
ALTER TABLE metrics.posting_week ADD COLUMN IF NOT EXISTS views_per_day_since_posting NUMERIC(12,4);
ALTER TABLE metrics.posting_week ADD COLUMN IF NOT EXISTS is_barrier_posting BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE metrics.posting_week ALTER COLUMN is_barrier_posting DROP DEFAULT;
ALTER TABLE metrics.barrier_skill ADD COLUMN IF NOT EXISTS barrier_postings  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics.barrier_skill ADD COLUMN IF NOT EXISTS barrier_companies INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics.barrier_skill ALTER COLUMN barrier_postings  DROP DEFAULT;
ALTER TABLE metrics.barrier_skill ALTER COLUMN barrier_companies DROP DEFAULT;
ALTER TABLE metrics.barrier_skill ADD COLUMN IF NOT EXISTS barrier_share  NUMERIC(8,4);
ALTER TABLE metrics.barrier_skill ADD COLUMN IF NOT EXISTS expected_share NUMERIC(8,4);
ALTER TABLE metrics.barrier_skill ADD COLUMN IF NOT EXISTS lift           NUMERIC(10,3);
ALTER TABLE metrics.barrier_skill DROP COLUMN IF EXISTS views_per_posting;   -- raw views per posting: favoured old postings
ALTER TABLE metrics.barrier_skill DROP COLUMN IF EXISTS views_vs_overall;
-- one bulk employer cannot make a barrier or shortage skill (thresholds.max_top_company_share)
ALTER TABLE metrics.barrier_skill  ADD COLUMN IF NOT EXISTS top_company_share NUMERIC(6,4);
ALTER TABLE metrics.shortage_skill ADD COLUMN IF NOT EXISTS top_company_share NUMERIC(6,4);
-- barrier compared within 2-digit SSOC groups (1-digit fallback)
ALTER TABLE metrics.posting_week ADD COLUMN IF NOT EXISTS barrier_group TEXT;
-- skill demand by SkillsFuture skill type, so technical skills can be shown apart
ALTER TABLE metrics.skill_demand ADD COLUMN IF NOT EXISTS skill_type TEXT;
-- shortage on direct employers only (thresholds.agency_ssic_prefix); metrics.group_signals is created above
ALTER TABLE metrics.posting_week ADD COLUMN IF NOT EXISTS posted_by_agency BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE metrics.posting_week ALTER COLUMN posted_by_agency DROP DEFAULT;
-- technical skills only by default on the dashboard (thresholds.soft_skill_tags, 8 Nov). Old weeks read FALSE
-- (shown only under "all skills") until they are rerun
ALTER TABLE metrics.barrier_skill  ADD COLUMN IF NOT EXISTS skill_type TEXT;
ALTER TABLE metrics.shortage_skill ADD COLUMN IF NOT EXISTS skill_type TEXT;
ALTER TABLE metrics.barrier_skill  ADD COLUMN IF NOT EXISTS is_technical BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE metrics.shortage_skill ADD COLUMN IF NOT EXISTS is_technical BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE metrics.barrier_skill  ALTER COLUMN is_technical DROP DEFAULT;
ALTER TABLE metrics.shortage_skill ALTER COLUMN is_technical DROP DEFAULT;
-- skill salary: one bulk employer cannot make a skill's pay either (thresholds.max_top_company_share, 8 Nov)
ALTER TABLE metrics.skill_salary ADD COLUMN IF NOT EXISTS top_company_share NUMERIC(6,4);
-- skill salary employer-weighted, with its employer count (8 Nov). Old weeks read 0 until they are rerun
ALTER TABLE metrics.skill_salary ADD COLUMN IF NOT EXISTS companies INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics.skill_salary ALTER COLUMN companies DROP DEFAULT;
