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
    is_shortage            BOOLEAN  NOT NULL,       -- reposted repeatedly and still few applications
    is_reposted_many_apps  BOOLEAN  NOT NULL,       -- reposted although it already had many applications
    is_entry_level_2plus   BOOLEAN  NOT NULL,       -- entry-level or junior position asking 2+ years
    PRIMARY KEY (week_end_date, job_uuid)
);

-- ---------------------------------------------------------------------------------------------
-- Skill tables use skill_ref = the SkillsFuture skill id when the MCF tag is mapped, else the MCF
-- skill uuid. Two MCF tags mapped to the same SkillsFuture skill are one row, and a posting carrying
-- both counts once.
-- ---------------------------------------------------------------------------------------------

-- Signal 1, barrier skills: which skills attract views but put applicants off?
CREATE TABLE IF NOT EXISTS metrics.barrier_skill (
    week_end_date       DATE    NOT NULL,
    snapshot_date       DATE    NOT NULL,
    skill_ref           TEXT    NOT NULL,
    skill_name          TEXT    NOT NULL,
    is_mapped           BOOLEAN NOT NULL,
    is_casl             BOOLEAN NOT NULL,
    postings            INTEGER NOT NULL,           -- open postings with the skill (at least thresholds.skill_min_postings)
    companies           INTEGER NOT NULL,           -- distinct employers among them (at least thresholds.min_companies)
    views               BIGINT  NOT NULL,
    applications        BIGINT  NOT NULL,
    views_per_posting   NUMERIC(12,1),
    apply_rate          NUMERIC(10,5),              -- applications / views for postings with the skill
    overall_apply_rate  NUMERIC(10,5),              -- applications / views over all open postings
    rate_vs_overall     NUMERIC(10,3),              -- apply_rate / overall_apply_rate. 0.6 = 40% fewer applications per view
    views_vs_overall    NUMERIC(10,3),              -- views_per_posting / overall views per posting
    is_barrier          BOOLEAN NOT NULL,           -- views_vs_overall >= 1 and rate_vs_overall <= thresholds.barrier_max_rate_ratio
    PRIMARY KEY (week_end_date, skill_ref)
);

-- Signal 2, skill shortages: which skills sit in postings reposted again and again with few applicants?
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
    shortage_share         NUMERIC(8,4),            -- shortage_postings / postings
    overall_shortage_share NUMERIC(8,4),            -- the same over all open postings
    lift                   NUMERIC(10,3),           -- shortage_share / overall_shortage_share. 2 = twice as common as usual
    median_repost_count    NUMERIC(8,1),
    median_applications    NUMERIC(10,1),
    PRIMARY KEY (week_end_date, skill_ref)
);

-- Signal 3, unrealistic requirements, three flags reported apart because they mean different things:
--   'reposted_many_apps'      : reposted although it already had many applications (P90 or more)
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
CREATE TABLE IF NOT EXISTS metrics.skill_salary (
    week_end_date      DATE    NOT NULL,
    snapshot_date      DATE    NOT NULL,
    skill_ref          TEXT    NOT NULL,
    skill_name         TEXT    NOT NULL,
    is_mapped          BOOLEAN NOT NULL,
    postings           INTEGER NOT NULL,            -- open postings with the skill and a usable salary (posting_week.salary_usable)
    vacancies          INTEGER,
    salary_min_median  NUMERIC(12,2),               -- median of the advertised minimum
    salary_max_median  NUMERIC(12,2),               -- median of the advertised maximum
    salary_mid_p25     NUMERIC(12,2),
    salary_mid_median  NUMERIC(12,2),
    salary_mid_p75     NUMERIC(12,2),
    mid_vs_overall     NUMERIC(8,3),                -- salary_mid_median / median over all open postings
    PRIMARY KEY (week_end_date, skill_ref)
);

-- The MOM validation is metrics.mcf_vs_mom_share (sql/init/03_mom_vacancies.sql), built by the
-- compare_mcf_with_mom task of the same DAG run from the same snapshot.
