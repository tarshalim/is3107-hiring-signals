-- Clean layer: a star schema for the hiring-difficulty dashboard (IS3107 L8, Kimball's four steps).
--
--   1. Business process : "a job posting is open on MyCareersFuture and people view and apply to it".
--   2. Grain            : ONE ROW OF fact_job_day = ONE POSTING ON ONE SNAPSHOT DAY.
--   3. Dimensions       : dim_job (what was posted, by whom, for which occupation), dim_date (when),
--                         dim_skill through bridge_job_skill (which skills the posting asks for).
--   4. Facts            : views_total, applications_total, repost_count, edit_count (running totals,
--                         semi-additive: never SUM them across days) and the increases since the previous
--                         snapshot, views_delta and applications_delta (additive: SUM across days and
--                         postings is meaningful).
--
-- Column names follow the metrics design's "column contract"
-- (Admin/project-research/report/metrics-design.md, section 1) so the downstream metrics SQL works unchanged.
--
-- Why a star and not a snowflake (L8): the dashboard asks "how many applications per skill per week",
-- "which occupations are reposted most". Those are GROUP BY questions over a few numeric columns, so a
-- fact table with foreign keys to a few wide dimensions needs few joins. Company and SSOC occupation are
-- DENORMALISED into dim_job on purpose (Kimball: resist normalising dimensions; the lecturer said the
-- instructor name goes straight into the course dimension). Storage is small enough that a snowflake
-- would only add joins.
--
-- Why Postgres and not MongoDB for this layer (L3/L4): the raw layer already keeps the nested JSON as
-- JSONB. The clean layer is fixed-schema, numeric, joined and aggregated, which is what a relational
-- store with B-tree indexes does best. The volume (about 100k rows a day, under 1 GB for three weeks)
-- is nowhere near the "too much data for one machine" point where NoSQL or Spark would be needed.
--
-- Idempotency (L7): every table has a natural key, loads use INSERT ... ON CONFLICT or
-- delete-then-insert per snapshot_date, so re-running a day gives the same result as running it once.
-- This file itself is idempotent (IF NOT EXISTS everywhere) and runs on first container start from
-- /docker-entrypoint-initdb.d, after 01_raw_schema.sql.

CREATE SCHEMA IF NOT EXISTS clean;

-- ---------------------------------------------------------------------------------------------
-- Reference tables (loaded from the SkillsFuture and SingStat files in data/reference/).
-- They are inputs to the transform, not part of the star, so they sit apart from dim_* tables.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean.ref_ssg_skill (
    skill_id      TEXT PRIMARY KEY,         -- SkillsFuture Unique Skills List id (40 hex chars)
    skill_title   TEXT NOT NULL,
    norm_title    TEXT NOT NULL,            -- normalised title used for exact matching
    skill_type    TEXT NOT NULL,            -- 'tsc' (technical) or 'ccs' (critical core skill)
    is_emerging   BOOLEAN NOT NULL DEFAULT FALSE,
    is_casl       BOOLEAN NOT NULL DEFAULT FALSE,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ref_ssg_skill_norm_title_idx ON clean.ref_ssg_skill (norm_title);

CREATE TABLE IF NOT EXISTS clean.ref_skill_alias (
    norm_alias    TEXT PRIMARY KEY,         -- normalised alias text, e.g. 'team player'
    skill_id      TEXT NOT NULL REFERENCES clean.ref_ssg_skill (skill_id),
    source        TEXT NOT NULL,            -- 'curated' (data/reference/skill_aliases.csv) or 'tsc_sector_title'
    confidence    NUMERIC(4,3) NOT NULL,    -- 0 to 1, set by the person who wrote the alias
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS clean.ref_ssoc (
    ssoc_code     TEXT PRIMARY KEY,         -- 5-digit SSOC 2020 code kept as TEXT (leading zeros, not a number)
    ssoc_title    TEXT NOT NULL,
    major_group   SMALLINT NOT NULL,        -- first digit, 1 to 9
    major_group_title TEXT,
    mom_group     TEXT NOT NULL,            -- MOM's 3 groups: PMET (1-3), Clerical/sales/services (4-5), Production/transport/cleaners/labourers (6-9)
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------------------------
-- Dimensions
-- ---------------------------------------------------------------------------------------------

-- dim_date: one row per snapshot day. date_key = yyyymmdd so the fact can filter by an integer
-- and the dashboard can group by week without date arithmetic. snapshot_fetched_at records when
-- that day's crawl finished (from raw.ingest_runs), which is what hours_since_prev is computed from:
-- the first interval was 17 hours (24 Oct 17:35 to 25 Oct 10:23), not 24.
CREATE TABLE IF NOT EXISTS clean.dim_date (
    date_key      INTEGER PRIMARY KEY,      -- 20261024
    date          DATE NOT NULL UNIQUE,
    year          SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    day           SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL,        -- 1 = Monday ... 7 = Sunday (ISO)
    day_name      TEXT NOT NULL,
    is_weekend    BOOLEAN NOT NULL,
    week_start    DATE NOT NULL,            -- Monday of that week, for week-on-week charts
    iso_week      SMALLINT NOT NULL,
    snapshot_fetched_at TIMESTAMPTZ         -- end of that day's crawl (NULL for calendar-only rows)
);

-- dim_job: one row per posting VERSION (slowly changing dimension, Type 2).
--
-- Why Type 2 and not Type 1 (overwrite): employers edit postings (editCount) and repost them
-- (repostCount, new posting and expiry dates change). A dashboard that asks "did applications pick up
-- after the salary was raised" needs the attributes AS THEY WERE on each day. Type 1 would silently
-- rewrite history. Type 2 adds a new row with a new surrogate key and closes the old one, so the fact
-- row for each day points at the version that was live that day.
--
-- Version rules (transform/scd.py, pure Python so it is unit-tested offline):
--   attr_hash   = hash of every tracked attribute (title, salary, experience, dates, skills ...).
--   same hash   -> only last_seen_date moves forward (nothing else changes: rerun-safe).
--   new hash on a later day -> old row gets valid_to = its last_seen_date and is_current = false,
--                 a new row is inserted with valid_from = that snapshot day.
--   new hash on the SAME day (a rerun with re-ingested data) -> that day's row is updated in place.
-- Company and SSOC occupation attributes live here, denormalised (star schema, see header).
CREATE TABLE IF NOT EXISTS clean.dim_job (
    job_key               BIGSERIAL PRIMARY KEY,       -- surrogate key (L8: dims get an integer key)
    job_uuid              TEXT NOT NULL,               -- natural key from MCF (payload.uuid)
    job_post_id           TEXT,                        -- metadata.jobPostId, e.g. MCF-2026-1888705
    title                 TEXT NOT NULL,
    company_uen           TEXT,
    company_name          TEXT,
    company_ssic_code     TEXT,                        -- postedCompany.ssicCode2020 (industry)
    company_ssic_desc     TEXT,
    is_posted_on_behalf   BOOLEAN NOT NULL DEFAULT FALSE,
    hiring_company_name   TEXT,                        -- only when an agency posts for another firm
    ssoc_code             TEXT,                        -- 5-digit SSOC 2020 as TEXT
    ssoc_title            TEXT,                        -- denormalised from ref_ssoc
    ssoc_major_group      SMALLINT,
    mom_group             TEXT,                        -- 'PMET', 'clerical_sales_services', 'production_transport'
    position_level        TEXT,                        -- first of positionLevels[].position
    position_levels       TEXT,                        -- all levels, '|' separated (about 30 postings have two)
    employment_types      TEXT,                        -- '|' separated
    category              TEXT,                        -- first MCF category
    categories            TEXT,                        -- all categories, '|' separated
    education_code        TEXT,                        -- ssecEqa, 'NR' = not required
    min_years_exp_raw     SMALLINT,                    -- as the employer typed it (0 to 88 seen)
    min_years_exp         SMALLINT,                    -- capped at 30 for analysis (40 before 4 Nov 2026)
    exp_out_of_range      BOOLEAN NOT NULL DEFAULT FALSE, -- raw value outside 0..30, flagged not dropped
    salary_min            NUMERIC(12,2),
    salary_max            NUMERIC(12,2),
    salary_mid            NUMERIC(12,2) GENERATED ALWAYS AS ((salary_min + salary_max) / 2) STORED,
    salary_type           TEXT,                        -- 'Monthly' for every posting seen so far
    salary_is_hidden      BOOLEAN NOT NULL DEFAULT FALSE,
    vacancies             INTEGER,
    original_posting_date DATE,
    new_posting_date      DATE,                        -- moves forward on a repost
    expiry_date           DATE,
    source_code           TEXT,                        -- 'Employer Portal' or 'ATS'
    attr_hash             TEXT NOT NULL,
    valid_from            DATE NOT NULL,               -- first snapshot day this version was seen
    valid_to              DATE,                        -- last snapshot day this version was seen; NULL while current
    is_current            BOOLEAN NOT NULL DEFAULT TRUE,
    last_seen_date        DATE NOT NULL,               -- last snapshot day the posting was seen with this version
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_uuid, valid_from)                      -- one version per posting per day: rerun-safe
);
-- The transform looks up "the current version of each posting" every day: a partial index keeps that
-- lookup small (one row per posting instead of every historical version).
CREATE UNIQUE INDEX IF NOT EXISTS dim_job_current_uuid_idx ON clean.dim_job (job_uuid) WHERE is_current;
-- Dashboard filters: by occupation and by company.
CREATE INDEX IF NOT EXISTS dim_job_ssoc_idx    ON clean.dim_job (ssoc_code);
CREATE INDEX IF NOT EXISTS dim_job_company_idx ON clean.dim_job (company_uen);

-- dim_skill: one row per distinct MCF skill tag (keyed by MCF's own skill uuid; the pilot showed one
-- uuid per tag name). The SkillsFuture mapping columns are filled once when the tag is first seen.
CREATE TABLE IF NOT EXISTS clean.dim_skill (
    skill_key        SERIAL PRIMARY KEY,
    mcf_skill_uuid   TEXT NOT NULL UNIQUE,
    mcf_skill_name   TEXT NOT NULL,                    -- raw tag as the employer typed it
    norm_name        TEXT NOT NULL,                    -- normalised (lower case, no punctuation)
    ssg_skill_id     TEXT REFERENCES clean.ref_ssg_skill (skill_id),
    ssg_skill_title  TEXT,
    ssg_skill_type   TEXT,                             -- 'tsc' / 'ccs'
    is_emerging      BOOLEAN,
    is_casl          BOOLEAN,
    match_method     TEXT NOT NULL,                    -- 'exact', 'alias', 'stem', 'fuzzy', 'unmatched'
    match_confidence NUMERIC(4,3),                     -- 1.0 exact, alias as curated, fuzzy = similarity
    first_seen_date  DATE NOT NULL,
    last_seen_date   DATE NOT NULL
);
CREATE INDEX IF NOT EXISTS dim_skill_ssg_idx ON clean.dim_skill (ssg_skill_id);
CREATE INDEX IF NOT EXISTS dim_skill_method_idx ON clean.dim_skill (match_method);

-- bridge_job_skill: a posting has 3 to 20 skills, so the many-to-many goes in a bridge table keyed
-- on the job VERSION (skills can be edited, so they belong to the version).
CREATE TABLE IF NOT EXISTS clean.bridge_job_skill (
    job_key       BIGINT  NOT NULL REFERENCES clean.dim_job (job_key) ON DELETE CASCADE,
    skill_key     INTEGER NOT NULL REFERENCES clean.dim_skill (skill_key),
    is_key_skill  BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (job_key, skill_key)
);
-- "All postings asking for skill X" is the dashboard's main skill query.
CREATE INDEX IF NOT EXISTS bridge_job_skill_skill_idx ON clean.bridge_job_skill (skill_key);

-- ---------------------------------------------------------------------------------------------
-- Fact table
-- ---------------------------------------------------------------------------------------------
-- fact_job_day: grain = one posting on one snapshot day. Long and narrow (L8).
--   views_total, applications_total, repost_count, edit_count : running totals copied from the API
--     that day. They are SEMI-ADDITIVE: adding Monday's and Tuesday's views of one posting double counts.
--   views_delta, applications_delta : increase since the previous snapshot of that posting. ADDITIVE.
--     Stored, not computed on the fly, because (a) the previous snapshot is not always "yesterday"
--     (about 50 postings a run are missed while paging, and the first interval was 17 h), so the
--     dashboard would need a self-join with a window function on 2 million rows every time, and
--     (b) a stored delta lets the DQ check "counters never fall" run once at load time.
--   hours_since_prev : length of that interval, so rates are expressed per 24 h (delta * 24 / hours).
--   prev_snapshot_date : which snapshot the delta is measured from. NULL on a posting's first snapshot.
CREATE TABLE IF NOT EXISTS clean.fact_job_day (
    snapshot_date       DATE    NOT NULL,
    job_uuid            TEXT    NOT NULL,          -- natural key, used for the day-over-day self join
    job_key             BIGINT  NOT NULL REFERENCES clean.dim_job (job_key),   -- version live that day
    date_key            INTEGER NOT NULL REFERENCES clean.dim_date (date_key),
    views_total         INTEGER NOT NULL,
    applications_total  INTEGER NOT NULL,
    repost_count        SMALLINT NOT NULL,
    edit_count          SMALLINT NOT NULL,
    status              TEXT,                      -- 'Open' or 'Re-open'
    days_since_new_posting      SMALLINT,          -- snapshot_date - new_posting_date (age of this posting period)
    days_since_original_posting SMALLINT,          -- snapshot_date - original_posting_date
    days_to_expiry      SMALLINT,                  -- expiry_date - snapshot_date
    prev_snapshot_date  DATE,
    hours_since_prev    NUMERIC(8,2),              -- NULL on the first snapshot
    is_first_snapshot   BOOLEAN NOT NULL,          -- no earlier snapshot of this posting
    views_delta         INTEGER,                   -- NULL when is_first_snapshot
    applications_delta  INTEGER,
    reposted_since_prev BOOLEAN,                   -- repost_count rose or new_posting_date moved since the previous snapshot
    edited_since_prev   BOOLEAN,                   -- edit_count rose since the previous snapshot
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_date, job_uuid)          -- delete-then-insert per snapshot_date, so reruns never duplicate
);
-- Per-posting history and the "latest earlier snapshot" lookup used for deltas.
CREATE INDEX IF NOT EXISTS fact_job_day_uuid_date_idx ON clean.fact_job_day (job_uuid, snapshot_date);
CREATE INDEX IF NOT EXISTS fact_job_day_job_key_idx   ON clean.fact_job_day (job_key);
-- Scale-out note (L9/L10): at many months of data this table would be range-partitioned by
-- snapshot_date (evenly sized daily or monthly partitions), which also makes delete-then-insert a
-- cheap partition truncate. Not needed for a three-week project, so kept as one table.

-- ---------------------------------------------------------------------------------------------
-- Operational tables (L7: save results to a proper store, not only Airflow logs)
-- ---------------------------------------------------------------------------------------------

-- rejected_rows: the "error table / dead letter queue" from L7. A payload that cannot become a fact
-- row (no uuid, no counters) is written here with the reason instead of crashing the whole day.
CREATE TABLE IF NOT EXISTS clean.rejected_rows (
    reject_id     BIGSERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    job_uuid      TEXT,
    reason        TEXT NOT NULL,
    rejected_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rejected_rows_date_idx ON clean.rejected_rows (snapshot_date);

-- load_log: one row per transform step per run, for the performance section of the report.
CREATE TABLE IF NOT EXISTS clean.load_log (
    load_id       BIGSERIAL PRIMARY KEY,
    run_id        TEXT NOT NULL,                     -- Airflow run_id or 'cli-<timestamp>'
    snapshot_date DATE NOT NULL,
    step          TEXT NOT NULL,                     -- 'parse_raw', 'upsert_dim_skill', ..., 'dag_run'
    started_at    TIMESTAMPTZ NOT NULL,
    finished_at   TIMESTAMPTZ NOT NULL,
    seconds       NUMERIC(10,3) NOT NULL,
    rows_in       INTEGER,
    rows_out      INTEGER,
    details       JSONB                              -- small free-form stats (counts per action)
);
CREATE INDEX IF NOT EXISTS load_log_date_idx ON clean.load_log (snapshot_date);

-- dq_results: one row per check per snapshot day (upserted, so a rerun overwrites the old verdict).
CREATE TABLE IF NOT EXISTS clean.dq_results (
    snapshot_date DATE NOT NULL,
    check_name    TEXT NOT NULL,
    category      TEXT NOT NULL,                     -- 'schema', 'range', 'business_rule', 'info'
    severity      TEXT NOT NULL,                     -- 'fail' stops the DAG, 'warn' only records, 'info' never fails
    status        TEXT NOT NULL,                     -- 'pass', 'warn', 'fail'
    observed      NUMERIC,                           -- the number the check measured
    threshold     NUMERIC,                           -- the limit it was compared with
    details       TEXT,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_date, check_name)
);
