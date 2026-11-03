-- MOM job vacancies by occupation (data.gov.sg) and the MCF-vs-MOM comparison metric.
--
-- External validation source from the proposal: do MCF postings spread across occupations the way
-- MOM's official vacancy survey does? MOM only publishes 3 broad occupation groups (Split A) and,
-- since 2013, a PME vs non-PME split (Split B), not the 9 SSOC major groups. Both sides are mapped
-- to these groups through data/reference/ssoc_major_to_mom_group.csv and mom_occupation_labels.csv.
--
-- This file is idempotent (IF NOT EXISTS / CREATE OR REPLACE). It runs on first container start after
-- 01 and 02. On an existing database run it by hand (see README, "MOM vacancy comparison").

-- ---------------------------------------------------------------------------------------------
-- Raw: rows exactly as data.gov.sg returned them. Re-fetching upserts, so no duplicates.
-- Keyed on (dataset, quarter, occupation) and not on the API's _id, because _id is a row number
-- that can shift when MOM republishes the file.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.mom_vacancy_rows (
    dataset_id   TEXT        NOT NULL,      -- data.gov.sg dataset id
    quarter      TEXT        NOT NULL,      -- as published, e.g. '2026-Q2'
    occupation   TEXT        NOT NULL,      -- MOM's label, e.g. 'clerical, sales and services workers'
    payload      JSONB       NOT NULL,      -- the whole API row
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_id, quarter, occupation)
);

-- ---------------------------------------------------------------------------------------------
-- Clean: one row per quarter per MOM occupation group, with the vacancy count as a number.
-- mom_group codes for Split A match clean.dim_job.mom_group, so the two sides join directly.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean.mom_vacancy_quarterly (
    quarter              TEXT     NOT NULL,     -- '2026-Q2'
    quarter_start        DATE     NOT NULL,     -- 2026-04-01, for charts
    year                 SMALLINT NOT NULL,
    quarter_num          SMALLINT NOT NULL,     -- 1 to 4
    split                CHAR(1)  NOT NULL,     -- 'A' = 3 groups (since 1998), 'B' = PME vs non-PME (since 2013)
    mom_group            TEXT     NOT NULL,     -- 'PMET', 'clerical_sales_services', 'production_transport', 'PME', 'non_PME'
    group_title          TEXT     NOT NULL,
    job_vacancy          INTEGER,               -- NULL if MOM suppressed the figure ('-')
    -- Before 2006 MOM surveyed only private firms with 25+ employees; from 2006 the public sector is
    -- included. So the series has a level break at 2006: compare trends within one side of it.
    covers_public_sector BOOLEAN  NOT NULL,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (quarter, mom_group)
);

-- ---------------------------------------------------------------------------------------------
-- Metrics
-- ---------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS metrics;

-- One row per MCF snapshot per split per group. Each weekly_metrics run uses the latest snapshot of
-- ITS week (no run of consecutive days needed), so there is one snapshot date per week as history.
-- A rerun deletes and rewrites its week's rows, so it never duplicates them.
--
-- Shares are within a split and exclude the 'unknown' bucket (postings whose ssocCode is missing,
-- not 5 digits, or starts with 0 / a letter, which MOM's groups do not cover). The 'unknown' row
-- keeps its posting count so the report can say how many were dropped.
CREATE TABLE IF NOT EXISTS metrics.mcf_vs_mom_share (
    mcf_snapshot_date     DATE     NOT NULL,    -- latest snapshot of the run's week in clean.fact_job_day
    split                 CHAR(1)  NOT NULL,    -- 'A' (main comparison) or 'B' (PME vs non-PME)
    mom_group             TEXT     NOT NULL,    -- group code, or 'unknown'
    group_title           TEXT     NOT NULL,
    mcf_postings          INTEGER  NOT NULL,    -- postings open on that snapshot day
    mcf_vacancies         INTEGER  NOT NULL,    -- sum of numberOfVacancies (MOM counts vacancies, not postings)
    mcf_share_pct         NUMERIC(5,2),         -- share of postings among known groups; NULL for 'unknown'
    mcf_vacancy_share_pct NUMERIC(5,2),         -- same, weighted by numberOfVacancies
    mom_quarter           TEXT,                 -- latest MOM quarter, e.g. '2026-Q2'
    mom_vacancies         INTEGER,
    mom_share_pct         NUMERIC(5,2),
    share_gap_pp          NUMERIC(6,2),         -- mcf_share_pct - mom_share_pct, in percentage points
    mom_vacancies_1y_ago  INTEGER,              -- same quarter one year earlier
    mom_yoy_pct           NUMERIC(7,2),         -- % change in MOM vacancies over that year
    mom_vacancies_5y_ago  INTEGER,              -- same quarter five years earlier
    mom_5y_pct            NUMERIC(7,2),
    disagrees             BOOLEAN  NOT NULL,    -- |share_gap_pp| >= threshold below
    disagree_threshold_pp NUMERIC(5,2) NOT NULL,
    computed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (mcf_snapshot_date, split, mom_group)
);

-- Long MOM series for the dashboard's trend chart: every quarter since 1998, with each group's share
-- of that quarter's total within its split. A view, so it is always in step with the clean table.
CREATE OR REPLACE VIEW metrics.mom_vacancy_trend AS
SELECT quarter,
       quarter_start,
       year,
       split,
       mom_group,
       group_title,
       job_vacancy,
       round(100.0 * job_vacancy / NULLIF(sum(job_vacancy) OVER (PARTITION BY quarter, split), 0), 2) AS share_pct,
       covers_public_sector
FROM clean.mom_vacancy_quarterly;
