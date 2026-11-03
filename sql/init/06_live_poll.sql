-- Near-real-time feed: the first pages of MyCareersFuture's newest-first list, polled every 30 minutes by
-- dags/mcf_live_poll_dag.py (ingestion/mcf_live.py). Design, numbers and deploy steps: docs/live-feed.md.
--
-- Why it exists: the daily crawl sees each posting once a day, at whatever age it happens to be at 10:00.
-- The live poll sees every new posting within 30 minutes of going live and then every 30 minutes while it
-- stays on the first pages, so we get each posting's own early curve (views and applications by hours
-- since posting) and the data freshness in minutes.
--
-- Grain of raw.mcf_live_poll: ONE ROW = ONE POSTING SEEN IN ONE POLL. Append-only: an observation is never
-- updated. The key (poll_ts, job_uuid) makes a retried load a no-op (ON CONFLICT DO NOTHING).
--
-- This file is idempotent (IF NOT EXISTS / CREATE OR REPLACE). It runs on first container start after 01 to
-- 05. On an existing warehouse run it by hand (docs/live-feed.md, "Deploy on the collector laptop").

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS metrics;

-- ---------------------------------------------------------------------------------------------
-- Raw: slim rows, about 250 bytes each instead of the 7.7 KB full JSON (the daily crawl keeps that).
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.mcf_live_poll (
    poll_ts               TIMESTAMPTZ NOT NULL,   -- when the poll started; the same for every row of one poll
    job_uuid              TEXT        NOT NULL,
    list_rank             SMALLINT    NOT NULL,   -- position in the newest-first list, 0 = newest (page * 100 + i)
    created_at            TIMESTAMPTZ,            -- metadata.createdAt: when the posting went live (the list's sort key)
    updated_at            TIMESTAMPTZ,            -- metadata.updatedAt (moves on an edit)
    new_posting_date      DATE,                   -- moves forward on a repost
    original_posting_date DATE,
    repost_count          SMALLINT,
    edit_count            SMALLINT,
    views                 INTEGER,                -- metadata.totalNumberOfView, running total
    applications          INTEGER,                -- metadata.totalNumberJobApplication, running total
    job_status            TEXT,                   -- 'Open' or 'Re-open'
    ssoc_code             TEXT,                   -- 5-digit SSOC 2020 as TEXT; first digit = major group
    category              TEXT,                   -- first of categories[], e.g. 'Engineering'
    title                 TEXT,                   -- for reading the demo table; not used in the analysis
    PRIMARY KEY (poll_ts, job_uuid)
);
-- One posting's curve, and "was this posting in an earlier poll".
CREATE INDEX IF NOT EXISTS mcf_live_poll_uuid_ts_idx ON raw.mcf_live_poll (job_uuid, poll_ts);

-- One row per poll, like raw.ingest_runs for the daily crawl: the run log for the performance section.
CREATE TABLE IF NOT EXISTS raw.mcf_live_poll_runs (
    poll_ts             TIMESTAMPTZ PRIMARY KEY,
    pages_fetched       SMALLINT    NOT NULL,
    rows_fetched        INTEGER     NOT NULL,   -- distinct postings kept
    duplicates_dropped  INTEGER     NOT NULL,   -- same posting on two pages (the list moved while paging)
    api_total           INTEGER,                -- open postings the API reported
    newest_created_at   TIMESTAMPTZ,
    oldest_created_at   TIMESTAMPTZ,            -- how far back the polled pages reached
    seconds_taken       NUMERIC     NOT NULL,
    finished_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per check per poll (quality/live_poll_checks.py), same columns as clean.dq_results.
CREATE TABLE IF NOT EXISTS raw.mcf_live_poll_dq (
    poll_ts      TIMESTAMPTZ NOT NULL,
    check_name   TEXT        NOT NULL,
    category     TEXT        NOT NULL,
    severity     TEXT        NOT NULL,
    status       TEXT        NOT NULL,
    observed     NUMERIC,
    threshold    NUMERIC,
    details      TEXT,
    checked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (poll_ts, check_name)
);

-- ---------------------------------------------------------------------------------------------
-- Derived views (always in step with the raw rows; small enough not to need a table)
-- ---------------------------------------------------------------------------------------------

-- Postings the live poll caught while brand new: first seen within 1 hour of createdAt and not a repost.
-- Only these have a curve that starts near hour 0. Group labels come from the first sighting.
CREATE OR REPLACE VIEW metrics.live_tracked_postings AS
SELECT f.job_uuid,
       f.created_at,
       f.first_seen_at,
       round((extract(epoch FROM f.first_seen_at - f.created_at) / 60)::numeric, 1) AS minutes_to_first_seen,
       f.title,
       f.ssoc_code,
       g.title AS ssoc_major_group,
       f.category
FROM (
    SELECT DISTINCT ON (job_uuid) job_uuid, created_at, poll_ts AS first_seen_at, title, ssoc_code, category,
           repost_count, new_posting_date, original_posting_date
    FROM raw.mcf_live_poll
    ORDER BY job_uuid, poll_ts
) f
LEFT JOIN (VALUES ('1', '1 Managers'), ('2', '2 Professionals'), ('3', '3 Associate professionals and technicians'),
                  ('4', '4 Clerical support workers'), ('5', '5 Service and sales workers'),
                  ('6', '6 Agricultural and fishery workers'), ('7', '7 Craftsmen and related trades workers'),
                  ('8', '8 Plant and machine operators and assemblers'), ('9', '9 Cleaners, labourers and related workers')
          ) g(digit, title) ON g.digit = left(f.ssoc_code, 1)
WHERE f.created_at IS NOT NULL
  AND f.first_seen_at BETWEEN f.created_at - interval '5 minutes' AND f.created_at + interval '1 hour'
  AND COALESCE(f.repost_count, 0) = 0
  AND f.new_posting_date IS NOT DISTINCT FROM f.original_posting_date;

-- Every observation of a tracked posting in its first 24.5 hours. Two sources:
--   live  : each 30-minute poll while the posting is on the polled pages (about 1.5 to 2 hours by day with
--           3 pages, longer at night when fewer postings arrive)
--   daily : the 10:00 crawl's snapshot of the same posting (clean.fact_job_day). That crawl also pages
--           newest first, so a day-old posting is fetched in its first minute or two; its time is taken as
--           the crawl's start (raw.ingest_runs.finished_at minus the fetch seconds), good to a couple of minutes.
-- The live points give the dense early curve the daily crawl cannot; the daily point extends it towards 24 h.
CREATE OR REPLACE VIEW metrics.live_curve_points AS
WITH tracked AS (
    SELECT * FROM metrics.live_tracked_postings
),
obs AS (
    SELECT p.job_uuid, 'live'::text AS source, p.poll_ts AS observed_at, p.views, p.applications
    FROM raw.mcf_live_poll p
    JOIN tracked t ON t.job_uuid = p.job_uuid
    UNION ALL
    SELECT f.job_uuid, 'daily'::text, i.finished_at - i.seconds_taken * interval '1 second',
           f.views_total, f.applications_total
    FROM tracked t
    JOIN clean.fact_job_day f
      ON f.job_uuid = t.job_uuid
     AND f.snapshot_date BETWEEN (t.created_at AT TIME ZONE 'Asia/Singapore')::date
                             AND (t.created_at AT TIME ZONE 'Asia/Singapore')::date + 1
    JOIN raw.ingest_runs i ON i.snapshot_date = f.snapshot_date
)
SELECT t.job_uuid,
       t.category,
       t.ssoc_major_group,
       t.created_at,
       o.source,
       o.observed_at,
       greatest(round((extract(epoch FROM o.observed_at - t.created_at) / 3600)::numeric, 2), 0) AS hours_since_posting,
       o.views,
       o.applications
FROM obs o
JOIN tracked t ON t.job_uuid = o.job_uuid
WHERE o.observed_at >= t.created_at - interval '5 minutes'
  AND o.observed_at <  t.created_at + interval '24 hours 30 minutes';

-- The first-24-hour curve: for each hour since posting (0 = the first hour), the postings measured at that
-- age and their median views and applications. One value per posting per hour (its latest in that hour).
-- group_type 'all' is every tracked posting; 'category' and 'ssoc_major_group' split it.
CREATE OR REPLACE VIEW metrics.live_first24h_curve AS
WITH per_hour AS (
    SELECT DISTINCT ON (job_uuid, hour) job_uuid, category, ssoc_major_group, hour, views, applications
    FROM (SELECT *, floor(hours_since_posting)::int AS hour FROM metrics.live_curve_points) x
    WHERE hour < 24
    ORDER BY job_uuid, hour, observed_at DESC
)
SELECT CASE WHEN grouping(category) = 0 THEN 'category'
            WHEN grouping(ssoc_major_group) = 0 THEN 'ssoc_major_group'
            ELSE 'all' END                                                        AS group_type,
       CASE WHEN grouping(category) = 0 THEN COALESCE(category, 'unknown')
            WHEN grouping(ssoc_major_group) = 0 THEN COALESCE(ssoc_major_group, 'unknown')
            ELSE 'all postings' END                                               AS group_name,
       hour                                                                       AS hours_since_posting,
       count(*)                                                                   AS postings,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY views)                         AS median_views,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY applications)                  AS median_applications,
       round(avg(applications), 2)                                                AS mean_applications,
       round(100.0 * avg(CASE WHEN applications > 0 THEN 1 ELSE 0 END), 1)        AS pct_with_application
FROM per_hour
GROUP BY GROUPING SETS ((hour), (category, hour), (ssoc_major_group, hour));

-- Headline numbers: applications and views 1, 6, 12 and 24 hours after posting, by group. Each posting
-- contributes its observation closest to that age, if one is within 30 minutes of it. `postings` is how
-- many postings were measured at that age (different horizons can come from different postings).
CREATE OR REPLACE VIEW metrics.live_first24h_summary AS
WITH at_h AS (
    SELECT DISTINCT ON (p.job_uuid, h.hours) p.job_uuid, p.category, p.ssoc_major_group, h.hours,
           p.views, p.applications
    FROM metrics.live_curve_points p
    JOIN (VALUES (1), (6), (12), (24)) h(hours) ON abs(p.hours_since_posting - h.hours) <= 0.5
    ORDER BY p.job_uuid, h.hours, abs(p.hours_since_posting - h.hours), p.observed_at
)
SELECT CASE WHEN grouping(category) = 0 THEN 'category'
            WHEN grouping(ssoc_major_group) = 0 THEN 'ssoc_major_group'
            ELSE 'all' END                                                        AS group_type,
       CASE WHEN grouping(category) = 0 THEN COALESCE(category, 'unknown')
            WHEN grouping(ssoc_major_group) = 0 THEN COALESCE(ssoc_major_group, 'unknown')
            ELSE 'all postings' END                                               AS group_name,
       hours                                                                      AS hours_since_posting,
       count(*)                                                                   AS postings,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY applications)                  AS median_applications,
       round(100.0 * avg(CASE WHEN applications > 0 THEN 1 ELSE 0 END), 1)        AS pct_with_application,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY views)                         AS median_views
FROM at_h
GROUP BY GROUPING SETS ((hours), (category, hours), (ssoc_major_group, hours));

-- One line per poll for the demo and the monitoring page: is the feed alive and how fresh is it?
CREATE OR REPLACE VIEW metrics.live_poll_status AS
SELECT r.poll_ts AT TIME ZONE 'Asia/Singapore'                                             AS poll_time_sgt,
       r.rows_fetched                                                                      AS postings_seen,
       (SELECT count(*) FROM raw.mcf_live_poll p
         WHERE p.poll_ts = r.poll_ts
           AND NOT EXISTS (SELECT 1 FROM raw.mcf_live_poll q
                            WHERE q.job_uuid = p.job_uuid AND q.poll_ts < r.poll_ts))      AS new_since_last_poll,
       round((extract(epoch FROM r.poll_ts - r.newest_created_at) / 60)::numeric, 1)       AS minutes_since_newest_posting,
       round((extract(epoch FROM r.poll_ts - r.oldest_created_at) / 3600)::numeric, 2)     AS hours_covered,
       r.pages_fetched,
       r.duplicates_dropped                                                                AS postings_on_two_pages,
       r.seconds_taken,
       (SELECT count(*) FROM raw.mcf_live_poll_dq d WHERE d.poll_ts = r.poll_ts AND d.status = 'fail') AS checks_failed,
       (SELECT count(*) FROM raw.mcf_live_poll_dq d WHERE d.poll_ts = r.poll_ts AND d.status = 'warn') AS checks_warned
FROM raw.mcf_live_poll_runs r;
