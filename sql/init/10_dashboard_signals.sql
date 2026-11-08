-- Dashboard views over the weekly signal tables (03_mom_vacancies.sql and 05_metrics_signals.sql), read by
-- the Hiring signals, Skill lookup and Market check tabs (dashboard/signal_queries.py).
--
-- Plain views, no new numbers: they only line up rows the weekly_metrics DAG already wrote, so they are
-- always in step with `metrics` and need no refresh. They live in `metrics`, not `mart`, because
-- transform.refresh_marts drops and rebuilds the mart schema every day.
--
-- Idempotent (CREATE OR REPLACE). CREATE OR REPLACE VIEW can only add columns at the end, so to rename or
-- reorder a column, DROP VIEW metrics.skill_signals, metrics.signal_weeks first. Runs on first container
-- start after 01 to 09. On an existing warehouse, apply it once by hand (docs/dashboard-signals.md):
--   docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/10_dashboard_signals.sql

CREATE SCHEMA IF NOT EXISTS metrics;

-- One row per week whose run finished with every check passed, with the finished week before it
-- (NULL for the first one) and the thresholds the run used. Weeks still running or with a failed check
-- are left out.
CREATE OR REPLACE VIEW metrics.signal_weeks AS
SELECT week_end_date,
       week_start_date,
       snapshot_date,
       snapshot_is_week_end,
       fallback_reason,
       pool_postings,
       carried_forward,
       thresholds,
       lag(week_end_date) OVER (ORDER BY week_end_date) AS prev_week_end_date
FROM metrics.run_log
WHERE status = 'ok';

-- One row per week and skill: every weekly signal for the skill in one place (the jobseeker's view).
-- The skills are those with a signal row that week: 100+ open postings (competitiveness by skill), or a
-- row in barrier_skill, shortage_skill or skill_salary. *_prev_week columns are NULL when there is no
-- earlier finished week; a FALSE there means the skill was not flagged that week.
CREATE OR REPLACE VIEW metrics.skill_signals AS
WITH skills AS (
    SELECT week_end_date, group_key AS skill_ref FROM metrics.competitiveness WHERE group_type = 'skill'
    UNION SELECT week_end_date, skill_ref FROM metrics.barrier_skill
    UNION SELECT week_end_date, skill_ref FROM metrics.shortage_skill
    UNION SELECT week_end_date, skill_ref FROM metrics.skill_salary
),
-- skill demand is by posting week: use only fully observed ones (docs/weekly-metrics.md), the latest
-- of the run and the fully observed one before it
demand_weeks AS (
    SELECT l.week_end_date,
           l.demand_week,
           (SELECT max(d.posting_week) FROM metrics.skill_demand d
             WHERE d.week_end_date = l.week_end_date AND d.week_fully_observed
               AND d.posting_week < l.demand_week) AS demand_week_before
    FROM (SELECT week_end_date, max(posting_week) AS demand_week
          FROM metrics.skill_demand WHERE week_fully_observed
          GROUP BY week_end_date) l
),
skill_info AS (
    SELECT week_end_date, skill_ref, max(skill_name) AS skill_name, bool_or(is_mapped) AS is_mapped,
           max(skill_type) AS skill_type
    FROM metrics.skill_demand
    GROUP BY week_end_date, skill_ref
)
SELECT s.week_end_date,
       w.snapshot_date,
       w.prev_week_end_date,
       s.skill_ref,
       COALESCE(b.skill_name, sh.skill_name, sa.skill_name, c.group_label, i.skill_name) AS skill_name,
       COALESCE(b.is_mapped, sh.is_mapped, sa.is_mapped, i.is_mapped)                   AS is_mapped,
       COALESCE(b.skill_type, sh.skill_type, i.skill_type)                              AS skill_type,
       COALESCE(b.is_casl, sh.is_casl)                                                  AS is_casl,
       COALESCE(c.postings, b.postings)                                                 AS open_postings,
       -- demand: share of the postings first posted in that week that ask for the skill (0 if none did)
       dw.demand_week,
       CASE WHEN dw.demand_week IS NOT NULL THEN COALESCE(d1.new_postings, 0) END        AS demand_new_postings,
       CASE WHEN dw.demand_week IS NOT NULL THEN COALESCE(d1.share_of_week, 0) END       AS demand_share,
       dw.demand_week_before,
       CASE WHEN dw.demand_week_before IS NOT NULL THEN COALESCE(d0.share_of_week, 0) END AS demand_share_week_before,
       -- pay (open postings with a usable monthly salary)
       sa.postings          AS salary_postings,
       sa.salary_mid_p25,
       sa.salary_mid_median,
       sa.salary_mid_p75,
       sa.mid_vs_overall    AS salary_vs_overall,
       -- shortage: listed = lift 1.5+, 25+ shortage postings from 10+ employers, no employer above half of them
       sh.skill_ref IS NOT NULL AS shortage_listed,
       sh.shortage_postings,
       sh.shortage_companies,
       sh.shortage_share,
       sh.overall_shortage_share,
       sh.lift              AS shortage_lift,
       sh.top_company_share AS shortage_top_company_share,
       CASE WHEN w.prev_week_end_date IS NOT NULL THEN psh.skill_ref IS NOT NULL END     AS shortage_listed_prev_week,
       psh.lift             AS shortage_lift_prev_week,
       -- barrier: is_barrier as the weekly run set it (lift 1.5+, 25+ barrier postings, 10+ employers)
       COALESCE(b.is_barrier, FALSE) AS is_barrier,
       b.barrier_postings,
       b.barrier_companies,
       b.barrier_share,
       b.expected_share     AS barrier_expected_share,
       b.lift               AS barrier_lift,
       b.top_company_share  AS barrier_top_company_share,
       CASE WHEN w.prev_week_end_date IS NOT NULL THEN COALESCE(pb.is_barrier, FALSE) END AS is_barrier_prev_week,
       pb.lift              AS barrier_lift_prev_week,
       -- competitiveness: applications gained per open posting per 24 hours
       c.apps_per_posting_day_24h,
       c.median_apps_per_day_since_posting,
       o.apps_per_posting_day_24h  AS overall_apps_per_posting_day_24h,
       pc.apps_per_posting_day_24h AS apps_per_posting_day_24h_prev_week,
       -- added last (a replaced view can only gain columns at the end): applications per view against all
       -- postings, which a barrier skill must keep below thresholds.barrier_max_rate_vs_overall
       b.rate_vs_overall    AS barrier_rate_vs_overall,
       -- a technical skill (SkillsFuture 'tsc', or an unmapped tag that is not a trait or attitude): the default
       -- view of the shortage and barrier lists. NULL when the skill has neither row
       COALESCE(b.is_technical, sh.is_technical) AS is_technical,
       -- the shortage lift's base: postings with the skill by direct employers (open_postings counts agencies too)
       sh.postings          AS shortage_base_postings
FROM skills s
JOIN metrics.signal_weeks w ON w.week_end_date = s.week_end_date
LEFT JOIN metrics.barrier_skill b   ON b.week_end_date = s.week_end_date AND b.skill_ref = s.skill_ref
LEFT JOIN metrics.shortage_skill sh ON sh.week_end_date = s.week_end_date AND sh.skill_ref = s.skill_ref
LEFT JOIN metrics.skill_salary sa   ON sa.week_end_date = s.week_end_date AND sa.skill_ref = s.skill_ref
LEFT JOIN metrics.competitiveness c ON c.week_end_date = s.week_end_date AND c.group_type = 'skill'
                                   AND c.group_key = s.skill_ref
LEFT JOIN metrics.competitiveness o ON o.week_end_date = s.week_end_date AND o.group_type = 'overall'
LEFT JOIN skill_info i              ON i.week_end_date = s.week_end_date AND i.skill_ref = s.skill_ref
LEFT JOIN demand_weeks dw           ON dw.week_end_date = s.week_end_date
LEFT JOIN metrics.skill_demand d1   ON d1.week_end_date = s.week_end_date AND d1.skill_ref = s.skill_ref
                                   AND d1.posting_week = dw.demand_week
LEFT JOIN metrics.skill_demand d0   ON d0.week_end_date = s.week_end_date AND d0.skill_ref = s.skill_ref
                                   AND d0.posting_week = dw.demand_week_before
LEFT JOIN metrics.shortage_skill psh ON psh.week_end_date = w.prev_week_end_date AND psh.skill_ref = s.skill_ref
LEFT JOIN metrics.barrier_skill pb   ON pb.week_end_date = w.prev_week_end_date AND pb.skill_ref = s.skill_ref
LEFT JOIN metrics.competitiveness pc ON pc.week_end_date = w.prev_week_end_date AND pc.group_type = 'skill'
                                    AND pc.group_key = s.skill_ref;
