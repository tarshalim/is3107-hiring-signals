"""Fill an EMPTY warehouse with synthetic data at the real scale, to try the dashboard without the real crawl.

    python -m dashboard.demo_data --days 21 --postings 250000
    python -m transform.refresh_marts

Writes straight into the clean star schema (reference tables, dim_*, bridge, fact_job_day, ops tables).
About 100k postings are open on a typical day with the defaults. Skills follow a skewed distribution that
depends on the occupation, skill 7 ("Skill 7") gets steadily more common so the trending tab has something
to show, and a quarter of the postings hide their salary. The numbers mean nothing: never use this
against a warehouse that holds real data (it refuses when clean.fact_job_day is not empty).
"""
import argparse
import time
from datetime import date, timedelta

SKILLS, TAGS, OCCUPATIONS = 300, 400, 90
CATEGORIES = ("Information Technology", "Engineering", "Banking and Finance", "Sales / Retail", "Admin / Secretarial",
              "Healthcare / Pharmaceutical", "Education and Training", "Logistics / Supply Chain", "F&B",
              "Manufacturing", "Marketing / Public Relations", "Others")
LEVELS = ("Fresh/entry level", "Non-executive", "Junior Executive", "Executive", "Senior Executive", "Manager",
          "Senior Management")

REFERENCE_SQL = """
INSERT INTO clean.ref_ssoc (ssoc_code, ssoc_title, major_group, major_group_title, mom_group)
SELECT m::text || lpad(g::text, 4, '0'), 'Occupation ' || m::text || lpad(g::text, 4, '0'), m, 'Major group ' || m,
       CASE WHEN m <= 3 THEN 'PMET' WHEN m <= 5 THEN 'clerical_sales_services' ELSE 'production_transport' END
FROM (SELECT g, 1 + (g %% 9) AS m FROM generate_series(1, %(occ)s) g) t;

INSERT INTO clean.ref_ssg_skill (skill_id, skill_title, norm_title, skill_type, is_emerging)
SELECT 'ssg-' || g, 'Skill ' || g, 'skill ' || g, CASE WHEN g %% 5 = 0 THEN 'ccs' ELSE 'tsc' END, g %% 11 = 0
FROM generate_series(1, %(skills)s) g;

INSERT INTO clean.dim_skill (skill_key, mcf_skill_uuid, mcf_skill_name, norm_name, ssg_skill_id, ssg_skill_title,
                             ssg_skill_type, is_emerging, is_casl, match_method, match_confidence,
                             first_seen_date, last_seen_date)
SELECT g, 'u' || g, 'tag ' || g, 'tag ' || g,
       CASE WHEN g <= %(tags)s - 50 THEN 'ssg-' || (1 + (g - 1) %% %(skills)s) END,
       CASE WHEN g <= %(tags)s - 50 THEN 'Skill ' || (1 + (g - 1) %% %(skills)s) END,
       CASE WHEN g <= %(tags)s - 50 THEN CASE WHEN (1 + (g - 1) %% %(skills)s) %% 5 = 0 THEN 'ccs' ELSE 'tsc' END END,
       CASE WHEN g <= %(tags)s - 50 THEN (1 + (g - 1) %% %(skills)s) %% 11 = 0 END,
       CASE WHEN g <= %(tags)s - 50 THEN FALSE END,
       CASE WHEN g > %(tags)s - 50 THEN 'unmatched' WHEN g > %(skills)s THEN 'alias' ELSE 'exact' END,
       CASE WHEN g > %(tags)s - 50 THEN NULL WHEN g > %(skills)s THEN 0.9 ELSE 1.0 END,
       %(first)s, %(last)s
FROM generate_series(1, %(tags)s) g;
SELECT setval('clean.dim_skill_skill_key_seq', %(tags)s);

INSERT INTO clean.dim_date (date_key, date, year, month, day, day_of_week, day_name, is_weekend, week_start, iso_week)
SELECT to_char(d, 'YYYYMMDD')::int, d::date, extract(year FROM d), extract(month FROM d), extract(day FROM d),
       extract(isodow FROM d), trim(to_char(d, 'Day')), extract(isodow FROM d) >= 6,
       date_trunc('week', d)::date, extract(week FROM d)
FROM generate_series(%(first)s::date, %(last)s::date, interval '1 day') d;
"""

JOBS_SQL = """
CREATE TEMP TABLE demo_job ON COMMIT DROP AS
SELECT g AS job_key, 'job-' || g AS uuid, floor(random() * %(days)s)::int AS first_day,
       1 + floor(-ln(1 - random()) * 10)::int AS life,
       1 + floor(%(occ)s * power(random(), 1.5))::int AS occ,
       1 + floor(%(cats)s * random() * random())::int AS cat,
       1 + floor(600 * power(random(), 2))::int AS comp,
       floor(7 * power(random(), 1.3))::int AS lvl,
       random() AS r_sal, random() AS r_hide, random() AS r_exp
FROM generate_series(1, %(postings)s) g;

INSERT INTO clean.dim_job (job_key, job_uuid, title, ssoc_code, ssoc_title, ssoc_major_group, mom_group, category,
                           company_name, company_ssic_desc, position_level, min_years_exp,
                           salary_min, salary_max, salary_is_hidden, vacancies, attr_hash, valid_from, last_seen_date)
SELECT j.job_key, j.uuid, 'Posting ' || j.job_key, o.ssoc_code, o.ssoc_title, o.major_group, o.mom_group,
       (%(categories)s::text[])[j.cat],
       'Company ' || j.comp, 'Industry ' || (1 + j.comp %% 25),
       (%(levels)s::text[])[1 + j.lvl], j.lvl * 2 + floor(j.r_exp * 3)::int,
       CASE WHEN j.r_sal < 0.02 THEN 0
            WHEN o.major_group <= 3 THEN round(((4000 + 5000 * j.r_sal) * (1 + 0.10 * j.lvl))::numeric, -2)
            WHEN o.major_group <= 5 THEN round(((2000 + 1500 * j.r_sal) * (1 + 0.10 * j.lvl))::numeric, -2)
            ELSE round(((1800 + 1200 * j.r_sal) * (1 + 0.10 * j.lvl))::numeric, -2) END,
       CASE WHEN j.r_sal < 0.02 THEN 0
            WHEN o.major_group <= 3 THEN round(((4000 + 5000 * j.r_sal) * (1 + 0.10 * j.lvl))::numeric * 1.3, -2)
            WHEN o.major_group <= 5 THEN round(((2000 + 1500 * j.r_sal) * (1 + 0.10 * j.lvl))::numeric * 1.3, -2)
            ELSE round(((1800 + 1200 * j.r_sal) * (1 + 0.10 * j.lvl))::numeric * 1.3, -2) END,
       j.r_hide < 0.25, 1 + (j.job_key %% 3), 'h' || j.job_key,
       %(first)s::date + j.first_day, %(first)s::date + least(j.first_day + j.life - 1, %(days)s - 1)
FROM demo_job j
JOIN (SELECT ssoc_code, ssoc_title, major_group, mom_group, row_number() OVER (ORDER BY ssoc_code) AS n
      FROM clean.ref_ssoc) o ON o.n = j.occ;
SELECT setval('clean.dim_job_job_key_seq', %(postings)s);

INSERT INTO clean.bridge_job_skill (job_key, skill_key)
SELECT job_key, skill_key FROM (
    SELECT j.job_key, 1 + ((floor(%(skills)s * power(random(), 2.5))::int + (j.occ %% 9) * 29) %% %(tags)s) AS skill_key
    FROM demo_job j CROSS JOIN LATERAL generate_series(1, 3 + (j.job_key %% 9)::int) s
    UNION ALL
    SELECT job_key, 7 FROM demo_job WHERE random() < 0.02 + 0.10 * first_day / GREATEST(%(days)s - 1, 1)
) x
GROUP BY job_key, skill_key;

-- skills whose number is a multiple of 13 pay 12 percent more each, so the salary tabs and the estimator have a signal
UPDATE clean.dim_job d
SET salary_min = round(d.salary_min * p.mult, -1), salary_max = round(d.salary_max * p.mult, -1)
FROM (SELECT b.job_key, power(1.12, count(*)) AS mult
      FROM clean.bridge_job_skill b JOIN clean.dim_skill s USING (skill_key)
      WHERE s.skill_key <= %(skills)s AND s.skill_key %% 13 = 0 GROUP BY b.job_key) p
WHERE p.job_key = d.job_key AND d.salary_min > 0;

INSERT INTO clean.fact_job_day (snapshot_date, job_uuid, job_key, date_key, views_total, applications_total, repost_count,
        edit_count, status, days_since_original_posting, prev_snapshot_date, hours_since_prev, is_first_snapshot,
        views_delta, applications_delta, reposted_since_prev, edited_since_prev)
SELECT %(first)s::date + i, j.uuid, j.job_key, to_char(%(first)s::date + i, 'YYYYMMDD')::int,
       20 * (i - j.first_day + 1), 2 * (i - j.first_day + 1), 0, 0, 'Open', i - j.first_day,
       CASE WHEN i > j.first_day THEN %(first)s::date + i - 1 END,
       CASE WHEN i > j.first_day THEN 24 END, i = j.first_day,
       CASE WHEN i > j.first_day THEN floor(random() * 40)::int END,
       CASE WHEN i > j.first_day THEN floor(random() * (1 + 5 * (10 - o.major_group) / 9.0))::int END,
       CASE WHEN i > j.first_day THEN random() < 0.01 END, FALSE
FROM demo_job j
JOIN clean.dim_job d ON d.job_key = j.job_key
JOIN clean.ref_ssoc o ON o.ssoc_code = d.ssoc_code
CROSS JOIN LATERAL generate_series(j.first_day, least(j.first_day + j.life - 1, %(days)s - 1)) i;
"""

OPS_SQL = """
INSERT INTO raw.ingest_runs (snapshot_date, pages_fetched, jobs_fetched, api_total, seconds_taken, finished_at)
SELECT d::date, 1000, c, c + 40, 1800 + random() * 300, d + interval '10 hours'
FROM (SELECT snapshot_date AS d, count(*) AS c FROM clean.fact_job_day GROUP BY 1) t;

INSERT INTO clean.load_log (run_id, snapshot_date, step, started_at, finished_at, seconds, rows_in, rows_out)
SELECT 'demo-' || d, d, s.step, d + interval '10 hours 30 minutes', d + interval '10 hours 40 minutes', s.sec, c, c
FROM (SELECT snapshot_date AS d, count(*) AS c FROM clean.fact_job_day GROUP BY 1) t,
     (VALUES ('parse_raw', 40.0), ('upsert_dim_job', 55.0), ('load_fact_job_day', 70.0)) s(step, sec);

INSERT INTO clean.dq_results (snapshot_date, check_name, category, severity, status, observed, threshold, details)
SELECT d, c.name, 'business_rule', 'fail', CASE WHEN c.name = 'salary_hidden_share' AND extract(day FROM d)::int %% 7 = 0 THEN 'warn' ELSE 'pass' END,
       c.obs, c.thr, 'synthetic'
FROM (SELECT DISTINCT snapshot_date AS d FROM clean.fact_job_day) t,
     (VALUES ('raw_rows_present', 100000, 100), ('salary_hidden_share', 0.25, 0.3), ('counters_never_fall', 0, 0)) c(name, obs, thr);
"""


def seed(conn, days: int = 21, postings: int = 250_000, first: date | None = None) -> dict:
    first = first or date.today() - timedelta(days=days - 1)
    params = {"days": days, "postings": postings, "first": first, "last": first + timedelta(days=days - 1),
              "occ": OCCUPATIONS, "skills": SKILLS, "tags": TAGS, "cats": len(CATEGORIES),
              "categories": list(CATEGORIES), "levels": list(LEVELS)}
    started = time.perf_counter()
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM clean.fact_job_day")
            if cur.fetchone()[0]:
                raise SystemExit("clean.fact_job_day is not empty: refusing to add synthetic data to a real warehouse")
            for sql in (REFERENCE_SQL, JOBS_SQL, OPS_SQL):
                cur.execute(sql, params)
            cur.execute("ANALYZE")
            cur.execute("SELECT count(*) FROM clean.fact_job_day")
            fact_rows = cur.fetchone()[0]
    return {"first_day": str(first), "days": days, "fact_rows": fact_rows,
            "seconds": round(time.perf_counter() - started, 1)}


if __name__ == "__main__":
    from transform.db import connect

    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--days", type=int, default=21)
    parser.add_argument("--postings", type=int, default=250_000)
    args = parser.parse_args()
    connection = connect()
    try:
        print(seed(connection, args.days, args.postings))
    finally:
        connection.close()
