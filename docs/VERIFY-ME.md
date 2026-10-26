# VERIFY-ME: reproduce every number in docs/role2-cleaning-and-warehouse.md

Branch: `feature/clean-and-warehouse`. Everything below was run on Mon 26 Oct 2026 on Brian's laptop
(Windows 11, Python 3.12, Docker Desktop with the WSL memory cap at 6 GB, Postgres 16 in Docker).
Nothing touches the live MyCareersFuture API. Expect about 10 minutes end to end.

## 0. Inputs you need (private, never committed)

| File | Where | Size |
|---|---|---|
| `2026-10-24.jsonl.gz` | `C:\Users\bchua\Desktop\SchoolStuff\IS3107\Admin\` (team Drive copy) | 150.4 MB |
| `2026-10-25.jsonl.gz` | same | 145.6 MB |
| `ingest_runs.csv` | same (export of Tarsha's `raw.ingest_runs`, 2 rows) | 205 bytes |

Reference files are IN the repo under `data/reference/` (SkillsFuture Unique Skills List, TSC mapping,
SSOC 2020 definitions, `skill_aliases.csv`).

## 1. Offline checks (no Docker)

```bash
cd C:\Users\bchua\Desktop\SchoolStuff\IS3107\is3107-hiring-signals
git checkout feature/clean-and-warehouse
pip install -r requirements.txt          # adds openpyxl and rapidfuzz
python -m pytest -q                      # expect: 44 passed, 1 skipped (the Postgres test skips without a DSN)
python -c "import ast,sys; ast.parse(open('dags/daily_transform_dag.py',encoding='utf-8').read()); print('DAG parses')"
```

## 2. Warehouse up (warehouse container only)

Check free commit memory first (Task Manager, Performance, Memory: "Committed" must have at least 7 GB headroom).

```bash
cp .env.example .env     # if you have no .env yet; set WAREHOUSE_PASSWORD to anything, e.g. jobs-local-pw
docker compose up -d warehouse
docker compose ps warehouse            # wait for "(healthy)"
docker compose exec -T warehouse psql -U jobs -d jobs -c "\dt clean.*"   # 11 tables
```

If the `warehouse` volume already existed before this branch, the init scripts did not run again. Apply
the clean schema by hand (it is idempotent):

```bash
docker compose exec -T warehouse psql -U jobs -d jobs < sql/init/02_clean_schema.sql
```

## 3. Replay the two raw days (same upsert as the ingest DAG's load_raw, 30 s each)

```bash
python -m transform.load_reference
#   expect ssg_skills 2388, aliases_total 429 (154 curated + 280 from the TSC file), aliases_invalid [], ssoc_codes 997
python -m ingestion.load_snapshot_file ../Admin/2026-10-24.jsonl.gz --snapshot-date 2026-10-24 --ingest-runs ../Admin/ingest_runs.csv
#   expect rows_loaded 91825
python -m ingestion.load_snapshot_file ../Admin/2026-10-25.jsonl.gz --snapshot-date 2026-10-25 --ingest-runs ../Admin/ingest_runs.csv
#   expect rows_loaded 88916
```

`--ingest-runs` stamps `fetched_at` with the collector's finish time (17:35 and 10:23 SGT), so the
"snapshot label matches the Singapore fetch date" check passes exactly as it does on Tarsha's machine.

## 4. Transform day 1, day 2, then day 2 again (idempotency)

```bash
python -X utf8 -m transform.run --date 2026-10-24
python -X utf8 -m transform.run --date 2026-10-25
python -X utf8 -m transform.run --date 2026-10-25      # the rerun
```

What each run prints and what to expect:

| Run | parse_raw | dim_skill | dim_job | bridge | fact | total | SCD plan |
|---|---|---|---|---|---|---|---|
| 24 Oct | 23.9 s, 91,825 in/out, 1,329,304 skill mentions | 4.1 s, 21,606 new tags | 8.0 s | 14.2 s, 1,329,304 rows | 2.8 s, 91,825 rows | **53 s** | inserted 91,825 |
| 25 Oct | 23.1 s, 88,916 | 0.5 s, 12 new tags | 2.6 s | 0.5 s, 13,989 rows | 3.5 s, 88,916 rows, 87,969 with previous | **30 s** | inserted 992, closed 45, touched 87,924 |
| 25 Oct rerun | 23.5 s | 0.3 s, 0 new | 0.5 s | 0.0 s, 0 rows (no version changed, so nothing to rebuild) | 3.4 s, deleted 88,916 then inserted 88,916 | **28 s** | inserted 0, closed 0, touched 88,916 |

Timings vary by a few seconds between runs. Row counts must match exactly. Each run ends with
`dq summary: {... 'fail': 0 ...}` and a `skill match rate` block (see section 6).

Checksum proof that the rerun changed nothing (run this before and after the rerun, the three md5 values
must be identical: `eca49cd2...`, `94fdca62...`, `6ef375f5...` on the build laptop):

```bash
docker compose exec -T warehouse psql -U jobs -d jobs -x -c "SELECT (SELECT count(*) || '|' || md5(string_agg(job_uuid||'|'||job_key||'|'||views_total||'|'||applications_total||'|'||coalesce(views_delta::text,'-')||'|'||coalesce(applications_delta::text,'-')||'|'||coalesce(hours_since_prev::text,'-')||'|'||is_first_snapshot||'|'||coalesce(reposted_since_prev::text,'-'), E'\n' ORDER BY job_uuid)) FROM clean.fact_job_day WHERE snapshot_date=DATE '2026-10-25') AS fact_25, (SELECT count(*) || '|' || md5(string_agg(job_uuid||'|'||job_key||'|'||attr_hash||'|'||valid_from||'|'||coalesce(valid_to::text,'-')||'|'||is_current||'|'||last_seen_date, E'\n' ORDER BY job_uuid, valid_from)) FROM clean.dim_job) AS dim_job, (SELECT count(*) || '|' || md5(string_agg(job_key||'|'||skill_key||'|'||is_key_skill, E'\n' ORDER BY job_key, skill_key)) FROM clean.bridge_job_skill) AS bridge;"
```

Optional extra: rerun the OLDER day (`--date 2026-10-24`) and check the checksums again. Expect
`out_of_order: 45` in the dim_job step (the 45 superseded versions are left alone) and
`next_snapshot_rows_recomputed: 87969`, with the checksums unchanged.

## 5. Numbers to read off the warehouse

```bash
docker compose exec -T warehouse psql -U jobs -d jobs
```

Row counts (after both days):

```sql
SELECT 'dim_job' t, count(*) FROM clean.dim_job UNION ALL SELECT 'dim_job current', count(*) FROM clean.dim_job WHERE is_current
UNION ALL SELECT 'dim_skill', count(*) FROM clean.dim_skill UNION ALL SELECT 'bridge_job_skill', count(*) FROM clean.bridge_job_skill
UNION ALL SELECT 'fact_job_day', count(*) FROM clean.fact_job_day UNION ALL SELECT 'rejected_rows', count(*) FROM clean.rejected_rows;
-- 92817 | 92772 | 21618 | 1343293 | 180741 | 0
```

Sizes:

```sql
SELECT n.nspname||'.'||c.relname, pg_size_pretty(pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE c.relkind='r' AND n.nspname IN ('raw','clean') ORDER BY pg_total_relation_size(c.oid) DESC;
-- raw.mcf_job_snapshots 947 MB; clean.dim_job 166 MB; clean.bridge_job_skill 120 MB; clean.fact_job_day 72 MB; clean.dim_skill 10 MB; rest under 1 MB
-- clean layer total: 370 MB for two days (first load, before any rerun bloat)
```

Step timings as stored (not just printed):

```sql
SELECT run_id, snapshot_date, step, seconds, rows_in, rows_out FROM clean.load_log ORDER BY load_id;
```

Data-quality verdicts:

```sql
SELECT snapshot_date, status, count(*) FROM clean.dq_results GROUP BY 1,2 ORDER BY 1,2;
-- 2026-10-24: pass 26, warn 4   (experience_out_of_range=2, and three "no previous day" warnings)
-- 2026-10-25: pass 29, warn 1   (experience_out_of_range=2)
SELECT check_name, status, observed FROM clean.dq_results WHERE status <> 'pass' ORDER BY snapshot_date, check_name;
```

Sanity numbers (compare with Admin/data-verification-25Oct.md section 7):

```sql
SELECT f.snapshot_date, count(*) postings,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY views_total) median_views,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY applications_total) median_apps,
       round(100.0*avg((applications_total=0)::int),1) pct_zero_apps,
       round(100.0*avg((repost_count>=1)::int),1) pct_reposted,
       round(100.0*avg((j.min_years_exp>=2)::int) FILTER (WHERE j.position_level='Fresh/entry level'),1) pct_entry_2yrs
FROM clean.fact_job_day f JOIN clean.dim_job j USING (job_key) GROUP BY 1 ORDER BY 1;
-- 2026-10-24 | 91825 | 77 | 2 | 31.3 | 20.5 | 6.6
-- 2026-10-25 | 88916 | 77 | 2 | 31.1 | 20.6 | 6.6
```

Deltas on 25 Oct (compare with the verification note's "counters" table):

```sql
SELECT count(*) with_prev, min(hours_since_prev), sum(views_delta), sum(applications_delta),
       count(*) FILTER (WHERE views_delta<0) views_fell, count(*) FILTER (WHERE views_delta>0) views_rose,
       count(*) FILTER (WHERE reposted_since_prev) reposted, count(*) FILTER (WHERE edited_since_prev) edited
FROM clean.fact_job_day WHERE snapshot_date='2026-10-25' AND NOT is_first_snapshot;
-- 87969 | 16.80 | 212357 | 10611 | 0 | 73152 | 35 | 11
```

## 6. Skill match rate

Printed at the end of every `transform.run`, or:

```bash
python -c "from transform.db import connect; from transform.skill_mapping import match_rate_report; import json; print(json.dumps(match_rate_report(connect(), '2026-10-25'), indent=1))"
```

Expect, for 25 Oct: mentions 1,286,746, distinct tags 21,540, mapped share by mentions 0.259, by distinct
tags 0.097. By method (mentions): alias about 183k, exact about 149k, fuzzy about 1k, stem about 0.26k.

If you changed `data/reference/skill_aliases.csv` or the thresholds: `python -m transform.load_reference --remap`
re-matches every tag in place (prints how many changed), then rerun the two `transform.run` lines to refresh
`dq_results`.

## 7. Postgres integration test (optional, 2 seconds, uses a scratch database `is3107_test`)

```bash
WAREHOUSE_TEST_DSN=postgresql://jobs:<your WAREHOUSE_PASSWORD>@localhost:5433/jobs python -m pytest tests/test_integration_warehouse.py -v
```

On PowerShell, set the variable first: `$env:WAREHOUSE_TEST_DSN="postgresql://jobs:<pw>@localhost:5433/jobs"`, then run the same pytest command.
It drops and recreates `is3107_test` each time and never touches the `jobs` database.

## 8. Shut down

```bash
docker compose down            # keeps the volume, so the loaded data survives
# then, if nothing else needs Docker:
docker desktop stop
wsl --shutdown
```

## What was NOT run

- The Airflow DAG was not executed in a container (Airflow is not installed on the laptop and the full
  compose stack does not fit the 6 GB Docker cap next to a loaded warehouse). It was parsed with `ast`
  (tests/test_transform_dag.py) and its task bodies call the same functions the CLI runs
  (`transform_snapshot`, `run_checks`, `match_rate_report`, `write_run_summary`, `load_reference`).
  Running it for real on the collector machine: `docker compose up -d`, enable `daily_mcf_transform`,
  trigger with run config `{"snapshot_date": "2026-10-25"}`, and take the Graph View screenshot.
- The live MCF API was not called. The two raw days were replayed from the shared gz files.
