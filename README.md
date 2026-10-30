# Singapore Hiring-Difficulty Dashboard (IS3107 Group 2)

A daily Airflow pipeline that snapshots MyCareersFuture job postings, cleans them and maps their
skills to the SkillsFuture framework, stores them in PostgreSQL, and powers a dashboard of:
skill demand trends, competitiveness, barrier skills, potential skill shortages, and potentially
unrealistic requirements.

## Layout

| Folder | What goes there |
|---|---|
| `dags/` | Airflow DAGs (`daily_ingest_dag.py` now, weekly metrics later) |
| `ingestion/` | API clients (MyCareersFuture now; Adzuna and MOM later) |
| `transform/` | Cleaning, deduplication, skill standardisation |
| `quality/` | Data-quality checks |
| `sql/init/` | Warehouse schema, applied when the database is first created |
| `dashboard/` | Streamlit dashboard (`app.py`, one module per tab in `tabs/`) |
| `data/reference/` | Small static files (e.g. SkillsFuture framework). Everything else in `data/` is git-ignored |
| `tests/` | `pytest` tests |

## Run it locally

You need Docker Desktop.

```bash
cp .env.example .env          # then change the passwords
mkdir -p logs data            # on Linux, also: echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose up -d
```

Open http://localhost:8080 and log in with `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`.
Turn on the `daily_mcf_ingest` DAG and click **Trigger** to run it once now.

For a quick first test, set `MCF_MAX_PAGES=2` in `.env` (200 postings), run `docker compose up -d`
again, and trigger the DAG. Set it back to `0` for real daily runs.

Check the data landed (warehouse is on port 5433):

```bash
docker compose exec warehouse psql -U jobs -d jobs -c \
  "SELECT snapshot_date, count(*) FROM raw.mcf_job_snapshots GROUP BY 1;"
```

## Daily ingestion

`fetch_snapshot → check_snapshot → load_raw`, every day at 06:00 Singapore time.

- **fetch_snapshot** pages through the MyCareersFuture API (100 postings per page, retries with
  backoff on 429/5xx) and saves the raw postings to `data/raw/mcf/<date>.jsonl.gz`.
- **check_snapshot** fails the run if it got fewer than 100 postings or under 90% of the API's total.
- **load_raw** upserts into `raw.mcf_job_snapshots` (one row per posting per day, so re-running a
  day never duplicates) and logs counts and runtime to `raw.ingest_runs` for the performance section.

`catchup=False`: view and application counts can't be fetched for past days, so the DAG never backfills.

## Daily cleaning and warehouse load (role 2)

`wait_for_raw` + `load_reference_tables` → `transform_snapshot` → `run_quality_checks` → `refresh_dashboard_marts` + `log_run_stats`
(`dags/daily_transform_dag.py`, 10:30 Singapore time, after the ingest DAG).

- **transform_snapshot** reads one day from `raw.mcf_job_snapshots`, cleans every posting
  (`transform/parse.py`), maps new skill tags to the SkillsFuture Unique Skills List
  (`transform/skill_mapping.py`) and loads the star schema in `clean`: `dim_job` (SCD Type 2),
  `dim_skill`, `bridge_job_skill`, `dim_date`, `fact_job_day` (one row per posting per day with the
  increase since the previous snapshot). Rerunning a day gives the same tables.
- **run_quality_checks** runs 30 schema, range and business-rule checks and stores the verdicts in
  `clean.dq_results`. A `fail` severity check fails the run.
- **refresh_dashboard_marts** rebuilds the `mart` schema (`sql/init/03_dashboard_marts.sql`) that the
  dashboard reads. It runs only after the quality checks pass.
- **log_run_stats** writes step timings and the skill match rate to `clean.load_log`.

Without Airflow (for teammates and verification):

```bash
python -m ingestion.load_snapshot_file data/raw/mcf/2026-10-25.jsonl.gz --ingest-runs ingest_runs.csv  # replay a saved day
python -m transform.run --date 2026-10-25                                                           # transform + checks
python -m transform.refresh_marts                                                                   # rebuild the dashboard marts
```

Design notes and measured numbers: `docs/role2-cleaning-and-warehouse.md`, ER diagram: `docs/er-diagram.md`,
step-by-step reproduction: `docs/VERIFY-ME.md`.

## Dashboard

```bash
docker compose up -d dashboard        # http://localhost:8501
# or, against the warehouse on port 5433:
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

Five tabs, all filterable by window (7/14/30 days), occupation group, SSOC major group, job category, skill
type and emerging skills:

- **Trending skills**: top 20 skills by share of postings, last-7-days vs previous-7-days change in share, daily trend.
- **Salary**: P25 to P75 monthly salary bands for skills, skill pairs and occupations (disclosed salaries only).
- **Hiring difficulty**: applications per posting, repost rate and days open, combined into a relative index.
- **Career moves**: occupations with similar skill demand, the skill gap between two occupations, and the
  occupations your own skills cover best.
- **Pipeline health**: ingestion coverage, transform timings, data-quality verdicts and skill-match quality.

The tabs read pre-aggregated materialised views in schema `mart`, so a click is a lookup, not a scan of the
fact table. The marts are rebuilt by the DAG's `refresh_dashboard_marts` task. On a database created before
`03_dashboard_marts.sql` existed, build them once with `python -m transform.refresh_marts`.
Week-over-week change needs 14 days of data; until then the dashboard says so instead of showing a number.

## Tests

```bash
pip install -r requirements.txt
pytest            # offline; the Postgres end-to-end test runs only when WAREHOUSE_TEST_DSN is set
```

## Team

- Name (GitHub username): area of responsibility
