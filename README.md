# Singapore Hiring-Difficulty Dashboard (IS3107 Group 2)

A daily Airflow pipeline that snapshots MyCareersFuture job postings, cleans them and maps their
skills to the SkillsFuture framework, stores them in PostgreSQL, and powers a dashboard of:
skill demand trends, competitiveness, barrier skills, potential skill shortages, and potentially
unrealistic requirements.

## Layout

| Folder | What goes there |
|---|---|
| `dags/` | Airflow DAGs (daily ingest, daily transform, weekly metrics) |
| `ingestion/` | API clients (MyCareersFuture, MOM via data.gov.sg) |
| `transform/` | Cleaning, deduplication, skill standardisation |
| `metrics/` | Weekly hiring signals built from the clean layer |
| `quality/` | Data-quality checks |
| `sql/init/` | Warehouse schema, applied when the database is first created |
| `dashboard/` | Dashboard app |
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

`wait_for_raw` + `load_reference_tables` → `transform_snapshot` → `run_quality_checks` → `log_run_stats`
(`dags/daily_transform_dag.py`, 10:30 Singapore time, after the ingest DAG).

- **transform_snapshot** reads one day from `raw.mcf_job_snapshots`, cleans every posting
  (`transform/parse.py`), maps new skill tags to the SkillsFuture Unique Skills List
  (`transform/skill_mapping.py`) and loads the star schema in `clean`: `dim_job` (SCD Type 2),
  `dim_skill`, `bridge_job_skill`, `dim_date`, `fact_job_day` (one row per posting per day with the
  increase since the previous snapshot). Rerunning a day gives the same tables.
- **run_quality_checks** runs 30 schema, range and business-rule checks and stores the verdicts in
  `clean.dq_results`. A `fail` severity check fails the run.
- **log_run_stats** writes step timings and the skill match rate to `clean.load_log`.

Without Airflow (for teammates and verification):

```bash
python -m ingestion.load_snapshot_file data/raw/mcf/2026-10-25.jsonl.gz --ingest-runs ingest_runs.csv  # replay a saved day
python -m transform.run --date 2026-10-25                                                           # transform + checks
```

Design notes and measured numbers: `docs/role2-cleaning-and-warehouse.md`, ER diagram: `docs/er-diagram.md`,
step-by-step reproduction: `docs/VERIFY-ME.md`.

## Weekly metrics (MOM comparison and hiring signals)

One DAG, `weekly_metrics` (`dags/weekly_metrics_dag.py`, Tuesdays 14:00 Singapore time), with two independent
branches that read the same week and the same snapshot:

```
refresh_mom_stats -> compare_mcf_with_mom
pick_snapshot -> build_posting_week -> 6 signal tables in parallel -> check_and_log
```

- **refresh_mom_stats** downloads MOM's "Number of Job Vacancy by Occupation - Quarterly" from data.gov.sg
  (`d_86dffa3d28e4c3ee085c3f99abad6e9f`, 1998-Q1 onwards, about 450 rows) with `ingestion/mom_client.py` and
  upserts `raw.mom_vacancy_rows` and `clean.mom_vacancy_quarterly`. Figures before 2006 exclude the public sector.
- **compare_mcf_with_mom** writes `metrics.mcf_vs_mom_share`: per occupation group, the share of MCF postings in
  the latest snapshot of the run's week vs MOM's share of vacancies in the latest quarter, MOM's 1-year and
  5-year change, and a `disagrees` flag when the shares differ by 10 percentage points or more.
  `metrics.mom_vacancy_trend` is the long MOM series for the trend chart.
- **pick_snapshot** → **build_posting_week** → six signal tables → **check_and_log** (`metrics/`) fill the tables of
  `sql/init/05_metrics_signals.sql` from the open postings of the week end (`metrics.posting_week`): barrier
  skills, skill shortages, unrealistic requirements, competitiveness, skill demand and skill salary.
  `metrics.run_log` records the week (`week_end_date`, the interval's last day), the snapshot used and the checks.
  `metrics.mcf_vs_mom_share` above is their MOM validation. Same steps without Airflow:
  `python -m metrics.run --week-ending 2026-11-03`. Table meanings, thresholds, validation: `docs/weekly-metrics.md`.
- Each run covers its data interval (Tuesday 14:00 to the next Tuesday 14:00 SGT, so snapshots Wednesday to
  Tuesday), both branches use the latest snapshot of that week (`transform/snapshot_week.py`), and the run fails
  with the week's dates if that week has no snapshot. `catchup=True` from 20 Oct 2026, so unpausing the DAG
  backfills one run per week since the first snapshot (24 Oct). A rerun replaces its week's rows.
- MOM publishes only 3 occupation groups (Split A) and PME vs non-PME (Split B, since 2013), so both sides are
  mapped from the SSOC first digit with `data/reference/ssoc_major_to_mom_group.csv` and
  `data/reference/mom_occupation_labels.csv`. Postings with no usable SSOC code are counted in an `unknown` row
  and left out of the shares.

On a warehouse created before these tables existed (the collector laptop), add them once by hand, then let
compose recreate the Airflow containers for the new `./metrics` mount. Every file is safe to run twice. Never
`docker compose down -v`: it deletes the warehouse volume. Full steps, including how to fill the weeks the DAG
already ran: `docs/weekly-metrics.md`, section 3.

```bash
docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/03_mom_vacancies.sql
docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/05_metrics_signals.sql
docker compose up -d
# Windows PowerShell:
Get-Content sql/init/03_mom_vacancies.sql | docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
Get-Content sql/init/05_metrics_signals.sql | docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
docker compose up -d
```

Without Airflow: `python -m transform.mom_vacancies` (or `--skip-download` to rebuild only the comparison;
`--week-ending 2026-10-27` for a past week, default today).

## Extra data (`ext` schema)

Ten daily snapshots are a short window, so `sql/init/04_ext_schema.sql` adds three sources that widen it
(details, licences, row counts and findings: `docs/extra-data.md`):

- **2019 MyCareersFuture crawl** (Kaggle, 20,296 postings) for "2019 vs now" on skills and pay
- **MOM wage tables** (data.gov.sg API): actual wages for 562 SSOC occupations (June 2024), and median
  income by occupation group 2001 to 2024
- **Posting history** rebuilt from the posting dates inside our own snapshots: weekly new postings per skill
  and per SSOC group, with a coverage label per week (only postings still open on 24 Oct can be seen)

```bash
python -m transform.posting_history                                   # after the daily transform
python -m ingestion.datagov_wages                                     # downloads through the official API
curl -L -o data/ext/kaggle_mcf2019/archive.zip --create-dirs   https://www.kaggle.com/api/v1/datasets/download/asrsaiteja/mycareerfuture-job-postings
python -m ingestion.kaggle_mcf2019 data/ext/kaggle_mcf2019/archive.zip
```

The `weekly_ext_data` DAG (`dags/weekly_ext_data_dag.py`, Mondays 12:00) runs the same steps every week. On a
warehouse created before this file existed, apply it once (safe to run twice), then unpause the DAG:

```bash
docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/04_ext_schema.sql
# Windows PowerShell:
Get-Content sql/init/04_ext_schema.sql | docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

## Tests

```bash
pip install -r requirements.txt
pytest            # offline; the Postgres end-to-end test runs only when WAREHOUSE_TEST_DSN is set
```

## Team

- Name (GitHub username): area of responsibility
