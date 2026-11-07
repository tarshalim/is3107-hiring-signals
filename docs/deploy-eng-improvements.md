# Deploying the engineering improvements on the collector laptop

Written Fri 6 Nov 2026 for Tarsha. One checklist for everything on branch `feature/eng-improvements`: alerts,
schema drift, our own Airflow image, the Parquet lake, the scale report. Every step is additive and safe to run
twice. Nothing changes the crawl, the live poll, their schedules or any existing table.

**Never run `docker compose down -v`** (it deletes the warehouse volume). Check free memory before step 3 and do
steps 2 and 3 at a quiet time: not from 09:55 until the 10:00 crawl has finished, and not just before a :00 or :30
live poll.

## 1. Pull and add the three tables (any time)

```bash
git pull
docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/09_ops_alerts.sql
docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/10_ops_schema_fields.sql
docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/11_ops_lake_exports.sql
```

Windows PowerShell, for each file:
`Get-Content sql/init/09_ops_alerts.sql | docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'`

What happens at once, with no restart (the scheduler re-parses the DAG files within about 30 seconds):
- every DAG writes failures and retries to `ops.alert_log` (`docs/alerts-and-monitoring.md`);
- the ingest DAG gets a fourth task, `check_schema_drift`, beside `load_raw` (warn only, about 22 s);
- a new DAG, `weekly_lake_export`, appears **paused**.

If step 1 is skipped, the alert callback only logs a warning and `check_schema_drift` is skipped: nothing fails.

## 2. Optional: Telegram alerts

Create a bot with @BotFather, add it to the team chat, and put the two values in `.env` (never in git):

```
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

They reach the containers in step 3.

## 3. Switch to our own Airflow image (`docs/docker-images.md`)

```bash
docker compose build          # 1 to 2 minutes, needs internet once; the running stack keeps running
docker compose up -d          # recreates the Airflow containers and the dashboard on the pinned images
docker compose ps
docker compose exec airflow-scheduler python -c "import openpyxl, rapidfuzz; print(openpyxl.__version__, rapidfuzz.__version__)"
#   expect 3.1.5 3.14.6
```

From now on the containers no longer pip-install at every start. A task running at the moment of `up -d` is
retried by its DAG's retries.

## 4. When convenient: the lake

```bash
docker compose exec airflow-scheduler airflow dags unpause weekly_lake_export
```

It backfills one run per week since 20 Oct (a few seconds per day, at most two days at a time, about 7 MB a day
under `data/lake/`). The Spark job is optional and heavier (2 GB container): run it on any laptop with a copy of
`data/lake/`, not necessarily the collector (`docs/lake-and-spark.md`, section 3).

## 5. Optional: history and numbers

```bash
python -m quality.snapshot_checks "data/raw/mcf/*.jsonl.gz"     # schema-drift history from the saved files
python -m ops.scale_report --raw-dir data/raw/mcf --restore-file data/raw/mcf/<latest>.jsonl.gz --with-transform \
    --note "Collector warehouse, all real days."                 # rewrites the numbers in docs/scale-and-cost.md
```

Both read the warehouse on port 5433 with the `.env` login. The restore test uses a scratch database that it drops
at the end.

## 6. Check

```bash
docker compose exec warehouse psql -U jobs -d jobs -c "SELECT count(*) FROM ops.alert_log;"
docker compose exec warehouse psql -U jobs -d jobs -c "SELECT snapshot_date, status, details FROM clean.dq_results WHERE check_name = 'raw_schema_drift' ORDER BY 1 DESC LIMIT 3;"
```

The dashboard's Pipeline health tab shows an "Alerts and retries" block.

## Undo

Each step can be undone without touching data: `git checkout` the previous commit and `docker compose up -d`
brings back `apache/airflow:2.10.5` with the start-time install; the three new tables can stay (nothing else reads
them).
