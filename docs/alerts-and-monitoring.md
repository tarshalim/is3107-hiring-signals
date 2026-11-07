# Alerts, monitoring and schema drift

Written Fri 6 Nov 2026. Code: `dags/common/alerts.py`, `sql/init/09_ops_alerts.sql`, the "Alerts and retries" part
of `dashboard/tabs/pipeline.py`; schema drift (section 7): `quality/snapshot_checks.py`, the ingest DAG's
`check_schema_drift` task, `sql/init/11_ops_schema_fields.sql`. Tests: `tests/test_alerts.py`,
`tests/test_snapshot_checks.py`, `tests/test_integration_ops.py`.

## 1. What it does

Every DAG now reports its own failures. A shared callback writes **one row per event** to `ops.alert_log` and, if
the team sets a Telegram bot token, sends the same text to the team chat.

| Event | When | Airflow hook |
|---|---|---|
| `task_retry` | a task failed and Airflow will try it again (e.g. the API's 429 rate limit) | `on_retry_callback` |
| `task_failed` | a task failed for good: retries used up, a failed data-quality check, a sensor timeout | `on_failure_callback` |
| `dag_run_failed` | the run failed with no task to blame, e.g. `mcf_live_poll` hitting its 25-minute `dagrun_timeout` (the scheduler then marks the unfinished tasks skipped, so no task callback fires) | DAG-level `on_failure_callback` |

Each row has `dag_id`, `task_id`, `run_id`, `try_number`, the exception as `Type: message` (first 2,000
characters), the link to the task log in the Airflow UI, `notified` (Telegram sent) and `created_at`.

Wiring, the same two lines in all five DAG files:

```python
from common.alerts import TASK_ALERTS, on_dag_failure
@dag(..., default_args={"retries": 2, "retry_delay": ..., **TASK_ALERTS}, on_failure_callback=on_dag_failure)
```

`dags/common/` is on Airflow's import path because Airflow adds the DAG folder to `sys.path`, and
`dags/.airflowignore` stops the scheduler from parsing it as a DAG file. No compose mount was needed, so the
helper reaches the running containers the moment the file is pulled.

## 2. Why it is built this way

- **Why alert at all** (wk7 [074], his $10,000 failed-job story; wk11 [104], cron has "no mechanism to know whether
  it have really completed"): a red square in the Grid View helps only if someone is looking. The collector runs on
  one laptop, so a failed 10:00 crawl is a lost day (the API cannot be asked for yesterday).
- **Why a table, not just the log** (wk11 [133], store results "into a separate database"): the dashboard and the
  report can count failures and retries per DAG, and the rows survive Airflow's log cleanup. Retries matter too: a
  retry followed by a green run is the pipeline recovering on its own, which is the evidence for "design for
  failure" (wk7 [106]).
- **It never raises.** A callback that throws is logged by Airflow next to the real error and can bury it. Every
  step is wrapped and only logged, and the warehouse connect has a 5 s timeout so a down warehouse cannot hold
  the task slot.
- **The warehouse may be the thing that broke**, so a failed insert still sends the Telegram message, marked
  "not written to ops.alert_log".
- **Idempotent.** The key `(dag_id, task_id, run_id, try_number, event)` (`UNIQUE NULLS NOT DISTINCT`, so the NULL
  `task_id` of a run-level row still counts) means a callback that runs twice writes one row and sends one message.
- **No secrets in git.** Telegram runs only when `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are set in `.env`.
  The token is part of the API URL, so it is replaced by `***` in any error text before that is logged.
- **Runs that failed because a task failed are not logged twice:** the DAG-level callback skips reason
  `task_failure`, because that task's own row already has the error.

## 3. Proof (6 Nov 2026)

In the real `apache/airflow:2.10.5` image, with every repo DAG mounted plus a throwaway DAG whose one task always
fails (`retries=1`), `airflow dags test` wrote exactly:

| alert_id | event | task_id | try_number | exception |
|---|---|---|---|---|
| 1 | task_retry | always_fails | 1 | RuntimeError: probe: simulated MCF API outage (HTTP 503) |
| 2 | task_failed | always_fails | 2 | RuntimeError: probe: simulated MCF API outage (HTTP 503) |

and no `dag_run_failed` row (that run failed because of the task). `airflow dags list-import-errors` reported no
errors for the five DAGs. Offline tests cover the row, truncation, deduplication, the warehouse being down,
Telegram errors not leaking the token, and that every DAG file wires both callbacks.

## 4. Dashboard

The Pipeline health tab has a new **Alerts and retries** block under the data-quality checks: failures and retries
in the last 7 days, the latest alert time (SGT), and the last 14 days of events with a link to each Airflow log.
It reads `ops.alert_log` only (cached 5 minutes, like the tab's other run-log queries). On a warehouse without the
table it says to apply `09_ops_alerts.sql` instead of failing.

## 5. Deploy on the collector laptop

Nothing changes in how the DAGs run. Until step 1 is done the callback only logs a warning line.
**Never run `docker compose down -v`.**

```bash
git pull
# 1. Add the ops schema and table (safe to run twice)
docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/09_ops_alerts.sql
# Windows PowerShell:
Get-Content sql/init/09_ops_alerts.sql | docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
# 2. Optional Telegram: create a bot with @BotFather, add it to the team chat, put the two values in .env
#    TELEGRAM_BOT_TOKEN=...   TELEGRAM_CHAT_ID=...   then recreate the Airflow containers so they see them:
docker compose up -d
# 3. Check: empty until something fails or retries
docker compose exec warehouse psql -U jobs -d jobs -c "SELECT created_at, event, dag_id, task_id, try_number, left(exception, 80) FROM ops.alert_log ORDER BY created_at DESC LIMIT 10;"
```

The DAG files pick up the callback as soon as the scheduler re-parses them (within about 30 seconds of `git pull`),
with no restart.

## 6. For the report and video

- One sentence and the table: "every task failure and retry is a row in `ops.alert_log`, with the error and a log
  link, and a Telegram message to the team".
- Count retries per DAG over the collection period (`SELECT dag_id, event, count(*) FROM ops.alert_log GROUP BY 1, 2`):
  the crawl's rate-limit retries that recovered by themselves are the "design for failure" evidence.
- Video: show the Pipeline health tab's Alerts and retries block.

## 7. Schema-drift check

Dr Lek, about a source changing its schema: "in real life, you wouldn't know" (wk2 [53:04], about 11 times). On
2 Nov the team compared the API's JSON paths by hand. Now the ingest DAG does it every day:

```
fetch_snapshot -> check_snapshot -+-> load_raw
                                  +-> check_schema_drift
```

- **What it compares.** Every field path of every posting in the day's gz file (`metadata.totalNumberOfView`,
  `skills[].skill`, `[]` = inside a list), with how many postings have it and the JSON types seen, against the
  latest earlier day stored in `ops.schema_fields`. Three kinds of change: a **new** path, a **missing** path and a
  **type change** (e.g. `salary.minimum` from number to string; a field that is null all day is not a type
  change).
- **Where the verdict goes.** One row, `raw_schema_drift`, in the existing `clean.dq_results` (category `schema`,
  severity `warn`), next to the transform's 30 checks, so the Pipeline health tab's data-quality table shows it
  with no dashboard change. `observed` is the number of changes, `details` names them with posting counts, for
  example `compared with 2026-11-05 (158 paths, today 159); 1 new: metadata.newCounter (88000 postings)`.
  The first day has nothing to compare with, so it warns once and stores the baseline.
- **Why warn and not fail.** A new field breaks nothing (the parser reads named fields). A missing or retyped field
  that matters also shows up in the transform's own checks (missing core attributes, rejected rows). Failing would
  stop the day's load, and a lost crawl day cannot be fetched again.
- **Why a separate task beside `load_raw`, not inside `check_snapshot`:** if the drift step ever errors (a database
  hiccup), `load_raw` still runs. It reads the gz file, so it does not wait for the load either. Small, atomic
  tasks (wk7 [106]).
- **Whole file, not a sample.** The rarest path on 24 Oct, `schemes[].subScheme.*`, is in 0.7% of postings, and
  reading all 91,825 postings takes about 22 s, so sampling would save little and could make a rare path look
  missing. `DRIFT_SAMPLE` in `quality/snapshot_checks.py` can cap it if a day ever gets much bigger.
- **Idempotent.** A rerun deletes and rewrites that day's paths and upserts the one verdict row. An older day
  reloaded later is compared with the day before it, not with a later day.
- Trial runs (`MCF_MAX_PAGES` > 0) skip it, so 200 postings never become the baseline. A warehouse without
  `ops.schema_fields` skips it with a message instead of failing.

**Measured on the real snapshots** (throwaway warehouse, 6 Nov): 24 Oct, 91,825 postings, **158 paths**, stored as
the baseline; 25 Oct, 88,916 postings, 158 paths, **no change** (`pass`). The same task callable ran inside the
`is3107-airflow:2.10.5-r1` image against the 25 Oct file with the same result.

**Fill the history from the saved files** (oldest first; safe to rerun), so the report can say "checked on every
day from 24 Oct":

```bash
python -m quality.snapshot_checks "data/raw/mcf/*.jsonl.gz"      # WAREHOUSE_* from .env, port 5433; quoted so it also works in PowerShell
```

### Deploy

```bash
git pull
docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/11_ops_schema_fields.sql
# Windows PowerShell:
Get-Content sql/init/11_ops_schema_fields.sql | docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
# optional, fill the history from the saved files (on the laptop, with the warehouse on port 5433):
python -m quality.snapshot_checks "data/raw/mcf/*.jsonl.gz"
# check
docker compose exec warehouse psql -U jobs -d jobs -c "SELECT snapshot_date, status, observed, details FROM clean.dq_results WHERE check_name = 'raw_schema_drift' ORDER BY 1;"
```

The new task appears in the ingest DAG at the next scheduler parse; nothing else in the crawl changes. Between
the crawl (about 10:25) and the transform's checks (about 10:50) the latest day in `clean.dq_results` holds only
this one row, so the Pipeline health tab briefly shows one check for today. That is expected.
