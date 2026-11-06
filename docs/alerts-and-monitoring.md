# Alerts and monitoring

Written Fri 6 Nov 2026. Code: `dags/common/alerts.py`, `sql/init/09_ops_alerts.sql`, the "Alerts and retries" part
of `dashboard/tabs/pipeline.py`. Tests: `tests/test_alerts.py`, `tests/test_integration_ops.py`.

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
