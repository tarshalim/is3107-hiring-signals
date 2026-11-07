# Scale, cost and restore

Written Fri 6 Nov 2026. Script: `ops/scale_report.py` (tests: `tests/test_scale_report.py`). The block between the
GENERATED markers at the end is written by the script; everything else is by hand. **Re-run the script on the
collector's warehouse before quoting numbers** (command in section 5): the block below was generated on Brian's
laptop from a throwaway warehouse holding the two real days of 24 and 25 Oct, plus the collector's own run logs
exported on 6 Nov (14 days of `raw.ingest_runs`, `clean.load_log` and Airflow task tries).

## 1. Why this section exists

- "Assume it's huge" (wk5 [11:45]) and "ten megabyte, not ten terabyte ... assume this is one gigabyte"
  (wk5 [1:46:41]): our volume is already past his bar, so the question is how it grows, not whether it is big.
- "Does it work" is not enough (wk3 [1:37:15]): runtimes and sizes with numbers.
- Rent vs buy (wk1 [1:14:53], wk10 [044]) and "no such thing as cheap and good" (wk1 [1:12:41]).
- Backups that take days to restore "will not work in the real world" (wk4 [19:37]): so a restore is timed, not assumed.

## 2. How it grows

One snapshot day is about 90,000 postings. Per day (generated table below): about 140 MB of gz file, about 470 MB of
raw JSONB in Postgres with its TOAST and index, about 150 MB of clean star, about 7 MB of Parquet. A year is
therefore roughly **170 GB of raw JSONB**, 55 GB of clean star (an upper bound: `dim_job` and the bridge grow with
new postings, not with days), 50 GB of gz files and under 3 GB of Parquet.

What we would change at that size (none of it is needed for the project's three weeks):

1. **Partition the daily tables by `snapshot_date`** (monthly range partitions, even in size as wk10 [102] asks).
   The delete-then-insert of a day becomes a cheap partition operation, and old raw months can be detached.
2. **Keep only a rolling window of raw JSONB in Postgres** (say 30 days, about 14 GB). Older raw days live as gz on
   object storage, where a GB costs about a quarter of block storage, and the restore test shows a day comes back in
   well under a minute. The clean star stays in Postgres; history for heavy batch questions lives in the Parquet lake.
3. **Spark only for history-wide questions.** `docs/lake-and-spark.md` measured the crossover: on 2 real days
   Postgres answers weekly skill demand in 7 s and Spark needs about 40 s with start-up; on 30 days (2.7 million
   fact rows) Spark on 2 cores already matches or beats Postgres. Our weekly signals read one week-end snapshot,
   not the whole history, so they stay in SQL.

## 3. Cost: laptop vs a small VM

| Option | Monthly cost | Fits our job? |
|---|---|---|
| Collector laptop (today) | $0 extra (already bought) | Yes, but one machine: it must be on at 10:00 and every 30 minutes, the 6 GB WSL cap limits what else runs, and a dead laptop is a lost day |
| One 4 GB VM (e.g. AWS t3.medium, Singapore, on demand) | about **$38.50** plus disk (see the generated table for a year of storage) | Yes. "A daily batch job is a good instance where you will use IaaS" (wk10 [056]): Docker Compose runs unchanged, and decoupling storage (object storage for gz and Parquet) from compute (the VM) means the VM can be resized or replaced (wk10 [098]) |
| Serverless functions | cents | **No** for the crawl: it runs 28 minutes on a typical day and up to 80 (generated table), past the 15-minute limit of a typical function. "PaaS kills long jobs" (wk10 [062]) |
| Managed Postgres | more than the VM for the same size | Only when nobody wants to run backups and upgrades; not worth it for a three-week project |

Prices are AWS on-demand list prices for Singapore as published in 2025 (VM $0.0528 an hour, block storage $0.096 and
object storage $0.025 per GB-month). Check them on calculator.aws before putting them in the report. The trade-off
in one line: the laptop is free but is a single point of failure; about $40 a month buys a machine that is always on.

## 4. Restore

The gz file of each day is the backup of the raw zone (kept on the laptop and copied to Drive every few days). The
script's restore test rebuilds one day from its gz file into a scratch database with the same code the ingest DAG
uses (`ingestion/load_snapshot_file.py`), checks that the row count equals the distinct postings and the lines in the
file and that a posting's JSON is identical, optionally rebuilds the clean tables for that day, then drops the
scratch database. Generated result below.

What it means: one day comes back in under a minute of loading plus about a minute of transform, so the whole
collection period (about 20 days) is back in under an hour on a laptop. A year would take hours, mostly because the
transform must replay days in order (SCD2), which is one more reason to keep the clean star in a database that is
itself backed up (`pg_dump`) rather than only rebuildable.

## 5. Re-run on the collector's warehouse

```bash
# on the collector laptop, warehouse on port 5433, from the repo root
python -m ops.scale_report --raw-dir data/raw/mcf --restore-file data/raw/mcf/<latest>.jsonl.gz --with-transform \
    --note "Collector warehouse, all real days."
# the Airflow task tries, if wanted (exported from Airflow's own database):
docker compose exec -T airflow-db psql -U airflow -d airflow -c "\copy (SELECT dag_id, task_id, run_id, state, try_number, start_date, end_date, round(duration::numeric, 1) AS duration_s FROM task_instance) TO STDOUT WITH CSV HEADER" > airflow_task_runtimes.csv
python -m ops.scale_report --task-runtimes airflow_task_runtimes.csv --raw-dir data/raw/mcf
```

The restore test needs about 0.5 GB of free disk for the scratch database while it runs and drops it afterwards. It
never touches the real tables. Run it at a quiet time (not during the 10:00 crawl).

## 6. Generated numbers

<!-- BEGIN GENERATED: ops/scale_report.py -->

_Generated 2026-11-06 23:18 +0800 (measurements); re-rendered 2026-11-07 by `python -m ops.scale_report` from warehouse `localhost:5544/jobs` (2 snapshot days loaded). Warehouse sizes and the restore test: a throwaway Postgres 16 on Brian's laptop holding the real 24 and 25 Oct snapshots. Collection, transform and task tables: the collector's own logs exported on 6 Nov (14 real days). Re-run on the collector's warehouse._

### Collection (ingest_runs.csv)

| | |
|---|---|
| Days collected | 14 (2026-10-24 to 2026-11-06) |
| Postings per day | 89,714 median (85,859 to 91,825) |
| Posting-days so far | 1,249,749 |
| Crawl time | 28 min median, 80 min longest |
| Coverage (fetched / API total) | 99.2% median, 98.1% lowest |

### Transform time per day (transform_load_log.csv)

| Step | Days | Median s | Max s |
|---|---|---|---|
| dag_run | 10 | 77.8 | 1321.2 |
| parse_raw | 14 | 44.8 | 166.2 |
| load_fact_job_day | 14 | 9.1 | 39.3 |
| upsert_dim_job | 14 | 6.1 | 22.0 |
| load_bridge_job_skill | 14 | 1.9 | 35.4 |
| upsert_dim_skill | 14 | 1.0 | 6.0 |
| upsert_dim_date | 14 | 0.0 | 0.0 |
| write_rejected | 14 | 0.0 | 0.0 |

Left out as not run times (over a day, logged before the 30 Oct fix): {'dag_run': 4}.

### Airflow task runs (from Airflow's task_instance table)

| DAG | Task | Runs | Succeeded | Median s | Max s | Needed a retry | Ended failed |
|---|---|---|---|---|---|---|---|
| daily_mcf_ingest | check_snapshot | 16 | 16 | 0.5 | 3.1 | 0 | 0 |
| daily_mcf_ingest | fetch_snapshot | 16 | 16 | 1571.6 | 4830.0 | 4 | 0 |
| daily_mcf_ingest | load_raw | 16 | 16 | 72.0 | 229.7 | 0 | 0 |
| daily_mcf_transform | load_reference_tables | 12 | 8 | 2.8 | 28.9 | 0 | 0 |
| daily_mcf_transform | log_run_stats | 12 | 8 | 4.9 | 13.0 | 0 | 0 |
| daily_mcf_transform | refresh_dashboard_marts | 2 | 2 | 83.2 | 88.6 | 0 | 0 |
| daily_mcf_transform | run_quality_checks | 12 | 8 | 9.2 | 26.1 | 0 | 0 |
| daily_mcf_transform | train_salary_model | 2 | 2 | 15.6 | 19.1 | 0 | 0 |
| daily_mcf_transform | transform_snapshot_task | 12 | 8 | 66.2 | 239.5 | 0 | 0 |
| daily_mcf_transform | wait_for_raw | 12 | 8 | 0.3 | 1.3 | 0 | 0 |
| mcf_live_poll | check_poll | 59 | 46 | 0.3 | 1.5 | 0 | 0 |
| mcf_live_poll | daily_ingest_not_running | 59 | 56 | 0.4 | 17406.6 | 0 | 0 |
| mcf_live_poll | fetch_newest | 59 | 46 | 5.5 | 1182.8 | 1 | 0 |
| mcf_live_poll | load_poll | 59 | 46 | 0.3 | 1.3 | 0 | 0 |
| weekly_metrics | build_barrier_skill | 2 | 2 | 51.5 | 69.7 | 2 | 0 |
| weekly_metrics | build_competitiveness | 2 | 2 | 81.3 | 93.3 | 2 | 0 |
| weekly_metrics | build_posting_week | 2 | 2 | 29.0 | 36.8 | 2 | 0 |
| weekly_metrics | build_shortage_skill | 2 | 2 | 38.5 | 41.6 | 2 | 0 |
| weekly_metrics | build_skill_demand | 2 | 2 | 31.5 | 33.3 | 2 | 0 |
| weekly_metrics | build_skill_salary | 2 | 2 | 33.3 | 34.9 | 2 | 0 |
| weekly_metrics | build_unrealistic_requirements | 2 | 2 | 8.6 | 12.6 | 2 | 0 |
| weekly_metrics | check_and_log | 2 | 2 | 1.9 | 2.2 | 2 | 0 |
| weekly_metrics | compare_mcf_with_mom | 2 | 2 | 2.6 | 2.8 | 2 | 0 |
| weekly_metrics | pick_snapshot | 2 | 2 | 0.9 | 1.2 | 2 | 0 |
| weekly_metrics | refresh_mom_stats | 2 | 2 | 3.2 | 3.7 | 2 | 0 |
| weekly_metrics | wait_for_week_end_snapshot | 2 | 2 | 1.0 | 1.1 | 2 | 0 |

### Warehouse size now

| Schema | Size |
|---|---|
| raw | 947.4 MB |
| clean | 307.0 MB |
| mart | 105.3 MB |
| ops | 0.2 MB |
| ext | 0.2 MB |
| metrics | 0.1 MB |
| ml | 0.0 MB |
| **database total** | **1.3 GB** |

Largest tables:

| Table | Size | Rows (estimate) |
|---|---|---|
| raw.mcf_job_snapshots | 947.3 MB | 180,741 |
| clean.bridge_job_skill | 121.1 MB | 1,329,304 |
| clean.dim_job | 120.1 MB | 92,817 |
| mart.skill_slice_day | 72.8 MB | 502,953 |
| clean.fact_job_day | 54.3 MB | 180,741 |
| mart.occupation_skill_latest | 10.6 MB | 124,545 |
| clean.dim_skill | 10.2 MB | 21,618 |
| mart.industry_skill_latest | 7.0 MB | 57,288 |

### Growth per snapshot day and projection

| Store | Per day | 30 days | 90 days | 1 year (365 days) | $ a month to keep 1 year |
|---|---|---|---|---|---|
| Raw JSONB (raw.mcf_job_snapshots) | 473.6 MB | 13.9 GB | 41.6 GB | 168.8 GB | $16.21 |
| Clean star (fact, dim_job, bridge, dim_skill) | 152.8 MB | 4.5 GB | 13.4 GB | 54.5 GB | $5.23 |
| Raw gz files (data/raw/mcf) | 141.1 MB | 4.1 GB | 12.4 GB | 50.3 GB | $1.26 |
| Parquet lake (data/lake/fact_job_day) | 7.1 MB | 212.0 MB | 636.1 MB | 2.5 GB | $0.06 |

Rows per day: 90,370 raw rows a day (median of 2 days). Prices: block storage $0.096/GB-month (Postgres on a VM disk), object storage $0.025/GB-month (files), a 4 GB VM $38.54/month; AWS Singapore list prices, check before quoting.

### Restore test: 2026-10-25.jsonl.gz

| | |
|---|---|
| File | 138.8 MB, 88,916 postings |
| Read the file only | 2.4 s |
| **Restore into raw (same code as load_raw)** | **30.6 s**, 88,916 rows, 466.0 MB in Postgres |
| Checks | rows = distinct postings = lines in file, first posting's JSON identical: passed |
| Rebuild clean for the day | 58.5 s (88,916 fact rows), plus 1.7 s to load the reference files once |

<!-- END GENERATED -->
