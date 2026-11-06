# Columnar lake and Spark, with numbers

Written Fri 6 Nov 2026. Code: `transform/lake_export.py`, `dags/weekly_lake_export_dag.py`,
`sql/init/11_ops_lake_exports.sql`, `spark/skill_demand.py`, `spark/benchmark.py`, the `spark` service in
`docker-compose.yml`. Tests: `tests/test_lake_export.py`, `tests/test_integration_ops.py`.

**Which data the numbers come from.** Section 4 is measured on the **real** snapshots of 24 and 25 Oct 2026
(91,825 and 88,916 postings) replayed into a throwaway Postgres 16 container on Brian's laptop and run through the
real transform. Section 5 is **synthetic**: those two real days copied 15 times into 30 consecutive days, to see how
the tools scale. Re-run `spark/benchmark.py` on the collector's warehouse (14+ real days) before quoting final numbers.

## 1. What it is

```
clean star (Postgres) --weekly_lake_export--> data/lake/fact_job_day/snapshot_date=YYYY-MM-DD/part-0.parquet
                                              data/lake/dim_skill/part-0.parquet
                                                   |
                                    spark/skill_demand.py (PySpark, compose profile "spark")
                                                   |
                                              data/lake/_spark/skill_demand_weekly/   weekly skill demand
```

- **Lake layout.** One Parquet file per snapshot day, in a Hive-style folder `snapshot_date=...`, so Spark and
  pyarrow see `snapshot_date` as a column and skip days a query does not need (partition pruning). Each file is
  that day's `clean.fact_job_day` rows (grain: one posting on one snapshot day) with the attributes of the
  `dim_job` version live that day and the posting's skill keys as a list column. `dim_skill` is one small file
  rewritten every run, so a skill remap reaches every past day without rewriting old files.
- **Explicit schema** (`FACT_COLUMNS` in `transform/lake_export.py`): every day has the same column types, never
  inferred. A day whose source JSON changed is caught upstream by the schema-drift check, not by a lake file with
  different types.
- **Idempotent.** Each day is written to a hidden temp file in its folder and renamed over `part-0.parquet` in one
  step, so a rerun overwrites the day and a reader never sees half a file (files starting with `.` or `_` are
  ignored by Spark and pyarrow). A rerun gives a byte-identical file (checked with md5 in the Airflow image). A
  day with no rows in clean loses its folder, so the lake mirrors clean. Every write is logged in
  `ops.lake_exports` (rows, bytes, seconds).
- **Memory.** Rows stream from a server-side cursor in batches of 50,000 (one Parquet row group each), so the
  export never holds a day as Python objects.
- **The DAG** `weekly_lake_export`: `list_week_days -> export_day` (one mapped task per day, at most 2 at once) plus
  `export_dim_skill`, then `summarise`. Tuesdays 21:00 SGT, the same Wednesday-to-Tuesday week as
  `weekly_metrics`, `catchup=True` from 20 Oct (unpausing it builds the whole lake), paused when first deployed.
- **The Spark job** `spark/skill_demand.py`: weekly skill demand. For each week and skill (`skill_ref`, the
  SkillsFuture id or the MCF tag, as in `metrics/`): distinct postings, posting-days, share of the week's
  postings, and the applications those postings received that week. Explode the skill list (map), broadcast-join
  the small `dim_skill` (no shuffle), group by week and skill (reduce by key). `spark/benchmark.py` runs the same
  question as SQL on the clean star and checks the two answers match row for row.

## 2. Why these choices

- **Columnar, "you must know"** (wk2 [1:14:17]) and **"IO is always the bottleneck"** (wk5 [51:03]). The raw zone
  keeps whole JSON documents: any question about two fields still reads and parses every document. In Parquet a
  question reads only its columns, compressed per column. Section 4 shows this on our data: the same answer from
  3.4 s (JSONB) or 9.3 s (gz + Python) down to 0.01 s (Parquet).
- **Postgres stays the system of record and the dashboard's source.** The lake is an analytics copy: cheap to keep
  (about 7 MB a day), readable by Spark, pandas and DuckDB, and it decouples heavy batch reads from the warehouse
  the dashboard and the DAGs use (wk10 [098], "we want decoupling" of storage and compute).
- **A DAG of its own, weekly.** Nothing reads the lake within a week, and one run writes seven small files. A
  separate DAG means a failed export never turns `weekly_metrics` or the daily transform red, and it can be paused
  on a laptop short of memory without touching the collector's path. One mapped task per day keeps tasks small
  and atomic (wk7 [106]): a failure retries one day.
- **Why the raw zone is not copied into the lake.** The gz files are already the raw backup (the restore test in
  `docs/scale-and-cost.md` rebuilds a day from one in about half a minute), every field the analysis reads is in
  the fact export, and a full raw Parquet copy needs a schema inferred per day (29 nested top-level columns),
  which would change whenever the API's shape does. The benchmark still converts one raw day to Parquet, to give
  the like-for-like size (section 4).
- **Spark: the right tool for the job, decided with numbers.** He invited it twice ("you might do this in your
  project", wk5 [23:07]; wk9 [150]) and also warned "don't over engineer things" (wk5 [1:46:10]) and that it is
  "not so straightforward to set it up" (wk9 [133]). So it is built, pinned, run and measured, but kept out of the
  daily path: it is a one-off job under a compose profile, and section 5 says when it would start to pay.

## 3. Runbook

```bash
# 0. once: the export log table (safe to run twice) and the Spark image (about 540 MB)
docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/11_ops_lake_exports.sql
docker pull apache/spark:3.5.7-python3
# 1. build the lake: unpause weekly_lake_export (it backfills every week), or from the laptop:
python -m transform.lake_export --all                  # every day in clean; --date YYYY-MM-DD for one day
# 2. run the Spark job (never started by `docker compose up -d`)
docker compose --profile spark run --rm spark
# 3. measure: sizes, five-way scan, Spark against Postgres (writes data/lake/_bench/benchmark.md)
python spark/benchmark.py --raw-dir data/raw/mcf
```

Memory: the export is a few tens of MB; the Spark container is capped at 2 GB (local mode, 2 cores, 1 GB driver)
and removed when it finishes. Check free memory before step 2 on the collector laptop, or run steps 2 and 3 on
another laptop with a copy of `data/lake/` (about 7 MB a day).

PowerShell: the same commands, with `Get-Content sql/init/11_ops_lake_exports.sql | docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'` for step 0.

## 4. Measured on the real snapshots (24 and 25 Oct 2026)

`python spark/benchmark.py --raw-dir <folder with the two gz files> --repeat 3` after `transform.lake_export --all`
and the Spark job. MB = 2^20 bytes. "Warm" is the median of the repeated runs.

**Size of one day** (24 Oct, 91,825 postings):

| Format | Size | Compared with the gz file |
|---|---|---|
| JSONL.gz, the raw file the crawl writes | 143.5 MB | 1x |
| JSONB in Postgres, `payload` column only | 404.5 MB | 2.8x |
| JSONB in Postgres, whole raw table with TOAST and index | 473.6 MB a day | 3.3x |
| Full raw payload as Parquet (zstd, schema inferred by pyarrow, benchmark only) | 81.2 MB (29.2 MB without the HTML `description`) | 0.57x |
| Clean star in Postgres (fact, dim_job, bridge, dim_skill) | 152.8 MB a day (fact table alone 27.2 MB) | 1.1x |
| **Clean fact as Parquet (the lake)**: fact columns, the live version's attributes and the skill list | **7.1 MB** | **0.05x** |

So the lake keeps every column the analysis reads in 1/20 of the gz file and 1/65 of the raw table. Exporting a
day takes about 2.3 s warm (15 s the first time, when Postgres reads the day from disk), in the Airflow image too.

**One question, five ways**: postings, views and applications per category on 24 Oct.

| Method | First run | Warm | Answer (postings, views, applications) |
|---|---|---|---|
| gz file, Python `json` (what a script would do) | 9.32 s | (1 run) | 91,825 / 11,267,108 / 811,534 |
| gz file, pyarrow's C++ JSON reader | 2.96 s | 2.18 s | same |
| Postgres JSONB (`payload->'metadata'->>...`) | 4.45 s | 3.44 s | same |
| Postgres clean star (fact joined to dim_job) | 0.164 s | 0.037 s | same |
| **Parquet lake, 3 columns with pyarrow** | 0.104 s | **0.010 s** | same |

All five give the same totals. The JSON formats must read and parse every whole document for three numbers; the
columnar file reads three columns. That is wk5's "IO is always the bottleneck" measured on our own data: 340 times
faster than JSONB, 900 times faster than parsing the gz file in Python.

**Weekly skill demand, Postgres against Spark** (180,741 fact rows, 21,468 result rows):

| | First run | Warm | Notes |
|---|---|---|---|
| Postgres 16, `SKILL_DEMAND_SQL` on the clean star | 7.7 s | 7.2 s | default settings |
| Spark 3.5.7, `local[2]`, 1 GB driver, on the lake | 16.5 s | 9.0 to 11.5 s | plus start-up: the spark-submit process took 39 s for three runs, `docker compose run` 43 s wall |

The two answers are **identical** (21,468 rows each, no row only on one side, no value different). At this size
Postgres wins: Spark's start-up alone (container, JVM, session) is longer than the whole SQL query.

## 5. Synthetic scale-up: 30 days

To see the trend, the two real days were copied 15 times into 30 consecutive days, in a scratch database (`fact`
rows re-dated, same postings, same skills) and in a matching scratch lake (the same Parquet files under new
`snapshot_date=` folders). **Synthetic**: the same postings repeat, so this measures the engines on 15 times the
rows, not new market behaviour. 2,711,115 fact rows (759 MB fact table in Postgres, 213 MB of Parquet), about 33
million posting-skill rows after the explode, 107,340 result rows.

| | Per run | Notes |
|---|---|---|
| Postgres 16, default `work_mem` (4 MB) | 150.5 s, 150.0 s | one query, one machine |
| Postgres 16, `work_mem` raised to 256 MB | 158.7 s | no faster: the time goes into the `DISTINCT` and `count(DISTINCT ...)` steps, and Postgres does not parallelise aggregates with `DISTINCT`, so the query runs in one process |
| Spark 3.5.7, `local[2]`, 1.5 GB driver | 122.9 s, 122.6 s, 135.6 s | 388 s for the whole process (3 runs), 391 s wall |

Answers identical again (107,340 rows). From 2 to 30 days the data grew 15 times; Postgres took 21 times longer,
Spark's job 7 to 14 times longer. With only 2 cores Spark is already level with Postgres. With more cores or
machines Spark's share of the work splits further ("involve multiple machines, not just one machine", wk1 [32:00]),
while the single Postgres query cannot.

**What we conclude for the project.** Today's jobs never scan the whole history: the daily transform reads one day,
the weekly signals read one week-end snapshot, and the dashboard reads small precomputed marts. Each of those is
seconds in Postgres, so the pipeline stays Postgres plus Python, and Spark would only add the start-up time and a
JVM on a 6 GB laptop. Spark, on the Parquet lake, is the tool for history-wide questions once there are months of
snapshots: at 30 days it is already level on 2 cores, and it is the one of the two that scales out. That is the
justification with numbers that "no Spark in the daily path" needed.

## 6. Deploy on the collector laptop

Additive: a new SQL file, a new paused DAG and an optional compose service. Nothing that runs today changes.

1. `git pull`, then apply `sql/init/11_ops_lake_exports.sql` (step 0 of the runbook).
2. The DAG needs pyarrow, which the base Airflow image already has (19.0.0), so it works with or without the new
   image from `docs/docker-images.md`.
3. Unpause `weekly_lake_export` when convenient (`docker compose exec airflow-scheduler airflow dags unpause
   weekly_lake_export`). It backfills one run per week since 20 Oct, two days at a time, each day a few seconds.
4. Add `data/lake/` to the usual backups only if wanted: it can always be rebuilt from the warehouse.
