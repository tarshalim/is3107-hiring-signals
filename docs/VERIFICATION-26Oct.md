# Independent verification of the cleaning and warehouse layer (Mon 26 Oct 2026)

Branch `feature/clean-and-warehouse`, 6 commits on top of main, verified by a second agent that did not write
the code. Everything was reproduced from scratch on Brian's laptop: the old `warehouse` Docker volume was
removed (`docker compose down -v`, the volume only held reloadable data), the warehouse container was started
alone, the init scripts ran clean (13 tables), and both raw days were replayed from the shared gz files.
Nothing touched the live MyCareersFuture API and the ingest DAG was not run. Inputs: `Admin/2026-10-24.jsonl.gz`,
`Admin/2026-10-25.jsonl.gz`, `Admin/ingest_runs.csv` (kept outside the repo).

Verdict in one line: **the numbers are real and reproducible to the last row and the last md5, the design
follows what the lecturer taught, and one real bug was found and fixed (the DAG could not be imported by
Airflow). GO for a PR, with the fix commit included.**

## 1. Claimed numbers against reproduced numbers

Every row count matched exactly. Timings differ by a few seconds, as the builder said they would.

| What | Claimed (docs) | Reproduced 26 Oct | Match |
|---|---|---|---|
| Offline pytest | 44 passed, 1 skipped | 44 passed, 1 skipped in 4.5 s (45 after the new test below) | yes |
| Reference load | ssg_skills 2,388, aliases 429 (154 + 280), invalid [], ssoc 997 | 2,388 / 429 (154 + 280) / [] / 997 | yes |
| Raw replay rows | 91,825 and 88,916 | 91,825 (37.5 s) and 88,916 (32.4 s) | yes |
| 24 Oct parse | 91,825 in and out, 1,329,304 skill mentions, 0 rejected | same | yes |
| 24 Oct dim_skill | 21,606 new tags | 21,606 | yes |
| 24 Oct SCD plan | inserted 91,825 | inserted 91,825 | yes |
| 24 Oct bridge | 1,329,304 rows | 1,329,304 | yes |
| 25 Oct parse | 88,916, 1,286,746 mentions | same | yes |
| 25 Oct dim_skill | 12 new tags | 12 | yes |
| 25 Oct SCD plan | inserted 992, closed 45, touched 87,924 | 992 / 45 / 87,924 | yes |
| 25 Oct bridge | 13,989 rows | 13,989 | yes |
| 25 Oct fact | 88,916 rows, 87,969 with a previous snapshot | 88,916 / 87,969 | yes |
| 25 Oct rerun | inserted 0, closed 0, touched 88,916, deleted 88,916 then inserted 88,916, bridge 0 | same | yes |
| 24 Oct rerun (older day) | out_of_order 45, next_snapshot_rows_recomputed 87,969 | 45 / 87,969 | yes |
| md5 fact_25 / dim_job / bridge | eca49cd2... / 94fdca62... / 6ef375f5... | eca49cd2de6697656e3208641ec9d77e / 94fdca629c2304dc6db54c14d8dec53d / 6ef375f55ea137adc86c184c96fac49c | yes, identical before and after both reruns |
| dim_job / current / dim_skill / bridge / fact / rejected | 92,817 / 92,772 / 21,618 / 1,343,293 / 180,741 / 0 | 92,817 / 92,772 / 21,618 / 1,343,293 / 180,741 / 0 | yes |
| DQ 24 Oct | 26 pass, 4 warn, 0 fail | 26 / 4 / 0 (experience_out_of_range = 2, plus three "no previous day" warnings) | yes |
| DQ 25 Oct | 29 pass, 1 warn, 0 fail | 29 / 1 / 0 (experience_out_of_range = 2) | yes |
| Skill match 25 Oct | 25.9% of mentions, 9.7% of tags, alias about 183k, exact about 149k, fuzzy about 1k, stem about 0.26k | 25.91% / 9.68%, alias 183,049, exact 149,248, fuzzy 905, stem 257, unmatched 953,287 | yes |
| Median views / applications | 77 / 2 both days | 77 / 2 both days | yes |
| Zero applications | 31.3% / 31.1% | 31.3% / 31.1% | yes |
| Reposted (repost_count 1+) | 20.5% / 20.6% | 20.5% / 20.6% | yes |
| Entry level asking 2+ years | 6.6% of 14,701 / 6.6% of 14,260 | 6.6% of 14,701 / 6.6% of 14,260 | yes |
| PMET share | 72.7% / 72.8% | 72.7% / 72.8% | yes |
| Skills per posting | 14.5 | 14.48 / 14.47 | yes |
| Deltas 25 Oct | 87,969 rows, 16.80 h, +212,357 views, +10,611 applications, 0 fell, 73,152 rose, 35 reposted, 11 edited | identical, and hours_since_prev is 16.80 on every row | yes |
| First seen / gone on 25 Oct | 947 / 3,856 | 947 / 3,856 | yes |
| Whole day timings | 53 s / 30 s / 28 s | 58 s / 34 s / 32 s (bridge 16.5 s instead of 14.2 s on day 1) | close |
| clean.fact_job_day size | 72 MB | **54 MB** | no, docs corrected |
| clean layer total (first load) | 370 MB | **353 MB** | no, docs corrected |
| Three week projection | 36 MB a day for the fact, about 1.1 GB clean | 27 MB a day, about 0.9 GB | docs corrected |
| Integration test | 2 s | 1 passed in 1.8 s, whole suite 46 passed with the DSN set | yes |

The three size numbers were the only mismatches. They were in the builder's favour (the real tables are
smaller), and most likely came from measuring after development reruns. Table sizes in the docs now say
54 MB, 353 MB and 0.9 GB.

## 2. Independent cross-check on the gz files (plain Python, not the builder's code)

Script read both files line by line and tracked its own attribute set, so this is a second opinion, not a
re-run.

| Measure | 24 Oct | 25 Oct | Warehouse agrees |
|---|---|---|---|
| Lines / distinct uuids / duplicate lines | 91,825 / 91,825 / 0 | 88,916 / 88,916 / 0 | yes |
| Payloads missing a counter | 0 | 0 | yes (0 rejected) |
| Median views / applications | 77 / 2 | 77 / 2 | yes |
| Zero applications | 31.3% | 31.1% | yes |
| Reposted 1+ / edited 1+ | 20.5% / 6.9% | 20.6% / 6.8% | yes |
| Skills per posting, skill mentions | 14.48, 1,329,304 | 14.47, 1,286,746 | yes |
| Distinct skill uuids, uuids with 2+ spellings | 21,606, 0 | 21,540, 0 | yes (dim_skill keyed on uuid is safe) |
| Entry level, asking 2+ years | 14,701, 970 (6.6%) | 14,260, 940 (6.6%) | yes |
| Experience above 40 / values above 30 | 2 / 32, 50, 88 | 2 / 32, 50, 88 | yes (2 flagged, 3 postings above 30) |
| Two position levels | 31 | 30 | yes |
| PMET share | 72.7% | 72.8% | yes |
| deletedAt set / of those reposted | 18,815 / 18,815 | 18,336 / 18,336 | confirms the report finding |
| applications above views | 0 | 0 | yes |

Across the two days: 87,969 postings present on both, 947 new, 3,856 gone. No view, application or repost
counter fell on any of the 87,969. 73,152 gained views, total +212,357 views and +10,611 applications. 35
reposted (counter rose or posting date moved), 11 edited. Exactly 45 postings changed any tracked attribute,
and the fields were: new posting date 35, expiry date 36, skills 5, title 5, SSOC 1, vacancies 1, salary 2.
That is the builder's list to the digit, from an independent attribute set.

## 3. Bugs found

| # | Severity | What | Fixed |
|---|---|---|---|
| 1 | **High** | `dags/daily_transform_dag.py` could not be imported by Airflow. It imports `transform.load_reference`, which did `import openpyxl` at module level, and the official `apache/airflow` image has no openpyxl. Loaded with `DagBag` inside `apache/airflow:3.0.1` (course image) and `apache/airflow:2.11.2` (nearest local image to the team's 2.10.5): `daily_mcf_ingest` loaded, `daily_mcf_transform` had an import error and would never appear in the UI. The builder's `ast.parse` test cannot see this. | Yes. openpyxl is now imported inside `_load_workbook()` with a clear message if missing, `docker-compose.yml` sets `_PIP_ADDITIONAL_REQUIREMENTS: openpyxl>=3.1 rapidfuzz>=3.0` (the image's entrypoint installs them at start, verified), and a new test asserts no module in the DAG's import chain imports openpyxl at top level. After the fix both DAGs load in the 3.0.1 container with the five tasks and the expected dependencies, schedule `30 10 * * *` Asia/Singapore, catchup false, max_active_runs 1, sensor in reschedule mode with a 10,800 s timeout. |
| 2 | Medium | rapidfuzz is also missing from the image, so inside Airflow the fuzzy tier would silently fall back to difflib, whose ratios differ. A tag first seen in the container could be mapped differently from the same tag on the laptop, and difflib is far slower on 21,606 tags. | Yes, by the same compose line. |
| 3 | Low | Three storage numbers in the docs were too high (72 MB fact, 370 MB total, 1.1 GB projection). | Yes, corrected to 54 MB, 353 MB, 0.9 GB. |
| 4 | Low | `metrics-design.md` signal 4.1 fails with `column reference "date_key" is ambiguous` (subquery joins `fact_job_day f2` to `dim_date d2` and selects bare `MAX(date_key)`). It also says 4 postings ask more than 30 years (there are 3) and uses `DATE :week_start`, which only works if the psql variable carries its own quotes. | Yes, in the metrics doc (not the schema). Added a note that `skill_week` keyed on `skill_key` shows two rows for two MCF tags mapped to the same SkillsFuture title. |
| 5 | Low | The fuzzy tier matches 13 tags (905 mentions, 0.07%) and several are doubtful: "eCommunications" to "Communication" (0.963), "System Integrators" to "Systems Integration" (0.914), "Contractor Management" to "Contract Management" (0.950, already listed as a known limit). | No. Either drop the tier or keep documenting it as low confidence. It changes nothing measurable. |
| 6 | Low | `fix_next_snapshot_deltas` (SQL path) computes `reposted_since_prev` as `x OR (a <> b)`, which is NULL when a posting date is NULL, while the Python path treats a NULL date as "not moved". No NULL posting dates exist in the data, so the two paths agree today (identical checksums prove it). | No. Would be `IS DISTINCT FROM` with a `COALESCE`. |
| 7 | Low | `log_run_stats` measures seconds from the DAG run start, so inside Airflow the `dag_run` row includes the sensor's waiting time. The CLI's `dag_run` row is transform time only. Fine, but the performance section should quote the step rows, not `dag_run`. | No. |
| 8 | Low | `is3107_test` scratch database is left on the server after the integration test. Harmless, dropped at cleanup. | No. |

No SQL injection: every user value goes through psycopg2 parameters. f-strings only interpolate constant
column lists and a `date` object formatted as digits (the named cursor). `.env` is git-ignored and no
password is printed.

## 4. Design against the lectures

**Star schema (L8).** Kimball's four steps are followed in order and the grain is declared first: one posting
on one snapshot day, which is the lecturer's "one row in a CSV", not an aggregate. `fact_job_day` is long
and narrow (19 columns, 180,741 rows) and `dim_job` is short and wide (40 columns). Facts are the numbers
that change every day (views, applications, repost and edit counters), which is exactly the lecturer's rule
"if it fluctuates it is a fact, if it stays fixed it is a dimension". Salary, title and skills sit in the
dimension because they change for about 1% of postings a day. Surrogate keys are plain integers
(`job_key BIGSERIAL`, `skill_key SERIAL`, `date_key` yyyymmdd) and the natural key `job_uuid` is kept
alongside. Company and SSOC title are denormalised into `dim_job`, the instructor-name point from the
workshop. Star over snowflake is argued the way the lecturer argued it (fewer joins, storage is not the
cost here). The skill many-to-many uses a bridge table, which is standard Kimball. Verified against the
loaded data: 0 fact rows point at a version not live that day, 0 postings have two current versions, 0
unknown SSOC codes, 0 NULL mom_group, 0 NULL salary or position level.

**Additive versus semi-additive.** Correct and written down in the schema comments: running totals are
semi-additive (never SUM across days), the stored deltas are additive. Storing the delta at load time is
justified (the previous snapshot is not always yesterday, the first interval was 16.8 h, and the "counters
never fall" check needs it).

**SCD Type 2.** Done right. `valid_from`, `valid_to` (last day seen, so no overlap), `is_current`, a new
surrogate key per version, and `attr_hash` as the change detector so counters never create a version. The
45 changed postings have their old version closed on 24 Oct and the 25 Oct fact rows point at the new
version (checked directly: 45 of 45). The planner is a pure function with 7 unit tests including the
idempotent rerun and the out-of-order case.

**DAG (L7, T8).** Five small tasks with explicit dependencies: `wait_for_raw` and `load_reference_tables`
start in parallel (the lecturer's "separate blocks"), then transform, then quality, then the run log, so a
failed quality check reruns only itself. Idempotent by delete-then-insert per `snapshot_date` and
`ON CONFLICT` upserts, which are the exact fixes from the 2 AM retry scenario, and proven here by identical
md5s after a same-day rerun and an older-day rerun. A missing day becomes one clearly failed run (sensor
timeout after 3 h, reschedule mode, so no worker slot is held) instead of a hang. Results go to tables
(`dq_results`, `load_log`, `rejected_rows`), the lecturer's "load results to a separate location, not
Airflow logs". Dates are labelled in Singapore time, the UTC versus SGT point. Bad rows go to a dead letter
table and odd values are flagged, not dropped. Style is TaskFlow (`@dag`, `@task`, `@task.sensor`), the
same as Tarsha's ingest DAG rather than the T8 notebook's `PythonOperator` style. Both are taught forms.
Imports work on both Airflow lines: `get_current_context` has a 3.x then 2.x fallback, and the DAG loaded
in the 3.0.1 and 2.11.2 containers after the fix. The 2.10.5 image was not pulled, but 2.11 and 2.10 share
the same API for everything used here.

**Over-engineered or missing for a 3 week student project.** The layer is bigger than most student
projects (about 1,500 lines, 30 quality checks, a 5 tier skill ladder, out-of-order reload handling), but
each piece maps to a lecture point and is tested, and the whole day runs in about 35 s. Two pieces cost
more than they give: the fuzzy tier (13 tags, several wrong) and the TSC alias import (280 aliases, no
measurement of how many tags they catch). Nothing essential is missing. What is still missing is one real
Airflow run on the collector machine for the Graph View screenshot, which needs the compose fix in this
branch (first start now pip-installs two packages, so it needs internet and about 20 s extra).

**Performance.** Measured on the loaded warehouse: "all postings on 25 Oct asking skill X" 47 ms (bitmap
index on the bridge), applications per skill over all 1.34 million bridge rows 219 ms (parallel hash
join), the transform's previous-snapshot lookup 84 ms. The bridge load is 16.5 s on day 1 and 0.5 s in
steady state because only changed versions are rebuilt. The 1.3 million row bridge is not a problem at this
scale. Partitioning the fact by `snapshot_date` is correctly deferred.

## 5. Metrics design SQL on this schema

Every block in `Admin/project-research/report/metrics-design.md` was executed against the loaded warehouse
inside one rolled-back transaction (so the warehouse was left untouched), with `week_start = 2026-10-19` and
`as_of = 2026-10-25`.

| Block | Runs | Result on two days |
|---|---|---|
| 3 `v_job_day7` | yes | 66,685 postings, all `observed_late` (no clean cohort can reach day 7 yet, as the doc says) |
| 3 `v_day7_thresholds` | yes | empty for the clean cohort. Over observed_late: views P75 170, applications P90 27 |
| 4.1 `skill_week` | **after fixing `MAX(date_key)` to `MAX(f2.date_key)`** | 11 skills with 30+ new postings (Collaboration 77, Communication 68, Marketing 57, ...). Two "Collaboration" rows (two MCF tags, one SSG title) |
| 4.2 `competitiveness_week` by category | yes | 42 categories. Apply rate flow: Engineering 3.4%, F&B 3.6%, Accounting 6.1%, IT 7.2%, Banking and Finance 8.5%, the pattern the doc expected |
| 4.3 `posting_flags` and barrier lift | yes (flags filled with the doc's prose rule to exercise it) | 2.0% barrier postings, 101 skills with support. Top lift: Forklift Operation 7.6, Cuts 5.5, Aircraft 5.2, Machining 4.4 |
| 4.4 shortage flag | yes | 7,680 of 92,772 postings (8.3%, doc expected 8 to 10%). No repost observed inside the window yet |
| 4.5B entry level by category | yes | Building and Construction 29.2% of 284, Engineering 22.2% of 1,389, Manufacturing 17.6% |
| 7 MOM comparison | yes (with `ref.mom_vacancy` stubbed from the doc's numbers) | PMET 72.7% of postings, 66.8% of vacancies, 56.2% of MOM, the doc's expected 72 to 73 / 67 / 56 |

Column contract: every column the metrics doc names exists with that name and type. Nothing in the
schema needed to change.

## 6. Changes made on this branch

1. `transform/load_reference.py`: openpyxl imported inside `_load_workbook()` instead of at module level.
2. `docker-compose.yml`: `_PIP_ADDITIONAL_REQUIREMENTS: openpyxl>=3.1 rapidfuzz>=3.0` in the shared Airflow env.
3. `tests/test_transform_dag.py`: new test `test_dag_import_chain_needs_no_packages_missing_from_the_airflow_image`.
4. `docs/role2-cleaning-and-warehouse.md`, `docs/VERIFY-ME.md`: size numbers 54 MB / 353 MB / 0.9 GB, test count 45.
5. `docs/VERIFICATION-26Oct.md`: this file.

Outside the repo (not committed, `Admin/` is not a git repository): `project-research/report/metrics-design.md`
got the `MAX(f2.date_key)` fix, 4 to 3 postings, the psql variable note and the duplicate display_skill note.

## 7. Go or no-go

**GO.** Open the PR with the fix commit. Before merging, one person should run the DAG once on the collector
machine (`docker compose up -d`, enable `daily_mcf_transform`, trigger with `{"snapshot_date": "2026-10-25"}`)
and take the Graph View screenshot, and tell Tarsha that `docker-compose.yml` now installs two packages at
container start.

## 8. How this was run and cleaned up

Fresh volume (`docker compose down -v`, then `docker compose up -d warehouse`), reference load 2 s, raw
replay 38 s and 33 s, transform 24 Oct 59 s, 25 Oct 34 s, 25 Oct rerun 32 s, 24 Oct rerun 34 s, md5 of
fact_25, dim_job, bridge, dim_skill and fact_24 identical at all three measurement points. Integration test
with `WAREHOUSE_TEST_DSN` set: passed. Cleanup: `is3107_test` dropped, `docker compose down` (volume kept,
so the loaded warehouse survives), `docker desktop stop`, `wsl --shutdown`. Commit memory headroom before
starting: 23.8 GB free of 49.6 GB.
