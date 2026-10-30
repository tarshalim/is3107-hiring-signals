# Role 2: cleaning and warehouse (design, reasons, measured numbers)

Written for the report's "Data Ingestion and Cleaning", "Data Modeling and Storage" and "Performance" sections.
Plain English, one paragraph per decision, each with the reason and the lecture it comes from. Everything was
measured on the real 24 and 25 Oct snapshots (91,825 and 88,916 postings) on a laptop. Reproduction steps:
`docs/VERIFY-ME.md`. Diagram: `docs/er-diagram.md`.

## 1. What this layer does

```
raw.mcf_job_snapshots (JSONB, one row per posting per day, 490 MB a day)
        |  transform/transform_snapshot.py   (one day at a time, one transaction)
        v
clean.dim_job (SCD2)   clean.dim_skill  <- bridge_job_skill   clean.dim_date
                 \            |                                /
                  \           v                               /
                   ------ clean.fact_job_day (posting x day) -
                              |
            quality/clean_checks.py -> clean.dq_results, clean.load_log, clean.rejected_rows
```

The ingest DAG (Tarsha) lands the API answer untouched in `raw`. This layer turns it into a star schema the
dashboard (CH) can query with plain GROUP BY. The lecturer said he grades this middle part, so every rule is
written down and tested.

## 2. Design decisions and why

### 2.1 Why a star schema, and what the grain is (L8, Kimball's four steps)

1. Business process: a posting is open on MyCareersFuture and job seekers view it and apply.
2. Grain: **one posting on one snapshot day**. Declared first, before dimensions or facts, as the lecture insisted.
   Think "one row in a CSV": posting X on 25 Oct had 130 views and 6 applications.
3. Dimensions: `dim_job` (what was posted, by whom, for which SSOC occupation), `dim_date` (when),
   `dim_skill` through `bridge_job_skill` (which skills, 3 to 20 per posting).
4. Facts: `views_total`, `applications_total`, `repost_count`, `edit_count`, `views_delta`, `applications_delta`.

Why star and not snowflake: the dashboard's questions are "applications per skill per week", "which occupations
are reposted most", "do entry-level postings ask for 2+ years". Those are GROUP BY questions on a few numeric
columns, so a fact table with foreign keys to a few wide dimensions needs few joins. Company and SSOC title are
copied into `dim_job` on purpose (denormalised), exactly like the instructor name going straight into the course
dimension in the workshop. Storage is tiny (the whole clean layer is about 350 MB for two days), so the snowflake's only
benefit, less redundancy, buys nothing here. Why not one flat table: the lecturer's own answer, a database with
B-tree indexes finds "all postings asking for skill X" without scanning everything, and the dimension rows stay
one per posting version instead of being repeated 14.5 times per skill.

### 2.2 Facts: additive or not (L8)

`views_total` and `applications_total` are the API's running totals. They are **semi-additive**: adding Monday's and
Tuesday's views of the same posting double counts. The dashboard must take the latest value or the maximum. The
stored increases `views_delta` and `applications_delta` are **additive**: SUM over postings and days is meaningful,
and that is what the competitiveness signal needs (applications per day per skill). Storing the delta instead of
computing it on the fly was a deliberate choice: the previous snapshot of a posting is not always yesterday (about
50 postings a run are missed while paging, and the first interval was 17 hours, not 24), so the dashboard would
need a window function over the whole fact table every time. Computing it once at load also lets the quality
check "counters never fall" run on stored numbers. `hours_since_prev` is stored with it so rates can be
expressed per 24 hours (on 25 Oct every row has 16.80 hours).

### 2.3 dim_job is a slowly changing dimension, Type 2 (L8 extra notes)

Employers edit postings (6.9% have `editCount` of 1 or more) and repost them (20.5%), which changes salary, title,
skills, posting and expiry dates. A dashboard asking "did applications pick up after the salary was raised" needs
the attributes **as they were on each day**. Type 1 (overwrite) would silently rewrite history. Type 2 adds a new
row with a new surrogate key and closes the old one (`valid_from`, `valid_to`, `is_current`), and each fact row
points at the version that was live that day. The change detector is `attr_hash`, a SHA-1 over every tracked
attribute plus the skill list, so the daily counters (which belong in the fact) never create a version.

Measured on the real data: 24 Oct created 91,825 versions. 25 Oct created 992 (947 brand-new postings and 45
changed ones) and closed 45. Of the 45 changes, 35 were reposts (new posting date moved), 36 changed the expiry
date, 5 the title, 5 the skills, 2 the salary, 1 the vacancies and 1 the SSOC code. So the history is small
(about 1% of postings change a day) and cheap to keep, and it is exactly the history the repost signal needs.

### 2.4 The skill bridge is keyed on the version, not the posting

Skills can be edited (5 of the 45 changes above), so `bridge_job_skill` hangs off `job_key` (the version). The
metrics SQL joins `fact.job_key = bridge.job_key`, which automatically picks the skill list that was live that
day. `dim_job` also carries `job_uuid`, the natural key, for day-over-day self joins that must survive a version
change (the delta of a reposted posting is still measured against its previous snapshot).

### 2.5 Cleaning rules (L7: schema, range, business rules. Flag, do not drop)

| Rule | Why |
|---|---|
| Strings trimmed, inner whitespace collapsed | 85 titles a day have stray spaces, and a whitespace-only edit must not create a new version |
| SSOC code kept as 5-digit TEXT | '01234' is a code, not a number, and a leading zero would vanish as an integer |
| Salary to NUMERIC, missing to NULL | none missing so far, but the API allows it |
| Years of experience outside 0..40: capped at 40, `exp_out_of_range = true`, raw value kept | one posting asks for 88 years and one for 50. Dropping them would hide the "unrealistic requirement" story, and capping without a flag would hide the error. The dashboard can exclude flagged rows |
| Dates to DATE, unreadable to NULL | counted by the dates_inconsistent check |
| Position level: first level kept as `position_level`, all levels kept as `position_levels` | 31 postings have two levels |
| MOM group derived from the SSOC first digit | lets the dashboard compare with MOM's three vacancy groups |
| Payload with no uuid or no counters: rejected to `clean.rejected_rows` with a reason | the "error table / dead letter queue" from L7, so one bad record never stops the day. 0 on both real days, 1 in the integration test |

### 2.6 Skill mapping v1 (why it is a ladder, and the honest match rate)

MCF tags are free text from employers: 21,606 distinct tags on 24 Oct, "Team Player", "Microsoft Excel",
"Able To Work Independently". The SkillsFuture Unique Skills List has 2,388 official skills in sector wording.
Only 8.9% of tags (11.6% of mentions) match by name. The mapping is a ladder, and every row records the
method and a confidence so the dashboard can filter:

1. **exact** on the normalised name (lower case, no punctuation, parentheticals removed), confidence 1.0
2. **alias**: 154 curated aliases in `data/reference/skill_aliases.csv` (mostly generic tags mapped to the 16
   Critical Core Skills, each with its own confidence written by hand) plus 280 sector titles and previous titles
   from the TSC-to-USL mapping file (0.9)
3. **stem**: equal after plural stripping and stopword removal, word order kept ("Development Tools" and "Tools
   Development" are different skills), 0.95
4. **fuzzy**: rapidfuzz ratio of at least 90 on the whole key AND each word in the same position at ratio 91 or
   with an 8-letter shared prefix. The word guard is what makes it safe: a plain ratio gave "Cargo Warehouse
   Operations" to "Cargo Warehousing Operations" (right) and "Production Management" to "Product Management"
   (wrong) the same score of 92. The first full run still let four wrong pairs through at a word bar of 85
   (Quantitative to Qualitative, Attrition to Attribution, Curation to Creation, Customs to Customer), so the bar
   was raised to 91 and the tags re-mapped in place (`load_reference --remap`, 9 rows changed)
5. **unmatched**: kept in `dim_skill` so nothing is lost and the gap is reported

Measured on 25 Oct: **25.9% of skill mentions** and **9.7% of distinct tags** are mapped (exact 149k mentions,
alias 183k, fuzzy 1k, stem 0.3k, unmatched 953k). The curated aliases more than doubled the coverage from the
11.6% that exact matching gives. The remaining long tail is semantic, not spelling: "Construction",
"Housekeeping", "Scheduling", "Electrical", "Sales" (the top unmatched tags, 4,000 to 7,000 mentions each) have no
single USL skill. Precision was preferred over recall because a wrong label silently corrupts the barrier-skills
signal, while an unmatched tag only shrinks the sample. v2 would add an embedding or LLM pass on the top 500
unmatched tags, with human review.

### 2.7 Why Postgres for this layer and not MongoDB (L3, L4)

The raw layer already keeps the nested JSON as JSONB, which is the document-store use case. The clean layer is the
opposite: fixed schema, numeric measures, joins and aggregates, foreign keys that must hold (every fact row points
at a real version). That is what a relational store with B-tree indexes does best, and the lecturer's own reason
for NoSQL, "too much data for one machine", does not apply at 100k rows a day. One Postgres container also keeps
the team's stack small enough for the 6 GB Docker cap on the collector laptop.

### 2.8 Why plain Python and SQL, not Spark (L5, T6, L9)

One day is about 90,000 postings, about 700 MB of JSON. A server-side cursor streams it in batches of 2,000, so
memory stays flat and the parse takes 24 seconds. Spark would add a cluster, a JVM and a connector for a job one
process finishes in under a minute. The heavy set operations (bridge load, SSOC enrichment, next-day delta
recompute) already run inside Postgres, ELT style, using the warehouse's own compute. Spark stays the scale-out
path and the code is shaped for it: the parse step is a pure function per payload (a map), the SCD and delta steps
are joins on `job_uuid` (a reduce by key). The fact table would be range-partitioned by `snapshot_date` (even,
daily partitions, as the lecture on partitioning asked) once it held months rather than weeks.

### 2.9 How it stays idempotent (L7, the 2 AM retry scenario)

- The whole day is **one transaction**: a crash leaves nothing half-loaded ("not here, not there").
- `fact_job_day` and `rejected_rows` are **delete-then-insert per snapshot_date**.
- `dim_job` versions are keyed on `(job_uuid, valid_from)`: a rerun with the same data only moves
  `last_seen_date` (planner says "touch"), a rerun with changed data updates that day's version in place.
- `dim_skill`, `dim_date` and the reference tables use `INSERT ... ON CONFLICT`.
- The bridge is rebuilt only for versions inserted or changed in this run.
- Reloading an **older** day recomputes the next snapshot's deltas against it, and leaves versions that started
  later alone (planner says "out of order").

Proof: the rerun of 25 Oct reported inserted 0, closed 0, touched 88,916, deleted 88,916 then inserted 88,916, and
the md5 of the fact, dimension and bridge tables was identical before and after. The rerun of 24 Oct (an older
day) reported out_of_order 45 and recomputed 87,969 next-day rows, again with identical checksums. The same is
tested on a 4-posting fixture in `tests/test_integration_warehouse.py` and in pure Python in `tests/test_scd.py`.

### 2.10 How a missing day is handled (L7: the pipeline must not break because a source is down)

The transform DAG starts with a sensor on `raw.ingest_runs` for the day's row (the ingest DAG writes it last). It
pokes every 5 minutes in `reschedule` mode (no worker slot held) and gives up after 3 hours, so a day the collector
never delivered becomes one clearly failed run with a timeout message, not a hang and not a half-built day. The
next day's run is independent and the delta logic measures against the latest earlier snapshot, so one missing day
only widens `hours_since_prev` to about 48. A day that exists in raw but was never transformed is reloaded by
triggering the DAG with `{"snapshot_date": "YYYY-MM-DD"}`, or from the shell with `python -m transform.run`.

Why a sensor rather than an Airflow Dataset/Asset dependency: the contract is "the day's rows are in the
warehouse", whoever put them there, the ingest DAG or a teammate replaying a saved gz file (the mock replay the
lecturer said is fine, and how this layer was built without touching the live API). It also needs no change to
the collector's DAG and runs unchanged on Airflow 2.10 (team image) and 3.0 (course notebook).

### 2.11 Data quality: what fails the run and what only warns

30 checks in `quality/clean_checks.py`, each one SQL statement returning one number, tagged schema, range,
business rule or info, with a severity. `fail` stops the DAG (the data stays loaded so the problem can be seen).
`warn` is recorded only. `info` puts a number on the record every day.

| Fails the run | Why it is fatal |
|---|---|
| raw rows under 100, fact + rejected rows not equal to raw rows | the day is incomplete or the parse lost rows |
| more than one current version per posting, overlapping version ranges, fact row pointing at a version not live that day | the SCD2 history would be wrong and every downstream number with it |
| negative counters | impossible values |
| more than 1% of postings with out-of-range experience | the field changed meaning |
| more than 1% of view or application counters falling | counters were reset or the days are mislabelled |
| crawl coverage under 90% (from `raw.ingest_runs`) | the collector's own threshold, repeated here |
| more than half of yesterday's postings missing today | the crawl was cut short (normal churn is 4%) |

Warnings: any counter falling at all, applications above views, salary min above max, inconsistent dates,
missing core attributes, out-of-range experience (expected: 2 postings), versions without skills, rejected rows,
and the snapshot label not matching the crawl's Singapore date (L7: UTC vs SGT. A replayed file loaded later
would only warn).

Results on the real data: 24 Oct 26 pass, 4 warn (the 2 flagged postings, and three checks that need a previous
day). 25 Oct 29 pass, 1 warn (the 2 flagged postings). No counter fell between the two days, no application
count exceeded its views, and every raw row became exactly one fact row.

## 3. Measured numbers (laptop, Postgres 16 in Docker, 6 GB cap)

### Rows and sizes after two days

| Table | Rows | Size (pg_total_relation_size, first load) |
|---|---|---|
| raw.mcf_job_snapshots | 180,741 | 947 MB (JSONB in TOAST) |
| clean.dim_job | 92,817 versions (92,772 current) | 166 MB |
| clean.bridge_job_skill | 1,343,293 | 120 MB |
| clean.fact_job_day | 180,741 | 54 MB |
| clean.dim_skill | 21,618 | 10 MB |
| reference, log, dq, rejected | 4,000 odd | under 2 MB |
| **clean layer total** | | **353 MB** |

Projection for three weeks (21 days): fact grows 27 MB a day, dim_job about 2 MB a day (1,000 new versions),
bridge about 1.3 MB a day, so about **0.9 GB clean** next to about 10 GB raw. The raw JSON is the cost. The
clean layer is a tenth of it, which is the point of keeping full JSON only in raw. (Reruns leave dead tuples that
autovacuum reuses. After four reruns and a VACUUM the clean total showed 503 MB, which a VACUUM FULL or a
partition truncate would give back.)

### Step timings (stored in clean.load_log)

| Step | 24 Oct (first day) | 25 Oct | 25 Oct rerun |
|---|---|---|---|
| parse_raw (stream, parse, clean, stage skills) | 23.9 s | 23.1 s | 23.5 s |
| upsert_dim_skill (match new tags) | 4.1 s (21,606 tags) | 0.5 s (12 tags) | 0.3 s |
| upsert_dim_job (SCD2 plan and apply) | 8.0 s (91,825 inserts) | 2.6 s (992 inserts, 45 closes, 87,924 touches) | 0.5 s |
| load_bridge_job_skill | 14.2 s (1,329,304 rows) | 0.5 s (13,989 rows) | 0.0 s |
| load_fact_job_day (deltas, delete-then-insert) | 2.8 s | 3.5 s | 3.4 s |
| **whole day** | **53 s** | **30 s** | **28 s** |
| raw replay from gz (ingestion/load_snapshot_file.py) | 31.6 s | 30.5 s | |

Steady state is about 30 seconds a day, dominated by parsing 700 MB of JSON in Python. The warehouse work is
under 7 seconds. Reference load: 1.8 s. Offline test suite: 44 tests in 4 seconds. Postgres integration test: 2 s.

### Sanity numbers from the clean layer (match the raw-data check of 25 Oct)

| | 24 Oct | 25 Oct |
|---|---|---|
| Postings | 91,825 | 88,916 |
| Median views / applications | 77 / 2 | 77 / 2 |
| Zero applications | 31.3% | 31.1% |
| Reposted (repost_count 1+) | 20.5% | 20.6% |
| Entry level asking 2+ years | 6.6% of 14,701 | 6.6% of 14,260 |
| PMET share | 72.7% | 72.8% |
| Skills per posting | 14.5 | 14.5 |
| Postings with a previous snapshot | | 87,969 (16.8 h earlier) |
| Views / applications gained in those 16.8 h | | +212,357 / +10,611 (0 fell) |
| Reposted / edited since previous snapshot | | 35 / 11 |
| First seen on 25 Oct / gone by 25 Oct | | 947 / 3,856 |

One finding for the report: `metadata.deletedAt` is set on exactly the 18,815 reposted postings and on none of
the others, so it marks the end of the previous posting period, not a deleted posting.

## 4. Limitations and next steps

- Skill coverage is 26% of mentions. The gap is semantic. Next: embeddings or an LLM pass on the top 500 unmatched
  tags with human review, and growing `skill_aliases.csv` (the `--remap` command re-labels old rows in place).
- `position_level` keeps the first level only for the 31 postings with two levels. `position_levels` has both.
- An older day reloaded after later days: versions that started later are left alone (reported as out of order).
  Reloading many old days should be done in date order.
- The Airflow DAG was parsed and its functions were exercised through the CLI and tests, but it was not run inside
  a container on this laptop (Airflow is not installed locally and the full stack does not fit the 6 GB Docker cap
  beside a loaded warehouse). It needs one run on the collector machine for the Graph View screenshot.
- The fact table is not partitioned. At months of data it should be range-partitioned by `snapshot_date`.
- Fuzzy matching still accepts near-synonyms such as "Contractor Management" to "Contract Management". They carry
  a confidence below 1 and can be excluded.

## 5. Column contract with the metrics layer

The names follow the metrics design (`Admin/project-research/report/metrics-design.md`, section 1) so its SQL works
unchanged: `fact_job_day(date_key, job_key, job_uuid, views_total, applications_total, views_delta,
applications_delta, hours_since_prev, repost_count, edit_count, status, days_since_new_posting,
days_since_original_posting, is_first_snapshot, reposted_since_prev)`, `dim_date(date_key, date, week_start,
is_weekend)`, `dim_job(job_key, job_uuid, title, position_level, min_years_exp, salary_min, salary_max,
salary_mid, vacancies, category, ssoc_code, mom_group, company_uen, company_name, is_posted_on_behalf,
original_posting_date, new_posting_date, valid_from, valid_to, is_current)`, `dim_skill(skill_key,
mcf_skill_name, ssg_skill_title, is_casl, is_emerging, match_method)`, `bridge_job_skill(job_key, skill_key,
is_key_skill)` re-inserted per SCD2 version. Two extras beyond the contract: `fact_job_day.snapshot_date` (the
delete-then-insert key, also handy for filters) and `fact_job_day.edited_since_prev`. `mom_group` values are
`PMET`, `clerical_sales_services`, `production_transport`.
