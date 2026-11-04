# Weekly metrics: what the tables mean and how to run them

One DAG, `weekly_metrics` (`dags/weekly_metrics_dag.py`), fills the `metrics` schema once a week. It reads only
`clean`, writes only `metrics`, and the dashboard reads only `metrics` (Dr Lek: results are loaded to a separate
location and read from there). It has two independent branches that share one week and one snapshot:

```
refresh_mom_stats -> compare_mcf_with_mom                       metrics.mcf_vs_mom_share  (sql/init/03_mom_vacancies.sql)

pick_snapshot -> build_posting_week -> 6 signal tables -> check_and_log
                                                                 metrics.run_log, posting_week, barrier_skill, shortage_skill,
                                                                 unrealistic_requirements, competitiveness, skill_demand,
                                                                 skill_salary            (sql/init/05_metrics_signals.sql)
```

The MOM branch is documented in the README ("Weekly metrics") and in `transform/mom_vacancies.py`. This file
covers the signal branch (code: `metrics/`). `metrics.mcf_vs_mom_share` is the MOM validation: the signals no
longer have a MOM table of their own.

## 1. Which week, which snapshot

**A run covers its Airflow data interval**, Tuesday 14:00 to the next Tuesday 14:00 Singapore time, so the
seven snapshot days Wednesday to Tuesday. `pick_snapshot` turns the interval into Singapore dates with
`transform/snapshot_week.py` exactly as `compare_mcf_with_mom` does, and uses the **latest snapshot of that
week**, normally the Tuesday itself (ingest 10:00, transform 10:30, DAG 14:00). If Tuesday's transform was late,
the latest earlier day of the week is used. A week with no snapshot at all fails the run with a message naming
the week, without retries, and writes nothing. So for any run the signals and the MOM comparison describe the
same week from the same snapshot.

**How the week is stored.** Every signal table is keyed by `week_end_date`, the Singapore date of the interval
end (the Tuesday), and carries `snapshot_date`, the snapshot used. `metrics.run_log` also records
`week_start_date` (the Wednesday), whether the snapshot was the Tuesday (`snapshot_is_week_end`) and, when it
was not, why (`fallback_reason`).

**Backfill.** `catchup=True` from 20 Oct, `max_active_runs=1`: unpausing the DAG creates one run per week since
the first snapshot (24 Oct), oldest first. Each takes its own week from its data interval, so a backfilled run
computes that week's numbers, never today's. To redo a week, clear its run: every table deletes and rewrites
that week's rows in one transaction, and other weeks are never touched.

**Which postings are open at the week end.** Every posting seen on the snapshot day. A posting the crawl
skipped that day is **not** treated as closed (the 2 Nov data check found 570 to 1,030 open postings a day
skipped on 27 to 29 Oct that came back one or two days later). It still counts as open if it was seen in the
2 snapshots before **and** in the 2 snapshots after, and its numbers come from its latest sighting on or before
the snapshot (`seen_on_snapshot = false`, `last_seen_date`). Only a later sighting proves a skip: of the 3,856
postings gone between 24 and 25 Oct, 182 still had an expiry date in the future, but the 2 Nov data check found
only 33 skipped open postings that day, so "expiry not passed" alone would have kept about 150 closed postings
open. On the Tuesday the run is scheduled, no later snapshot exists yet, so that week is provisional by the 35
to 55 postings a normal crawl skips. Clearing the run after Wednesday's snapshot lands makes it final;
backfilled weeks are final straight away.

**Per-day figures are per 24 hours.** Runs were 16.8 to 28.1 hours apart, and a skipped posting's next delta
spans about 48 hours. Every flow number is `24 * sum(delta) / sum(hours_since_prev)` over the week, never a raw
sum of daily deltas.

**Running totals are not summed.** `views_total` and `applications_total` are the API's running totals
(semi-additive), so each posting contributes its latest value once.

## 2. The tables

All tables are keyed by `week_end_date` first and carry `snapshot_date`. Skill tables use `skill_ref`: the
SkillsFuture skill id when the MCF tag is mapped, otherwise the MCF tag. Two tags mapped to the same
SkillsFuture skill (for example "Team Player" and "Teamwork", both Collaboration) are one row, and a posting
with both counts once. `skill_name` is the SkillsFuture title or the MCF tag. `is_mapped` says which.

| Table | One row per | What it answers | How to read it |
|---|---|---|---|
| `run_log` | week | Did the run work, from which snapshot? | `snapshot_date`, `fallback_reason`, `pool_postings`, `carried_forward`, `status` (`ok` or `failed_checks`), `steps` (rows and seconds per table), `checks` |
| `posting_week` | open posting at the week end | The base of every signal (a periodic snapshot fact) | Attributes of the posting version live on `last_seen_date`, its running totals, `apps_per_day_since_posting`, `salary_usable`, `mom_group`, and three flags: `is_shortage`, `is_reposted_many_apps`, `is_entry_level_2plus`. About 61 MB a week |
| `barrier_skill` | skill | Which skills attract views but put applicants off? | `apply_rate` = applications / views of postings with the skill. `rate_vs_overall` = that rate / the rate of all open postings (0.6 means 40% fewer applications per view). `views_vs_overall` the same for views per posting. `is_barrier` = views at or above average and `rate_vs_overall` at most 0.8. Only skills in 100+ postings from 10+ employers |
| `shortage_skill` | skill | Which skills sit in postings reposted again and again with few applicants? | A posting is a shortage posting when it was reposted twice (the highest count seen) and has at most 2 applications in total (totals carry over a repost, so this is the conservative side). `lift` = share of the skill's postings that are shortage postings / the same share over all open postings. Only skills with 25+ shortage postings from 10+ employers (on 25 Oct "Aircraft Systems" had 40 shortage postings from one employer). `median_repost_count` and `median_applications` describe the skill's shortage postings only. `is_casl` lets the report compare with SSG's own list of skills needed by growth jobs |
| `unrealistic_requirements` | flag and group | Which jobs are reposted despite many applicants? Which entry-level jobs ask for 2+ years? | Three flags. `reposted_many_apps`: reposted and at or above the week's P90 of applications. `entry_level_2plus_years`: "Fresh/entry level" asking 2 to 30 years (above 30 are typing errors). `junior_2plus_years`: the same for "Junior Executive", kept apart because the share is seven times higher. Groups: `overall`, `category`, `ssoc_major_group`, `position_level`. `flagged_share`, and the median salary of flagged against other postings. `rule` spells out the rule with this week's numbers |
| `competitiveness` | group | Which roles and skills get the most applicants per day? | Stock: `median_apps_per_day_since_posting` = applications so far / days since the original posting date, per open posting. Flow: `apps_per_posting_day_24h` = applications gained this week per posting per 24 hours, and `apply_rate_flow` = applications gained / views gained. Groups: `overall`, `mom_group` (the same Split A groups as `metrics.mcf_vs_mom_share`), `ssoc_major_group`, `ssoc` (occupation), `category`, `skill` |
| `skill_demand` | skill and posting week | Which skills are rising? | Postings per skill by the Monday of their original posting date (a calendar week, not the Wednesday to Tuesday run week), from every posting seen up to the snapshot (open or since closed), for the last 6 posting weeks. Compare weeks with `share_of_week` (the skill's share of that week's postings), not raw counts. Use only rows with `week_fully_observed = true` for trends: weeks before collection began only contain postings that were still open on 24 Oct |
| `skill_salary` | skill | What do postings asking for the skill pay? | Monthly salary as advertised, open postings with a usable salary only: median minimum, median maximum, P25, median and P75 of the midpoint, and `mid_vs_overall`. Usable means a minimum of at least $300 and a maximum of at most $100,000: below are hourly or daily rates typed into the monthly field (1,395 postings on 25 Oct, for example $12 to $24 "a month" for a packing job) and placeholders like $1, above are annual figures |

The MOM validation (does MCF look like the wider vacancy market?) is `metrics.mcf_vs_mom_share`, written by
`compare_mcf_with_mom` from the same snapshot: per MOM occupation group, MCF's share of postings against MOM's
share of vacancies in the latest quarter.

### Minimum sample sizes and thresholds

Stored with every run in `metrics.run_log.thresholds`. Change them in `metrics/build_week.py` (`THRESHOLDS`).

| Threshold | Value | Why |
|---|---|---|
| Postings carrying a skill (barrier, shortage, competitiveness by skill) | 100 | one employer's bulk postings cannot make a skill |
| Postings in a category, occupation or position level | 30 | a stable median |
| Postings with a salary, per skill | 30 | a stable median |
| Postings per skill across the demand window | 30 | keeps the table to skills with a real series |
| Shortage postings carrying a skill | 25 | from the metrics design |
| Employers behind a barrier skill, or behind a skill's shortage postings | 10 | before this rule the top four shortage skills on 25 Oct each came from a single employer; with 5, Metal Fabrication (57 shortage postings) and Product Management (85) on 3 Nov each came from 5 firms |
| Usable monthly salary | $300 to $100,000 | hourly or daily rates below, annual figures above |
| Barrier: rate below average | 0.8 of the average | at least 20% fewer applications per view |
| Shortage: reposts, applications | 2 reposts, at most 2 applications | 2 is the highest repost count seen on 25 Oct (6,163 postings). At most the median posting's 2 applications |
| Reposted with many applications | P90 of open postings that week | data-driven (23 on 25 Oct), recorded in the `rule` text |
| Entry level, junior | "Fresh/entry level", "Junior Executive", 2 to 30 years, two separate flags | 32, 50 and 88 years are typing errors |
| Crawl skip | up to 2 snapshots in a row | skipped postings came back within one or two days |

## 3. Deploy once on the collector laptop (where the warehouse volume lives)

The init scripts in `sql/init/` only run when the warehouse volume is first created, and the collector's
volume already exists, so new tables are added by hand once. Every file is idempotent (safe to run twice). Do
this outside 10:00 to 11:00 SGT so the restart in step 4 does not interrupt the daily ingest or transform.

1. If `weekly_metrics` is already unpaused, pause it in the UI.
2. `git pull` on `main`.
3. Apply the new SQL, in name order: `03_mom_vacancies.sql` (MOM tables, skip if already applied),
   `04_ext_schema.sql` (only once the extra-data PR is merged) and `05_metrics_signals.sql` (this branch):
   ```bash
   docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/05_metrics_signals.sql
   ```
   ```powershell
   Get-Content sql/init/05_metrics_signals.sql | docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
   ```
   Check: `docker compose exec warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\dt metrics.*"'`
   lists `mcf_vs_mom_share` and the 8 signal tables.
4. `docker compose up -d`. `docker-compose.yml` now mounts `./metrics` into the Airflow containers, and compose
   recreates the webserver and scheduler to pick it up. The warehouse and its data are untouched. Never
   `docker compose down -v`: `-v` deletes the warehouse volume with every snapshot in it.
5. `docker compose exec airflow-scheduler airflow dags list-import-errors` should print no error for
   `weekly_metrics_dag.py`.
6. Fill the weeks:
   - If `weekly_metrics` never ran: unpause it. The scheduler backfills one run per week since 20 Oct (weeks
     ending Tue 27 Oct, then Tue 3 Nov), each with the MOM comparison and the signals.
   - If it already ran (only the two MOM tasks then): unpause it, and in Grid view clear each finished run
     ("Clear", existing tasks). Clearing re-queues the run, Airflow adds the new signal tasks to it, and the
     MOM tasks simply rewrite their own rows. The same from the command line:
     `docker compose exec airflow-scheduler airflow tasks clear weekly_metrics -s 2026-10-20 -e 2026-10-28 -y`.
7. Check both branches used the same snapshot per week:
   `SELECT week_end_date, snapshot_date, status, pool_postings, carried_forward, seconds FROM metrics.run_log ORDER BY 1;`
   and `SELECT DISTINCT mcf_snapshot_date FROM metrics.mcf_vs_mom_share ORDER BY 1;`.

Any week by hand, without Airflow: `python -m metrics.run --week-ending 2026-11-03` (the 7 snapshot days ending
on that day, same rule as `python -m transform.mom_vacancies --week-ending`; needs `WAREHOUSE_DSN` or the `.env`
values). If the clean layer is ever rebuilt, rerun the weeks.

For the video: Graph View shows the two branches: the MOM download and comparison, and the snapshot pick,
`build_posting_week`, the fan-out of six tables and the fan-in to `check_and_log`.

## 4. Checks (in `metrics.run_log.checks`)

`fail` stops the run (the rows stay for inspection): no open postings, no overall competitiveness row, a share
outside 0 to 1, a negative rate. `warn` is recorded only: a signal table with no rows, more than 2% of open
postings carried over a crawl skip, a snapshot that is not the week's Tuesday, a snapshot that failed a daily
data-quality check. `info` puts the 24-hour application rate and the observed days per posting on record.

## 5. Validation on real data (24 and 25 Oct)

Run on 3 Nov 2026 on Brian's laptop in a throwaway Postgres 16 container, not the team's warehouse: the schema
applied to an empty database, reference files loaded, the 24 and 25 Oct files replayed with
`ingestion/load_snapshot_file.py`, both days transformed with `transform.run`, then the signals for the week
ending 25 Oct. The container was removed afterwards. The raw files never entered the repo. This ran before the
merge into Tarsha's DAG, with Monday weeks and a `mom_validation` table (since dropped). The signal SQL did not
change in the merge, and `python -m metrics.run --week-ending 2026-10-25` picks the same days (snapshot 25 Oct,
flow from 19 Oct), so the numbers below stand.

The clean layer came out exactly as verified before: 91,825 and 88,916 postings, 992 versions inserted and 45
closed on 25 Oct, data quality 26 pass and 4 warn, then 29 pass and 1 warn, no fail.

Week ending 25 Oct (snapshot 25 Oct):

| Table | Rows | Seconds |
|---|---|---|
| posting_week | 88,916 | 1.3 |
| barrier_skill | 2,344 | 3.8 |
| shortage_skill | 157 | 4.5 |
| unrealistic_requirements | 139 | 0.5 |
| competitiveness | 2,726 | 9.8 |
| skill_demand | 34,221 | 3.0 |
| skill_salary | 6,505 | 3.1 |
| **whole week** | | **26** |

Every check passed, with no warning. A second run deleted and reinserted every row, and the md5 of every table
was identical before and after.

Cross-checks with numbers verified independently before:

- Open postings 88,916, the whole 25 Oct file. None carried forward, as expected with no later day loaded.
- Entry level asking 2+ years: 939 of 14,259 (6.6%). The 26 Oct verification counted 940 of 14,260 for 25 Oct,
  the difference being the one entry-level posting asking more than 30 years.
- "Many applications" (P90) is 23, the pilot's estimate.
- PMET is 72.8% of open postings and 66.9% of their vacancies, the metrics design's 72 to 73 and 67.
- 10,611 applications gained by 87,969 postings in 16.8 hours is 0.1723 applications per posting per 24
  hours. The raw per-interval figure is 0.1206, so without the 24-hour normalisation the first day reads 30%
  low.

Example findings (one snapshot day, so first readings to confirm on the 27 Oct and 3 Nov weeks):

1. **Competitiveness differs ninefold between role groups.** Managers (SSOC major group 1) gain 0.30
   applications per posting per 24 hours, craftsmen and related trades (group 7) 0.03. Quantity surveyors
   have 1,276 open postings and gain 0.006 applications per posting per day, about one application per 175
   posting-days, against 0.79 for data scientists (409 postings).
2. **Experience asks are mild at entry level and common at junior level.** 6.6% of entry-level postings
   (939 of 14,259) ask 2+ years, rising to 29% in Building and Construction (80 of 273) and 22% in Engineering
   (296 of 1,319), and they do not pay more (median midpoint $3,350 against $3,400). At Junior Executive level
   46% ask 2+ years (4,278 of 9,297), and those postings pay a median $3,750 against $3,150.
3. **Shortage skills are technician and manufacturing skills.** 2.6% of open postings were reposted twice and
   still have at most 2 applications. Postings asking Metal Fabrication are 9.7 times as likely to be one of
   them (66 of 261, 6 employers), CNC 8.3 times (122 of 564, 9 employers), Machining 7.4 times (138 of 719,
   13 employers). None of the top 20 is on SSG's CASL list (6 of all 157 are), so this signal does not simply
   echo SSG's growth list.

Barrier skills on this day are low-paid manual and service tags: Packaging is in 1,001 open postings from 458
employers, draws average views, and gets 3.3 applications per 100 views against 7.2 overall
(`rate_vs_overall` 0.45).

## 6. Limitations

- Two weeks of data by the report freeze: week-on-week change exists for one pair of weeks, so a trend is two
  points.
- Views, applications and repost counts carry over a repost, and reposts before 24 Oct cannot be dated. The
  shortage rule uses totals, which is the conservative side.
- Applications per day since posting falls with posting age (most applications come early), so older
  occupations look less competitive. The 24-hour flow numbers are the fairer comparison within one week.
- MCF applications are only applications through MCF, and views are page views, not people.
- Only about a quarter of skill mentions map to a SkillsFuture skill. Unmapped tags are kept under their MCF
  name, so the skill tables mix both kinds (`is_mapped`).
- Every signal is an indication of hiring difficulty, not proof. There is no hiring outcome in the data.

## 7. Tests

`tests/test_metrics_weeks.py` (week dates from the data interval, the crawl-skip window, a snapshot outside the
week refused), `tests/test_metrics_sql.py` (every builder writes only its own table for one week, delete before
insert, every table keyed by week, no stray `%` for psycopg2, 05 runs after 03 and redefines none of its tables,
the snapshot picked through `snapshot_week` like the MOM comparison, a week without snapshot fails before any
write), `tests/test_weekly_metrics_dag.py` (Tarsha's MOM checks plus the signal tasks, the week taken from the
data interval, the compose mount), and `tests/test_integration_metrics.py` (Postgres, skipped without
`WAREHOUSE_TEST_DSN`): every `sql/init` file applied twice, a 7-posting, 3-day fixture through the real transform,
checked against numbers worked out by hand, including a crawl skip, the 24-hour normalisation over 16.8, 24 and
40.8 hour intervals, the entry and junior flags, an hourly rate typed as a monthly salary, the first scheduled
week falling back to Monday, the MOM comparison of that week using the same snapshot and the same occupation
counts, a week without snapshot writing nothing, and a rerun that changes nothing.
