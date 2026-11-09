# Weekly metrics: what the tables mean and how to run them

One DAG, `weekly_metrics` (`dags/weekly_metrics_dag.py`), fills the `metrics` schema once a week. It reads only
`clean` and writes only `metrics` (Dr Lek: results are loaded to a separate location and read from there). The
Streamlit dashboard's three weekly tabs (Hiring signals, Skill lookup, Market check) read these tables, through
the views `metrics.signal_weeks` and `metrics.skill_signals` (`sql/init/10_dashboard_signals.sql`) for the week
picked in the sidebar, and Market check reads `metrics.mcf_vs_mom_share`. The daily tabs read their own
precomputed layer: the `mart` materialized views (`sql/init/07_dashboard_marts.sql`, rebuilt by the daily
transform), the salary estimator's stored model in `ml`, and for Pipeline health the run logs (`raw.ingest_runs`,
`clean.load_log`, `clean.dq_results`, `ops.alert_log`). The one link back is Trending skills, which takes the
generic and soft skill tag lists from the latest finished `metrics.run_log` so it drops the same tags. The DAG has
two independent branches that share one week and one snapshot:

```
refresh_mom_stats -> compare_mcf_with_mom                       metrics.mcf_vs_mom_share  (sql/init/03_mom_vacancies.sql)

pick_snapshot -> build_posting_week -> 7 signal tables -> check_and_log
                                                                 metrics.run_log, posting_week, barrier_skill, shortage_skill,
                                                                 unrealistic_requirements, competitiveness, skill_demand,
                                                                 skill_salary, group_signals (sql/init/05_metrics_signals.sql)
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
MCF tags too ambiguous to read as a skill are left out of every skill table (`barrier_skill`, `shortage_skill`,
`competitiveness` by skill, `skill_demand`, `skill_salary`; since 8 Nov): the explicit list, each tag with its
kind and a one-line reason, is `data/reference/generic_skill_tags.csv` (for example "Cuts": hair cuts, meat cuts
or cost cuts). Kinds: `ambiguous` (several meanings), `fragment` (part of a longer skill), `sector` (an industry
name, which the occupation already gives). The postings themselves and their flags stay; the list is stored with
every run in `run_log.thresholds.generic_skill_tags`. To change it, edit the CSV and rerun the weeks.

| Table | One row per | What it answers | How to read it |
|---|---|---|---|
| `run_log` | week | Did the run work, from which snapshot? | `snapshot_date`, `fallback_reason`, `pool_postings`, `carried_forward`, `status` (`ok` or `failed_checks`), `steps` (rows and seconds per table), `checks` |
| `posting_week` | open posting at the week end | The base of every signal (a periodic snapshot fact) | Attributes of the posting version live on `last_seen_date`, its running totals, `apps_per_day_since_posting`, `views_per_day_since_posting`, `salary_usable`, `mom_group`, and five flags: `is_shortage`, `is_reposted_many_apps`, `is_barrier_posting`, `is_entry_2plus` ("Fresh/entry level" asking 2+ years) and `is_junior_2plus` ("Junior Executive" asking 2+ years; one column for both until 4 Nov). `posted_by_agency`: the poster's SSIC 2020 code starts with 78 (employment agencies, 43% of open postings on 3 Nov). About 61 MB a week |
| `barrier_skill` | skill | Which skills sit in postings people look at a lot and then do not apply to, compared with similar jobs? | A posting is a barrier posting (`posting_week.is_barrier_posting`) when its views per day since first posted are at or above the P75 of its comparison group (`posting_week.barrier_group`), and its applications per view are at most 0.8 times the group's. The comparison group is the posting's 2-digit SSOC group (for example 24, business and administration professionals) when that group has 200+ open postings that week, else its 1-digit major group (on 3 Nov: 31 two-digit groups, 6 fallbacks, 575 postings on a fallback; 27 Oct: 628). `barrier_share` = the skill's barrier postings / its postings. `expected_share` = the share if each of those postings behaved like the average posting of its own comparison group. `lift` = `barrier_share` / `expected_share`. `is_barrier` = lift 1.5 or more, with 25+ barrier postings from 10+ employers, and `top_company_share` (the share of those barrier postings from the skill's single biggest employer) at most 0.5, and `rate_vs_overall` below 1: the skill's postings get fewer applications per view than all open postings, not only than similar jobs (since 7 Nov; before, 9 of the top 15 were accounting skills such as General Ledger at 1.43 times the market's applications per view, low only next to clerical jobs, the most applied-to group). Per day so old postings are not favoured, within the group so an occupation's own pattern (cleaners and drivers get many views and few applications whatever the skill) is not read as a skill barrier. Until 4 Nov this used raw views per posting against all postings. `apply_rate` and `rate_vs_overall` stay as descriptive columns. Only skills in 100+ postings from 10+ employers |
| `shortage_skill` | skill | Which skills sit in postings reposted again and again with few applicants? | A posting is a shortage posting when it was reposted twice (the highest count seen) and has at most 2 applications in total (totals carry over a repost, so this is the conservative side). **Direct employers only** (since 7 Nov): postings by employment agencies (`posted_by_agency`) are left out of every count, because agencies keep evergreen ads up and repost them; on 3 Nov they were 43% of open postings but 60% of shortage postings (3.5% of theirs, 1,348 of 38,171, against 1.8%), and one agency alone made a third of all shortage postings. `lift` = share of the skill's postings that are shortage postings / the same share over all open postings by direct employers. Only skills with 25+ shortage postings from 10+ employers (on 25 Oct "Aircraft Systems" had 40 shortage postings from one employer), and no single employer behind more than half of them (`top_company_share` at most 0.5). `median_repost_count` and `median_applications` describe the skill's shortage postings only. `is_casl` lets the report compare with SSG's own list of skills needed by growth jobs |
| `unrealistic_requirements` | flag and group | Which entry-level jobs ask for 2+ years? Which jobs are reposted despite many applicants? | Three flags, read in this order. `entry_level_2plus_years` (the lead finding): "Fresh/entry level" asking 2 to 30 years (above 30 are typing errors); on 3 Nov 6.3% (914 of 14,611), with no sign of a pay premium (median $3,445 against $3,400, +1%, but an
employer-clustered 95% interval of −15% to +17%, so the data cannot tell +1% from ±15%), which makes it the candidate
unrealistic ask. One agency (RN Care, SSIC 78104) posts 299 of the 914 (33%); without it the share is 4.4%, and for
direct employers only it is 5.0% (479 of 9,598). Agencies are not left out of this flag. `junior_2plus_years`: the same for "Junior Executive", kept apart because the share is seven times higher (47% on 3 Nov), but these pay 21% more than other Junior Executive postings, so they read more as mid-level roles labelled junior than as an unrealistic ask. `reposted_many_apps` (a minor signal): reposted and at or above the week's P90 of applications per day since first posted (per day, so a posting is not flagged just for being old: until 4 Nov this was the P90 of total applications, 23, which mostly picked postings open 50 to 94 days); **direct employers only** since 8 Nov, as for shortage (its top posters were agencies), while the P90 is still over all open postings. Groups: `overall`, `category`, `ssoc_major_group`, `position_level`. `flagged_share`, and the median salary of flagged against other postings. `rule` spells out the rule with this week's numbers |
| `competitiveness` | group | Which roles and skills get the most applicants per day? | Stock: `median_apps_per_day_since_posting` = applications so far / days since the original posting date, per open posting. Flow: `apps_per_posting_day_24h` = applications gained this week per posting per 24 hours, and `apply_rate_flow` = applications gained / views gained. Groups: `overall`, `mom_group` (the same Split A groups as `metrics.mcf_vs_mom_share`), `ssoc_major_group`, `ssoc` (occupation), `category`, `skill` |
| `skill_demand` | skill and posting week | Which skills are rising? | Postings per skill by the Monday of their original posting date (a calendar week, not the Wednesday to Tuesday run week), from every posting seen up to the snapshot (open or since closed), for the last 6 posting weeks. Compare weeks with `share_of_week` (the skill's share of that week's postings), not raw counts. `skill_type` is the SkillsFuture type, `tsc` (technical skill) or `ccs` (critical core skill), so technical skills can be shown apart; it is empty for MCF tags not mapped to SkillsFuture. Use only rows with `week_fully_observed = true` for trends: weeks before collection began only contain postings that were still open on 24 Oct |
| `group_signals` | group | Which cause shows up where? | Per job category and SSOC major group (and `overall`): `shortage_share` (of direct employers' postings), `barrier_share`, `entry_2plus_share` (of entry-level postings with a usable experience value), `agency_share` and the median salary, each from the same `posting_week` flags as the skill tables. Applications per day for the same groups are in `competitiveness` (same `group_type` and `group_key`). Groups below 30 postings are left out, and a share whose base is below 30 is NULL. Feeds the summary grid of the Hiring signals tab |
| `skill_salary` | skill | What do postings asking for the skill pay? | Monthly salary as advertised, open postings with a usable salary only, **employer-weighted** (since 8 Nov): each employer's median first, then the median across employers, so a recruiter with 54 ads counts once. Median minimum, median maximum, P25, median and P75 of the midpoint, all across employers, and `mid_vs_overall` against the market median weighted the same way. `postings` and `companies` (employers) give the sample. Only skills with 100+ such postings from 10+ employers, none above half of them. Usable means a minimum of at least $300 and a maximum of at most $100,000: below are hourly or daily rates typed into the monthly field (1,395 postings on 25 Oct, for example $12 to $24 "a month" for a packing job) and placeholders like $1, above are annual figures |

The MOM validation (does MCF look like the wider vacancy market?) is `metrics.mcf_vs_mom_share`, written by
`compare_mcf_with_mom` from the same snapshot: per MOM occupation group, MCF's share of postings against MOM's
share of vacancies in the latest quarter.

### Minimum sample sizes and thresholds

Stored with every run in `metrics.run_log.thresholds`. Change them in `metrics/build_week.py` (`THRESHOLDS`).

| Threshold | Value | Why |
|---|---|---|
| Postings carrying a skill (barrier, shortage, competitiveness by skill) | 100 | one employer's bulk postings cannot make a skill |
| Postings in a category, occupation or position level | 30 | a stable median |
| Postings with a usable salary, per skill (`skill_salary`) | 100 | a stable median: with 30 the top medians on 3 Nov came from 32 to 34 postings |
| Postings per skill across the demand window | 30 | keeps the table to skills with a real series |
| Shortage postings carrying a skill | 25 | from the metrics design |
| Employers behind a barrier skill, a skill's shortage postings, or its salaried postings (`skill_salary`, since 8 Nov) | 10 | before this rule the top four shortage skills on 25 Oct each came from a single employer; with 5, Metal Fabrication (57 shortage postings) and Product Management (85) on 3 Nov each came from 5 firms |
| Biggest employer's share of a skill's barrier, shortage or salaried postings | at most 0.5 | 10 employers can still be one bulk poster plus nine with a posting each. Stored as `top_company_share` |
| Usable monthly salary | $300 to $100,000 | hourly or daily rates below, annual figures above |
| Barrier posting | views per day at or above the comparison group's P75, applications per view at most 0.8 of the group's; the group is the 2-digit SSOC group if it has 200+ postings, else the 1-digit major group | the most-viewed quarter of similar jobs, with at least 20% fewer applications per view than similar jobs |
| Barrier skill | lift 1.5 or more, 25+ barrier postings, applications per view below the market's (`barrier_max_rate_vs_overall` 1.0) | half again as common as its occupations predict; the same flagged-posting minimum as shortage; and people really do apply less, not only less than in a crowded occupation |
| Employment agency (shortage only) | poster's SSIC 2020 code starts with 78 (`agency_ssic_prefix`) | the official industry code of the poster, on every posting; agencies repost evergreen ads |
| Shortage: reposts, applications | 2 reposts, at most 2 applications | 2 is the highest repost count seen on 25 Oct (6,163 postings). At most the median posting's 2 applications |
| Generic skill tags | the list in `data/reference/generic_skill_tags.csv` | an explicit, reviewed list with a reason per tag, not a rule that could drop real skills |
| Reposted with many applications | reposted at least once, and the P90 of applications per day since posting that week; direct employers only | data-driven, recorded in the `rule` text. Reposted once is enough here (twice for shortage): a posting reposted even once while drawing many applications is the odd case |
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
   lists `mcf_vs_mom_share` and the 9 tables of `05`: `run_log`, `posting_week` and the 7 signal tables.
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
`build_posting_week`, the fan-out of seven signal tables and the fan-in to `check_and_log`.

## 4. Checks (in `metrics.run_log.checks`)

`fail` stops the run (the rows stay for inspection): no open postings, no overall competitiveness row, a share
outside 0 to 1, a negative rate. `warn` is recorded only: a signal table with no rows, more than 2% of open
postings carried over a crawl skip, a snapshot that is not the week's Tuesday, a snapshot that failed a daily
data-quality check. `info` puts the 24-hour application rate and the observed days per posting on record.

## 5. Validation on real data (24 and 25 Oct; first validation run, old definitions)

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

Barrier skills on this day (old definition, raw views against all postings) are low-paid manual and service
tags: Packaging is in 1,001 open postings from 458 employers, draws average views, and gets 3.3 applications
per 100 views against 7.2 overall (`rate_vs_overall` 0.45). The 4 Nov review read this as an occupation effect,
which is why barrier skills are now judged within similar jobs (2-digit SSOC groups, 1-digit as fallback).

### Findings that hold (weeks ending 27 Oct and 3 Nov, critical review of 8 Nov)

The three findings that survived every check in the 8 Nov critical review (current rules, both weeks rebuilt at
21:39 SGT on 8 Nov). Numbers are for 3 Nov unless stated. Intervals are employer-clustered bootstraps, because one
employer can post hundreds of ads. The first week (27 Oct) is partial: 4 snapshots from Saturday 24 Oct.

1. **Construction cost and site skills are hard to fill on MCF, robustly, but the data cannot isolate one cause.**
   Among direct employers' postings, Tendering is 2.96x as likely to be a shortage posting as the average (26
   shortage postings, 26 employers, 95% interval 1.90 to 4.17), Quantity Surveying 2.43x (33, 33 employers, 1.67 to
   3.24), Cost Control 2.14x (36, 31, 1.41 to 2.99), Technical Drawing 1.82x (74, 60, 1.34 to 2.36) and Civil
   Engineering 1.68x (64, 54, 1.22 to 2.16). It holds within occupation (lift against the postings' own 2-digit SSOC
   groups 1.69 to 3.25), no commission-sales firm posts any of these shortage postings, the biggest employer posts
   3% to 8%, and six rule variants give 1.66x to 3.97x. On 27 Oct Quantity Surveying (2.06x), Cost Control (2.01x)
   and Technical Drawing (1.69x) were listed; Tendering (2.46x, 22 postings, under the minimum of 25) and Civil
   Engineering (1.44x, under 1.5) were not. Caveats: a shortage posting here is an ad open about three months
   (median 89 days) with at most 2 MCF applications, which reads as "hard to fill on MCF" more than as a proven
   shortage. And construction shows every other pattern too: barrier skills (Construction Drawings, Site
   Inspections, Structural Engineering), the most entry-level postings asking 2+ years (29%), and less than half the
   market's applicants per posting-day (0.148 against 0.345). Several causes overlap in this sector, which is itself
   the result.
2. **Competition separates cleanly from the other causes.** Clerical support workers get 0.630 MCF applications per
   posting per 24 hours against 0.079 for craftsmen and related trades, about 8x (27 Oct: 0.507 against 0.046,
   11x). The ranking of 319 occupations (30+ postings) is nearly the same in both weeks (rank correlation 0.96; 16
   of the bottom 20 and 15 of the top 20 recur). Most applicants: administration managers (2.14 a day),
   budgeting and financial accounting managers (1.89), financial analysts (1.69). Fewest: beauticians, masseurs,
   tilers, structural metal erectors, automotive mechanics. Where difficulty exists in the oversubscribed groups it
   is about selection, not supply. Caveats: the ranking is strong, the size less so (11x to 8x, the first week
   partial); MCF applications only, and trades often hire outside MCF.
3. **Experience asks split into two stories, told apart by pay.** At Junior Executive level 47% of postings ask 2+
   years (4,386 of 9,416, 3,081 employers), and they pay 20.6% more than other Junior Executive postings (95%
   interval +11.1% to +29.0%; 27 Oct 45.9% and +19.0%): mid-level roles labelled junior rather than an unrealistic
   ask. At entry level 6.3% ask 2+ years (914 of 14,611, 307 employers; interval 3.5% to 10.9%; 27 Oct 6.5%), with
   no sign of a pay premium ($3,445 against $3,400, +1%, interval −14.6% to +17.1%): the candidate unrealistic ask.
   Caveats: one agency, RN Care, posts a third of the entry-level flags (299 of 914); without it the share is 4.4%,
   and for direct employers only 5.0%. Neither pay comparison holds occupation fixed, and "labelled junior" is an
   interpretation of the pay gap, not something observed.

### Skill salary check (weeks ending 27 Oct and 3 Nov, run 8 Nov)

Project Resources topped `skill_salary` at a median $14,000, and 54 of its 109 salaried postings were one
recruiter's $10-18k site manager ads. Two rules from shortage and barrier now apply to skill pay as well: no
employer may post more than half of a skill's salaried postings (`max_top_company_share`), and the postings must
come from at least 10 employers (`min_companies`). The week was rebuilt with both rules inside a transaction that
was then rolled back:

| # | Skill | Postings | Median monthly pay | vs market | Biggest employer's share | Employers |
|---|---|---|---|---|---|---|
| 1 | Project Resources | 109 | $14,000 | 3.46x | 49.5% | 46 |
| 2 | Enterprise Architecture | 108 | $10,500 | 2.59x | 5.6% | 75 |
| 3 | Solution Architecture | 193 | $10,000 | 2.47x | 5.7% | 123 |
| 4 | Direct experience | 136 | $9,250 | 2.28x | 36.0% | 59 |
| 5 | AI Agents | 182 | $9,000 | 2.22x | 4.4% | 127 |

The top 5 did not change. `min_companies` dropped 1 of 2,209 skills, none near the top. Project Resources passes
both rules: it has 46 employers, and its biggest one posts 49.5% of its postings, just under the cap.
(Security Governance and Project Governance tie AI Agents at $9,000, so which of them is 5th is arbitrary.)

Neither rule stops one bulk poster from setting a skill's median. Rules on which skills get a row cannot fix
this, so the median itself was changed: `skill_salary` is now employer-weighted (each employer's median first,
then the median across employers), and both weeks were rebuilt. Median monthly midpoint, old (per posting)
against new (per employer):

| # | Week ending 27 Oct, old | Week ending 27 Oct, new | Week ending 3 Nov, old | Week ending 3 Nov, new |
|---|---|---|---|---|
| 1 | Solution Architecture $10,000 | Family Medicine $10,450 | Project Resources $14,000 | Family Medicine $10,950 |
| 2 | Data Governance $8,750 | Solution Architecture $10,062 | Enterprise Architecture $10,500 | Enterprise Architecture $10,500 |
| 3 | Project Governance $8,750 | Influencing Key Stakeholders $9,500 | Solution Architecture $10,000 | Solution Architecture $10,000 |
| 4 | Microservice $8,750 | Google Cloud $9,125 | Direct experience $9,250 | Strategic Leadership $9,750 |
| 5 | Influencing Key Stakeholders $8,625 | Governance $9,000 | AI Agents $9,000 | AI Agents $9,500 |
| 6 | Governance $8,500 | Lead implementation $9,000 | Project Governance $9,000 | Influencing Key Stakeholders $9,500 |
| 7 | AI Agents $8,500 | Project Governance $8,975 | Security Governance $9,000 | Data Governance $9,000 |
| 8 | Data Engineering $8,500 | Data Governance $8,750 | Influencing Key Stakeholders $8,838 | Governance $9,000 |
| 9 | Agile Delivery $8,500 | Agile Delivery $8,750 | Cross-Functional Team Leadership $8,750 | Agile Methodologies $9,000 |
| 10 | Cross Functional Project Management $8,425 | Microservices Design $8,750 | Data Governance $8,750 | Project Governance $9,000 |

Project Resources falls from $14,000 to $6,000 (46 employers) and Direct experience from $9,250 to $5,250
(59 employers): both had been lifted by one bulk poster. Family Medicine moves the other way, from $5,750 to
the top in both weeks (46 and 49 employers): one recruiter posted 45 and 54 of its salaried postings at a
median $5,750, which set the old median; the other employers, mostly with a posting or two each, pay more.
The market median the ratios compare with barely moves (about $4,000). Ties are ordered by postings.

What changed with it (commit `97c3f85`):

- `metrics.skill_salary`: the median minimum, median maximum, and P25, median and P75 of the midpoint are all
  taken across employers' own medians. `mid_vs_overall` compares with the market median weighted the same way.
  The rules are unchanged: 100+ salaried postings, 10+ employers, no employer above half of them.
- New column `companies` (employers behind the salaried postings), next to `postings`. An existing warehouse
  gets it from the upgrade block at the end of `05_metrics_signals.sql`; weeks not rerun read 0.
- `metrics.skill_signals` gains `salary_companies` (`10_dashboard_signals.sql`). The Skill lookup "Pay" tile
  says the figure is across the skill's employers, each counted once.
- The Salary tab is unchanged: it reads `mart.skill_salary_latest`, which still takes medians per posting.
- Deployed on 8 Nov: `05` and `10` applied to the warehouse, then
  `python -m metrics.run --week-ending 2026-10-27 2026-11-03`. Both weeks finished with status `ok`:
  2,239 skills (27 Oct) and 2,208 (3 Nov), one fewer each than before because of the 10-employer
  minimum.

## 6. Limitations

- Three weeks of data by the report freeze (ending 27 Oct, 3 Nov and 10 Nov; the first holds only the
  24 to 27 Oct snapshots): week-on-week change exists for two pairs of weeks, so a trend is at most three points.
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
