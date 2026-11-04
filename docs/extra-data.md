# Extra data: what was added, where it came from, what it shows

We collected 10 daily snapshots of every open MyCareersFuture (MCF) posting, 24 Oct to 2 Nov 2026. That is a
short window, so this branch adds three sources in a new `ext` schema (`sql/init/04_ext_schema.sql`). Each one
either reaches further back in time or adds a second, official view of the same jobs. MOM job vacancies are
not here on purpose: Tarsha's MOM ingestion loads them into `clean.mom_vacancy_quarterly`
(`sql/init/03_mom_vacancies.sql`), and the `weekly_metrics` DAG compares them with MCF.

All numbers below were measured on 3 Nov 2026 by rebuilding the clean layer from all 10 snapshot files in a
throwaway Postgres 16 container (every day passed its quality checks with no `fail`) and then running the
loaders in this branch on top.

## 1. Summary

| Dataset | Publisher, licence | Retrieved | Tables | Rows | Time covered |
|---|---|---|---|---|---|
| 2019 MCF crawl (Kaggle) | Kaggle user asrsaiteja, licence not stated | 3 Nov 2026 | `ext.mcf2019_posting`, `ext.mcf2019_skill`, `ext.mcf2019_posting_skill` | 20,296 postings, 2,538 tags, 405,920 posting-tag rows | posted 9 Jul 2018 to 18 Jun 2019, 98.5% in May and June 2019 |
| Resident Occupational Wages, June 2024 | MOM Occupational Wage Survey via data.gov.sg, Singapore Open Data Licence v1.0 | 3 Nov 2026 | `ext.mom_occupational_wage` | 562 occupations, all matched to a 5-digit SSOC code | June 2024 |
| Median income by occupation group and sex | MOM Labour Force Survey via data.gov.sg, Singapore Open Data Licence v1.0 | 3 Nov 2026 | `ext.mom_income_by_occupation` | 352 (8 groups x 2 sexes x 22 years) | June 2001 to June 2024 (no 2005, 2006) |
| Posting history (derived from our snapshots) | our own data | rebuilt weekly | `ext.posting_period`, `ext.posting_history_week`, `..._skill_week`, `..._ssoc_week` | 122,101 posting periods of 119,469 jobs, 33 weeks, 174,987 skill-week rows, 268 SSOC-week rows | weeks of 23 Mar to 2 Nov 2026, of which 28 Sep onward is usable as demand |

`ext.source_registry` keeps one row per external file: URL, licence, download time, sha256, rows in file and
rows loaded. The whole `ext` schema takes 144 MB. Load times on the laptop: posting history 6 s, both
data.gov.sg tables 18 s including the download, the Kaggle file 17 s.

## 2. 2019 MyCareersFuture crawl (Kaggle)

**Exact dataset.** https://www.kaggle.com/datasets/asrsaiteja/mycareerfuture-job-postings, version 1, one file
`mycareersfuture.json` (54.5 MB, 15.5 MB zipped), uploaded 11 Sep 2023 by "Asrst". It is the only MCF dataset
we found on Kaggle (web search and Kaggle's public search API). The posting dates and `JOB-2019-...` ids show it
is a crawl of the postings open in mid June 2019.

**Download.** No login or API token is needed. Kaggle's public download endpoint serves it directly:

```bash
curl -L -o data/ext/kaggle_mcf2019/archive.zip --create-dirs \
  https://www.kaggle.com/api/v1/datasets/download/asrsaiteja/mycareerfuture-job-postings
python -m ingestion.kaggle_mcf2019 data/ext/kaggle_mcf2019/archive.zip
```

If Kaggle ever starts asking for a login, the same file comes from `kaggle datasets download -d
asrsaiteja/mycareerfuture-job-postings` with a Kaggle API token (Brian's account).

**Licence.** Kaggle shows "Unknown". So the raw file is never committed (`data/ext/` and `mycareersfuture.json`
are in `.gitignore`), it is used only for coursework analysis, and the report should credit the uploader. The
postings were originally public MCF pages, collected by someone else in 2019. We did not crawl MCF for any of it.

**What it has.** Title, company, categories, position level, employment type, salary range, minimum experience,
exactly 20 skill tags per posting, posting and expiry date, and the application count when crawled (median 4,
19% with none). **What it lacks.** No SSOC code, no views, no repost count, and it is a sample of about 20,000
postings, not every open posting.

**Cleaning rules** (`ingestion/kaggle_mcf2019.py`, tested in `tests/test_ext_kaggle.py`):

| What the scraper left | Rule |
|---|---|
| every value is text, `not_available` means missing | NULL |
| 1,148 recruiter postings have `Recruiter` or `via Recruiter` as the job id, and store their lists as Python text | lists parsed with `ast.literal_eval` (never `eval`), key = hash of the whole record, `is_recruiter_post` flag |
| categories split on `/` (`Admin / Secretarial` became two items) | rejoined into MCF's own 41 category names, which match today's `clean.dim_job.categories` |
| salary as `$1,800to$2,200Monthly`, `$80,000to$100,000Annually`, `Up to$8,000Monthly`, `Salary undisclosed` | numbers, annual divided by 12, `salary_disclosed` false for the 2,844 undisclosed |
| experience as `2 years  exp` | integer |
| 2 records entirely `not_available` | rejected and counted (20,298 in, 20,296 loaded) |

**How it joins to today's data.**

- Skills by tag text: both years use MCF's tag list, so `ext.mcf2019_skill.tag_key = lower(clean.dim_skill.mcf_skill_name)`.
  2,030 of the 2,538 tags from 2019 still exist today, and they carry 82% of the 2019 tag mentions.
- Skills by SkillsFuture title: the same `SkillMatcher` as the daily transform maps 35% of the 2019 mentions.
- SSOC by title: 2019 postings borrow the SSOC code of today's postings with the same normalised title, only when
  that title has 3 or more postings today and one code covers at least 60% of them. 6,067 of 20,296 postings
  (30%) get a code. Rerun `python -m ingestion.kaggle_mcf2019 --map-ssoc-only` after more days are loaded.

**Caveats.** Every 2019 posting carries exactly 20 tags (most likely a cap at the time), today it is 14.5 on
average with a different vocabulary, so compare skill **ranks** of tags that exist in both years, not raw shares. Six of today's top 20
tags did not exist in 2019 (Interpersonal Skills, Team Player, Able To Work Independently, Scheduling,
Multitasking, Administrative Support) and "Management" fell from 64% of postings (rank 1) to 1.6% (rank 88),
which is a change in MCF's tagging, not in demand. The 2019 sample leans to IT: 22.5% of its postings list
Information Technology against 9.5% on 2 Nov 2026, so compare pay within a category or SSOC group, never the
overall median.

Views: `ext.v_skill_2019_vs_now` (tag, postings, share and rank in both years), `ext.v_salary_2019_vs_now`
(median advertised monthly salary per MCF category, a posting counts in every category it lists),
`ext.v_pay_change_by_ssoc_group` (advertised pay change next to the change in actual resident income).

## 3. MOM wage tables (data.gov.sg)

Both come through the official data.gov.sg API (`initiate-download`, then `poll-download` returns a signed URL)
with `python -m ingestion.datagov_wages`. The CSVs are saved to `data/ext/datagov/` (git-ignored).

**Resident Occupational Wages, June 2024** (`d_9917e751f7498502f70052a940a3f312`,
https://data.gov.sg/datasets/d_9917e751f7498502f70052a940a3f312/view). 562 occupations with the 25th, 50th and
75th percentile of monthly gross and basic wage. Gross wage is basic pay plus overtime, commissions and
allowances, before employee CPF, without employer CPF or bonuses, which is the closest official match to an
advertised monthly salary. The file has no SSOC code, only the occupation title, so it is matched to SSOC 2020
titles from `data/reference/ssoc2020_detailed_definitions.xlsx` (case, punctuation and plural "s" ignored,
bracketed text kept because it separates real occupations, titles shared by two codes never guessed). All 562
match. On 2 Nov 2026, 70,787 of 85,859 postings (82%) sit in one of these occupations.
Caveats: residents only, private establishments with 25 or more employees, and June 2024 is about two years
before our postings, so wages have grown a little since.

**Median gross monthly income of full-time employed residents by occupation group and sex, 2001 to 2024**
(`d_8f024ddf2553d81ee00ede55b1d9b0ff`, https://data.gov.sg/datasets/d_8f024ddf2553d81ee00ede55b1d9b0ff/view,
adapted from SingStat table M920131). Eight SSOC major groups (agricultural workers are not published) for men
and women separately. It includes employer CPF and one twelfth of bonuses, so it is used only for **growth**
over time (for example 2019 to 2024 next to the 2019 crawl), never for levels against advertised pay. 2005 and
2006 are not published.

Views: `ext.v_advertised_vs_actual_wage` (per 5-digit SSOC: median advertised min, max and midpoint on the
latest snapshot next to the OWS percentiles, and `ratio_mid_to_median`), and the LFS columns of
`ext.v_pay_change_by_ssoc_group`.

## 4. Posting history rebuilt from our own snapshots

**Idea.** Every snapshot row carries `original_posting_date`, `new_posting_date` (moves forward on a repost) and
`expiry_date`. So the first snapshot already says when each open posting was first posted, back to 23 Mar 2026.
`python -m transform.posting_history` turns `clean.dim_job` into one row per posting period (a first post or a
repost) and weekly counts per skill and per SSOC major group. It only reads the clean layer, so it needs no
extra scraping. MCF was not crawled for closed or old postings.

**The catch: survivorship.** We can only see a posting that was still open on 24 Oct or later. What the data
says about how long postings stay open:

- A posting period lasts 7, 14, 21 or 30 days (expiry minus new posting date, never anything else).
- Among periods that started in the 6 days before 24 Oct (all of which are still visible), the mix is 30 days
  67.7%, 14 days 15.5%, 7 days 14.6%, 21 days 2.2%. Among all postings in the 24 Oct snapshot it is 84% 30 days,
  because short postings from earlier weeks had already gone. That gap is the survivorship bias in one number.
- Employers also close postings before expiry: 0.38% of open postings a day (a posting that disappears for at
  least two snapshots while its expiry date is still ahead), so about 11% over 30 days.
- All 18,866 jobs first posted more than 30 days before 24 Oct had been reposted. Older weeks are therefore made
  only of jobs that were hard enough to fill to be reposted.

**Coverage label per week** (`ext.posting_history_week.coverage_class`):

| Label | Meaning | Weeks |
|---|---|---|
| observed | starts on or after 24 Oct, every posting seen by the daily crawl | 26 Oct |
| near_complete | starts at most 6 days before 24 Oct, every period was still open apart from early closures | 19 Oct |
| partial | some days within 30 days before 24 Oct, only the longer periods survived | 21 Sep to 12 Oct |
| survivors_only | every day more than 30 days before, only reposted jobs. Never read these weeks as demand | 23 Mar to 14 Sep (26 weeks) |
| in_progress | runs past the last snapshot | 2 Nov |

`est_coverage` is the estimated share of a week's posting periods we can see: for a period that started g days
before 24 Oct, P(seen) = share of periods lasting at least g days x (1 - 0.0038)^g. Dividing each day's count by
its P(seen) gives `periods_started_est`. A week with days nobody could have seen (for example 21 to 23 Sep) gets
no estimate.

| Week from | Jobs first posted | Periods started | of which reposts | Label | est_coverage | Periods, corrected |
|---|---|---|---|---|---|---|
| 21 Sep | 7,147 | 7,400 | 1,603 | partial | | |
| 28 Sep | 13,975 | 17,521 | 4,291 | partial | 0.620 | 28,282 |
| 5 Oct | 15,039 | 18,063 | 3,858 | partial | 0.668 | 27,051 |
| 12 Oct | 20,061 | 23,703 | 4,353 | partial | 0.837 | 28,311 |
| 19 Oct | 22,201 | 27,256 | 5,191 | near_complete | 0.990 | 27,542 |
| 26 Oct | 22,882 | 27,792 | 5,023 | observed | 1.000 | 27,792 |

The corrected column is a check on the method: the four estimated weeks land within 3% of the one week that
was fully observed. So **10 days of snapshots give 5 usable weekly points (28 Sep to 26 Oct) for skill and
SSOC demand**, the last two of them nearly complete, plus the observed week.

**For the "skill demand by posting week" chart:** use `ext.v_skill_week_share`, keep weeks from 28 Sep, and
show the label or `est_coverage` beside the bars. Shares are steadier than counts under survivorship, for
example (share of posting periods): Interpersonal Skills 15.3%, 14.1%, 14.2%, 16.1%, 16.8% and Microsoft Excel
11.4%, 11.2%, 10.4%, 10.1%, 9.8% over the weeks of 28 Sep to 26 Oct.

```sql
SELECT week_start, mcf_skill_name, share_of_periods, coverage_class, est_coverage
FROM ext.v_skill_week_share
WHERE week_start >= '2026-09-28' AND mcf_skill_name IN ('Python', 'SQL', 'Customer Service')
ORDER BY mcf_skill_name, week_start;
```

**Other caveats.** A repost replaces the earlier period in the data, so an earlier period of a reposted job is
not counted in its own week (it shows up only in "jobs first posted"). The coverage model assumes posting
lengths and early closures did not change over these weeks and that postings spread evenly over the days of a
week. Skills, SSOC and pay of a period are taken from the first version we saw.

## 5. Tables and views

| Object | Grain or content |
|---|---|
| `ext.source_registry` | one row per external file: URL, licence, retrieved_at, sha256, rows |
| `ext.mcf2019_posting` | one 2019 posting (salary, experience, categories, borrowed SSOC) |
| `ext.mcf2019_skill`, `ext.mcf2019_posting_skill` | distinct 2019 tags with SkillsFuture match, and the posting-tag bridge |
| `ext.mom_occupational_wage` | one occupation in one OWS year, with its SSOC code |
| `ext.mom_income_by_occupation` | one year x SSOC major group x sex |
| `ext.posting_period` | one posting period (job_uuid, new_posting_date) with the version first seen |
| `ext.posting_history_week` | one week: jobs first posted, periods, reposts, coverage label and estimate |
| `ext.posting_history_skill_week`, `ext.posting_history_ssoc_week` | one week x skill tag, one week x SSOC major group |
| `ext.v_now_posting` | postings on the latest snapshot, with the attributes of that day |
| `ext.v_skill_2019_vs_now`, `ext.v_salary_2019_vs_now`, `ext.v_pay_change_by_ssoc_group` | 2019 vs now |
| `ext.v_advertised_vs_actual_wage` | advertised pay vs OWS wages per SSOC occupation |
| `ext.v_skill_week_share` | share of each week's postings per skill, with the coverage label |

The `weekly_ext_data` DAG (`dags/weekly_ext_data_dag.py`, Mondays 12:00) refreshes the wage tables, loads the
Kaggle file when its checksum changes (skips when the file is missing), re-borrows SSOC codes, and rebuilds the
posting history. The three branches run in parallel. The loaders live in `ingestion/` and the history in
`transform/` so the existing Docker volume mounts already cover them.

## 6. Three findings the report can use

**1. Without a survivorship correction, the history shows growth that is not there.** Counted naively, posting
periods that started in the week of 28 Sep (17,521) rose to 27,792 in the week of 26 Oct, a 59% "increase". After
correcting for postings that had already expired or closed before our first snapshot, every week from 28 Sep to
26 Oct had about 27,000 to 28,300 new posting periods (about 22,000 brand-new jobs plus 5,000 reposts a week),
a flat market. All 18,866 jobs first posted more than 30 days before 24 Oct were reposts. Those long-lived
reposted jobs ask about twice as often as fresh postings for manufacturing and quality skills (Quality Assurance
2.6% vs 1.3%, Troubleshooting 6.7% vs 3.7%, Manufacturing 7.5% vs 4.1%, ISO 2.4% vs 1.3%) and half as often for
Sales (4.6% vs 7.6%) and Marketing (3.6% vs 6.6%), a lead for the skill shortage signal.

**2. Advertised pay sits below actual pay for PMET jobs and above it for blue-collar and service jobs.** For the
70,787 postings on 2 Nov whose occupation is in MOM's June 2024 wage survey, 58% advertise a salary midpoint below
the median gross wage of residents in that occupation (median ratio 0.91). It splits by level: managers 74% below
(median ratio 0.74), professionals 70% (0.83), associate professionals and technicians 61% (0.90), but service
and sales workers only 30% (1.18), craftsmen 22% (1.10) and plant and machine operators 23% (1.31). Examples: sales
manager, 2,766 postings, advertised midpoint 4,500 against a resident median of 7,049 (0.64). Office clerk, 2,257
postings, 2,750 against 1,822 (1.51). Lorry driver 3,500 against 2,159 (1.62). The wage survey is two years
older, which would narrow the PMET gap a little and widen the other one.

**3. Since 2019, advertised pay rose most in lower-paid work, and faster than residents' actual income there.**
The overall median advertised salary looks 11% lower than in 2019 (4,000 vs 4,500), but that is a mix effect
(the 2019 sample is 22.5% IT postings, today 9.5%). Within categories it rose in 12 of the 22 large ones:
Admin / Secretarial 2,350 to 3,150 (+34%), Customer Service 2,650 to 3,500 (+32%), F&B 2,800 to 3,500 (+25%),
Information Technology 6,250 to 6,800 (+9%), while Banking and Finance fell from 6,863 to 6,000 (-13%). By SSOC
group (2019 postings with a borrowed code), advertised pay for service and sales workers rose 41% and for
clerical workers 29%, against a 21% and 18% rise in those groups' actual median income from 2019 to 2024. For
professionals it is the other way: advertised +5%, actual income +17%.

## 7. Reproduce

```bash
docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/04_ext_schema.sql   # once, on an existing warehouse
python -m transform.posting_history
python -m ingestion.datagov_wages
python -m ingestion.kaggle_mcf2019 data/ext/kaggle_mcf2019/archive.zip               # after the curl in section 2
pytest tests/test_ext_*.py                                                           # offline
WAREHOUSE_TEST_DSN=postgresql://jobs:<pw>@localhost:5433/jobs pytest tests/test_integration_ext.py
```

Test coverage: `tests/test_ext_kaggle.py` (formats and cleaning rules), `tests/test_ext_datagov.py` (CSV layouts,
SSOC title matching, the API polling), `tests/test_ext_history.py` (coverage labels and estimates),
`tests/test_ext_dag.py` (DAG structure), and `tests/test_integration_ext.py` (all tables and views on a real
Postgres after the real daily transform, plus "a rerun changes nothing").

## 8. Open points

- The Kaggle licence is unknown. Fine for a course report with credit, but do not publish the raw file.
- The 2019 crawl has no views or reposts, so it cannot extend the competitiveness or repost signals, only skills,
  pay and applications.
- The posting history only needs the clean layer, so it improves by itself as more days are collected: every
  new day adds a fully observed day, and the oldest partial weeks stay as they are.
