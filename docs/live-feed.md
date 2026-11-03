# Near-real-time feed (`mcf_live_poll`)

Written Tue 3 Nov 2026. Code: `dags/mcf_live_poll_dag.py`, `ingestion/mcf_live.py`, `quality/live_poll_checks.py`,
`sql/init/06_live_poll.sql`. Tests: `tests/test_mcf_live.py`, `test_live_poll_checks.py`, `test_live_poll_dag.py`,
`test_integration_live_poll.py`.

## 1. What it does

Every 30 minutes Airflow reads the first 3 pages (300 postings) of the same public, newest-first list the daily
crawl walks, and appends one slim row per posting to `raw.mcf_live_poll`. A new posting is seen within about 30
minutes of going live, then again every 30 minutes while it stays on those pages. That gives each posting its own
early curve of views and applications by hours since posting. The daily crawl cannot do this: it sees each posting
once a day, at whatever age it happens to be at 10:00.

```
fetch_newest  ->  load_poll  ->  check_poll
(3 API calls,     (append to      (9 checks -> raw.mcf_live_poll_dq,
 slim gz file)     raw.mcf_live_poll, fails the run on a 'fail' check)
                   raw.mcf_live_poll_runs)
```

| Object | What it holds |
|---|---|
| `raw.mcf_live_poll` | one row per posting per poll: poll time, uuid, list position, createdAt, posting dates, repost and edit counts, views, applications, SSOC code, first category, title. Key `(poll_ts, job_uuid)`, append-only |
| `raw.mcf_live_poll_runs` | one row per poll: pages, postings, postings seen on two pages, API total, seconds, a signature of each page as served |
| `raw.mcf_live_poll_dq` | one row per check per poll, same columns as `clean.dq_results` |
| `metrics.live_poll_pages` | one row per page per poll: was it an unchanged copy of an earlier poll's same page (the API's cache)? |
| `metrics.live_tracked_postings` | postings first seen within 1 hour of going live and not reposts: the ones whose curve starts near hour 0 |
| `metrics.live_curve_points` | every observation of a tracked posting in its first 24.5 hours: the 30-minute live points (minus cached copies), plus the daily crawl's point (from `clean.fact_job_day`) |
| `metrics.live_first24h_curve` | per hour since posting (0 to 23): postings measured, median views and applications, % with an application. Overall, by category, by SSOC major group |
| `metrics.live_first24h_summary` | the headline table: median applications and views at 1, 6, 12 and 24 hours, same groups |
| `metrics.live_poll_status` | one line per poll for the demo: postings seen, new since last poll, minutes since the newest posting, pages unchanged, checks failed or warned |

Settings: `MCF_LIVE_PAGES` (default 3) in `.env`. 2 s pause between pages. Same retry and backoff session as the
daily crawl.

## 2. Numbers

| | Live feed | For scale: daily crawl |
|---|---|---|
| API calls a day | 48 polls x 3 pages = **144** (plus retries, if any) | about 900 |
| Time per poll | about 5 s of fetching (measured 4.7 s), load 11 ms, checks 7 ms | about 24 min |
| Postgres a day | about **5.2 MB** (14,400 rows with both indexes, measured on a simulated day built from real rows) | about 490 MB JSONB |
| Files a day | about **0.8 MB** (`data/raw/mcf_live/<date>/<time>.jsonl.gz`, 17 KB a poll) | about 150 MB gz |

Running to the 15 Nov deadline (12 days) is about 60 MB in Postgres and 10 MB of files. The 10:00 poll overlaps
the start of the daily crawl and adds 3 calls to its roughly 900, so it does not change the crawl's load on the API.

**Terms.** The same use as the daily crawl and the 3 Nov plan (`Admin/PROF-WANTS-AND-GAPS.md`, D2): only the public
list of open postings in its default order, a pause between pages, no job-ID lookups, no closed postings, raw data
private, only totals published.

## 3. What the real polls returned (3 Nov 2026)

**First poll, 13:11 SGT** (the poller's code, into a throwaway Postgres):
- 3 pages in 4.7 s, all checks passed (the only warning: no previous poll yet). API total: 90,122 open postings.
- **234 distinct postings, not 300.** 66 postings were on two pages. Page 0 came back exactly as an earlier call 17
  minutes before had seen it (same order, same counters on all 100), while pages 1 and 2 were newer. The API
  caches pages and sends no cache headers.
- The newest posting was 16.8 minutes old and the 3 pages reached back **1.41 hours**. New postings arrive at about
  3 a minute at midday. 130 of the 234 were caught within an hour of going live. Of the 234, 81 had at least one view
  and 16 had an application (at most 9 views and 1 application). Top categories: Customer Service 25, Building and
  Construction 20.

**Second poll, 13:47 SGT** (the real DAG, run once with `dag.test()` in the `apache/airflow:2.10.5` image):
- All three tasks succeeded. Checks: 7 passed, `pages_out_of_step_share` warned (22% of postings on two pages).
- **Every page was the same as 36 minutes earlier**: same newest posting (now 53.6 minutes old), same API total,
  same counters. Page 0 had not changed for 53 minutes.

What the feed does about the cache, without cache-busting (that would load their servers and step outside the
usage we agreed):
- A posting on two pages is kept once, from the copy with the higher totals (totals never fall, so it is newer).
- Each page's signature is stored. A page equal to an earlier poll's same page is a cached copy, so its rows stay
  in raw (evidence) but are left out of the curves (they would show old counters at a later age). The info check
  `pages_unchanged_share` records the share per poll, so a day of polling measures how long the cache lives: a
  validation finding for the report.
- A fresh page's rows are labelled with the poll time, so they can be up to 30 minutes late. That is fine for curves
  in hours, and it is why polling more often than every 30 minutes would not make the data fresher.

So with 3 pages a daytime posting stays on the polled pages for about 1.5 to 2 hours (longer at night). The live
points give the dense first hours, and the daily crawl adds one point later in the first day (it also pages newest
first, so a day-old posting is fetched in its first minute or two; its time is the crawl's start). The 6, 12 and
24-hour figures therefore pool postings measured at that age, and each row says how many postings it rests on.
Raising `MCF_LIVE_PAGES` to 10 would cover about 5 daytime hours for 480 calls a day.

## 4. Why a 30-minute poll and not Kafka

MyCareersFuture is a pull-only REST list, not an event stream: it pushes nothing, so even with Kafka the producer
would be this same poller, writing to a topic instead of a table. The volume is tiny (about 3 new postings a minute
at peak, 300 rows and 11 ms of inserts per poll, 5 MB a day), the counters we study change over hours, and the API
itself serves pages that can be 15+ minutes old, so a stream would add no real freshness. Kafka would add a broker,
a consumer, topic retention and several hundred MB to 1 GB of memory on a laptop already capped at 6 GB, "more moving parts that
will fail" (wk1 [1:18:42]) for a job where "people might say that I don't need stream processing" (wk10 [115]).
Airflow already gives the schedule, retries, run history and a red run on failure, and the key `(poll_ts, job_uuid)`
makes reruns safe. **Upgrade path, not built:** if the feed grows (more sources, faster polls, or several consumers
such as an alert service and the dashboard), put a Kafka topic between `fetch_newest` and `load_poll`, as in the T7
lab. The poller publishes each poll's slim rows to a `mcf.live.postings` topic keyed by `job_uuid`, and a consumer
group loads them with the same `ON CONFLICT (poll_ts, job_uuid) DO NOTHING`, so replaying the topic stays
idempotent. The topic is then the buffer that decouples the two sides (the poller keeps running while the warehouse
is down, wk10 [113]) and its retention replaces the gz files for replay.

## 5. Deploy on the collector laptop

Check memory first: this adds no container, and a run takes seconds, but the laptop rules still apply. **Never run
`docker compose down -v`: it deletes the warehouse volume and every snapshot in it.**

```bash
git pull                                   # main, once feature/live-feed is merged
# 1. Add the tables and views (safe to run twice; needs the 01 and 02 tables, which are already there)
docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/06_live_poll.sql
# Windows PowerShell:
Get-Content sql/init/06_live_poll.sql | docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
# 2. Recreate the Airflow containers so they get MCF_LIVE_PAGES (the warehouse keeps its data)
docker compose up -d
# 3. Turn the DAG on (or flip the toggle in the UI). It runs at the next :00 or :30; press Trigger to run now.
docker compose exec airflow-scheduler airflow dags unpause mcf_live_poll
# 4. Check it
docker compose exec warehouse psql -U jobs -d jobs -c "SELECT * FROM metrics.live_poll_status ORDER BY poll_time_sgt DESC LIMIT 5;"
```

Every 30 minutes not polled is lost (the counters move on), so turn it on as soon as it is merged. Add
`data/raw/mcf_live/` to the usual gz backups (under 1 MB a day).

## 6. What to show in the report and video

Report:
1. Airflow Graph View of `mcf_live_poll` (3 tasks) and the Grid View with a day of green runs every 30 minutes.
2. `metrics.live_first24h_summary` for `group_type = 'all'` and the main categories: the analysis table.
3. A chart from `metrics.live_first24h_curve` (median applications and % with an application, hours 0 to 23).
4. `metrics.live_poll_status` for a few polls (freshness in minutes, new postings per poll, pages unchanged), and
   `raw.mcf_live_poll_dq` rows for `pages_out_of_step_share` and `pages_unchanged_share`: the API's cache is a
   validation finding (how often a poll brought nothing new, measured over a day).
5. Section 4's paragraph on Kafka, with the numbers from section 2.

Video (about 1 minute):
1. Grid View: press Trigger (or wait for :00 or :30) and watch the 3 tasks turn green in about 10 seconds.
2. Run `SELECT * FROM metrics.live_poll_status ORDER BY poll_time_sgt DESC LIMIT 3;` before and after: a new row,
   new postings since the last poll, minutes since the newest posting.
3. One posting's curve: `SELECT source, hours_since_posting, views, applications FROM metrics.live_curve_points
   WHERE job_uuid = '<uuid>' ORDER BY observed_at;`
4. One sentence: "a 30-minute micro-batch, because the source is a pull API with a cache, at 144 calls and 5 MB a
   day; Kafka is the upgrade path if it grows."

## 7. Limits

- 3 pages cover about 1.5 to 2 daytime hours. Between that and the daily crawl's point a posting has no
  observations, so the 6 and 12-hour figures rest on fewer postings (night postings, and those the daily crawl
  happened to see at that age).
- Different horizons can come from different postings, and postings created at different times of day.
- Reposts are not tracked (their counters carry over from the first posting period).
- Rows are labelled with the poll's start time. Pages are fetched within about 5 s of it, but the API's cache can
  make a page up to one poll interval older (cached copies of an earlier poll are dropped from the curves; the
  freshness column shows the age of the newest posting).
- If the cache turns out to live for hours, most polls bring nothing new. `pages_unchanged_share` will show it
  within a day; then say so in the report rather than poll faster.
