# Dashboard: the weekly signal tabs

The question the project answers: **which skills and occupations are hard to hire for in Singapore, and is the
cause a shortage, a barrier or an unrealistic ask?** The weekly `metrics` tables hold the answer
(`docs/weekly-metrics.md`), but no tab showed them. Three tabs now do, placed first in the app.

They read only `metrics` (the tables of `sql/init/03_mom_vacancies.sql` and `05_metrics_signals.sql`, plus two
views in `sql/init/10_dashboard_signals.sql`), never `clean` or `raw`: the numbers are the stored weekly results,
only filtered and lined up with the week before. All SQL is parameterised and cached with `st.cache_data`
(`dashboard/signal_queries.py`: one hour, the week list ten minutes).

## What was added

**Week selector** (sidebar, "Weekly signals"): the finished weeks from `metrics.signal_weeks`, newest first, with
the snapshot the week was read from. The daily filters below it apply only to the six daily tabs.

| Tab, section | What it shows | Reads |
|---|---|---|
| Hiring signals, top | Four tiles, one per signal: share of shortage postings and the top shortage skills, number of barrier skills and the strongest, entry-level postings asking 2+ years with their pay gap (Junior Executive second, with its higher pay), the gap in applications per posting between occupation groups. One caution line from the MOM comparison | `skill_signals`, `unrealistic_requirements`, `competitiveness`, `mcf_vs_mom_share` |
| Which cause shows up where? | A grid: one row per job category (500+ postings) or SSOC major group, one column per signal (few applicants, shortage, barrier, entry level asking 2+ years). The number is the group's value; the colour is how many times the market it is, turned so red always points to hard to fill, blue the opposite, grey about the same (capped at 4x). Under each value, the postings it rests on; a cell on fewer than 100 postings is left blank with "too few" in muted text and the postings under it, outside the colour scale and with its own legend entry (on 3 Nov only four entry-level cells, e.g. Personal Care / Beauty's 42). Rows with the fewest applicants first. Hover: the cell's base, postings, agency share, median pay | `group_signals`, `competitiveness` |
| 1. Shortage | Top 15 skills by lift, bar text = shortage postings and employers, dark bar = also listed last week, light = new this week; every listed skill in a table | `skill_signals` (from `shortage_skill`) |
| 2. Barrier | Top 15 skills the run flagged (`is_barrier`), same encoding | `skill_signals` (from `barrier_skill`) |
| 3. Unrealistic ask | Leads with entry level asking 2+ years (6.3% on 3 Nov, +1% pay, so no sign of a pay premium: the clearest unrealistic ask; the caption adds that one agency posted about a third of them and that direct employers alone are at 5.0%), then Junior Executive (47%, but +21% pay: more likely mid-level roles labelled junior); each with its share, change since last week, pay against other postings and that reading of the pay gap. By job category, share bars next to a pay comparison. Below, "reposted despite many applicants" as one line: a minor signal with its share of direct employers' postings | `unrealistic_requirements` |
| 4. Competition | Applications per posting per 24 hours by SSOC major group (30+ postings) and by occupation (fewest or most, minimum-postings slider); last week as a black tick | `competitiveness` |
| Skill lookup | One skill, every signal: demand share in the latest fully observed posting week, pay band and premium, shortage, barrier, competition, a one-line verdict, and this week against last week. Opens on Quantity Surveying (the construction cost cluster, the strongest shortage finding) when the week has it | `skill_signals` |
| Market check | MCF share of postings against MOM share of vacancies by group (3 groups, or PME and non-PME), gap written on the chart, one line on what it means for the signals, and a table with MOM's 1- and 5-year change and applications per posting | `mcf_vs_mom_share`, `competitiveness` (`mom_group`) |

**Views** (`sql/init/10_dashboard_signals.sql`, plain views, no new data, nothing to refresh):

- `metrics.signal_weeks`: `run_log` weeks with `status = 'ok'`, their snapshot, thresholds and
  `prev_week_end_date` (the finished week before; a failed week is skipped).
- `metrics.skill_signals`: one row per week and skill with 100+ open postings or any signal row. Demand comes
  only from fully observed posting weeks (0 when no new posting asked for the skill), the `*_prev_week` columns
  are NULL in the first week. Also handy in psql for the video:
  `SELECT skill_name, shortage_lift, barrier_lift, apps_per_posting_day_24h FROM metrics.skill_signals WHERE week_end_date = '2026-11-03' AND skill_name = 'Quantity Surveying';`

Read as `docs/weekly-metrics.md` says: barrier skills are the rows the run flagged, shortage skills every row of
`shortage_skill` (the run already applied 25+ postings, 10+ employers, no employer above half), skill trends only
from `week_fully_observed` rows, flows per 24 hours, weeks that failed their checks left out. Every threshold in
the captions comes from that week's `run_log.thresholds`.

**App wiring** (`dashboard/app.py`): the weekly tabs come first and work even before the daily marts are built.
With empty marts the daily tabs say so instead of the app stopping, and Pipeline health still renders. A weekly
view that fails shows its error inside its own tab and leaves the others working.

## Deploy on the collector laptop

Nothing changes in Airflow, Docker images or requirements. Do it outside 10:00 to 11:00 SGT.

1. `git pull` on `main` once this branch is merged.
2. Add the two views (safe to run twice):
   ```bash
   docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/init/10_dashboard_signals.sql
   ```
   ```powershell
   Get-Content sql/init/10_dashboard_signals.sql | docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
   ```
3. Check: `docker compose exec warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT * FROM metrics.signal_weeks"'`
   lists the finished weeks.
4. `docker compose restart dashboard` (the code is mounted, no rebuild), open http://localhost:8501 and use the
   menu's "Clear cache" once.

If `feature/eng-improvements` is merged as well, its `09_ops_alerts.sql` is applied the same way; 09 and 10 do not
depend on each other. Never `docker compose down -v`.

## Tested

6 Nov 2026 on Brian's laptop, in a throwaway Postgres 16 container: every `sql/init` file applied, the real
exports of the weeks ending 27 Oct and 3 Nov loaded into `metrics` (`clean` and `mart` empty). The tabs showed
the stored numbers of that day, and the container was removed afterwards. The shortage and barrier numbers have
changed since with the 7 and 8 Nov rules (below). With both weeks as rebuilt on 8 Nov (21:39 SGT), the tables
the tabs read hold, for 3 Nov: 1.8% of direct employers' postings are shortage postings, 14 shortage skills (11 of
them listed on 27 Oct too) and 41 barrier skills (22 of them listed on 27 Oct too), General Ledger not flagged;
Junior Executive 47% asking 2+ years and paid +21%, entry level 6.3% and +1%, clerical support 0.63 against
craftsmen 0.08 applications per posting per day (8x), administration manager 2.14, PMET 73% of MCF postings
against 56% of MOM vacancies.

Tests: `tests/test_dashboard_signals.py` (logic, and offline checks that the views and queries read only
`metrics` and are parameterised) and `tests/test_integration_dashboard_signals.py` (Postgres, skipped without
`WAREHOUSE_TEST_DSN`: three hand-made weeks, one failed, checked against hand-worked values, then the whole app
rendered with Streamlit AppTest).

## What changed on 9 Nov (wording only, from the 8 Nov critical review)

- Entry level reads "with no sign of a pay premium", not "for the same pay": the +1% gap has a 95% interval of
  −15% to +17%. Its caption adds that one agency posted about a third of these postings and that the rate is
  5.0% for direct employers only (3 Nov).
- Two totals for one week are labelled apart: Market check counts MCF postings on the snapshot day, the weekly
  signal tabs count open postings in the week's pool (which includes carried-forward postings).
- Two salaries and two demand measures are labelled apart: the daily Salary tab shows the median per posting
  (daily), Skill lookup the employer-weighted median (weekly); Trending compares the last 7 days against the 7
  before, Skill lookup's demand counts new postings by posting week.
- Shortage: a caption notes that Spa rests on 26 shortage postings, just over the minimum of 25 (3 Nov).

## What changed on 8 Nov

- **Unrealistic ask leads with entry level** asking 2+ years (no sign of a pay premium), Junior Executive second with
  the note that it pays 21% more. The tile at the top follows the same order.
- **Reposted despite many applicants** counts direct employers only, as shortage does, and is shown as a minor
  signal with its share.
- **Generic skill tags** (`data/reference/generic_skill_tags.csv`) are left out of every skill-level signal.

## What changed on 7 Nov

Two rules were tightened after a check of the week ending 3 Nov, and a summary grid was added (rebuild old weeks
by clearing their weekly_metrics runs, or `python -m metrics.run --week-ending 2026-10-27 2026-11-03`):

- **Shortage reads direct employers only.** Employment agencies (SSIC 78) posted 43% of open postings but 60% of
  shortage postings; one agency alone made 738 of the 2,255. Shortage share 2.5% -> 1.8%, listed skills 74 -> 41 (now 14 on 3 Nov, after the 8 Nov generic tags and
  the shortage lift of at least 1.5).
  The construction skills stay (Tendering, Quantity Surveying, Cost Control, Technical Drawing); Revit, HVAC and
  B2B Sales were agency-driven and drop out.
- **A barrier skill must also convert below the market.** Barrier skills 81 -> 43 (now 41, after the 8 Nov generic tags): the accounting cluster
  (General Ledger, QuickBooks, Monthly Closing) is gone, because those postings still get 1.1 to 1.9 times the
  market's applications per view. Site, engineering and service skills remain.
- **Summary grid** (above): reads across the signals per group, which the four separate skill lists could not.

To apply on the collector laptop: `05_metrics_signals.sql` (adds `posting_week.posted_by_agency` and
`metrics.group_signals`) and `10_dashboard_signals.sql` (adds `barrier_rate_vs_overall` at the end of
`skill_signals`), both safe to run twice, then rerun the finished weeks and restart the dashboard.

## Notes for CH

- Your style is kept: one module per tab, SQL in a cached queries module, Plotly, `st.info` for empty states.
  The new SQL sits in `signal_queries.py` instead of `queries.py` only so the branches merge cleanly.
- Colours reuse the two blues of your salary band chart: dark = flagged last week too, light = new this week.
- The daily Hiring difficulty tab (a relative index on the daily marts) was removed on 8 Nov: it ranked
  occupations differently from the weekly signals. Competition (weekly, applications per posting per 24 hours) is
  the measure the report and video quote.
- To change a view's columns, `DROP VIEW metrics.skill_signals, metrics.signal_weeks` first: `CREATE OR REPLACE
  VIEW` can only add columns at the end.
- Two weeks exist so far, so "flagged last week too" is a two-point comparison, and skill demand has one fully
  observed posting week until the 9 Nov snapshot. Both fill in by themselves as weeks are added.
