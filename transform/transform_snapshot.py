"""Load ONE snapshot day from raw.mcf_job_snapshots into the clean star schema, idempotently.

Steps (each timed and written to clean.load_log, L7: monitor performance, store results properly):
    parse_raw            stream the day's payloads from raw in batches, parse and clean each one
                         (transform/parse.py), stage the skill lists in a TEMP table
    upsert_dim_date      one row for the snapshot day (with the crawl's finish time from raw.ingest_runs)
    upsert_dim_skill     new skill tags get mapped to SkillsFuture once (transform/skill_mapping.py)
    upsert_dim_job       slowly changing dimension Type 2 (transform/scd.py decides, this file applies)
    load_bridge_job_skill  skills of today's new or changed versions
    load_fact_job_day    delete-then-insert the day's rows with deltas against the previous snapshot
    write_rejected       payloads that could not become a fact row (dead letter table)

Idempotency (L7, the 2 AM retry scenario): the whole day runs in ONE transaction, so a crash leaves
nothing half-loaded. dim rows use natural keys with ON CONFLICT or in-place updates, the fact and
rejected tables are delete-then-insert per snapshot_date. Re-running a day gives the same tables.
If an OLDER day is re-run after later days exist, the next snapshot's deltas are recomputed so they
still measure against the reloaded day.

Why plain Python + SQL and not Spark (L5/T6): one day is about 90,000 postings (about 700 MB of
JSON). A server-side cursor streams it in batches of 2,000, so memory stays flat and the parse
takes under a minute on a laptop. Spark would add a cluster, a JVM and a connector for a job that a
single process finishes in minutes. Spark is the scale-out path if the source grew to tens of
millions of rows a day: the parse step is already a pure function per payload (map), the SCD
and delta steps are joins on job_uuid (reduce by key), which is exactly the shape Spark runs on
many nodes. The heavy set operations (bridge, SSOC enrichment, next-day delta fix) already run
inside Postgres (ELT style, L7: use the warehouse's compute).
"""
import json
import time
from datetime import date, datetime, timezone

from psycopg2.extras import execute_batch, execute_values

from transform.deltas import compute_delta
from transform.parse import FACT_COLUMNS, JOB_COLUMNS, RejectRow, parse_posting
from transform.scd import CurrentVersion, plan_versions
from transform.skill_mapping import SkillMatcher, normalise

BATCH_SIZE = 2000          # payloads per fetch from the server-side cursor
SKILL_FLUSH = 20000        # staged skill rows per INSERT

DIM_JOB_INSERT_COLUMNS = JOB_COLUMNS + ("attr_hash", "valid_from", "valid_to", "is_current", "last_seen_date")
FACT_INSERT_COLUMNS = (
    "snapshot_date", "job_uuid", "job_key", "date_key",
    "views_total", "applications_total", "repost_count", "edit_count", "status",
    "days_since_new_posting", "days_since_original_posting", "days_to_expiry",
    "prev_snapshot_date", "hours_since_prev", "is_first_snapshot",
    "views_delta", "applications_delta", "reposted_since_prev", "edited_since_prev",
)


class StepLog:
    """Collects (step, started, finished, seconds, rows_in, rows_out, details) for clean.load_log."""

    def __init__(self):
        self.steps = []

    def start(self, name):
        return _Step(self, name)


class _Step:
    def __init__(self, log, name):
        self.log, self.name = log, name
        self.rows_in = self.rows_out = None
        self.details = {}

    def __enter__(self):
        self.started = datetime.now(timezone.utc)
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        finished = datetime.now(timezone.utc)
        seconds = round(time.perf_counter() - self.t0, 3)
        self.log.steps.append({
            "step": self.name, "started_at": self.started, "finished_at": finished, "seconds": seconds,
            "rows_in": self.rows_in, "rows_out": self.rows_out, "details": self.details or None,
        })
        print(f"[{self.name}] {seconds}s in={self.rows_in} out={self.rows_out} {self.details or ''}")
        return False


def date_key(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def dim_date_row(d: date, fetched_at):
    monday = d.fromordinal(d.toordinal() - d.weekday())
    return (date_key(d), d, d.year, d.month, d.day, d.isoweekday(), d.strftime("%A"),
            d.isoweekday() >= 6, monday, d.isocalendar()[1], fetched_at)


def snapshot_fetched_at(cur, d: date):
    """End of the crawl for that day: raw.ingest_runs.finished_at when the collector logged it,
    otherwise the latest fetched_at of the raw rows (the load time, for replayed files)."""
    cur.execute("SELECT finished_at FROM raw.ingest_runs WHERE snapshot_date = %s", (d,))
    row = cur.fetchone()
    if row and row[0]:
        return row[0]
    cur.execute("SELECT max(fetched_at) FROM raw.mcf_job_snapshots WHERE snapshot_date = %s", (d,))
    return cur.fetchone()[0]


def write_load_log(cur, run_id: str, snapshot_date: date, steps: list):
    execute_values(cur, """
        INSERT INTO clean.load_log (run_id, snapshot_date, step, started_at, finished_at, seconds, rows_in, rows_out, details)
        VALUES %s
    """, [(run_id, snapshot_date, s["step"], s["started_at"], s["finished_at"], s["seconds"],
           s["rows_in"], s["rows_out"], json.dumps(s["details"], default=str) if s["details"] else None)
          for s in steps])


def transform_snapshot(conn, snapshot_date: date, run_id: str | None = None,
                       matcher: SkillMatcher | None = None, batch_size: int = BATCH_SIZE) -> dict:
    if isinstance(snapshot_date, str):
        snapshot_date = date.fromisoformat(snapshot_date)
    run_id = run_id or f"cli-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    log = StepLog()
    t_all = time.perf_counter()
    stats = {"snapshot_date": str(snapshot_date), "run_id": run_id}

    with conn:                      # one transaction: the day is fully loaded or not at all
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw.mcf_job_snapshots WHERE snapshot_date = %s", (snapshot_date,))
            n_raw = cur.fetchone()[0]
            if n_raw == 0:
                raise ValueError(f"No raw rows for {snapshot_date}: nothing to transform")
            stats["raw_rows"] = n_raw

            cur.execute("""CREATE TEMP TABLE stage_skill (
                               job_uuid TEXT, skill_uuid TEXT, skill_name TEXT, is_key_skill BOOLEAN
                           ) ON COMMIT DROP""")

            # ---- 1. parse ---------------------------------------------------------------------
            staged, rejected = {}, []
            with log.start("parse_raw") as step:
                skill_rows = []
                # A named (server-side) cursor streams rows instead of pulling 700 MB into memory.
                with conn.cursor(name=f"raw_{snapshot_date:%Y%m%d}") as rc:
                    rc.itersize = batch_size
                    rc.execute("SELECT job_uuid, payload FROM raw.mcf_job_snapshots WHERE snapshot_date = %s",
                               (snapshot_date,))
                    for job_uuid, payload in rc:
                        try:
                            row = parse_posting(payload)
                        except RejectRow as exc:
                            rejected.append((snapshot_date, job_uuid, str(exc)))
                            continue
                        if row["job_uuid"] != job_uuid:
                            rejected.append((snapshot_date, job_uuid, "uuid in payload differs from key"))
                            continue
                        staged[job_uuid] = row
                        skill_rows.extend((job_uuid, s_uuid, s_name, is_key) for s_uuid, s_name, is_key in row["skills"])
                        if len(skill_rows) >= SKILL_FLUSH:
                            execute_values(cur, "INSERT INTO stage_skill VALUES %s", skill_rows, page_size=5000)
                            skill_rows = []
                if skill_rows:
                    execute_values(cur, "INSERT INTO stage_skill VALUES %s", skill_rows, page_size=5000)
                cur.execute("CREATE INDEX ON stage_skill (job_uuid)")
                cur.execute("SELECT count(*) FROM stage_skill")
                step.rows_in, step.rows_out = n_raw, len(staged)
                step.details = {"rejected": len(rejected), "skill_mentions": cur.fetchone()[0]}
            stats["staged"], stats["rejected"] = len(staged), len(rejected)

            # ---- 2. dim_date ----------------------------------------------------------------
            with log.start("upsert_dim_date") as step:
                fetched_at = snapshot_fetched_at(cur, snapshot_date)
                cur.execute("""
                    INSERT INTO clean.dim_date (date_key, date, year, month, day, day_of_week, day_name,
                                                is_weekend, week_start, iso_week, snapshot_fetched_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date_key) DO UPDATE SET snapshot_fetched_at = EXCLUDED.snapshot_fetched_at
                """, dim_date_row(snapshot_date, fetched_at))
                step.rows_out = 1
                step.details = {"snapshot_fetched_at": str(fetched_at)}

            # ---- 3. dim_skill ---------------------------------------------------------------
            with log.start("upsert_dim_skill") as step:
                cur.execute("""
                    SELECT DISTINCT ON (s.skill_uuid) s.skill_uuid, s.skill_name
                    FROM stage_skill s
                    WHERE NOT EXISTS (SELECT 1 FROM clean.dim_skill d WHERE d.mcf_skill_uuid = s.skill_uuid)
                    ORDER BY s.skill_uuid, s.skill_name
                """)
                new_tags = cur.fetchall()
                if new_tags:
                    matcher = matcher or SkillMatcher.from_db(conn)
                    rows = []
                    for s_uuid, s_name in new_tags:
                        m = matcher.match(s_name)
                        rows.append((s_uuid, s_name, normalise(s_name), m.skill_id, m.skill_title, m.skill_type,
                                     m.is_emerging, m.is_casl, m.method, m.confidence, snapshot_date, snapshot_date))
                    execute_values(cur, """
                        INSERT INTO clean.dim_skill (mcf_skill_uuid, mcf_skill_name, norm_name, ssg_skill_id,
                            ssg_skill_title, ssg_skill_type, is_emerging, is_casl, match_method, match_confidence,
                            first_seen_date, last_seen_date)
                        VALUES %s ON CONFLICT (mcf_skill_uuid) DO NOTHING
                    """, rows, page_size=2000)
                cur.execute("""
                    UPDATE clean.dim_skill d SET last_seen_date = %s
                    FROM (SELECT DISTINCT skill_uuid FROM stage_skill) s
                    WHERE d.mcf_skill_uuid = s.skill_uuid AND d.last_seen_date < %s
                """, (snapshot_date, snapshot_date))
                step.rows_in, step.rows_out = len(new_tags), len(new_tags)
            stats["skills_new"] = len(new_tags)

            # ---- 4. dim_job (SCD Type 2) ----------------------------------------------------
            with log.start("upsert_dim_job") as step:
                cur.execute("SELECT job_uuid, job_key, attr_hash, valid_from, last_seen_date FROM clean.dim_job WHERE is_current")
                current = {u: CurrentVersion(k, h, vf, ls) for u, k, h, vf, ls in cur.fetchall()}
                plan = plan_versions(snapshot_date, {u: r["attr_hash"] for u, r in staged.items()}, current)

                if plan.close:
                    execute_batch(cur, """
                        UPDATE clean.dim_job SET is_current = FALSE, valid_to = %s, last_seen_date = %s, updated_at = now()
                        WHERE job_key = %s
                    """, [(vt, vt, k) for k, vt in plan.close], page_size=1000)

                changed_keys = [k for _u, k in plan.update_in_place]   # versions whose skills must be rebuilt
                if plan.insert:
                    cols = ", ".join(DIM_JOB_INSERT_COLUMNS)
                    rows = [tuple(staged[u][c] for c in JOB_COLUMNS) + (staged[u]["attr_hash"], snapshot_date, None, True, snapshot_date)
                            for u in plan.insert]
                    inserted = execute_values(cur, f"INSERT INTO clean.dim_job ({cols}) VALUES %s RETURNING job_key",
                                              rows, page_size=1000, fetch=True)
                    changed_keys.extend(r[0] for r in inserted)

                if plan.update_in_place:
                    set_cols = [c for c in JOB_COLUMNS if c != "job_uuid"] + ["attr_hash"]
                    assignments = ", ".join(f"{c} = %s" for c in set_cols)
                    execute_batch(cur, f"UPDATE clean.dim_job SET {assignments}, updated_at = now() WHERE job_key = %s",
                                  [tuple(staged[u][c] for c in JOB_COLUMNS if c != "job_uuid") + (staged[u]["attr_hash"], k)
                                   for u, k in plan.update_in_place], page_size=1000)

                if plan.touch:
                    cur.execute("UPDATE clean.dim_job SET last_seen_date = %s WHERE job_key = ANY(%s) AND last_seen_date < %s",
                                (snapshot_date, plan.touch, snapshot_date))

                # Denormalise the SSOC title into the dimension (star schema: no join needed later).
                cur.execute("""
                    UPDATE clean.dim_job j SET ssoc_title = r.ssoc_title
                    FROM clean.ref_ssoc r
                    WHERE j.valid_from = %s AND j.ssoc_code = r.ssoc_code AND j.ssoc_title IS DISTINCT FROM r.ssoc_title
                """, (snapshot_date,))
                step.rows_in, step.rows_out = len(staged), len(plan.insert) + len(plan.update_in_place)
                step.details = plan.counts()
            stats.update({f"versions_{k}": v for k, v in plan.counts().items()})

            # ---- 5. bridge_job_skill: only versions inserted or changed in THIS run ----------
            # A version that was merely touched already has its bridge rows (they were loaded in the
            # same transaction that created the version), so a rerun does not rewrite 1.3 million rows.
            with log.start("load_bridge_job_skill") as step:
                if changed_keys:
                    cur.execute("DELETE FROM clean.bridge_job_skill WHERE job_key = ANY(%s)", (changed_keys,))
                    cur.execute("""
                        INSERT INTO clean.bridge_job_skill (job_key, skill_key, is_key_skill)
                        SELECT j.job_key, d.skill_key, bool_or(s.is_key_skill)
                        FROM stage_skill s
                        JOIN clean.dim_job j ON j.job_uuid = s.job_uuid AND j.job_key = ANY(%s)
                        JOIN clean.dim_skill d ON d.mcf_skill_uuid = s.skill_uuid
                        GROUP BY j.job_key, d.skill_key
                    """, (changed_keys,))
                    step.rows_out = cur.rowcount
                else:
                    step.rows_out = 0
                step.rows_in = len(changed_keys)
            stats["bridge_rows"] = log.steps[-1]["rows_out"]

            # ---- 6. fact_job_day ------------------------------------------------------------
            with log.start("load_fact_job_day") as step:
                cur.execute("""
                    SELECT job_uuid, job_key FROM clean.dim_job
                    WHERE valid_from <= %s AND (valid_to IS NULL OR valid_to >= %s)
                """, (snapshot_date, snapshot_date))
                key_for = dict(cur.fetchall())
                # Latest earlier snapshot of each posting (not always yesterday), with its fetch time
                # and posting date so hours_since_prev and reposted_since_prev can be computed.
                cur.execute("""
                    SELECT DISTINCT ON (f.job_uuid) f.job_uuid, f.snapshot_date, dd.snapshot_fetched_at,
                           f.views_total, f.applications_total, f.repost_count, f.edit_count, j.new_posting_date
                    FROM clean.fact_job_day f
                    JOIN clean.dim_date dd ON dd.date_key = f.date_key
                    JOIN clean.dim_job j ON j.job_key = f.job_key
                    WHERE f.snapshot_date < %s
                    ORDER BY f.job_uuid, f.snapshot_date DESC
                """, (snapshot_date,))
                prev = {}
                for u, d, fa, v, a, rc_, ec, npd in cur.fetchall():
                    prev[u] = {"snapshot_date": d, "fetched_at": fa, "views_total": v, "applications_total": a,
                               "repost_count": rc_, "edit_count": ec, "new_posting_date": npd}
                dk = date_key(snapshot_date)
                rows = []
                for u, r in staged.items():
                    job_key = key_for.get(u)
                    if job_key is None:
                        rejected.append((snapshot_date, u, "no dim_job version covers this snapshot day"))
                        continue
                    cur_row = {"snapshot_date": snapshot_date, "fetched_at": fetched_at,
                               "views_total": r["views"], "applications_total": r["applications"],
                               "repost_count": r["repost_count"], "edit_count": r["edit_count"],
                               "new_posting_date": r["new_posting_date"]}
                    delta = compute_delta(cur_row, prev.get(u))
                    rows.append((
                        snapshot_date, u, job_key, dk,
                        r["views"], r["applications"], r["repost_count"], r["edit_count"], r["job_status"],
                        (snapshot_date - r["new_posting_date"]).days if r["new_posting_date"] else None,
                        (snapshot_date - r["original_posting_date"]).days if r["original_posting_date"] else None,
                        (r["expiry_date"] - snapshot_date).days if r["expiry_date"] else None,
                        delta["prev_snapshot_date"], delta["hours_since_prev"], delta["is_first_snapshot"],
                        delta["views_delta"], delta["applications_delta"],
                        delta["reposted_since_prev"], delta["edited_since_prev"],
                    ))
                cur.execute("DELETE FROM clean.fact_job_day WHERE snapshot_date = %s", (snapshot_date,))
                deleted = cur.rowcount
                cols = ", ".join(FACT_INSERT_COLUMNS)
                execute_values(cur, f"INSERT INTO clean.fact_job_day ({cols}) VALUES %s", rows, page_size=5000)
                fixed = fix_next_snapshot_deltas(cur, snapshot_date)
                step.rows_in, step.rows_out = len(staged), len(rows)
                step.details = {"deleted_before_insert": deleted, "with_previous": sum(1 for r in rows if not r[14]),
                                "next_snapshot_rows_recomputed": fixed}
            stats["fact_rows"] = len(rows)
            stats["fact_rows_with_previous"] = log.steps[-1]["details"]["with_previous"]
            stats["next_snapshot_rows_recomputed"] = fixed

            # ---- 7. rejected rows (dead letter table) ---------------------------------------
            with log.start("write_rejected") as step:
                cur.execute("DELETE FROM clean.rejected_rows WHERE snapshot_date = %s", (snapshot_date,))
                if rejected:
                    execute_values(cur, "INSERT INTO clean.rejected_rows (snapshot_date, job_uuid, reason) VALUES %s", rejected)
                step.rows_out = len(rejected)
            stats["rejected"] = len(rejected)

            write_load_log(cur, run_id, snapshot_date, log.steps)

    stats["elapsed_seconds"] = round(time.perf_counter() - t_all, 3)
    stats["steps"] = [{"step": s["step"], "seconds": s["seconds"], "rows_in": s["rows_in"], "rows_out": s["rows_out"]}
                      for s in log.steps]
    return stats


def fix_next_snapshot_deltas(cur, snapshot_date: date) -> int:
    """When an older day is reloaded, the NEXT snapshot of each posting must measure its delta against
    the reloaded rows again. Same arithmetic as transform/deltas.py, done in SQL for the whole set.
    Returns the number of rows updated (0 in the normal daily case, where no later day exists)."""
    cur.execute("SELECT 1 FROM clean.fact_job_day WHERE snapshot_date > %s LIMIT 1", (snapshot_date,))
    if cur.fetchone() is None:
        return 0
    cur.execute("""
        WITH nxt AS (
            SELECT d.job_uuid, d.snapshot_date AS prev_date,
                   (SELECT min(x.snapshot_date) FROM clean.fact_job_day x
                     WHERE x.job_uuid = d.job_uuid AND x.snapshot_date > d.snapshot_date) AS next_date
            FROM clean.fact_job_day d WHERE d.snapshot_date = %s
        )
        UPDATE clean.fact_job_day n
        SET prev_snapshot_date  = p.snapshot_date,
            hours_since_prev    = round(EXTRACT(EPOCH FROM (dn.snapshot_fetched_at - dp.snapshot_fetched_at)) / 3600.0, 2),
            is_first_snapshot   = FALSE,
            views_delta         = n2.views_total - p.views_total,
            applications_delta  = n2.applications_total - p.applications_total,
            reposted_since_prev = (n2.repost_count > p.repost_count) OR (jn.new_posting_date <> jp.new_posting_date),
            edited_since_prev   = n2.edit_count > p.edit_count
        FROM nxt
        JOIN clean.fact_job_day n2 ON n2.job_uuid = nxt.job_uuid AND n2.snapshot_date = nxt.next_date
        JOIN clean.fact_job_day p  ON p.job_uuid = nxt.job_uuid AND p.snapshot_date = nxt.prev_date
        JOIN clean.dim_date dp ON dp.date_key = p.date_key
        JOIN clean.dim_job  jp ON jp.job_key = p.job_key
        JOIN clean.dim_date dn ON dn.date_key = n2.date_key
        JOIN clean.dim_job  jn ON jn.job_key = n2.job_key
        WHERE n.job_uuid = n2.job_uuid AND n.snapshot_date = n2.snapshot_date
    """, (snapshot_date,))
    return cur.rowcount


def write_run_summary(conn, run_id: str, snapshot_date, stats: dict, dq_summary: dict | None, seconds: float):
    """One 'dag_run' row per run in clean.load_log: the number the report's performance section quotes."""
    if isinstance(snapshot_date, str):
        snapshot_date = date.fromisoformat(snapshot_date)
    now = datetime.now(timezone.utc)
    details = {k: v for k, v in stats.items() if k != "steps"}
    if dq_summary is not None:
        details["dq"] = dq_summary
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO clean.load_log (run_id, snapshot_date, step, started_at, finished_at, seconds, rows_in, rows_out, details)
                VALUES (%s, %s, 'dag_run', %s, %s, %s, %s, %s, %s)
            """, (run_id, snapshot_date, now, now, round(seconds, 3), stats.get("raw_rows"), stats.get("fact_rows"),
                  json.dumps(details, default=str)))
