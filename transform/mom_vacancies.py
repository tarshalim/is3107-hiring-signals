"""MOM job vacancies (data.gov.sg): raw -> clean, and the MCF-vs-MOM occupation share comparison.

Two steps, each a task in dags/weekly_metrics_dag.py:
  refresh_mom_stats(conn)   download the MOM dataset, upsert raw.mom_vacancy_rows and
                            clean.mom_vacancy_quarterly (full history, about 450 rows).
  build_comparison(conn, after, up_to)
                            compare the latest MCF snapshot of one week with MOM's latest quarter, per
                            occupation group, into metrics.mcf_vs_mom_share.

Matching the two sides: MCF postings carry a 5-digit ssocCode; its first digit is the SSOC major group.
MOM does not publish the 9 major groups, only 3 broad groups (Split A) and PME vs non-PME (Split B,
since 2013). data/reference/ssoc_major_to_mom_group.csv maps each major group to both splits, and
data/reference/mom_occupation_labels.csv maps MOM's label text to the same group codes.

Things to keep in mind when reading the numbers (for the report):
  - MOM counts vacancies at the end of the quarter from an employer survey. MCF is the set of postings
    open on one day. So we compare SHARES (mix across occupations), never raw counts.
  - MOM figures before 2006 exclude the public sector (private firms with 25+ employees only).
  - MCF covers only jobs posted on MCF, and many employers must post PMET roles there, so some PMET
    over-representation in MCF is expected.

CLI:  python -m transform.mom_vacancies [--skip-download] [--week-ending YYYY-MM-DD]
"""
import argparse
import csv
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path

from psycopg2.extras import Json, execute_values

from ingestion import mom_client
from transform.snapshot_week import SGT, latest_snapshot_in_week

REPO_ROOT = Path(__file__).resolve().parent.parent
REFERENCE_DIR = REPO_ROOT / "data" / "reference"
LABELS_CSV = REFERENCE_DIR / "mom_occupation_labels.csv"
SSOC_CSV = REFERENCE_DIR / "ssoc_major_to_mom_group.csv"

SPLITS = ("A", "B")
UNKNOWN = "unknown"
DISAGREE_PP = 10.0          # flag a group when the two shares differ by 10 percentage points or more
PUBLIC_SECTOR_FROM = 2006   # MOM's survey includes the public sector from 2006 onwards


# ---------------------------------------------------------------------------------------------
# Reference files
# ---------------------------------------------------------------------------------------------
def read_label_map(path: Path = LABELS_CSV) -> dict:
    """MOM label text -> {"split", "mom_group", "group_title"}."""
    with open(path, encoding="utf-8", newline="") as f:
        return {row["mom_label"].strip().lower(): row for row in csv.DictReader(f)}


def read_ssoc_map(path: Path = SSOC_CSV) -> dict:
    """SSOC first digit ('1'..'9') -> {"A": group, "B": group}."""
    with open(path, encoding="utf-8", newline="") as f:
        return {row["ssoc_major_group"]: {"A": row["split_a_group"], "B": row["split_b_group"]}
                for row in csv.DictReader(f)}


def digit_to_group(digit, split: str, ssoc_map: dict) -> str:
    """Group for an SSOC first digit in one split. Digits with no mapping -> 'unknown': None (no code),
    0 (armed forces) and letters (SSOC 'X' groups) are not in MOM's groups."""
    return ssoc_map.get(digit or "", {}).get(split, UNKNOWN)


# ---------------------------------------------------------------------------------------------
# Parsing API rows (pure functions, tested offline)
# ---------------------------------------------------------------------------------------------
def parse_quarter(text: str):
    """'2026-Q2' -> (2026, 2, date(2026, 4, 1))."""
    m = re.fullmatch(r"(\d{4})-Q([1-4])", str(text).strip())
    if not m:
        raise ValueError(f"unexpected quarter format: {text!r}")
    year, q = int(m.group(1)), int(m.group(2))
    return year, q, date(year, 3 * q - 2, 1)


def parse_vacancy(text):
    """'39300' -> 39300, '1,200' -> 1200. MOM writes '-' for a suppressed figure -> None."""
    cleaned = str(text if text is not None else "").replace(",", "").strip()
    if cleaned in ("", "-", "na", "NA"):
        return None
    return int(float(cleaned))


def to_clean_rows(api_rows: list, label_map: dict):
    """API rows -> (clean rows as dicts, sorted list of labels missing from the reference file)."""
    out, unmapped = {}, set()
    for r in api_rows:
        label = str(r["occupation"]).strip().lower()
        ref = label_map.get(label)
        if ref is None:
            unmapped.add(label)
            continue
        year, q, start = parse_quarter(r["quarter"])
        key = (r["quarter"], ref["mom_group"])
        # dict keyed on the primary key: if the API ever repeats a row, the last copy wins and the
        # upsert below never sees the same key twice in one statement.
        out[key] = {
            "quarter": r["quarter"], "quarter_start": start, "year": year, "quarter_num": q,
            "split": ref["split"], "mom_group": ref["mom_group"], "group_title": ref["group_title"],
            "job_vacancy": parse_vacancy(r["job_vacancy"]),
            "covers_public_sector": year >= PUBLIC_SECTOR_FROM,
        }
    return list(out.values()), sorted(unmapped)


def split_totals_mismatch(clean_rows: list, tolerance: int = 200) -> list:
    """Quarters where Split A and Split B totals differ by more than `tolerance` (they should be equal,
    give or take MOM's rounding to the nearest 100). Only quarters that have both splits are checked."""
    totals = {}
    for r in clean_rows:
        if r["job_vacancy"] is not None:
            totals.setdefault(r["quarter"], {}).setdefault(r["split"], 0)
            totals[r["quarter"]][r["split"]] += r["job_vacancy"]
    return sorted(q for q, t in totals.items() if "A" in t and "B" in t and abs(t["A"] - t["B"]) > tolerance)


# ---------------------------------------------------------------------------------------------
# Step 1: refresh MOM data
# ---------------------------------------------------------------------------------------------
def refresh_mom_stats(conn, session=None, dataset_id: str = mom_client.DATASET_ID) -> dict:
    """Download the full MOM series and upsert raw + clean. Safe to rerun: both tables are keyed on
    quarter + occupation, so a rerun updates the same rows."""
    fetched = mom_client.fetch_all(session=session, dataset_id=dataset_id)
    api_rows = fetched.pop("rows")
    clean_rows, unmapped = to_clean_rows(api_rows, read_label_map())
    if unmapped:
        # A new MOM label would silently shrink a group's total, so stop and ask for the mapping.
        raise ValueError(f"MOM labels not in {LABELS_CSV.name}: {unmapped}. Add them and rerun.")

    # Same idea as to_clean_rows: one raw row per primary key.
    raw = {(r["quarter"], r["occupation"]): r for r in api_rows}
    with conn, conn.cursor() as cur:   # one transaction: raw and clean change together or not at all
        execute_values(cur, """
            INSERT INTO raw.mom_vacancy_rows (dataset_id, quarter, occupation, payload)
            VALUES %s
            ON CONFLICT (dataset_id, quarter, occupation)
            DO UPDATE SET payload = EXCLUDED.payload, fetched_at = now()
        """, [(dataset_id, q, occ, Json(r)) for (q, occ), r in raw.items()])
        execute_values(cur, """
            INSERT INTO clean.mom_vacancy_quarterly
                (quarter, quarter_start, year, quarter_num, split, mom_group, group_title,
                 job_vacancy, covers_public_sector)
            VALUES %s
            ON CONFLICT (quarter, mom_group) DO UPDATE SET
                quarter_start = EXCLUDED.quarter_start, year = EXCLUDED.year,
                quarter_num = EXCLUDED.quarter_num, split = EXCLUDED.split,
                group_title = EXCLUDED.group_title, job_vacancy = EXCLUDED.job_vacancy,
                covers_public_sector = EXCLUDED.covers_public_sector, loaded_at = now()
        """, [(r["quarter"], r["quarter_start"], r["year"], r["quarter_num"], r["split"], r["mom_group"],
               r["group_title"], r["job_vacancy"], r["covers_public_sector"]) for r in clean_rows])

    quarters = sorted({r["quarter"] for r in clean_rows})
    return {
        **fetched,
        "raw_rows": len(raw),
        "clean_rows": len(clean_rows),
        "first_quarter": quarters[0] if quarters else None,
        "latest_quarter": quarters[-1] if quarters else None,
        "split_total_mismatches": split_totals_mismatch(clean_rows),
    }


# ---------------------------------------------------------------------------------------------
# Step 2: compare with MCF
# ---------------------------------------------------------------------------------------------
def _pct(part, whole):
    return round(100.0 * part / whole, 2) if part is not None and whole else None


def _change_pct(new, old):
    return round(100.0 * (new - old) / old, 2) if new is not None and old else None


def compare_shares(mcf_by_ssoc: dict, ssoc_map: dict, titles: dict, mom_latest: dict,
                   mom_1y: dict, mom_5y: dict, threshold_pp: float = DISAGREE_PP) -> list:
    """Pure function behind metrics.mcf_vs_mom_share.

    mcf_by_ssoc : first SSOC digit (or None) -> (postings, vacancies), from one MCF snapshot
    titles      : mom_group -> (split, group_title)
    mom_latest, mom_1y, mom_5y : mom_group -> vacancies in the latest quarter, 1 and 5 years earlier
    Returns one dict per split per group, plus one 'unknown' row per split.
    """
    rows = []
    for split in SPLITS:
        # Add up MCF postings per group of this split ('unknown' collects unmapped digits).
        mcf = {}
        for digit, (postings, vacancies) in mcf_by_ssoc.items():
            group = digit_to_group(digit, split, ssoc_map)
            p, v = mcf.get(group, (0, 0))
            mcf[group] = (p + postings, v + vacancies)

        groups = [g for g, (s, _t) in titles.items() if s == split]
        mcf_total_p = sum(mcf.get(g, (0, 0))[0] for g in groups)       # 'unknown' left out on purpose
        mcf_total_v = sum(mcf.get(g, (0, 0))[1] for g in groups)
        mom_total = sum(mom_latest.get(g) or 0 for g in groups)

        for g in groups:
            postings, vacancies = mcf.get(g, (0, 0))
            mcf_share = _pct(postings, mcf_total_p)
            mom_share = _pct(mom_latest.get(g), mom_total)
            gap = round(mcf_share - mom_share, 2) if mcf_share is not None and mom_share is not None else None
            rows.append({
                "split": split, "mom_group": g, "group_title": titles[g][1],
                "mcf_postings": postings, "mcf_vacancies": vacancies,
                "mcf_share_pct": mcf_share, "mcf_vacancy_share_pct": _pct(vacancies, mcf_total_v),
                "mom_vacancies": mom_latest.get(g), "mom_share_pct": mom_share, "share_gap_pp": gap,
                "mom_vacancies_1y_ago": mom_1y.get(g), "mom_yoy_pct": _change_pct(mom_latest.get(g), mom_1y.get(g)),
                "mom_vacancies_5y_ago": mom_5y.get(g), "mom_5y_pct": _change_pct(mom_latest.get(g), mom_5y.get(g)),
                "disagrees": gap is not None and abs(gap) >= threshold_pp,
            })

        # Counted, never shared: tells the report how many postings could not be matched.
        unknown_p, unknown_v = mcf.get(UNKNOWN, (0, 0))
        rows.append({
            "split": split, "mom_group": UNKNOWN, "group_title": "Missing or invalid ssocCode",
            "mcf_postings": unknown_p, "mcf_vacancies": unknown_v,
            "mcf_share_pct": None, "mcf_vacancy_share_pct": None, "mom_vacancies": None,
            "mom_share_pct": None, "share_gap_pp": None, "mom_vacancies_1y_ago": None, "mom_yoy_pct": None,
            "mom_vacancies_5y_ago": None, "mom_5y_pct": None, "disagrees": False,
        })
    return rows


def _mcf_counts(cur, snapshot_date) -> dict:
    """{first ssoc digit or None: (postings, vacancies)} for one snapshot. dim_job.ssoc_code is already
    NULL when the raw ssocCode was not 5 digits (transform/parse.py)."""
    # A posting without numberOfVacancies counts as one vacancy.
    cur.execute("""
        SELECT left(j.ssoc_code, 1) AS digit, count(*) AS postings, sum(coalesce(j.vacancies, 1)) AS vacancies
        FROM clean.fact_job_day f
        JOIN clean.dim_job j ON j.job_key = f.job_key
        WHERE f.snapshot_date = %s
        GROUP BY 1
    """, (snapshot_date,))
    return {digit: (int(p), int(v)) for digit, p, v in cur.fetchall()}


def _mom_quarter(cur, year: int, quarter_num: int) -> dict:
    cur.execute("SELECT mom_group, job_vacancy FROM clean.mom_vacancy_quarterly WHERE year = %s AND quarter_num = %s",
                (year, quarter_num))
    return dict(cur.fetchall())


def build_comparison(conn, after, up_to, threshold_pp: float = DISAGREE_PP, snapshot_date=None) -> dict:
    """Write metrics.mcf_vs_mom_share for one MCF snapshot in the week (after, up_to], against MOM's latest
    quarter. The DAG passes snapshot_date, the day pick_snapshot chose for the signals, so both use the same
    day even if the transform commits a new day in between. Without it (CLI) the latest snapshot in the week
    is used (transform/snapshot_week.py), raising NoSnapshotInWeek if the week has none.

    Rerun-safe: in one transaction it first deletes the rows of every snapshot in this week, then inserts.
    So a rerun replaces the week's rows, even when it picks a later day than the first try did (e.g. the
    first try ran before Tuesday's transform finished and used Monday)."""
    if snapshot_date is not None and not after < snapshot_date <= up_to:
        raise ValueError(f"snapshot {snapshot_date} is not in the week {after + timedelta(days=1)} to {up_to}")
    with conn, conn.cursor() as cur:
        if snapshot_date is None:
            snapshot_date = latest_snapshot_in_week(cur, after, up_to)
        mcf_by_ssoc = _mcf_counts(cur, snapshot_date)
        cur.execute("SELECT quarter, year, quarter_num FROM clean.mom_vacancy_quarterly ORDER BY quarter_start DESC LIMIT 1")
        latest = cur.fetchone()
        if latest is None:
            raise ValueError("No MOM quarter in clean.mom_vacancy_quarterly: run refresh_mom_stats first.")
        mom_quarter, year, q = latest
        titles = {r["mom_group"]: (r["split"], r["group_title"]) for r in read_label_map().values()}
        rows = compare_shares(mcf_by_ssoc, read_ssoc_map(), titles, _mom_quarter(cur, year, q),
                              _mom_quarter(cur, year - 1, q), _mom_quarter(cur, year - 5, q), threshold_pp)

        columns = ["split", "mom_group", "group_title", "mcf_postings", "mcf_vacancies", "mcf_share_pct",
                   "mcf_vacancy_share_pct", "mom_vacancies", "mom_share_pct", "share_gap_pp",
                   "mom_vacancies_1y_ago", "mom_yoy_pct", "mom_vacancies_5y_ago", "mom_5y_pct", "disagrees"]
        cur.execute("DELETE FROM metrics.mcf_vs_mom_share WHERE mcf_snapshot_date > %s AND mcf_snapshot_date <= %s",
                    (after, up_to))
        execute_values(cur, f"""
            INSERT INTO metrics.mcf_vs_mom_share
                (mcf_snapshot_date, mom_quarter, disagree_threshold_pp, {", ".join(columns)})
            VALUES %s
        """, [(snapshot_date, mom_quarter, threshold_pp, *[r[c] for c in columns]) for r in rows])

    return {
        "week": f"{after + timedelta(days=1)} to {up_to}",
        "mcf_snapshot_date": str(snapshot_date),
        "mom_quarter": mom_quarter,
        "rows": len(rows),
        "unknown_postings": next(r["mcf_postings"] for r in rows if r["mom_group"] == UNKNOWN),
        "disagreeing_groups": [f'{r["split"]}:{r["mom_group"]} ({r["share_gap_pp"]:+} pp)'
                               for r in rows if r["disagrees"]],
    }


def main():
    from transform.db import connect
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--skip-download", action="store_true", help="only rebuild the comparison")
    ap.add_argument("--week-ending", type=date.fromisoformat, default=datetime.now(SGT).date(),
                    help="last day of the 7-day snapshot week to compare, YYYY-MM-DD (default: today, SGT)")
    args = ap.parse_args()
    conn = connect()
    try:
        if not args.skip_download:
            print("refresh:", json.dumps(refresh_mom_stats(conn), default=str))
        after = args.week_ending - timedelta(days=7)
        print("compare:", json.dumps(build_comparison(conn, after, args.week_ending), default=str))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
