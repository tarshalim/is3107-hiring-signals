"""Load two MOM wage tables from data.gov.sg (official open data API) into the ext schema.

    mom_ows_2024               Resident Occupational Wages, June 2024 (Occupational Wage Survey).
                               562 occupations with gross and basic wage percentiles. Matched to 5-digit
                               SSOC 2020 codes by title, so advertised salaries in clean.dim_job can be
                               set against what residents in the same job are actually paid.
    mom_lfs_income_occupation  Median gross monthly income of full-time employed residents by occupation
                               group and sex, 2001 to 2024 (Labour Force Survey). The long-run series that
                               puts "advertised pay 2019 vs now" next to real income growth.

Neither table is about job vacancies, so nothing here overlaps the team's MOM vacancy ingestion
(ingestion/mom_client.py, clean.mom_vacancy_quarterly).

    python -m ingestion.datagov_wages                      # download both through the API, then load
    python -m ingestion.datagov_wages --offline            # load the copies already in data/ext/datagov/

How the download works (data.gov.sg API v1, no key needed for these volumes): GET
/datasets/{id}/initiate-download, then poll /datasets/{id}/poll-download until it returns a signed URL,
then GET that URL. The CSV is saved to data/ext/datagov/<dataset id>.csv (git-ignored) and its sha256 and
download time go into ext.source_registry. Licence: Singapore Open Data Licence v1.0 (attribution).
"""
import argparse
import csv
import json
import re
import sys
import time
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from ingestion.ext_sources import EXT_DATA_DIR, MOM_LFS_INCOME_OCCUPATION, MOM_OWS_2024, register_source

API_BASE = "https://api-open.data.gov.sg/v1/public/api/datasets"
DATASET_IDS = {
    MOM_OWS_2024.source_id: "d_9917e751f7498502f70052a940a3f312",
    MOM_LFS_INCOME_OCCUPATION.source_id: "d_8f024ddf2553d81ee00ede55b1d9b0ff",
}
DOWNLOAD_DIR = EXT_DATA_DIR / "datagov"
SSOC_FILE = Path(__file__).resolve().parent.parent / "data" / "reference" / "ssoc2020_detailed_definitions.xlsx"

# Labour Force Survey occupation groups -> SSOC 2020 major group (6, agricultural, is not published).
LFS_GROUPS = {
    "managers & administrators (including working proprietors)": 1,
    "professionals": 2,
    "associate professionals & technicians": 3,
    "clerical support workers": 4,
    "service & sales workers": 5,
    "craftsmen & related trades workers": 7,
    "plant & machine operators & assemblers": 8,
    "cleaners, labourers & related workers": 9,
}
OWS_WAGE_COLUMNS = (
    ("gross_wage_p25", "mthly_gross_wage_25_pctile"), ("gross_wage_p50", "mthly_gross_wage_50_pctile"),
    ("gross_wage_p75", "mthly_gross_wage_75_pctile"), ("basic_wage_p25", "mthly_basic_wage_25_pctile"),
    ("basic_wage_p50", "mthly_basic_wage_50_pctile"), ("basic_wage_p75", "mthly_basic_wage_75_pctile"),
)


# ---------------------------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------------------------
def make_session():
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "IS3107-student-project (NUS)"
    return session


def download_dataset(dataset_id: str, out_dir: Path = DOWNLOAD_DIR, session=None,
                     polls: int = 10, pause: float = 2.0) -> Path:
    """initiate-download, poll until a URL is ready, save the CSV. Raises after `polls` tries."""
    session = session or make_session()
    out_dir.mkdir(parents=True, exist_ok=True)
    session.get(f"{API_BASE}/{dataset_id}/initiate-download", timeout=30).raise_for_status()
    url = None
    for _ in range(polls):
        resp = session.get(f"{API_BASE}/{dataset_id}/poll-download", timeout=30)
        resp.raise_for_status()
        url = ((resp.json() or {}).get("data") or {}).get("url")
        if url:
            break
        time.sleep(pause)
    if not url:
        raise RuntimeError(f"data.gov.sg did not return a download URL for {dataset_id} after {polls} polls")
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    path = out_dir / f"{dataset_id}.csv"
    path.write_bytes(resp.content)
    return path


# ---------------------------------------------------------------------------------------------
# Parsing (pure, tested offline)
# ---------------------------------------------------------------------------------------------
def _stem(token: str) -> str:
    if len(token) > 4 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 3 and token.endswith("s") and not token.endswith(("ss", "us", "is")):
        return token[:-1]
    return token


def occupation_key(title) -> str:
    """Lower case, '&' -> 'and', punctuation -> space, plural 's' dropped per word. Parenthesised text is
    KEPT because it separates real occupations ('accountant (excluding tax accountant)')."""
    text = str(title or "").lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(_stem(t) for t in text.split())


def ssoc_title_lookup(ssoc_rows) -> dict:
    """[(code, title, ...)] -> {occupation_key: code}. Keys shared by two codes are dropped, never guessed."""
    lookup, clash = {}, set()
    for code, title, *_ in ssoc_rows:
        key = occupation_key(title)
        if key in lookup and lookup[key] != code:
            clash.add(key)
        lookup.setdefault(key, code)
    for key in clash:
        lookup.pop(key, None)
    return lookup


def to_money(value):
    text = str(value if value is not None else "").replace(",", "").strip()
    if not text or text.lower() in ("na", "n.a.", "-", "s", "x"):
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def parse_ows(rows, ssoc_lookup: dict) -> list[dict]:
    """CSV rows of 'Resident Occupational Wages' -> ext.mom_occupational_wage rows."""
    out = []
    for r in rows:
        occupation = re.sub(r"\s+", " ", (r.get("occ_desc") or "")).strip()
        if not occupation:
            continue
        code = ssoc_lookup.get(occupation_key(occupation))
        row = {"year": int(r["year"]), "occupation": occupation, "ssoc_code": code,
               "ssoc_major_group": int(code[0]) if code else None}
        for col, src in OWS_WAGE_COLUMNS:
            row[col] = to_money(r.get(src))
        out.append(row)
    return out


def parse_lfs_income(rows) -> list[dict]:
    """Wide CSV ('DataSeries', '2024', '2023', ...) with one row per 'Group - Male/Female' -> long rows.
    Unknown groups raise, so a renamed series is noticed instead of silently dropped."""
    out = []
    for r in rows:
        series = (r.get("DataSeries") or "").strip()
        if not series:
            continue
        group, _, sex = series.rpartition(" - ")
        major = LFS_GROUPS.get(group.strip().lower())
        if major is None or sex.strip().lower() not in ("male", "female"):
            raise ValueError(f"unexpected Labour Force Survey series: {series!r}")
        for key, value in r.items():
            if key and re.fullmatch(r"\d{4}", key.strip()):
                amount = to_money(value)
                if amount is not None:
                    out.append({"year": int(key), "ssoc_major_group": major, "occupation_group": group.strip(),
                                "sex": sex.strip().lower(), "median_income": amount})
    return out


def read_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------------------------
def _retrieved_at(path: Path):
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def load_ows(conn, path: Path, ssoc_rows=None) -> dict:
    from psycopg2.extras import execute_values
    if ssoc_rows is None:
        from transform.load_reference import read_ssoc
        ssoc_rows = read_ssoc(SSOC_FILE)
    raw = read_csv(path)
    rows = parse_ows(raw, ssoc_title_lookup(ssoc_rows))
    years = sorted({r["year"] for r in rows})
    cols = ("year", "occupation", "ssoc_code", "ssoc_major_group") + tuple(c for c, _ in OWS_WAGE_COLUMNS)
    with conn, conn.cursor() as cur:
        cur.execute("DELETE FROM ext.mom_occupational_wage WHERE year = ANY(%s)", (years,))
        execute_values(cur, f"INSERT INTO ext.mom_occupational_wage ({', '.join(cols)}) VALUES %s",
                       [tuple(r[c] for c in cols) for r in rows])
        register_source(cur, MOM_OWS_2024, retrieved_at=_retrieved_at(path), file_path=path,
                        rows_in_file=len(raw), rows_loaded=len(rows),
                        coverage_start=date(min(years), 6, 1) if years else None,
                        coverage_end=date(max(years), 6, 30) if years else None)
    matched = sum(1 for r in rows if r["ssoc_code"])
    return {"rows": len(rows), "ssoc_matched": matched, "unmatched": [r["occupation"] for r in rows if not r["ssoc_code"]],
            "years": years}


def load_lfs_income(conn, path: Path) -> dict:
    from psycopg2.extras import execute_values
    raw = read_csv(path)
    rows = parse_lfs_income(raw)
    years = sorted({r["year"] for r in rows})
    cols = ("year", "ssoc_major_group", "occupation_group", "sex", "median_income")
    with conn, conn.cursor() as cur:
        cur.execute("DELETE FROM ext.mom_income_by_occupation")
        execute_values(cur, f"INSERT INTO ext.mom_income_by_occupation ({', '.join(cols)}) VALUES %s",
                       [tuple(r[c] for c in cols) for r in rows])
        register_source(cur, MOM_LFS_INCOME_OCCUPATION, retrieved_at=_retrieved_at(path), file_path=path,
                        rows_in_file=len(raw), rows_loaded=len(rows),
                        coverage_start=date(min(years), 6, 1) if years else None,
                        coverage_end=date(max(years), 6, 30) if years else None)
    return {"rows": len(rows), "series": len(raw), "years": f"{years[0]}-{years[-1]}" if years else None,
            "missing_years": [y for y in range(years[0], years[-1] + 1) if y not in years] if years else []}


LOADERS = {
    MOM_OWS_2024.source_id: (MOM_OWS_2024, load_ows),
    MOM_LFS_INCOME_OCCUPATION.source_id: (MOM_LFS_INCOME_OCCUPATION, load_lfs_income),
}


def main(argv=None):
    from transform.db import connect
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--offline", action="store_true", help="use the CSVs already in --dir, no download")
    ap.add_argument("--dir", default=str(DOWNLOAD_DIR))
    args = ap.parse_args(argv)
    out_dir = Path(args.dir)
    conn = connect()
    try:
        for source_id, (_source, loader) in LOADERS.items():
            dataset_id = DATASET_IDS[source_id]
            path = out_dir / f"{dataset_id}.csv"
            if not args.offline:
                path = download_dataset(dataset_id, out_dir)
            print(source_id, json.dumps(loader(conn, path), default=str))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
