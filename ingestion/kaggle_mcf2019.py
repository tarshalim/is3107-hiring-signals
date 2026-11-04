"""Load the 2019 MyCareersFuture crawl from Kaggle into ext.mcf2019_* (for "2019 vs now").

Dataset: https://www.kaggle.com/datasets/asrsaiteja/mycareerfuture-job-postings (one file,
mycareersfuture.json, 54 MB, {"jobs": [...]}, 20,298 rows). Public: Kaggle's download endpoint serves it
without a login. Licence: none stated, so the raw file stays out of git (data/ is ignored) and is only
used for coursework analysis. Get it with:

    mkdir -p data/ext/kaggle_mcf2019
    curl -L -o data/ext/kaggle_mcf2019/archive.zip \
        https://www.kaggle.com/api/v1/datasets/download/asrsaiteja/mycareerfuture-job-postings
    python -m ingestion.kaggle_mcf2019 data/ext/kaggle_mcf2019/archive.zip     # .zip or the extracted .json

What the scraper left behind, and the cleaning rule for each (L7: flag, do not silently drop):
  - every value is a string, 'not_available' means missing          -> NULL
  - 1,148 recruiter postings have 'Recruiter' / 'via Recruiter' as the job id, and their skill and
    category lists are stored as Python list text ("['AutoCAD', ...]")  -> parsed safely, keyed by a
    hash of the whole record, is_recruiter_post = true
  - categories were split on '/' ("Admin / Secretarial" became ['Admin ', ' Secretarial'])
                                                                    -> rejoined into MCF's category names
  - salary as text: '$1,800to$2,200Monthly', '$80,000to$100,000Annually', 'Up to$8,000Monthly',
    'Salary undisclosed'                                            -> numbers, annual / 12, disclosed flag
  - experience as '2 years  exp'                                    -> integer
  - 2 rows are entirely 'not_available'                             -> rejected (counted, not loaded)
  - a repeated posting_key (none in version 1, but a re-crawl could have them) -> first copy kept
Result on version 1: 20,298 records, 2 rejected, 20,296 loaded, every posting lists exactly 20 skills.

Skills are joined to today's data by tag text (both years use MCF's own tag list) and mapped to the
SkillsFuture list with the same SkillMatcher as the daily transform. 2019 postings carry no SSOC code, so
map_titles_to_ssoc() borrows one from today's postings with the same normalised title, only when that
title is common (3+ postings) and one code dominates (60%+). Rerun it after more snapshots are loaded.
"""
import argparse
import ast
import hashlib
import io
import json
import re
import sys
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from ingestion.ext_sources import KAGGLE_MCF_2019, register_source
from transform.parse import clean_text
from transform.skill_mapping import UNMATCHED, SkillMatcher, normalise

MISSING = "not_available"
JOB_ID_RE = re.compile(r"^JOB-\d{4}-\d+$")
SALARY_RE = re.compile(r"^\$([\d,]+)\s*to\s*\$([\d,]+)\s*(Monthly|Annually|Hourly|Daily)?$", re.I)
SALARY_UPTO_RE = re.compile(r"^Up to\s*\$([\d,]+)\s*(Monthly|Annually|Hourly|Daily)?$", re.I)
EXP_RE = re.compile(r"(\d+)\s*years?", re.I)
TITLE_MIN_SUPPORT = 3        # today's postings with that title before we trust its SSOC code
TITLE_MIN_SHARE = 0.6        # share of them that must carry the same code


class RejectRow(Exception):
    pass


def is_missing(value) -> bool:
    return value is None or (isinstance(value, str) and value.strip() in ("", MISSING))


def parse_list(value) -> list[str]:
    """A real list, or its Python text form "['a', 'b']" (recruiter rows). Never eval: literal_eval only."""
    if is_missing(value):
        return []
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            try:
                value = ast.literal_eval(text)
            except (ValueError, SyntaxError):
                value = [x for x in text.strip("[]").split("', '")]
        else:
            value = [text]
    return [str(x) for x in value if not is_missing(x)]


def parse_categories(value) -> list[str]:
    """['Admin ', ' Secretarial'] -> ['Admin / Secretarial']. The scraper split MCF's category text on '/',
    and several categories were joined with ', ' first, so rejoin on ' / ' and split on ','."""
    parts = [p.strip() for p in parse_list(value)]
    if not parts:
        return []
    joined = " / ".join(p for p in parts if p)
    out = []
    for c in joined.split(","):
        c = re.sub(r"\s*/\s*", " / ", c).strip()
        if c and c not in out:
            out.append(c)
    return out


def _money(text) -> Decimal:
    return Decimal(text.replace(",", ""))


def parse_salary(value) -> dict:
    """-> {disclosed, period, min, max, monthly_min, monthly_max}. Annual pay is divided by 12."""
    out = {"disclosed": False, "period": None, "min": None, "max": None, "monthly_min": None, "monthly_max": None}
    if is_missing(value):
        return out
    text = re.sub(r"\s+", " ", str(value)).strip()
    m = SALARY_RE.match(text)
    if m:
        lo, hi, period = _money(m.group(1)), _money(m.group(2)), (m.group(3) or "Monthly").title()
    else:
        m = SALARY_UPTO_RE.match(text)
        if not m:
            return out                                  # 'Salary undisclosed' and anything unexpected
        lo, hi, period = None, _money(m.group(1)), (m.group(2) or "Monthly").title()
    if period not in ("Monthly", "Annually"):
        return {**out, "disclosed": True, "period": period, "min": lo, "max": hi}
    div = Decimal(12) if period == "Annually" else Decimal(1)
    return {"disclosed": True, "period": period, "min": lo, "max": hi,
            "monthly_min": (lo / div).quantize(Decimal("0.01")) if lo is not None else None,
            "monthly_max": (hi / div).quantize(Decimal("0.01"))}


def parse_experience(value):
    if is_missing(value):
        return None
    m = EXP_RE.search(str(value))
    return int(m.group(1)) if m else None


def parse_int(value):
    if is_missing(value):
        return None
    try:
        return int(str(value).replace(",", "").strip())
    except ValueError:
        return None


def parse_date(value):
    if is_missing(value):
        return None
    try:
        return datetime.strptime(str(value).strip(), "%d %b %Y").date()
    except ValueError:
        return None


def tag_key(name) -> str | None:
    """Same key as lower(clean.dim_skill.mcf_skill_name): trimmed, single spaces, lower case."""
    text = clean_text(name)
    return text.lower() if text else None


def title_key(title) -> str:
    """Title without [bracketed] or (parenthesised) extras, normalised like skill names.
    'Admin Assistant [$1.5k-$2.2k / Bukit Batok]' -> 'admin assistant'."""
    return normalise(re.sub(r"\[[^\]]*\]", " ", str(title or "")))


def record_hash(job: dict) -> str:
    blob = json.dumps(job, sort_keys=True, ensure_ascii=False)
    return "h" + hashlib.sha1(blob.encode("utf-8")).hexdigest()[:20]


def parse_job(job: dict, source_row: int) -> dict:
    """One raw record -> ext.mcf2019_posting row plus its skill list. RejectRow when there is no title."""
    if not isinstance(job, dict):
        raise RejectRow("record is not an object")
    title = clean_text(None if is_missing(job.get("job_title")) else job.get("job_title"))
    if not title:
        raise RejectRow("no job title")
    raw_id = clean_text(job.get("job_id")) or ""
    job_id = raw_id if JOB_ID_RE.match(raw_id) else None
    levels = [x.strip() for x in str(job.get("seniority") or "").split(",") if x.strip() and x.strip() != MISSING]
    emp = [x.strip() for x in str(job.get("employment_type") or "").split(",") if x.strip() and x.strip() != MISSING]
    salary = parse_salary(job.get("salary"))
    skills, seen = [], set()
    for name in parse_list(job.get("skills_required")):
        key = tag_key(name)
        if key and key not in seen:
            seen.add(key)
            skills.append((key, clean_text(name)))
    categories = parse_categories(job.get("job_category"))
    return {
        "posting_key": job_id or record_hash(job),
        "job_id": job_id,
        "source_row": source_row,
        "is_recruiter_post": raw_id in ("Recruiter", "via Recruiter"),
        "company_name": None if is_missing(job.get("company_name")) else clean_text(job.get("company_name")),
        "title": title,
        "title_key": title_key(title) or title.lower(),
        "employment_types": "|".join(emp) or None,
        "position_level": levels[0] if levels else None,
        "position_levels": "|".join(levels) or None,
        "categories": "|".join(categories) or None,
        "min_years_exp": parse_experience(job.get("min_experience")),
        "salary_disclosed": salary["disclosed"],
        "salary_period": salary["period"],
        "salary_min": salary["min"],
        "salary_max": salary["max"],
        "salary_monthly_min": salary["monthly_min"],
        "salary_monthly_max": salary["monthly_max"],
        "applications": parse_int(job.get("no_of_applications")),
        "posting_date": parse_date(job.get("posting_date")),
        "expiry_date": parse_date(job.get("expiry_date")),
        "n_skills": len(skills),
        "skills": skills,
    }


def read_jobs(path: Path) -> list:
    """The {"jobs": [...]} file, straight from Kaggle's archive.zip or extracted."""
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".json")]
            if not names:
                raise ValueError(f"no .json file inside {path}")
            with zf.open(names[0]) as f:
                body = json.load(io.TextIOWrapper(f, encoding="utf-8"))
    else:
        with open(path, encoding="utf-8") as f:
            body = json.load(f)
    return body["jobs"] if isinstance(body, dict) else body


def parse_all(jobs: list) -> tuple[list, dict]:
    """Parse every record, keep the first copy of each posting_key. Returns (rows, stats)."""
    rows, keys = [], set()
    stats = Counter()
    for i, job in enumerate(jobs):
        stats["records"] += 1
        try:
            row = parse_job(job, i)
        except RejectRow:
            stats["rejected"] += 1
            continue
        if row["posting_key"] in keys:
            stats["duplicates_dropped"] += 1
            continue
        keys.add(row["posting_key"])
        rows.append(row)
    stats["loaded"] = len(rows)
    return rows, dict(stats)


POSTING_COLUMNS = ("posting_key", "job_id", "source_row", "is_recruiter_post", "company_name", "title", "title_key",
                   "employment_types", "position_level", "position_levels", "categories", "min_years_exp",
                   "salary_disclosed", "salary_period", "salary_min", "salary_max", "salary_monthly_min",
                   "salary_monthly_max", "applications", "posting_date", "expiry_date", "n_skills")


def skill_rows(rows: list, matcher: SkillMatcher | None) -> list[tuple]:
    """Distinct tags with their SkillsFuture match (unmatched when no reference tables are loaded)."""
    names = {}
    for r in rows:
        for key, name in r["skills"]:
            names.setdefault(key, name)
    out = []
    for key, name in sorted(names.items()):
        m = matcher.match(name) if matcher else UNMATCHED
        out.append((key, name, normalise(name), m.skill_id, m.skill_title, m.method, m.confidence))
    return out


def build_title_ssoc_lookup(pairs, min_support: int = TITLE_MIN_SUPPORT, min_share: float = TITLE_MIN_SHARE) -> dict:
    """[(title, ssoc_code)] from today's postings -> {title_key: (ssoc_code, share)} for titles that are
    common enough and dominated by one code. A title like 'Manager' with many codes is left out."""
    counts = defaultdict(Counter)
    for title, code in pairs:
        if code:
            counts[title_key(title)][code] += 1
    lookup = {}
    for key, c in counts.items():
        if not key:
            continue
        total = sum(c.values())
        code, n = c.most_common(1)[0]
        if total >= min_support and n / total >= min_share:
            lookup[key] = (code, round(n / total, 3))
    return lookup


def map_titles_to_ssoc(conn) -> dict:
    """Fill ext.mcf2019_posting.ssoc_code from today's postings (current dim_job versions). Idempotent."""
    with conn, conn.cursor() as cur:
        cur.execute("SELECT title, ssoc_code FROM clean.dim_job WHERE is_current AND ssoc_code IS NOT NULL")
        lookup = build_title_ssoc_lookup(cur.fetchall())
        cur.execute("SELECT posting_key, title_key FROM ext.mcf2019_posting")
        updates = []
        for posting_key, key in cur.fetchall():
            if key in lookup:
                code, share = lookup[key]
                updates.append((code, int(code[0]), "title", share, posting_key))
        cur.execute("UPDATE ext.mcf2019_posting SET ssoc_code = NULL, ssoc_major_group = NULL, ssoc_match = NULL, "
                    "ssoc_match_share = NULL")
        if updates:
            from psycopg2.extras import execute_batch
            execute_batch(cur, """
                UPDATE ext.mcf2019_posting SET ssoc_code = %s, ssoc_major_group = %s, ssoc_match = %s,
                    ssoc_match_share = %s
                WHERE posting_key = %s
            """, updates, page_size=1000)
        cur.execute("SELECT count(*) FROM ext.mcf2019_posting")
        total = cur.fetchone()[0]
    return {"titles_in_lookup": len(lookup), "postings_matched": len(updates), "postings": total,
            "match_share": round(len(updates) / total, 4) if total else None}


def _matcher_or_none(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM clean.ref_ssg_skill")
        if cur.fetchone()[0] == 0:
            print("WARNING: clean.ref_ssg_skill is empty, 2019 tags stay unmatched. "
                  "Run python -m transform.load_reference, then reload.")
            return None
    return SkillMatcher.from_db(conn)


def load_kaggle_mcf2019(conn, path: Path, matcher: SkillMatcher | None = None, retrieved_at=None) -> dict:
    """Full reload (the dataset is static): delete, insert, register the source. One transaction."""
    from psycopg2.extras import execute_values
    jobs = read_jobs(path)
    rows, stats = parse_all(jobs)
    matcher = matcher if matcher is not None else _matcher_or_none(conn)
    skills = skill_rows(rows, matcher)
    retrieved_at = retrieved_at or datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    dates = [r["posting_date"] for r in rows if r["posting_date"]]
    with conn, conn.cursor() as cur:
        cur.execute("DELETE FROM ext.mcf2019_posting_skill")
        cur.execute("DELETE FROM ext.mcf2019_posting")
        cur.execute("DELETE FROM ext.mcf2019_skill")
        execute_values(cur, f"INSERT INTO ext.mcf2019_posting ({', '.join(POSTING_COLUMNS)}) VALUES %s",
                       [tuple(r[c] for c in POSTING_COLUMNS) for r in rows], page_size=2000)
        execute_values(cur, """INSERT INTO ext.mcf2019_skill (tag_key, skill_name, norm_name, ssg_skill_id,
                           ssg_skill_title, match_method, match_confidence) VALUES %s""", skills, page_size=2000)
        execute_values(cur, "INSERT INTO ext.mcf2019_posting_skill (posting_key, tag_key) VALUES %s",
                       [(r["posting_key"], key) for r in rows for key, _ in r["skills"]], page_size=5000)
        register_source(cur, KAGGLE_MCF_2019, retrieved_at=retrieved_at, file_path=path,
                        rows_in_file=stats["records"], rows_loaded=len(rows),
                        coverage_start=min(dates) if dates else None, coverage_end=max(dates) if dates else None,
                        extra_notes=f"Rejected {stats.get('rejected', 0)} empty records, dropped "
                                    f"{stats.get('duplicates_dropped', 0)} duplicates.")
    methods = Counter(s[5] for s in skills)
    stats.update({"distinct_tags": len(skills), "tag_match_methods": dict(methods),
                  "posting_skill_rows": sum(len(r["skills"]) for r in rows),
                  "posting_date_min": str(min(dates)) if dates else None,
                  "posting_date_max": str(max(dates)) if dates else None})
    return stats


def main(argv=None):
    from transform.db import connect
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", nargs="?", help="archive.zip from Kaggle, or the extracted mycareersfuture.json")
    ap.add_argument("--map-ssoc-only", action="store_true", help="only refresh the title -> SSOC match")
    args = ap.parse_args(argv)
    if not args.map_ssoc_only and not args.path:
        ap.error("give the path to archive.zip or mycareersfuture.json (see the module docstring)")
    conn = connect()
    try:
        if not args.map_ssoc_only:
            print("load:", json.dumps(load_kaggle_mcf2019(conn, Path(args.path)), indent=2, default=str))
        print("ssoc:", json.dumps(map_titles_to_ssoc(conn)))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
