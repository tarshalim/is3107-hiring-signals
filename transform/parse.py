"""Turn one raw MCF payload (the JSON exactly as the API returned it) into the slim typed row
that the clean layer stores.

These are pure functions (no database), so they are unit-tested offline in tests/test_parse.py.
JSON paths follow the field map in the research pack (data-sources-and-fields.md, section A) and
the data check of 25 Oct (every field below was 100% filled on both days).

Cleaning rules, following L7 (validation = schema, range, business rules) and the lecturer's
point that clean data should be flagged, not silently dropped:
  - strings are trimmed and inner whitespace collapsed (85 titles a day have stray spaces)
  - the SSOC code stays TEXT: '01234' is a code, not a number
  - salary -> numeric (Decimal); missing -> NULL
  - years of experience outside 0..30 is capped at 30 and FLAGGED with exp_out_of_range
    (postings ask for 32, 50 and 88 years); the raw value is kept in min_years_exp_raw. The weekly
    metrics use the same cap (metrics/build_week.py imports EXP_MAX), so there is one number
  - dates 'YYYY-MM-DD' -> date; anything unreadable -> NULL (the DQ step counts them)
  - a payload with no uuid or no view/application counters cannot become a fact row, so it is
    rejected (RejectRow) and written to clean.rejected_rows instead of crashing the day
"""
import hashlib
import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

EXP_MIN = 0
EXP_MAX = 30     # was 40 until 4 Nov; the metrics already treated 31+ as typing errors
SEP = "|"

# dim_job columns that the parser fills, in insert order. attr_hash is computed over these plus
# the skill list, so any change in them creates a new SCD Type 2 version.
JOB_COLUMNS = (
    "job_uuid", "job_post_id", "title",
    "company_uen", "company_name", "company_ssic_code", "company_ssic_desc",
    "is_posted_on_behalf", "hiring_company_name",
    "ssoc_code", "ssoc_major_group", "mom_group",
    "position_level", "position_levels", "employment_types", "category", "categories", "education_code",
    "min_years_exp_raw", "min_years_exp", "exp_out_of_range",
    "salary_min", "salary_max", "salary_type", "salary_is_hidden",
    "vacancies", "original_posting_date", "new_posting_date", "expiry_date", "source_code",
)

# Fact measures taken from metadata. Running totals as of the snapshot.
FACT_COLUMNS = ("views", "applications", "repost_count", "edit_count", "job_status")


class RejectRow(Exception):
    """The payload cannot become a fact row. The message is the reason stored in clean.rejected_rows."""


def clean_text(value):
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def to_int(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_decimal(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def to_date(value):
    """'2026-10-24' or '2026-10-24T09:02:03.000Z' -> date; anything else -> None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def to_ssoc(value):
    """SSOC 2020 codes are 5 digits. Keep as text; anything else becomes NULL and is counted by DQ."""
    text = clean_text(value)
    if text and re.fullmatch(r"\d{5}", text):
        return text
    return None


def mom_group(ssoc_code):
    """MOM reports vacancies in three occupation groups keyed on the SSOC major group (first digit)."""
    if not ssoc_code:
        return None
    first = ssoc_code[0]
    if first in "123":
        return "PMET"
    if first in "45":
        return "clerical_sales_services"
    if first in "6789":
        return "production_transport"
    return None


def cap_experience(raw):
    """Return (capped_value, out_of_range_flag). None stays None."""
    if raw is None:
        return None, False
    if raw < EXP_MIN:
        return EXP_MIN, True
    if raw > EXP_MAX:
        return EXP_MAX, True
    return raw, False


def _joined(items):
    items = [clean_text(x) for x in items]
    items = [x for x in items if x]
    return SEP.join(items) if items else None


def parse_skills(payload):
    """[(skill_uuid, skill_name, is_key_skill)] with blanks dropped and duplicates collapsed."""
    seen = {}
    for s in payload.get("skills") or []:
        if not isinstance(s, dict):
            continue
        name = clean_text(s.get("skill"))
        uuid = clean_text(s.get("uuid"))
        if not name or not uuid:
            continue
        seen[uuid] = (uuid, name, bool(s.get("isKeySkill")) or seen.get(uuid, (None, None, False))[2])
    return sorted(seen.values())


def attr_hash(job: dict, skills) -> str:
    """Stable hash of the tracked attributes. Changing any of them means a new dim_job version."""
    tracked = {k: job.get(k) for k in JOB_COLUMNS if k != "job_uuid"}
    tracked["skills"] = [(u, k) for u, _name, k in skills]
    blob = json.dumps(tracked, sort_keys=True, default=str)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


def parse_posting(payload: dict) -> dict:
    """One raw payload -> {'job_uuid', <JOB_COLUMNS>..., <FACT_COLUMNS>..., 'skills', 'attr_hash'}."""
    if not isinstance(payload, dict):
        raise RejectRow("payload is not a JSON object")
    job_uuid = clean_text(payload.get("uuid"))
    if not job_uuid:
        raise RejectRow("missing uuid")
    md = payload.get("metadata") or {}
    views = to_int(md.get("totalNumberOfView"))
    applications = to_int(md.get("totalNumberJobApplication"))
    if views is None or applications is None:
        raise RejectRow("missing view or application counter")

    posted = payload.get("postedCompany") or {}
    hiring = payload.get("hiringCompany") or {}
    salary = payload.get("salary") or {}
    salary_type = (salary.get("type") or {}).get("salaryType") if isinstance(salary.get("type"), dict) else salary.get("type")
    levels = [clean_text(p.get("position")) for p in payload.get("positionLevels") or [] if isinstance(p, dict)]
    levels = [x for x in levels if x]
    ssoc = to_ssoc(payload.get("ssocCode"))
    exp_raw = to_int(payload.get("minimumYearsExperience"))
    exp_capped, exp_flag = cap_experience(exp_raw)
    categories = [clean_text(c.get("category")) for c in payload.get("categories") or [] if isinstance(c, dict)]
    categories = [c for c in categories if c]

    job = {
        "job_uuid": job_uuid,
        "job_post_id": clean_text(md.get("jobPostId")),
        "title": clean_text(payload.get("title")) or "(untitled)",
        "company_uen": clean_text(posted.get("uen")),
        "company_name": clean_text(posted.get("name")),
        "company_ssic_code": clean_text(posted.get("ssicCode2020")),
        "company_ssic_desc": clean_text(posted.get("ssicDescription2020")),
        "is_posted_on_behalf": bool(md.get("isPostedOnBehalf")),
        "hiring_company_name": clean_text(hiring.get("name")) if isinstance(hiring, dict) else None,
        "ssoc_code": ssoc,
        "ssoc_major_group": int(ssoc[0]) if ssoc else None,
        "mom_group": mom_group(ssoc),
        "position_level": levels[0] if levels else None,
        "position_levels": _joined(levels),
        "employment_types": _joined(e.get("employmentType") for e in payload.get("employmentTypes") or [] if isinstance(e, dict)),
        "category": categories[0] if categories else None,
        "categories": _joined(categories),
        "education_code": clean_text(payload.get("ssecEqa")),
        "min_years_exp_raw": exp_raw,
        "min_years_exp": exp_capped,
        "exp_out_of_range": exp_flag,
        "salary_min": to_decimal(salary.get("minimum")),
        "salary_max": to_decimal(salary.get("maximum")),
        "salary_type": clean_text(salary_type),
        "salary_is_hidden": bool(md.get("isHideSalary")),
        "vacancies": to_int(payload.get("numberOfVacancies")),
        "original_posting_date": to_date(md.get("originalPostingDate")),
        "new_posting_date": to_date(md.get("newPostingDate")),
        "expiry_date": to_date(md.get("expiryDate")),
        "source_code": clean_text(payload.get("sourceCode")),
    }
    skills = parse_skills(payload)
    row = dict(job)
    row.update({
        "views": views,
        "applications": applications,
        "repost_count": to_int(md.get("repostCount")) or 0,
        "edit_count": to_int(md.get("editCount")) or 0,
        "job_status": clean_text((payload.get("status") or {}).get("jobStatus")) if isinstance(payload.get("status"), dict) else None,
        "skills": skills,
        "attr_hash": attr_hash(job, skills),
    })
    return row
