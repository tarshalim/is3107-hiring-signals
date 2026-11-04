"""Offline tests for ingestion/kaggle_mcf2019.py: the 2019 crawl's text formats and the cleaning rules.

The records below copy the shapes found in the real file (made-up companies and values), including the
recruiter rows whose lists are stored as Python text and whose job id is 'Recruiter'.
"""
import json
import zipfile
from datetime import date
from decimal import Decimal

import pytest

from ingestion.kaggle_mcf2019 import (RejectRow, build_title_ssoc_lookup, parse_all, parse_categories, parse_experience,
                                parse_job, parse_list, parse_salary, read_jobs, skill_rows, tag_key, title_key)
from transform.skill_mapping import SkillMatcher, SkillRef

EMPLOYER_ROW = {
    "company_name": "EXAMPLE TECH PTE. LTD.", "job_title": "Admin Assistant [$1.5k-$2.2k  /  Bukit Batok  /  Immediate]",
    "employment_type": "Permanent, Full Time", "seniority": "Non-executive, Fresh/entry level",
    "job_category": ["Admin ", " Secretarial"], "location": "1 EXAMPLE ROAD 000001",
    "salary": "$1,800to$2,200Monthly", "min_experience": "1 year  exp",
    "skills_required": ["Microsoft Excel", "Team Player", "Microsoft Excel ", "Data Entry"],
    "requirements_and_role": "text", "job_requirements": "text", "company_info": "text",
    "posting_date": "09 May 2019", "expiry_date": "08 Jun 2019", "no_of_applications": "7",
    "job_id": "JOB-2019-0098255",
}
RECRUITER_ROW = {
    "company_name": "EXAMPLE SEARCH", "job_title": "IT Project Engineer (up to S$3500, MNC)",
    "employment_type": "Full Time", "seniority": "Senior Executive",
    "job_category": "['Information Technology, Public ', ' Civil Service']", "location": "not_available",
    "salary": "$80,000to$96,000Annually", "min_experience": "not_available",
    "skills_required": "['AutoCAD', 'Civil Engineering']", "requirements_and_role": "text",
    "job_requirements": "text", "company_info": "not_available", "posting_date": "08 May 2019",
    "expiry_date": "07 Jun 2019", "no_of_applications": "0", "job_id": "Recruiter",
}
EMPTY_ROW = {k: "not_available" for k in EMPLOYER_ROW}


def test_employer_row_is_parsed_and_cleaned():
    r = parse_job(EMPLOYER_ROW, 0)
    assert r["posting_key"] == "JOB-2019-0098255" and r["job_id"] == "JOB-2019-0098255"
    assert r["is_recruiter_post"] is False
    assert r["title"] == "Admin Assistant [$1.5k-$2.2k / Bukit Batok / Immediate]"   # inner spaces collapsed
    assert r["title_key"] == "admin assistant"                                        # [extras] removed
    assert r["categories"] == "Admin / Secretarial"                                   # scraper's split undone
    assert r["employment_types"] == "Permanent|Full Time"
    assert r["position_level"] == "Non-executive" and r["position_levels"] == "Non-executive|Fresh/entry level"
    assert r["salary_disclosed"] is True and r["salary_period"] == "Monthly"
    assert r["salary_monthly_min"] == Decimal("1800.00") and r["salary_monthly_max"] == Decimal("2200.00")
    assert r["min_years_exp"] == 1 and r["applications"] == 7
    assert r["posting_date"] == date(2019, 5, 9) and r["expiry_date"] == date(2019, 6, 8)
    assert [k for k, _ in r["skills"]] == ["microsoft excel", "team player", "data entry"]   # duplicate tag dropped
    assert r["n_skills"] == 3


def test_recruiter_row_uses_a_record_hash_and_parses_list_text():
    r = parse_job(RECRUITER_ROW, 5)
    assert r["job_id"] is None and r["is_recruiter_post"] is True
    assert r["posting_key"].startswith("h") and len(r["posting_key"]) == 21
    assert r["posting_key"] == parse_job(dict(RECRUITER_ROW), 9)["posting_key"]       # same record, same key
    assert r["categories"] == "Information Technology|Public / Civil Service"
    assert [k for k, _ in r["skills"]] == ["autocad", "civil engineering"]
    assert r["salary_period"] == "Annually"
    assert r["salary_monthly_min"] == Decimal("6666.67") and r["salary_monthly_max"] == Decimal("8000.00")
    assert r["min_years_exp"] is None and r["company_name"] == "EXAMPLE SEARCH"


@pytest.mark.parametrize("text, expected", [
    ("$1,800to$2,200Monthly", (True, "Monthly", Decimal("1800"), Decimal("2200"))),
    ("Up to$8,000Monthly", (True, "Monthly", None, Decimal("8000"))),
    ("Salary undisclosed", (False, None, None, None)),
    ("not_available", (False, None, None, None)),
])
def test_salary_formats(text, expected):
    s = parse_salary(text)
    assert (s["disclosed"], s["period"], s["min"], s["max"]) == expected


def test_small_parsers():
    assert parse_experience("10 years  exp") == 10 and parse_experience("not_available") is None
    assert parse_list("not_available") == [] and parse_list("['a', 'b']") == ["a", "b"]
    assert parse_list("__import__('os')") == ["__import__('os')"]          # text, never evaluated
    assert parse_categories(["Accounting ", " Auditing ", " Taxation, Banking and Finance"]) == \
        ["Accounting / Auditing / Taxation", "Banking and Finance"]
    assert tag_key("  Microsoft   Excel ") == "microsoft excel" and tag_key(" ") is None
    assert title_key("Senior Consultant (SAP HCM)") == "senior consultant"


def test_empty_rows_are_rejected_and_duplicates_dropped():
    with pytest.raises(RejectRow):
        parse_job(EMPTY_ROW, 0)
    rows, stats = parse_all([EMPLOYER_ROW, EMPTY_ROW, dict(EMPLOYER_ROW), RECRUITER_ROW])
    assert stats == {"records": 4, "rejected": 1, "duplicates_dropped": 1, "loaded": 2}
    assert [r["source_row"] for r in rows] == [0, 3]                          # first copy kept


def test_read_jobs_from_zip_or_json(tmp_path):
    body = {"jobs": [EMPLOYER_ROW]}
    (tmp_path / "mycareersfuture.json").write_text(json.dumps(body), encoding="utf-8")
    with zipfile.ZipFile(tmp_path / "archive.zip", "w") as zf:
        zf.writestr("mycareersfuture.json", json.dumps(body))
    assert read_jobs(tmp_path / "mycareersfuture.json") == read_jobs(tmp_path / "archive.zip") == [EMPLOYER_ROW]


def test_skill_rows_use_the_same_matcher_as_the_daily_transform():
    skills = {"s1": SkillRef("s1", "Data Entry", "tsc", False, False)}
    matcher = SkillMatcher(skills, {"team player": ("s1", 0.5)})
    rows, _ = parse_all([EMPLOYER_ROW])
    out = {r[0]: r for r in skill_rows(rows, matcher)}
    assert out["data entry"][5] == "exact" and out["team player"][5] == "alias"
    assert out["microsoft excel"][5] == "unmatched"
    assert all(r[5] == "unmatched" for r in skill_rows(rows, None))


def test_title_to_ssoc_lookup_needs_support_and_a_dominant_code():
    pairs = [("Admin Assistant", "41101")] * 4 + [("Admin Assistant (Temp)", "41102")] \
        + [("Manager", "12111"), ("Manager", "13299"), ("Manager", "14111")] + [("Rare Title", "21111")]
    lookup = build_title_ssoc_lookup(pairs)
    assert lookup["admin assistant"] == ("41101", 0.8)        # 4 of 5 share one code
    assert "manager" not in lookup                             # no dominant code
    assert "rare title" not in lookup                          # too few postings
