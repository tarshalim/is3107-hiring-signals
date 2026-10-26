"""Offline tests for transform/parse.py: field paths, cleaning rules, edge cases."""
from datetime import date
from decimal import Decimal

import pytest

from transform.parse import (JOB_COLUMNS, RejectRow, cap_experience, mom_group, parse_posting,
                             to_date, to_ssoc)


def test_parses_the_documented_json_paths(payload):
    row = parse_posting(payload)
    assert row["job_uuid"] == "b3247c0d730be62bb3c2cc9e988f97cf"
    assert row["job_post_id"] == "MCF-2026-1888705"
    assert row["title"] == "Staff Nurse (Health Screening)"          # trimmed, inner spaces collapsed
    assert row["company_uen"] == "201841218K"
    assert row["company_ssic_code"] == "78104"
    assert row["ssoc_code"] == "22200" and isinstance(row["ssoc_code"], str)
    assert row["ssoc_major_group"] == 2 and row["mom_group"] == "PMET"
    assert row["position_level"] == "Non-executive"
    assert row["employment_types"] == "Temporary|Contract"
    assert row["category"] == "Healthcare / Pharmaceutical"
    assert row["categories"] == "Healthcare / Pharmaceutical|Others"
    assert row["education_code"] == "62"
    assert row["salary_min"] == Decimal("3500") and row["salary_max"] == Decimal("4200")
    assert row["salary_type"] == "Monthly"
    assert row["vacancies"] == 4
    assert row["new_posting_date"] == date(2026, 10, 24)
    assert row["expiry_date"] == date(2026, 11, 7)
    assert row["views"] == 120 and row["applications"] == 7
    assert row["repost_count"] == 0 and row["edit_count"] == 0
    assert row["job_status"] == "Open"
    assert [s[1] for s in row["skills"]] == ["Team Player", "Nursing", "Perform Ecg"]  # sorted by uuid
    assert any(s[2] for s in row["skills"])                                           # key skill kept
    assert all(c in row for c in JOB_COLUMNS)


def test_experience_88_years_is_capped_and_flagged(make_payload):
    row = parse_posting(make_payload(minimumYearsExperience=88))
    assert row["min_years_exp_raw"] == 88
    assert row["min_years_exp"] == 40
    assert row["exp_out_of_range"] is True


def test_experience_in_range_is_not_flagged(make_payload):
    row = parse_posting(make_payload(minimumYearsExperience=5))
    assert row["min_years_exp"] == 5 and row["exp_out_of_range"] is False
    assert cap_experience(None) == (None, False)
    assert cap_experience(-1) == (0, True)


def test_missing_salary_becomes_null_not_error(make_payload):
    row = parse_posting(make_payload(salary=None))
    assert row["salary_min"] is None and row["salary_max"] is None and row["salary_type"] is None
    row = parse_posting(make_payload(**{"salary.minimum": None}))
    assert row["salary_min"] is None and row["salary_max"] == Decimal("4200")


def test_bad_dates_and_codes_become_null(make_payload):
    row = parse_posting(make_payload(**{"metadata.expiryDate": "not a date", "ssocCode": "2220"}))
    assert row["expiry_date"] is None
    assert row["ssoc_code"] is None and row["ssoc_major_group"] is None and row["mom_group"] is None
    assert to_date("2026-10-24T09:02:03.000Z") == date(2026, 10, 24)
    assert to_ssoc(" 01234 ") == "01234"
    assert mom_group("51234") == "clerical_sales_services"
    assert mom_group("91234") == "production_transport"


def test_two_position_levels_keep_both(make_payload):
    row = parse_posting(make_payload(positionLevels=[{"position": "Non-executive"}, {"position": "Fresh/entry level"}]))
    assert row["position_level"] == "Non-executive"
    assert row["position_levels"] == "Non-executive|Fresh/entry level"


def test_agency_posting_keeps_hiring_company(make_payload):
    row = parse_posting(make_payload(**{"metadata.isPostedOnBehalf": True},
                                     hiringCompany={"uen": "199602299Z", "name": "KUNYI (SINGAPORE) PTE LTD"}))
    assert row["is_posted_on_behalf"] is True
    assert row["hiring_company_name"] == "KUNYI (SINGAPORE) PTE LTD"


def test_reject_when_no_uuid_or_counters(make_payload):
    with pytest.raises(RejectRow, match="uuid"):
        parse_posting(make_payload(uuid="  "))
    with pytest.raises(RejectRow, match="counter"):
        parse_posting(make_payload(**{"metadata.totalNumberOfView": None}))
    with pytest.raises(RejectRow):
        parse_posting("not a dict")


def test_attr_hash_is_stable_and_ignores_counters(make_payload):
    a = parse_posting(make_payload())
    b = parse_posting(make_payload(**{"metadata.totalNumberOfView": 999, "metadata.totalNumberJobApplication": 50}))
    assert a["attr_hash"] == b["attr_hash"]           # daily counters are facts, not attributes


def test_attr_hash_changes_on_edit_repost_or_skill_change(make_payload):
    base = parse_posting(make_payload())["attr_hash"]
    assert parse_posting(make_payload(**{"salary.maximum": 5000}))["attr_hash"] != base
    assert parse_posting(make_payload(**{"metadata.newPostingDate": "2026-10-30"}))["attr_hash"] != base
    assert parse_posting(make_payload(skills=[{"skill": "Nursing", "uuid": "c1311fa3447790f02b8e9181846c2205", "isKeySkill": False}]))["attr_hash"] != base
    assert parse_posting(make_payload(title="Staff Nurse (Health Screening)"))["attr_hash"] == base  # whitespace only
