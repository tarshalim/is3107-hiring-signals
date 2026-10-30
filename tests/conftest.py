"""Shared test helpers: a realistic MCF payload built from the field map, with overrides."""
import copy

import pytest

BASE_PAYLOAD = {
    "uuid": "b3247c0d730be62bb3c2cc9e988f97cf",
    "sourceCode": "Employer Portal",
    "title": "  Staff Nurse   (Health Screening) ",
    "description": "<p>long html</p>",
    "minimumYearsExperience": 1,
    "skills": [
        {"skill": "Nursing", "uuid": "c1311fa3447790f02b8e9181846c2205", "confidence": None, "isKeySkill": False},
        {"skill": "Perform Ecg", "uuid": "da4391e36189c1ccfd049a3f0509e1f8", "confidence": None, "isKeySkill": True},
        {"skill": "Team Player", "uuid": "aaaa0000aaaa0000aaaa0000aaaa0000", "confidence": None, "isKeySkill": False},
    ],
    "ssocCode": "22200",
    "ssocVersion": "2020v3",
    "numberOfVacancies": 4,
    "categories": [{"id": 18, "category": "Healthcare / Pharmaceutical"}, {"id": 2, "category": "Others"}],
    "employmentTypes": [{"id": 2, "employmentType": "Temporary"}, {"id": 3, "employmentType": "Contract"}],
    "positionLevels": [{"id": 11, "position": "Non-executive"}],
    "status": {"id": 102, "jobStatus": "Open"},
    "ssecEqa": "62",
    "postedCompany": {
        "uen": "201841218K", "name": "HRNET VENTURES PTE. LTD.", "ssicCode2020": "78104",
        "ssicDescription2020": "EMPLOYMENT AGENCIES", "responsiveEmployer": {"isResponsive": True},
    },
    "hiringCompany": None,
    "salary": {"maximum": 4200, "minimum": 3500, "type": {"id": 4, "salaryType": "Monthly"}},
    "metadata": {
        "jobPostId": "MCF-2026-1888705",
        "deletedAt": None,
        "createdAt": "2026-10-24T09:02:03.000Z",
        "updatedAt": "2026-10-24T09:02:03.000Z",
        "editCount": 0,
        "repostCount": 0,
        "totalNumberOfView": 120,
        "newPostingDate": "2026-10-24",
        "originalPostingDate": "2026-10-24",
        "expiryDate": "2026-11-07",
        "totalNumberJobApplication": 7,
        "isPostedOnBehalf": False,
        "isHideSalary": False,
    },
}


def _make_payload(**overrides) -> dict:
    """Deep copy of the base payload; dotted keys reach into nested dicts, e.g. metadata.editCount=1."""
    payload = copy.deepcopy(BASE_PAYLOAD)
    for key, value in overrides.items():
        parts = key.split(".")
        target = payload
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = value
    return payload


# Exposed as a fixture (not imported) because a third-party `tests` package on the laptop shadows
# `tests.conftest` imports; fixtures are injected by name so they are immune to that.
@pytest.fixture
def make_payload():
    return _make_payload


@pytest.fixture
def payload():
    return _make_payload()
