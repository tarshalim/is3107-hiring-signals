"""Offline tests for ingestion/datagov_wages.py: the two MOM CSV layouts, SSOC title matching, and the API download."""
from decimal import Decimal

import pytest

from ingestion import datagov_wages as datagov
from ingestion.datagov_wages import occupation_key, parse_lfs_income, parse_ows, ssoc_title_lookup, to_money

SSOC_ROWS = [  # (code, title, major group, major title, mom group), as transform.load_reference.read_ssoc returns
    ("24111", "Accountant (excluding tax accountant)", 2, "Professionals", "PMET"),
    ("24112", "Tax accountant", 2, "Professionals", "PMET"),
    ("41310", "Typists and word processing operators", 4, "Clerical", "clerical_sales_services"),
    ("99991", "Same title", 9, "x", "production_transport"),
    ("99992", "Same title", 9, "x", "production_transport"),
]
OWS_ROWS = [
    {"year": "2024", "occ_desc": "accountant (excluding tax accountant)", "mthly_gross_wage_75_pctile": "6695",
     "mthly_gross_wage_50_pctile": "5498", "mthly_gross_wage_25_pctile": "4600", "mthly_basic_wage_75_pctile": "6620",
     "mthly_basic_wage_50_pctile": "5434", "mthly_basic_wage_25_pctile": "4552"},
    {"year": "2024", "occ_desc": "typist and word processing operator", "mthly_gross_wage_75_pctile": "na",
     "mthly_gross_wage_50_pctile": "2,600", "mthly_gross_wage_25_pctile": "", "mthly_basic_wage_75_pctile": "2700",
     "mthly_basic_wage_50_pctile": "2500", "mthly_basic_wage_25_pctile": "2300"},
    {"year": "2024", "occ_desc": "same title", "mthly_gross_wage_50_pctile": "2000"},
]
LFS_ROWS = [
    {"DataSeries": "Professionals - Male", "2024": "9156", "2019": "7805", "2006": "na"},
    {"DataSeries": "Cleaners, Labourers & Related Workers - Female", "2024": "2051", "2019": "1521", "2006": ""},
]


def test_occupation_key_keeps_parentheses_and_drops_plurals():
    assert occupation_key("Typists and word processing operators") == occupation_key("typist and word processing operator")
    assert occupation_key("Accountant (excluding tax accountant)") != occupation_key("Tax accountant")
    assert occupation_key("Gardeners & horticultural workers") == "gardener and horticultural worker"


def test_ssoc_lookup_drops_ambiguous_titles():
    lookup = ssoc_title_lookup(SSOC_ROWS)
    assert lookup[occupation_key("Tax accountant")] == "24112"
    assert occupation_key("Same title") not in lookup


def test_parse_ows_matches_ssoc_and_reads_wages():
    rows = parse_ows(OWS_ROWS, ssoc_title_lookup(SSOC_ROWS))
    acc, typist, same = rows
    assert acc["ssoc_code"] == "24111" and acc["ssoc_major_group"] == 2
    assert acc["gross_wage_p50"] == Decimal("5498") and acc["basic_wage_p25"] == Decimal("4552")
    assert typist["ssoc_code"] == "41310"                                   # plural on one side only
    assert typist["gross_wage_p50"] == Decimal("2600") and typist["gross_wage_p75"] is None
    assert same["ssoc_code"] is None and same["ssoc_major_group"] is None   # ambiguous title is not guessed


def test_parse_lfs_income_goes_long_and_skips_missing_years():
    rows = parse_lfs_income(LFS_ROWS)
    assert len(rows) == 4                                                   # 2006 missing in both series
    by = {(r["ssoc_major_group"], r["sex"], r["year"]): r["median_income"] for r in rows}
    assert by[(2, "male", 2024)] == Decimal("9156")
    assert by[(9, "female", 2019)] == Decimal("1521")


def test_parse_lfs_income_fails_loudly_on_an_unknown_series():
    with pytest.raises(ValueError, match="unexpected"):
        parse_lfs_income([{"DataSeries": "Astronauts - Male", "2024": "1"}])


def test_to_money():
    assert to_money("1,234") == Decimal("1234") and to_money("na") is None and to_money(None) is None


class FakeResp:
    def __init__(self, body=None, content=b""):
        self.body, self.content = body, content

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class FakeSession:
    """initiate-download, then poll-download answers 'not ready' once, then a signed URL."""

    def __init__(self):
        self.calls = []
        self.polls = 0

    def get(self, url, timeout):
        self.calls.append(url)
        if url.endswith("/initiate-download"):
            return FakeResp({"code": 0})
        if url.endswith("/poll-download"):
            self.polls += 1
            return FakeResp({"data": {"status": "PENDING"}} if self.polls == 1 else {"data": {"url": "https://signed/file"}})
        return FakeResp(content=b"year,occ_desc\n2024,actuary\n")


def test_download_polls_until_the_url_is_ready(tmp_path):
    session = FakeSession()
    path = datagov.download_dataset("d_test", tmp_path, session=session, pause=0)
    assert path.read_bytes() == b"year,occ_desc\n2024,actuary\n"
    assert session.polls == 2 and session.calls[0].endswith("d_test/initiate-download")
    assert session.calls[-1] == "https://signed/file"
