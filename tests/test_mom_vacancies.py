"""Offline tests for the MOM transform and the MCF-vs-MOM comparison (pure functions, no database)."""
from datetime import date

import pytest

from transform import mom_vacancies as mv
from transform.parse import mom_group


def test_parse_quarter():
    assert mv.parse_quarter("2026-Q2") == (2026, 2, date(2026, 4, 1))
    assert mv.parse_quarter("1998-Q4") == (1998, 4, date(1998, 10, 1))
    with pytest.raises(ValueError):
        mv.parse_quarter("2026Q2")


def test_parse_vacancy():
    assert mv.parse_vacancy("39300") == 39300
    assert mv.parse_vacancy("1,200") == 1200
    assert mv.parse_vacancy("-") is None and mv.parse_vacancy(None) is None


def test_reference_files_cover_every_group():
    labels = mv.read_label_map()
    ssoc = mv.read_ssoc_map()
    assert sorted(ssoc) == [str(d) for d in range(1, 10)]
    groups_a = {r["mom_group"] for r in labels.values() if r["split"] == "A"}
    groups_b = {r["mom_group"] for r in labels.values() if r["split"] == "B"}
    assert {m["A"] for m in ssoc.values()} == groups_a
    assert {m["B"] for m in ssoc.values()} == groups_b


def test_split_a_csv_agrees_with_dim_job_mom_group():
    """dim_job.mom_group comes from transform/parse.py; the CSV must give the same Split A group."""
    ssoc = mv.read_ssoc_map()
    for digit, groups in ssoc.items():
        assert mom_group(f"{digit}1234") == groups["A"]


def test_digit_to_group_unknowns():
    ssoc = mv.read_ssoc_map()
    assert mv.digit_to_group("6", "A", ssoc) == "production_transport"   # the agreed assumption
    assert mv.digit_to_group("3", "B", ssoc) == "non_PME"
    for bad in (None, "", "0", "X"):
        assert mv.digit_to_group(bad, "A", ssoc) == mv.UNKNOWN


def _api_row(quarter, label, vac):
    return {"_id": 1, "quarter": quarter, "occupation": label, "job_vacancy": vac}


def test_to_clean_rows_maps_labels_and_flags_pre_2006():
    rows, unmapped = mv.to_clean_rows([
        _api_row("2005-Q4", "clerical, sales and services workers", "8100"),
        _api_row("2026-Q2", "Professionals, managers and executives", "22800"),   # case differs
        _api_row("2026-Q2", "something new", "5"),
    ], mv.read_label_map())
    assert unmapped == ["something new"]
    by_group = {(r["quarter"], r["mom_group"]): r for r in rows}
    old = by_group[("2005-Q4", "clerical_sales_services")]
    assert old["split"] == "A" and old["job_vacancy"] == 8100 and old["covers_public_sector"] is False
    assert by_group[("2026-Q2", "PME")]["covers_public_sector"] is True


def test_to_clean_rows_dedupes_repeated_rows():
    label = "clerical, sales and services workers"
    rows, _ = mv.to_clean_rows([_api_row("2026-Q2", label, "1"), _api_row("2026-Q2", label, "2")], mv.read_label_map())
    assert len(rows) == 1 and rows[0]["job_vacancy"] == 2


def test_split_totals_mismatch():
    rows = [
        {"quarter": "2026-Q2", "split": "A", "job_vacancy": 69900},
        {"quarter": "2026-Q2", "split": "B", "job_vacancy": 69800},   # within rounding
        {"quarter": "2026-Q1", "split": "A", "job_vacancy": 70000},
        {"quarter": "2026-Q1", "split": "B", "job_vacancy": 60000},
        {"quarter": "2000-Q1", "split": "A", "job_vacancy": 1},       # no Split B before 2013: not checked
    ]
    assert mv.split_totals_mismatch(rows) == ["2026-Q1"]


def _titles():
    return {r["mom_group"]: (r["split"], r["group_title"]) for r in mv.read_label_map().values()}


def test_compare_shares():
    # MCF: 600 PMET (digits 1-3), 200 clerical (4-5), 200 production (6-9), 50 unknown.
    mcf = {"1": (200, 200), "2": (300, 300), "3": (100, 100), "4": (150, 150), "5": (50, 50),
           "8": (200, 400), None: (40, 40), "0": (10, 10)}
    mom = {"PMET": 39300, "clerical_sales_services": 12800, "production_transport": 17800,
           "PME": 22800, "non_PME": 47100}
    mom_1y = {"PMET": 39300 * 2}
    rows = mv.compare_shares(mcf, mv.read_ssoc_map(), _titles(), mom, mom_1y, {})
    got = {(r["split"], r["mom_group"]): r for r in rows}

    pmet = got[("A", "PMET")]
    assert pmet["mcf_postings"] == 600 and pmet["mcf_share_pct"] == 60.0       # 600 / 1000, unknown excluded
    assert pmet["mom_share_pct"] == 56.22                                        # 39300 / 69900
    assert pmet["share_gap_pp"] == 3.78 and pmet["disagrees"] is False
    assert pmet["mom_yoy_pct"] == -50.0 and pmet["mom_5y_pct"] is None
    assert got[("A", "production_transport")]["mcf_vacancy_share_pct"] == 33.33  # 400 / 1200 vacancies

    pme = got[("B", "PME")]                                                      # digits 1-2 only
    assert pme["mcf_share_pct"] == 50.0 and pme["mom_share_pct"] == 32.62
    assert pme["share_gap_pp"] == 17.38 and pme["disagrees"] is True

    for split in mv.SPLITS:
        unknown = got[(split, mv.UNKNOWN)]
        assert unknown["mcf_postings"] == 50 and unknown["mcf_share_pct"] is None and not unknown["disagrees"]
    # Shares within a split add up to 100.
    for split in mv.SPLITS:
        total = sum(r["mcf_share_pct"] for r in rows if r["split"] == split and r["mom_group"] != mv.UNKNOWN)
        assert total == pytest.approx(100, abs=0.05)


def test_compare_shares_threshold_is_inclusive():
    mcf = {"1": (70, 70), "4": (30, 30)}
    mom = {"PMET": 60, "clerical_sales_services": 40, "production_transport": 0, "PME": 1, "non_PME": 1}
    rows = mv.compare_shares(mcf, mv.read_ssoc_map(), _titles(), mom, {}, {}, threshold_pp=10)
    assert next(r for r in rows if r["mom_group"] == "PMET")["disagrees"] is True   # exactly 10 pp
