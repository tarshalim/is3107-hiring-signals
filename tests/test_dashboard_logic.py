"""Pure-pandas dashboard logic and filter handling (no database needed)."""
import pytest

pd = pytest.importorskip("pandas")

from dashboard import transitions as t  # noqa: E402
from dashboard.filters import Filters  # noqa: E402
from dashboard.scoring import difficulty_index  # noqa: E402


@pytest.fixture
def profiles():
    occupations = pd.DataFrame({
        "ssoc_code": ["A", "B", "C"],
        "ssoc_title": ["Data analyst", "Data engineer", "Receptionist"],
        "mom_group": ["PMET", "PMET", "clerical_sales_services"],
        "postings": [100, 50, 40],
    })
    skills = pd.DataFrame([
        ("A", "sql", "SQL", "tsc", 80), ("A", "py", "Python", "tsc", 50), ("A", "xl", "Excel", "tsc", 30),
        ("B", "sql", "SQL", "tsc", 45), ("B", "py", "Python", "tsc", 40), ("B", "aws", "AWS", "tsc", 25),
        ("C", "xl", "Excel", "tsc", 30), ("C", "com", "Communication", "ccs", 20),
    ], columns=["ssoc_code", "skill_id", "skill_label", "skill_type", "skill_postings"])
    return occupations, t.skill_shares(occupations, skills)


def test_filters_params_use_empty_lists_for_no_restriction():
    p = Filters().params()
    assert p == {"window": 30, "mom": [], "major": [], "cat": [], "stype": [], "emerging": False}
    p = Filters(window_days=7, mom_groups=("PMET",), major_groups=(2, 3), emerging_only=True).params()
    assert p["mom"] == ["PMET"] and p["major"] == [2, 3] and p["window"] == 7 and p["emerging"] is True


def test_filters_are_hashable_for_the_cache():
    assert hash(Filters(mom_groups=("PMET",))) == hash(Filters(mom_groups=("PMET",)))
    assert Filters(window_days=7) != Filters(window_days=30)


def test_skill_shares_are_fractions_of_the_occupation(profiles):
    _, shares = profiles
    row = shares[(shares["ssoc_code"] == "A") & (shares["skill_id"] == "sql")].iloc[0]
    assert row["share"] == pytest.approx(0.8)


def test_similar_occupations_rank_the_data_roles_together(profiles):
    occupations, shares = profiles
    similar = t.similar_occupations("A", occupations, shares)
    assert similar["ssoc_code"].tolist() == ["B", "C"]
    assert similar.iloc[0]["similarity"] > similar.iloc[1]["similarity"]
    assert "A" not in similar["ssoc_code"].tolist()


def test_similar_occupations_for_unknown_code_is_empty(profiles):
    occupations, shares = profiles
    assert t.similar_occupations("nope", occupations, shares).empty


def test_skill_gap_lists_target_skills_the_source_lacks(profiles):
    _, shares = profiles
    gap = t.skill_gap("A", "B", shares)
    assert gap.iloc[0]["skill_label"] == "AWS"                 # B asks for it (50%), A never does
    assert gap.iloc[0]["gap"] == pytest.approx(0.5)
    assert gap["gap"].is_monotonic_decreasing


def test_coverage_ranks_by_share_of_demand_covered(profiles):
    occupations, shares = profiles
    ranked = t.coverage_by_skills(["xl"], occupations, shares)
    assert ranked.iloc[0]["ssoc_code"] == "C"                  # Excel is most of the receptionist's demand
    assert ranked["coverage"].between(0, 1).all()
    assert t.coverage_by_skills([], occupations, shares).empty


def test_missing_skills_excludes_owned_ones(profiles):
    _, shares = profiles
    missing = t.missing_skills("B", ["sql"], shares)
    assert "SQL" not in missing["skill_label"].tolist()
    assert missing.iloc[0]["skill_label"] == "Python"


def test_difficulty_index_ranks_few_applications_many_reposts_long_open_as_hardest():
    frame = pd.DataFrame({
        "applications_per_posting": [0.5, 2.0, 5.0],
        "repost_rate": [0.20, 0.10, 0.02],
        "avg_days_open": [40, 20, 5],
    })
    scores = difficulty_index(frame)
    assert scores.is_monotonic_decreasing
    assert scores.between(0, 100).all()


def test_difficulty_index_skips_signals_with_no_data():
    frame = pd.DataFrame({"applications_per_posting": [1.0, 3.0], "repost_rate": [0.2, 0.1],
                          "avg_days_open": [float("nan"), float("nan")]})
    scores = difficulty_index(frame)
    assert scores.notna().all() and scores.iloc[0] != scores.iloc[1]
