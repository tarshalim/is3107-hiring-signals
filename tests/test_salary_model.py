"""Salary model: feature building, fitting on synthetic postings, and prediction (no database needed)."""
import math
import random

import pytest

np = pytest.importorskip("numpy")

from transform.salary_model import (  # noqa: E402
    MAX_YEARS, TARGET_ERROR, SalaryModel, feature_kind, fit, row_features, ssoc_parts,
)


def _rows(n, seed):
    """Salary = base by occupation x level x skill premium x 1.03 per year of experience, with +-5% noise."""
    rng = random.Random(seed)
    base = {"25122": 7000, "41101": 2500}
    levels = {"Junior": 0.8, "Senior": 1.3}
    rows = []
    for _ in range(n):
        ssoc = rng.choice(list(base))
        level = rng.choice(list(levels))
        years = rng.randint(0, 10)
        skills = [s for s in ("py", "sql", "excel") if rng.random() < 0.4]
        salary = (base[ssoc] * levels[level] * (1.25 if "py" in skills else 1.0) * 1.03 ** years
                  * rng.uniform(0.95, 1.05))
        rows.append({"ssoc_code": ssoc, "position_level": level, "category": "IT", "min_years_exp": years,
                     "skills": skills, "salary": salary})
    return rows


@pytest.fixture(scope="module")
def model():
    return fit(_rows(1500, 1), _rows(400, 2))


def test_ssoc_parts_handles_missing_and_non_numeric_codes():
    assert ssoc_parts("25122") == ("2", "251")
    assert ssoc_parts(None) == ("?", "?")
    assert ssoc_parts("?") == ("?", "?")
    assert ssoc_parts("AB123") == ("?", "?")


def test_row_features_covers_every_attribute_once():
    feats = dict(row_features({"ssoc_code": "25122", "position_level": "Senior", "category": "IT",
                               "min_years_exp": 5, "skills": ["b", "a", "a"]}))
    assert feats == {"ssoc1:2": 1.0, "ssoc3:251": 1.0, "level:Senior": 1.0, "category:IT": 1.0,
                     "years_exp": 0.5, "skill:a": 1.0, "skill:b": 1.0}


def test_row_features_skips_missing_values_and_caps_years():
    feats = dict(row_features({"ssoc_code": "25122", "min_years_exp": 99}))
    assert "level:None" not in feats and not any(f.startswith("category:") for f in feats)
    assert feats["years_exp"] == MAX_YEARS / 10
    assert dict(row_features({"ssoc_code": None, "min_years_exp": None}))["years_exp"] == 0.0


def test_feature_kind():
    assert feature_kind("skill:ssg-1") == "skill"
    assert feature_kind("years_exp") == "numeric"


def test_the_model_recovers_the_structure_of_the_data(model):
    m = model.metrics
    assert m["n_train"] == 1500 and m["n_test"] == 400
    assert m["mape"] < TARGET_ERROR
    assert m["mape"] < m["baseline_mape"]
    assert m["within_15"] > 0.9
    assert math.exp(model.coefs["skill:py"]) == pytest.approx(1.25, abs=0.05)
    assert model.coefs["level:Senior"] > model.coefs["level:Junior"]
    assert model.coefs["years_exp"] > 0


def test_predictions_follow_the_inputs(model):
    base = {"ssoc_code": "25122", "position_level": "Junior", "category": "IT", "min_years_exp": 2, "skills": []}
    low = model.predict(base)
    assert 0 < low["low"] < low["estimate"] < low["high"]
    assert low["estimate"] == pytest.approx(7000 * 0.8 * 1.03 ** 2, rel=0.08)
    assert model.predict({**base, "skills": ["py"]})["estimate"] > low["estimate"] * 1.15
    assert model.predict({**base, "position_level": "Senior"})["estimate"] > low["estimate"] * 1.4
    assert model.predict({**base, "ssoc_code": "41101"})["estimate"] < low["estimate"] / 2


def test_prediction_explains_its_parts_and_ignores_unknown_features(model):
    result = model.predict({"ssoc_code": "99999", "position_level": "Senior", "min_years_exp": 3,
                            "skills": ["never-seen"]})
    kinds = {p["kind"] for p in result["parts"]}
    assert "level" in kinds and "numeric" in kinds
    assert not any(p["feature"] == "skill:never-seen" for p in result["parts"])
    assert result["estimate"] > 0


def test_an_empty_row_predicts_from_the_intercept_alone():
    plain = SalaryModel(intercept=math.log(5000), coefs={})
    assert plain.predict({})["estimate"] == pytest.approx(5000)
