"""Tab 7: a salary estimator trained on the postings that disclose a salary (stretch goal in the proposal)."""
import math

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard import queries
from transform.salary_model import TARGET_ERROR, feature_kind

NOTE = ("A ridge regression on the log of the monthly salary midpoint, trained daily on postings that disclose "
        "a range. It uses the occupation, position level, job category, years of experience asked for and the "
        "skills listed. The coefficients describe how pay differs across postings; they are associations, "
        "not what a skill would add to one person's pay.")


def _names(model, kind: str) -> dict:
    """feature name -> label for the features of one kind, most common in training first."""
    names = [f for f in model.coefs if feature_kind(f) == kind]
    names.sort(key=lambda f: -model.support.get(f, 0))
    return {f: model.labels.get(f, f) for f in names}


def quality_cards(model):
    m = model.metrics
    st.markdown("**How accurate is it?**")
    ok = m["mape"] <= TARGET_ERROR
    a, b, c, d = st.columns(4)
    a.metric("Mean absolute % error", f"{m['mape']:.1%}",
             delta=f"proposal target {TARGET_ERROR:.0%}: {'met' if ok else 'missed'}",
             delta_color="normal" if ok else "inverse",
             help="Average of |estimate - advertised midpoint| / advertised midpoint on postings the model never saw.")
    b.metric("Within 15% of the midpoint", f"{m['within_15']:.0%}", help="Share of held-out postings estimated within 15%.")
    c.metric("Typical error (SGD)", f"{m['mae']:,.0f}", help="Mean absolute error per month on held-out postings.")
    d.metric("Simple baseline error", f"{m['baseline_mape']:.1%}",
             help="Predicting the median salary of the posting's occupation group.")
    st.caption(f"Trained on {m['n_train']:,} postings and checked on {m['n_test']:,} held-out postings "
               f"({m['n_features']:,} features), data as of {m['snapshot_date']}.")


def render():
    st.subheader("Salary estimator")
    st.caption(NOTE)
    model = queries.salary_model()
    if model is None:
        st.info("No salary model has been trained yet. It trains after the daily transform, or run "
                "`python -m transform.salary_model` once the warehouse holds enough postings with salaries.")
        return
    quality_cards(model)

    occupations = queries.estimator_occupations()
    levels = _names(model, "level")
    categories = _names(model, "category")
    skills = _names(model, "skill")
    if occupations.empty:
        st.info("No occupations to choose from yet.")
        return

    st.markdown("**Estimate a posting**")
    left, right = st.columns(2)
    codes = occupations["ssoc_code"].tolist()
    titles = dict(zip(occupations["ssoc_code"], occupations["ssoc_title"]))
    busiest = int(occupations["postings"].idxmax())
    occupation = left.selectbox("Occupation", codes, index=busiest, format_func=lambda c: titles[c], key="est_occ")
    level = left.selectbox("Position level", ["Not specified"] + list(levels.values()), key="est_level")
    category = right.selectbox("Job category", ["Not specified"] + list(categories.values()), key="est_cat")
    years = right.slider("Years of experience asked for", 0, 20, 3, key="est_years")
    chosen = st.multiselect("Skills listed", list(skills), format_func=skills.get, key="est_skills")

    row = {"ssoc_code": occupation, "min_years_exp": years,
           "skills": [f.split(":", 1)[1] for f in chosen]}
    if level != "Not specified":
        row["position_level"] = level
    if category != "Not specified":
        row["category"] = category
    result = model.predict(row)

    a, b = st.columns(2)
    a.metric("Estimated monthly salary midpoint", f"SGD {result['estimate']:,.0f}")
    b.metric("Likely range (80% of postings)", f"SGD {result['low']:,.0f} to {result['high']:,.0f}",
             help="From the spread of errors on held-out postings. Disclosed salaries are advertised ranges, "
                  "not what people are paid.")

    effects = {}
    for part in result["parts"]:
        kind = part["kind"]
        if kind in ("ssoc1", "ssoc3"):
            name = f"Occupation: {titles.get(occupation, occupation)}"
        elif kind == "level":
            name = f"Level: {part['label']}"
        elif kind == "category":
            name = f"Category: {part['label']}"
        elif kind == "numeric":
            name = f"Experience: {years} years"
        else:
            name = f"Skill: {part['label']}"
        effects[name] = (1 + effects.get(name, 0) / 100) * (1 + part["effect_pct"] / 100) * 100 - 100
    if effects:
        frame = pd.DataFrame({"part": list(effects), "effect": list(effects.values())})
        frame = frame.reindex(frame["effect"].abs().sort_values().index)
        fig = px.bar(frame, x="effect", y="part", orientation="h", color="effect",
                     color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
                     labels={"effect": "Effect on the estimate (%)", "part": ""},
                     title="What drives this estimate")
        fig.update_layout(coloraxis_showscale=False, height=max(260, 30 * len(frame) + 100))
        st.plotly_chart(fig, width="stretch")

    _suggestions(model, occupation, set(chosen), skills)


def _suggestions(model, occupation: str, chosen: set, skills: dict):
    _occupations, asked = queries.occupation_skill_profiles()
    asked = asked[asked["ssoc_code"] == occupation]
    rows = []
    for skill_id in asked["skill_id"]:
        feature = f"skill:{skill_id}"
        coef = model.coefs.get(feature)
        if coef is None or feature in chosen or model.support.get(feature, 0) < 50:
            continue
        rows.append({"Skill": skills.get(feature, feature), "Effect on estimate": math.exp(coef) - 1})
    if not rows:
        return
    rows.sort(key=lambda r: -r["Effect on estimate"])
    st.markdown("**Skills in this occupation associated with higher pay**")
    st.dataframe([{**r, "Effect on estimate": f"{r['Effect on estimate']:+.1%}"} for r in rows[:8]],
                 width="stretch", hide_index=True)
