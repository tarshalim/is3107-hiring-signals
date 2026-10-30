"""Tab 4: career moves. (a) from an occupation to similar ones and the skills to learn; (b) from the skills you have."""
import plotly.express as px
import streamlit as st

from dashboard import queries, transitions as logic


def _label(row) -> str:
    return f"{row['ssoc_title']} ({row['ssoc_code']})"


def render():
    st.subheader("Career moves")
    st.caption("Based on the skills asked for in postings open on the latest snapshot day. "
               "These are market signals, not a guarantee of fit. Occupations with fewer than 20 postings are left out.")
    occupations, skills = queries.occupation_skill_profiles()
    if occupations.empty or skills.empty:
        st.info("Not enough data yet.")
        return
    shares = logic.skill_shares(occupations, skills)
    labels = {r["ssoc_code"]: _label(r) for _, r in occupations.iterrows()}

    mode = st.radio("Start from", ["An occupation I know", "Skills I have"], horizontal=True)
    if mode == "An occupation I know":
        source = st.selectbox("Current occupation", list(labels), format_func=labels.get)
        similar = logic.similar_occupations(source, occupations, shares)
        if similar.empty:
            st.info("No comparable occupations.")
            return
        similar = similar.assign(similarity_pct=similar["similarity"] * 100)
        st.plotly_chart(px.bar(similar.iloc[::-1], x="similarity_pct", y="ssoc_title", orientation="h",
                               hover_data={"postings": True}, title="Occupations with the most similar skill demand",
                               labels={"similarity_pct": "Skill similarity (%)", "ssoc_title": ""}),
                        width="stretch")
        target = st.selectbox("Skill gap to", similar["ssoc_code"].tolist(), format_func=labels.get)
        gap = logic.skill_gap(source, target, shares)
        st.markdown("**Skills to learn** (large gap = common in the target, rare in your current occupation)")
        st.dataframe(gap.assign(target_share=(gap["target_share"] * 100).round(0),
                                source_share=(gap["source_share"] * 100).round(0),
                                gap=(gap["gap"] * 100).round(0))
                     .rename(columns={"skill_label": "Skill", "skill_type": "Type", "target_share": "% of target postings",
                                      "source_share": "% of your occupation's postings", "gap": "Gap (points)"}),
                     width="stretch", hide_index=True)
    else:
        catalogue = (shares.groupby(["skill_id", "skill_label"])["share"].sum().reset_index()
                     .sort_values("share", ascending=False))
        names = dict(zip(catalogue["skill_id"], catalogue["skill_label"]))
        owned = st.multiselect("Skills you have", list(names), format_func=names.get)
        ranked = logic.coverage_by_skills(owned, occupations, shares)
        if ranked.empty:
            st.info("Pick one or more skills to see matching occupations.")
            return
        top = ranked.head(15).assign(coverage_pct=lambda d: d["coverage"] * 100)
        st.plotly_chart(px.bar(top.iloc[::-1], x="coverage_pct", y="ssoc_title", orientation="h",
                               hover_data={"matched_skills": True, "postings": True},
                               title="Occupations your skills cover best",
                               labels={"coverage_pct": "% of the occupation's skill demand covered", "ssoc_title": ""}),
                        width="stretch")
        target = st.selectbox("Missing skills for", top["ssoc_code"].tolist(), format_func=labels.get)
        missing = logic.missing_skills(target, owned, shares)
        st.dataframe(missing.assign(share=(missing["share"] * 100).round(0))
                     .rename(columns={"skill_label": "Skill", "skill_type": "Type", "share": "% of postings"}),
                     width="stretch", hide_index=True)
