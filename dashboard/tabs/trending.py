"""Tab 1: top-20 trending skills over the window, share of postings and week-over-week change."""
import plotly.express as px
import streamlit as st

from dashboard import queries
from dashboard.filters import SKILL_TYPE_LABELS, Filters


def render(f: Filters, days_of_history: int):
    st.subheader(f"Top 20 skills, last {f.window_days} days")
    st.caption("Share = percentage of postings (in the filtered slice) that list the skill. "
               "Change compares the last 7 days with the 7 days before, on share, so it is not distorted "
               "by the total number of postings changing.")
    top = queries.trending_skills(f)
    if top.empty:
        st.info("No skills match these filters. Widen the filters or the window.")
        return

    top = top.assign(share_pct=top["share"] * 100,
                     type_label=top["skill_type"].map(SKILL_TYPE_LABELS))
    chart = px.bar(top.iloc[::-1], x="share_pct", y="skill_label", color="type_label", orientation="h",
                   labels={"share_pct": "% of postings", "skill_label": "", "type_label": "Skill type"},
                   hover_data={"mentions": True, "share_pct": ":.1f"})
    chart.update_layout(height=max(420, 28 * len(top)), legend_orientation="h", legend_y=-0.15)
    left, right = st.columns([3, 2])
    left.plotly_chart(chart, width="stretch")

    with right:
        st.markdown("**Week-over-week change**")
        if days_of_history < 14:
            st.caption("Needs 14 days of history.")
        else:
            movers = top.dropna(subset=["wow_pct"]).sort_values("wow_pct")
            if movers.empty:
                st.caption("Not enough postings in the previous week to compare.")
            else:
                shown = movers.assign(change=movers["wow_pct"].round(1))[["skill_label", "change"]]
                bars = px.bar(shown, x="change", y="skill_label", orientation="h",
                              color="change", color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
                              labels={"change": "% change in share", "skill_label": ""})
                bars.update_layout(height=max(420, 28 * len(shown)), coloraxis_showscale=False)
                st.plotly_chart(bars, width="stretch")

    st.markdown("**Daily share of the top skills**")
    default = top["skill_label"].head(5).tolist()
    picked = st.multiselect("Skills to plot", top["skill_label"].tolist(), default=default)
    if picked:
        ids = tuple(top.loc[top["skill_label"].isin(picked), "skill_id"])
        daily = queries.skill_share_by_day(f, ids)
        if not daily.empty:
            daily = daily.assign(share_pct=daily["share"] * 100)
            line = px.line(daily, x="snapshot_date", y="share_pct", color="skill_label", markers=True,
                           labels={"share_pct": "% of postings", "snapshot_date": "", "skill_label": "Skill"})
            st.plotly_chart(line, width="stretch")

    with st.expander("Table"):
        st.dataframe(top[["skill_label", "type_label", "is_emerging", "mentions", "share_pct", "wow_pct"]]
                     .rename(columns={"skill_label": "Skill", "type_label": "Type", "is_emerging": "Emerging",
                                      "mentions": "Posting-days", "share_pct": "% of postings",
                                      "wow_pct": "WoW change %"}),
                     width="stretch", hide_index=True)
