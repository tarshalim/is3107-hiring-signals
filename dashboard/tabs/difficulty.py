"""Tab 3: which occupations and skills are hard to hire for (few applications, many reposts, long open)."""
import plotly.express as px
import streamlit as st

from dashboard import queries
from dashboard.filters import Filters
from dashboard.scoring import difficulty_index

NOTE = ("A relative signal built from MyCareersFuture activity, not a measured hiring outcome: an occupation scores "
        "higher when its postings get fewer applications, are reposted more often and stay open longer than "
        "other occupations on screen. The industry filter does not apply to occupations.")


def render(f: Filters):
    st.subheader("Hiring difficulty")
    st.caption(NOTE)

    daily = queries.market_by_day(f)
    if not daily.empty:
        left, right = st.columns(2)
        left.plotly_chart(px.line(daily, x="snapshot_date", y="applications_per_posting", markers=True,
                                  title="Applications per posting per day",
                                  labels={"snapshot_date": "", "applications_per_posting": ""}),
                          width="stretch")
        right.plotly_chart(px.line(daily.assign(repost_pct=daily["repost_rate"] * 100), x="snapshot_date",
                                   y="repost_pct", markers=True, title="Postings reposted (% of open postings)",
                                   labels={"snapshot_date": "", "repost_pct": ""}),
                           width="stretch")

    occupations = queries.occupation_difficulty(f)
    if occupations.empty:
        st.info("No occupations have enough postings with these filters.")
        return
    occupations = occupations.assign(difficulty=difficulty_index(occupations))
    hardest = occupations.sort_values("difficulty", ascending=False).head(25)
    chart = px.bar(hardest.iloc[::-1], x="difficulty", y="ssoc_title", orientation="h",
                   hover_data={"applications_per_posting": ":.2f", "repost_rate": ":.1%",
                               "avg_days_open": ":.0f", "avg_open_postings": ":.0f"},
                   labels={"difficulty": "Difficulty index (0 to 100)", "ssoc_title": ""},
                   title="Hardest occupations to hire for")
    chart.update_layout(height=max(420, 26 * len(hardest)))
    st.plotly_chart(chart, width="stretch")

    scatter = px.scatter(occupations, x="applications_per_posting", y="avg_days_open", size="avg_open_postings",
                         color="mom_group", hover_name="ssoc_title",
                         labels={"applications_per_posting": "Applications per posting per day",
                                 "avg_days_open": "Average days open", "mom_group": "Group"},
                         title="Applications vs time open (bubble = open postings)")
    st.plotly_chart(scatter, width="stretch")

    st.markdown("**Skills in the hardest-to-fill demand**")
    skills = queries.skill_difficulty(f)
    if not skills.empty:
        skills = skills.assign(difficulty=difficulty_index(skills.assign(avg_days_open=float("nan"))))
        st.dataframe(skills.sort_values("difficulty", ascending=False)
                     .rename(columns={"skill_label": "Skill", "skill_type": "Type", "posting_days": "Posting-days",
                                      "applications_per_posting": "Applications / posting / day",
                                      "repost_rate": "Repost rate", "difficulty": "Difficulty index"}),
                     width="stretch", hide_index=True)
