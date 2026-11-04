"""Tab 6: which industries and employers are hiring, how pay compares across industries, and what they ask for."""
import plotly.express as px
import streamlit as st

from dashboard import queries
from dashboard.filters import Filters
from dashboard.tabs.salary import band_chart

NOTE = ("Industry is the employer's SSIC industry as reported on MyCareersFuture; postings without one are "
        "shown as 'Unknown industry'. These views respond to the occupation group and occupation filters and "
        "ignore the job category filter. Counts are posting-days over the window.")


def _industries(f: Filters, days: int):
    overview = queries.industry_overview(f)
    if overview.empty:
        st.info("No industries have postings with these filters.")
        return overview

    left, right = st.columns(2)
    chart = px.bar(overview.iloc[::-1], x="avg_open_postings", y="industry", orientation="h",
                   hover_data={"share": ":.1%", "applications_per_posting": ":.2f"},
                   labels={"avg_open_postings": "Average open postings per day", "industry": ""},
                   title=f"Postings by industry (last {f.window_days} days)")
    chart.update_layout(height=max(420, 24 * len(overview)))
    left.plotly_chart(chart, width="stretch")

    with right:
        movers = overview.dropna(subset=["recent_share", "prev_share"])
        if days < 14 or movers.empty:
            st.info("Week-over-week change in industry share needs 14 days of history.")
        else:
            movers = movers.assign(change=(movers["recent_share"] - movers["prev_share"]) * 100)
            movers = movers.reindex(movers["change"].abs().sort_values(ascending=False).index).head(12)
            movers = movers.sort_values("change")
            fig = px.bar(movers, x="change", y="industry", orientation="h", color="change",
                         color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
                         labels={"change": "Share of postings, percentage points", "industry": ""},
                         title="Biggest week-over-week share changes")
            fig.update_layout(coloraxis_showscale=False, height=max(420, 24 * len(movers)))
            st.plotly_chart(fig, width="stretch")
    return overview


def _industry_pay(min_n: int):
    pay = queries.industry_salaries(min_n)
    if pay.empty:
        st.info("No industries have enough salary data.")
        return
    st.plotly_chart(band_chart(pay, "industry", "Industry salary bands (latest day, ignores filters)"),
                    width="stretch")


def _industry_skills(f: Filters, overview):
    st.markdown("**Skills by industry**")
    heat = queries.industry_skill_heatmap(f)
    if heat.empty:
        st.info("No skill data for the largest industries.")
    else:
        matrix = heat.pivot(index="industry", columns="skill_label", values="share").fillna(0)
        matrix = matrix.loc[:, matrix.sum().sort_values(ascending=False).index]
        fig = px.imshow(matrix * 100, aspect="auto", color_continuous_scale="Blues",
                        labels={"color": "% of postings", "x": "", "y": ""},
                        title="Share of each industry's postings asking for a skill (latest day)")
        fig.update_layout(height=max(380, 34 * len(matrix) + 160), xaxis_tickangle=-40)
        st.plotly_chart(fig, width="stretch")

    options = [i for i in overview["industry"].tolist() if i != "Unknown industry"]
    if not options:
        return
    industry = st.selectbox("Industry detail", options, key="industry_pick")
    skills = queries.industry_top_skills(f, industry)
    if skills.empty:
        st.info("No skills are shared by at least 2 postings in this industry.")
        return
    fig = px.bar(skills.iloc[::-1], x="share", y="skill_label", orientation="h", color="skill_type",
                 labels={"share": "Share of the industry's postings", "skill_label": "", "skill_type": "Type"},
                 title=f"Top skills in {industry}")
    fig.update_xaxes(tickformat=".0%")
    st.plotly_chart(fig, width="stretch")


def _employers(f: Filters):
    st.markdown("**Top employers**")
    st.caption("Employers with the most open postings on the latest snapshot day (the hiring company where "
               "MyCareersFuture shows one, otherwise the poster).")
    top = queries.top_employers(f)
    if top.empty:
        st.info("No employers with these filters.")
        return
    held = top["postings"].sum() / top["total_postings"].iloc[0]
    a, b = st.columns(2)
    a.metric("Employers hiring", f"{int(top['employers'].iloc[0]):,}")
    b.metric(f"Share of postings held by the top {len(top)}", f"{held:.1%}")

    chart = px.bar(top.iloc[::-1], x="postings", y="employer", orientation="h", color="industry",
                   hover_data={"vacancies": True, "applications_per_posting": ":.2f"},
                   labels={"postings": "Open postings", "employer": "", "industry": "Industry"})
    chart.update_layout(height=max(420, 26 * len(top)), legend_orientation="h", legend_y=-0.15)
    st.plotly_chart(chart, width="stretch")

    employer = st.selectbox("Employer detail", top["employer"].tolist(), key="employer_pick")
    skills = queries.employer_top_skills(f, employer)
    if skills.empty:
        st.info("This employer has no skill asked for by 2 or more of its postings.")
        return
    fig = px.bar(skills.iloc[::-1], x="share", y="skill_label", orientation="h", color="skill_type",
                 labels={"share": "Share of the employer's postings", "skill_label": "", "skill_type": "Type"},
                 title=f"Top skills at {employer}")
    fig.update_xaxes(tickformat=".0%")
    st.plotly_chart(fig, width="stretch")


def render(f: Filters, days: int):
    st.subheader("Industries and employers")
    st.caption(NOTE)
    overview = _industries(f, days)
    st.markdown("**Industry pay**")
    min_n = st.slider("Minimum postings with a salary", 5, 200, 30, step=5, key="industry_min_n")
    _industry_pay(min_n)
    if not overview.empty:
        _industry_skills(f, overview)
    _employers(f)
