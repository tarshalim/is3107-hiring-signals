"""Tab 2: monthly salary bands (P25 to P75, median marked) for skills, skill pairs and occupations."""
import plotly.graph_objects as go
import streamlit as st

from dashboard import queries
from dashboard.filters import Filters

NOTE = ("Monthly salary in SGD from postings that disclose a range (midpoint of min and max), for postings "
        "open on the latest snapshot day. The bar spans the 25th to 75th percentile; the marker is the median. "
        "Skills and pairs ignore the occupation and industry filters.")


def band_chart(frame, label_col: str, title: str):
    frame = frame.sort_values("salary_p50")
    fig = go.Figure()
    fig.add_trace(go.Bar(y=frame[label_col], x=frame["salary_p75"] - frame["salary_p25"], base=frame["salary_p25"],
                         orientation="h", marker_color="#9ecae1", name="P25 to P75",
                         customdata=frame[["salary_p25", "salary_p75"]],
                         hovertemplate="%{y}<br>P25 %{customdata[0]:,.0f} to P75 %{customdata[1]:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(y=frame[label_col], x=frame["salary_p50"], mode="markers",
                             marker=dict(color="#08519c", size=9), name="Median",
                             customdata=frame[["salary_postings"]],
                             hovertemplate="%{y}<br>Median %{x:,.0f}<br>Postings with salary: %{customdata[0]}<extra></extra>"))
    fig.update_layout(title=title, height=max(380, 26 * len(frame)), xaxis_title="SGD per month",
                      yaxis_title="", legend_orientation="h", legend_y=-0.12)
    return fig


def render(f: Filters):
    st.subheader("Salary bands")
    st.caption(NOTE)
    min_n = st.slider("Minimum postings with a salary", 5, 100, 20, step=5, key="salary_min_n")

    skills = queries.skill_salaries(f, min_n)
    pairs = queries.skill_pair_salaries(f, max(5, min_n // 2))
    occupations = queries.occupation_salaries(f, min_n)

    left, right = st.columns(2)
    with left:
        if skills.empty:
            st.info("No skills have enough salary data with these filters.")
        else:
            st.plotly_chart(band_chart(skills, "skill_label", "Highest-paying skills (by median)"),
                            width="stretch")
    with right:
        if pairs.empty:
            st.info("No skill pairs have enough salary data with these filters.")
        else:
            pairs = pairs.assign(combination=pairs["skill_a"] + " + " + pairs["skill_b"])
            st.plotly_chart(band_chart(pairs, "combination", "Highest-paying skill combinations"),
                            width="stretch")

    st.markdown("**By occupation**")
    if occupations.empty:
        st.info("No occupations have enough salary data with these filters.")
    else:
        st.plotly_chart(band_chart(occupations, "ssoc_title", "Highest-paying occupations (by median)"),
                        width="stretch")
