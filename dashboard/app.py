"""Singapore hiring-signals dashboard (Streamlit). Run: streamlit run dashboard/app.py"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st  # noqa: E402

from dashboard import queries  # noqa: E402
from dashboard.filters import (  # noqa: E402
    MOM_GROUP_LABELS, SKILL_TYPE_LABELS, SKILL_TYPES, WINDOW_OPTIONS, Filters,
)
from dashboard.tabs import difficulty, pipeline, salary, transitions, trending  # noqa: E402

st.set_page_config(page_title="SG hiring signals", page_icon=":bar_chart:", layout="wide")


def sidebar_filters(options: dict) -> Filters:
    st.sidebar.header("Filters")
    window = st.sidebar.radio("Window (days)", WINDOW_OPTIONS, index=WINDOW_OPTIONS.index(30), horizontal=True)

    groups = options["mom_groups"]["mom_group"].tolist()
    mom = st.sidebar.multiselect("Occupation group", groups, format_func=lambda g: MOM_GROUP_LABELS.get(g, g))

    majors = options["major_groups"]
    major_labels = dict(zip(majors["major_group"], majors["major_group_title"]))
    major = st.sidebar.multiselect("Occupation (SSOC major group)", list(major_labels),
                                   format_func=lambda m: f"{m}: {major_labels[m]}")

    industry = st.sidebar.multiselect("Job category (industry)", options["categories"]["category"].tolist())

    stype = st.sidebar.multiselect("Skill type", SKILL_TYPES, format_func=SKILL_TYPE_LABELS.get)
    emerging = st.sidebar.checkbox("Emerging skills only")
    st.sidebar.caption("Leave a filter empty to include everything.")
    return Filters(window_days=window, mom_groups=tuple(mom), major_groups=tuple(major),
                   categories=tuple(industry), skill_types=tuple(stype), emerging_only=emerging)


def main():
    st.title("Singapore hiring signals")
    try:
        cov = queries.coverage().iloc[0]
    except Exception as exc:  # no connection or marts not built yet
        st.error("Could not read the dashboard marts from the warehouse.")
        st.code(str(exc))
        st.info("Check the warehouse is running (`docker compose up -d warehouse`) and build the marts with "
                "`python -m transform.refresh_marts`.")
        return
    if not cov["days"]:
        st.warning("The marts are empty. Run the transform DAG, then `python -m transform.refresh_marts`.")
        return

    st.caption(f"MyCareersFuture postings, {cov['first_day']} to {cov['last_day']} ({int(cov['days'])} daily snapshots). "
               "Counts are posting-days: a posting open for 5 days counts 5 times.")

    f = sidebar_filters(queries.filter_options())
    if int(cov["days"]) < 14:
        st.info(f"Only {int(cov['days'])} days of history: week-over-week changes need 14 days and "
                "are hidden until then.")

    tab_trend, tab_salary, tab_diff, tab_move, tab_pipe = st.tabs(
        ["Trending skills", "Salary", "Hiring difficulty", "Career moves", "Pipeline health"])
    with tab_trend:
        trending.render(f, int(cov["days"]))
    with tab_salary:
        salary.render(f)
    with tab_diff:
        difficulty.render(f)
    with tab_move:
        transitions.render()
    with tab_pipe:
        pipeline.render()


main()
