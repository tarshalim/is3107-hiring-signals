"""Singapore hiring-signals dashboard (Streamlit). Run: streamlit run dashboard/app.py"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st  # noqa: E402

from dashboard import queries, signal_queries  # noqa: E402
from dashboard.filters import (  # noqa: E402
    MOM_GROUP_LABELS, SKILL_TYPE_LABELS, SKILL_TYPES, WINDOW_OPTIONS, Filters,
)
from dashboard.signals import week_label  # noqa: E402
from dashboard.tabs import (  # noqa: E402
    difficulty, estimator, industries, market, pipeline, salary, signals, skill_lookup, transitions, trending,
)

st.set_page_config(page_title="SG hiring signals", page_icon=":bar_chart:", layout="wide")

WEEKLY_TABS = ("Hiring signals", "Skill lookup", "Market check")


def week_picker():
    """Sidebar week selector for the weekly signal tabs: (picked row of metrics.signal_weeks, error)."""
    st.sidebar.header("Weekly signals")
    try:
        weeks = signal_queries.weeks()
    except Exception as exc:  # no connection, or sql/init/10_dashboard_signals.sql not applied yet
        st.sidebar.caption("Weekly signals unavailable.")
        return None, exc
    if weeks.empty:
        st.sidebar.caption("No finished weekly_metrics run yet.")
        return None, None
    pick = st.sidebar.selectbox("Week", range(len(weeks)), format_func=lambda i: week_label(weeks.iloc[i]),
                                key="signal_week")
    week = weeks.iloc[pick]
    snap = week["snapshot_date"]
    st.sidebar.caption(f"Snapshot used: {snap.day} {snap:%b %Y}. Applies to {', '.join(WEEKLY_TABS)}. "
                       "The filters below apply to the daily tabs only.")
    return week, None


def no_weeks(error):
    if error is not None:
        st.error("Could not read the weekly signals (metrics.signal_weeks).")
        st.code(str(error))
        st.info("Apply `sql/init/10_dashboard_signals.sql` once (docs/dashboard-signals.md) and check the warehouse "
                "is running.")
    else:
        st.info("No weekly_metrics run has finished yet. Unpause the weekly_metrics DAG (docs/weekly-metrics.md).")


def no_marts(error):
    if error is not None:
        st.error("Could not read the dashboard marts from the warehouse.")
        st.code(str(error))
        st.info("Check the warehouse is running (`docker compose up -d warehouse`) and build the marts with "
                "`python -m transform.refresh_marts`.")
    else:
        st.info("The daily marts are empty. Run the transform DAG, then `python -m transform.refresh_marts`.")


def sidebar_filters(options: dict) -> Filters:
    st.sidebar.header("Filters")
    window = st.sidebar.radio("Window (days)", WINDOW_OPTIONS, index=WINDOW_OPTIONS.index(30), horizontal=True)

    groups = options["mom_groups"]["mom_group"].tolist()
    mom = st.sidebar.multiselect("Occupation group", groups, format_func=lambda g: MOM_GROUP_LABELS.get(g, g))

    majors = options["major_groups"]
    major_labels = dict(zip(majors["major_group"], majors["major_group_title"]))
    major = st.sidebar.multiselect("Occupation (SSOC major group)", list(major_labels),
                                   format_func=lambda m: f"{m}: {major_labels[m]}")

    industry = st.sidebar.multiselect("Job category (MyCareersFuture)", options["categories"]["category"].tolist())

    stype = st.sidebar.multiselect("Skill type", SKILL_TYPES, format_func=SKILL_TYPE_LABELS.get)
    emerging = st.sidebar.checkbox("Emerging skills only")
    st.sidebar.caption("Leave a filter empty to include everything.")
    return Filters(window_days=window, mom_groups=tuple(mom), major_groups=tuple(major),
                   categories=tuple(industry), skill_types=tuple(stype), emerging_only=emerging)


def main():
    st.title("Singapore hiring signals")
    week, week_error = week_picker()
    try:
        cov, marts_error = queries.coverage().iloc[0], None
    except Exception as exc:  # no connection or marts not built yet
        cov, marts_error = None, exc
    marts_ready = cov is not None and bool(cov["days"])

    if marts_ready:
        st.caption(f"MyCareersFuture postings, {cov['first_day']} to {cov['last_day']} ({int(cov['days'])} daily "
                   "snapshots). Counts are posting-days: a posting open for 5 days counts 5 times.")
        f = sidebar_filters(queries.filter_options())
        if int(cov["days"]) < 14:
            st.info(f"Only {int(cov['days'])} days of history: week-over-week changes need 14 days and "
                    "are hidden until then.")

    tabs = st.tabs([*WEEKLY_TABS, "Trending skills", "Salary", "Industries & employers", "Salary estimator",
                    "Hiring difficulty", "Career moves", "Pipeline health"])
    # the weekly tabs read only `metrics`, so they work even before the daily marts are built
    for tab, view in zip(tabs, (signals.render, skill_lookup.render, market.render)):
        with tab:
            if week is None:
                no_weeks(week_error)
                continue
            try:
                view(week)
            except Exception as exc:  # keep the other tabs usable if one weekly view fails
                st.error("This view failed to load.")
                st.exception(exc)
    tab_trend, tab_salary, tab_ind, tab_est, tab_diff, tab_move, tab_pipe = tabs[len(WEEKLY_TABS):]
    if not marts_ready:
        for tab in (tab_trend, tab_salary, tab_ind, tab_est, tab_diff, tab_move):
            with tab:
                no_marts(marts_error)
        with tab_pipe:
            if marts_error is None:
                pipeline.render()       # reads raw and clean too: shows why the marts are empty
            else:
                no_marts(marts_error)
        return
    with tab_trend:
        trending.render(f, int(cov["days"]))
    with tab_salary:
        salary.render(f)
    with tab_ind:
        industries.render(f, int(cov["days"]))
    with tab_est:
        estimator.render()
    with tab_diff:
        difficulty.render(f)
    with tab_move:
        transitions.render()
    with tab_pipe:
        pipeline.render()


main()
