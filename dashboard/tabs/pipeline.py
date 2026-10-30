"""Tab 5: is the pipeline healthy and is the skill extraction any good (the proposal's two reliability goals)."""
import plotly.express as px
import streamlit as st

from dashboard import queries

STATUS_ICON = {"pass": "âœ…", "warn": "âš ï¸", "fail": "âŒ"}


def render():
    st.subheader("Pipeline health")

    fresh = queries.freshness().iloc[0]
    c1, c2, c3 = st.columns(3)
    c1.metric("Latest raw snapshot", str(fresh["raw_latest"]))
    c2.metric("Latest transformed day", str(fresh["clean_latest"]))
    c3.metric("Dashboard data as of", str(fresh["mart_latest"]))
    if fresh["clean_latest"] != fresh["mart_latest"]:
        st.warning("The dashboard marts are behind the warehouse. Run `python -m transform.refresh_marts`.")

    runs = queries.ingest_runs()
    st.markdown("**Daily ingestion**")
    if runs.empty:
        st.info("No ingestion runs recorded yet.")
    else:
        runs = runs.sort_values("snapshot_date")
        expected_days = (runs["snapshot_date"].max() - runs["snapshot_date"].min()).days + 1
        missing = expected_days - len(runs)
        a, b = st.columns(2)
        a.metric("Days with a snapshot", f"{len(runs)} of {expected_days}", delta=f"{-missing} missing" if missing else None,
                 delta_color="inverse")
        coverage = runs["jobs_fetched"] / runs["api_total"].where(runs["api_total"] > 0)
        b.metric("Latest fetch coverage", f"{coverage.iloc[-1]:.1%}" if coverage.notna().iloc[-1] else "n/a")
        st.plotly_chart(px.bar(runs, x="snapshot_date", y="jobs_fetched", title="Postings fetched per day",
                               labels={"snapshot_date": "", "jobs_fetched": ""}), width="stretch")

    steps = queries.load_steps()
    if not steps.empty:
        st.markdown("**Transform duration by step**")
        st.plotly_chart(px.bar(steps, x="snapshot_date", y="seconds", color="step",
                               labels={"snapshot_date": "", "seconds": "Seconds"}), width="stretch")

    st.markdown("**Data-quality checks**")
    dq = queries.dq_results()
    if dq.empty:
        st.info("No quality checks recorded yet.")
    else:
        latest = dq[dq["snapshot_date"] == dq["snapshot_date"].max()]
        counts = latest["status"].value_counts()
        st.write(f"{latest['snapshot_date'].iloc[0]}: "
                 + ", ".join(f"{STATUS_ICON.get(s, '')} {n} {s}" for s, n in counts.items()))
        st.dataframe(latest.assign(status=latest["status"].map(lambda s: f"{STATUS_ICON.get(s, '')} {s}"))
                     [["status", "check_name", "category", "severity", "observed", "threshold", "details"]],
                     width="stretch", hide_index=True)
        trend = dq.groupby(["snapshot_date", "status"]).size().reset_index(name="checks")
        st.plotly_chart(px.bar(trend, x="snapshot_date", y="checks", color="status",
                               color_discrete_map={"pass": "#2ca02c", "warn": "#ff7f0e", "fail": "#d62728"},
                               labels={"snapshot_date": ""}), width="stretch")

    st.markdown("**Skill extraction quality**")
    match = queries.skill_match_quality()
    if not match.empty:
        total = match["skill_tags"].sum()
        unmatched = match.loc[match["match_method"] == "unmatched", "skill_tags"].sum()
        st.metric("MCF skill tags mapped to an SSG skill", f"{1 - unmatched / total:.1%}")
        st.plotly_chart(px.bar(match, x="match_method", y="skill_tags", hover_data={"avg_confidence": ":.2f"},
                               labels={"match_method": "Match method", "skill_tags": "Skill tags"}),
                        width="stretch")
