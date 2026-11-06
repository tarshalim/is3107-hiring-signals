"""Tab 5: is the pipeline healthy and is the skill extraction any good (the proposal's two reliability goals)."""
import plotly.express as px
import streamlit as st

from dashboard import queries
from dashboard.db import run_query

STATUS_ICON = {"pass": "\u2705", "warn": "\u26a0\ufe0f", "fail": "\u274c"}
EVENT_ICON = {"task_failed": "\u274c", "dag_run_failed": "\u274c", "task_retry": "\U0001f501"}

# Proposal targets
RELIABILITY_TARGET = 0.95
EXTRACTION_TARGET = 0.85


@st.cache_data(ttl=300, show_spinner=False)
def alert_log(days: int = 14):
    """Task failures and retries written by the shared Airflow callback (dags/common/alerts.py), newest first.
    None when this warehouse does not have ops.alert_log yet (sql/init/09_ops_alerts.sql not applied).
    Kept in this tab rather than queries.py so the alerts block stays self-contained."""
    if not run_query("SELECT to_regclass('ops.alert_log') IS NOT NULL AS ok").iloc[0]["ok"]:
        return None
    return run_query("""
        SELECT to_char(created_at AT TIME ZONE 'Asia/Singapore', 'YYYY-MM-DD HH24:MI') AS time_sgt,
               extract(epoch FROM now() - created_at) / 86400.0 AS age_days,
               event, dag_id, task_id, try_number, exception, log_url, notified, run_id
        FROM ops.alert_log
        WHERE created_at > now() - make_interval(days => %(days)s)
        ORDER BY created_at DESC
        LIMIT 200
    """, {"days": days})


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
        reliability = len(runs) / expected_days
        a, b, c = st.columns(3)
        a.metric("Daily run reliability", f"{reliability:.1%}",
                 delta=f"target {RELIABILITY_TARGET:.0%}: {'met' if reliability >= RELIABILITY_TARGET else 'missed'}",
                 delta_color="normal" if reliability >= RELIABILITY_TARGET else "inverse",
                 help="Days with a successful snapshot divided by calendar days in the last 30 days of history. "
                      "A failed day leaves no snapshot, so it counts as missing.")
        b.metric("Days with a snapshot", f"{len(runs)} of {expected_days}",
                 delta=f"{-missing} missing" if missing else None, delta_color="inverse")
        coverage = runs["jobs_fetched"] / runs["api_total"].where(runs["api_total"] > 0)
        c.metric("Latest fetch coverage", f"{coverage.iloc[-1]:.1%}" if coverage.notna().iloc[-1] else "n/a")
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

    st.markdown("**Alerts and retries**")
    alerts = alert_log()
    if alerts is None:
        st.info("The alert log is not set up on this warehouse yet: apply `sql/init/09_ops_alerts.sql` "
                "(docs/alerts-and-monitoring.md).")
    elif alerts.empty:
        st.success("No task failures or retries in the last 14 days.")
    else:
        week = alerts[alerts["age_days"] < 7]
        a, b, c = st.columns(3)
        a.metric("Failures, last 7 days", int((week["event"] != "task_retry").sum()),
                 help="Tasks that failed for good (retries used up) plus runs that failed on their own, "
                      "e.g. a timed-out run.")
        b.metric("Retries, last 7 days", int((week["event"] == "task_retry").sum()),
                 help="A task failed and Airflow tried it again. A retry followed by a green run is the pipeline "
                      "recovering by itself, e.g. from the API's rate limit.")
        c.metric("Latest alert (SGT)", alerts["time_sgt"].iloc[0])
        st.dataframe(alerts.assign(event=alerts["event"].map(lambda e: f"{EVENT_ICON.get(e, '')} {e}"))
                     [["time_sgt", "event", "dag_id", "task_id", "try_number", "exception", "log_url"]],
                     width="stretch", hide_index=True,
                     column_config={"log_url": st.column_config.LinkColumn("Airflow log", display_text="open")})

    st.markdown("**Skill extraction quality**")
    match = queries.skill_match_quality()
    if not match.empty:
        total = match["skill_tags"].sum()
        unmatched = match.loc[match["match_method"] == "unmatched", "skill_tags"].sum()
        mapped = 1 - unmatched / total
        st.metric("MCF skill tags mapped to an SSG skill", f"{mapped:.1%}",
                  delta=f"proposal target {EXTRACTION_TARGET:.0%}: {'met' if mapped >= EXTRACTION_TARGET else 'missed'}",
                  delta_color="normal" if mapped >= EXTRACTION_TARGET else "inverse")
        st.caption("This is a proxy. The proposal's 85% target is accuracy on 200 hand-labelled postings, which "
                   "needs a labelled sample; the mapping rate only shows how many skill tags found an SSG skill.")
        st.plotly_chart(px.bar(match, x="match_method", y="skill_tags", hover_data={"avg_confidence": ":.2f"},
                               labels={"match_method": "Match method", "skill_tags": "Skill tags"}),
                        width="stretch")
