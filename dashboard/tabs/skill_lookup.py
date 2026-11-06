"""Skill lookup: every weekly signal for one skill in one place (the jobseeker's view). Reads
metrics.skill_signals (sql/init/10_dashboard_signals.sql) for the week picked in the sidebar."""
import pandas as pd
import streamlit as st

from dashboard import signal_queries as sq, signals as logic

KIND = {"tsc": "SkillsFuture technical skill", "ccs": "SkillsFuture critical core skill"}


def _kind(row) -> str:
    if not bool(row.get("is_mapped")):
        return "MyCareersFuture skill tag (not mapped to SkillsFuture)"
    return KIND.get(row.get("skill_type"), "SkillsFuture skill")


def _tile(col, label: str, verdict: dict):
    with col.container(border=True):
        st.metric(label, verdict["value"], delta=verdict.get("delta"), delta_color="off")
        st.caption(verdict["caption"])


def _history(row) -> pd.DataFrame:
    """This week against last week for the signals that have both."""
    def fmt(value, kind):
        if logic.missing(value):
            return "n/a"
        return logic.times(value) if kind == "x" else f"{value:.3f}"
    lines = [
        ("Shortage lift", fmt(row["shortage_lift"], "x"), fmt(row["shortage_lift_prev_week"], "x")),
        ("Barrier lift", fmt(row["barrier_lift"], "x"), fmt(row["barrier_lift_prev_week"], "x")),
        ("Applications per posting per day", fmt(row["apps_per_posting_day_24h"], "n"),
         fmt(row["apps_per_posting_day_24h_prev_week"], "n")),
    ]
    return pd.DataFrame(lines, columns=["Signal", "This week", "Last week"])


def render(week):
    st.subheader("Skill lookup")
    st.caption("Pick a skill to see every weekly signal for it: is it in demand, what does it pay, is it short, "
               "do people avoid its postings, how many others apply? Skills with 100+ open postings.")
    skills = sq.skill_signals(week["week_end_date"])
    if skills.empty:
        st.info("No skill signals for this week.")
        return
    t = logic.thresholds(week)
    options = logic.skill_options(skills)
    refs = list(options)
    default = logic.default_skill(skills)
    ref = st.selectbox("Skill (type to search)", refs, index=refs.index(default) if default in refs else 0,
                       format_func=options.get, key="lookup_skill")
    row = skills[skills["skill_ref"] == ref].iloc[0]

    st.markdown(f"**{logic.lookup_summary(row, t)}**")
    facts = [_kind(row)]
    if not logic.missing(row["open_postings"]):
        snap = pd.Timestamp(row["snapshot_date"])
        facts.append(f"{int(row['open_postings']):,} open postings on {snap.day} {snap:%b}")
    if bool(row.get("is_casl")):
        facts.append("on SSG's list of skills needed by growth jobs (CASL)")
    st.caption(". ".join(facts) + ".")

    cols = st.columns(5)
    _tile(cols[0], "Demand", logic.demand_verdict(row))
    _tile(cols[1], "Pay", logic.pay_verdict(row, t))
    _tile(cols[2], "Shortage", logic.shortage_verdict(row, t))
    _tile(cols[3], "Barrier", logic.barrier_verdict(row, t))
    _tile(cols[4], "Competition", logic.competition_verdict(row))

    if logic.has_previous(week):
        prev = pd.Timestamp(week["prev_week_end_date"])
        st.markdown(f"**This week against the week ending {prev.day} {prev:%b}**")
        st.dataframe(_history(row), width="stretch", hide_index=True)
