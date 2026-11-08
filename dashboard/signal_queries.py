"""SQL for the weekly signal tabs (Hiring signals, Skill lookup, Market check). Cached like dashboard/queries.py.

Reads only the `metrics` schema the weekly_metrics DAG writes (sql/init/03 and 05) and the two views over it in
sql/init/10_dashboard_signals.sql. Nothing here goes back to `clean` or `raw`: the numbers are the stored
weekly results, only filtered and lined up with the week before.
"""
import streamlit as st

from dashboard.db import run_query

TTL = 3600
WEEKS_TTL = 600            # a new week appears within 10 minutes of the weekly run


@st.cache_data(ttl=WEEKS_TTL, show_spinner=False)
def weeks():
    """Finished weeks, newest first, with the snapshot used, the finished week before each, and how many snapshot
    days each of the two had (fewer than 7: a partial week)."""
    return run_query("""
        SELECT week_end_date, week_start_date, snapshot_date, snapshot_is_week_end, fallback_reason,
               pool_postings, carried_forward, thresholds, prev_week_end_date,
               snapshots, first_snapshot_date, prev_snapshots, prev_first_snapshot_date
        FROM metrics.signal_weeks
        ORDER BY week_end_date DESC
    """)


@st.cache_data(ttl=TTL, show_spinner=False)
def skill_signals(week_end_date):
    """Every weekly signal per skill for one week (one row per skill), from metrics.skill_signals."""
    return run_query("""
        SELECT *
        FROM metrics.skill_signals
        WHERE week_end_date = %(week)s
        ORDER BY open_postings DESC NULLS LAST, skill_name
    """, {"week": week_end_date})


@st.cache_data(ttl=TTL, show_spinner=False)
def unrealistic(week_end_date):
    """The three unrealistic-requirement flags by group, with the same row's share the week before."""
    return run_query("""
        SELECT u.flag, u.group_type, u.group_key, u.group_label, u.base_postings, u.flagged_postings,
               u.flagged_share, u.flagged_salary_mid_median, u.other_salary_mid_median, u.rule,
               p.flagged_share AS flagged_share_prev_week
        FROM metrics.unrealistic_requirements u
        JOIN metrics.signal_weeks w ON w.week_end_date = u.week_end_date
        LEFT JOIN metrics.unrealistic_requirements p
               ON p.week_end_date = w.prev_week_end_date AND p.flag = u.flag
              AND p.group_type = u.group_type AND p.group_key = u.group_key
        WHERE u.week_end_date = %(week)s
        ORDER BY u.flag, u.group_type, u.flagged_share DESC
    """, {"week": week_end_date})


@st.cache_data(ttl=TTL, show_spinner=False)
def competitiveness(week_end_date, group_types: tuple = ("overall", "mom_group", "ssoc_major_group", "ssoc")):
    """Applications per posting per 24 hours by group, with the same group's value the week before."""
    return run_query("""
        SELECT c.group_type, c.group_key, c.group_label, c.postings, c.apps_per_posting_day_24h,
               c.median_apps_per_day_since_posting, c.apply_rate_flow,
               p.apps_per_posting_day_24h AS apps_per_posting_day_24h_prev_week
        FROM metrics.competitiveness c
        JOIN metrics.signal_weeks w ON w.week_end_date = c.week_end_date
        LEFT JOIN metrics.competitiveness p
               ON p.week_end_date = w.prev_week_end_date AND p.group_type = c.group_type
              AND p.group_key = c.group_key
        WHERE c.week_end_date = %(week)s AND c.group_type = ANY(%(types)s::text[])
        ORDER BY c.group_type, c.apps_per_posting_day_24h DESC NULLS LAST
    """, {"week": week_end_date, "types": list(group_types)})


@st.cache_data(ttl=TTL, show_spinner=False)
def group_signals(week_end_date):
    """The share of each posting flag per job category and SSOC major group (and overall), lined up with the
    same group's applications per posting per day."""
    return run_query("""
        SELECT g.group_type, g.group_key, g.group_label, g.postings, g.agency_share, g.direct_postings,
               g.shortage_postings, g.shortage_share, g.barrier_postings, g.barrier_share, g.entry_postings,
               g.entry_2plus_postings, g.entry_2plus_share, g.salary_mid_median, c.apps_per_posting_day_24h
        FROM metrics.group_signals g
        LEFT JOIN metrics.competitiveness c
               ON c.week_end_date = g.week_end_date AND c.group_type = g.group_type AND c.group_key = g.group_key
        WHERE g.week_end_date = %(week)s
        ORDER BY g.group_type, g.postings DESC
    """, {"week": week_end_date})


@st.cache_data(ttl=TTL, show_spinner=False)
def market_share(week_end_date):
    """MCF share of postings against MOM share of vacancies (both splits) from the week's own snapshot,
    with the gap the week before."""
    return run_query("""
        SELECT m.split, m.mom_group, m.group_title, m.mcf_snapshot_date, m.mcf_postings, m.mcf_vacancies,
               m.mcf_share_pct, m.mcf_vacancy_share_pct, m.mom_quarter, m.mom_vacancies, m.mom_share_pct,
               m.share_gap_pp, m.mom_yoy_pct, m.mom_5y_pct, m.disagrees, m.disagree_threshold_pp,
               p.share_gap_pp AS share_gap_pp_prev_week
        FROM metrics.signal_weeks w
        JOIN metrics.mcf_vs_mom_share m ON m.mcf_snapshot_date = w.snapshot_date
        LEFT JOIN metrics.signal_weeks pw ON pw.week_end_date = w.prev_week_end_date
        LEFT JOIN metrics.mcf_vs_mom_share p
               ON p.mcf_snapshot_date = pw.snapshot_date AND p.split = m.split AND p.mom_group = m.mom_group
        WHERE w.week_end_date = %(week)s
        ORDER BY m.split, m.mcf_share_pct DESC NULLS LAST
    """, {"week": week_end_date})
