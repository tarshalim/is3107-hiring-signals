"""Market check: does MyCareersFuture look like the whole vacancy market? MCF share of open postings against
MOM's share of job vacancies by occupation group, from metrics.mcf_vs_mom_share (sql/init/03_mom_vacancies.sql)."""
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard import signal_queries as sq, signals as logic

SPLITS = {"A": "3 occupation groups", "B": "PME and non-PME"}
MCF_COLOR, MOM_COLOR = "#08519c", "#8c8c8c"


def _chart(rows):
    labels = [logic.wrap_label(s, 28) for s in rows["group_title"]]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=labels, y=rows["mcf_share_pct"], name="MyCareersFuture postings", marker_color=MCF_COLOR,
                         hovertemplate="%{x}<br>MCF %{y:.1f}% of open postings<extra></extra>"))
    fig.add_trace(go.Bar(x=labels, y=rows["mom_share_pct"], name="MOM job vacancies", marker_color=MOM_COLOR,
                         hovertemplate="%{x}<br>MOM %{y:.1f}% of vacancies<extra></extra>"))
    top = rows[["mcf_share_pct", "mom_share_pct"]].max(axis=1)
    for label, gap, y, big in zip(labels, rows["share_gap_pp"], top, rows["disagrees"]):
        if logic.missing(gap):
            continue
        text = f"<b>{gap:+.1f} pts</b>" if big else f"{gap:+.1f} pts"
        fig.add_annotation(x=label, y=y, text=text, showarrow=False, yshift=14,
                           font=dict(size=15 if big else 13, color="#222222" if big else "#666666"))
    fig.update_layout(barmode="group", bargap=0.35, bargroupgap=0.08, yaxis_title="Share (%)", height=420,
                      yaxis_range=[0, float(top.max()) * 1.18], legend_orientation="h", legend_y=-0.15,
                      margin=dict(t=30))
    return fig


def _table(rows, comp):
    table = rows.copy()
    if not comp.empty:
        apps = comp[comp["group_type"] == "mom_group"].set_index("group_key")["apps_per_posting_day_24h"]
        table["apps"] = table["mom_group"].map(apps)
    cols = {"group_title": "Group", "mcf_postings": "MCF postings", "mcf_share_pct": "MCF %",
            "mom_vacancies": "MOM vacancies", "mom_share_pct": "MOM %", "share_gap_pp": "Gap (pts)",
            "share_gap_pp_prev_week": "Gap last week", "mcf_vacancy_share_pct": "MCF % by vacancies",
            "mom_yoy_pct": "MOM 1-year change %", "mom_5y_pct": "MOM 5-year change %",
            "apps": "Applications per posting per day"}
    one = st.column_config.NumberColumn(format="%.1f")
    config = {"MCF postings": st.column_config.NumberColumn(format="localized"),
              "MOM vacancies": st.column_config.NumberColumn(format="localized"),
              "Applications per posting per day": st.column_config.NumberColumn(format="%.2f"),
              **{name: one for name in ("MCF %", "MOM %", "Gap (pts)", "Gap last week", "MCF % by vacancies",
                                        "MOM 1-year change %", "MOM 5-year change %")}}
    st.dataframe(table[[c for c in cols if c in table]].rename(columns=cols), width="stretch", hide_index=True,
                 column_config=config)
    st.caption("MCF % by vacancies weights each posting by its number of vacancies, as MOM counts vacancies. "
               "MOM changes compare the same quarter one and five years earlier.")


def render(week):
    st.subheader("Market check: is MyCareersFuture the whole market?")
    market = sq.market_share(week["week_end_date"])
    if market.empty:
        st.info("No MOM comparison for this week's snapshot. The compare_mcf_with_mom task of weekly_metrics writes "
                "it.")
        return
    snap = pd.Timestamp(market["mcf_snapshot_date"].iloc[0])
    st.caption(f"Share of MyCareersFuture open postings on {snap.day} {snap:%b} against share of MOM job vacancies "
               f"in {market['mom_quarter'].dropna().iloc[0] if market['mom_quarter'].notna().any() else 'n/a'} "
               "(the latest quarter MOM has published), by occupation group. Gap = MCF minus MOM, in points.")
    split = st.radio("Groups", list(SPLITS), format_func=SPLITS.get, horizontal=True, key="market_split")
    rows = logic.market_rows(market, split)
    if rows.empty:
        st.info("No groups with a share for this split.")
        return

    gap = logic.market_gap(market, split)
    if gap:
        st.markdown(f"{gap['line']} **{gap['meaning']}**")
    st.plotly_chart(_chart(rows), width="stretch")
    notes = [f"Bold gap: {rows['disagree_threshold_pp'].iloc[0]:.0f}+ points, where MCF and MOM disagree."]
    unknown = market[(market["split"] == split) & (market["mom_group"] == "unknown")]
    if not unknown.empty and int(unknown["mcf_postings"].iloc[0]) > 0:
        notes.append(f"{int(unknown['mcf_postings'].iloc[0]):,} postings with a missing or invalid occupation code "
                     "are left out of the shares.")
    st.caption(" ".join(notes))
    comp = sq.competitiveness(week["week_end_date"]) if split == "A" else pd.DataFrame()
    _table(rows, comp)
