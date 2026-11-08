"""Hiring signals: which skills and jobs are hard to hire for, and is the cause a shortage, a barrier or an
unrealistic ask? Reads the weekly `metrics` tables for the week picked in the sidebar."""
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard import signal_queries as sq, signals as logic

SHOWN = 15                      # skills or occupations per chart
LAST_WEEK = dict(symbol="line-ns-open", size=16, color="#333333", line=dict(width=2))
FLAG_COLORS = ("#08519c", "#9ecae1")
TECHNICAL_NOTE = ("Soft-skill results (Adaptability, Customer Orientation, Enjoy Working with People, Self Motivation) "
                  "are inflated by commission-sales firms: direct-selling \"marketing\" companies that keep reposting "
                  "ads few people apply to. They are not employment agencies, so the agency filter does not catch "
                  "them, which is why the shortage and barrier lists show technical skills by default.")
FLAG_NAMES = {
    "entry_level_2plus_years": ("Asking 2+ years", "Other entry-level postings"),
    "junior_2plus_years": ("Asking 2+ years", "Other Junior Executive postings"),
}


def render(week):
    end = week["week_end_date"]
    t = logic.thresholds(week)
    st.subheader("Which skills and jobs are hard to hire for, and why?")
    st.caption(f"{logic.week_caption(week)} Three possible causes: a **shortage** (employers repost and nobody "
               "applies), a **barrier** (people look, then do not apply) or an **unrealistic ask** (too much "
               "experience, or still searching despite applicants).")

    skills = sq.skill_signals(end)
    unreal = sq.unrealistic(end)
    comp = sq.competitiveness(end)
    market = sq.market_share(end)
    groups = sq.group_signals(end)
    if skills.empty and unreal.empty and comp.empty:
        st.info("This week has no signal rows. Check the weekly_metrics run on the Pipeline health tab.")
        return
    technical = st.toggle("Technical skills only", value=True, key="signals_technical",
                          help="Hides SkillsFuture critical core skills (e.g. Adaptability, Collaboration) and MCF "
                               "tags that are traits or attitudes (e.g. Positive Attitude, Self Motivation; list in "
                               "data/reference/soft_skill_tags.csv) from the shortage and barrier lists.")
    st.caption(TECHNICAL_NOTE if technical else "Showing all skills. " + TECHNICAL_NOTE)
    shortage = logic.technical_only(logic.shortage_skills(skills), technical) if not skills.empty else skills
    barrier = logic.technical_only(logic.barrier_skills(skills), technical) if not skills.empty else skills
    labels = logic.skill_options(skills) if not skills.empty else {}

    _headline(shortage, barrier, unreal, comp, market, t, skills)
    st.divider()
    _summary(groups, t)
    _shortage(shortage, labels, t)
    _barrier(barrier, labels, t)
    _unrealistic(unreal, t)
    _competitiveness(comp, t, week)


# --- headline -------------------------------------------------------------------------------------

def _names(rows, lift_col: str, n: int = 3) -> str:
    return ", ".join(f"{name} {logic.times(v)}" for name, v in zip(rows["skill_name"].head(n), rows[lift_col].head(n)))


def _overall(unreal, flag: str):
    rows = unreal[(unreal["flag"] == flag) & (unreal["group_type"] == "overall")]
    return None if rows.empty else rows.iloc[0]


def _headline(shortage, barrier, unreal, comp, market, t, skills):
    a, b, c, d = st.columns(4)
    with a.container(border=True):
        # the market's share is on every listed row; read it before the technical filter, which may leave none
        shares = skills["overall_shortage_share"].dropna() if not skills.empty else skills
        share = shares.iloc[0] if len(shares) else None
        st.metric("Shortage", logic.pct(share),
                  help=f"Open postings reposted {t['shortage_min_reposts']}+ times that still have at most "
                       f"{t['shortage_max_applications']} applications, as a share of open postings by direct "
                       "employers. Employment agencies are left out: they keep evergreen ads up and repost them.")
        text = (f"of direct employers' postings are reposted {t['shortage_min_reposts']}+ times with at most "
                f"{t['shortage_max_applications']} applications.")
        if not shortage.empty:
            kept = logic.carried_over(shortage)
            text += f" Most at risk: {_names(shortage, 'shortage_lift')} as likely."
            text += f" {kept[0]} of {kept[1]} skills also listed last week." if kept else ""
        st.caption(text)
    with b.container(border=True):
        st.metric("Barrier", f"{len(barrier):,} skills",
                  help=f"Skills whose postings are {t['barrier_min_lift']}+ times as likely as similar jobs to be "
                       "among the most viewed while getting fewer applications per view, and that get fewer "
                       "applications per view than the average posting too.")
        text = "whose postings people view, then apply to less than similar jobs."
        if not barrier.empty:
            kept = logic.carried_over(barrier)
            text += f" Strongest: {_names(barrier, 'barrier_lift')}."
            text += f" {kept[0]} of {kept[1]} also flagged last week." if kept else ""
        st.caption(text)
    with c.container(border=True):
        # entry level leads: the clearest unrealistic ask. Junior Executive second, read through its pay
        entry, junior = _overall(unreal, "entry_level_2plus_years"), _overall(unreal, "junior_2plus_years")
        if entry is None:
            st.metric("Unrealistic ask", "n/a")
        else:
            prev = entry["flagged_share_prev_week"]
            st.metric("Unrealistic ask", logic.pct(entry["flagged_share"]),
                      delta=None if logic.missing(prev) else logic.points(entry["flagged_share"] - prev),
                      delta_color="off", help="Delta: change since last week, in percentage points.")
            text = (f"of entry-level postings ask 2+ years of experience, and pay {_gap_text(entry, short=True)} "
                    "against other entry-level postings.")
            if junior is not None:
                text += (f" Junior Executive: {logic.pct(junior['flagged_share'], 0)} ask 2+ years, but pay "
                         f"{_gap_text(junior, short=True)}.")
            st.caption(text)
    with d.container(border=True):
        groups = comp[comp["group_type"] == "ssoc_major_group"]
        spread = logic.spread(groups, t["group_min_postings"]) if not groups.empty else None
        if spread is None or spread["ratio"] is None:
            st.metric("Competition", "n/a")
        else:
            st.metric("Competition", f"{spread['ratio']:.0f}x",
                      help="Applications gained per open posting per 24 hours, most against least applied-to "
                           f"SSOC major group (groups with {t['group_min_postings']}+ open postings).")
            st.caption(f"more applicants per posting for {spread['top']['group_label'].lower()} "
                       f"({spread['top']['apps_per_posting_day_24h']:.2f} a day) than for "
                       f"{spread['bottom']['group_label'].lower()} ({spread['bottom']['apps_per_posting_day_24h']:.2f}).")
    gap = logic.market_gap(market) if not market.empty else None
    if gap:
        st.caption(f"**Read with care:** {gap['line']} {gap['meaning']} (Market check tab).")


def _gap_text(row, short: bool = False) -> str:
    gap = logic.pay_gap(row["flagged_salary_mid_median"], row["other_salary_mid_median"])
    if gap is None:
        return "n/a"
    if short:
        return f"{gap:+.0%}"
    return (f"{gap:+.0%} ({logic.money(row['flagged_salary_mid_median'])} against "
            f"{logic.money(row['other_salary_mid_median'])} median)")


# --- summary grid ---------------------------------------------------------------------------------

GRID_SCALE = [[0.0, "#4f8fdc"], [0.5, "#f0efec"], [1.0, "#e0675a"]]   # blue below the market, grey same, red above
GRID_TICKS = {-2: "1/4 (easier)", -1: "1/2", 0: "same", 1: "2x", 2: "4x (harder)"}
GRID_ROWS = {"category": "Job category", "ssoc_major_group": "Occupation group (SSOC)"}
GRID_MIN_POSTINGS = 500       # job categories shown; smaller ones are noisy and make the grid too tall
GRID_TOO_FEW = "#8a8984"      # muted text and legend outline for cells on fewer than logic.GRID_MIN_BASE postings


def _summary(groups, t):
    st.markdown("#### Which cause shows up where?")
    st.caption("Each cell compares a group with all open postings. **Red points to hard to fill** (fewer applicants, "
               "or more of the flag than the market), **blue to easier to fill**, pale grey is about the same. The "
               "number is the group's own value, with the postings it rests on below it. **Blank cells marked "
               f"\"too few\"** rest on fewer than {logic.GRID_MIN_BASE} postings and are not coloured. Read across "
               "a row to see which causes a group shows; rows with the fewest applicants come first.")
    if groups.empty or (groups["group_type"] == "overall").sum() == 0:
        st.info("No summary rows this week. Rerun the weekly_metrics DAG to build metrics.group_signals.")
        return
    overall = groups[groups["group_type"] == "overall"].iloc[0]
    kind = st.radio("Rows", list(GRID_ROWS), format_func=GRID_ROWS.get, horizontal=True, key="signals_grid_rows")
    rows = groups[groups["group_type"] == kind]
    if kind == "category":
        rows = rows[rows["postings"] >= GRID_MIN_POSTINGS]
    if rows.empty:
        st.info("No group has enough postings.")
        return
    st.plotly_chart(_summary_figure(rows, overall), width="stretch")
    st.caption(f"Few applicants: applications per open posting per day (red = fewer than all postings). Shortage: "
               f"share of direct employers' postings reposted {t['shortage_min_reposts']}+ times with at most "
               f"{t['shortage_max_applications']} applications. Barrier: share of postings among the most viewed of "
               f"their occupation that get few applications per view. Entry level asking 2+ years: share of entry-level "
               f"postings. n/a: fewer than {t['group_min_postings']} postings under the share."
               + (f" Job categories with {GRID_MIN_POSTINGS:,}+ open postings." if kind == "category" else ""))


def _summary_figure(rows, overall):
    grid = logic.summary_grid(rows, overall)
    shown = grid["rows"]
    labels = [logic.short_label(lab, 34) for lab in shown["group_label"]]
    hover = [[f"{lab}<br>{title}: {grid['text'][i][j]}, from {grid['bases'][i][j]:,} postings"
              + (f" (under {logic.GRID_MIN_BASE}: too few to read)" if grid["small"][i][j] else "")
              + f"<br>{shown.at[i, 'postings']:,} open postings, {logic.pct(shown.at[i, 'agency_share'], 0)} by "
              f"agencies<br>Median pay {logic.money(shown.at[i, 'salary_mid_median'])} "
              f"(all: {logic.money(overall['salary_mid_median'])})"
              for j, (title, _, _, _) in enumerate(logic.GRID_COLUMNS)] for i, lab in enumerate(shown["group_label"])]

    def cells(small: bool):
        """Cell text for the coloured cells (value, then the postings it rests on) or for the too-few ones."""
        return [[f"{'<i>too few</i>' if small else t}<br><span style='font-size:10px'>{b:,} postings</span>" if s == small else ""
                 for t, b, s in zip(text_row, base_row, small_row)]
                for text_row, base_row, small_row in zip(grid["text"], grid["bases"], grid["small"])]

    common = dict(x=grid["headers"], y=labels, texttemplate="%{text}", customdata=hover,
                  hovertemplate="%{customdata}<extra></extra>", xgap=2, ygap=2)
    fig = go.Figure(go.Heatmap(
        z=grid["z"], text=cells(False), textfont=dict(size=12, color="#0b0b0b"),
        colorscale=GRID_SCALE, zmin=-logic.GRID_CAP, zmax=logic.GRID_CAP,
        colorbar=dict(title=dict(text="vs all postings", side="right"), tickvals=list(GRID_TICKS),
                      ticktext=list(GRID_TICKS.values()), thickness=12, len=0.6), **common))
    # cells on too few postings: left blank (transparent) with muted "too few" text, so they can't be
    # mistaken for the pale grey "about the same" of the colour scale
    fig.add_trace(go.Heatmap(
        z=[[1 if s else None for s in row] for row in grid["small"]], text=cells(True),
        textfont=dict(size=12, color=GRID_TOO_FEW), colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(0,0,0,0)"]],
        showscale=False, **common))
    # legend entry for the blank cells (the colour bar only covers the coloured ones)
    fig.add_trace(go.Scatter(
        x=[None], y=[None], mode="markers", name=f"Fewer than {logic.GRID_MIN_BASE} postings: too few to read",
        marker=dict(symbol="square-open", size=12, color=GRID_TOO_FEW), hoverinfo="skip"))
    fig.update_yaxes(autorange="reversed", automargin=True, ticksuffix="  ")   # first row (fewest applicants) on top
    fig.update_xaxes(side="top", automargin=True)
    fig.update_layout(height=max(360, 40 * len(shown) + 150), margin=dict(t=70, b=50, l=10, r=10),
                      plot_bgcolor="rgba(0,0,0,0)", showlegend=True,
                      legend=dict(orientation="h", x=0, xanchor="left", y=0, yref="container", yanchor="bottom"))
    return fig


# --- shortage and barrier -------------------------------------------------------------------------

def _lift_chart(rows, labels: dict, lift_col: str, n_col: str, firms_col: str, x_title: str, ref_text: str,
                hover: dict):
    shown = rows.head(SHOWN)
    shown = shown.assign(label=shown["skill_ref"].map(labels),
                         text=[logic.bar_text(n, c) for n, c in zip(shown[n_col], shown[firms_col])])
    fig = px.bar(shown.iloc[::-1], x=lift_col, y="label", orientation="h", color="stability", text="text",
                 color_discrete_map=logic.STABILITY_COLORS,
                 category_orders={"stability": [logic.STABLE, logic.NEW, logic.FIRST]},
                 hover_data={"label": False, "text": False, "stability": False, **hover},
                 labels={lift_col: x_title, "label": "", "stability": "",
                         "open_postings": "Open postings with the skill",
                         "shortage_base_postings": "Direct employers' postings with the skill", n_col: "Flagged postings",
                         firms_col: "Employers behind them", "shortage_top_company_share": "Biggest employer's share",
                         "barrier_share": "Share that are barrier postings",
                         "barrier_expected_share": "Share expected from similar jobs"})
    fig.update_traces(textposition="outside", cliponaxis=False, textfont_size=12)
    # colouring by stability makes one trace per label, which would group the bars instead of ranking them
    fig.update_yaxes(categoryorder="array", categoryarray=shown["label"].tolist()[::-1])
    fig.add_vline(x=1, line_width=1, line_color="#888888", annotation_text=ref_text,
                  annotation_position="top", annotation_font_color="#666666")
    fig.update_layout(height=max(380, 30 * len(shown) + 130), legend_orientation="h", legend_y=-0.1,
                      xaxis_range=[0, float(shown[lift_col].max()) * 1.5], margin=dict(t=40))
    return fig


def _table(rows, labels: dict, columns: dict):
    table = rows.assign(skill=rows["skill_ref"].map(labels))[["skill", *columns]].rename(columns={"skill": "Skill", **columns})
    st.dataframe(table, width="stretch", hide_index=True)


def _shortage(rows, labels: dict, t):
    st.markdown("#### 1. Shortage: employers repost, nobody applies")
    st.caption(f"How many times more likely a skill's postings are than the average direct employer's posting to be "
               f"reposted "
               f"{t['shortage_min_reposts']}+ times with at most {t['shortage_max_applications']} applications. "
               f"Listed from {t['shortage_min_lift']}x, with {t['shortage_min_flagged']}+ such postings from "
               f"{t['min_companies']}+ employers, with "
               f"no employer behind more than {t['max_top_company_share']:.0%} of them. Direct employers only: "
               f"employment agencies (SSIC {t['agency_ssic_prefix']}) keep evergreen ads up and repost them, so their "
               "postings are left out.")
    if rows.empty:
        st.info("No skill meets the shortage minimums this week.")
        return
    st.plotly_chart(_lift_chart(rows, labels, "shortage_lift", "shortage_postings", "shortage_companies",
                                "Times as likely as the average direct employer's posting",
                                "average direct employer's posting",
                                {"shortage_base_postings": ":,", "shortage_top_company_share": ":.0%"}),
                    width="stretch")
    st.caption("Lift = shortage postings / direct employers' postings with the skill, divided by the same share over "
               "all direct employers' postings.")
    with st.expander(f"All {len(rows)} listed shortage skills"):
        _table(rows, labels, {"shortage_lift": "Lift", "shortage_postings": "Shortage postings",
                              "shortage_base_postings": "Direct employers' postings",
                              "shortage_companies": "Employers",
                              "shortage_lift_prev_week": "Lift last week", "stability": "Stability"})


def _barrier(rows, labels: dict, t):
    st.markdown("#### 2. Barrier: people look, then do not apply")
    st.caption(f"How many times more likely a skill's postings are than similar jobs (same SSOC group) to be in "
               f"the most-viewed quarter while getting at most {t['barrier_max_rate_ratio']} times the group's "
               f"applications per view. Flagged from {t['barrier_min_lift']}x with {t['barrier_min_flagged']}+ "
               f"such postings from {t['min_companies']}+ employers, and only if the skill's postings also get fewer "
               "applications per view than the average posting. Moves more week to week than shortage.")
    if rows.empty:
        st.info("No skill is flagged as a barrier skill this week.")
        return
    st.plotly_chart(_lift_chart(rows, labels, "barrier_lift", "barrier_postings", "barrier_companies",
                                "Times as likely as similar jobs", "as similar jobs",
                                {"open_postings": ":,", "barrier_share": ":.0%", "barrier_expected_share": ":.0%"}),
                    width="stretch")
    with st.expander(f"All {len(rows)} barrier skills"):
        _table(rows, labels, {"barrier_lift": "Lift", "barrier_postings": "Barrier postings",
                              "barrier_companies": "Employers", "open_postings": "Open postings",
                              "barrier_lift_prev_week": "Lift last week", "stability": "Stability"})


# --- unrealistic asks -----------------------------------------------------------------------------

def _unrealistic(unreal, t):
    st.markdown("#### 3. Unrealistic ask: entry-level jobs asking for experience")
    if unreal.empty:
        st.info("No unrealistic-requirement rows this week.")
        return
    cols = st.columns(2)
    for col, flag in zip(cols, logic.EXPERIENCE_FLAGS):      # entry level first, Junior Executive second
        row = _overall(unreal, flag)
        with col.container(border=True):
            if row is None:
                st.metric(logic.FLAG_LABELS[flag], "n/a")
                continue
            prev = row["flagged_share_prev_week"]
            st.metric(logic.FLAG_LABELS[flag], logic.pct(row["flagged_share"]),
                      delta=None if logic.missing(prev) else logic.points(row["flagged_share"] - prev),
                      delta_color="off", help=row["rule"])
            gap = logic.pay_gap(row["flagged_salary_mid_median"], row["other_salary_mid_median"])
            reading = logic.experience_ask_reading(gap)
            st.caption(f"{int(row['flagged_postings']):,} of {int(row['base_postings']):,} postings. They pay "
                       f"{_gap_text(row)}" + (f": {reading}." if reading else "."))

    flag = st.radio("By job category", list(logic.EXPERIENCE_FLAGS), format_func=logic.FLAG_LABELS.get,
                    horizontal=True, key="signals_flag")
    rows = unreal[(unreal["flag"] == flag) & (unreal["group_type"] == "category")]
    if rows.empty:
        st.info(f"No job category has {t['group_min_postings']}+ postings for this flag.")
    else:
        _unrealistic_by_category(unreal, rows, flag, t)
    _reposted_many_apps(_overall(unreal, "reposted_many_apps"))


def _reposted_many_apps(row):
    """The minor signal: one line with its share, under the experience asks."""
    if row is None:
        return
    st.markdown(f"**Minor signal: {logic.FLAG_LABELS['reposted_many_apps'].lower()}.** "
                f"{logic.pct(row['flagged_share'])} of direct employers' open postings "
                f"({int(row['flagged_postings']):,} of {int(row['base_postings']):,}) were reposted although they "
                f"draw many applications per day; they pay {_gap_text(row)}. Too few to read by skill or category: "
                "an employer still searching may be choosy, or hiring several people from one ad.",
                help=row["rule"])


def _unrealistic_by_category(unreal, rows, flag, t):
    rows = rows.sort_values("flagged_share", ascending=False).head(SHOWN)
    rows = rows.assign(label=rows["group_label"].map(logic.short_label))
    order = rows["label"].tolist()[::-1]
    overall = _overall(unreal, flag)

    left, right = st.columns(2)
    x_title = "Share of the category's postings"
    share = px.bar(rows.iloc[::-1], x="flagged_share", y="label", orientation="h",
                   hover_data={"label": False, "group_label": True, "flagged_postings": ":,", "base_postings": ":,"},
                   labels={"flagged_share": x_title, "label": "", "group_label": "Category",
                           "flagged_postings": "Flagged", "base_postings": "Postings"},
                   title=f"{logic.FLAG_LABELS[flag]}, by job category")
    share.update_xaxes(tickformat=".0%")
    if overall is not None:
        share.add_vline(x=overall["flagged_share"], line_width=1, line_color="#888888")
        share.update_layout(title_subtitle_text=f"Line: all categories, {logic.pct(overall['flagged_share'])}")
    share.update_layout(height=max(380, 28 * len(rows) + 140), margin=dict(t=90, b=70))
    left.plotly_chart(share, width="stretch")
    right.plotly_chart(_pay_chart(rows, flag, order), width="stretch")
    st.caption(f"Job categories with {t['group_min_postings']}+ postings, the {len(rows)} highest shares. Pay is "
               "the median advertised monthly midpoint, postings with a usable salary.")


def _pay_chart(rows, flag: str, order: list):
    flagged_name, other_name = FLAG_NAMES[flag]
    fig = go.Figure()
    xs, ys = [], []
    for label, a, b in zip(rows["label"], rows["other_salary_mid_median"], rows["flagged_salary_mid_median"]):
        xs += [a, b, None]
        ys += [label, label, None]
    fig.add_trace(go.Scatter(x=xs, y=ys, mode="lines", line=dict(color="#c6dbef", width=3), showlegend=False,
                             hoverinfo="skip"))
    for column, name, color in (("other_salary_mid_median", other_name, FLAG_COLORS[1]),
                                ("flagged_salary_mid_median", flagged_name, FLAG_COLORS[0])):
        fig.add_trace(go.Scatter(x=rows[column], y=rows["label"], mode="markers", name=name,
                                 marker=dict(color=color, size=11, line=dict(color="white", width=2)),
                                 hovertemplate="%{y}<br>" + name + ": %{x:$,.0f}<extra></extra>"))
    fig.update_yaxes(categoryorder="array", categoryarray=order)
    # legend on top, so the plot area matches the share chart beside it row for row
    fig.update_layout(title="Median monthly pay", xaxis_title="SGD per month", xaxis_tickprefix="$",
                      height=max(380, 28 * len(rows) + 140), margin=dict(t=90, b=70),
                      legend=dict(orientation="h", x=0, y=1.0, yanchor="bottom"))
    return fig


# --- competitiveness ------------------------------------------------------------------------------

def _apps_chart(rows, overall, title: str, overall_line: bool, last_week: str = "Last week"):
    """Applications per posting per day as bars with the value written at the end, last week's value as a tick
    (named last_week: it says when last week was partial), and the all-postings value as a line (overall_line)
    or in the axis title."""
    values = rows["apps_per_posting_day_24h"]
    digits = 2 if values.min() >= 0.1 else 3
    fig = go.Figure()
    fig.add_trace(go.Bar(x=values, y=rows["label"], orientation="h", name="This week",
                         text=[f"{v:.{digits}f}" for v in values], textposition="outside", cliponaxis=False,
                         customdata=rows[["group_label", "postings"]],
                         hovertemplate="%{customdata[0]}<br>%{x:.3f} applications per posting per day"
                                       "<br>%{customdata[1]:,} open postings<extra></extra>"))
    prev = rows["apps_per_posting_day_24h_prev_week"]
    if prev.notna().any():
        fig.add_trace(go.Scatter(x=prev, y=rows["label"], mode="markers", name=last_week, marker=LAST_WEEK,
                                 hovertemplate="%{y}<br>" + last_week + " %{x:.3f}<extra></extra>"))
    subtitle = None
    if overall is not None:
        if overall_line:
            fig.add_vline(x=overall, line_width=1, line_color="#888888")
            subtitle = f"Line: all postings, {overall:.2f}"
        else:
            subtitle = f"All postings: {overall:.2f}"
    top = max(values.max(), prev.max() if prev.notna().any() else 0, overall if overall_line and overall else 0)
    fig.update_yaxes(categoryorder="array", categoryarray=rows["label"].tolist())
    fig.update_layout(title=dict(text=title, subtitle_text=subtitle), xaxis_title="Applications per posting per day",
                      xaxis_range=[0, float(top) * 1.2], height=max(380, 28 * len(rows) + 160),
                      margin=dict(t=110, b=70), legend=dict(orientation="h", x=0, y=1.0, yanchor="bottom"))
    return fig


def _labelled(rows, group_type: str):
    labels = [logic.short_label(logic.group_label(group_type, k, v)) for k, v in zip(rows["group_key"], rows["group_label"])]
    rows = rows.assign(label=labels)
    dupes = rows["label"].duplicated(keep=False)
    return rows.assign(label=[f"{lab} ({k})" if d else lab for lab, k, d in zip(rows["label"], rows["group_key"], dupes)])


def _competitiveness(comp, t, week):
    st.markdown("#### 4. Competition: applications per posting per day")
    last_week = logic.last_week_name(week)
    st.caption("Applications gained during the week per open posting, per 24 hours. Low means few applicants for "
               "each opening: hard to fill for employers, less competition for jobseekers. The tick is last week"
               + (f", a partial week ({logic.partial_week(week, prev=True)}): its values rest on fewer days."
                  if last_week != "Last week" else "."))
    if comp.empty:
        st.info("No competitiveness rows this week.")
        return
    overall_rows = comp[comp["group_type"] == "overall"]
    overall = None if overall_rows.empty else overall_rows.iloc[0]["apps_per_posting_day_24h"]

    left, right = st.columns(2)
    with left:
        groups = comp[(comp["group_type"] == "ssoc_major_group") & (comp["postings"] >= t["group_min_postings"])]
        groups = _labelled(groups.sort_values("apps_per_posting_day_24h"), "ssoc_major_group")
        if groups.empty:
            st.info("No occupation group has enough postings.")
        else:
            st.plotly_chart(_apps_chart(groups, overall, "By occupation group (SSOC major group)", True, last_week),
                            width="stretch")
            st.caption(f"Groups with {t['group_min_postings']}+ open postings.")
    with right:
        a, b = st.columns(2)
        side = a.radio("Show", ["Fewest applicants", "Most applicants"], horizontal=True, key="signals_comp_side")
        low = int(t["group_min_postings"])
        min_n = b.slider("Minimum open postings", low, 1000, max(100, low), step=10, key="signals_comp_min")
        occ = comp[(comp["group_type"] == "ssoc") & (comp["postings"] >= min_n)]
        if occ.empty:
            st.info("No occupation has that many open postings.")
            return
        # the first row is drawn at the bottom, so reverse: the most extreme occupation ends up on top
        occ = occ.sort_values("apps_per_posting_day_24h", ascending=side == "Fewest applicants").head(SHOWN)
        occ = _labelled(occ.iloc[::-1], "ssoc")
        st.plotly_chart(_apps_chart(occ, overall, f"Occupations with the {side.lower()}", side == "Most applicants",
                                    last_week), width="stretch")
        st.caption(f"{side} among {int((comp['group_type'] == 'ssoc').sum()):,} occupations with "
                   f"{t['group_min_postings']}+ open postings, keeping those with {int(min_n):,}+.")
