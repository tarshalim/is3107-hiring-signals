"""Logic for the weekly signal tabs (pure pandas, no database): week labels, stability, headline numbers and
the plain-words verdicts of the skill lookup.

Input frames come from dashboard/signal_queries.py. Thresholds come from the week's run
(metrics.run_log.thresholds), so the words always match the rule that produced the numbers.
"""
import math
import textwrap

import pandas as pd

from dashboard.filters import MOM_GROUP_LABELS

STABLE = "Also flagged last week"
NEW = "New this week"
FIRST = "No earlier week to compare"
STABILITY_COLORS = {STABLE: "#08519c", NEW: "#9ecae1", FIRST: "#6baed6"}

# The values in metrics/build_week.py THRESHOLDS on 6 Nov 2026, used only if a run did not record one
DEFAULT_THRESHOLDS = {
    "skill_min_postings": 100, "group_min_postings": 30, "salary_min_postings": 100, "min_companies": 10,
    "shortage_min_flagged": 25, "shortage_min_reposts": 2, "shortage_max_applications": 2,
    "barrier_min_lift": 1.5, "barrier_min_flagged": 25, "barrier_max_rate_ratio": 0.8,
    "max_top_company_share": 0.5, "entry_min_years": 2,
}

# Short names for the MOM groups in running text (metrics.mcf_vs_mom_share.mom_group, both splits)
GROUP_SHORT = {
    "PMET": "PMET jobs",
    "clerical_sales_services": "clerical, sales and service jobs",
    "production_transport": "production, transport and labouring jobs",
    "PME": "PME jobs",
    "non_PME": "non-PME jobs",
}

FLAG_LABELS = {
    "entry_level_2plus_years": "Entry level asking 2+ years",
    "junior_2plus_years": "Junior Executive asking 2+ years",
    "reposted_many_apps": "Reposted despite many applicants",
}


# --- formatting -----------------------------------------------------------------------------------

def missing(value) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value)) or value is pd.NA


def times(value) -> str:
    """2.535 -> '2.5x'."""
    return "n/a" if missing(value) else f"{value:.1f}x"


def money(value) -> str:
    return "n/a" if missing(value) else f"${value:,.0f}"


def pct(share, digits: int = 1) -> str:
    """A share (0.0626) as a percentage ('6.3%')."""
    return "n/a" if missing(share) else f"{share * 100:.{digits}f}%"


def points(delta_share) -> str | None:
    """A change in a share as percentage points, for st.metric's delta ('+0.3 pts'); None when unknown."""
    if missing(delta_share):
        return None
    return f"{delta_share * 100:+.1f} pts"


def pay_gap(flagged, other) -> float | None:
    """How much more (or less) the flagged postings pay, as a fraction of the others' median."""
    if missing(flagged) or missing(other) or not other:
        return None
    return flagged / other - 1


def short_label(text: str, width: int = 42) -> str:
    """Cut a long occupation or skill name so chart labels do not squeeze the plot (the hover keeps it whole)."""
    text = str(text)
    return text if len(text) <= width else text[: width - 3].rstrip() + "..."


def wrap_label(text: str, width: int = 24) -> str:
    """Break a long axis label over lines (Plotly line break) instead of cutting it."""
    return "<br>".join(textwrap.wrap(str(text), width)) or str(text)


def group_short(mom_group: str, title: str) -> str:
    return GROUP_SHORT.get(mom_group, str(title).lower())


def group_label(group_type: str, key: str, label: str) -> str:
    if group_type == "mom_group":
        return MOM_GROUP_LABELS.get(key, label)
    return label


# --- weeks ----------------------------------------------------------------------------------------

def week_label(week) -> str:
    """The week selector's option text, e.g. 'Week ending Tue 3 Nov 2026'."""
    end = pd.Timestamp(week["week_end_date"])
    return f"Week ending {end:%a} {end.day} {end:%b %Y}"


def week_caption(week) -> str:
    start, end, snap = (pd.Timestamp(week[c]) for c in ("week_start_date", "week_end_date", "snapshot_date"))
    text = (f"{start:%a} {start.day} {start:%b} to {end:%a} {end.day} {end:%b}, read from the {snap.day} {snap:%b} "
            f"snapshot: {int(week['pool_postings']):,} open postings.")
    if not week.get("snapshot_is_week_end", True):
        text += " The week's Tuesday snapshot was missing, so an earlier day was used."
    return text


def thresholds(week) -> dict:
    stored = week.get("thresholds") if week is not None else None
    return {**DEFAULT_THRESHOLDS, **(stored if isinstance(stored, dict) else {})}


def has_previous(week) -> bool:
    return week is not None and not missing(week.get("prev_week_end_date"))


# --- shortage and barrier lists -------------------------------------------------------------------

def stability(flagged_prev_week) -> str:
    """Label for a skill given whether it was flagged the week before (None: no earlier week)."""
    if missing(flagged_prev_week):
        return FIRST
    return STABLE if bool(flagged_prev_week) else NEW


def shortage_skills(skills: pd.DataFrame) -> pd.DataFrame:
    """Skills listed in metrics.shortage_skill, highest lift first, with their stability label."""
    rows = skills[skills["shortage_listed"].fillna(False).astype(bool)]
    rows = rows.sort_values(["shortage_lift", "shortage_postings"], ascending=False)
    return rows.assign(stability=rows["shortage_listed_prev_week"].map(stability))


def barrier_skills(skills: pd.DataFrame) -> pd.DataFrame:
    """Skills the weekly run flagged as barrier skills, highest lift first, with their stability label."""
    rows = skills[skills["is_barrier"].fillna(False).astype(bool)]
    rows = rows.sort_values(["barrier_lift", "barrier_postings"], ascending=False)
    return rows.assign(stability=rows["is_barrier_prev_week"].map(stability))


def carried_over(rows: pd.DataFrame) -> tuple[int, int] | None:
    """(flagged last week too, flagged this week) for a list from shortage_skills / barrier_skills; None if
    there is no earlier week."""
    if rows.empty or (rows["stability"] == FIRST).all():
        return None
    return int((rows["stability"] == STABLE).sum()), len(rows)


def bar_text(postings, companies) -> str:
    return f"{int(postings):,} postings, {int(companies):,} employers"


# --- competitiveness ------------------------------------------------------------------------------

def spread(groups: pd.DataFrame, min_postings: int) -> dict | None:
    """Most and least applied-to group among those with min_postings open postings, and the ratio."""
    rows = groups[(groups["postings"] >= min_postings) & groups["apps_per_posting_day_24h"].notna()]
    if len(rows) < 2:
        return None
    rows = rows.sort_values("apps_per_posting_day_24h", ascending=False)
    top, bottom = rows.iloc[0], rows.iloc[-1]
    ratio = top["apps_per_posting_day_24h"] / bottom["apps_per_posting_day_24h"] if bottom["apps_per_posting_day_24h"] else None
    return {"top": top, "bottom": bottom, "ratio": ratio}


# --- market check ---------------------------------------------------------------------------------

def market_rows(market: pd.DataFrame, split: str) -> pd.DataFrame:
    """The groups of one split that MOM covers (the 'unknown' bucket has no share and is left out)."""
    return market[(market["split"] == split) & (market["mom_group"] != "unknown") & market["mcf_share_pct"].notna()]


def market_gap(market: pd.DataFrame, split: str = "A") -> dict | None:
    """The biggest over- and under-representation of a split, in words, from the stored gaps:
    {"line": what the data says, "meaning": how to read the signals, "over": short name of the group}."""
    rows = market_rows(market, split).dropna(subset=["share_gap_pp"])
    if rows.empty:
        return None
    over = rows.loc[rows["share_gap_pp"].idxmax()]
    under = rows.loc[rows["share_gap_pp"].idxmin()]
    over_name = group_short(over["mom_group"], over["group_title"])
    line = (f"{over_name[0].upper() + over_name[1:]} are {over['mcf_share_pct']:.0f}% of MyCareersFuture postings "
            f"but {over['mom_share_pct']:.0f}% of MOM vacancies ({over['share_gap_pp']:+.0f} points)")
    meaning = f"read the weekly signals as mostly about {over_name}"
    if under["mom_group"] != over["mom_group"] and under["share_gap_pp"] < 0:
        under_name = group_short(under["mom_group"], under["group_title"])
        line += f"; {under_name} are {under['mcf_share_pct']:.0f}% against {under['mom_share_pct']:.0f}%"
        meaning += f": readings for {under_name} rest on fewer postings than their share of real vacancies"
    return {"line": line + ".", "meaning": meaning[0].upper() + meaning[1:] + ".", "over": over_name}


# --- skill lookup ---------------------------------------------------------------------------------

def skill_options(skills: pd.DataFrame) -> dict:
    """skill_ref -> label, most open postings first. A name shared by two skills gets its kind added."""
    rows = skills.sort_values("open_postings", ascending=False, na_position="last")
    dupes = rows["skill_name"].duplicated(keep=False)
    labels = {}
    for ref, name, mapped, dupe in zip(rows["skill_ref"], rows["skill_name"], rows["is_mapped"], dupes):
        labels[ref] = f"{name} ({'SkillsFuture skill' if mapped else 'MCF tag'})" if dupe else str(name)
    return labels


def default_skill(skills: pd.DataFrame, shown: int = 15) -> str | None:
    """The lookup opens on the most common skill of the shortage list the Hiring signals tab shows, so a
    first-time viewer sees a filled-in example; otherwise on the most common skill."""
    if skills.empty:
        return None
    top = shortage_skills(skills).head(shown)
    pool = top if not top.empty else skills
    return pool.sort_values("open_postings", ascending=False, na_position="last")["skill_ref"].iloc[0]


def demand_verdict(row) -> dict:
    if missing(row.get("demand_week")):
        return {"value": "n/a", "delta": None,
                "caption": "No fully observed posting week in this run yet, so no fair share to show."}
    week = pd.Timestamp(row["demand_week"])
    delta = None
    if not missing(row.get("demand_share_week_before")):
        delta = points(row["demand_share"] - row["demand_share_week_before"])
    return {"value": pct(row["demand_share"]), "delta": delta,
            "caption": f"of new postings in the week of {week.day} {week:%b} ask for it "
                       f"({int(row['demand_new_postings']):,} postings)."}


def pay_verdict(row, t: dict) -> dict:
    if missing(row.get("salary_mid_median")):
        return {"value": "n/a", "delta": None,
                "caption": f"Fewer than {t['salary_min_postings']} open postings with a usable salary."}
    return {"value": money(row["salary_mid_median"]), "delta": None,
            "caption": f"median a month, {times(row['salary_vs_overall'])} the median posting. Half pay "
                       f"{money(row['salary_mid_p25'])} to {money(row['salary_mid_p75'])}."}


def _was(flag_prev_week) -> str:
    if missing(flag_prev_week):
        return ""
    return " Flagged last week too." if bool(flag_prev_week) else " Not flagged last week."


def shortage_verdict(row, t: dict) -> dict:
    if not bool(row.get("shortage_listed")):
        return {"value": "No", "delta": None, "flagged": False,
                "caption": f"Fewer than {t['shortage_min_flagged']} postings reposted {t['shortage_min_reposts']}+ "
                           f"times with at most {t['shortage_max_applications']} applications, from "
                           f"{t['min_companies']}+ employers."}
    lift = row["shortage_lift"]
    caption = (f"as likely as the average posting to be reposted {t['shortage_min_reposts']}+ times with at most "
               f"{t['shortage_max_applications']} applications ({int(row['shortage_postings']):,} postings, "
               f"{int(row['shortage_companies']):,} employers).")
    if lift < 1:
        caption += " Below 1x: no shortage signal."
    return {"value": times(lift), "delta": None, "flagged": lift >= 1,
            "caption": caption + _was(row.get("shortage_listed_prev_week"))}


def barrier_verdict(row, t: dict) -> dict:
    if missing(row.get("barrier_lift")):
        return {"value": "No", "delta": None, "flagged": False,
                "caption": f"Not enough postings: needs {t['skill_min_postings']}+ open postings from "
                           f"{t['min_companies']}+ employers."}
    if not bool(row.get("is_barrier")):
        return {"value": "No", "delta": None, "flagged": False,
                "caption": f"{times(row['barrier_lift'])} as often as similar jobs predict; flagged from "
                           f"{t['barrier_min_lift']}x with {t['barrier_min_flagged']}+ barrier postings."
                           + _was(row.get("is_barrier_prev_week"))}
    return {"value": times(row["barrier_lift"]), "delta": None, "flagged": True,
            "caption": f"as often as similar jobs predict: viewed a lot, then fewer applications per view "
                       f"({int(row['barrier_postings']):,} postings, {int(row['barrier_companies']):,} employers)."
                       + _was(row.get("is_barrier_prev_week"))}


def competition_verdict(row) -> dict:
    apps, overall = row.get("apps_per_posting_day_24h"), row.get("overall_apps_per_posting_day_24h")
    if missing(apps):
        return {"value": "n/a", "delta": None, "caption": "Fewer than 100 open postings."}
    delta = None
    if not missing(row.get("apps_per_posting_day_24h_prev_week")):
        delta = f"{apps - row['apps_per_posting_day_24h_prev_week']:+.2f} vs last week"
    if missing(overall) or not overall or not apps:
        caption = "applications per posting per day."
    elif apps < overall:
        caption = (f"applications per posting per day, {overall / apps:.1f} times fewer than the average posting "
                   f"({overall:.2f}): less competition.")
    else:
        caption = (f"applications per posting per day, {apps / overall:.1f} times the average posting "
                   f"({overall:.2f}): more competition.")
    return {"value": f"{apps:.2f}", "delta": delta, "caption": caption}


def lookup_summary(row, t: dict) -> str:
    """One sentence answering the business question for one skill."""
    causes = []
    shortage = shortage_verdict(row, t)
    if shortage["flagged"]:
        causes.append(f"a shortage signal ({shortage['value']})")
    barrier = barrier_verdict(row, t)
    if barrier["flagged"]:
        causes.append(f"a barrier signal ({barrier['value']})")
    name = row["skill_name"]
    if causes:
        text = f"{name} shows " + " and ".join(causes) + "."
    else:
        text = f"{name} shows no shortage or barrier signal this week."
    apps, overall = row.get("apps_per_posting_day_24h"), row.get("overall_apps_per_posting_day_24h")
    if not missing(apps) and not missing(overall) and overall:
        text += f" Its postings get {'fewer' if apps < overall else 'more'} applicants than average"
        if not missing(row.get("salary_vs_overall")):
            text += f" and pay {times(row['salary_vs_overall'])} the median posting"
        text += "."
    return text
