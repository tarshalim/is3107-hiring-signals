"""Read-only critical-review analysis behind docs/private/critical-review.md (8 Nov 2026).

It only runs SELECTs: nothing in the warehouse is written. It needs pandas and the warehouse connection, so it runs
inside the dashboard container (which has no scipy: rank correlations are Pearson on ranks). From the repo root:

    PowerShell:  Get-Content analysis/critical_review.py -Raw | docker compose exec -T dashboard python - full
    bash:        docker compose exec -T dashboard python - full < analysis/critical_review.py

Arguments: none runs sections D to K; "full" adds A to C (the slow employer-clustered bootstraps).

The numbers the report cites are the 8 Nov output, saved in docs/private/critical-review-output-8Nov.txt. A rerun
reads the warehouse as it is then, so after a week is rebuilt (or once the 10 Nov week exists) the values will
differ from that file. WEEKS below fixes which weeks are compared.
"""
import sys
sys.path.insert(0, "/app")
import numpy as np
import pandas as pd
from dashboard.db import run_query

pd.set_option("display.width", 250)
pd.set_option("display.max_columns", 40)
pd.set_option("display.max_rows", 200)
rng = np.random.default_rng(3107)
B = 2000
WEEKS = ["2026-10-27", "2026-11-03"]
COMMISSION = ["REZOLUTE GLOBALZ", "STRIVE MARKETING", "NXTLVL", "WAVEWORKS", "FORTE MARKETING", "NOVEX ORGANIZATION",
              "ALTIUS ORG", "THRIVE ORGANIZATION", "YVOLVE", "ELEVATEH"]

pw = run_query("""
SELECT pw.week_end_date::text AS w, pw.job_uuid, pw.company_name, pw.ssoc_code, left(pw.ssoc_code,2) AS ssoc2,
       pw.ssoc_major_group AS maj, pw.mom_group, pw.category, pw.position_level, pw.min_years_exp, pw.exp_out_of_range,
       pw.salary_mid, pw.salary_usable, pw.views_total, pw.applications_total, pw.repost_count,
       pw.days_since_original_posting AS age, pw.apps_per_day_since_posting AS apd, pw.views_per_day_since_posting AS vpd,
       pw.is_shortage, pw.is_barrier_posting, pw.is_reposted_many_apps, pw.is_entry_2plus, pw.is_junior_2plus,
       pw.posted_by_agency AS agency, pw.barrier_group, j.company_ssic_code AS ssic
FROM metrics.posting_week pw JOIN clean.dim_job j ON j.job_key = pw.job_key
""")
pw["commission"] = pw.company_name.str.upper().str.contains("|".join(COMMISSION), regex=True)
print("rows", pw.groupby("w").size().to_dict())

listed = run_query("""
SELECT DISTINCT skill_ref FROM metrics.shortage_skill
UNION SELECT DISTINCT skill_ref FROM metrics.barrier_skill WHERE is_barrier
UNION SELECT DISTINCT COALESCE(ssg_skill_id, mcf_skill_uuid) FROM clean.dim_skill
      WHERE lower(mcf_skill_name) IN ('tendering','civil engineering','supplier sourcing','electrical','autodesk revit',
                                      'hvac','b2b sales','cost planning','quantity surveying','collaboration')
""")
refs = listed.skill_ref.tolist()
ps = run_query("""
SELECT DISTINCT pw.week_end_date::text AS w, pw.job_uuid, COALESCE(s.ssg_skill_id, s.mcf_skill_uuid) AS skill_ref
FROM metrics.posting_week pw
JOIN clean.bridge_job_skill b ON b.job_key = pw.job_key
JOIN clean.dim_skill s ON s.skill_key = b.skill_key
WHERE COALESCE(s.ssg_skill_id, s.mcf_skill_uuid) = ANY(%(r)s)
""", {"r": refs})
names = run_query("""
SELECT DISTINCT ON (COALESCE(ssg_skill_id, mcf_skill_uuid)) COALESCE(ssg_skill_id, mcf_skill_uuid) AS skill_ref,
       COALESCE(ssg_skill_title, mcf_skill_name) AS skill_name, COALESCE(is_casl,false) AS is_casl
FROM clean.dim_skill ORDER BY COALESCE(ssg_skill_id, mcf_skill_uuid), skill_key""")
ps = ps.merge(names, on="skill_ref")


def cluster_ci(df, num_col, den_col=None, ref=1.0, b=B):
    """Employer-clustered bootstrap of sum(num)/sum(den)/ref. df has company_name. Returns (est, lo, hi)."""
    g = df.groupby("company_name").agg(n=(num_col, "size"), k=(num_col, "sum"))
    if den_col is not None:
        g["d"] = df.groupby("company_name")[den_col].sum()
    else:
        g["d"] = g["n"]
    k, d = g.k.to_numpy(float), g.d.to_numpy(float)
    est = k.sum() / d.sum() / ref
    E = len(g)
    out = []
    for start in range(0, b, 250):
        m = min(250, b - start)
        wts = rng.multinomial(E, np.full(E, 1 / E), size=m)
        out.append((wts @ k) / (wts @ d) / ref)
    out = np.concatenate(out)
    return est, np.quantile(out, 0.025), np.quantile(out, 0.975)


def wilson(k, n, z=1.96):
    p = k / n
    den = 1 + z * z / n
    c = (p + z * z / (2 * n)) / den
    h = z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return c - h, c + h


WEEKS_ABC = WEEKS if "full" in sys.argv else []
print("\n=== A. Headline shares: estimate, Wilson 95% CI, employer-clustered bootstrap 95% CI ===")
rows = []
for w in WEEKS_ABC:
    d = pw[pw.w == w]
    direct = d[~d.agency]
    entry = d[(d.position_level == "Fresh/entry level") & ~d.exp_out_of_range & (d.min_years_exp <= 30)]
    junior = d[(d.position_level == "Junior Executive") & ~d.exp_out_of_range & (d.min_years_exp <= 30)]
    for name, sub, col in [("agency share", d, "agency"), ("shortage share (direct)", direct, "is_shortage"),
                           ("barrier posting share", d, "is_barrier_posting"),
                           ("reposted many apps (direct)", direct, "is_reposted_many_apps"),
                           ("entry 2+ yrs", entry, "is_entry_2plus"), ("junior 2+ yrs", junior, "is_junior_2plus")]:
        sub = sub.assign(f=sub[col].astype(int))
        k, n = sub.f.sum(), len(sub)
        lo, hi = wilson(k, n)
        e, blo, bhi = cluster_ci(sub, "f")
        rows.append([w, name, k, n, sub.company_name.nunique(), round(e * 100, 2), f"{lo*100:.2f}-{hi*100:.2f}",
                     f"{blo*100:.2f}-{bhi*100:.2f}"])
if rows:
    print(pd.DataFrame(rows, columns=["week", "share", "k", "n", "employers", "pct", "wilson95", "cluster95"]).to_string(index=False))


def weighted_median(x, w):
    o = np.argsort(x)
    x, w = x[o], w[o]
    c = np.cumsum(w)
    return x[np.searchsorted(c, c[-1] / 2)]


print("\n=== B. Pay gap flagged vs other (median salary mid, usable salaries), employer-clustered bootstrap ===")
for w in WEEKS_ABC:
    d = pw[(pw.w == w) & pw.salary_usable & ~pw.exp_out_of_range & (pw.min_years_exp <= 30)]
    for lvl, col in [("Fresh/entry level", "is_entry_2plus"), ("Junior Executive", "is_junior_2plus")]:
        s = d[d.position_level == lvl].reset_index(drop=True)
        comp = s.company_name.astype("category").cat.codes.to_numpy()
        E = comp.max() + 1
        x = s.salary_mid.to_numpy(float)
        f = s[col].to_numpy(bool)
        est = np.median(x[f]) / np.median(x[~f]) - 1
        bs = []
        for _ in range(500):
            ew = rng.multinomial(E, np.full(E, 1 / E))
            pwt = ew[comp]
            m1, m0 = (pwt > 0) & f, (pwt > 0) & ~f
            bs.append(weighted_median(x[m1], pwt[m1]) / weighted_median(x[m0], pwt[m0]) - 1)
        print(w, lvl, f"n_flagged={f.sum()} n_other={(~f).sum()} gap={est:+.1%} CI95 {np.quantile(bs,.025):+.1%}..{np.quantile(bs,.975):+.1%}")

print("\n=== C. Entry 2+ by agency vs direct, and years asked ===")
for w in WEEKS_ABC:
    d = pw[(pw.w == w) & (pw.position_level == "Fresh/entry level") & ~pw.exp_out_of_range & (pw.min_years_exp <= 30)]
    print(w, d.groupby("agency").is_entry_2plus.agg(["sum", "size", "mean"]).round(4).to_dict("index"))
    print("   years among flagged:", d[d.is_entry_2plus].min_years_exp.value_counts().sort_index().head(8).to_dict())
    top = d[d.is_entry_2plus].company_name.value_counts()
    print(f"   flagged employers={top.size}, top employer share={top.iloc[0]/top.sum():.1%} ({top.index[0]}), top10 share={top.head(10).sum()/top.sum():.1%}")

print("\n=== D. Skill lifts: original, CI, occupation-adjusted, without commission firms, other week ===")


def skill_table(w, kind):
    d = pw[pw.w == w].copy()
    if kind == "shortage":
        d = d[~d.agency]
        flag = "is_shortage"
        grp_share = d.groupby("ssoc2")[flag].mean()
        d["exp_occ"] = d.ssoc2.map(grp_share)
        overall = d[flag].mean()
    else:
        flag = "is_barrier_posting"
        grp_share = d.groupby("barrier_group")[flag].mean()
        d["exp_occ"] = d.barrier_group.map(grp_share)
        overall = d[flag].mean()
    j = ps[ps.w == w].merge(d, on=["w", "job_uuid"])
    out = []
    for (ref, nm), s in j.groupby(["skill_ref", "skill_name"]):
        s = s.assign(f=s[flag].astype(int))
        k, n = s.f.sum(), len(s)
        if n < 100 or k < 10:
            continue
        flagged = s[s.f == 1]
        topshare = flagged.company_name.value_counts().iloc[0] / k
        if kind == "shortage":
            est, lo, hi = cluster_ci(s, "f", ref=overall)
            adj = (k / n) / s.exp_occ.mean()
        else:
            # barrier lift is already within occupation: expected = mean group share
            est, lo, hi = cluster_ci(s.assign(e=s.exp_occ), "f", den_col="e")
            adj = est
        nc = s[~s.commission]
        kc, nn = nc.f.sum(), len(nc)
        lift_nc = (kc / nn) / (overall if kind == "shortage" else nc.exp_occ.mean()) if nn else np.nan
        out.append(dict(skill=nm, n=n, k=k, emp=flagged.company_name.nunique(), top=round(topshare, 2),
                        lift=round(est, 2), ci=f"{lo:.2f}-{hi:.2f}", occ_adj=round(adj, 2),
                        comm_share=round(flagged.commission.mean(), 2), lift_no_comm=round(lift_nc, 2), k_no_comm=kc))
    return pd.DataFrame(out).set_index("skill")


for kind in ["shortage", "barrier"]:
    t27, t03 = skill_table(WEEKS[0], kind), skill_table(WEEKS[1], kind)
    stored = run_query(f"SELECT week_end_date::text w, skill_name FROM metrics.{kind}_skill"
                       + (" WHERE is_barrier" if kind == "barrier" else ""))
    l27 = set(stored[stored.w == WEEKS[0]].skill_name)
    l03 = set(stored[stored.w == WEEKS[1]].skill_name)
    print(f"\n--- {kind}: listed 27 Oct={len(l27)}, 3 Nov={len(l03)}, both={len(l27 & l03)}, Jaccard={len(l27&l03)/len(l27|l03):.2f}")
    for w, t, lst, other in [(WEEKS[1], t03, l03, t27), (WEEKS[0], t27, l27, t03)]:
        show = t[t.index.isin(lst)].copy()
        show["other_wk_lift"] = other.lift.reindex(show.index)
        show["other_wk_k"] = other.k.reindex(show.index)
        print(f"\n{kind} listed, week {w}:")
        print(show.sort_values("lift", ascending=False).to_string())
    both = t27.join(t03, lsuffix="_27", rsuffix="_03", how="inner")
    print(f"\n{kind}: skills with >=10 flagged both weeks: {len(both)}; Spearman of lift = "
          f"{both.lift_27.rank().corr(both.lift_03.rank()):.2f}; Pearson = {both.lift_27.corr(both.lift_03):.2f}")
    # extra skills of interest not listed
    for nm in ["Tendering", "Civil Engineering", "Supplier Sourcing", "Electrical", "Autodesk Revit", "HVAC", "B2B Sales",
               "Cost Planning", "Collaboration"]:
        if kind == "shortage":
            print(f"   {nm}: 27 Oct {t27.loc[nm, ['k','lift','ci','occ_adj']].to_dict() if nm in t27.index else '-'}"
                  f" | 3 Nov {t03.loc[nm, ['k','lift','ci','occ_adj']].to_dict() if nm in t03.index else '-'}")

print("\n=== E. Does shortage overlap barrier? (posting level, direct employers) ===")
for w in WEEKS:
    d = pw[(pw.w == w) & ~pw.agency]
    print(w, pd.crosstab(d.is_shortage, d.is_barrier_posting).to_dict())

print("\n=== F. Shortage postings: age, reposts, PMET/EP-salary mix, commission firms, top employers ===")
for w in WEEKS:
    d = pw[(pw.w == w) & ~pw.agency]
    s, o = d[d.is_shortage], d[~d.is_shortage]
    print(w, f"median age shortage={s.age.median()} other={o.age.median()}; median views shortage={s.views_total.median()} "
             f"other={o.views_total.median()}; PMET shortage={(s.mom_group=='PMET').mean():.1%} other={(o.mom_group=='PMET').mean():.1%}; "
             f"salary_mid>=5600 shortage={(s.salary_usable & (s.salary_mid>=5600)).mean():.1%} other={(o.salary_usable & (o.salary_mid>=5600)).mean():.1%}; "
             f"commission share of shortage={s.commission.mean():.1%} of direct postings={d.commission.mean():.1%}")
    print("   repost_count distribution (all open):", pw[pw.w == w].repost_count.value_counts().sort_index().to_dict())
    print("   zero-application share:", round((pw[pw.w == w].applications_total == 0).mean(), 3),
          " median views/apps:", pw[pw.w == w].views_total.median(), pw[pw.w == w].applications_total.median())
    tops = s.groupby(["company_name", "ssic"]).size().sort_values(ascending=False).head(12)
    print("   top direct-employer shortage posters:\n", tops.to_string())

print("\n=== G. Sensitivity of shortage cluster to the rule (3 Nov) ===")
d = pw[pw.w == WEEKS[1]].copy()
j = ps[ps.w == WEEKS[1]].merge(d, on=["w", "job_uuid"])
cluster = ["Tendering", "Quantity Surveying", "Cost Control", "Technical Drawing", "Civil Engineering"]
soft = ["Adaptability", "Customer Orientation", "Developing People", "Spa"]
variants = {
    "base: reposts>=2, apps<=2, direct": lambda x: (x.repost_count >= 2) & (x.applications_total <= 2) & ~x.agency,
    "apps<=1": lambda x: (x.repost_count >= 2) & (x.applications_total <= 1) & ~x.agency,
    "reposts>=1": lambda x: (x.repost_count >= 1) & (x.applications_total <= 2) & ~x.agency,
    "apps/day<=0.05": lambda x: (x.repost_count >= 2) & (x.apd <= 0.05) & ~x.agency,
    "incl agencies": lambda x: (x.repost_count >= 2) & (x.applications_total <= 2),
    "direct, no commission": lambda x: (x.repost_count >= 2) & (x.applications_total <= 2) & ~x.agency & ~x.commission,
}
res = {}
for vn, fn in variants.items():
    base = d[~d.agency] if "agencies" not in vn else d
    if "commission" in vn:
        base = base[~base.commission]
    ov = fn(base).mean()
    jj = j[j.job_uuid.isin(base.job_uuid)]
    ff = fn(jj)
    res[vn] = {nm: round(ff[jj.skill_name == nm].mean() / ov, 2) for nm in cluster + soft}
print(pd.DataFrame(res).to_string())

print("\n=== H. Group level: do signals separate? Spearman across job categories (3 Nov) ===")
gs = run_query("""SELECT g.week_end_date::text w, g.group_type, g.group_key, g.postings, g.shortage_share, g.barrier_share,
                  g.entry_2plus_share, g.agency_share, c.apps_per_posting_day_24h apd24, c.median_apps_per_day_since_posting med_apd
                  FROM metrics.group_signals g JOIN metrics.competitiveness c USING (week_end_date, group_type, group_key)
                  WHERE g.group_type IN ('category','ssoc_major_group')""")
for w in WEEKS:
    c = gs[(gs.w == w) & (gs.group_type == "category") & (gs.postings >= 500)]
    print(w, "categories (500+):", len(c))
    print(c[["shortage_share", "barrier_share", "entry_2plus_share", "agency_share", "apd24"]].rank().corr().round(2).to_string())
m = gs[gs.group_type == "category"].pivot(index="group_key", columns="w", values=["shortage_share", "barrier_share", "apd24"])
for col in ["shortage_share", "barrier_share", "apd24"]:
    print(f"category {col}: Spearman 27 Oct vs 3 Nov = {m[col].rank().corr().iloc[0,1]:.2f}")
mg = gs[gs.group_type == "ssoc_major_group"].pivot(index="group_key", columns="w", values="apd24")
print("major group apd24:\n", mg.round(3).to_string())

print("\n=== I. Competitiveness occupations (30+ postings): stability ===")
cp = run_query("""SELECT week_end_date::text w, group_key, group_label, postings, apps_per_posting_day_24h a
                  FROM metrics.competitiveness WHERE group_type='ssoc' AND postings >= 30""")
p = cp.pivot(index="group_label", columns="w", values="a").dropna()
print("occupations both weeks:", len(p), "Spearman:", round(p.rank().corr().iloc[0, 1], 2))
for w in WEEKS:
    t = cp[cp.w == w].sort_values("a")
    print(w, "fewest:", t.head(8)[["group_label", "postings", "a"]].values.tolist())
    print(w, "most:", t.tail(8)[["group_label", "postings", "a"]].values.tolist())
bot = set(cp[cp.w == WEEKS[0]].nsmallest(20, "a").group_label) & set(cp[cp.w == WEEKS[1]].nsmallest(20, "a").group_label)
top = set(cp[cp.w == WEEKS[0]].nlargest(20, "a").group_label) & set(cp[cp.w == WEEKS[1]].nlargest(20, "a").group_label)
print("bottom-20 overlap", len(bot), "top-20 overlap", len(top))

print("\n=== J. Skill salary stability ===")
ss = run_query("SELECT week_end_date::text w, skill_name, postings, companies, salary_mid_median m, mid_vs_overall r FROM metrics.skill_salary")
q = ss.pivot(index="skill_name", columns="w", values="m").dropna()
print("skills both weeks", len(q), "Spearman", round(q.rank().corr().iloc[0, 1], 3))
for k in (10, 20):
    a = set(ss[ss.w == WEEKS[0]].nlargest(k, "m").skill_name)
    b = set(ss[ss.w == WEEKS[1]].nlargest(k, "m").skill_name)
    print(f"top {k} overlap {len(a & b)}")
print(ss[ss.w == WEEKS[1]].nlargest(10, "m").to_string(index=False))

print("\n=== K. CASL base rate ===")
print(run_query("""SELECT count(*) FILTER (WHERE is_casl) casl_skills, count(*) all_skills FROM clean.dim_skill""").to_string())
for t, extra in [("shortage_skill", ""), ("barrier_skill", "")]:
    print(t, run_query(f"""SELECT week_end_date::text w, count(*) rows, count(*) FILTER (WHERE is_casl) casl
                           FROM metrics.{t} GROUP BY 1 ORDER BY 1""").to_dict("records"))
print("barrier flagged:", run_query("""SELECT week_end_date::text w, count(*) FILTER (WHERE is_barrier) flagged,
       count(*) FILTER (WHERE is_barrier AND is_casl) casl FROM metrics.barrier_skill GROUP BY 1""").to_dict("records"))

