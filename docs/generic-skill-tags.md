# Generic skill tags: exclusion list

**Status: approved 8 Nov 2026** (with "Electrical" removed from the draft), applied from the weeks ending 27 Oct
and 3 Nov on.

## Why

Some MyCareersFuture skill tags are too vague to read as a skill. "Cuts" can mean hair cuts, meat cuts or cost
cuts, so a "Cuts" shortage says nothing about which job is hard to fill. On 3 Nov, tags like these were near the
top of the shortage list (Cuts 5.2x, Disposition 4.1x, Treatment 3.1x).

The tags below are left out of every **skill-level** signal: `barrier_skill`, `shortage_skill`,
`competitiveness` by skill, `skill_demand` and `skill_salary`. The postings themselves and their flags stay, so
group-level numbers (the summary grid, competitiveness by occupation, unrealistic requirements) do not change.

The list lives in `data/reference/generic_skill_tags.csv` (columns `mcf_skill_name`, `kind`, `reason`) and is
stored with every weekly run in `metrics.run_log.thresholds.generic_skill_tags`. To change it, edit the CSV and
rerun the weeks.

## The list

Kinds: **ambiguous** (several meanings), **fragment** (part of a longer skill), **sector** (an industry name,
which the occupation already gives).

| Tag | Kind | Reason |
|---|---|---|
| Cuts | ambiguous | Hair cuts, meat or fish cuts, or cost cuts: the tag alone does not say which job |
| Disposition | ambiguous | A person's temperament, disposal of waste or assets, or a legal or loan disposition |
| Treatment | ambiguous | Medical treatment, beauty or spa treatment, or water and waste treatment |
| Drawing | ambiguous | Art, technical drawing or drawing up documents (Technical Drawing is its own tag) |
| Painting | ambiguous | Fine art or building and spray painting |
| Design | ambiguous | Graphic, interior, fashion, engineering or software design |
| Screening | ambiguous | Health screening, candidate screening or security screening |
| Management | ambiguous | Does not say management of what: people, projects, stock or a business |
| Mechanical | fragment | Part of a longer skill: mechanical trade work or mechanical engineering (Mechanical Engineering is its own tag) |
| Structural | fragment | Part of a longer skill: structural engineering, steelwork or structural design |
| Construction | sector | An industry, not a skill: it marks the sector the posting is in, which the occupation already does |
| Manufacturing | sector | An industry, not a skill: it marks the sector the posting is in, which the occupation already does |
| Healthcare | sector | An industry, not a skill: it marks the sector the posting is in, which the occupation already does |
| Hospitality | sector | An industry, not a skill: it marks the sector the posting is in, which the occupation already does |
| Finance | sector | A field, not a skill: accounting, financial analysis and reporting have their own tags |

All 15 are single MCF tags, and none is mapped to a SkillsFuture skill, so matching on the tag name is exact.
None of them appears in `data/reference/skill_aliases.csv`.

## Open postings per tag (week ending 3 Nov)

| Tag | Open postings |
|---|---|
| Construction | 7,066 |
| Manufacturing | 3,920 |
| Treatment | 3,171 |
| Healthcare | 3,065 |
| Drawing | 2,349 |
| Hospitality | 2,087 |
| Design | 1,525 |
| Screening | 1,512 |
| Mechanical | 1,478 |
| Management | 1,443 |
| Disposition | 584 |
| Finance | 451 |
| Cuts | 350 |
| Structural | 236 |
| Painting | 175 |

## Effect on the week ending 3 Nov

| List | Before | Expected after | Tags that drop out |
|---|---|---|---|
| Shortage skills | 41 | 34 | Cuts, Disposition, Treatment, Healthcare, Manufacturing, Drawing, Construction |
| Barrier skills | 43 | 41 | Painting, Structural |

The other skills keep the same numbers: each skill is computed on its own, and no postings are removed.

## Left off on purpose

| Tag | Why it stays |
|---|---|
| Spa | Points to one family of jobs (spa therapists) |
| Medicine | Points to one family of jobs (medical and pharmacy roles) |
| Networking | Already mapped to SkillsFuture's Networking skill |
| Electrical | Still points to one field of work (electrical trades and engineering), and at 5,092 open postings on 3 Nov it is too large to drop. Dropped from the draft at review |
| Soft skills (Communication, Adaptability, Customer Orientation, Self Management, ...) | Clear meaning, just broad. If they should leave the shortage list, it is better to show technical skills (SkillsFuture type `tsc`) apart than to add them here |

