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
| Foot | fragment | Part of a longer skill: foot reflexology, foot massage or foot care (topped the 27 Oct shortage list at 6.9x; added 8 Nov) |
| Construction | sector | An industry, not a skill: it marks the sector the posting is in, which the occupation already does |
| Manufacturing | sector | An industry, not a skill: it marks the sector the posting is in, which the occupation already does |
| Healthcare | sector | An industry, not a skill: it marks the sector the posting is in, which the occupation already does |
| Hospitality | sector | An industry, not a skill: it marks the sector the posting is in, which the occupation already does |
| Finance | sector | A field, not a skill: accounting, financial analysis and reporting have their own tags |

All 16 are single MCF tags, and none is mapped to a SkillsFuture skill, so matching on the tag name is exact.
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
| Foot | 328 |
| Structural | 236 |
| Painting | 175 |

## Effect (measured when the list was first applied, 8 Nov)

Both weeks were first rebuilt with the list from `main` (commit `778014b`), then three more times on 8 Nov as other
rules changed, the last after `97c3f85` (21:39 SGT, all checks passing). Each run records the 16 tags in
`run_log.thresholds.generic_skill_tags`.

The table below is the list's own effect, measured at that first rebuild with 15 tags (before Foot was added) and
before the shortage list needed a lift of at least 1.5:

| Week ending | Shortage skills (before -> after) | Barrier skills (before -> after) |
|---|---|---|
| 27 Oct | 53 -> 47 | 59 -> 58 |
| 3 Nov | 41 -> 34 | 43 -> 41 |

With all 16 tags and the lift rule, the shortage lists now hold 25 skills (27 Oct) and 14 (3 Nov); the barrier
counts are unchanged at 58 and 41.

For 3 Nov the tags that dropped out are Cuts, Disposition, Treatment, Healthcare, Manufacturing, Drawing and
Construction from shortage, and Painting and Structural from barrier, as expected. Electrical stayed on the
shortage list then, but has been off it since the lift rule (1.21x on 3 Nov, under 1.5). The other skills keep the
same numbers: each skill is computed on its own, and no postings are removed.

### New top 10 shortage skills, week ending 3 Nov

Direct employers only, generic tags excluded.

| # | Skill | Lift | Shortage postings | Employers | Biggest employer's share |
|---|---|---|---|---|---|
| 1 | Spa | 4.4x | 26 | 16 | 12% |
| 2 | Tendering | 3.0x | 26 | 26 | 4% |
| 3 | Adaptability | 2.6x | 62 | 17 | 34% |
| 4 | Quantity Surveying | 2.4x | 33 | 33 | 3% |
| 5 | Cost Control | 2.1x | 36 | 31 | 8% |
| 6 | Customer Orientation | 2.1x | 351 | 114 | 31% |
| 7 | Supplier Sourcing | 1.8x | 32 | 28 | 9% |
| 8 | Technical Drawing | 1.8x | 74 | 60 | 7% |
| 9 | Developing People | 1.7x | 224 | 68 | 39% |
| 10 | Civil Engineering | 1.7x | 64 | 54 | 5% |

How to read it:

- **Spa is first on only 26 shortage postings**, just over the minimum of 25, so it is a weak first place.
- **Construction and cost skills are half the list** (Tendering, Quantity Surveying, Cost Control, Technical
  Drawing, Civil Engineering), each spread over many employers (biggest employer 3% to 8%): the strongest
  shortage story.
- **Three soft skills are in the top 10** (Adaptability, Customer Orientation, Developing People). They are broad,
  and one employer is behind 31% to 39% of their shortage postings. Showing technical skills apart on the shortage
  chart would keep them from crowding out the sector story (see "Left off on purpose").

### Unrealistic requirements, week ending 3 Nov (same rebuild)

| Flag | Share | Postings | Median pay, flagged vs others |
|---|---|---|---|
| Entry level asking 2+ years (lead) | 6.3% | 914 of 14,611 | $3,445 vs $3,400 (+1%) |
| Junior Executive asking 2+ years | 47% | 4,386 of 9,416 | $3,800 vs $3,150 (+21%) |
| Reposted despite many applicants (minor, direct employers only) | 1.0% | 527 of 50,496 | $4,600 vs $4,000 |

## Left off on purpose

| Tag | Why it stays |
|---|---|
| Spa | Points to one family of jobs (spa therapists) |
| Medicine | Points to one family of jobs (medical and pharmacy roles) |
| Networking | Already mapped to SkillsFuture's Networking skill |
| Electrical | Still points to one field of work (electrical trades and engineering), and at 5,092 open postings on 3 Nov it is too large to drop. Dropped from the draft at review |
| Soft skills (Communication, Adaptability, Customer Orientation, Self Management, ...) | Clear meaning, just broad. If they should leave the shortage list, it is better to show technical skills (SkillsFuture type `tsc`) apart than to add them here |

