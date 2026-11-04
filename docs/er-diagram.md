# ER diagram: clean layer (star schema)

Source of truth: `sql/init/02_clean_schema.sql`. Grain of the fact table: **one posting on one snapshot day**.
Rendered PNG: none in the repo (no Mermaid renderer available offline on the build laptop). Paste the block below
into https://mermaid.live or the GitHub markdown preview to render it, then screenshot for the report.

```mermaid
erDiagram
    dim_date  ||--o{ fact_job_day     : "date_key"
    dim_job   ||--o{ fact_job_day     : "job_key (version live that day)"
    dim_job   ||--o{ bridge_job_skill : "job_key"
    dim_skill ||--o{ bridge_job_skill : "skill_key"
    ref_ssg_skill ||--o{ dim_skill    : "ssg_skill_id (nullable)"
    ref_ssg_skill ||--o{ ref_skill_alias : "skill_id"
    ref_ssoc  ||..o{ dim_job          : "ssoc_code (denormalised title)"

    fact_job_day {
        date     snapshot_date PK "with job_uuid"
        text     job_uuid PK
        bigint   job_key FK
        int      date_key FK
        int      views_total "running total, semi-additive"
        int      applications_total "running total, semi-additive"
        smallint repost_count
        smallint edit_count
        text     status "Open / Re-open"
        smallint days_since_new_posting
        smallint days_since_original_posting
        smallint days_to_expiry
        date     prev_snapshot_date
        numeric  hours_since_prev "16.8 on 25 Oct"
        boolean  is_first_snapshot
        int      views_delta "additive"
        int      applications_delta "additive"
        boolean  reposted_since_prev
        boolean  edited_since_prev
    }
    dim_job {
        bigint   job_key PK "surrogate, one per VERSION"
        text     job_uuid "natural key (MCF uuid)"
        text     job_post_id
        text     title
        text     company_uen
        text     company_name
        text     company_ssic_code
        text     company_ssic_desc
        boolean  is_posted_on_behalf
        text     hiring_company_name
        text     ssoc_code "5-digit text"
        text     ssoc_title "from ref_ssoc"
        smallint ssoc_major_group
        text     mom_group "PMET / clerical_sales_services / production_transport"
        text     position_level
        text     position_levels
        text     employment_types
        text     category "first MCF category"
        text     categories
        text     education_code
        smallint min_years_exp_raw "0 to 88 as typed"
        smallint min_years_exp "capped at 30"
        boolean  exp_out_of_range
        numeric  salary_min
        numeric  salary_max
        numeric  salary_mid "generated"
        text     salary_type
        boolean  salary_is_hidden
        int      vacancies
        date     original_posting_date
        date     new_posting_date
        date     expiry_date
        text     source_code
        text     attr_hash "SCD2 change detector"
        date     valid_from "SCD2"
        date     valid_to "SCD2, NULL while current"
        boolean  is_current
        date     last_seen_date
    }
    dim_skill {
        int      skill_key PK
        text     mcf_skill_uuid "unique"
        text     mcf_skill_name "raw tag"
        text     norm_name
        text     ssg_skill_id FK "NULL if unmatched"
        text     ssg_skill_title
        text     ssg_skill_type "tsc / ccs"
        boolean  is_emerging
        boolean  is_casl
        text     match_method "exact / alias / stem / fuzzy / unmatched"
        numeric  match_confidence
        date     first_seen_date
        date     last_seen_date
    }
    bridge_job_skill {
        bigint   job_key PK
        int      skill_key PK
        boolean  is_key_skill
    }
    dim_date {
        int      date_key PK "yyyymmdd"
        date     date
        smallint year
        smallint month
        smallint day
        smallint day_of_week
        text     day_name
        boolean  is_weekend
        date     week_start "Monday"
        smallint iso_week
        timestamptz snapshot_fetched_at "end of that day's crawl"
    }
    ref_ssg_skill {
        text     skill_id PK
        text     skill_title
        text     norm_title
        text     skill_type
        boolean  is_emerging
        boolean  is_casl
    }
    ref_skill_alias {
        text     norm_alias PK
        text     skill_id FK
        text     source "curated / tsc_sector_title / tsc_previous_title"
        numeric  confidence
    }
    ref_ssoc {
        text     ssoc_code PK
        text     ssoc_title
        smallint major_group
        text     major_group_title
        text     mom_group
    }
```

Operational tables outside the star (not drawn): `clean.rejected_rows` (dead letter table), `clean.load_log`
(step timings per run), `clean.dq_results` (one verdict per check per day). The raw layer stays as it was:
`raw.mcf_job_snapshots(snapshot_date, job_uuid, fetched_at, payload JSONB)` and `raw.ingest_runs`.

## Reading the diagram in Kimball's terms (L8)

| Step | Answer |
|---|---|
| 1. Business process | A posting is open on MyCareersFuture and job seekers view it and apply |
| 2. Grain | One posting on one snapshot day (`fact_job_day`) |
| 3. Dimensions | `dim_job` (what, who, which occupation: company and SSOC denormalised in), `dim_date` (when), `dim_skill` through `bridge_job_skill` (which skills) |
| 4. Facts | `views_total`, `applications_total`, `repost_count`, `edit_count` (semi-additive running totals) and `views_delta`, `applications_delta` (additive increases) |

The fact table is long and narrow (180,741 rows for two days, 19 columns), the dimensions are short and wide
(`dim_job` has 40 columns), which is the shape the lecture described.
