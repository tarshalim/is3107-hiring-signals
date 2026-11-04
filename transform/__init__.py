"""Cleaning and loading of daily MCF snapshots into the clean star schema (IS3107 role 2).

Modules:
    parse              raw JSON payload -> slim typed row (pure, unit-tested)
    scd                slowly changing dimension (Type 2) planner for dim_job (pure, unit-tested)
    deltas             day-over-day increase arithmetic for the fact table (pure, unit-tested)
    skill_mapping      MCF skill tag -> SkillsFuture Unique Skills List (exact, alias, stem, fuzzy)
    load_reference     loads the SkillsFuture and SSOC files into clean.ref_* tables
    transform_snapshot loads one snapshot day end to end, idempotently
    run                command line entry point used outside Airflow
    posting_history    weekly posting history rebuilt from posting dates (ext schema, docs/extra-data.md)
    db                 connection helper (reads .env, or WAREHOUSE_DSN)
"""
