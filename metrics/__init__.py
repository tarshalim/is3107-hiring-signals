"""Weekly hiring signals built from the clean star schema into the metrics schema (IS3107 role 3).

Modules:
    weeks       the week's dates, the crawl-skip window, which postings count as open (pure, unit-tested)
    build_week  the SQL for each signal table, the checks and the run log, one week at a time, idempotently
    run         command line entry point used outside Airflow (python -m metrics.run --week-ending YYYY-MM-DD)

The week and the snapshot come from transform/snapshot_week.py, like the MOM comparison. The Airflow tasks
are in dags/weekly_metrics_dag.py. Table meanings and deployment: docs/weekly-metrics.md.
"""
