"""Which day a daily_mcf_transform run loads, and how long it took. Kept free of Airflow so it is
unit-tested offline (tests/test_run_context.py); dags/daily_transform_dag.py calls these.
"""
from transform.snapshot_week import SGT


def snapshot_date_for_run(override, data_interval_end) -> str:
    """The snapshot day a run loads, 'YYYY-MM-DD'.

    Run config {"snapshot_date": ...} wins. Otherwise the Singapore date of the run's data_interval_end:
    the 10:30 SGT slot the run belongs to. So a scheduled run loads its own day, and clearing an old run
    reloads THAT run's day (the wall clock would reload today's). A manual trigger gets the latest 10:30
    slot before it (before 10:30 that is yesterday); pass snapshot_date in the config to pick a day.
    """
    if override:
        return str(override)
    return data_interval_end.astimezone(SGT).date().isoformat()
