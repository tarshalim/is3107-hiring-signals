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


def run_timings(elapsed_seconds, run_start, sensor_end, now) -> dict:
    """Times for the 'dag_run' row of clean.load_log.

    seconds is the transform's own run time (transform_snapshot's elapsed_seconds), the number the report's
    performance section quotes. The old value, now minus the DAG run's start, also counted the hours the
    sensor waited for a late ingest and, for a rerun, the days since the run was first created (348,055 s).
    The sensor wait and the whole run's wall time are kept apart, in details. A missing time gives None.
    """
    def gap(a, b):
        return round((b - a).total_seconds(), 3) if a and b else None
    return {"seconds": round(elapsed_seconds, 3),
            "sensor_wait_seconds": gap(run_start, sensor_end),
            "run_wall_seconds": gap(run_start, now)}
