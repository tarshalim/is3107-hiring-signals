"""Rebuild the dashboard marts (schema `mart`) from the clean star schema.

    python -m transform.refresh_marts

Applies sql/init/03_dashboard_marts.sql, which drops and recreates the schema in one transaction, so a
rerun gives the same result and readers never see a half-built mart. The daily transform DAG calls
rebuild_marts() as its last task.
"""
import time
from pathlib import Path

MARTS_SQL = Path(__file__).resolve().parent.parent / "sql" / "init" / "03_dashboard_marts.sql"
MARTS = (
    "slice_day", "skill_slice_day", "occupation_day",
    "occupation_latest", "occupation_skill_latest", "skill_salary_latest", "skill_pair_latest",
)


def rebuild_marts(conn, sql_path: Path = MARTS_SQL) -> dict:
    """Returns {"seconds": ..., "rows": {mart: row count}}."""
    started = time.perf_counter()
    sql = Path(sql_path).read_text(encoding="utf-8")
    rows = {}
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for mart in MARTS:
                cur.execute(f"SELECT count(*) FROM mart.{mart}")
                rows[mart] = cur.fetchone()[0]
    return {"seconds": round(time.perf_counter() - started, 2), "rows": rows}


if __name__ == "__main__":
    from transform.db import connect

    connection = connect()
    try:
        print(rebuild_marts(connection))
    finally:
        connection.close()
