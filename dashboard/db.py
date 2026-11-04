"""Warehouse access for the dashboard: one short-lived connection per query, results as DataFrames."""
import sys
from decimal import Decimal
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from transform.db import connect  # noqa: E402


def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    conn = connect()
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            columns = [c.name for c in cur.description]
            frame = pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        conn.close()
    for col in frame.columns:
        # Postgres numeric / bigint sums arrive as Decimal objects, which Plotly and arithmetic handle badly
        if frame[col].dtype == object and frame[col].map(lambda v: isinstance(v, Decimal)).any():
            frame[col] = pd.to_numeric(frame[col].astype(float))
    return frame
