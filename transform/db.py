"""Warehouse connection for running the transform outside Airflow (CLI, tests, verification).

Inside Airflow the DAG uses PostgresHook("warehouse") instead, so the same code runs in both places.
Order of precedence: WAREHOUSE_DSN env var, then WAREHOUSE_* env vars, then the repo's .env file.
The warehouse container publishes port 5433 on the laptop (docker-compose.yml).
"""
import os
from pathlib import Path

import psycopg2

REPO_ROOT = Path(__file__).resolve().parent.parent


def _read_env_file(path: Path) -> dict:
    values = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def warehouse_dsn() -> str:
    dsn = os.environ.get("WAREHOUSE_DSN")
    if dsn:
        return dsn
    env = _read_env_file(REPO_ROOT / ".env")

    def pick(name, default):
        return os.environ.get(name, env.get(name, default))

    user = pick("WAREHOUSE_USER", "jobs")
    password = pick("WAREHOUSE_PASSWORD", "change-me")
    database = pick("WAREHOUSE_DB", "jobs")
    host = pick("WAREHOUSE_HOST", "localhost")
    port = pick("WAREHOUSE_PORT", "5433")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def connect(dsn: str | None = None):
    return psycopg2.connect(dsn or warehouse_dsn())
