"""Structure checks on dags/mcf_live_poll_dag.py without importing Airflow (parsed with ast),
same approach as tests/test_transform_dag.py."""
import ast
from pathlib import Path

DAG_FILE = Path(__file__).resolve().parent.parent / "dags" / "mcf_live_poll_dag.py"
SQL_FILE = DAG_FILE.parent.parent / "sql" / "init" / "06_live_poll.sql"


def _tree():
    return ast.parse(DAG_FILE.read_text(encoding="utf-8"))


def _dag_kwargs(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and getattr(dec.func, "id", None) == "dag":
                    return {kw.arg: kw.value for kw in dec.keywords}
    raise AssertionError("no @dag(...) decorator found")


def test_dag_settings():
    kwargs = _dag_kwargs(_tree())
    assert ast.literal_eval(kwargs["dag_id"]) == "mcf_live_poll"
    assert ast.literal_eval(kwargs["schedule"]) == "*/30 * * * *"
    assert ast.literal_eval(kwargs["catchup"]) is False           # a missed poll cannot be fetched later
    assert ast.literal_eval(kwargs["max_active_runs"]) == 1
    assert "minutes=25" in ast.unparse(kwargs["dagrun_timeout"])  # a slow run never overlaps the next slot
    assert "tz=SGT" in ast.unparse(kwargs["start_date"])


def test_three_small_tasks_in_order():
    tree = _tree()
    tasks = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
             and any(ast.unparse(d).startswith("task") for d in n.decorator_list)]
    assert tasks == ["fetch_newest", "load_poll_task", "check_poll"]
    src = DAG_FILE.read_text(encoding="utf-8")
    assert "check_poll(load_poll_task(fetch_newest()))" in src
    assert '@task(task_id="load_poll")' in src
    assert "AirflowFailException" in src and "run_live_checks" in src


def test_poll_size_comes_from_the_environment_with_a_default_of_3():
    src = DAG_FILE.read_text(encoding="utf-8")
    assert 'os.environ.get("MCF_LIVE_PAGES", DEFAULT_PAGES)' in src
    from ingestion.mcf_live import DEFAULT_PAGES, DEFAULT_PAUSE
    assert DEFAULT_PAGES == 3 and DEFAULT_PAUSE >= 1.0


def test_sql_file_is_rerunnable_and_append_only():
    sql = SQL_FILE.read_text(encoding="utf-8")
    code = "\n".join(line.split("--")[0] for line in sql.splitlines())    # drop comments (they may hold ';')
    statements = [s.strip() for s in code.split(";") if s.strip()]
    assert len(statements) >= 9
    for statement in statements:
        assert statement.startswith(("CREATE SCHEMA IF NOT EXISTS", "CREATE TABLE IF NOT EXISTS",
                                     "CREATE INDEX IF NOT EXISTS", "CREATE OR REPLACE VIEW")), statement[:60]
    assert "PRIMARY KEY (poll_ts, job_uuid)" in code and "UPDATE" not in code.upper().replace("UPDATED_AT", "")
