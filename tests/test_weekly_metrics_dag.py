"""Structure checks on dags/weekly_metrics_dag.py without importing Airflow (parsed with ast),
same approach as tests/test_transform_dag.py."""
import ast
from pathlib import Path

DAG_FILE = Path(__file__).resolve().parent.parent / "dags" / "weekly_metrics_dag.py"


def _tree():
    return ast.parse(DAG_FILE.read_text(encoding="utf-8"))


def _dag_kwargs(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and getattr(dec.func, "id", None) == "dag":
                    return {kw.arg: kw.value for kw in dec.keywords}
    raise AssertionError("no @dag(...) decorator found")


def test_has_refresh_mom_stats_then_comparison():
    tree = _tree()
    tasks = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
             and any(ast.unparse(d) == "task" for d in n.decorator_list)]
    assert tasks == ["refresh_mom_stats", "compare_mcf_with_mom"]
    assert "compare_mcf_with_mom(refresh_mom_stats())" in DAG_FILE.read_text(encoding="utf-8")


def test_dag_settings():
    kwargs = _dag_kwargs(_tree())
    assert ast.literal_eval(kwargs["dag_id"]) == "weekly_metrics"
    assert ast.literal_eval(kwargs["catchup"]) is True      # backfills one run per week since 20 Oct
    assert ast.literal_eval(kwargs["max_active_runs"]) == 1
    assert ast.literal_eval(kwargs["schedule"]) == "0 14 * * 2"
    assert ast.unparse(kwargs["start_date"]) == "pendulum.datetime(2026, 10, 20, 14, tz=SGT)"


def test_comparison_takes_the_week_from_the_run():
    """The comparison must read the run's data interval, not the latest snapshot in the table."""
    src = DAG_FILE.read_text(encoding="utf-8")
    assert "def compare_mcf_with_mom(mom_stats: dict, data_interval_start=None, data_interval_end=None)" in src
    assert "snapshot_week.week_bounds(data_interval_start, data_interval_end)" in src
    code = src.replace(ast.get_docstring(_tree(), clean=False), "")   # the docstring may mention it
    assert "max(snapshot_date)" not in code
