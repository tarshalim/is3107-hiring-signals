"""Structure checks on dags/daily_transform_dag.py without importing Airflow (parsed with ast).

Airflow is not installed on laptops (it runs in Docker), so the DAG file is read as a syntax tree.
This proves the file parses and that the design points in its docstring are really in the code.
"""
import ast
from pathlib import Path

DAG_FILE = Path(__file__).resolve().parent.parent / "dags" / "daily_transform_dag.py"


def _dag_decorator_kwargs(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and getattr(dec.func, "id", None) == "dag":
                    return {kw.arg: kw.value for kw in dec.keywords}
    raise AssertionError("no @dag(...) decorator found")


def _task_function_names(tree):
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                target = dec.func if isinstance(dec, ast.Call) else dec
                dotted = ast.unparse(target)
                if dotted in ("task", "task.sensor"):
                    names.append((node.name, dotted))
    return names


def test_dag_file_parses_and_has_the_planned_tasks():
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    tasks = dict(_task_function_names(tree))
    assert tasks["wait_for_raw"] == "task.sensor"
    for name in ("load_reference_tables", "transform_snapshot_task", "run_quality_checks", "log_run_stats"):
        assert tasks[name] == "task"


def test_dag_settings_follow_l7():
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    kwargs = _dag_decorator_kwargs(tree)
    assert ast.literal_eval(kwargs["catchup"]) is False
    assert ast.literal_eval(kwargs["dag_id"]) == "daily_mcf_transform"
    assert ast.literal_eval(kwargs["max_active_runs"]) == 1
    assert "Asia/Singapore" in ast.unparse(kwargs["start_date"]) or "SGT" in ast.unparse(kwargs["start_date"])
    default_args = {k.value: v for k, v in zip(kwargs["default_args"].keys, kwargs["default_args"].values)}
    assert ast.literal_eval(default_args["retries"]) >= 1


def test_sensor_has_a_timeout_so_a_missing_day_fails_clearly():
    src = DAG_FILE.read_text(encoding="utf-8")
    assert "timeout=SENSOR_TIMEOUT" in src and "mode=\"reschedule\"" in src
    assert "AirflowFailException" in src


def _module_level_imports(path):
    """Names imported at the top level of a module (not inside a function)."""
    names = set()
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Import):
            names.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    return names


def test_dag_import_chain_needs_no_packages_missing_from_the_airflow_image():
    """The official apache/airflow image has no openpyxl or rapidfuzz. The DAG file imports transform.* and
    quality.* at module level, so a top-level `import openpyxl` anywhere in that chain makes the whole DAG
    fail to import in the container (found on 26 Oct 2026 with Airflow 2.11 and 3.0.1 images). openpyxl must
    stay a function-level import; rapidfuzz is already guarded by try/except."""
    repo = DAG_FILE.parent.parent
    for rel in ("transform/load_reference.py", "transform/transform_snapshot.py", "transform/skill_mapping.py",
                "transform/parse.py", "transform/scd.py", "transform/deltas.py", "quality/clean_checks.py"):
        imports = _module_level_imports(repo / rel)
        assert "openpyxl" not in imports, f"{rel} imports openpyxl at module level"
