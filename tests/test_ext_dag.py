"""Structure checks on dags/weekly_ext_data_dag.py without importing Airflow (parsed with ast), same approach
as tests/test_transform_dag.py."""
import ast
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DAG_FILE = REPO / "dags" / "weekly_ext_data_dag.py"


def _tree():
    return ast.parse(DAG_FILE.read_text(encoding="utf-8"))


def _dag_kwargs(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and getattr(dec.func, "id", None) == "dag":
                    return {kw.arg: kw.value for kw in dec.keywords}
    raise AssertionError("no @dag(...) decorator found")


def test_dag_has_the_planned_tasks_and_settings():
    tree = _tree()
    tasks = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                target = dec.func if isinstance(dec, ast.Call) else dec
                if ast.unparse(target) == "task":
                    tasks.add(node.name)
    assert tasks == {"refresh_wage_tables", "load_kaggle_2019", "borrow_ssoc_for_2019_titles",
                     "build_posting_history_task", "summary"}
    kwargs = _dag_kwargs(tree)
    assert ast.literal_eval(kwargs["dag_id"]) == "weekly_ext_data"
    assert ast.literal_eval(kwargs["catchup"]) is False
    assert ast.literal_eval(kwargs["max_active_runs"]) == 1
    assert "Asia/Singapore" in ast.unparse(kwargs["start_date"])


def test_missing_kaggle_file_skips_instead_of_failing():
    src = DAG_FILE.read_text(encoding="utf-8")
    assert "AirflowSkipException" in src and 'trigger_rule="none_failed"' in src


def _module_level_imports(path):
    names = set()
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Import):
            names.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    return names


def test_import_chain_needs_no_packages_missing_from_the_airflow_image():
    """openpyxl is not in the official Airflow image (see tests/test_transform_dag.py), so the modules this DAG
    imports must only import it inside functions."""
    for rel in ("ingestion/datagov_wages.py", "ingestion/ext_sources.py", "ingestion/kaggle_mcf2019.py",
                "transform/posting_history.py"):
        assert "openpyxl" not in _module_level_imports(REPO / rel), rel
