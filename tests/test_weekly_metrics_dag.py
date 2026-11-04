"""Structure checks on dags/weekly_metrics_dag.py without importing Airflow (parsed with ast),
same approach as tests/test_transform_dag.py: Airflow is not installed on laptops, it runs in Docker."""
import ast
from pathlib import Path

from metrics.build_week import SIGNAL_TABLES

REPO = Path(__file__).resolve().parent.parent
DAG_FILE = REPO / "dags" / "weekly_metrics_dag.py"


def _tree():
    return ast.parse(DAG_FILE.read_text(encoding="utf-8"))


def _dag_kwargs(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and getattr(dec.func, "id", None) == "dag":
                    return {kw.arg: kw.value for kw in dec.keywords}
    raise AssertionError("no @dag(...) decorator found")


def _decorator_name(dec):
    return ast.unparse(dec.func if isinstance(dec, ast.Call) else dec)


def _task_functions(tree):
    """@task, @task(...) and @task.sensor(...) functions, in file order."""
    return {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
            and any(_decorator_name(d) in ("task", "task.sensor") for d in n.decorator_list)}


def test_has_refresh_mom_stats_then_comparison():
    tree = _tree()
    tasks = list(_task_functions(tree))
    assert tasks[:2] == ["refresh_mom_stats", "compare_mcf_with_mom"]
    assert "compare_mcf_with_mom(refresh_mom_stats())" in DAG_FILE.read_text(encoding="utf-8")


def test_has_the_signal_tasks():
    tasks = list(_task_functions(_tree()))
    assert tasks[2:] == ["wait_for_week_end_snapshot", "pick_snapshot", "build_posting_week", "build_signal",
                         "check_and_log"]
    src = DAG_FILE.read_text(encoding="utf-8")
    assert "week = pick_snapshot()" in src
    assert "for table in SIGNAL_TABLES" in src and 'override(task_id=f"build_{table}")' in src
    assert "posting >> built" in src                   # every signal waits for posting_week
    assert "check_and_log(week, posting, signals)" in src
    assert "mom_validation" not in SIGNAL_TABLES        # metrics.mcf_vs_mom_share is the MOM validation


def test_dag_settings():
    kwargs = _dag_kwargs(_tree())
    assert ast.literal_eval(kwargs["dag_id"]) == "weekly_metrics"
    assert ast.literal_eval(kwargs["catchup"]) is True      # backfills one run per week since 20 Oct
    assert ast.literal_eval(kwargs["max_active_runs"]) == 1
    assert ast.literal_eval(kwargs["schedule"]) == "0 14 * * 2"
    assert ast.unparse(kwargs["start_date"]) == "pendulum.datetime(2026, 10, 20, 14, tz=SGT)"
    default_args = {k.value: v for k, v in zip(kwargs["default_args"].keys, kwargs["default_args"].values)}
    assert ast.literal_eval(default_args["retries"]) >= 1
    assert "params" not in kwargs                      # the week comes from the data interval only


def test_comparison_takes_the_week_from_the_run():
    """The comparison must read the run's data interval, not the latest snapshot in the table."""
    src = DAG_FILE.read_text(encoding="utf-8")
    assert "def compare_mcf_with_mom(mom_stats: dict, data_interval_start=None, data_interval_end=None)" in src
    assert "snapshot_week.week_bounds(data_interval_start, data_interval_end)" in src
    code = src.replace(ast.get_docstring(_tree(), clean=False), "")   # the docstring may mention it
    assert "max(snapshot_date)" not in code


def test_signals_take_the_week_from_the_run_like_the_comparison():
    """pick_snapshot is the only signal task that decides the week, and it decides it as compare_mcf_with_mom
    does: data interval -> snapshot_week.week_bounds -> latest snapshot in the week (resolve_week), failing
    without retries when the week has no snapshot."""
    tasks = _task_functions(_tree())
    pick = tasks["pick_snapshot"]
    assert [a.arg for a in pick.args.args] == ["data_interval_start", "data_interval_end", "run_id"]
    body = ast.unparse(pick)
    assert "snapshot_week.week_bounds(data_interval_start, data_interval_end)" in body
    assert "resolve_week(conn, after, up_to)" in body
    assert "except snapshot_week.NoSnapshotInWeek" in body and "AirflowFailException" in body
    src = DAG_FILE.read_text(encoding="utf-8")
    assert "get_current_context" not in src            # no run-config override of the week
    for name in ("build_posting_week", "build_signal", "check_and_log"):
        assert "data_interval" not in ast.unparse(tasks[name]), name   # they use pick_snapshot's week


def test_signals_wait_for_a_late_tuesday_then_fall_back_instead_of_failing():
    """Review 4 Nov #2: on 27 and 28 Oct ingest ended at 14:49 and 15:31, after the 14:00 run."""
    tasks = _task_functions(_tree())
    sensor = tasks["wait_for_week_end_snapshot"]
    dec = {kw.arg: kw.value for kw in sensor.decorator_list[0].keywords}
    assert ast.literal_eval(dec["mode"]) == "reschedule"
    assert ast.literal_eval(dec["soft_fail"]) is True              # timeout -> skipped, not failed
    assert "snapshot_week.week_end_ready(cur, up_to)" in ast.unparse(sensor)
    src = DAG_FILE.read_text(encoding="utf-8")
    timeout = next(n.value for n in _tree().body if isinstance(n, ast.Assign)
                   and getattr(n.targets[0], "id", None) == "WEEK_END_TIMEOUT")
    hours = eval(compile(ast.Expression(timeout), "<const>", "eval"), {"int": int}) / 3600
    assert 20 <= 14 + hours <= 21                                   # after the transform gives up at 20:00
    pick_dec = {kw.arg: kw.value for kw in tasks["pick_snapshot"].decorator_list[0].keywords}
    assert ast.literal_eval(pick_dec["trigger_rule"]) == "none_failed"
    assert "wait_for_week_end_snapshot() >> week" in src


def test_every_signal_table_has_a_builder_and_the_dag_builds_them_all():
    sql = (REPO / "sql" / "init" / "05_metrics_signals.sql").read_text(encoding="utf-8")
    tables = {line.split("metrics.")[1].split()[0] for line in sql.splitlines()
              if line.startswith("CREATE TABLE IF NOT EXISTS metrics.")}
    assert tables == {"run_log", "posting_week", *SIGNAL_TABLES}


def _module_level_imports(path):
    names = set()
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Import):
            names.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    return names


def test_import_chain_works_on_the_airflow_image_and_without_airflow():
    """The DAG imports metrics.*, quality.* and transform.*: no openpyxl at module level (missing from the
    official image, see tests/test_transform_dag.py), and no airflow either, so the CLI and tests run on a laptop."""
    for rel in ("metrics/weeks.py", "metrics/build_week.py", "metrics/run.py", "quality/clean_checks.py",
                "transform/snapshot_week.py", "transform/mom_vacancies.py"):
        imports = _module_level_imports(REPO / rel)
        assert "openpyxl" not in imports, rel
        assert "airflow" not in imports, rel


def test_compose_mounts_the_metrics_package_into_airflow():
    compose = (REPO / "docker-compose.yml").read_text(encoding="utf-8")
    assert "./metrics:/opt/airflow/metrics" in compose
