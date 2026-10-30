"""Offline tests for quality/clean_checks.py: severity logic and the run loop with a fake connection."""
from quality.clean_checks import CHECKS, Check, evaluate, failed_checks, run_checks, summarise


def check(severity="warn", op="<=", threshold=0):
    return Check("x", "range", severity, "SELECT 1", op, threshold, "desc")


def test_pass_warn_fail_follow_severity():
    assert evaluate(check("warn"), 0).status == "pass"
    assert evaluate(check("warn"), 3).status == "warn"
    assert evaluate(check("fail"), 3).status == "fail"
    assert evaluate(check("fail", ">=", 0.9), 0.95).status == "pass"
    assert evaluate(check("fail", ">=", 0.9), 0.5).status == "fail"
    assert evaluate(check("fail", "==", 0), 0).status == "pass"


def test_info_never_fails_but_records_the_number():
    r = evaluate(check("info"), 0.123)
    assert r.status == "pass" and r.observed == 0.123


def test_missing_data_is_a_warning_not_a_failure():
    r = evaluate(check("fail", ">=", 0.9), None)
    assert r.status == "warn" and r.observed is None and "no data" in r.details


def test_every_registered_check_is_well_formed():
    names = [c.name for c in CHECKS]
    assert len(names) == len(set(names))
    for c in CHECKS:
        assert c.severity in {"fail", "warn", "info"}
        assert c.category in {"schema", "range", "business_rule", "info"}
        assert c.op in {"<=", ">=", "=="}
        assert c.sql.lstrip().upper().startswith("SELECT")
    assert {"schema", "range", "business_rule"} <= {c.category for c in CHECKS}
    assert any(c.severity == "fail" for c in CHECKS) and any(c.severity == "warn" for c in CHECKS)


class FakeCursor:
    """Answers each SELECT with the next canned value and records the final INSERT."""

    def __init__(self, answers):
        self.answers = list(answers)
        self.executed = []
        self.last = None

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self.last = self.answers.pop(0) if self.answers else None

    def fetchone(self):
        return (self.last,)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, answers):
        self.cur = FakeCursor(answers)

    def cursor(self):
        return self.cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_run_checks_evaluates_and_stores(monkeypatch):
    checks = [Check("a", "schema", "fail", "SELECT %(d)s", "==", 0, "d"),
              Check("b", "range", "warn", "SELECT %(d)s", "<=", 0, "d"),
              Check("c", "info", "info", "SELECT %(d)s", ">=", 0, "d")]
    stored = {}
    monkeypatch.setattr("quality.clean_checks.execute_values", lambda cur, sql, rows: stored.update({"rows": rows}))
    results = run_checks(FakeConn([0, 5, 0.42]), "2026-10-25", checks)
    assert [r.status for r in results] == ["pass", "warn", "pass"]
    assert failed_checks(results) == []
    assert summarise(results) == {"checks": 3, "pass": 2, "warn": 1, "fail": 0, "failed": [], "warned": ["b"]}
    assert len(stored["rows"]) == 3 and stored["rows"][0][1] == "a"
    results = run_checks(FakeConn([7, 0, None]), "2026-10-25", checks)
    assert [r.name for r in failed_checks(results)] == ["a"]
