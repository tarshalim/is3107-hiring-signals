"""Offline tests for quality/live_poll_checks.py: the checks are well formed and the run loop stores verdicts."""
from quality import live_poll_checks as lpc


def test_checks_are_well_formed_and_cover_the_basics():
    names = [c.name for c in lpc.LIVE_CHECKS]
    assert len(names) == len(set(names))
    assert {"poll_rows_present", "poll_duplicate_keys", "poll_rows_equal_fetched"} <= set(names)
    for c in lpc.LIVE_CHECKS:
        assert c.severity in {"fail", "warn", "info"}
        assert c.category in {"schema", "range", "business_rule", "info"}
        assert c.op in {"<=", ">=", "=="}
        assert c.sql.lstrip().upper().startswith("SELECT")
        assert "%(t)s" in c.sql and "%(d)s" not in c.sql            # keyed on the poll, not the day
    assert any(c.severity == "fail" for c in lpc.LIVE_CHECKS) and any(c.severity == "warn" for c in lpc.LIVE_CHECKS)


class FakeCursor:
    def __init__(self, answers):
        self.answers = list(answers)
        self.params = []
        self.last = None

    def execute(self, sql, params=None):
        self.params.append(params)
        self.last = self.answers.pop(0)

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


def _answers(**overrides):
    """A healthy poll: 300 rows, all stored, no duplicates, nothing odd, 30 minutes since the last poll."""
    base = {"poll_rows_present": 300, "poll_rows_equal_fetched": 0, "poll_duplicate_keys": 0,
            "counters_missing_or_negative": 0, "counters_fell_since_prev_poll": 0, "list_newest_first": 0.0,
            "pages_out_of_step_share": 0.0, "minutes_since_prev_poll": 30, "pages_unchanged_share": 0.0}
    base.update(overrides)
    return [base[c.name] for c in lpc.LIVE_CHECKS]


def test_healthy_poll_passes_and_is_stored(monkeypatch):
    stored = {}
    monkeypatch.setattr(lpc, "execute_values", lambda cur, sql, rows: stored.update(sql=sql, rows=rows))
    conn = FakeConn(_answers())
    results = lpc.run_live_checks(conn, "2026-11-03T04:55:07+00:00")
    assert {r.status for r in results} == {"pass"}
    assert all(p == {"t": "2026-11-03T04:55:07+00:00"} for p in conn.cur.params)
    assert "raw.mcf_live_poll_dq" in stored["sql"] and "ON CONFLICT (poll_ts, check_name)" in stored["sql"]
    assert len(stored["rows"]) == len(lpc.LIVE_CHECKS) and stored["rows"][0][0] == "2026-11-03T04:55:07+00:00"


def test_empty_poll_or_lost_rows_fail_the_run(monkeypatch):
    monkeypatch.setattr(lpc, "execute_values", lambda cur, sql, rows: None)
    results = lpc.run_live_checks(FakeConn(_answers(poll_rows_present=0, poll_rows_equal_fetched=300)), "t")
    assert sorted(r.name for r in lpc.failed_checks(results)) == ["poll_rows_equal_fetched", "poll_rows_present"]


def test_odd_but_not_fatal_things_only_warn(monkeypatch):
    monkeypatch.setattr(lpc, "execute_values", lambda cur, sql, rows: None)
    results = lpc.run_live_checks(FakeConn(_answers(counters_fell_since_prev_poll=2, list_newest_first=0.4,
                                                    minutes_since_prev_poll=90)), "t")
    assert lpc.failed_checks(results) == []
    assert lpc.summarise(results)["warned"] == ["counters_fell_since_prev_poll", "list_newest_first",
                                                "minutes_since_prev_poll"]


def test_a_cached_page_0_is_flagged_but_does_not_fail(monkeypatch):
    """3 Nov 2026, first real poll: 66 of 300 postings were on two pages because page 0 was 17 minutes old."""
    monkeypatch.setattr(lpc, "execute_values", lambda cur, sql, rows: None)
    results = lpc.run_live_checks(FakeConn(_answers(pages_out_of_step_share=66 / 300)), "t")
    assert lpc.summarise(results)["warned"] == ["pages_out_of_step_share"] and lpc.failed_checks(results) == []


def test_a_poll_served_entirely_from_cache_is_recorded_not_failed(monkeypatch):
    """3 Nov 2026, second real poll: all 3 pages were identical to the poll 36 minutes earlier."""
    monkeypatch.setattr(lpc, "execute_values", lambda cur, sql, rows: None)
    results = {r.name: r for r in lpc.run_live_checks(FakeConn(_answers(pages_unchanged_share=1.0)), "t")}
    assert results["pages_unchanged_share"].status == "pass" and results["pages_unchanged_share"].observed == 1.0


def test_first_poll_ever_warns_instead_of_failing(monkeypatch):
    monkeypatch.setattr(lpc, "execute_values", lambda cur, sql, rows: None)
    results = lpc.run_live_checks(FakeConn(_answers(minutes_since_prev_poll=None)), "t")
    by_name = {r.name: r for r in results}
    assert by_name["minutes_since_prev_poll"].status == "warn" and lpc.failed_checks(results) == []
