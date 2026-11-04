"""Offline tests for the data.gov.sg MOM client: no network needed."""
import pytest

from ingestion import mom_client


class FakeResp:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class FakeSession:
    """Serves `rows` in limit/offset pages, like data.gov.sg's datastore_search."""

    def __init__(self, rows, success=True):
        self.rows = rows
        self.success = success
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append(params)
        if not self.success:
            return FakeResp({"success": False, "error": {"message": "Not found"}})
        page = self.rows[params["offset"]:params["offset"] + params["limit"]]
        return FakeResp({"success": True, "result": {"records": page, "total": len(self.rows)}})


def _rows(n):
    return [{"_id": i, "quarter": "2026-Q2", "occupation": f"group {i}", "job_vacancy": "100"} for i in range(n)]


def test_fetch_all_pages_until_total(monkeypatch):
    monkeypatch.setattr(mom_client, "PAGE_SIZE", 2)
    session = FakeSession(_rows(5))
    stats = mom_client.fetch_all(session=session, pause=0)
    assert [r["_id"] for r in stats["rows"]] == [0, 1, 2, 3, 4]
    assert stats["pages_fetched"] == 3 and stats["api_total"] == 5 and stats["rows_fetched"] == 5
    assert [c["offset"] for c in session.calls] == [0, 2, 4]
    assert all(c["resource_id"] == mom_client.DATASET_ID for c in session.calls)


def test_fetch_all_one_page_when_it_fits():
    session = FakeSession(_rows(3))
    stats = mom_client.fetch_all(session=session, pause=0)
    assert stats["rows_fetched"] == 3 and len(session.calls) == 1


def test_api_error_flag_raises():
    with pytest.raises(RuntimeError, match="data.gov.sg returned an error"):
        mom_client.fetch_all(session=FakeSession([], success=False), pause=0)


def test_session_retries_rate_limits_but_not_400():
    retry = mom_client.make_session().get_adapter(mom_client.BASE_URL).max_retries
    assert isinstance(retry, mom_client.LoggedRetry)
    assert 429 in retry.status_forcelist and 503 in retry.status_forcelist
    assert 400 not in retry.status_forcelist
