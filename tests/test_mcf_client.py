"""Offline tests for the MCF client: no network needed."""
from ingestion import mcf_client


class FakeResp:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class FakeSession:
    """Serves pages of fake postings; uuid 'dup' appears on two pages."""

    def __init__(self, pages):
        self.pages = pages
        self.calls = 0

    def get(self, url, params, timeout):
        self.calls += 1
        page = params["page"]
        jobs = self.pages[page] if page < len(self.pages) else []
        return FakeResp({"results": jobs, "total": sum(len(p) for p in self.pages)})


def test_snapshot_dedupes_and_counts(tmp_path, monkeypatch):
    monkeypatch.setattr(mcf_client, "PAGE_SIZE", 2)
    pages = [[{"uuid": "a"}, {"uuid": "dup"}], [{"uuid": "dup"}, {"uuid": "b"}], [{"uuid": "c"}]]
    session = FakeSession(pages)
    stats = mcf_client.snapshot_to_file(tmp_path / "s.jsonl.gz", session=session)
    uuids = [j["uuid"] for j in mcf_client.read_snapshot(tmp_path / "s.jsonl.gz")]
    assert uuids == ["a", "dup", "b", "c"]
    assert stats["jobs_fetched"] == 4
    assert stats["pages_fetched"] == 3
    assert stats["api_total"] == 5


def test_read_snapshot_batches(tmp_path, monkeypatch):
    monkeypatch.setattr(mcf_client, "PAGE_SIZE", 10)
    session = FakeSession([[{"uuid": str(i)} for i in range(5)]])
    path = tmp_path / "s.jsonl.gz"
    mcf_client.snapshot_to_file(path, session=session)
    sizes = [len(b) for b in mcf_client.read_snapshot_batches(path, 2)]
    assert sizes == [2, 2, 1]


def test_max_pages_stops_early(tmp_path, monkeypatch):
    monkeypatch.setattr(mcf_client, "PAGE_SIZE", 1)
    session = FakeSession([[{"uuid": str(i)}] for i in range(10)])
    stats = mcf_client.snapshot_to_file(tmp_path / "s.jsonl.gz", max_pages=3, session=session)
    assert stats["pages_fetched"] == 3
    assert session.calls == 3


def test_session_retries_400_and_429():
    retry = mcf_client.make_session().get_adapter(mcf_client.BASE_URL).max_retries
    assert isinstance(retry, mcf_client.LoggedRetry)
    assert {400, 429} <= set(retry.status_forcelist)


class FakeUrllib3Resp:
    """Just enough of a urllib3 response for Retry.increment."""
    status = 400

    def get_redirect_location(self):
        return False

    def get_retry_after(self):
        return None


def test_retry_logs_reason(caplog):
    retry = mcf_client.make_session().get_adapter(mcf_client.BASE_URL).max_retries
    with caplog.at_level("WARNING", logger="ingestion.mcf_client"):
        next_retry = retry.increment("GET", "/v2/jobs?page=364", response=FakeUrllib3Resp())
    assert "page=364" in caplog.text and "reason: 400" in caplog.text
    # The next retry must still be a LoggedRetry, or only the first retry would be logged.
    assert isinstance(next_retry, mcf_client.LoggedRetry)
    assert next_retry.total == retry.total - 1


class Fake429(FakeUrllib3Resp):
    status = 429


def test_retries_are_counted_by_reason_across_retry_copies(tmp_path, monkeypatch):
    """urllib3 makes a new Retry per retry: the session's counter must see every one (Pipeline health, 8 Nov)."""
    session = mcf_client.make_session()
    retry = session.get_adapter(mcf_client.BASE_URL).max_retries
    second = retry.increment("GET", "/v2/jobs?page=1", response=Fake429())
    third = second.increment("GET", "/v2/jobs?page=1", response=Fake429())
    third.increment("GET", "/v2/jobs?page=2", response=FakeUrllib3Resp())
    assert session.retry_counts == {"429": 2, "400": 1}
    # snapshot_to_file reports the session's 429s; a session without our retry reports None (unknown)
    monkeypatch.setattr(mcf_client, "PAGE_SIZE", 2)
    pages = [[{"uuid": "a"}]]
    session.get = FakeSession(pages).get
    assert mcf_client.snapshot_to_file(tmp_path / "a.jsonl.gz", session=session)["retries_429"] == 2
    assert mcf_client.snapshot_to_file(tmp_path / "b.jsonl.gz", session=FakeSession(pages))["retries_429"] is None
