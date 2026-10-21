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


def test_max_pages_stops_early(tmp_path, monkeypatch):
    monkeypatch.setattr(mcf_client, "PAGE_SIZE", 1)
    session = FakeSession([[{"uuid": str(i)}] for i in range(10)])
    stats = mcf_client.snapshot_to_file(tmp_path / "s.jsonl.gz", max_pages=3, session=session)
    assert stats["pages_fetched"] == 3
    assert session.calls == 3
