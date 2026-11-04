"""Offline tests for the near-real-time poller (ingestion/mcf_live.py): no network, no database."""
from datetime import datetime, timedelta, timezone

from ingestion import mcf_client, mcf_live

NOW = datetime(2026, 11, 3, 4, 55, 7, 123456, tzinfo=timezone.utc)


class FakeResp:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class FakeSession:
    """Serves pages of postings in order and counts the calls."""

    def __init__(self, pages, total=None):
        self.pages = pages
        self.total = total if total is not None else sum(len(p) for p in pages)
        self.params = []

    def get(self, url, params, timeout):
        assert url == mcf_client.BASE_URL          # the same public list as the daily crawl, nothing else
        self.params.append(dict(params))
        page = params["page"]
        jobs = self.pages[page] if page < len(self.pages) else []
        return FakeResp({"results": jobs, "total": self.total})


def _job(uuid, created, views=0, apps=0, **extra):
    job = {"uuid": uuid, "title": f" Job  {uuid} ", "ssocCode": "25121",
           "categories": [{"id": 21, "category": "Information Technology"}, {"id": 2, "category": "Others"}],
           "status": {"id": 102, "jobStatus": "Open"},
           "metadata": {"createdAt": created, "updatedAt": created, "newPostingDate": created[:10],
                        "originalPostingDate": created[:10], "repostCount": 0, "editCount": 0,
                        "totalNumberOfView": views, "totalNumberJobApplication": apps}}
    job.update(extra)
    return job


def test_slim_posting_keeps_only_the_needed_fields(payload):
    row = mcf_live.slim_posting(payload, 7)
    assert set(row) == set(mcf_live.COLUMNS)
    assert row["job_uuid"] == payload["uuid"] and row["list_rank"] == 7
    assert row["created_at"] == "2026-10-24T09:02:03+00:00"
    assert row["new_posting_date"] == "2026-10-24" and row["original_posting_date"] == "2026-10-24"
    assert (row["views"], row["applications"], row["repost_count"], row["edit_count"]) == (120, 7, 0, 0)
    assert row["job_status"] == "Open" and row["ssoc_code"] == "22200"
    assert row["category"] == "Healthcare / Pharmaceutical"            # first category only
    assert row["title"] == "Staff Nurse (Health Screening)"            # whitespace collapsed
    assert "description" not in row and "skills" not in row             # the heavy fields stay out


def test_slim_posting_survives_missing_and_bad_values(make_payload):
    job = make_payload(categories=[], ssocCode=None, status=None,
                       **{"metadata.createdAt": "not a date", "metadata.totalNumberOfView": None,
                          "metadata.newPostingDate": ""})
    row = mcf_live.slim_posting(job, 0)
    assert row["created_at"] is None and row["new_posting_date"] is None and row["views"] is None
    assert row["category"] is None and row["ssoc_code"] is None and row["job_status"] is None
    assert row["applications"] == 7


def test_poll_reads_only_the_first_pages_with_a_pause(monkeypatch):
    monkeypatch.setattr(mcf_client, "PAGE_SIZE", 2)
    monkeypatch.setattr(mcf_live, "PAGE_SIZE", 2)
    sleeps = []
    monkeypatch.setattr(mcf_client.time, "sleep", sleeps.append)
    pages = [[_job(f"u{p}{i}", f"2026-11-03T04:{50 - 2 * p - i:02d}:00.000Z") for i in range(2)] for p in range(10)]
    session = FakeSession(pages)
    rows, stats = mcf_live.poll_newest(max_pages=3, pause=2.5, session=session, now=NOW)
    assert [p["page"] for p in session.params] == [0, 1, 2]           # 3 calls, never more
    assert sleeps == [2.5, 2.5]                                       # a pause between pages, none after the last
    assert stats["pages_fetched"] == 3 and stats["rows_fetched"] == 6
    assert [r["list_rank"] for r in rows] == [0, 1, 2, 3, 4, 5]
    assert stats["poll_ts"] == "2026-11-03T04:55:07+00:00"           # whole seconds, UTC
    assert stats["newest_created_at"] == "2026-11-03T04:50:00+00:00"
    assert stats["oldest_created_at"] == "2026-11-03T04:45:00+00:00"
    assert stats["api_total"] == 20
    assert len(stats["page_signatures"]) == 3 and len(set(stats["page_signatures"])) == 3


def test_page_signature_changes_with_counters_and_order_only():
    t = "2026-11-03T04:50:00.000Z"
    page = [_job("a", t, views=1), _job("b", t, views=0)]
    same = [_job("a", t, views=1, title="edited title"), _job("b", t, views=0)]
    assert mcf_live.page_signature(page) == mcf_live.page_signature(same)          # a cached copy matches
    assert mcf_live.page_signature(page) != mcf_live.page_signature([_job("a", t, views=2), _job("b", t)])
    assert mcf_live.page_signature(page) != mcf_live.page_signature(page[::-1])
    assert len(mcf_live.page_signature(page)) == 16


def test_poll_keeps_a_posting_once_when_the_list_shifts(monkeypatch):
    monkeypatch.setattr(mcf_client, "PAGE_SIZE", 2)
    monkeypatch.setattr(mcf_live, "PAGE_SIZE", 2)
    monkeypatch.setattr(mcf_client.time, "sleep", lambda s: None)
    t = "2026-11-03T04:50:00.000Z"
    pages = [[_job("a", t), _job("b", t)], [_job("b", t), {"title": "no uuid"}], [_job("c", t)]]
    rows, stats = mcf_live.poll_newest(max_pages=3, session=FakeSession(pages, total=100), now=NOW)
    assert [(r["job_uuid"], r["list_rank"]) for r in rows] == [("a", 0), ("b", 1), ("c", 4)]
    assert stats["duplicates_dropped"] == 1 and stats["rows_without_uuid"] == 1 and stats["rows_fetched"] == 3


def test_a_posting_on_two_pages_keeps_its_fresher_copy(monkeypatch):
    """Page 0 served from a cache 17 minutes old (3 Nov 2026): the page-1 copy has the higher totals."""
    monkeypatch.setattr(mcf_client, "PAGE_SIZE", 2)
    monkeypatch.setattr(mcf_live, "PAGE_SIZE", 2)
    monkeypatch.setattr(mcf_client.time, "sleep", lambda s: None)
    t = "2026-11-03T04:50:00.000Z"
    pages = [[_job("a", t, views=0), _job("b", t, views=1, apps=0)],          # stale page 0
             [_job("b", t, views=4, apps=1), _job("c", t, views=2)],          # fresher page 1
             [_job("c", t, views=1)]]                                         # an older copy loses to page 1's
    rows, stats = mcf_live.poll_newest(max_pages=3, session=FakeSession(pages, total=100), now=NOW)
    kept = {r["job_uuid"]: (r["list_rank"], r["views"], r["applications"]) for r in rows}
    assert kept == {"a": (0, 0, 0), "b": (2, 4, 1), "c": (3, 2, 0)}
    assert stats["duplicates_dropped"] == 2 and stats["rows_fetched"] == 3


def test_poll_is_skipped_while_the_daily_ingest_runs():
    """27 Oct 2026: the crawl retried until 14:49 SGT. Every poll in that time must leave the API alone."""
    afternoon = datetime(2026, 10, 27, 6, 30, tzinfo=timezone.utc)                # 14:30 SGT
    assert "daily_mcf_ingest has 1 active run" in mcf_live.ingest_clash(afternoon, 1)
    assert mcf_live.ingest_clash(afternoon, 0) is None                             # crawl finished: poll again


def test_poll_is_always_skipped_at_ten_sgt():
    """At 10:00 the ingest run may not exist yet, so the slot is skipped even with no active run."""
    ten_sgt = datetime(2026, 11, 4, 2, 0, tzinfo=timezone.utc)
    assert "10:00 SGT" in mcf_live.ingest_clash(ten_sgt, 0)
    for minutes in (-30, 30):                                                      # 09:30 and 10:30 still poll
        assert mcf_live.ingest_clash(ten_sgt + timedelta(minutes=minutes), 0) is None
    ten_utc = datetime(2026, 11, 4, 10, 0, tzinfo=timezone.utc)                    # 18:00 SGT, not the crawl
    assert mcf_live.ingest_clash(ten_utc, 0) is None


def test_poll_stops_when_the_api_runs_out(monkeypatch):
    monkeypatch.setattr(mcf_client.time, "sleep", lambda s: None)
    session = FakeSession([[_job("a", "2026-11-03T04:50:00.000Z")]])
    rows, stats = mcf_live.poll_newest(max_pages=3, session=session, now=NOW)
    assert len(session.params) == 1 and stats["pages_fetched"] == 1 and len(rows) == 1


def test_poll_file_round_trip(tmp_path):
    path = mcf_live.poll_file_path(tmp_path, "2026-11-03T12:55:07+08:00")
    assert path == tmp_path / "2026-11-03" / "045507.jsonl.gz"        # named by UTC time, sorts in order
    rows = [mcf_live.slim_posting(_job("a", "2026-11-03T04:50:00.000Z", views=3), 0)]
    mcf_live.write_poll_file(path, rows)
    assert mcf_live.read_poll_file(path) == rows


class FakeCursor:
    def __init__(self):
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return (2,)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self):
        self.cur = FakeCursor()

    def cursor(self):
        return self.cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_load_poll_appends_idempotently_and_logs_the_run(monkeypatch):
    captured = {}

    def fake_execute_values(cur, sql, values, page_size):
        captured.update(sql=sql, values=values)

    monkeypatch.setattr(mcf_live, "execute_values", fake_execute_values)
    rows = [mcf_live.slim_posting(_job(u, "2026-11-03T04:50:00.000Z", views=v), i)
            for i, (u, v) in enumerate([("a", 5), ("b", 0)])]
    stats = {"poll_ts": "2026-11-03T04:55:07+00:00", "pages_fetched": 1, "rows_fetched": 2, "duplicates_dropped": 0,
             "api_total": 90051, "newest_created_at": "2026-11-03T04:50:00+00:00",
             "oldest_created_at": "2026-11-03T04:50:00+00:00", "seconds_taken": 1.6,
             "page_signatures": ["0123456789abcdef"]}
    conn = FakeConn()
    assert mcf_live.load_poll(conn, stats, rows) == 2
    assert "ON CONFLICT (poll_ts, job_uuid) DO NOTHING" in captured["sql"]
    assert "DO UPDATE" not in captured["sql"]                          # append-only: observations never change
    assert captured["values"][0][:3] == ("2026-11-03T04:55:07+00:00", "a", 0)
    assert len(captured["values"][0]) == 1 + len(mcf_live.COLUMNS)
    run_sql, run_params = conn.cur.executed[0]
    assert "raw.mcf_live_poll_runs" in run_sql and "ON CONFLICT (poll_ts)" in run_sql
    assert run_params[0] == stats["poll_ts"] and run_params[2] == 2 and run_params[-1] == ["0123456789abcdef"]
