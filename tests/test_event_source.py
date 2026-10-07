"""
App-supplied output-event sources (signal_replay.events) and the final
collection wait.

Everything is mocked: no SNMP or HTTP leaves the machine. Final-wait tests
use an injected clock and sleep so the 5-minute file cadence of a
file-based log runs in milliseconds.
"""

import asyncio
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest
from dateutil import tz

import signal_replay as sr
from signal_replay.collector import DataCollector, DatabaseManager
from signal_replay.events import (
    _SourceAdapter,
    from_local_naive,
    required_event_codes,
    to_local_naive,
)

DEVICE = "dev"
START = datetime(2026, 1, 1, 12, 0, 0)

MAXTIME_XML = b"""<?xml version="1.0"?>
<EventResponses>
  <Event ID="1" TimeStamp="01-01-2026 12:00:01.5" EventTypeID="1" Parameter="2"/>
  <Event ID="2" TimeStamp="01-01-2026 12:00:02.0" EventTypeID="82" Parameter="5"/>
</EventResponses>
"""


def _frame(rows):
    return pd.DataFrame(rows, columns=["TimeStamp", "EventTypeID", "Parameter"])


def _no_http():
    """Patch requests.get so any HTTP call fails the test."""
    return patch("requests.get", side_effect=AssertionError("HTTP must not be used"))


def _db_rows(db_path, device_id=DEVICE, run_number=None):
    con = duckdb.connect(str(db_path))
    try:
        query = "SELECT device_id, run_number, timestamp, event_id, parameter FROM events WHERE device_id = ?"
        params = [device_id]
        if run_number is not None:
            query += " AND run_number = ?"
            params.append(run_number)
        return con.execute(query + " ORDER BY timestamp, event_id, parameter", params).df()
    finally:
        con.close()


def _collector(tmp_path, source, **kwargs):
    devices = kwargs.pop("devices", {DEVICE: sr.CollectionTarget(DEVICE, "192.0.2.1", None)})
    return DataCollector(str(tmp_path / "c.db"), devices, event_source=source, **kwargs)


class FakeClock:
    def __init__(self, start):
        self.now = start
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += timedelta(seconds=seconds)


# ---------------------------------------------------------------------------
# Default MAXTIME source
# ---------------------------------------------------------------------------

def test_default_source_matches_fetch_output_data_and_passes_since():
    response = MagicMock(content=MAXTIME_XML)
    response.raise_for_status.return_value = None
    since = datetime(2026, 1, 1, 11, 59)
    with patch("requests.get", return_value=response) as get:
        direct = sr.fetch_output_data("192.0.2.1", 8080, since=since)
        via_source = sr.MaxtimeHttpEventSource().fetch(
            sr.CollectionTarget(DEVICE, "192.0.2.1", 8080), since
        )
    pd.testing.assert_frame_equal(direct, via_source)
    assert len(direct) == 2
    url = get.call_args_list[1].args[0]
    assert url.startswith("http://192.0.2.1:8080/v1/asclog/xml/full?since=01-01-2026 11:59:00")


def test_default_source_skips_targets_without_http_port(tmp_path):
    collector = DataCollector(
        str(tmp_path / "c.db"),
        {
            "a": sr.CollectionTarget("a", "192.0.2.1", None),
            "b": sr.CollectionTarget("b", "192.0.2.2", 80),
        },
    )
    assert collector.active_device_ids == ["b"]
    # A custom source collects every device, http_port or not.
    custom = DataCollector(
        str(tmp_path / "d.db"),
        {"a": sr.CollectionTarget("a", "192.0.2.1", None)},
        event_source=lambda target, since: [],
    )
    assert custom.active_device_ids == ["a"]


def test_legacy_tuple_device_configs_still_work(tmp_path):
    collector = DataCollector(str(tmp_path / "c.db"), {7: (("192.0.2.1", 161), [("Ph2", "Ph6")], 80)})
    assert collector.targets["7"] == sr.CollectionTarget("7", "192.0.2.1", 80)
    assert collector.incompatible_pairs["7"] == [("Ph2", "Ph6")]


# ---------------------------------------------------------------------------
# Custom sources: sync, async, class form
# ---------------------------------------------------------------------------

def test_custom_sync_source_is_used_instead_of_http(tmp_path):
    calls = []

    def source(target, since):
        calls.append((target, since))
        return _frame([[START + timedelta(seconds=5), 1, 2]])

    collector = _collector(tmp_path, source)
    with _no_http():
        collector.collect_once(3, START)

    assert len(calls) == 1
    target, since = calls[0]
    assert isinstance(target, sr.CollectionTarget) and target.device_id == DEVICE
    # The first poll asks for events from a little before the run start.
    assert since == START - timedelta(seconds=60)
    rows = _db_rows(tmp_path / "c.db")
    assert rows["run_number"].tolist() == [3]
    assert rows["event_id"].tolist() == [1]


def test_custom_async_source_runs_on_one_private_loop(tmp_path):
    loops = []
    caller = threading.get_ident()

    async def source(target, since):
        await asyncio.sleep(0)
        loops.append((id(asyncio.get_running_loop()), threading.get_ident()))
        return _frame([[START + timedelta(seconds=len(loops)), 1, 2]])

    collector = _collector(tmp_path, source)
    with _no_http():
        collector.collect_once(1, START)
        collector.collect_once(1, START)
    collector.close()

    assert len(loops) == 2
    assert loops[0][0] == loops[1][0]  # same loop, so cached async clients keep working
    assert loops[0][1] != caller
    assert len(_db_rows(tmp_path / "c.db")) == 2


def test_async_source_keeps_its_loop_across_collectors(tmp_path):
    """A loop-bound client cached by the source must survive collector.close()."""

    class CachingSource:
        def __init__(self):
            self.client_loop = None
            self.loops = []

        async def fetch(self, target, since):
            loop = asyncio.get_running_loop()
            if self.client_loop is None:
                self.client_loop = loop  # like an httpx.AsyncClient bound to its loop
            # A cached client fails on any other (or a closed) loop.
            assert loop is self.client_loop, "client bound to a different event loop"
            assert not self.client_loop.is_closed()
            fut = self.client_loop.create_future()
            self.client_loop.call_soon(fut.set_result, None)
            await fut
            self.loops.append(id(loop))
            return _frame([[START + timedelta(seconds=len(self.loops)), 1, 2]])

    source = CachingSource()
    for index in range(3):  # one collector per simulation / batch
        collector = _collector(tmp_path / f"sim{index}", source)
        with _no_http():
            collector.collect_once(1, START)
        collector.close()
        assert collector.collection_health[DEVICE]["failures"] == 0
    assert len(source.loops) == 3
    assert len(set(source.loops)) == 1
    # A new EventSource wrapper around the same function shares the loop too.
    seen = []

    async def fn(target, since):
        seen.append(asyncio.get_running_loop())
        return _frame([[START, 1, 2]])

    for index in range(2):
        collector = _collector(tmp_path / f"wrap{index}", sr.EventSource(fn))
        with _no_http():
            collector.collect_once(1, START)
        collector.close()
    assert len(seen) == 2 and seen[0] is seen[1] and not seen[0].is_closed()


def test_class_form_and_event_source_wrapper(tmp_path):
    class Source:
        ordered = True

        def fetch(self, target, since):
            return _frame([[START + timedelta(seconds=1), 1, 2]])

    adapter = _SourceAdapter(Source())
    assert adapter.ordered is True
    wrapped = _SourceAdapter(sr.EventSource(lambda t, s: [], source_timezone="UTC", name="omni"))
    assert wrapped.ordered is False and wrapped.source_timezone == "UTC" and wrapped.name == "omni"
    assert isinstance(Source(), sr.OutputEventSource)
    with pytest.raises(TypeError):
        _SourceAdapter(42)

    collector = _collector(tmp_path, Source())
    with _no_http():
        collector.collect_once(1, START)
    assert len(_db_rows(tmp_path / "c.db")) == 1


# ---------------------------------------------------------------------------
# Schema normalisation
# ---------------------------------------------------------------------------

def test_list_of_dicts_with_app_column_names():
    records = [
        {"DeviceId": 12, "TimeStamp": datetime(2026, 1, 1, 12, 0, 1), "EventId": 1, "Parameter": 2},
        {"DeviceId": 12, "TimeStamp": datetime(2026, 1, 1, 12, 0, 2), "EventId": 10, "Parameter": 2},
        {"DeviceId": 13, "TimeStamp": datetime(2026, 1, 1, 12, 0, 3), "EventId": 1, "Parameter": 4},
    ]
    df = sr.normalize_output_events(records, device_id="12")
    assert list(df.columns) == list(sr.OUTPUT_EVENT_COLUMNS)
    assert df["EventTypeID"].tolist() == [1, 10]
    assert str(df["TimeStamp"].dtype) == "datetime64[ns]"
    assert df["EventTypeID"].dtype == "int64"

    # Device ids compare in normalised form: int 12, "12" and 12.0 all match.
    assert len(sr.normalize_output_events(records, device_id=12.0)) == 2


def test_lower_case_aliases_strings_and_extra_columns():
    df = pd.DataFrame({
        "timestamp": ["2026-01-01 12:00:01.5"],
        "event_id": ["82"],
        "parameter": ["5"],
        "device_id": ["dev"],
        "note": ["ignored"],
    })
    out = sr.normalize_output_events(df, device_id=DEVICE)
    assert out.iloc[0].tolist() == [pd.Timestamp("2026-01-01 12:00:01.5"), 82, 5]


def test_missing_column_raises_clear_error():
    with pytest.raises(ValueError, match="Parameter"):
        sr.normalize_output_events([{"TimeStamp": START, "EventId": 1}])
    with pytest.raises(TypeError):
        sr.normalize_output_events("not events")
    assert sr.normalize_output_events([]).empty
    assert sr.normalize_output_events(None).empty


def test_tz_aware_and_source_timezone_utc_conversion():
    utc_noon = datetime(2026, 7, 1, 16, 0, 0)
    aware = [{"TimeStamp": utc_noon.replace(tzinfo=timezone.utc), "EventId": 1, "Parameter": 2}]
    naive_utc = [{"TimeStamp": utc_noon, "EventId": 1, "Parameter": 2}]

    expected = pd.Timestamp("2026-07-01 12:00:00")  # New York is UTC-4 in July
    out_aware = sr.normalize_output_events(aware, local_timezone="America/New_York")
    out_naive = sr.normalize_output_events(
        naive_utc, source_timezone="UTC", local_timezone="America/New_York"
    )
    assert out_aware["TimeStamp"].tolist() == [expected]
    assert out_naive["TimeStamp"].tolist() == [expected]
    # Naive input with no source_timezone is already local.
    assert sr.normalize_output_events(naive_utc)["TimeStamp"].tolist() == [pd.Timestamp(utc_noon)]

    # Default local zone is this PC's zone.
    local_expected = (
        pd.Timestamp(utc_noon, tz="UTC").tz_convert(tz.tzlocal()).tz_localize(None)
    )
    assert sr.normalize_output_events(aware)["TimeStamp"].tolist() == [local_expected]

    # Scalar helpers round-trip.
    local = to_local_naive(utc_noon, source_timezone="UTC", local_timezone="America/New_York")
    assert local == expected.to_pydatetime()
    assert from_local_naive(local, source_timezone="UTC", local_timezone="America/New_York") == utc_noon


def test_dst_change_in_source_timezone_keeps_rows_and_complete_through(caplog):
    ny = "America/New_York"
    # 2025-11-02: 01:00-02:00 happens twice (EDT, UTC-4, then EST, UTC-5).
    through = to_local_naive(datetime(2025, 11, 2, 1, 30), source_timezone=ny, local_timezone="UTC")
    assert through == datetime(2025, 11, 2, 5, 30)  # first occurrence, never overstated

    def rows(times):
        return [{"TimeStamp": t, "EventId": 1, "Parameter": 2} for t in times]

    day = datetime(2025, 11, 2)
    with caplog.at_level(logging.WARNING, logger="signal_replay.events"):
        out = sr.normalize_output_events(
            rows([day.replace(hour=0, minute=59), day.replace(hour=1, minute=30),
                  day.replace(hour=1, minute=31), day.replace(hour=2, minute=1)]),
            source_timezone=ny, local_timezone="UTC",
        )
    assert len(out) == 4
    assert any("DST change" in r.getMessage() for r in caplog.records)

    # In log order across the repeat, the second pass is recognised as EST.
    ordered = sr.normalize_output_events(
        rows([day.replace(hour=1, minute=30), day.replace(hour=1, minute=50),
              day.replace(hour=1, minute=10), day.replace(hour=1, minute=40)]),
        source_timezone=ny, local_timezone="UTC",
    )
    assert ordered["TimeStamp"].tolist() == [
        pd.Timestamp("2025-11-02 05:30"), pd.Timestamp("2025-11-02 05:50"),
        pd.Timestamp("2025-11-02 06:10"), pd.Timestamp("2025-11-02 06:40"),
    ]
    # Spring forward: 02:30 does not exist and is moved to 03:00 EDT.
    spring = sr.normalize_output_events(
        rows([datetime(2025, 3, 9, 2, 30)]), source_timezone=ny, local_timezone="UTC",
    )
    assert spring["TimeStamp"].tolist() == [pd.Timestamp("2025-03-09 07:00")]


def test_source_timezone_applies_to_rows_since_and_complete_through(tmp_path):
    seen = {}
    start_local = datetime(2026, 7, 1, 12, 0, 0)
    start_utc = (
        pd.Timestamp(start_local).tz_localize(tz.tzlocal()).tz_convert("UTC").tz_localize(None).to_pydatetime()
    )

    def source(target, since):
        seen["since"] = since
        return sr.FetchResult(
            [{"DeviceId": 5, "TimeStamp": start_utc + timedelta(seconds=30), "EventId": 1, "Parameter": 2}],
            complete_through=start_utc + timedelta(minutes=5),
        )

    collector = _collector(
        tmp_path,
        sr.EventSource(source, source_timezone="UTC"),
        devices={"5": sr.CollectionTarget(5, "192.0.2.1")},
    )
    complete = collector.collect_once(1, start_local)

    assert seen["since"] == start_utc - timedelta(seconds=60)
    assert complete["5"] == start_local + timedelta(minutes=5)
    rows = _db_rows(tmp_path / "c.db", device_id="5")
    assert rows["timestamp"].tolist() == [pd.Timestamp(start_local + timedelta(seconds=30))]


# ---------------------------------------------------------------------------
# Watermark and de-duplication
# ---------------------------------------------------------------------------

def test_ordered_source_overlap_is_not_duplicated(tmp_path):
    polls = [
        _frame([[START + timedelta(seconds=1), 1, 2], [START + timedelta(seconds=2), 10, 2]]),
        _frame([[START + timedelta(seconds=2), 10, 2], [START + timedelta(seconds=3), 1, 4]]),
    ]
    collector = _collector(tmp_path, sr.EventSource(lambda t, s: polls.pop(0), ordered=True))
    collector.collect_once(1, START)
    collector.collect_once(1, START)
    assert len(_db_rows(tmp_path / "c.db")) == 3


def test_unordered_source_keeps_late_rows_and_redelivery_is_idempotent(tmp_path):
    late = _frame([[START + timedelta(seconds=1), 1, 2]])
    polls = [
        _frame([[START + timedelta(seconds=10), 1, 4]]),
        pd.concat([late, _frame([[START + timedelta(seconds=10), 1, 4]])]),
        pd.concat([late, _frame([[START + timedelta(seconds=10), 1, 4]])]),
    ]
    collector = _collector(tmp_path, lambda t, s: polls.pop(0))
    for _ in range(3):
        collector.collect_once(1, START)
    rows = _db_rows(tmp_path / "c.db")
    assert rows["timestamp"].tolist() == [
        pd.Timestamp(START + timedelta(seconds=1)),
        pd.Timestamp(START + timedelta(seconds=10)),
    ]

    # The ordered (MAXTIME-style) filter would have dropped the late row.
    watermark = (pd.Timestamp(START + timedelta(seconds=10)), 1, 4)
    assert DataCollector._new_rows_since_watermark(late, watermark).empty


# ---------------------------------------------------------------------------
# Error contract and health
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("error", [TimeoutError("slow"), OSError("down"), sr.EventSourceError("not ready")])
def test_source_errors_are_non_fatal_and_counted(tmp_path, caplog, error):
    def source(target, since):
        if target.device_id == "bad":
            raise error
        return _frame([[START + timedelta(seconds=1), 1, 2]])

    collector = _collector(
        tmp_path,
        source,
        devices={
            "bad": sr.CollectionTarget("bad", "192.0.2.1"),
            "good": sr.CollectionTarget("good", "192.0.2.2"),
        },
    )
    err = threading.Event()
    with caplog.at_level(logging.WARNING, logger="signal_replay.collector"):
        for _ in range(3):
            collector.collect_once(1, START, error_event=err)

    assert not err.is_set()
    health = collector.health_snapshot()
    assert health["bad"]["polls"] == 3
    assert health["bad"]["failures"] == 3
    assert health["bad"]["degraded"] is True
    assert type(error).__name__ in health["bad"]["last_error"] or "not ready" in health["bad"]["last_error"]
    assert health["good"]["failures"] == 0
    assert health["good"]["rows"] == 1
    assert health["good"]["first_timestamp"] == START + timedelta(seconds=1)
    assert health["good"]["complete_through"] is not None
    cannot = [r for r in caplog.records if "Cannot collect output events for bad" in r.getMessage()]
    assert len(cannot) == 1
    assert any("consecutive failed polls for bad" in r.getMessage() for r in caplog.records)


def test_source_call_timeout_counts_as_failure(tmp_path):
    release = threading.Event()

    def source(target, since):
        release.wait(5)
        return []

    collector = _collector(tmp_path, source, source_timeout_seconds=0.3)
    t0 = time.monotonic()
    collector.collect_once(1, START)
    release.set()
    assert time.monotonic() - t0 < 2.0
    assert "timed out" in collector.health_snapshot()[DEVICE]["last_error"]


# ---------------------------------------------------------------------------
# Run attribution and clock offset
# ---------------------------------------------------------------------------

def test_run_attribution_and_clock_offset(tmp_path):
    # Controller clock is 30 s behind the PC: offset +30 s.
    rows = _frame([
        [START - timedelta(seconds=40), 1, 2],   # -> START-10s: before the run, dropped
        [START - timedelta(seconds=20), 10, 2],  # -> START+10s: kept
    ])
    collector = _collector(tmp_path, lambda t, s: rows, clock_offsets={DEVICE: 30.0})
    collector.collect_once(1, START)
    run1 = _db_rows(tmp_path / "c.db", run_number=1)
    assert run1["timestamp"].tolist() == [pd.Timestamp(START + timedelta(seconds=10))]

    # Pushing rows for run 2 does not touch run 1.
    n = collector.ingest(DEVICE, [{"TimeStamp": START + timedelta(minutes=5), "EventId": 1, "Parameter": 4}],
                         2, START + timedelta(minutes=4, seconds=30))
    assert n == 1
    assert len(_db_rows(tmp_path / "c.db", run_number=1)) == 1
    run2 = _db_rows(tmp_path / "c.db", run_number=2)
    # The push path applies the clock offset too.
    assert run2["timestamp"].tolist() == [pd.Timestamp(START + timedelta(minutes=5, seconds=30))]


def test_signal_config_collection_settings_validate():
    sig = sr.SignalConfig(device_id=DEVICE, ip="192.0.2.1", clock_offset_seconds=-1.5,
                          source_timezone="UTC", collection_extra={"omni_id": 7})
    assert sig.clock_offset_seconds == -1.5
    with pytest.raises(ValueError, match="source_timezone"):
        sr.SignalConfig(device_id=DEVICE, ip="192.0.2.1", source_timezone="Mars/Base")
    with pytest.raises(ValueError, match="collection_extra"):
        sr.SignalConfig(device_id=DEVICE, ip="192.0.2.1", collection_extra=["x"])


# ---------------------------------------------------------------------------
# Final collection wait
# ---------------------------------------------------------------------------

class FileLogSource:
    """A controller log written in 5-minute files: data shows up when a file closes."""

    def __init__(self, clock, events, close_every=timedelta(minutes=5), stuck_at=None):
        self.clock = clock
        self.events = events
        self.close_every = close_every
        self.stuck_at = stuck_at
        self.calls = 0

    def _closed_through(self):
        if self.stuck_at is not None:
            return self.stuck_at
        now = self.clock()
        step = self.close_every.total_seconds()
        floor = (now - datetime(2026, 1, 1)).total_seconds() // step * step
        return datetime(2026, 1, 1) + timedelta(seconds=floor)

    def fetch(self, target, since):
        self.calls += 1
        closed = self._closed_through()
        rows = [
            {"DeviceId": target.device_id, "TimeStamp": ts, "EventId": e, "Parameter": p}
            for ts, e, p in self.events
            if ts < closed and (since is None or ts >= since)
        ]
        return sr.FetchResult(rows, complete_through=closed)


# Phase 2 and 6 green together at 12:02:30, in the file that closes at 12:05.
CONFLICT_EVENTS = [
    (START + timedelta(seconds=30), 1, 2),
    (START + timedelta(seconds=90), 10, 2),
    (START + timedelta(seconds=140), 1, 2),
    (START + timedelta(seconds=150), 1, 6),
]


def test_final_wait_polls_until_file_with_conflict_closes(tmp_path):
    replay_end = START + timedelta(minutes=3)
    clock = FakeClock(replay_end + timedelta(seconds=10))
    source = FileLogSource(clock, CONFLICT_EVENTS)
    found = []
    collector = _collector(
        tmp_path, source,
        incompatible_pairs={DEVICE: [("Ph2", "Ph6")]},
        clock=clock, sleep=clock.sleep,
        final_collection_poll_seconds=20, final_collection_timeout_seconds=900,
    )

    # In-run poll: only the first, already closed file (nothing yet).
    collector.collect_once(1, START)
    assert collector.detect_conflicts(DEVICE, 1) == []

    outcome = collector.finalize_run(
        1, START, replay_end + timedelta(seconds=10), conflict_callback=found.extend,
    )

    assert outcome["status"] == "complete"
    assert clock.now >= datetime(2026, 1, 1, 12, 5)
    assert 100 <= outcome["waited_seconds"] <= 140
    assert all(s == 20 for s in clock.sleeps)
    assert [c.conflict_details for c in found] == ["Ph2 & Ph6"]
    assert found[0].timestamp == pd.Timestamp(START + timedelta(seconds=150))
    health = outcome["health"][DEVICE]
    assert health["complete_through"] == datetime(2026, 1, 1, 12, 5)
    assert health["rows"] == 4


def test_final_wait_does_not_store_events_after_the_target(tmp_path):
    # The file that completes the run also holds the controller's free-running
    # events after the replay, including a conflict that the replay did not cause.
    replay_end = START + timedelta(minutes=3)
    target = replay_end + timedelta(seconds=10)
    clock = FakeClock(target)
    events = CONFLICT_EVENTS[:2] + [
        (START + timedelta(minutes=4), 1, 2),
        (START + timedelta(minutes=4, seconds=5), 1, 6),
    ]
    source = FileLogSource(clock, events)
    found = []
    collector = _collector(
        tmp_path, source,
        incompatible_pairs={DEVICE: [("Ph2", "Ph6")]},
        clock=clock, sleep=clock.sleep,
        final_collection_poll_seconds=20, final_collection_timeout_seconds=900,
    )
    outcome = collector.finalize_run(1, START, target, conflict_callback=found.extend)

    assert outcome["status"] == "complete"
    assert clock.now >= datetime(2026, 1, 1, 12, 5)  # waited for the file holding 12:04
    assert found == []
    rows = _db_rows(tmp_path / "c.db", run_number=1)
    assert len(rows) == 2
    assert rows["timestamp"].max() <= pd.Timestamp(target)
    assert outcome["health"][DEVICE]["rows"] == 2


def test_since_does_not_skip_past_reported_complete_through(tmp_path):
    # A source returns rows from a file still being written (newer than
    # complete_through). The next poll must ask again from complete_through,
    # not from the newest stored row, or late rows in the gap are never sent.
    calls = []
    base = START + timedelta(minutes=1)

    def source(target, since):
        calls.append(since)
        if len(calls) == 1:
            return sr.FetchResult(_frame([[base + timedelta(seconds=120), 1, 2]]),
                                  complete_through=base + timedelta(seconds=60))
        return sr.FetchResult(_frame([[base + timedelta(seconds=90), 1, 4]]),
                              complete_through=base + timedelta(seconds=180))

    collector = _collector(tmp_path, source)
    with _no_http():
        collector.collect_once(1, START)
        collector.collect_once(1, START)
        collector.collect_once(1, START)

    assert calls[1] <= base + timedelta(seconds=60)
    assert calls[1] >= START - timedelta(seconds=60)
    # Once complete through base+180, the hint trails the newest stored row again.
    assert calls[2] == base + timedelta(seconds=110)
    assert len(_db_rows(tmp_path / "c.db")) == 2


def test_final_wait_timeout_marks_incomplete_but_still_checks_conflicts(tmp_path, caplog):
    replay_end = START + timedelta(minutes=3)
    clock = FakeClock(replay_end)
    # The log has the conflict but never reports complete past 12:03.
    events = CONFLICT_EVENTS
    source = FileLogSource(clock, events, stuck_at=START + timedelta(minutes=2, seconds=40))
    found = []
    collector = _collector(
        tmp_path, source,
        incompatible_pairs={DEVICE: [("Ph2", "Ph6")]},
        clock=clock, sleep=clock.sleep,
        final_collection_poll_seconds=15, final_collection_timeout_seconds=60,
    )
    with caplog.at_level(logging.WARNING, logger="signal_replay.collector"):
        outcome = collector.finalize_run(1, START, replay_end, conflict_callback=found.extend)

    assert outcome["status"] == "incomplete"
    assert outcome["incomplete_devices"] == [DEVICE]
    assert 60 <= outcome["waited_seconds"] <= 75
    assert [c.conflict_details for c in found] == ["Ph2 & Ph6"]
    assert any("Run 1 is incomplete" in r.getMessage() for r in caplog.records)


def test_plain_source_is_complete_at_fetch_time(tmp_path):
    clock = FakeClock(START + timedelta(minutes=10))
    calls = []

    def source(target, since):
        calls.append(since)
        return _frame([[START + timedelta(seconds=5), 1, 2]])

    collector = _collector(tmp_path, source, clock=clock, sleep=clock.sleep)
    outcome = collector.finalize_run(1, START, START + timedelta(minutes=10))
    assert outcome["status"] == "complete"
    assert len(calls) == 1
    assert clock.sleeps == []


def test_plain_source_that_keeps_failing_gives_up_without_full_timeout(tmp_path):
    clock = FakeClock(START + timedelta(minutes=10))

    def source(target, since):
        raise OSError("unreachable")

    collector = _collector(tmp_path, source, clock=clock, sleep=clock.sleep,
                           final_collection_poll_seconds=20)
    outcome = collector.finalize_run(1, START, START + timedelta(minutes=10))
    assert outcome["status"] == "incomplete"
    assert len(clock.sleeps) == 2  # three attempts, not 900 s of polling


def test_stop_during_final_wait_returns_promptly(tmp_path):
    stop = threading.Event()
    source = FileLogSource(datetime.now, [], stuck_at=START)
    collector = _collector(tmp_path, source, final_collection_poll_seconds=30,
                           final_collection_timeout_seconds=900)
    timer = threading.Timer(0.5, stop.set)
    timer.start()
    t0 = time.monotonic()
    outcome = collector.finalize_run(1, START, datetime.now(), stop_event=stop)
    elapsed = time.monotonic() - t0
    timer.cancel()
    assert outcome["status"] == "stopped"
    assert elapsed < 2.0


def test_missing_code_warning_once_per_run(tmp_path, caplog):
    clock = FakeClock(START + timedelta(minutes=10))
    rows = _frame([[START + timedelta(seconds=5), 1, 2], [START + timedelta(seconds=9), 10, 2]])
    collector = _collector(
        tmp_path, lambda t, s: rows, clock=clock, sleep=clock.sleep,
        required_codes={DEVICE: required_event_codes([("Ph2", "O3")], adaptive_latency=True)},
    )
    with caplog.at_level(logging.WARNING, logger="signal_replay.collector"):
        collector.collect_once(1, START)
        collector.finalize_run(1, START, START + timedelta(minutes=10))
    messages = [r.getMessage() for r in caplog.records if "never included event code" in r.getMessage()]
    assert len(messages) == 2
    assert any("82" in m and "adaptive latency" in m for m in messages)
    assert any("61, 63, 65" in m and "conflict detection" in m for m in messages)


def test_no_rows_warning(tmp_path, caplog):
    clock = FakeClock(START + timedelta(minutes=10))
    collector = _collector(tmp_path, lambda t, s: [], clock=clock, sleep=clock.sleep)
    with caplog.at_level(logging.WARNING, logger="signal_replay.collector"):
        collector.finalize_run(1, START, START + timedelta(minutes=10))
    assert any("No output events were stored for dev in run 1" in r.getMessage() for r in caplog.records)


def test_required_event_codes():
    assert required_event_codes(None) == {}
    assert required_event_codes([("Ph2", "OPed17")]) == {"conflict detection": {1, 10, 67, 65}}
    assert required_event_codes([], adaptive_latency=True) == {"adaptive latency": {82}}


# ---------------------------------------------------------------------------
# Orchestrator and batch runner integration (SignalReplay.run mocked)
# ---------------------------------------------------------------------------

def _input_events():
    base = datetime(2024, 1, 1, 12)
    return pd.DataFrame([
        {"timestamp": base + timedelta(seconds=i), "event_id": 82 if i % 2 == 0 else 81,
         "parameter": 1, "device_id": DEVICE}
        for i in range(4)
    ])


def _fake_replay_run(delay=0.2):
    def run(self):
        started = datetime.now()
        time.sleep(delay)
        return started
    return run


def _sim(tmp_path, source, **kwargs):
    signal = sr.SignalConfig(
        device_id=DEVICE, ip="127.0.0.1", udp_port=1025, http_port=None,
        incompatible_pairs=kwargs.pop("pairs", [("Ph2", "Ph6")]),
    )
    kwargs.setdefault("skip_comparison", True)
    kwargs.setdefault("post_replay_settle_seconds", 0)
    return sr.ATCSimulation(
        signals=[signal], events=_input_events(), replays=kwargs.pop("replays", 1),
        stop_on_conflict=kwargs.pop("stop_on_conflict", False),
        db_path=str(tmp_path / "sim.db"), event_source=source, **kwargs,
    )


def test_simulation_detects_conflict_from_custom_source(tmp_path):
    def source(target, since):
        now = datetime.now()
        return [
            {"DeviceId": target.device_id, "TimeStamp": now, "EventId": 1, "Parameter": 2},
            {"DeviceId": target.device_id, "TimeStamp": now, "EventId": 1, "Parameter": 6},
        ]

    with _no_http(), patch.object(sr.SignalReplay, "run", _fake_replay_run()):
        result = _sim(tmp_path, source).run()

    assert result["completed_runs"] == [1]
    assert result["incomplete_runs"] == []
    assert [c["conflict_details"] for c in result["conflicts"]] == ["Ph2 & Ph6"]
    health = result["collection_health"][DEVICE]
    assert health["polls"] >= 1 and health["failures"] == 0
    assert result["collection_health_by_run"][1][DEVICE]["rows"] == health["rows"]
    assert DatabaseManager(str(tmp_path / "sim.db")).get_run_status(1) == "completed"


def test_simulation_marks_run_incomplete_after_final_timeout(tmp_path):
    def source(target, since):
        return sr.FetchResult([], complete_through=datetime(2020, 1, 1))

    with _no_http(), patch.object(sr.SignalReplay, "run", _fake_replay_run()):
        sim = _sim(tmp_path, source, final_collection_timeout_seconds=0.5,
                   final_collection_poll_seconds=0.1)
        result = sim.run()

    assert result["completed_runs"] == [1]
    assert result["incomplete_runs"] == [1]
    assert result["stop_reason"] == "completed"
    assert "incomplete" in sim.format_summary()
    db = DatabaseManager(str(tmp_path / "sim.db"))
    assert db.get_run_status(1) == "incomplete"
    assert db.get_completed_run_numbers() == [1]


def test_cancel_during_final_wait_returns_quickly(tmp_path):
    def source(target, since):
        return sr.FetchResult([], complete_through=datetime(2020, 1, 1))

    marks = {}
    with _no_http(), patch.object(sr.SignalReplay, "run", _fake_replay_run()):
        sim = _sim(tmp_path, source, final_collection_timeout_seconds=600,
                   final_collection_poll_seconds=30)

        def _cancel():
            sim.request_stop()
            marks["t"] = time.monotonic()

        timer = threading.Timer(1.5, _cancel)
        timer.start()
        result = sim.run()
        returned = time.monotonic()
        timer.cancel()

    # The stop fired while the run sat in its 30 s poll wait.
    assert returned - marks["t"] < 2.0
    assert result["cancelled"] is True
    assert result["cancelled_run"] == 1
    assert result["completed_runs"] == []
    assert DatabaseManager(str(tmp_path / "sim.db")).get_run_status(1) == "cancelled"


def test_adaptive_latency_with_custom_source(tmp_path):
    db = DatabaseManager(str(tmp_path / "c.db"))
    base = datetime(2026, 1, 1, 9, 0, 0)
    source_rows = pd.DataFrame(
        [(base + timedelta(seconds=30 * i), 82, 1) for i in range(4)],
        columns=["timestamp", "event_id", "parameter"],
    )
    db.insert_input_detector_events(source_rows, DEVICE)

    def source(target, since):
        return [
            {"TimeStamp": base + timedelta(seconds=30 * i, milliseconds=400), "EventId": 82, "Parameter": 1}
            for i in range(4)
        ]

    collector = _collector(tmp_path, source)
    with _no_http():
        collector.collect_once(1, base - timedelta(minutes=1))

    manager = sr.AdaptiveLatencyOffsetManager(
        db_manager=db, run_number=1, device_ids=[DEVICE],
        initial_offset_seconds=0.2, lookback_minutes=5.0, min_samples=3,
    )
    manager.set_device_date_shift(DEVICE, timedelta(0))
    result = manager.update_once(now=base + timedelta(minutes=4))[0]
    assert result.applied is True
    assert result.sample_count == 4
    # Collected events lag the source by 0.4 s on top of the 0.2 s already applied.
    assert round(result.target_offset_seconds, 3) == 0.6


def test_adaptive_latency_window_ends_where_output_events_are_complete(tmp_path, caplog):
    db = DatabaseManager(str(tmp_path / "c.db"))
    base = datetime(2026, 1, 1, 9, 0, 0)
    source_rows = pd.DataFrame(
        [(base + timedelta(seconds=30 * i), 82, 1) for i in range(4)],
        columns=["timestamp", "event_id", "parameter"],
    )
    db.insert_input_detector_events(source_rows, DEVICE)
    collector = _collector(tmp_path, lambda target, since: [
        {"TimeStamp": base + timedelta(seconds=30 * i, milliseconds=400), "EventId": 82, "Parameter": 1}
        for i in range(4)
    ])
    with _no_http():
        collector.collect_once(1, base - timedelta(minutes=1))

    def _manager():
        manager = sr.AdaptiveLatencyOffsetManager(
            db_manager=db, run_number=1, device_ids=[DEVICE],
            initial_offset_seconds=0.2, lookback_minutes=5.0, min_samples=3,
        )
        manager.set_device_date_shift(DEVICE, timedelta(0))
        return manager

    # The file-based source is complete only through 09:02, eight minutes behind.
    now = base + timedelta(minutes=10)
    lagging = _manager()
    assert lagging.update_once(now=now)[0].applied is False  # window 09:05-09:10 is empty
    with caplog.at_level(logging.WARNING, logger="signal_replay.latency"):
        assert lagging.warn_if_never_applied() == [DEVICE]
    assert any("never adjusted" in r.getMessage() for r in caplog.records)

    fixed = _manager()
    result = fixed.update_after_poll(now, data_complete_through={DEVICE: base + timedelta(minutes=2)})[0]
    assert result.applied is True
    assert result.sample_count == 4
    assert result.window_end == base + timedelta(minutes=2)
    assert fixed.warn_if_never_applied() == []


def test_controller_type_is_a_free_label():
    signal = sr.SignalConfig(device_id=DEVICE, ip="192.0.2.1")
    signal.events = _input_events()
    config = sr.SimulationConfig(signals=[signal], events=None, controller_type="OMNI",
                                 event_source=lambda t, s: [])
    assert config.controller_type == "OMNI"
    with pytest.raises(ValueError, match="event_source"):
        sr.SimulationConfig(signals=[signal], events=None, event_source=42)


def test_batch_runner_passes_event_source_and_target_extra(tmp_path):
    events = tmp_path / "events.parquet"
    _input_events().to_parquet(events, index=False)
    suite = sr.SoftwareTestSuite(
        suite_name="s", software_version="new", baseline_version="old",
        scenarios=[sr.TestScenario(
            scenario_id="S1", database_name="S1.bin", events_source=str(events),
            test_type=sr.TestType.SIMILARITY, collection_extra={"omni_id": 7},
        )],
        batches=[sr.TestBatch(batch_id="b1", assignments={"S1": "127.0.0.1:9701"})],
        output_dir=str(tmp_path / "out"),
        final_collection_timeout_seconds=120,
    )

    def source(target, since):
        return []

    created = []

    class FakeSim:
        def __init__(self, **kwargs):
            created.append(kwargs)

        def run(self):
            return {"cancelled": False, "collection_error": False}

        def request_stop(self, reason="user"):
            pass

    with patch("signal_replay.batch_runner.ATCSimulation", FakeSim):
        sr.BatchRunner(suite, run_log=False, event_source=source).run(
            db_loader_callback=lambda name, target: True
        )

    kwargs = created[0]
    assert kwargs["event_source"] is source
    assert kwargs["final_collection_timeout_seconds"] == 120
    extra = kwargs["signals"][0].collection_extra
    assert extra == {"scenario_id": "S1", "database_name": "S1.bin",
                     "assignment": "127.0.0.1:9701", "omni_id": 7}

    # Without a runner-level source, the suite's is used.
    suite.event_source = source
    assert sr.BatchRunner(suite, run_log=False).event_source is source
