"""
Progress callbacks, status snapshots and non-blocking database-load prompts.

Every SNMP and HTTP call is mocked; nothing here touches the network.
"""

import ast
import builtins
import io
import json
import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest

import signal_replay as sr
from signal_replay import progress as progress_mod
from signal_replay.collector import DataCollector, DatabaseManager
from signal_replay.progress import ProgressEvent, ProgressReporter, Stage, StatusTracker

DEVICE = "dev"
SRC_DIR = Path(__file__).resolve().parents[1] / "src" / "signal_replay"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _detector_events(n_events: int, eps: float = 2.0, device_id: str = DEVICE) -> pd.DataFrame:
    """``n_events`` vehicle detector ON/OFF events on detectors 1-3."""
    base = datetime(2024, 1, 1, 12)
    return pd.DataFrame([
        {
            "timestamp": base + timedelta(seconds=i / eps),
            "event_id": 82 if (i // 3) % 2 == 0 else 81,
            "parameter": 1 + i % 3,
            "device_id": device_id,
        }
        for i in range(n_events)
    ])


class _FakeController:
    def __init__(self):
        self.sends = []

    async def send(self, ip_port, group, state, dtype, community="public", timeout=2.0, *, snmp_engine):
        self.sends.append((dtype, int(group), int(state)))

    async def reset(self, ip_port, community="public", debug=False, timeout=2.0, *, snmp_engine):
        return None


@pytest.fixture
def controller():
    fake = _FakeController()
    with patch("signal_replay.replay.async_send_ntcip", fake.send), patch(
        "signal_replay.replay.async_reset_all_detectors", fake.reset
    ), patch("requests.get", side_effect=AssertionError("HTTP must not be used")):
        yield fake


def _empty_source(target, since):
    return []


class _Recorder:
    """on_progress callback that records events and the thread each ran on."""

    def __init__(self):
        self.events: List[ProgressEvent] = []
        self.threads: List[str] = []
        self._lock = threading.Lock()

    def __call__(self, ev: ProgressEvent) -> None:
        with self._lock:
            self.events.append(ev)
            self.threads.append(threading.current_thread().name)

    def stages(self) -> List[Stage]:
        return [ev.stage for ev in self.events]

    def of(self, stage: Stage) -> List[ProgressEvent]:
        return [ev for ev in self.events if ev.stage == stage]


def _sim(tmp_path, events, on_progress, *, replays=1, speed=25.0, source=_empty_source, **kwargs):
    signal = sr.SignalConfig(
        device_id=DEVICE, ip="127.0.0.1", udp_port=1025, http_port=None,
        incompatible_pairs=kwargs.pop("pairs", []),
    )
    kwargs.setdefault("skip_comparison", True)
    kwargs.setdefault("post_replay_settle_seconds", 0)
    return sr.ATCSimulation(
        signals=[signal],
        events=events,
        replays=replays,
        stop_on_conflict=False,
        db_path=str(tmp_path / "sim.db"),
        simulation_speed=speed,
        collection_interval_minutes=0.01,
        event_source=source,
        on_progress=on_progress,
        **kwargs,
    )


def _first(stages: List[Stage], stage: Stage) -> int:
    return stages.index(stage)


# ---------------------------------------------------------------------------
# ProgressEvent / reporter units
# ---------------------------------------------------------------------------

def test_progress_event_is_frozen_with_fraction_and_json_dict():
    ev = ProgressEvent(Stage.REPLAY, "x", events_sent=5, events_total=20, extra={"t": datetime(2026, 1, 1)})
    assert ev.fraction == 0.25
    assert ProgressEvent(Stage.BATCH, "b", index=1, total=4).fraction == 0.25
    assert ProgressEvent(Stage.SETUP, "s").fraction is None
    with pytest.raises(Exception):
        ev.message = "changed"
    data = ev.to_dict()
    json.dumps(data)
    assert data["stage"] == "replay" and data["extra"]["t"] == "2026-01-01T00:00:00"
    assert ProgressEvent("collect", "c").stage is Stage.COLLECT


def test_reporter_swallows_and_logs_callback_errors(caplog):
    def boom(_ev):
        raise RuntimeError("callback broke")

    reporter = ProgressReporter(boom)
    with caplog.at_level(logging.ERROR, logger="signal_replay.progress"):
        ev = reporter.emit(Stage.SETUP, "hello", log=False)
    assert ev is not None
    records = [r for r in caplog.records if "on_progress callback failed" in r.getMessage()]
    assert records and records[0].exc_info is not None


def test_reporter_throttles_by_key_but_status_sees_everything(monkeypatch):
    rec = _Recorder()
    reporter = ProgressReporter(rec, min_interval=10.0)
    for sent in range(1, 6):
        reporter.emit(Stage.REPLAY, "", device_id="a", events_sent=sent, events_total=5,
                      throttle_key=("replay", "a"), log=False)
    reporter.emit(Stage.REPLAY, "", device_id="a", events_sent=5, events_total=5,
                  throttle_key=("replay", "a"), force=True, log=False)
    assert [ev.events_sent for ev in rec.events] == [1, 5]
    assert reporter.status.snapshot()["devices"]["a"]["events_sent"] == 5


def test_status_tracker_countdown_and_states():
    tracker = StatusTracker()
    assert tracker.snapshot()["state"] == "idle"
    tracker.start(total_runs=3)
    tracker.update(ProgressEvent(Stage.WAITING, "w", device_id="a", seconds_until_start=100.0))
    snap = tracker.snapshot()
    assert snap["state"] == "running" and snap["total_runs"] == 3
    assert 95.0 < snap["seconds_until_start"] <= 100.0
    tracker.update(ProgressEvent(Stage.REPLAY, "r", device_id="a", events_sent=0, events_total=4))
    assert tracker.snapshot()["seconds_until_start"] is None
    tracker.set_state("stopping")
    tracker.set_state("cancelled", stop_reason="user")
    snap = tracker.snapshot()
    assert snap["state"] == "cancelled" and snap["stop_reason"] == "user"
    assert snap["finished_at"] is not None
    tracker.set_state("stopping")  # ignored once finished
    assert tracker.snapshot()["state"] == "cancelled"
    with pytest.raises(ValueError):
        tracker.set_state("bogus")


# ---------------------------------------------------------------------------
# ATCSimulation
# ---------------------------------------------------------------------------

def test_stage_order_and_replay_progress_for_mocked_run(tmp_path, controller):
    rec = _Recorder()
    sim = _sim(tmp_path, _detector_events(50), rec)
    result = sim.run()
    assert result["completed_runs"] == [1]

    stages = rec.stages()
    order = [Stage.SETUP, Stage.STORE_INPUT, Stage.RUN_START, Stage.REPLAY]
    positions = [_first(stages, s) for s in order]
    assert positions == sorted(positions)
    last_replay = max(i for i, s in enumerate(stages) if s == Stage.REPLAY)
    assert last_replay < _first(stages, Stage.RUN_COMPLETE) < _first(stages, Stage.DONE)
    assert stages[-1] == Stage.DONE
    assert Stage.CANCELLED not in stages and Stage.ERROR not in stages
    assert Stage.DETECTOR_RESET in stages
    assert Stage.COLLECT in stages

    replay = rec.of(Stage.REPLAY)
    sent = [ev.events_sent for ev in replay]
    assert sent == sorted(sent)
    assert replay[-1].events_sent == replay[-1].events_total
    assert replay[-1].extra["complete"] is True
    assert all(ev.device_id == DEVICE for ev in replay)
    assert all(ev.run_number == 1 and ev.total_runs == 1 for ev in replay)


def test_run_number_and_total_runs_across_two_replays(tmp_path, controller):
    rec = _Recorder()
    sim = _sim(tmp_path, _detector_events(20), rec, replays=2, speed=50.0)
    result = sim.run()
    assert result["completed_runs"] == [1, 2]

    starts = rec.of(Stage.RUN_START)
    assert [(ev.run_number, ev.total_runs) for ev in starts] == [(1, 2), (2, 2)]
    completes = rec.of(Stage.RUN_COMPLETE)
    assert [ev.run_number for ev in completes] == [1, 2]
    finals = [ev for ev in rec.of(Stage.REPLAY) if ev.extra.get("complete")]
    assert [ev.run_number for ev in finals] == [1, 2]
    assert {ev.total_runs for ev in rec.of(Stage.REPLAY)} == {2}


def test_callbacks_run_on_worker_threads(tmp_path, controller):
    rec = _Recorder()
    main = threading.current_thread().name
    _sim(tmp_path, _detector_events(20), rec, speed=50.0).run()
    replay_threads = {t for ev, t in zip(rec.events, rec.threads) if ev.stage == Stage.REPLAY}
    collect_threads = {t for ev, t in zip(rec.events, rec.threads) if ev.stage == Stage.COLLECT}
    assert replay_threads and main not in replay_threads
    assert any(t.startswith("collect-run") for t in collect_threads)


def test_raising_callback_does_not_abort_run(tmp_path, controller, caplog):
    calls = []

    def boom(ev):
        calls.append(ev.stage)
        raise ValueError("app bug")

    with caplog.at_level(logging.ERROR, logger="signal_replay.progress"):
        result = _sim(tmp_path, _detector_events(20), boom, speed=50.0).run()
    assert result["completed_runs"] == [1]
    assert result["cancelled"] is False
    assert Stage.REPLAY in calls and calls[-1] == Stage.DONE
    failures = [r for r in caplog.records if "on_progress callback failed" in r.getMessage()]
    assert failures and all(r.exc_info is not None for r in failures)


def test_replay_events_are_throttled(tmp_path, controller, monkeypatch):
    interval = 0.25
    monkeypatch.setattr(progress_mod, "DEFAULT_MIN_INTERVAL_SECONDS", interval)
    rec = _Recorder()
    # 60 events over 30 s of source time at 15x: about 2 s of replay.
    _sim(tmp_path, _detector_events(60), rec, speed=15.0).run()
    replay = rec.of(Stage.REPLAY)
    duration = (replay[-1].timestamp - replay[0].timestamp).total_seconds()
    assert duration > 1.0
    assert len(replay) <= duration / interval + 2
    assert len(replay) < 60  # far fewer than one per send


def test_get_status_from_another_thread_during_run(tmp_path, controller):
    release = threading.Event()

    def source(target, since):
        # Report complete data only once the test has seen the final wait.
        through = datetime.now() + timedelta(hours=1) if release.is_set() else datetime(2000, 1, 1)
        return sr.FetchResult(events=[], complete_through=through)

    sim = _sim(
        tmp_path, _detector_events(40), None, speed=20.0, source=source,
        final_collection_poll_seconds=0.2, final_collection_timeout_seconds=60,
    )
    assert sim.get_status()["state"] == "idle"
    box = {}
    worker = threading.Thread(target=lambda: box.setdefault("result", sim.run()), name="app-worker")
    worker.start()

    seen_running = seen_progress = None
    seen_final = None
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        status = sim.get_status()
        json.dumps(status)  # JSON-safe at every moment
        if status["state"] == "running":
            seen_running = status
            dev = status["devices"].get(DEVICE) or {}
            if dev.get("events_sent"):
                seen_progress = status
            final = status["final_collection"]
            if final and final["waiting"]:
                seen_final = status
                break
        time.sleep(0.02)
    release.set()
    worker.join(30)
    assert not worker.is_alive()

    assert seen_running is not None and seen_running["total_runs"] == 1
    assert seen_running["run_number"] == 1
    assert seen_progress is not None
    assert seen_final is not None
    final = seen_final["final_collection"]
    assert final["needed"] and DEVICE in final["devices"]
    assert final["seconds_left"] is not None and final["seconds_left"] <= 60
    assert seen_final["stage"] == "final_collection"
    assert seen_final["devices"][DEVICE]["events_sent"] == seen_final["devices"][DEVICE]["events_total"]

    done = sim.get_status()
    assert done["state"] == "completed" and done["stage"] == "done"
    assert done["final_collection"]["waiting"] is False
    assert done["final_collection"]["status"] == "complete"
    assert done["collection"][DEVICE]["polls"] >= 1
    assert done["elapsed_seconds"] > 0 and done["started_at"] and done["finished_at"]
    assert box["result"]["completed_runs"] == [1]


def test_status_reports_cancel_and_conflicts(tmp_path, controller):
    def source(target, since):
        now = datetime.now()
        return [
            {"DeviceId": target.device_id, "TimeStamp": now, "EventId": 1, "Parameter": 2},
            {"DeviceId": target.device_id, "TimeStamp": now, "EventId": 1, "Parameter": 6},
        ]

    rec = _Recorder()
    sim = _sim(tmp_path, _detector_events(20), rec, speed=50.0, source=source, pairs=[("Ph2", "Ph6")])
    sim.run()
    conflicts = rec.of(Stage.CONFLICT)
    assert conflicts and conflicts[0].device_id == DEVICE and conflicts[0].run_number == 1
    status = sim.get_status()
    assert status["conflicts_found"] >= 1
    assert status["conflicts"][0]["device_id"] == DEVICE

    rec2 = _Recorder()
    (tmp_path / "b").mkdir()
    sim2 = _sim(tmp_path / "b", _detector_events(200), rec2, speed=1.0)
    timer = threading.Timer(1.0, sim2.request_stop)
    timer.start()
    try:
        sim2.run()
    finally:
        timer.cancel()
    assert rec2.stages()[-1] == Stage.CANCELLED
    assert sim2.get_status()["state"] == "cancelled"
    assert sim2.get_status()["stop_reason"] == "user"


def test_tod_wait_reports_seconds_until_start(tmp_path, controller):
    rec = _Recorder()
    now = datetime.now()
    start = now + timedelta(seconds=3)
    events = pd.DataFrame([
        {"timestamp": start + timedelta(seconds=i * 0.1), "event_id": 82 if i % 2 == 0 else 81,
         "parameter": 1, "device_id": DEVICE}
        for i in range(4)
    ])
    signal = sr.SignalConfig(device_id=DEVICE, ip="127.0.0.1", udp_port=1025, http_port=None,
                             tod_align=True)
    sim = sr.ATCSimulation(
        signals=[signal], events=events, replays=1, stop_on_conflict=False,
        db_path=str(tmp_path / "tod.db"), post_replay_settle_seconds=0,
        skip_comparison=True, event_source=_empty_source, on_progress=rec,
    )
    sim.run()
    waits = [ev for ev in rec.of(Stage.WAITING) if ev.extra.get("reason") == "tod_first_event"]
    assert waits and 0 < waits[0].seconds_until_start <= 3.0
    assert waits[0].device_id == DEVICE


# ---------------------------------------------------------------------------
# DataCollector
# ---------------------------------------------------------------------------

def test_collector_emits_collect_events_with_rows(tmp_path):
    rec = _Recorder()
    start = datetime.now() - timedelta(seconds=5)

    def source(target, since):
        return pd.DataFrame({
            "TimeStamp": [start + timedelta(seconds=i) for i in range(4)],
            "EventTypeID": [1, 8, 10, 11],
            "Parameter": [2, 2, 2, 2],
        })

    collector = DataCollector(
        str(tmp_path / "c.db"), {DEVICE: sr.CollectionTarget(DEVICE, "127.0.0.1", 80)},
        event_source=source, on_progress=rec,
    )
    collector.collect_once(1, start)
    collector.collect_once(1, start)  # re-delivery: nothing new
    collects = rec.of(Stage.COLLECT)
    assert [ev.extra["rows"] for ev in collects] == [4, 0]
    assert collects[0].device_id == DEVICE and collects[0].run_number == 1
    assert collects[0].extra["total_rows"] == 4 and collects[0].extra["polls"] == 1
    json.dumps(collects[0].to_dict())

    def broken(target, since):
        raise ConnectionError("down")

    rec2 = _Recorder()
    failing = DataCollector(
        str(tmp_path / "d.db"), {DEVICE: sr.CollectionTarget(DEVICE, "127.0.0.1", 80)},
        event_source=broken, on_progress=rec2,
    )
    failing.collect_once(1, start)
    (ev,) = rec2.of(Stage.COLLECT)
    assert ev.level == logging.WARNING and "down" in ev.extra["error"]
    assert ev.extra["failures"] == 1


# ---------------------------------------------------------------------------
# BatchRunner: progress, status and database-load prompts
# ---------------------------------------------------------------------------

def _suite(tmp_path, n_batches=1):
    events = tmp_path / "events.csv"
    _detector_events(4).to_csv(events, index=False)
    scenarios, batches = [], []
    for b in range(n_batches):
        sim_id, conf_id = f"S{b}", f"C{b}"
        scenarios.append(sr.TestScenario(
            scenario_id=sim_id, database_name=f"{sim_id}.bin",
            events_source=str(events), test_type=sr.TestType.SIMILARITY,
        ))
        scenarios.append(sr.TestScenario(
            scenario_id=conf_id, database_name=f"{conf_id}.bin",
            events_source=str(events), test_type=sr.TestType.CONFLICT,
            incompatible_pairs=[("Ph2", "Ph6")], replays=2,
        ))
        batches.append(sr.TestBatch(
            batch_id=f"b{b}",
            assignments={sim_id: "127.0.0.1:9701", conf_id: "127.0.0.1:9702"},
        ))
    return sr.SoftwareTestSuite(
        suite_name="suite", software_version="new", baseline_version="old",
        scenarios=scenarios, batches=batches, output_dir=str(tmp_path / "out"),
    )


class _FakeSimFactory:
    """Stands in for ATCSimulation inside BatchRunner and reports a little progress."""

    def __init__(self, during_run=None):
        self.created = []
        self.during_run = during_run

    def __call__(self, **kwargs):
        factory = self

        class _Sim:
            def __init__(self):
                self.kwargs = kwargs
                self.on_progress = kwargs.get("on_progress")
                factory.created.append(self)

            def request_stop(self, reason="user"):
                kwargs["stop_event"].set()

            def get_status(self):
                return {"state": "running", "kind": "simulation"}

            def run(self):
                device = kwargs["signals"][0].device_id
                self.on_progress(ProgressEvent(Stage.RUN_START, "run 1", run_number=1, total_runs=1))
                self.on_progress(ProgressEvent(
                    Stage.REPLAY, "", device_id=device, events_sent=3, events_total=3,
                    run_number=1, total_runs=1,
                ))
                if factory.during_run is not None:
                    factory.during_run()
                return {"cancelled": False, "completed_runs": [1], "conflicts": []}

        return _Sim()


def _seed_rows(db_path: Path, device_ids):
    db = DatabaseManager(str(db_path))
    now = datetime.now()
    df = pd.DataFrame({"TimeStamp": [now], "EventTypeID": [1], "Parameter": [2]})
    for device_id in device_ids:
        db.insert_events(df, device_id, 1, now - timedelta(seconds=1))


def _count_rows(db_path: Path) -> int:
    con = duckdb.connect(str(db_path))
    try:
        return con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    finally:
        con.close()


def test_batch_runner_emits_batch_and_db_load_events_with_ids(tmp_path):
    rec = _Recorder()
    suite = _suite(tmp_path)
    runner = sr.BatchRunner(suite, run_log=False, on_progress=rec)
    statuses = []
    factory = _FakeSimFactory(during_run=lambda: statuses.append(runner.get_status()))
    requests = []
    with patch("signal_replay.batch_runner.ATCSimulation", factory):
        result = runner.run(db_loader_callback=lambda req: requests.append(req) or True)
    assert result["completed_batches"] == ["b0"]

    loads = rec.of(Stage.AWAITING_DB_LOAD)
    assert [(ev.batch_id, ev.scenario_id, ev.index, ev.total) for ev in loads] == [
        ("b0", "S0", 1, 2), ("b0", "C0", 2, 2),
    ]
    assert loads[0].extra["database_name"] == "S0.bin"
    assert loads[0].extra["target"] == "127.0.0.1:9701"

    phases = [ev.extra.get("phase") for ev in rec.of(Stage.BATCH)]
    assert phases[0] == "batch_start" and phases[-1] == "batch_complete"
    assert "scenario_start" in phases and "scenario_complete" in phases
    assert rec.of(Stage.BATCH)[0].batch_id == "b0"

    # Simulation events are forwarded with the batch (and conflict scenario) filled in.
    replays = rec.of(Stage.REPLAY)
    assert [(ev.batch_id, ev.scenario_id, ev.device_id) for ev in replays] == [
        ("b0", None, "S0"), ("b0", "C0", "C0"),
    ]
    assert rec.stages()[-1] == Stage.DONE

    # AWAITING_DB_LOAD precedes each loader call.
    first_load = rec.stages().index(Stage.AWAITING_DB_LOAD)
    assert first_load < rec.stages().index(Stage.REPLAY)
    assert [(r.scenario_id, r.test_type) for r in requests] == [("S0", "similarity"), ("C0", "conflict")]

    # get_status() during the run, then after.
    assert statuses[0]["state"] == "running"
    assert statuses[0]["batch"]["batch_id"] == "b0" and statuses[0]["batch"]["total"] == 1
    assert statuses[0]["simulation"] == {"state": "running", "kind": "simulation"}
    assert statuses[0]["devices"]["S0"]["events_sent"] == 3
    json.dumps(statuses)
    final = runner.get_status()
    assert final["state"] == "completed" and final["batch"]["completed_batches"] == ["b0"]


def test_both_loader_callback_forms_are_accepted(tmp_path):
    suite = _suite(tmp_path)
    old_calls, new_calls = [], []

    def old_style(database_name, target):
        old_calls.append((database_name, target))
        return True

    def new_style(request):
        new_calls.append(request)
        return True

    for callback in (old_style, new_style):
        runner = sr.BatchRunner(suite, run_log=False)
        with patch("signal_replay.batch_runner.ATCSimulation", _FakeSimFactory()):
            runner.run_batch_once(suite.batches[0], db_loader_callback=callback)

    assert old_calls == [("S0.bin", "127.0.0.1:9701"), ("C0.bin", "127.0.0.1:9702")]
    assert [type(r) for r in new_calls] == [sr.DbLoadRequest, sr.DbLoadRequest]
    assert new_calls[1] == sr.DbLoadRequest(
        batch_id="b0", scenario_id="C0", database_name="C0.bin",
        target="127.0.0.1:9702", index=2, total=2, test_type="conflict",
    )


def test_loader_returning_false_fails_and_clears_batch(tmp_path):
    suite = _suite(tmp_path)
    runner = sr.BatchRunner(suite, run_log=False)
    cleared = []
    runner._clear_scenario_data = lambda _db, ids: cleared.append(list(ids))
    with patch("signal_replay.batch_runner.ATCSimulation", _FakeSimFactory()):
        with pytest.raises(RuntimeError, match="Database load callback failed for S0"):
            runner.run_batch_once(suite.batches[0], db_loader_callback=lambda req: False)
    assert ["S0", "C0"] in cleared
    assert runner.get_status()["state"] == "failed"


@pytest.mark.parametrize("method", ["run", "run_batch_once"])
def test_non_interactive_without_loader_raises_before_touching_data(tmp_path, monkeypatch, method):
    suite = _suite(tmp_path)
    runner = sr.BatchRunner(suite, run_log=False)
    db_path = runner._shared_db_path()
    _seed_rows(db_path, ["S0", "C0"])
    monkeypatch.setattr("sys.stdin", io.StringIO(""))
    monkeypatch.setattr(builtins, "input", lambda *_a: pytest.fail("input() must not be called"))
    factory = _FakeSimFactory()

    with patch("signal_replay.batch_runner.ATCSimulation", factory):
        with pytest.raises(ValueError, match="db_loader_callback is required"):
            if method == "run":
                runner.run()
            else:
                runner.run_batch_once(suite.batches[0])

    assert factory.created == []
    assert _count_rows(db_path) == 2
    assert not runner.checkpoint_path.exists()


def test_interactive_false_raises_even_with_a_tty(tmp_path, monkeypatch):
    class _Tty(io.StringIO):
        def isatty(self):
            return True

    monkeypatch.setattr("sys.stdin", _Tty(""))
    monkeypatch.setattr(builtins, "input", lambda *_a: pytest.fail("input() must not be called"))
    runner = sr.BatchRunner(_suite(tmp_path), run_log=False, interactive=False)
    with pytest.raises(ValueError):
        runner.run()


def test_interactive_mode_uses_console_loader(tmp_path, monkeypatch):
    prompts = []
    monkeypatch.setattr(builtins, "input", lambda prompt="": prompts.append(prompt) or "")
    rec = _Recorder()
    runner = sr.BatchRunner(_suite(tmp_path), run_log=False, interactive=True, on_progress=rec)
    with patch("signal_replay.batch_runner.ATCSimulation", _FakeSimFactory()):
        runner.run()
    assert len(prompts) == 2
    assert "Batch b0: load these databases before continuing" in prompts[0]
    assert "S0: S0.bin -> 127.0.0.1:9701" in prompts[0]
    assert "Press Enter when database loading is complete" in prompts[0]
    assert prompts[1].startswith("Load conflict database for C0 (C0.bin)")
    assert len(rec.of(Stage.AWAITING_DB_LOAD)) == 2


def test_tty_stdin_defaults_to_console_loader(tmp_path, monkeypatch):
    class _Tty(io.StringIO):
        def isatty(self):
            return True

    monkeypatch.setattr("sys.stdin", _Tty(""))
    prompts = []
    monkeypatch.setattr(builtins, "input", lambda prompt="": prompts.append(prompt) or "")
    runner = sr.BatchRunner(_suite(tmp_path), run_log=False)
    with patch("signal_replay.batch_runner.ATCSimulation", _FakeSimFactory()):
        runner.run_batch_once(runner.suite.batches[0])
    assert len(prompts) == 2


def test_stop_during_loader_cancels_before_simulation(tmp_path):
    suite = _suite(tmp_path)
    runner = sr.BatchRunner(suite, run_log=False)
    factory = _FakeSimFactory()

    def loader(request):
        runner.stop()
        return True

    rec_states = []
    with patch("signal_replay.batch_runner.ATCSimulation", factory):
        result = runner.run(db_loader_callback=loader)
        rec_states.append(runner.get_status()["state"])
    assert result["cancelled"] is True
    assert factory.created == []
    assert rec_states == ["cancelled"]


def test_no_input_calls_outside_console_db_loader():
    problems = []
    for path in sorted(SRC_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))

        def visit(node, func_name):
            for child in ast.iter_child_nodes(node):
                name = func_name
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    name = child.name if func_name is None else func_name
                if (
                    isinstance(child, ast.Call)
                    and isinstance(child.func, ast.Name)
                    and child.func.id == "input"
                    and name != "console_db_loader"
                ):
                    problems.append(f"{path.name}:{child.lineno} input() in {name}")
                visit(child, name)

        visit(tree, None)
    assert not problems, problems


# ---------------------------------------------------------------------------
# compare_software
# ---------------------------------------------------------------------------

def _phase_events(start: datetime, n_cycles: int = 20) -> pd.DataFrame:
    rows = []
    t = start
    for _ in range(n_cycles):
        for phase in (2, 6, 4, 8):
            rows.append((t, 1, phase))
            rows.append((t + timedelta(seconds=20), 8, phase))
            rows.append((t + timedelta(seconds=24), 10, phase))
            rows.append((t + timedelta(seconds=26), 11, phase))
            t += timedelta(seconds=13)
    return pd.DataFrame(rows, columns=["TimeStamp", "EventTypeID", "Parameter"])


def test_compare_software_reports_each_scenario_in_process(tmp_path):
    suite = _suite(tmp_path)
    start = datetime(2026, 1, 1, 12)
    for name in ("base", "new"):
        run_dir = tmp_path / name
        run_dir.mkdir()
        db_path = run_dir / "collected.db"
        db = DatabaseManager(str(db_path))
        db.insert_events(_phase_events(start), "S0", 1, start)
        db.insert_events(_phase_events(start), "C0", 1, start)
        (run_dir / "checkpoint.json").write_text(json.dumps({
            "scenario_db_map": {"S0": str(db_path), "C0": str(db_path)},
        }))

    rec = _Recorder()
    with patch("signal_replay.validation._make_pool", side_effect=AssertionError("no processes")):
        results = sr.compare_software(
            str(tmp_path / "base"), str(tmp_path / "new"), suite,
            max_workers=0, on_progress=rec,
        )
    assert [r.scenario_id for r in results] == ["S0", "C0"]
    compares = rec.of(Stage.COMPARE)
    assert [(ev.scenario_id, ev.index, ev.total) for ev in compares] == [("S0", 1, 2), ("C0", 2, 2)]
    assert compares[-1].fraction == 1.0
    assert rec.stages()[-1] == Stage.DONE
    assert all(t == threading.current_thread().name for t in rec.threads)


def test_compare_software_cancel_emits_cancelled(tmp_path):
    suite = _suite(tmp_path)
    for name in ("base", "new"):
        run_dir = tmp_path / name
        run_dir.mkdir()
        (run_dir / "checkpoint.json").write_text(json.dumps({
            "scenario_db_map": {"S0": str(run_dir / "missing.db"), "C0": str(run_dir / "missing.db")},
        }))
    stop = threading.Event()
    stop.set()
    rec = _Recorder()
    with pytest.raises(sr.OperationCancelled):
        sr.compare_software(
            str(tmp_path / "base"), str(tmp_path / "new"), suite,
            max_workers=0, stop_event=stop, on_progress=rec,
        )
    assert rec.stages()[-1] == Stage.CANCELLED
