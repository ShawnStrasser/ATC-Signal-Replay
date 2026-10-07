"""
Cancellation and safe-shutdown tests.

Every SNMP and HTTP call is mocked; nothing here touches the network.
Timing limits are generous because these tests run real threads and
wall-clock waits and the suite is known to be sensitive to machine load.
"""

import _thread
import asyncio
import json
import threading
import time
from datetime import datetime, timedelta
from typing import List, Optional
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest

import signal_replay as sr
from signal_replay.batch_runner import OperationCancelled
from signal_replay.collector import DatabaseManager


DEVICE = "dev"


def _detector_events(duration_s: float, eps: float, preempt: bool = False) -> pd.DataFrame:
    """Vehicle detector ON/OFF events on detectors 1-3, plus an optional preempt call."""
    base = datetime(2024, 1, 1, 12)
    rows = []
    for i in range(int(duration_s * eps)):
        rows.append({
            "timestamp": base + timedelta(seconds=i / eps),
            "event_id": 82 if (i // 3) % 2 == 0 else 81,
            "parameter": 1 + i % 3,
            "device_id": DEVICE,
        })
    if preempt:
        # Preempt 1 ON early and OFF only at the very end, so a stop leaves it ON.
        rows.append({"timestamp": base + timedelta(seconds=0.6), "event_id": 102,
                     "parameter": 1, "device_id": DEVICE})
        rows.append({"timestamp": base + timedelta(seconds=duration_s), "event_id": 104,
                     "parameter": 1, "device_id": DEVICE})
    return pd.DataFrame(rows)


def _output_frame(n: int = 5) -> pd.DataFrame:
    """Controller output events stamped at the time of the poll."""
    now = pd.Timestamp.now()
    return pd.DataFrame({
        "TimeStamp": [now + pd.Timedelta(milliseconds=100 * i) for i in range(n)],
        "EventTypeID": [1] * n,
        "Parameter": [2] * n,
    })


class FakeController:
    """Records SNMP SETs and serves HTTP polls without any network."""

    def __init__(self):
        self.sends: List[tuple] = []  # (monotonic, type, group, state)
        self.send_delay = 0.0
        self.hang_on_zero_after: Optional[float] = None  # monotonic time
        self.fetch_block: Optional[threading.Event] = None
        self.fetch_live = False  # return fresh output events on every poll
        self.fetch_calls = 0

    async def send(self, ip_port, group, state, dtype, community="public", timeout=2.0, *, snmp_engine):
        self.sends.append((time.monotonic(), dtype, int(group), int(state)))
        if (
            self.hang_on_zero_after is not None
            and int(state) == 0
            and time.monotonic() >= self.hang_on_zero_after
        ):
            await asyncio.Event().wait()  # controller never answers
        if self.send_delay:
            await asyncio.sleep(self.send_delay)

    async def reset(self, ip_port, community="public", debug=False, timeout=2.0, *, snmp_engine):
        return None

    def fetch(self, ip, http_port=80, since=None, request_timeout_seconds=30, **_kwargs):
        self.fetch_calls += 1
        if self.fetch_block is not None:
            self.fetch_block.wait(30)
        if self.fetch_live:
            return _output_frame()
        return pd.DataFrame(columns=["TimeStamp", "EventTypeID", "Parameter"])

    def sends_after(self, t: float, slack: float = 0.05) -> List[tuple]:
        """SETs started after ``t`` (plus a small slack for the stop race)."""
        return [s for s in self.sends if s[0] > t + slack]

    def resets_after(self, t: float) -> List[tuple]:
        """State-0 SETs started after ``t`` (``t`` is taken once the stop is set)."""
        return [s for s in self.sends if s[0] >= t and s[3] == 0]

    def final_states(self) -> dict:
        states = {}
        for _t, dtype, group, state in self.sends:
            states[(dtype, group)] = state
        return states


@pytest.fixture
def controller():
    fake = FakeController()
    with patch("signal_replay.replay.async_send_ntcip", fake.send), patch(
        "signal_replay.replay.async_reset_all_detectors", fake.reset
    ), patch("signal_replay.collector.fetch_output_data", fake.fetch):
        yield fake
    if fake.fetch_block is not None:
        fake.fetch_block.set()


def _make_sim(tmp_path, events, *, replays=1, settle=0.0, collect_min=0.01, **kwargs):
    signal = sr.SignalConfig(
        device_id=DEVICE, ip="127.0.0.1", udp_port=1025, http_port=80,
        cycle_length=kwargs.pop("cycle_length", 0),
        cycle_offset=kwargs.pop("cycle_offset", 0.0),
        incompatible_pairs=[],
    )
    kwargs.setdefault("skip_comparison", True)
    return sr.ATCSimulation(
        signals=[signal],
        events=events,
        replays=replays,
        stop_on_conflict=False,
        db_path=str(tmp_path / "sim.db"),
        post_replay_settle_seconds=settle,
        collection_interval_minutes=collect_min,
        **kwargs,
    )


def _stop_after(delay: float, action):
    """Run ``action`` after ``delay`` s on a timer; marks['t'] is when it returned."""
    marks = {}

    def _fire():
        action()
        marks["t"] = time.monotonic()

    timer = threading.Timer(delay, _fire)
    timer.daemon = True
    timer.start()
    return timer, marks


def _events_rows(db_path, run_number: int) -> int:
    con = duckdb.connect(str(db_path))
    try:
        return con.execute(
            "SELECT COUNT(*) FROM events WHERE run_number = ?", [run_number]
        ).fetchone()[0]
    finally:
        con.close()


# ---------------------------------------------------------------------------
# ATCSimulation
# ---------------------------------------------------------------------------

def test_request_stop_from_thread_returns_quickly_and_reports_cancel(tmp_path, controller):
    sim = _make_sim(tmp_path, _detector_events(120, 2), settle=30, skip_comparison=False)
    compare = MagicMock(return_value={})
    timer, marks = _stop_after(1.5, sim.request_stop)
    with patch("signal_replay.orchestrator.compare_all_runs", compare):
        result = sim.run()
    returned = time.monotonic()
    timer.cancel()

    assert returned - marks["t"] < 3.0
    assert result["cancelled"] is True
    assert result["stop_reason"] == "cancelled"
    assert result["cancel_reason"] == "user"
    assert result["stopped_early"] is True
    assert result["completed_runs"] == []
    assert result["cancelled_run"] == 1
    assert result["detectors_reset"] == {DEVICE: True}
    compare.assert_not_called()
    assert DatabaseManager(str(tmp_path / "sim.db")).get_run_status(1) == "cancelled"
    # Only resets (state 0) after the stop; every touched group ends at 0.
    assert all(s[3] == 0 for s in controller.sends_after(marks["t"]))
    assert controller.final_states() and set(controller.final_states().values()) == {0}


def test_shared_stop_event_cancels_like_request_stop(tmp_path, controller):
    stop_event = threading.Event()
    sim = _make_sim(tmp_path, _detector_events(60, 2), stop_event=stop_event)
    timer, marks = _stop_after(1.0, stop_event.set)
    result = sim.run()
    returned = time.monotonic()
    timer.cancel()

    assert returned - marks["t"] < 3.0
    assert result["cancelled"] is True
    assert result["stop_reason"] == "cancelled"


def test_request_stop_sets_shared_event_but_conflict_does_not(tmp_path, controller):
    shared = threading.Event()
    sim = _make_sim(tmp_path, _detector_events(5, 2), stop_event=shared)
    sim._set_stop("conflict", cancel=False)
    assert not shared.is_set()
    sim.request_stop()
    assert shared.is_set()
    assert sim._stop_reason == "conflict"  # first reason wins


def test_ctrl_c_in_main_thread_stops_cleanly_and_reraises(tmp_path, controller):
    sim = _make_sim(tmp_path, _detector_events(60, 2), settle=30)
    timer, marks = _stop_after(1.5, _thread.interrupt_main)
    try:
        with pytest.raises(KeyboardInterrupt):
            sim.run()
    finally:
        timer.cancel()
    returned = time.monotonic()

    assert returned - marks["t"] < 3.0
    assert sim._stop_event.is_set()
    assert sim.last_results is not None
    assert sim.last_results["cancelled"] is True
    assert sim.last_results["cancel_reason"] == "keyboard_interrupt"
    assert sim.last_results["detectors_reset"] == {DEVICE: True}
    assert set(controller.final_states().values()) == {0}
    assert all(s[3] == 0 for s in controller.sends_after(marks["t"]))
    assert DatabaseManager(str(tmp_path / "sim.db")).get_run_status(1) == "cancelled"


def test_slow_controller_backlog_is_dropped_on_stop(tmp_path, controller):
    # Each SET takes 2 s but events arrive at 4/s, so a backlog builds up.
    controller.send_delay = 2.0
    sim = _make_sim(tmp_path, _detector_events(60, 4))
    timer, marks = _stop_after(3.0, sim.request_stop)
    result = sim.run()
    returned = time.monotonic()
    timer.cancel()

    assert returned - marks["t"] < 5.0
    # The in-flight send was cancelled; the only SETs after the stop are resets,
    # one per touched group (no backlog of queued commands).
    assert all(s[3] == 0 for s in controller.sends_after(marks["t"]))
    assert len(controller.resets_after(marks["t"])) == len(controller.final_states())
    assert set(controller.final_states().values()) == {0}
    assert result["detectors_reset"] == {DEVICE: True}


def test_detectors_reset_preempt_first_on_stop(tmp_path, controller):
    sim = _make_sim(tmp_path, _detector_events(30, 2, preempt=True))
    timer, marks = _stop_after(2.0, sim.request_stop)
    result = sim.run()
    timer.cancel()

    finals = controller.final_states()
    assert ("Preempt", 1) in finals and ("Vehicle", 1) in finals
    assert set(finals.values()) == {0}
    reset_types = [s[1] for s in controller.resets_after(marks["t"])]
    assert reset_types.index("Preempt") < reset_types.index("Vehicle")
    assert result["detectors_reset"] == {DEVICE: True}


def test_detectors_reset_on_natural_completion(tmp_path, controller):
    sim = _make_sim(tmp_path, _detector_events(2, 4, preempt=True))
    result = sim.run()

    assert result["cancelled"] is False
    assert result["stop_reason"] == "completed"
    assert result["completed_runs"] == [1]
    assert result["detectors_reset"] == {DEVICE: True}
    last_two = controller.sends[-2:]
    assert [(s[1], s[3]) for s in last_two] == [("Preempt", 0), ("Vehicle", 0)]
    assert DatabaseManager(str(tmp_path / "sim.db")).get_run_status(1) == "completed"


def test_unanswered_reset_is_reported_and_bounded(tmp_path, controller):
    sim = _make_sim(
        tmp_path, _detector_events(60, 2),
        stop_grace_seconds=2.0, detector_reset_timeout_seconds=1.0,
    )

    def _stop():
        controller.hang_on_zero_after = time.monotonic()
        sim.request_stop()

    timer, marks = _stop_after(1.5, _stop)
    result = sim.run()
    returned = time.monotonic()
    timer.cancel()

    assert returned - marks["t"] < 2.0 + 1.0 + 2.0
    assert result["cancelled"] is True
    assert result["detectors_reset"] == {DEVICE: False}
    assert "Detector reset not confirmed" in sim.format_summary()


def test_stop_during_http_poll_returns_quickly_and_discards_late_rows(tmp_path, controller):
    controller.fetch_block = threading.Event()
    controller.fetch_live = True
    sim = _make_sim(tmp_path, _detector_events(60, 2))
    timer, marks = _stop_after(2.0, sim.request_stop)
    result = sim.run()
    returned = time.monotonic()
    timer.cancel()

    assert returned - marks["t"] < 4.0
    assert result["cancelled"] is True

    collectors = [t for t in threading.enumerate() if t.name.startswith("collect-run")]
    controller.fetch_block.set()
    for thread in collectors:
        thread.join(2.0)
        assert not thread.is_alive()
    # The poll that was in flight at the stop wrote nothing for the cancelled run.
    assert _events_rows(tmp_path / "sim.db", 1) == 0


def test_cancelled_run_is_not_completed_and_is_resumed(tmp_path, controller):
    controller.fetch_live = True
    sim = _make_sim(tmp_path, _detector_events(60, 2), replays=2)
    timer, _marks = _stop_after(2.5, sim.request_stop)
    result = sim.run()
    timer.cancel()

    db_path = tmp_path / "sim.db"
    assert result["cancelled"] is True
    assert result["completed_runs"] == []
    # Data up to the stop is kept, but the run does not count as completed.
    assert _events_rows(db_path, 1) > 0
    db = DatabaseManager(str(db_path))
    assert db.get_run_status(1) == "cancelled"
    assert db.get_completed_run_numbers(device_ids=[DEVICE]) == []
    assert db.get_max_run_number(device_ids=[DEVICE]) == 0

    resumed = _make_sim(tmp_path, _detector_events(1.5, 2), replays=2)
    assert resumed._run_offset == 0
    result2 = resumed.run()
    assert result2["cancelled"] is False
    assert result2["completed_runs"] == [1, 2]
    assert db.get_run_status(1) == "completed"


def test_stop_before_run_starts(tmp_path, controller):
    stop_event = threading.Event()
    stop_event.set()
    sim = _make_sim(tmp_path, _detector_events(30, 2), stop_event=stop_event)
    start = time.monotonic()
    result = sim.run()

    assert time.monotonic() - start < 2.0
    assert result["cancelled"] is True
    assert result["cancelled_run"] is None
    assert controller.sends == []
    assert DatabaseManager(str(tmp_path / "sim.db")).get_run_status(1) is None


def test_stop_during_settle_skips_remaining_runs(tmp_path, controller):
    sim = _make_sim(tmp_path, _detector_events(1, 2), replays=3, settle=30)
    started = []
    original = sim.db.mark_run_started

    def _spy(run_number, **kwargs):
        started.append(run_number)
        original(run_number, **kwargs)

    sim.db.mark_run_started = _spy
    timer, marks = _stop_after(3.0, sim.request_stop)
    result = sim.run()
    returned = time.monotonic()
    timer.cancel()

    assert returned - marks["t"] < 2.0
    assert started == [1]
    assert result["cancelled"] is True
    assert result["cancelled_run"] == 1
    assert result["completed_runs"] == []


def test_stop_during_cycle_alignment_wait(tmp_path, controller):
    # Cycle alignment can wait up to a full cycle before the first event.
    # Pick the offset so the alignment wait is about 60 s.
    events = _detector_events(10, 2)
    probe = sr.SignalConfig(device_id=DEVICE, ip="127.0.0.1", udp_port=1025)
    probe.events = events
    start = sr.SignalReplay(probe).original_start_time
    offset = ((datetime.now() - start).total_seconds() + 60.0) % 120
    sim = _make_sim(tmp_path, events, cycle_length=120, cycle_offset=offset)
    timer, marks = _stop_after(1.0, sim.request_stop)
    result = sim.run()
    returned = time.monotonic()
    timer.cancel()

    assert returned - marks["t"] < 2.0
    assert result["cancelled"] is True
    assert controller.sends == []


def test_no_replay_threads_left_after_cancel(tmp_path, controller):
    sim = _make_sim(tmp_path, _detector_events(60, 2))
    timer, _marks = _stop_after(1.0, sim.request_stop)
    sim.run()
    timer.cancel()

    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        leftovers = [
            t for t in threading.enumerate()
            if t is not threading.main_thread() and t.is_alive()
            and (not t.daemon or t.name.startswith("replay"))
        ]
        if not leftovers:
            break
        time.sleep(0.1)
    assert leftovers == []


def test_collection_fatal_error_stops_replay(tmp_path, controller):
    controller.fetch_live = True
    sim = _make_sim(tmp_path, _detector_events(60, 2))
    start = time.monotonic()
    with patch.object(DatabaseManager, "insert_events", side_effect=RuntimeError("disk full")):
        result = sim.run()

    assert time.monotonic() - start < 20.0
    assert result["collection_error"] is True
    assert result["stop_reason"] == "collection_error"
    assert result["cancelled"] is False
    assert result["completed_runs"] == []
    assert set(controller.final_states().values()) == {0}


def test_conflict_stop_reports_conflict_reason(tmp_path):
    class FakeCollector:
        def __init__(self, *args, **kwargs):
            pass

        def run_collection_loop(self, *args, **kwargs):
            return None

        def collect_once(self, *args, **kwargs):
            return {}

        def finalize_run(self, run_number, simulation_start_time, complete_target,
                         conflict_callback=None, **_kwargs):
            if conflict_callback is not None:
                conflict_callback([sr.ConflictRecord(
                    device_id=DEVICE, run_number=run_number,
                    timestamp=simulation_start_time, conflict_details="Ph2 & Ph6",
                )])
            return {"status": "complete"}

    signal = sr.SignalConfig(device_id=DEVICE, ip="127.0.0.1", udp_port=1025,
                             incompatible_pairs=[("Ph2", "Ph6")])
    with patch("signal_replay.orchestrator.DataCollector", FakeCollector), patch.object(
        sr.ATCSimulation, "_run_all_signals", return_value=({DEVICE: datetime.now()}, [])
    ):
        sim = sr.ATCSimulation(
            signals=[signal], events=_detector_events(2, 2), replays=3,
            stop_on_conflict=True, db_path=str(tmp_path / "c.db"),
            post_replay_settle_seconds=0, skip_comparison=True,
        )
        result = sim.run()

    assert result["stop_reason"] == "conflict"
    assert result["cancelled"] is False
    assert result["completed_runs"] == [1]
    assert "stopped early due to conflict" not in sim.format_summary()


def test_signal_replay_stop_while_send_in_flight(controller):
    controller.send_delay = 30.0
    config = sr.SignalConfig(device_id=DEVICE, ip="127.0.0.1", udp_port=1025)
    config.events = _detector_events(10, 2)
    stop = threading.Event()
    replay = sr.SignalReplay(config, stop_event=stop, detector_reset_timeout_seconds=0.5)
    timer, marks = _stop_after(0.5, stop.set)
    replay.run()
    returned = time.monotonic()
    timer.cancel()

    # The 30 s send was cancelled; the reset (also slow) is capped at 0.5 s.
    assert returned - marks["t"] < 2.0
    assert replay.detectors_reset is False


def test_signal_replay_interrupt_inside_running_loop_stops_and_resets(controller):
    # In Jupyter (or an async app) run() replays on a helper thread. A kernel
    # interrupt must stop that thread and let it reset the detectors.
    config = sr.SignalConfig(device_id=DEVICE, ip="127.0.0.1", udp_port=1025)
    config.events = _detector_events(60, 2)
    replay = sr.SignalReplay(config, detector_reset_timeout_seconds=2.0)

    async def _main():
        replay.run()

    loop = asyncio.new_event_loop()  # no asyncio SIGINT handler, like ipykernel
    timer, marks = _stop_after(1.0, _thread.interrupt_main)
    try:
        with pytest.raises(KeyboardInterrupt):
            loop.run_until_complete(_main())
    finally:
        timer.cancel()
        loop.close()
    returned = time.monotonic()

    assert replay._stop_event.is_set()
    assert returned - marks["t"] < 5.0
    assert replay.detectors_reset is True
    assert controller.sends, "the replay should have sent something before the interrupt"
    assert all(state == 0 for state in controller.final_states().values())
    time.sleep(0.5)
    assert not controller.sends_after(returned)


# ---------------------------------------------------------------------------
# BatchRunner and compare_software
# ---------------------------------------------------------------------------

def _suite(tmp_path, n_batches=2):
    events = tmp_path / "events.parquet"
    _detector_events(5, 2).to_parquet(events, index=False)
    scenarios = []
    batches = []
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
    """Stands in for ATCSimulation inside BatchRunner; never touches the network."""

    def __init__(self, block_until_stopped=False, on_init=None):
        self.created = []
        self.ran = []
        self.block_until_stopped = block_until_stopped
        self.on_init = on_init

    def __call__(self, **kwargs):
        factory = self

        class _Sim:
            def __init__(self):
                self.kwargs = kwargs
                self.stop_event = kwargs.get("stop_event")
                factory.created.append(self)
                if factory.on_init is not None:
                    factory.on_init()

            def request_stop(self, reason="user"):
                if self.stop_event is not None:
                    self.stop_event.set()

            def run(self):
                factory.ran.append([s.device_id for s in kwargs["signals"]])
                if factory.block_until_stopped:
                    assert self.stop_event is not None
                    if self.stop_event.wait(10):
                        return {"cancelled": True, "completed_runs": []}
                return {"cancelled": False, "completed_runs": [1]}

        return _Sim()


def test_batch_runner_stop_aborts_remaining_batches(tmp_path):
    suite = _suite(tmp_path)
    runner = sr.BatchRunner(suite, run_log=False)
    factory = _FakeSimFactory(block_until_stopped=True)
    cleared = []
    original_clear = runner._clear_scenario_data

    def _clear(db_path, ids):
        cleared.append(list(ids))
        original_clear(db_path, ids)

    runner._clear_scenario_data = _clear
    timer, marks = _stop_after(0.5, runner.stop)
    with patch("signal_replay.batch_runner.ATCSimulation", factory):
        result = runner.run(db_loader_callback=lambda _db, _target: True)
    returned = time.monotonic()
    timer.cancel()

    assert returned - marks["t"] < 3.0
    assert result["cancelled"] is True
    # Only the first similarity batch ever ran; no conflict scenario, no batch b1.
    assert factory.ran == [["S0"]]
    assert result["completed_batches"] == []
    assert result["cancelled_batches"] == ["b0"]
    assert ["S0"] in cleared and ["C0"] in cleared
    saved = json.loads((tmp_path / "out" / "new" / "checkpoint.json").read_text())
    assert saved["completed_batches"] == []
    assert "cancelled" not in saved

    # Resume: the cancelled batch is run again once the stop is cleared.
    runner.reset_stop()
    factory2 = _FakeSimFactory()
    with patch("signal_replay.batch_runner.ATCSimulation", factory2):
        result2 = runner.run(db_loader_callback=lambda _db, _target: True)
    assert result2["cancelled"] is False
    assert result2["completed_batches"] == ["b0", "b1"]
    assert result2["cancelled_batches"] == []
    assert factory2.ran[0] == ["S0"]


def test_batch_runner_stop_during_simulation_init_prevents_run(tmp_path):
    suite = _suite(tmp_path, n_batches=1)
    runner = sr.BatchRunner(suite, run_log=False)
    factory = _FakeSimFactory(on_init=runner.stop)
    with patch("signal_replay.batch_runner.ATCSimulation", factory):
        result = runner.run(db_loader_callback=lambda _db, _target: True)

    assert len(factory.created) == 1
    assert factory.ran == []
    assert result["cancelled"] is True
    assert result["completed_batches"] == []


def test_batch_runner_stop_before_run_starts_nothing(tmp_path):
    suite = _suite(tmp_path, n_batches=1)
    shared = threading.Event()
    runner = sr.BatchRunner(suite, run_log=False, stop_event=shared)
    assert runner.stop_event is shared
    runner.stop()
    factory = _FakeSimFactory()
    with patch("signal_replay.batch_runner.ATCSimulation", factory):
        result = runner.run(db_loader_callback=lambda _db, _target: True)
    assert shared.is_set()
    assert factory.created == []
    assert result["cancelled"] is True


def test_run_batch_once_raises_and_clears_on_cancel(tmp_path):
    suite = _suite(tmp_path, n_batches=1)
    runner = sr.BatchRunner(suite, run_log=False)
    factory = _FakeSimFactory(block_until_stopped=True)
    cleared = []
    runner._clear_scenario_data = lambda _db, ids: cleared.append(list(ids))
    timer, _marks = _stop_after(0.3, runner.stop)
    with patch("signal_replay.batch_runner.ATCSimulation", factory):
        with pytest.raises(OperationCancelled):
            runner.run_batch_once(suite.batches[0], db_loader_callback=lambda _db, _t: True)
    timer.cancel()
    assert ["S0", "C0"] in cleared
    assert factory.ran == [["S0"]]


def test_batch_runner_passes_shared_stop_event_to_simulations(tmp_path):
    suite = _suite(tmp_path, n_batches=1)
    runner = sr.BatchRunner(suite, run_log=False)
    factory = _FakeSimFactory()
    with patch("signal_replay.batch_runner.ATCSimulation", factory):
        runner.run(db_loader_callback=lambda _db, _target: True)
    assert len(factory.created) == 2
    assert all(sim.stop_event is runner.stop_event for sim in factory.created)


def test_compare_software_stop_event_terminates_pool(tmp_path):
    suite = _suite(tmp_path, n_batches=1)
    for name in ("base", "new"):
        run_dir = tmp_path / name
        run_dir.mkdir()
        (run_dir / "checkpoint.json").write_text(json.dumps({
            "scenario_db_map": {"S0": str(run_dir / "missing.db"), "C0": str(run_dir / "missing.db")},
        }))
    stop = threading.Event()
    stop.set()
    start = time.monotonic()
    with pytest.raises(OperationCancelled):
        sr.compare_software(
            str(tmp_path / "base"), str(tmp_path / "new"), suite,
            max_workers=1, stop_event=stop,
        )
    assert time.monotonic() - start < 30.0
