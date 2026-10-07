"""
Embedding signal_replay in an application: an offline, runnable sketch.

This script shows the pattern docs/integration.md describes, end to end, with
no hardware:

1. Failure replication on a worker thread: ATCSimulation is built and run
   on a background thread, the main thread polls ``get_status()`` the way a
   REST ``/status`` endpoint would, progress events go through a queue, and
   an optional Cancel sets the shared ``stop_event``.
2. A custom ``event_source`` returning a list of dicts with the application's
   own column names (``DeviceId``, ``TimeStamp``, ``EventId``, ``Parameter``).
3. Software-validation comparison of two event sets with
   ``compare_validation`` (in-process, ``max_workers=0``).
4. ``results_to_frames`` written into a separate application DuckDB file,
   after which the package's working folder is deleted.

Nothing here contacts a device. SNMP sends are replaced by an in-process
stand-in (``OfflineController``) and the output events come from a simulated
controller log. In a real application, drop the ``patch(...)`` lines, point
``SignalConfig`` at the bench controller and use your real poller as the
event source.

Run from the repository root (or anywhere the package is installed)::

    py examples/app_integration.py                 # replicate, then compare
    py examples/app_integration.py --cancel-after 3

Output goes to a temporary folder that is printed and removed at the end
(pass ``--keep`` to keep it).
"""

from __future__ import annotations

import argparse
import queue
import shutil
import tempfile
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd

import signal_replay as sr

DEVICE_ID = 1234                 # the application's integer DeviceId
CONFLICT_PAIRS = [("Ph2", "Ph4")]


# ---------------------------------------------------------------------------
# Offline stand-ins (not needed in a real application)
# ---------------------------------------------------------------------------

class OfflineController:
    """Records the SNMP sends the replay would make; nothing goes on the network."""

    def __init__(self) -> None:
        self.sends = 0
        self.resets = 0
        self._lock = threading.Lock()

    async def send(self, *args, **kwargs) -> None:
        with self._lock:
            self.sends += 1

    async def reset(self, *args, **kwargs) -> None:
        with self._lock:
            self.resets += 1


class SimulatedControllerLog:
    """Plays the part of the application's own poller for one controller.

    It produces a fast two-phase cycle (Ph2 then Ph4, 4 s) against the PC
    clock, and once ``fault_after_seconds`` have passed it starts Ph4 green
    while Ph2 is still green: the "field failure" being replicated. Rows
    use the application's column names and an integer DeviceId; the
    package normalizes them.
    """

    CYCLE = 4.0

    def __init__(self, fault_after_seconds: float) -> None:
        self.epoch = datetime.now().replace(microsecond=0) - timedelta(seconds=60)
        self.fault_at = datetime.now() + timedelta(seconds=fault_after_seconds)
        self.calls = 0

    def _cycle_events(self, start: datetime) -> list[tuple]:
        ph4_green = 0.5 if start >= self.fault_at else 2.0
        return [
            (start, 1, 2),                                        # Ph2 green
            (start + timedelta(seconds=1.2), 8, 2),               # Ph2 yellow
            (start + timedelta(seconds=1.8), 10, 2),              # Ph2 red clearance
            (start + timedelta(seconds=ph4_green), 1, 4),         # Ph4 green
            (start + timedelta(seconds=3.2), 8, 4),
            (start + timedelta(seconds=3.8), 10, 4),
        ]

    def __call__(self, target: sr.CollectionTarget, since: datetime | None) -> list[dict]:
        """The event source: ``(target, since) -> list of dicts``."""
        self.calls += 1
        now = datetime.now()
        since = since or self.epoch
        rows = []
        cycle_start = self.epoch
        while cycle_start <= now:
            for ts, event_id, parameter in self._cycle_events(cycle_start):
                if since <= ts <= now:
                    rows.append({"DeviceId": DEVICE_ID, "TimeStamp": ts,
                                 "EventId": event_id, "Parameter": parameter})
            cycle_start += timedelta(seconds=self.CYCLE)
        return rows


def detector_log(seconds: int = 60) -> pd.DataFrame:
    """A short input log of vehicle detector on/off events (the replayed input)."""
    base = datetime(2026, 1, 5, 7, 0)
    rows = []
    for i in range(0, seconds, 2):
        rows.append({"timestamp": base + timedelta(seconds=i), "event_id": 82,
                     "parameter": 1 + (i // 2) % 4, "device_id": DEVICE_ID})
        rows.append({"timestamp": base + timedelta(seconds=i + 1), "event_id": 81,
                     "parameter": 1 + (i // 2) % 4, "device_id": DEVICE_ID})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 1. Failure replication on a worker thread
# ---------------------------------------------------------------------------

class ReplicationJob:
    """What an application service object might look like."""

    def __init__(self, work_dir: Path, event_source) -> None:
        self.work_dir = work_dir
        self.event_source = event_source
        self.stop_event = threading.Event()       # Cancel button sets this
        self.progress: queue.Queue[sr.ProgressEvent] = queue.Queue()
        self.sim: sr.ATCSimulation | None = None
        self.result: sr.ReplicationResult | None = None
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, name="replication-worker", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def cancel(self) -> None:
        self.stop_event.set()

    def get_status(self) -> dict:
        """JSON-safe status for a REST endpoint."""
        if self.sim is None:
            return {"state": "starting" if self.error is None else "failed"}
        return self.sim.get_status()

    def _run(self) -> None:
        # Construction stores the input events in the working database, so
        # it runs on the worker thread too.
        try:
            self.sim = sr.ATCSimulation(
                signals=[
                    sr.SignalConfig(
                        device_id=str(DEVICE_ID),          # package ids are strings
                        ip="127.0.0.1", udp_port=50161,    # never contacted here
                        http_port=None,                    # events come from event_source
                        incompatible_pairs=CONFLICT_PAIRS,
                    )
                ],
                events=detector_log(),
                replays=3,
                stop_on_conflict=True,
                simulation_speed=10.0,                     # fast for the demo; use 1.0 for real
                collection_interval_minutes=0.02,
                post_replay_settle_seconds=1,
                skip_comparison=True,
                event_source=self.event_source,
                stop_event=self.stop_event,
                on_progress=self.progress.put,
                work_dir=self.work_dir,
            )
            self.result = self.sim.run()
        except BaseException as exc:  # an application would log this
            self.error = exc


def replicate(work_root: Path, cancel_after: float | None) -> sr.ReplicationResult:
    controller = OfflineController()
    source = SimulatedControllerLog(fault_after_seconds=10)
    job = ReplicationJob(work_root / "replication", source)

    with patch("signal_replay.replay.async_send_ntcip", controller.send), \
            patch("signal_replay.replay.async_reset_all_detectors", controller.reset):
        job.start()
        started = time.monotonic()
        last_line = ""
        while job.thread.is_alive():
            job.thread.join(0.5)
            # What a UI timer would do: drain events and show the status.
            while True:
                try:
                    event = job.progress.get_nowait()
                except queue.Empty:
                    break
                if event.stage in (sr.Stage.CONFLICT, sr.Stage.RUN_COMPLETE, sr.Stage.CANCELLED):
                    print(f"  event: {event.stage.value}: {event.message}")
            status = job.get_status()
            line = (f"  status: {status.get('state')} stage={status.get('stage')} "
                    f"run={status.get('run_number')}/{status.get('total_runs')} "
                    f"conflicts={status.get('conflicts_found', 0)}")
            if line != last_line:
                print(line)
                last_line = line
            if cancel_after is not None and time.monotonic() - started > cancel_after:
                print("  cancel requested")
                job.cancel()
                cancel_after = None

    if job.error is not None:
        raise job.error
    result = job.result
    print(f"Replication finished: stop_reason={result.stop_reason} replicated={result.replicated} "
          f"first_conflict_run={result.first_conflict_run} cancelled={result.cancelled}")
    print(f"  detectors reset: {result.detectors_reset}; SNMP sends (offline): {controller.sends}; "
          f"source polls: {source.calls}")
    for conflict in result.conflicts:
        print(f"  conflict run {conflict.run_number}: {conflict.conflict_details} "
              f"first {conflict.timestamp} x{conflict.occurrences}")
    return result


# ---------------------------------------------------------------------------
# 2. Software-validation comparison (baseline vs candidate events)
# ---------------------------------------------------------------------------

def phase_log(device_id: str, minutes: int, slow_cycles: range = range(0)) -> pd.DataFrame:
    """Synthetic controller output: a 60 s, two-phase cycle; ``slow_cycles`` run 3 s long."""
    base = datetime(2026, 1, 5, 7, 0)
    rows = []
    t = base
    for cycle in range(minutes):
        extra = 3 if cycle in slow_cycles else 0
        for offset, event_id, parameter in ((0, 1, 2), (25 + extra, 8, 2), (29 + extra, 10, 2),
                                            (31 + extra, 1, 4), (55 + extra, 8, 4), (58 + extra, 10, 4)):
            rows.append({"device_id": device_id, "timestamp": t + timedelta(seconds=offset),
                         "event_id": event_id, "parameter": parameter})
        t += timedelta(seconds=60 + extra)
    return pd.DataFrame(rows)


def validate() -> list[sr.ScenarioResult]:
    scenarios = [
        sr.TestScenario(scenario_id="A100", database_name="A100.bin", events_source="",
                        test_type=sr.TestType.SIMILARITY, tod_align=False),
        sr.TestScenario(scenario_id="B200", database_name="B200.bin", events_source="",
                        test_type=sr.TestType.SIMILARITY, tod_align=False),
    ]
    # In an application these are the baseline run's and the candidate run's
    # collected events (for example read from each run's working database).
    baseline = {"A100": phase_log("A100", 120), "B200": phase_log("B200", 120)}
    candidate = {"A100": phase_log("A100", 120), "B200": phase_log("B200", 120, slow_cycles=range(40, 80))}
    settings = sr.ValidationSettings(settle_minutes=0, baseline_label="2.15.1", candidate_label="2.18.1")
    results = sr.compare_validation(baseline, candidate, scenarios, settings, max_workers=0)
    for r in results:
        match = "n/a" if r.match_percentage is None else f"{r.match_percentage:.1f}%"
        timing = "n/a" if r.timing_match_percentage is None else f"{r.timing_match_percentage:.1f}%"
        print(f"  {r.scenario_id}: {'PASS' if r.passed else 'FAIL'} (sequence match {match}, "
              f"timing match {timing}){' error: ' + r.error if r.error else ''}")
    return results


# ---------------------------------------------------------------------------
# 3. Results into the application's own DuckDB
# ---------------------------------------------------------------------------

def store(app_db: Path, frames: dict[str, pd.DataFrame], prefix: str) -> None:
    """Append every frame to ``<prefix>_<name>``, creating tables on first use."""
    con = duckdb.connect(str(app_db))
    try:
        for name, df in frames.items():
            table = f"{prefix}_{name}"
            con.register("df", df)
            con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0")
            con.execute(f"INSERT INTO {table} SELECT * FROM df")
            con.unregister("df")
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--cancel-after", type=float, default=None,
                        help="cancel the replication after this many seconds")
    parser.add_argument("--keep", action="store_true", help="keep the temporary folder")
    args = parser.parse_args()

    root = Path(tempfile.mkdtemp(prefix="sr_app_"))
    app_db = root / "app.duckdb"              # the application's own database
    print(f"Working in {root}")
    try:
        print("1. Failure replication (offline)")
        result = replicate(root, args.cancel_after)
        store(app_db, sr.results_to_frames(result), "replay")
        # The run is over and no file is open: the app copies what it needs
        # from the package's working folder, then deletes it.
        print(f"  manifest files: {sr.read_manifest(result.work_dir)['files']}")
        shutil.rmtree(result.work_dir)

        print("2. Software validation comparison")
        results = validate()
        store(app_db, sr.results_to_frames(results), "validation")

        con = duckdb.connect(str(app_db), read_only=True)
        try:
            tables = con.execute(
                "SELECT table_name, estimated_size FROM duckdb_tables() ORDER BY table_name"
            ).fetchall()
        finally:
            con.close()
        print("3. Application database tables:")
        for table, rows in tables:
            print(f"  {table}: {rows} row(s)")
    finally:
        if args.keep:
            print(f"Kept {root}")
        else:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
