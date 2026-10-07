"""
Results model, storage contract and the validation comparison (stage 5).

Everything is mocked or synthetic: SignalReplay.run is replaced by a fake
that records replay timing, output events come from an in-test source, and
no SNMP or HTTP leaves the machine.
"""

import json
import os
import shutil
import threading
import time
from datetime import datetime, timedelta
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest

import signal_replay as sr
from signal_replay.collector import ConflictRecord, DatabaseManager, SCHEMA_VERSION
from signal_replay.comparison import DivergenceWindow, DTWResult, ComparisonResult

DEVICE = "dev"
SHIFT_DAYS = 3


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _no_http():
    return patch("requests.get", side_effect=AssertionError("HTTP must not be used"))


def _input_events(device_id=DEVICE):
    base = datetime(2024, 1, 1, 12)
    return pd.DataFrame([
        {"timestamp": base + timedelta(seconds=i), "event_id": 82 if i % 2 == 0 else 81,
         "parameter": 1, "device_id": device_id}
        for i in range(4)
    ])


def _fake_replay_run(state, delay=0.2, block_on=None):
    """SignalReplay.run replacement: records TOD-style timing, honours stops.

    ``state['calls']`` counts runs; the call numbered ``block_on`` keeps
    running until the simulation is stopped.
    """
    def run(self):
        with state["lock"]:
            state["calls"] += 1
            call = state["calls"]
        now = datetime.now()
        self.simulation_start_time = now
        self.replay_info.update(
            mode="tod", replay_start=now, date_shift_seconds=SHIFT_DAYS * 86400.0,
            source_start=now - timedelta(days=SHIFT_DAYS), events_sent=4, events_total=4,
        )
        deadline = time.monotonic() + delay
        while not self._stop_event.is_set():
            if call != block_on and time.monotonic() >= deadline:
                break
            time.sleep(0.02)
        self.replay_info["replay_end"] = datetime.now()
        self.replay_info["source_end"] = self.replay_info["replay_end"] - timedelta(days=SHIFT_DAYS)
        self.detectors_reset = True
        return now
    return run


def _source(state, conflict_on=(2,)):
    """Output events stamped 'now': a Ph2/Ph6 conflict during the listed runs."""
    def source(target, since):
        now = datetime.now()
        rows = [{"DeviceId": target.device_id, "TimeStamp": now, "EventId": 1, "Parameter": 2}]
        if state["calls"] in conflict_on:
            rows.append({"DeviceId": target.device_id, "TimeStamp": now, "EventId": 1, "Parameter": 6})
        return rows
    return source


def _state():
    return {"calls": 0, "lock": threading.Lock()}


def _sim(tmp_path=None, source=None, device_id=DEVICE, **kwargs):
    signal = sr.SignalConfig(
        device_id=device_id, ip="127.0.0.1", udp_port=1025, http_port=None,
        incompatible_pairs=kwargs.pop("pairs", [("Ph2", "Ph6")]),
    )
    kwargs.setdefault("skip_comparison", True)
    kwargs.setdefault("post_replay_settle_seconds", 0)
    if "work_dir" not in kwargs and "db_path" not in kwargs:
        kwargs["db_path"] = str(tmp_path / "sim.db")
    return sr.ATCSimulation(
        signals=[signal], events=_input_events(device_id), replays=kwargs.pop("replays", 1),
        stop_on_conflict=kwargs.pop("stop_on_conflict", True),
        event_source=source, **kwargs,
    )


def _phase_events(start, n_cycles=20, swap=range(0)):
    rows = []
    t = start
    for cycle in range(n_cycles):
        for phase in (2, 6, 4, 8):
            p = phase + 10 if cycle in swap else phase
            rows.append((t, 1, p))
            rows.append((t + timedelta(seconds=20), 8, p))
            rows.append((t + timedelta(seconds=24), 10, p))
            rows.append((t + timedelta(seconds=26), 11, p))
            t += timedelta(seconds=13)
    return pd.DataFrame(rows, columns=["timestamp", "event_id", "parameter"])


def _json_roundtrip(data):
    return json.loads(json.dumps(data, allow_nan=False))


# ---------------------------------------------------------------------------
# Replication semantics
# ---------------------------------------------------------------------------

def test_conflict_in_run_2_of_5_is_replicated_with_source_time(tmp_path):
    state = _state()
    with _no_http(), patch.object(sr.SignalReplay, "run", _fake_replay_run(state)):
        sim = _sim(tmp_path, _source(state), replays=5)
        result = sim.run()

    assert isinstance(result, sr.ReplicationResult)
    assert sim.result is result and sim.last_results is result
    assert result.stop_reason == "conflict"
    assert result.replicated is True
    assert result.first_conflict_run == 2
    assert result.runs_attempted == 2
    assert result.runs_completed == 2
    assert result.completed_runs == [1, 2]
    assert [(r.device_id, r.run_number, r.status) for r in result.runs] == [
        (DEVICE, 1, "completed"), (DEVICE, 2, "completed"),
    ]
    assert result.runs[0].date_shift_seconds == SHIFT_DAYS * 86400.0
    assert result.runs[0].events_sent == 4 and result.runs[0].detectors_reset is True

    conflict = result.conflicts[0]
    assert isinstance(conflict, sr.ConflictRecord)
    assert (conflict.device_id, conflict.run_number, conflict.conflict_details) == (DEVICE, 2, "Ph2 & Ph6")
    assert conflict.pairs == [("Ph2", "Ph6")]
    assert conflict.first_timestamp == conflict.timestamp
    assert conflict.occurrences >= 1
    assert conflict.source_equivalent_timestamp == conflict.timestamp - timedelta(days=SHIFT_DAYS)
    assert conflict.stored is True

    # 0.x dict view still works.
    assert result["completed_runs"] == [1, 2]
    assert result["conflicts"][0]["conflict_details"] == "Ph2 & Ph6"
    assert result["conflicts"][0]["source_equivalent_timestamp"] == conflict.source_equivalent_timestamp
    assert result.get("stop_reason") == "conflict"
    assert "replicated" in result and dict(result)["first_conflict_run"] == 2
    assert "first conflict in run 2" in sim.format_summary()

    # Storage: per-device run rows with timing, conflict row with the source time.
    db = DatabaseManager(str(tmp_path / "sim.db"), read_only=True)
    runs = db.get_runs([DEVICE])
    assert runs[["run_number", "status"]].values.tolist() == [[1, "completed"], [2, "completed"]]
    assert set(runs["run_uuid"]) == {result.run_uuid}
    assert runs["date_shift_seconds"].tolist() == [SHIFT_DAYS * 86400.0] * 2
    stored = db.get_conflicts(device_id=DEVICE)
    assert stored["run_uuid"].tolist() == [result.run_uuid]
    assert pd.Timestamp(stored["source_equivalent_timestamp"].iloc[0]) == pd.Timestamp(conflict.source_equivalent_timestamp)
    meta = db.get_meta()
    assert meta["schema_version"] == str(SCHEMA_VERSION)
    assert meta["package_version"] == sr.__version__
    assert meta["run_uuid"] == result.run_uuid


def test_cancelled_run_is_recorded_per_device_and_resumed(tmp_path):
    state = _state()
    with _no_http(), patch.object(sr.SignalReplay, "run", _fake_replay_run(state, block_on=2)):
        sim = _sim(tmp_path, _source(state, conflict_on=()), replays=3)

        def _stop_when_run_2_starts():
            deadline = time.monotonic() + 30
            while state["calls"] < 2 and time.monotonic() < deadline:
                time.sleep(0.02)
            time.sleep(0.2)
            sim.request_stop()

        stopper = threading.Thread(target=_stop_when_run_2_starts, daemon=True)
        stopper.start()
        result = sim.run()
        stopper.join(5)

        assert result.stop_reason == "cancelled"
        assert result.cancelled is True and result.cancelled_run == 2
        assert result.completed_runs == [1]
        assert [(r.run_number, r.status) for r in result.runs] == [(1, "completed"), (2, "cancelled")]
        db = DatabaseManager(str(tmp_path / "sim.db"))
        assert db.get_run_status(2, device_id=DEVICE) == "cancelled"
        assert db.get_run_status(1, device_id=DEVICE) == "completed"
        assert db.get_completed_run_numbers([DEVICE]) == [1]

        resumed = _sim(tmp_path, _source(state, conflict_on=()), replays=3)
        assert resumed._run_offset == 1
        result2 = resumed.run()

    assert result2.completed_runs == [2, 3]
    assert result2.stop_reason == "completed"
    assert db.get_completed_run_numbers([DEVICE]) == [1, 2, 3]


def test_resume_reruns_a_failed_middle_run_and_keeps_earlier_conflicts(tmp_path):
    state = _state()
    with _no_http(), patch.object(sr.SignalReplay, "run", _fake_replay_run(state)):
        first = _sim(tmp_path, _source(state, conflict_on=(2,)), replays=4, stop_on_conflict=False).run()
        assert first.completed_runs == [1, 2, 3, 4]
        assert first.first_conflict_run == 2

        db = DatabaseManager(str(tmp_path / "sim.db"))
        db.mark_run_failed(3, device_ids=[DEVICE])  # e.g. the replay failed in run 3
        rows_before = {r: len(db.get_events(device_id=DEVICE, run_number=r)) for r in (1, 2, 4)}

        resumed = _sim(tmp_path, _source(state, conflict_on=()), replays=4, stop_on_conflict=False)
        result = resumed.run()

    assert result.completed_runs == [3]
    assert result.runs_attempted == 1
    assert result.prior_completed_runs == [1, 2, 4]
    # The run-2 conflict found by the first call is still reported.
    assert result.replicated is True
    assert result.first_conflict_run == 2
    assert [(c.run_number, c.conflict_details) for c in result.conflicts] == [(2, "Ph2 & Ph6")]
    assert isinstance(result.conflicts[0].occurrences, int)
    assert db.get_completed_run_numbers([DEVICE]) == [1, 2, 3, 4]
    assert {r: len(db.get_events(device_id=DEVICE, run_number=r)) for r in (1, 2, 4)} == rows_before
    frames = sr.results_to_frames(result)
    assert frames["conflicts"]["run_number"].tolist() == [2]


def test_resume_reruns_only_the_device_that_failed_and_keeps_the_others_data(tmp_path):
    state = _state()

    def _two_signal_sim(source):
        signals = [
            sr.SignalConfig(device_id=d, ip="127.0.0.1", udp_port=1025, http_port=None,
                            incompatible_pairs=[("Ph2", "Ph6")])
            for d in ("A", "B")
        ]
        events = pd.concat([_input_events("A"), _input_events("B")], ignore_index=True)
        return sr.ATCSimulation(
            signals=signals, events=events, replays=2, stop_on_conflict=False,
            event_source=source, db_path=str(tmp_path / "sim.db"),
            skip_comparison=True, post_replay_settle_seconds=0,
        )

    with _no_http(), patch.object(sr.SignalReplay, "run", _fake_replay_run(state)):
        first = _two_signal_sim(_source(state, conflict_on=()))
        assert first.run().completed_runs == [1, 2]

        db = DatabaseManager(str(tmp_path / "sim.db"))
        stamp = datetime.now()
        db.insert_conflict(ConflictRecord(device_id="A", run_number=2, timestamp=stamp,
                                          conflict_details="Ph2 & Ph6"))
        db.mark_run_failed(2, device_ids=["B"])
        a_rows = len(db.get_events(device_id="A", run_number=2))
        assert a_rows > 0

        resumed = _two_signal_sim(_source(state, conflict_on=()))
        result = resumed.run()

    assert result.completed_runs == [2]
    assert [(r.device_id, r.run_number, r.status) for r in result.runs] == [("B", 2, "completed")]
    assert db.get_run_status(2, device_id="A") == "completed"
    assert db.get_run_status(2, device_id="B") == "completed"
    # A finished run 2 before: its events and its conflict were not cleared.
    assert len(db.get_events(device_id="A", run_number=2)) == a_rows
    assert db.get_conflicts(device_id="A", run_number=2)["conflict_details"].tolist() == ["Ph2 & Ph6"]
    assert result.replicated is True and result.first_conflict_run == 2


def test_conflict_store_failure_is_flagged(tmp_path):
    state = _state()
    with _no_http(), patch.object(sr.SignalReplay, "run", _fake_replay_run(state)), patch.object(
        DatabaseManager, "insert_conflict", side_effect=RuntimeError("disk full")
    ):
        sim = _sim(tmp_path, _source(state, conflict_on=(1,)), replays=2)
        result = sim.run()

    assert result.replicated is True
    assert result.conflicts[0].stored is False
    assert len(result.conflict_store_errors) == 1
    assert "disk full" in result.conflict_store_errors[0]
    assert result["conflict_store_errors"] == result.conflict_store_errors
    assert "could not be written" in sim.format_summary()
    assert DatabaseManager(str(tmp_path / "sim.db")).get_conflicts().empty


def test_replication_result_json_roundtrip(tmp_path):
    state = _state()
    with _no_http(), patch.object(sr.SignalReplay, "run", _fake_replay_run(state)):
        result = _sim(tmp_path, _source(state, conflict_on=(1,))).run()

    data = _json_roundtrip(result.to_dict())
    assert json.loads(result.to_json())["run_uuid"] == result.run_uuid
    back = sr.ReplicationResult.from_dict(data)
    for name in ("run_uuid", "stop_reason", "replicated", "first_conflict_run", "runs_attempted",
                 "runs_completed", "completed_runs", "cancelled", "db_path"):
        assert getattr(back, name) == getattr(result, name), name
    assert back.conflicts[0].timestamp == result.conflicts[0].timestamp
    assert back.conflicts[0].source_equivalent_timestamp == result.conflicts[0].source_equivalent_timestamp
    assert back.runs[0].replay_start == result.runs[0].replay_start
    assert back.collection_health_by_run.keys() == result.collection_health_by_run.keys()


# ---------------------------------------------------------------------------
# Working folder
# ---------------------------------------------------------------------------

def test_simulation_work_dir_files_manifest_and_delete(tmp_path):
    work_dir = tmp_path / "ws"
    state = _state()
    cwd_db = os.path.join(os.getcwd(), "atc_replay.db")
    existed = os.path.exists(cwd_db)
    with _no_http(), patch.object(sr.SignalReplay, "run", _fake_replay_run(state)):
        sim = _sim(source=_source(state, conflict_on=()), work_dir=work_dir, run_log=True,
                   replays=2, skip_comparison=False, stop_on_conflict=False)
        result = sim.run()

    assert result.work_dir == str(work_dir)
    assert result.db_path == str(work_dir / "replay.duckdb")
    assert os.path.exists(cwd_db) == existed  # nothing written to the current directory
    manifest = sr.read_manifest(work_dir)
    assert manifest["run_uuid"] == result.run_uuid
    assert manifest["package_version"] == sr.__version__
    assert manifest["schema_version"] == SCHEMA_VERSION
    assert manifest["kind"] == "replication"
    assert {"replay.duckdb", "run.log"} <= set(manifest["files"])
    assert result.comparisons, "input-vs-run comparisons were expected"

    # Post-run reads work read-only, then the folder can be deleted.
    runs = DatabaseManager(result.db_path, read_only=True).get_runs()
    assert len(runs) == 2
    frames = sr.results_to_frames(result)
    assert len(frames["runs"]) == 2 and len(frames["comparison_scores"]) == len(result.comparisons)
    shutil.rmtree(work_dir)
    assert not work_dir.exists()


def test_two_simulations_with_separate_work_dirs_do_not_interfere(tmp_path):
    state = _state()
    with _no_http(), patch.object(sr.SignalReplay, "run", _fake_replay_run(state)):
        first = _sim(source=_source(state, conflict_on=()), work_dir=tmp_path / "a", device_id="A").run()
        second = _sim(source=_source(state, conflict_on=()), work_dir=tmp_path / "b", device_id="B").run()

    assert first.run_uuid != second.run_uuid
    assert DatabaseManager(first.db_path, read_only=True).get_runs()["device_id"].tolist() == ["A"]
    assert DatabaseManager(second.db_path, read_only=True).get_runs()["device_id"].tolist() == ["B"]
    shutil.rmtree(tmp_path / "a")
    shutil.rmtree(tmp_path / "b")


def test_batch_runner_work_dir_context_manager_and_delete(tmp_path):
    work_dir = tmp_path / "batch_ws"
    events_path = tmp_path / "S1.parquet"
    _input_events("S1").to_parquet(events_path, index=False)
    suite = sr.SoftwareTestSuite(
        suite_name="suite", software_version="2.0", baseline_version="1.0",
        scenarios=[sr.TestScenario("S1", "S1.bin", str(events_path), sr.TestType.SIMILARITY)],
        batches=[sr.TestBatch("b1", {"S1": "127.0.0.1:9701:9701"})],
        output_dir=str(tmp_path / "must_not_be_used"),
        post_replay_settle_seconds=0,
    )
    state = _state()
    with _no_http(), patch.object(sr.SignalReplay, "run", _fake_replay_run(state)):
        with sr.BatchRunner(suite, work_dir=work_dir, event_source=_source(state, conflict_on=())) as runner:
            db_path = runner.run_batch_once(suite.batches[0], db_loader_callback=lambda req: True)

    assert db_path == work_dir / "collected.db"
    assert not (tmp_path / "must_not_be_used").exists()
    manifest = sr.read_manifest(work_dir)
    assert manifest["kind"] == "batch" and manifest["run_uuid"] == runner.run_uuid
    assert {"collected.db", "run.log"} <= set(manifest["files"])
    assert len(manifest["simulation_run_uuids"]) == 1
    assert DatabaseManager(str(db_path), read_only=True).get_run_status(1, device_id="S1") == "completed"
    shutil.rmtree(work_dir)


def test_store_comparison_result_closes_connection_on_failure(tmp_path):
    db_path = tmp_path / "bad.db"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE comparison_results (device_id INTEGER)")
    con.close()
    empty = DTWResult(float("inf"), float("inf"), [], 0, 0)
    result = ComparisonResult("X", 1, 2, empty, empty, [], 0.0)
    with pytest.raises(duckdb.Error):
        sr.comparison.store_comparison_result(str(db_path), result)
    os.remove(db_path)  # fails on Windows if the connection leaked


# ---------------------------------------------------------------------------
# simulation_runs per device, migration, check_conflicts episodes
# ---------------------------------------------------------------------------

def test_simulation_runs_keep_one_row_per_device(tmp_path):
    db = DatabaseManager(str(tmp_path / "shared.db"))
    db.mark_run_started(1, device_ids=["A"], run_uuid="u1")
    db.mark_run_completed(1, device_ids=["A"])
    db.mark_run_started(1, device_ids=["B"], run_uuid="u2")
    db.mark_run_cancelled(1, device_ids=["B"])

    assert db.get_run_status(1, device_id="A") == "completed"
    assert db.get_run_status(1, device_id="B") == "cancelled"
    assert db.get_run_status(1) == "cancelled"  # least finished over devices
    assert db.get_completed_run_numbers(["A"]) == [1]
    assert db.get_completed_run_numbers(["B"]) == []
    assert db.get_max_run_number(["A", "B"]) == 0
    runs = db.get_runs()
    assert runs[["device_id", "run_uuid"]].values.tolist() == [["A", "u1"], ["B", "u2"]]

    db.clear_device_data(["B"])
    assert db.get_runs()["device_id"].tolist() == ["A"]


def test_two_simulations_sharing_a_db_keep_their_own_run_status(tmp_path):
    state = _state()
    db_path = str(tmp_path / "shared.db")
    with _no_http(), patch.object(sr.SignalReplay, "run", _fake_replay_run(state)):
        _sim(source=_source(state, conflict_on=()), db_path=db_path, device_id="A").run()
        _sim(source=_source(state, conflict_on=()), db_path=db_path, device_id="B").run()

    db = DatabaseManager(db_path)
    runs = db.get_runs()
    assert runs[["device_id", "run_number", "status"]].values.tolist() == [
        ["A", 1, "completed"], ["B", 1, "completed"],
    ]
    assert runs["run_uuid"].nunique() == 2


def test_legacy_simulation_runs_table_is_migrated_in_place(tmp_path):
    db_path = str(tmp_path / "legacy.db")
    con = duckdb.connect(db_path)
    con.execute(
        "CREATE TABLE events (device_id VARCHAR, run_number INTEGER, timestamp TIMESTAMP, "
        "event_id INTEGER, parameter INTEGER, PRIMARY KEY (device_id, run_number, timestamp, event_id, parameter))"
    )
    con.execute(
        "INSERT INTO events VALUES ('A', 1, '2024-01-01 00:00:00', 1, 1), "
        "('B', 1, '2024-01-01 00:00:00', 1, 1), ('A', 2, '2024-01-01 00:00:00', 1, 1)"
    )
    con.execute("CREATE TABLE simulation_runs (run_number INTEGER PRIMARY KEY, status VARCHAR, "
                "started_at TIMESTAMP, completed_at TIMESTAMP)")
    con.execute("INSERT INTO simulation_runs VALUES (1, 'completed', now(), now()), (2, 'cancelled', now(), now())")
    con.close()

    db = DatabaseManager(db_path)
    runs = db.get_runs()
    assert runs[["device_id", "run_number", "status"]].values.tolist() == [
        ["A", 1, "completed"], ["A", 2, "cancelled"], ["B", 1, "completed"],
    ]
    assert db.get_max_run_number(["A"]) == 1
    assert db.get_meta()["schema_version"] == str(SCHEMA_VERSION)
    # Opening again is a no-op.
    assert len(DatabaseManager(db_path).get_runs()) == 3


def test_read_only_manager_answers_run_queries_on_a_legacy_db(tmp_path):
    db_path = str(tmp_path / "legacy.db")
    con = duckdb.connect(db_path)
    con.execute(
        "CREATE TABLE events (device_id VARCHAR, run_number INTEGER, timestamp TIMESTAMP, "
        "event_id INTEGER, parameter INTEGER, PRIMARY KEY (device_id, run_number, timestamp, event_id, parameter))"
    )
    con.execute(
        "INSERT INTO events VALUES ('A', 1, '2024-01-01 00:00:00', 1, 1), "
        "('B', 1, '2024-01-01 00:00:00', 1, 1), ('A', 2, '2024-01-01 00:00:00', 1, 1)"
    )
    con.execute("CREATE TABLE conflicts (device_id VARCHAR, run_number INTEGER, timestamp TIMESTAMP, "
                "conflict_details VARCHAR)")
    con.execute("CREATE TABLE simulation_runs (run_number INTEGER PRIMARY KEY, status VARCHAR, "
                "started_at TIMESTAMP, completed_at TIMESTAMP)")
    con.execute("INSERT INTO simulation_runs VALUES (1, 'completed', now(), now()), (2, 'cancelled', now(), now())")
    con.close()

    db = DatabaseManager(db_path, read_only=True)
    assert db.get_completed_run_numbers(["A"]) == [1]
    assert db.get_max_run_number(["A", "B"]) == 1
    assert db.get_run_status(1, "B") == "completed"
    assert db.get_run_status(2, "A") == "cancelled"
    assert db.get_runs(["A"])[["device_id", "run_number", "status"]].values.tolist() == [
        ["A", 1, "completed"], ["A", 2, "cancelled"],
    ]
    assert len(db.get_runs()) == 3
    # Nothing was written: the file still has the 0.x layout.
    con = duckdb.connect(db_path, read_only=True)
    assert "device_id" not in {r[1] for r in con.execute("PRAGMA table_info('simulation_runs')").fetchall()}
    con.close()


def test_check_conflicts_reports_last_timestamp_occurrences_and_duration():
    t0 = pd.Timestamp("2026-01-01 12:00:00")
    df = pd.DataFrame([
        (t0, 1, 2),
        (t0 + pd.Timedelta(seconds=1), 1, 6),   # conflict starts
        (t0 + pd.Timedelta(seconds=3), 10, 6),  # ends
        (t0 + pd.Timedelta(seconds=10), 1, 6),  # starts again
        (t0 + pd.Timedelta(seconds=14), 10, 2),  # ends
    ], columns=["TimeStamp", "EventTypeID", "Parameter"])

    conflicts = sr.check_conflicts(df, [("Ph2", "Ph6")])

    assert len(conflicts) == 1
    row = conflicts.iloc[0]
    assert row["TimeStamp"] == t0 + pd.Timedelta(seconds=1)
    assert row["Last_TimeStamp"] == t0 + pd.Timedelta(seconds=14)
    assert row["Occurrences"] == 2
    assert row["Duration_Seconds"] == pytest.approx(6.0)


# ---------------------------------------------------------------------------
# JSON round trips
# ---------------------------------------------------------------------------

def test_comparison_result_with_empty_side_is_json_safe():
    result = sr.compare_runs(_phase_events(datetime(2026, 1, 1, 12)), pd.DataFrame(
        columns=["timestamp", "event_id", "parameter"]), device_id="X")
    assert result.sequence_dtw.normalized_distance == float("inf")

    data = _json_roundtrip(result.to_dict())
    assert data["sequence_dtw"]["normalized_distance"] is None
    assert "warping_path" not in data["sequence_dtw"]
    back = sr.ComparisonResult.from_dict(data)
    assert back.sequence_dtw.normalized_distance == float("inf")
    assert back.match_percentage == result.match_percentage
    assert back.device_id == "X"


def test_comparison_result_roundtrip_keeps_windows_and_chunks():
    start = datetime(2026, 1, 1, 12)
    result = sr.compare_runs(
        _phase_events(start, 80), _phase_events(start + timedelta(seconds=100), 80, swap=range(40, 46)),
        device_id="X", auto_align=False, start_time_a=start, start_time_b=start + timedelta(seconds=100),
    )
    data = _json_roundtrip(result.to_dict())
    assert "warping_path" in _json_roundtrip(result.to_dict(include_warping_path=True))["sequence_dtw"]
    back = sr.ComparisonResult.from_dict(data)
    assert back.match_percentage == pytest.approx(result.match_percentage)
    assert len(back.divergence_windows) == len(result.divergence_windows) >= 1
    assert back.divergence_windows[0].start_timestamp_a == result.divergence_windows[0].start_timestamp_a
    assert [c.match_percentage for c in back.chunk_scores] == [c.match_percentage for c in result.chunk_scores]
    window = DivergenceWindow.from_dict(_json_roundtrip(result.divergence_windows[0].to_dict()))
    assert window == result.divergence_windows[0]


def test_scenario_result_with_timestamp_conflicts_roundtrip():
    result = sr.ScenarioResult(
        scenario_id="C1", test_type=sr.TestType.CONFLICT, software_version="2.0", passed=False,
        conflicts_found=[{"run_number": 1, "timestamp": pd.Timestamp("2026-01-01 12:00:01"),
                          "conflict_details": "Ph2 & Ph6"}],
        match_percentage=float("nan"),
        chunk_scores=[{"center_seconds": 1.0, "match_percentage": float("inf"), "window_seconds": 2.0}],
    )
    with pytest.raises(TypeError):
        json.dumps(result.conflicts_found)
    data = _json_roundtrip(result.to_dict())
    assert data["conflicts_found"][0]["timestamp"] == "2026-01-01T12:00:01"
    assert data["match_percentage"] is None and data["test_type"] == "conflict"
    back = sr.ScenarioResult.from_dict(data)
    assert back.test_type is sr.TestType.CONFLICT
    assert (back.scenario_id, back.passed, back.software_version) == ("C1", False, "2.0")


def test_conflict_and_run_record_roundtrip():
    conflict = sr.ConflictRecord(
        DEVICE, 2, pd.Timestamp("2026-01-01 12:00:01"), "Ph2 & Ph6; Ph4 & Ph8",
        last_timestamp=datetime(2026, 1, 1, 12, 0, 9), occurrences=2, duration_seconds=float("nan"),
    )
    data = _json_roundtrip(conflict.to_dict())
    assert data["duration_seconds"] is None
    back = sr.ConflictRecord.from_dict(data)
    assert back.timestamp == datetime(2026, 1, 1, 12, 0, 1)
    assert back.pairs == [("Ph2", "Ph6"), ("Ph4", "Ph8")]

    run = sr.RunRecord(DEVICE, 1, "completed", replay_start=datetime(2026, 1, 4, 12), mode="tod",
                       date_shift_seconds=3 * 86400.0)
    assert sr.RunRecord.from_dict(_json_roundtrip(run.to_dict())) == run
    assert run.source_time(datetime(2026, 1, 4, 12, 30)) == datetime(2026, 1, 1, 12, 30)
    relative = sr.RunRecord(DEVICE, 1, "completed", replay_start=datetime(2026, 1, 4, 12), mode="relative",
                            speed=2.0, date_shift_seconds=3 * 86400.0)
    assert relative.source_time(datetime(2026, 1, 4, 12, 10)) == datetime(2026, 1, 1, 12, 20)


# ---------------------------------------------------------------------------
# Divergence timestamps
# ---------------------------------------------------------------------------

def test_divergence_windows_have_absolute_timestamps():
    start = datetime(2026, 1, 1, 12)
    start_b = start + timedelta(seconds=100)
    result = sr.compare_runs(
        _phase_events(start, 80), _phase_events(start_b, 80, swap=range(40, 46)),
        device_id="X", auto_align=False, start_time_a=start, start_time_b=start_b,
    )
    assert result.divergence_windows
    for window in result.divergence_windows:
        assert window.start_timestamp_a == start + timedelta(seconds=window.original_start_seconds_a)
        assert window.end_timestamp_a == start + timedelta(seconds=window.original_end_seconds_a)
        assert window.start_timestamp_b == start_b + timedelta(seconds=window.original_start_seconds_b)
    first = result.divergence_windows[0]
    assert first.start_timestamp_a == start + timedelta(seconds=40 * 52)  # first swapped cycle
    assert (first.start_timestamp_b - first.start_timestamp_a) == timedelta(seconds=100)


def test_divergence_timestamps_with_auto_alignment_stay_in_each_sides_own_clock():
    start = datetime(2026, 1, 1, 12)
    a = _phase_events(start, 80)
    # B starts three cycles earlier and differs in its cycles 43-48.
    b_start = start - timedelta(seconds=3 * 52)
    b = _phase_events(b_start, 83, swap=range(43, 49))
    result = sr.compare_runs(a, b, device_id="X", auto_align=True)
    assert result.divergence_windows
    swapped_from = b_start + timedelta(seconds=43 * 52)
    swapped_to = b_start + timedelta(seconds=49 * 52 + 26)
    for window in result.divergence_windows:
        assert swapped_from - timedelta(seconds=30) <= window.start_timestamp_b <= swapped_to
        assert window.end_timestamp_b <= swapped_to + timedelta(seconds=30)
        assert a["timestamp"].min() <= window.start_timestamp_a <= a["timestamp"].max()


# ---------------------------------------------------------------------------
# compare_validation / compare_software
# ---------------------------------------------------------------------------

def _validation_suite(n_similarity=6, conflict=False):
    scenarios = [
        sr.TestScenario(f"S{i}", f"S{i}.bin", "unused.parquet", sr.TestType.SIMILARITY, tod_align=False)
        for i in range(n_similarity)
    ]
    if conflict:
        scenarios.append(sr.TestScenario(
            "C0", "C0.bin", "unused.parquet", sr.TestType.CONFLICT, replays=2,
            incompatible_pairs=[("Ph2", "Ph6")],
        ))
    return sr.SoftwareTestSuite(
        suite_name="suite", software_version="2.0", baseline_version="1.0",
        scenarios=scenarios, batches=[], analysis_settle_minutes=0.0,
    )


def _write_collected(db_path, scenario_ids, start, swap_for=()):
    db = DatabaseManager(str(db_path))
    for sid in scenario_ids:
        events = _phase_events(start, 60, swap=range(20, 26) if sid in swap_for else range(0))
        db.insert_events(
            events.rename(columns={"timestamp": "TimeStamp", "event_id": "EventTypeID", "parameter": "Parameter"}),
            sid, 1, start,
        )


def test_compare_software_six_scenarios_sharing_one_db_with_four_workers(tmp_path):
    suite = _validation_suite(6)
    ids = [s.scenario_id for s in suite.scenarios]
    start = datetime(2026, 1, 1, 12)
    for name in ("base", "new"):
        (tmp_path / name).mkdir()
    _write_collected(tmp_path / "base" / "collected.db", ids, start)
    _write_collected(tmp_path / "new" / "collected.db", ids, start + timedelta(days=1), swap_for={"S3"})
    # The baseline uses a checkpoint (BatchRunner.run layout), the new run does not.
    (tmp_path / "base" / "checkpoint.json").write_text(json.dumps(
        {"scenario_db_map": {sid: str(tmp_path / "base" / "collected.db") for sid in ids}}
    ))

    results = sr.compare_software(str(tmp_path / "base"), str(tmp_path / "new"), suite,
                                  output_dir=str(tmp_path / "out"), max_workers=4)

    assert [r.scenario_id for r in results] == ids
    assert all(r.error is None for r in results), [r.error for r in results]
    assert all(r.comparison is not None for r in results)
    assert results[3].num_divergences >= 1
    saved = json.loads((tmp_path / "out" / "comparison_results.json").read_text())
    assert [r["scenario_id"] for r in saved] == ids

    sequential = sr.compare_validation(
        str(tmp_path / "base" / "collected.db"), str(tmp_path / "new" / "collected.db"),
        suite, max_workers=1,
    )
    assert [(r.passed, r.match_percentage, r.num_divergences) for r in sequential] == [
        (r.passed, r.match_percentage, r.num_divergences) for r in results
    ]


class _DyingWorkerPool:
    """Stand-in for the process pool: the worker running ``killer`` dies.

    Jobs resolve shortly after submission on a timer thread. When the killer
    is among them, every unresolved job fails with BrokenProcessPool and the
    pool stays broken, which is what ProcessPoolExecutor does.
    """

    instances = []

    def __init__(self, workers, killer):
        from concurrent.futures.process import BrokenProcessPool

        self.workers = workers
        self.killer = killer
        self.broken_type = BrokenProcessPool
        self.broken = False
        self.unresolved = []
        self.lock = threading.Lock()
        self.timer = None
        _DyingWorkerPool.instances.append(self)

    def submit(self, fn, job):
        from concurrent.futures import Future

        with self.lock:
            if self.broken:
                raise self.broken_type("pool already broken")
            future = Future()
            self.unresolved.append((future, fn, job))
            if self.timer is None:
                self.timer = threading.Timer(0.2, self._resolve)
                self.timer.daemon = True
                self.timer.start()
            return future

    def _resolve(self):
        with self.lock:
            items, self.unresolved, self.timer = self.unresolved, [], None
            if any(job["scenario_id"] == self.killer for _f, _fn, job in items):
                self.broken = True
                for future, _fn, _job in items:
                    future.set_exception(self.broken_type("a worker process died"))
                return
        for future, fn, job in items:
            future.set_result(fn(job))

    def shutdown(self, wait=True, cancel_futures=False):
        pass


def test_compare_validation_survives_a_dying_worker(tmp_path):
    from signal_replay import validation as validation_mod

    suite = _validation_suite(5)
    ids = [s.scenario_id for s in suite.scenarios]
    start = datetime(2026, 1, 1, 12)
    _write_collected(tmp_path / "base.db", ids, start)
    _write_collected(tmp_path / "new.db", ids, start + timedelta(days=1))

    def fake_compare(job):
        return sr.ScenarioResult(
            scenario_id=job["scenario_id"], test_type=sr.TestType.SIMILARITY,
            software_version="2.0", passed=True,
        )

    _DyingWorkerPool.instances = []
    outcome = {}

    def _run():
        try:
            outcome["results"] = sr.compare_validation(
                str(tmp_path / "base.db"), str(tmp_path / "new.db"), suite, max_workers=2,
            )
        except BaseException as exc:  # pragma: no cover - reported below
            outcome["error"] = exc

    with patch.object(validation_mod, "_timed_compare", fake_compare), patch.object(
        validation_mod, "_make_pool", lambda n: _DyingWorkerPool(n, killer="S2")
    ):
        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        thread.join(30)
    assert not thread.is_alive(), "compare_validation hung after a worker died"
    assert "error" not in outcome, outcome.get("error")

    results = {r.scenario_id: r for r in outcome["results"]}
    assert [r.scenario_id for r in outcome["results"]] == ids
    assert results["S2"].passed is False
    assert "worker process died" in results["S2"].error
    for sid in ids:
        if sid != "S2":
            assert results[sid].error is None and results[sid].passed, sid
    # The scenarios caught in the broken pool were re-run one at a time.
    assert any(pool.workers == 1 for pool in _DyingWorkerPool.instances)


def test_compare_validation_with_dataframes_frames_and_conflicts(tmp_path):
    suite = _validation_suite(2, conflict=True)
    start = datetime(2026, 1, 1, 12)
    conflict_events = pd.DataFrame([
        {"run_number": 1, "timestamp": start, "event_id": 1, "parameter": 2},
        {"run_number": 1, "timestamp": start + timedelta(seconds=1), "event_id": 1, "parameter": 6},
        {"run_number": 1, "timestamp": start + timedelta(seconds=5), "event_id": 10, "parameter": 6},
    ])
    clean = conflict_events.iloc[[0]].copy()
    baseline = {"S0": _phase_events(start, 60), "S1": _phase_events(start, 60), "C0": conflict_events}
    candidate = {"S0": _phase_events(start, 60), "S1": _phase_events(start, 60, swap=range(20, 26)), "C0": clean}
    events = []
    results = sr.compare_validation(
        baseline, candidate, suite, sr.ValidationSettings(settle_minutes=0.0),
        on_progress=events.append, plots_dir=None,
    )

    assert [r.scenario_id for r in results] == ["S0", "S1", "C0"]
    assert results[0].passed is True and results[1].num_divergences >= 1
    conflict = results[2]
    assert conflict.passed is True  # baseline reproduced it, candidate did not
    assert [e.stage for e in events][-1] == sr.Stage.DONE

    frames = sr.results_to_frames(results, run_uuid="validation-1")
    assert set(frames) == set(sr.FRAME_COLUMNS)
    app = duckdb.connect(":memory:")
    try:
        for name, df in frames.items():
            assert list(df.columns) == sr.FRAME_COLUMNS[name]
            assert (df["run_uuid"] == "validation-1").all()
            app.register("df", df)
            app.execute(f"CREATE TABLE {name} AS SELECT * FROM df")
            app.execute(f"INSERT INTO {name} SELECT * FROM df")
            app.unregister("df")
            assert app.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0] == 2 * len(df)
        assert app.execute("SELECT COUNT(*) FROM scenario_results").fetchone()[0] == 6
        assert app.execute("SELECT COUNT(*) FROM comparison_scores").fetchone()[0] == 4
        kinds = {row[0] for row in app.execute("SELECT DISTINCT kind FROM scenario_findings").fetchall()}
        assert "analysis_diagnostic" in kinds
        windows = app.execute(
            "SELECT start_timestamp_a, start_timestamp_b FROM divergence_windows WHERE scenario_id = 'S1'"
        ).fetchall()
        assert windows and all(a is not None and b is not None for a, b in windows)
    finally:
        app.close()
    assert len(frames["conflicts"]) == 0  # the candidate had no conflict
    for result in results:
        _json_roundtrip(result.to_dict())


def test_results_to_frames_for_replication_result_inserts_into_app_db(tmp_path):
    state = _state()
    with _no_http(), patch.object(sr.SignalReplay, "run", _fake_replay_run(state)):
        result = _sim(tmp_path, _source(state, conflict_on=(2,)), replays=2, stop_on_conflict=False).run()

    frames = sr.results_to_frames(result)
    assert len(frames["runs"]) == 2 and len(frames["conflicts"]) == 1
    assert frames["runs"]["replay_start"].dtype.kind == "M"
    assert str(frames["runs"]["run_number"].dtype) == "Int64"
    app = duckdb.connect(":memory:")
    try:
        for name, df in frames.items():
            app.register("df", df)
            app.execute(f"CREATE TABLE replay_{name} AS SELECT * FROM df")
            app.unregister("df")
        row = app.execute(
            "SELECT run_uuid, device_id, run_number, source_equivalent_timestamp FROM replay_conflicts"
        ).fetchone()
    finally:
        app.close()
    assert row[0] == result.run_uuid and row[1] == DEVICE and row[2] == 2
    assert row[3] == result.conflicts[0].source_equivalent_timestamp


def test_validation_settings_from_script_mapping_and_suite():
    settings = sr.ValidationSettings.from_mapping({
        "settle_minutes": 7, "group_tolerance": 0.25, "max_divergence_plots": 2,
        "divergence_window_minutes": 4.0, "detector_similarity_threshold": 80.0,
        "phase_call_similarity_threshold": 85.0, "analysis_start_time": " 09:10 ",
        "sequence_threshold": 0.05,
    })
    assert (settings.settle_minutes, settings.group_tolerance, settings.max_plots) == (7, 0.25, 2)
    assert settings.window_minutes == 4.0 and settings.phase_call_threshold == 85.0
    assert settings.analysis_start_time == "09:10"

    suite = _validation_suite(1)
    suite.analysis_settle_minutes = 3.0
    suite.phase_call_similarity_threshold = 70.0
    from_suite = sr.ValidationSettings.from_suite(suite)
    assert (from_suite.settle_minutes, from_suite.phase_call_threshold) == (3.0, 70.0)
    assert (from_suite.baseline_label, from_suite.candidate_label) == ("1.0", "2.0")


def test_compare_validation_missing_inputs_give_error_results(tmp_path):
    suite = _validation_suite(2)
    results = sr.compare_validation({"S0": _phase_events(datetime(2026, 1, 1))}, {}, suite)
    assert [r.error for r in results] == [
        "No candidate events source for S0", "No baseline or candidate events source for S1",
    ]


def test_every_package_duckdb_connection_is_closed_in_finally():
    import ast
    from pathlib import Path

    openers = ("duckdb.connect(", "_connect_with_retry(", "self._read()", "_connect_read_only(")
    helpers = {"_connect_with_retry", "_read", "_connect_read_only", "_db"}
    problems = []
    for path in sorted(Path(sr.__file__).parent.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or node.name in helpers:
                continue
            source = ast.unparse(node)
            if any(opener in source for opener in openers) and "finally" not in source:
                problems.append(f"{path.name}:{node.lineno} {node.name}")
    assert not problems, problems


def test_empty_frames_have_column_types_for_create_table():
    """Tables created from empty frames get real types, so later inserts work."""
    empty = sr.results_to_frames([])
    con = duckdb.connect()
    try:
        for name, df in empty.items():
            con.register("df", df)
            con.execute(f"CREATE TABLE t_{name} AS SELECT * FROM df LIMIT 0")
            con.unregister("df")
            types = dict(con.execute(f"SELECT column_name, data_type FROM information_schema.columns "
                                     f"WHERE table_name = 't_{name}'").fetchall())
            assert types["run_uuid"] == "VARCHAR", (name, types)
            assert "INTEGER" not in types.values(), (name, types)
        result = sr.ScenarioResult(scenario_id="S1", test_type=sr.TestType.SIMILARITY,
                                   software_version="2.0", passed=True, match_percentage=99.0,
                                   notes="text", error=None)
        frames = sr.results_to_frames([result])
        con.register("df", frames["scenario_results"])
        con.execute("INSERT INTO t_scenario_results SELECT * FROM df")
        con.unregister("df")
        row = con.execute("SELECT scenario_id, passed, notes, error FROM t_scenario_results").fetchone()
        assert row == ("S1", True, "text", None)
    finally:
        con.close()
