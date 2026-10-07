import duckdb
from pathlib import Path
from unittest.mock import patch

import signal_replay as sr


def _build_suite(tmp_path: Path) -> sr.SoftwareTestSuite:
    scenario = sr.TestScenario(
        scenario_id="S1",
        database_name="S1.bin",
        events_source="S1.parquet",
        test_type=sr.TestType.SIMILARITY,
    )
    batch = sr.TestBatch(batch_id="batch_1", assignments={"S1": "127.0.0.1:1025"})
    return sr.SoftwareTestSuite(
        suite_name="suite",
        software_version="new",
        baseline_version="old",
        scenarios=[scenario],
        batches=[batch],
        output_dir=str(tmp_path),
    )


def test_failed_batch_is_not_marked_complete_and_data_is_cleared(tmp_path):
    suite = _build_suite(tmp_path)
    runner = sr.BatchRunner(suite, debug=False)

    db_path = runner.run_dir / "collected.db"
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE events (
            device_id VARCHAR,
            run_number INTEGER,
            timestamp TIMESTAMP,
            event_id INTEGER,
            parameter INTEGER
        )
        """
    )
    con.execute(
        "INSERT INTO events VALUES ('S1', 1, '2026-01-01 12:00:00', 1, 1)"
    )
    con.execute(
        "INSERT INTO events VALUES ('S2', 1, '2026-01-01 12:00:01', 1, 1)"
    )
    con.execute(
        """
        CREATE TABLE latency_offset_updates (
            run_number INTEGER,
            device_id VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE latency_offset_samples (
            run_number INTEGER,
            device_id VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )
    con.execute("INSERT INTO latency_offset_updates VALUES (1, 'S1', '2026-01-01 12:00:00')")
    con.execute("INSERT INTO latency_offset_updates VALUES (1, 'S2', '2026-01-01 12:00:01')")
    con.execute("INSERT INTO latency_offset_samples VALUES (1, 'S1', '2026-01-01 12:00:00')")
    con.execute("INSERT INTO latency_offset_samples VALUES (1, 'S2', '2026-01-01 12:00:01')")
    con.close()

    with patch.object(runner, "_run_similarity_batch", side_effect=RuntimeError("boom")):
        checkpoint = runner.run(db_loader_callback=lambda *_args: True)

    assert "batch_1" not in checkpoint.get("completed_batches", [])
    assert "S1" not in checkpoint.get("scenario_db_map", {})
    assert "batch_1" in checkpoint.get("batch_errors", {})

    con = duckdb.connect(str(db_path))
    rows = con.execute("SELECT COUNT(*) FROM events WHERE device_id = 'S1'").fetchone()[0]
    other_rows = con.execute("SELECT COUNT(*) FROM events WHERE device_id = 'S2'").fetchone()[0]
    latency_rows = con.execute("SELECT COUNT(*) FROM latency_offset_updates WHERE device_id = 'S1'").fetchone()[0]
    other_latency_rows = con.execute("SELECT COUNT(*) FROM latency_offset_updates WHERE device_id = 'S2'").fetchone()[0]
    sample_rows = con.execute("SELECT COUNT(*) FROM latency_offset_samples WHERE device_id = 'S1'").fetchone()[0]
    other_sample_rows = con.execute("SELECT COUNT(*) FROM latency_offset_samples WHERE device_id = 'S2'").fetchone()[0]
    con.close()
    assert rows == 0
    assert other_rows == 1
    assert latency_rows == 0
    assert other_latency_rows == 1
    assert sample_rows == 0
    assert other_sample_rows == 1


def test_similarity_batch_passes_suite_replay_latency_to_signal_config(tmp_path):
    suite = _build_suite(tmp_path)
    suite.replay_latency_offset_seconds = 1.55
    runner = sr.BatchRunner(suite, debug=False)
    captured = {}

    class FakeSimulation:
        def __init__(self, *, signals, **_kwargs):
            captured["signals"] = signals

        def run(self):
            return None

    with patch.object(sr.batch_runner, "ATCSimulation", FakeSimulation):
        runner._run_similarity_batch(
            suite.batches[0],
            ["S1"],
            db_loader_callback=lambda *_args: True,
        )

    assert captured["signals"][0].replay_latency_offset_seconds == 1.55


def test_similarity_batch_passes_adaptive_latency_settings_to_simulation(tmp_path):
    suite = _build_suite(tmp_path)
    suite.replay_latency_offset_lookback_min = 10.0
    suite.replay_latency_offset_min_samples = 12
    runner = sr.BatchRunner(suite, debug=False)
    captured = {}

    class FakeSimulation:
        def __init__(self, *, signals, **kwargs):
            captured["signals"] = signals
            captured["kwargs"] = kwargs

        def run(self):
            return None

    with patch.object(sr.batch_runner, "ATCSimulation", FakeSimulation):
        runner._run_similarity_batch(
            suite.batches[0],
            ["S1"],
            db_loader_callback=lambda *_args: True,
        )

    assert captured["kwargs"]["replay_latency_offset_lookback_min"] == 10.0
    assert captured["kwargs"]["replay_latency_offset_min_samples"] == 12


def test_similarity_batch_raises_when_simulation_reports_collection_error(tmp_path):
    suite = _build_suite(tmp_path)
    runner = sr.BatchRunner(suite, debug=False)

    class FakeSimulation:
        def __init__(self, **_kwargs):
            pass

        def run(self):
            return {"collection_error": True}

    with patch.object(sr.batch_runner, "ATCSimulation", FakeSimulation):
        try:
            runner._run_similarity_batch(
                suite.batches[0],
                ["S1"],
                db_loader_callback=lambda *_args: True,
            )
        except RuntimeError as exc:
            assert "Data collection failed during similarity batch batch_1" in str(exc)
        else:
            raise AssertionError("Expected collection error to fail the batch")


def test_conflict_scenario_passes_suite_replay_latency_to_signal_config(tmp_path):
    suite = _build_suite(tmp_path)
    suite.scenarios[0].test_type = sr.TestType.CONFLICT
    suite.replay_latency_offset_seconds = 1.55
    runner = sr.BatchRunner(suite, debug=False)
    captured = {}

    class FakeSimulation:
        def __init__(self, *, signals, **_kwargs):
            captured["signals"] = signals

        def run(self):
            return None

    with patch.object(sr.batch_runner, "ATCSimulation", FakeSimulation):
        runner._run_conflict_scenario(
            suite.batches[0],
            "S1",
            db_loader_callback=lambda *_args: True,
        )

    assert captured["signals"][0].replay_latency_offset_seconds == 1.55


def test_conflict_scenario_raises_when_simulation_reports_collection_error(tmp_path):
    suite = _build_suite(tmp_path)
    suite.scenarios[0].test_type = sr.TestType.CONFLICT
    runner = sr.BatchRunner(suite, debug=False)

    class FakeSimulation:
        def __init__(self, **_kwargs):
            pass

        def run(self):
            return {"collection_error": True}

    with patch.object(sr.batch_runner, "ATCSimulation", FakeSimulation):
        try:
            runner._run_conflict_scenario(
                suite.batches[0],
                "S1",
                db_loader_callback=lambda *_args: True,
            )
        except RuntimeError as exc:
            assert "Data collection failed during conflict scenario S1" in str(exc)
        else:
            raise AssertionError("Expected collection error to fail the scenario")


def _legacy_db(db_path: Path) -> None:
    """A collected.db as written by 0.x: simulation_runs keyed by run_number only."""
    con = duckdb.connect(str(db_path))
    con.execute(
        "CREATE TABLE events (device_id VARCHAR, run_number INTEGER, timestamp TIMESTAMP, "
        "event_id INTEGER, parameter INTEGER, "
        "PRIMARY KEY (device_id, run_number, timestamp, event_id, parameter))"
    )
    con.execute("INSERT INTO events VALUES ('S1', 1, '2026-01-01 12:00:00', 1, 1)")
    con.execute("INSERT INTO events VALUES ('S2', 1, '2026-01-01 12:00:01', 1, 1)")
    con.execute(
        "CREATE TABLE simulation_runs (run_number INTEGER PRIMARY KEY, status VARCHAR, "
        "started_at TIMESTAMP, completed_at TIMESTAMP)"
    )
    con.execute(
        "INSERT INTO simulation_runs VALUES (1, 'completed', '2026-01-01 12:00:00', '2026-01-01 12:10:00')"
    )
    con.close()


def test_clear_scenario_data_handles_legacy_simulation_runs(tmp_path):
    suite = _build_suite(tmp_path)
    runner = sr.BatchRunner(suite, debug=False)
    db_path = runner.run_dir / "collected.db"
    _legacy_db(db_path)

    runner._clear_scenario_data(db_path, ["S1"])

    con = duckdb.connect(str(db_path))
    assert con.execute("SELECT device_id FROM events").fetchall() == [("S2",)]
    assert con.execute("SELECT COUNT(*) FROM simulation_runs").fetchone()[0] == 1
    con.close()
    # Opening it migrates simulation_runs; the cleared scenario has no done run.
    db = sr.DatabaseManager(str(db_path))
    assert db.get_completed_run_numbers(["S1"]) == []
    assert db.get_completed_run_numbers(["S2"]) == [1]


def test_failed_batch_on_legacy_db_reports_the_real_error(tmp_path):
    suite = _build_suite(tmp_path)
    runner = sr.BatchRunner(suite, debug=False)
    db_path = runner.run_dir / "collected.db"
    _legacy_db(db_path)

    with patch.object(runner, "_run_similarity_batch", side_effect=RuntimeError("boom")):
        checkpoint = runner.run(db_loader_callback=lambda *_args: True)

    assert "boom" in str(checkpoint.get("batch_errors", {}).get("batch_1"))
    con = duckdb.connect(str(db_path))
    assert con.execute("SELECT COUNT(*) FROM events WHERE device_id = 'S1'").fetchone()[0] == 0
    con.close()
