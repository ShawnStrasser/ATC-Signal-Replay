from unittest.mock import patch
import sqlite3

import duckdb
import pandas as pd
import pytest

import signal_replay as sr


def _detector_events(device_id: str = "S1") -> pd.DataFrame:
    base = pd.Timestamp("2026-01-01 09:00:00")
    rows = []
    for index, event_id in enumerate((82, 81, 82, 81)):
        rows.append(
            {
                "timestamp": base + pd.Timedelta(seconds=index * 5),
                "event_id": event_id,
                "parameter": 1,
                "device_id": device_id,
            }
        )
    return pd.DataFrame(rows)


def test_signal_replay_releases_input_dataframe_after_feed_generation():
    signal = sr.SignalConfig(device_id="S1", ip="127.0.0.1", udp_port=9701, http_port=None)
    signal.events = _detector_events()

    replay = sr.SignalReplay(signal)

    assert replay.input_data is None
    assert replay.activation_feed is not None
    assert replay.get_run_duration() > 0


def test_signal_replay_loads_parquet_with_deviceid_column(tmp_path):
    parquet_path = tmp_path / "events.parquet"
    _detector_events().rename(columns={"device_id": "DeviceId"}).to_parquet(parquet_path, index=False)

    signal = sr.SignalConfig(device_id="S1", ip="127.0.0.1", udp_port=9701, http_port=None)
    signal.events = str(parquet_path)

    replay = sr.SignalReplay(signal)

    assert replay.input_data is None
    assert replay.activation_feed is not None
    assert replay.get_run_duration() > 0


def test_signal_replay_loads_comparison_events_from_parquet_eventtypeid_schema(tmp_path):
    parquet_path = tmp_path / "comparison_events.parquet"
    detector_df = _detector_events()
    comparison_df_source = pd.DataFrame(
        {
            "timestamp": [
                pd.Timestamp("2026-01-01 09:00:01"),
                pd.Timestamp("2026-01-01 09:00:06"),
                pd.Timestamp("2026-01-01 09:00:11"),
            ],
            "event_id": [1, 7, 9],
            "parameter": [2, 2, 2],
            "device_id": ["S1", "S1", "S1"],
        }
    )
    df = pd.concat([detector_df, comparison_df_source], ignore_index=True).rename(
        columns={
            "timestamp": "TimeStamp",
            "event_id": "EventTypeID",
            "parameter": "Parameter",
            "device_id": "DeviceId",
        }
    )
    df.to_parquet(parquet_path, index=False)

    signal = sr.SignalConfig(device_id="S1", ip="127.0.0.1", udp_port=9701, http_port=None)
    signal.events = str(parquet_path)

    replay = sr.SignalReplay(signal)
    comparison_df = replay.get_source_comparison_events()

    assert list(comparison_df.columns) == ["timestamp", "event_id", "parameter"]
    assert len(comparison_df) == len(comparison_df_source)
    assert comparison_df["event_id"].tolist() == comparison_df_source["event_id"].tolist()


def test_simulation_uses_preloaded_signal_events_without_central_distribution(temp_db_path):
    signal = sr.SignalConfig(device_id="S1", ip="127.0.0.1", udp_port=9701, http_port=None)
    preloaded_events = _detector_events()
    signal.events = preloaded_events

    class FakeDB:
        def __init__(self, _db_path):
            pass

        def get_max_run_number(self, device_ids=None):
            return 0

        def clear_run_data(self, _run_number=None, device_ids=None):
            return None

        def mark_run_started(self, _run_number):
            return None

        def mark_run_completed(self, _run_number):
            return None

        def insert_input_events(self, *_args, **_kwargs):
            return None

    def fake_store(self):
        self._cached_durations = {}

    with patch("signal_replay.orchestrator._distribute_events", side_effect=AssertionError("should not distribute")), patch(
        "signal_replay.orchestrator.DatabaseManager", FakeDB
    ), patch.object(sr.ATCSimulation, "_store_input_events", fake_store):
        sim = sr.ATCSimulation(signals=[signal], events=None, db_path=temp_db_path)

    assert sim.config.events is None
    assert sim.config.signals[0].events is preloaded_events


def test_similarity_batch_passes_per_signal_event_sources_to_simulation(tmp_path):
    events_1 = tmp_path / "events_1.parquet"
    events_2 = tmp_path / "events_2.parquet"
    _detector_events("S1").to_parquet(events_1, index=False)
    _detector_events("S2").to_parquet(events_2, index=False)

    scenarios = [
        sr.TestScenario(
            scenario_id="S1",
            database_name="S1.bin",
            events_source=str(events_1),
            test_type=sr.TestType.SIMILARITY,
        ),
        sr.TestScenario(
            scenario_id="S2",
            database_name="S2.bin",
            events_source=str(events_2),
            test_type=sr.TestType.SIMILARITY,
        ),
    ]
    suite = sr.FirmwareTestSuite(
        suite_name="suite",
        firmware_version="new",
        baseline_version="old",
        scenarios=scenarios,
        batches=[sr.TestBatch(batch_id="batch_1", assignments={"S1": "127.0.0.1:9701", "S2": "127.0.0.1:9702"})],
        output_dir=str(tmp_path),
    )
    runner = sr.BatchRunner(suite, debug=False)
    batch = suite.batches[0]
    captured = {}

    class FakeSimulation:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def run(self):
            return {"completed_runs": [1]}

    with patch("signal_replay.batch_runner.ATCSimulation", FakeSimulation):
        db_path = runner._run_similarity_batch(batch, ["S1", "S2"], db_loader_callback=lambda *_args: True)

    assert db_path == runner.run_dir / "collected.duckdb"
    assert captured["events"] is None
    # Events should be file paths (not loaded DataFrames) to avoid holding large data in memory
    assert all(isinstance(signal.events, str) for signal in captured["signals"])
    assert captured["signals"][0].events == str(events_1)
    assert captured["signals"][1].events == str(events_2)
    assert captured["snmp_send_retries"] == suite.snmp_send_retries
    assert captured["snmp_retry_backoff_seconds"] == suite.snmp_retry_backoff_seconds

    for handler in runner.logger.handlers:
        handler.close()
    runner.logger.handlers.clear()


def test_conflict_batch_uses_shared_version_db_without_rerun_mode(tmp_path):
    events_1 = tmp_path / "events_1.parquet"
    _detector_events("S1").to_parquet(events_1, index=False)

    scenario = sr.TestScenario(
        scenario_id="S1",
        database_name="S1.bin",
        events_source=str(events_1),
        test_type=sr.TestType.CONFLICT,
        replays=25,
    )
    suite = sr.FirmwareTestSuite(
        suite_name="suite",
        firmware_version="new",
        baseline_version="old",
        scenarios=[scenario],
        batches=[sr.TestBatch(batch_id="batch_1", assignments={"S1": "127.0.0.1:9701"})],
        output_dir=str(tmp_path),
    )
    runner = sr.BatchRunner(suite, debug=False)
    captured = {}

    class FakeSimulation:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def run(self):
            return {"completed_runs": [1]}

    with patch("signal_replay.batch_runner.ATCSimulation", FakeSimulation):
        db_path = runner._run_conflict_scenario(suite.batches[0], "S1", db_loader_callback=lambda *_args: True)

    assert db_path == runner.run_dir / "collected.duckdb"
    assert captured["replays"] == 25
    assert "replace_existing_device_data" not in captured

    for handler in runner.logger.handlers:
        handler.close()
    runner.logger.handlers.clear()


def test_database_manager_clear_run_data_can_scope_to_device_ids(temp_db_path):
    manager = sr.DatabaseManager(temp_db_path)

    con = duckdb.connect(temp_db_path)
    try:
        con.executemany(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?)",
            [
                ("S1", 1, pd.Timestamp("2026-01-01 12:00:00"), 1, 1),
                ("S1", 2, pd.Timestamp("2026-01-01 12:00:01"), 1, 1),
                ("S2", 1, pd.Timestamp("2026-01-01 12:00:02"), 1, 1),
            ],
        )
    finally:
        con.close()

    manager.clear_run_data(1, device_ids=["S1"])

    con = duckdb.connect(temp_db_path)
    try:
        remaining = con.execute(
            "SELECT device_id, run_number FROM events ORDER BY device_id, run_number"
        ).fetchall()
    finally:
        con.close()

    assert remaining == [("S1", 2), ("S2", 1)]


def test_database_manager_rejects_old_events_schema(temp_db_path):
    con = duckdb.connect(temp_db_path)
    con.execute(
        """
        CREATE TABLE events (
            device_id VARCHAR,
            run_number INTEGER,
            timestamp TIMESTAMP,
            event_id INTEGER,
            parameter INTEGER,
            PRIMARY KEY (device_id, timestamp, event_id, parameter)
        )
        """
    )
    con.close()

    with pytest.raises(RuntimeError, match="Unsupported events schema"):
        sr.DatabaseManager(temp_db_path)


def test_database_manager_accepts_current_events_schema(temp_db_path):
    con = duckdb.connect(temp_db_path)
    con.execute(
        """
        CREATE TABLE events (
            device_id VARCHAR,
            run_number INTEGER,
            timestamp TIMESTAMP,
            event_id INTEGER,
            parameter INTEGER,
            PRIMARY KEY (device_id, run_number, timestamp, event_id, parameter)
        )
        """
    )
    con.close()

    manager = sr.DatabaseManager(temp_db_path)

    assert manager.db_path == temp_db_path


def test_load_events_reads_sqlite_event_table(tmp_path):
    db_path = tmp_path / "events.db"
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            CREATE TABLE Event (
                Timestamp REAL,
                Tick REAL,
                EventTypeID INTEGER,
                Parameter INTEGER
            )
            """
        )
        con.execute(
            "INSERT INTO Event (Timestamp, Tick, EventTypeID, Parameter) VALUES (?, ?, ?, ?)",
            (1735732800, 5, 82, 17),
        )
        con.commit()
    finally:
        con.close()

    events = sr.load_events(str(db_path))

    assert list(events.columns) == ["timestamp", "event_id", "parameter"]
    assert len(events) == 1
    assert events.iloc[0]["event_id"] == 82
    assert events.iloc[0]["parameter"] == 17