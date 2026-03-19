from unittest.mock import patch
import sqlite3

import pandas as pd

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


def test_simulation_uses_preloaded_signal_events_without_central_distribution(temp_db_path):
    signal = sr.SignalConfig(device_id="S1", ip="127.0.0.1", udp_port=9701, http_port=None)
    preloaded_events = _detector_events()
    signal.events = preloaded_events

    class FakeDB:
        def __init__(self, _db_path):
            pass

        def get_max_run_number(self):
            return 0

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

    assert db_path == runner.run_dir / "batch_1.duckdb"
    assert captured["events"] is None
    # Events should be file paths (not loaded DataFrames) to avoid holding large data in memory
    assert all(isinstance(signal.events, str) for signal in captured["signals"])
    assert captured["signals"][0].events == str(events_1)
    assert captured["signals"][1].events == str(events_2)

    for handler in runner.logger.handlers:
        handler.close()
    runner.logger.handlers.clear()


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