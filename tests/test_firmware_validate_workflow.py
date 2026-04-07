import importlib.util
import json
from pathlib import Path

import duckdb
import pandas as pd

import signal_replay as sr


def _load_firmware_validate_module():
    module_path = Path(__file__).resolve().parents[1] / "firmware_validation" / "firmware_validate.py"
    spec = importlib.util.spec_from_file_location("firmware_validate_module", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_build_suite_uses_root_logs_for_replay_and_fallback_baseline_label(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    firmware_dir = tmp_path / "firmware_validation"
    (firmware_dir / "logs").mkdir(parents=True)
    (firmware_dir / "databases").mkdir()

    root_log = firmware_dir / "logs" / "S1.parquet"
    pd.DataFrame(
        [{"timestamp": pd.Timestamp("2026-01-01 09:00:00"), "event_id": 1, "parameter": 1}]
    ).to_parquet(root_log, index=False)
    (firmware_dir / "databases" / "S1.bin").write_bytes(b"db")

    settings = {
        "logs_dir": "logs",
        "databases_dir": "databases",
        "controller_targets": ["127.0.0.1:9701"],
        "firmware_version": "2.17.3",
        "baseline_version": "2.15.1",
        "results_dir": "results",
        "comparison": {},
    }
    catalog = [{"TSSU": "S1", "Type": "Similarity", "CycleLength": 0, "Offset": 0.0, "Notes": ""}]

    suite, _file_map = firmware_validate.build_suite(settings, firmware_dir, catalog, {})

    assert suite.baseline_version == "2.15.1 (source logs)"
    assert suite.scenarios[0].events_source == str(root_log)


def test_build_suite_zeroes_reported_settle_when_manual_start_replaces_it(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    firmware_dir = tmp_path / "firmware_validation"
    (firmware_dir / "logs").mkdir(parents=True)
    (firmware_dir / "databases").mkdir()

    root_log = firmware_dir / "logs" / "S1.parquet"
    pd.DataFrame(
        [{"timestamp": pd.Timestamp("2026-01-01 09:00:00"), "event_id": 1, "parameter": 1}]
    ).to_parquet(root_log, index=False)
    (firmware_dir / "databases" / "S1.bin").write_bytes(b"db")

    settings = {
        "logs_dir": "logs",
        "databases_dir": "databases",
        "controller_targets": ["127.0.0.1:9701"],
        "firmware_version": "2.17.3",
        "baseline_version": "2.15.1",
        "results_dir": "results",
        "comparison": {
            "settle_minutes": 10.0,
            "analysis_start_time": "09:10",
            "analysis_end_time": "07:00",
        },
    }
    catalog = [{"TSSU": "S1", "Type": "Similarity", "CycleLength": 0, "Offset": 0.0, "Notes": ""}]

    suite, _file_map = firmware_validate.build_suite(settings, firmware_dir, catalog, {})

    assert suite.scenarios[0].tod_align is True
    assert suite.analysis_start_time == "09:10"
    assert suite.analysis_end_time == "07:00"
    assert suite.analysis_settle_minutes == 0.0


def test_build_suite_keeps_settle_for_non_tod_scenarios(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    firmware_dir = tmp_path / "firmware_validation"
    (firmware_dir / "logs").mkdir(parents=True)
    (firmware_dir / "databases").mkdir()

    root_log = firmware_dir / "logs" / "S1.parquet"
    pd.DataFrame(
        [{"timestamp": pd.Timestamp("2026-01-01 09:00:00"), "event_id": 1, "parameter": 1}]
    ).to_parquet(root_log, index=False)
    (firmware_dir / "databases" / "S1.bin").write_bytes(b"db")

    settings = {
        "logs_dir": "logs",
        "databases_dir": "databases",
        "controller_targets": ["127.0.0.1:9701"],
        "firmware_version": "2.17.3",
        "baseline_version": "2.15.1",
        "results_dir": "results",
        "comparison": {
            "settle_minutes": 10.0,
            "analysis_start_time": "09:10",
        },
    }
    catalog = [{"TSSU": "S1", "Type": "Similarity", "CycleLength": 90, "Offset": 0.0, "Notes": ""}]

    suite, _file_map = firmware_validate.build_suite(settings, firmware_dir, catalog, {})

    assert suite.scenarios[0].tod_align is False
    assert suite.analysis_settle_minutes == 10.0


def test_build_suite_groups_conflict_scenarios_after_similarity_batches(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    firmware_dir = tmp_path / "firmware_validation"
    (firmware_dir / "logs").mkdir(parents=True)
    (firmware_dir / "databases").mkdir()

    for scenario_id in ("C1", "S1", "S2", "C2"):
        pd.DataFrame(
            [{"timestamp": pd.Timestamp("2026-01-01 09:00:00"), "event_id": 1, "parameter": 1}]
        ).to_parquet(firmware_dir / "logs" / f"{scenario_id}.parquet", index=False)
        (firmware_dir / "databases" / f"{scenario_id}.bin").write_bytes(b"db")

    settings = {
        "logs_dir": "logs",
        "databases_dir": "databases",
        "controller_targets": ["127.0.0.1:9701", "127.0.0.1:9702"],
        "firmware_version": "2.17.3",
        "baseline_version": "2.15.1",
        "results_dir": "results",
        "comparison": {},
    }
    catalog = [
        {"TSSU": "C1", "Type": "Conflict", "CycleLength": 0, "Offset": 0.0, "Notes": ""},
        {"TSSU": "S1", "Type": "Similarity", "CycleLength": 0, "Offset": 0.0, "Notes": ""},
        {"TSSU": "S2", "Type": "Similarity", "CycleLength": 0, "Offset": 0.0, "Notes": ""},
        {"TSSU": "C2", "Type": "Conflict", "CycleLength": 0, "Offset": 0.0, "Notes": ""},
    ]

    suite, _file_map = firmware_validate.build_suite(settings, firmware_dir, catalog, {})

    assert [scenario.scenario_id for scenario in suite.scenarios] == ["S1", "S2", "C1", "C2"]
    assert [batch.assignments for batch in suite.batches] == [
        {"S1": "127.0.0.1:9701:9701", "S2": "127.0.0.1:9702:9702"},
        {"C1": "127.0.0.1:9701:9701", "C2": "127.0.0.1:9702:9702"},
    ]
    assert [scenario.test_type for scenario in suite.scenarios[:2]] == [sr.TestType.SIMILARITY, sr.TestType.SIMILARITY]
    assert [scenario.test_type for scenario in suite.scenarios[2:]] == [sr.TestType.CONFLICT, sr.TestType.CONFLICT]


def test_resolve_baseline_log_prefers_versioned_baseline_logs(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    firmware_dir = tmp_path / "firmware_validation"
    (firmware_dir / "logs").mkdir(parents=True)
    versioned_dir = firmware_dir / "results" / "2.15.1" / "logs"
    versioned_dir.mkdir(parents=True)

    root_log = firmware_dir / "logs" / "S1.parquet"
    versioned_log = versioned_dir / "S1.parquet"
    pd.DataFrame([{"timestamp": pd.Timestamp("2026-01-01 09:00:00"), "event_id": 1, "parameter": 1}]).to_parquet(root_log, index=False)
    pd.DataFrame([{"timestamp": pd.Timestamp("2026-01-02 09:00:00"), "event_id": 2, "parameter": 2}]).to_parquet(versioned_log, index=False)

    settings = {
        "logs_dir": "logs",
        "results_dir": "results",
        "baseline_version": "2.15.1",
    }

    baseline_path, baseline_label = firmware_validate.resolve_baseline_log("S1", firmware_dir, settings)

    assert baseline_label == "2.15.1"
    assert baseline_path == versioned_log


def test_extract_collected_events_writes_versioned_logs(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    db_path = tmp_path / "shared.duckdb"
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
    con.close()

    suite = sr.FirmwareTestSuite(
        suite_name="suite",
        firmware_version="2.17.3",
        baseline_version="2.15.1",
        scenarios=[
            sr.TestScenario(
                scenario_id="S1",
                database_name="S1.bin",
                events_source="S1.parquet",
                test_type=sr.TestType.SIMILARITY,
            )
        ],
        batches=[],
        output_dir=str(tmp_path / "results"),
    )

    exported = firmware_validate._extract_collected_events(
        suite,
        {"scenario_db_map": {"S1": str(db_path)}},
        tmp_path / "results" / "2.17.3" / "logs",
    )

    assert exported["S1"].name == "S1.parquet"
    assert exported["S1"].exists()


def test_load_collected_events_from_duckdb_reads_single_scenario(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    db_path = tmp_path / "collected.duckdb"
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
    con.executemany(
        "INSERT INTO events VALUES (?, ?, ?, ?, ?)",
        [
            ("S1", 1, pd.Timestamp("2026-01-01 12:00:00"), 1, 1),
            ("S2", 1, pd.Timestamp("2026-01-01 12:01:00"), 9, 2),
            ("S1", 2, pd.Timestamp("2026-01-01 12:02:00"), 10, 3),
        ],
    )
    con.close()

    collected = firmware_validate._load_collected_events_from_duckdb(db_path, "S1")

    assert collected["device_id"].tolist() == ["S1", "S1"]
    assert collected["run_number"].tolist() == [1, 2]
    assert collected["event_id"].tolist() == [1, 10]


def test_export_device_csv_normalizes_columns_and_reads_duckdb(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    baseline_log = tmp_path / "baseline.parquet"
    pd.DataFrame(
        [
            {
                "TimeStamp": pd.Timestamp("2026-01-01 09:00:00"),
                "EventId": 13,
                "Parameter": 2,
            }
        ]
    ).to_parquet(baseline_log, index=False)

    db_path = tmp_path / "collected.duckdb"
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
        "INSERT INTO events VALUES ('S1', 1, '2026-01-01 09:01:00', 9, 2)"
    )
    con.close()

    suite = sr.FirmwareTestSuite(
        suite_name="suite",
        firmware_version="2.17.3",
        baseline_version="2.15.1",
        scenarios=[
            sr.TestScenario(
                scenario_id="S1",
                database_name="S1.bin",
                events_source=str(baseline_log),
                test_type=sr.TestType.SIMILARITY,
            )
        ],
        batches=[],
        output_dir=str(tmp_path / "results"),
    )

    out_dir = firmware_validate._export_device_csvs(
        suite,
        {"S1": str(db_path)},
        {"S1": baseline_log},
    )

    csv_path = out_dir / "S1.csv"
    combined = pd.read_csv(csv_path)

    assert list(combined.columns) == ["DeviceId", "timestamp", "EventId", "Parameter"]
    assert combined["DeviceId"].tolist() == ["baseline", "new"]
    assert combined["EventId"].tolist() == [13, 9]
    assert combined["Parameter"].tolist() == [2, 2]


def test_detect_current_batch_reruns_when_checkpoint_batch_members_changed(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    batch = sr.TestBatch(batch_id="batch_1", assignments={"S1": "127.0.0.1:9701"})
    suite = sr.FirmwareTestSuite(
        suite_name="suite",
        firmware_version="2.17.3",
        baseline_version="2.15.1",
        scenarios=[
            sr.TestScenario(
                scenario_id="S1",
                database_name="S1.bin",
                events_source="S1.parquet",
                test_type=sr.TestType.SIMILARITY,
            )
        ],
        batches=[batch],
        output_dir=str(tmp_path / "results"),
    )
    run_dir = tmp_path / "results" / "2.17.3"
    run_dir.mkdir(parents=True)
    with open(run_dir / "checkpoint.json", "w", encoding="utf-8") as handle:
        json.dump(
            {
                "completed_batches": ["batch_1"],
                "batch_members": {"batch_1": ["OLD_DEVICE"]},
            },
            handle,
        )

    current_batch, completed = firmware_validate.detect_current_batch(suite)

    assert current_batch is not None
    assert current_batch.batch_id == "batch_1"
    assert completed == {"batch_1"}


def test_run_analysis_computes_conflicts_from_saved_output_logs(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    firmware_dir = tmp_path / "firmware_validation"
    logs_dir = firmware_dir / "logs"
    results_dir = firmware_dir / "results"
    logs_dir.mkdir(parents=True)
    (results_dir / "2.17.3").mkdir(parents=True)

    baseline_log = logs_dir / "CF1.parquet"
    pd.DataFrame(
        [
            {"timestamp": pd.Timestamp("2026-01-01 09:00:00"), "event_id": 1, "parameter": 1},
            {"timestamp": pd.Timestamp("2026-01-01 09:00:05"), "event_id": 1, "parameter": 2},
            {"timestamp": pd.Timestamp("2026-01-01 09:00:10"), "event_id": 10, "parameter": 1},
            {"timestamp": pd.Timestamp("2026-01-01 09:00:11"), "event_id": 10, "parameter": 2},
        ]
    ).to_parquet(baseline_log, index=False)

    db_path = results_dir / "2.17.3" / "collected.duckdb"
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
    con.executemany(
        "INSERT INTO events VALUES (?, ?, ?, ?, ?)",
        [
            ("CF1", 1, pd.Timestamp("2026-01-02 09:00:00"), 1, 1),
            ("CF1", 1, pd.Timestamp("2026-01-02 09:00:10"), 10, 1),
            ("CF1", 1, pd.Timestamp("2026-01-02 09:00:20"), 1, 2),
            ("CF1", 1, pd.Timestamp("2026-01-02 09:00:30"), 10, 2),
            ("CF1", 2, pd.Timestamp("2026-01-02 10:00:00"), 1, 1),
            ("CF1", 2, pd.Timestamp("2026-01-02 10:00:05"), 1, 2),
            ("CF1", 2, pd.Timestamp("2026-01-02 10:00:10"), 10, 1),
            ("CF1", 2, pd.Timestamp("2026-01-02 10:00:11"), 10, 2),
        ],
    )
    con.close()

    with open(results_dir / "2.17.3" / "checkpoint.json", "w", encoding="utf-8") as handle:
        json.dump(
            {
                "completed_batches": ["batch_1"],
                "scenario_db_map": {"CF1": str(db_path)},
            },
            handle,
        )

    suite = sr.FirmwareTestSuite(
        suite_name="suite",
        firmware_version="2.17.3",
        baseline_version="2.15.1",
        scenarios=[
            sr.TestScenario(
                scenario_id="CF1",
                database_name="CF1.bin",
                events_source=str(baseline_log),
                test_type=sr.TestType.CONFLICT,
                replays=5,
                incompatible_pairs=[("Ph1", "Ph2")],
            )
        ],
        batches=[sr.TestBatch(batch_id="batch_1", assignments={"CF1": "127.0.0.1:9701:9701"})],
        output_dir=str(results_dir),
    )
    settings = {
        "logs_dir": "logs",
        "results_dir": "results",
        "firmware_version": "2.17.3",
        "baseline_version": "2.15.1",
        "comparison": {},
        "analysis_workers": 1,
    }

    results = firmware_validate.run_analysis(suite, settings, firmware_dir)

    assert len(results) == 1
    assert not (results_dir / "2.17.3" / "logs").exists()
    result = results[0]
    assert result.test_type == sr.TestType.CONFLICT
    assert result.match_percentage is None
    assert result.passed is False
    assert result.runs_completed == 2
    assert result.total_runs == 5
    assert result.conflicts_found == [
        {
            "run_number": 2,
            "timestamp": "2026-01-02 10:00:05",
            "conflict_details": "Ph1 & Ph2",
        }
    ]
    assert "Conflict observed on 2.17.3 in run(s): 2." in result.notes