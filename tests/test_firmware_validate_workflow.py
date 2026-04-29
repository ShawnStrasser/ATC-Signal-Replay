import importlib.util
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

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


def test_load_coord_split_schedules_groups_rows_by_device(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    coord_dir = tmp_path / "coord_patterns"
    coord_dir.mkdir()
    (coord_dir / "2B045_coord_splits.csv").write_text(
        "Phase,start_time,end_time\nP2,09:00:00,09:00:32\nP6,09:00:00,09:00:44\n",
        encoding="utf-8",
    )
    (coord_dir / "ignore_me.csv").write_text(
        "Phase,start_time,end_time\nP4,10:00:00,10:00:30\n",
        encoding="utf-8",
    )

    schedules = firmware_validate._load_coord_split_schedules(str(coord_dir))

    assert list(schedules) == ["2B045"]
    assert schedules["2B045"] == [
        {
            "phase": 2,
            "start_time": datetime.strptime("09:00:00", "%H:%M:%S").time(),
            "end_time": datetime.strptime("09:00:32", "%H:%M:%S").time(),
        },
        {
            "phase": 6,
            "start_time": datetime.strptime("09:00:00", "%H:%M:%S").time(),
            "end_time": datetime.strptime("09:00:44", "%H:%M:%S").time(),
        },
    ]


def test_load_coord_split_schedules_refreshes_csvs_from_json(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    coord_dir = tmp_path / "coord_patterns"
    coord_dir.mkdir()
    (tmp_path / "coord_split_schedule.py").write_text(
        "from pathlib import Path\n"
        "def convert_all_pattern_files(coord_patterns_dir: Path):\n"
        "    out = coord_patterns_dir / '2B049_coord_splits.csv'\n"
        "    out.write_text('Phase,start_time,end_time\\nP4,10:00:00,10:00:30\\n', encoding='utf-8')\n"
        "    return [(out, 1)]\n",
        encoding="utf-8",
    )
    (coord_dir / "2B049.json").write_text("{}", encoding="utf-8")

    schedules = firmware_validate._load_coord_split_schedules(str(coord_dir))

    assert list(schedules) == ["2B049"]
    assert schedules["2B049"][0]["phase"] == 4


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
    assert suite.batches == []
    assert [scenario.test_type for scenario in suite.scenarios[:2]] == [sr.TestType.SIMILARITY, sr.TestType.SIMILARITY]
    assert [scenario.test_type for scenario in suite.scenarios[2:]] == [sr.TestType.CONFLICT, sr.TestType.CONFLICT]


def test_resolve_baseline_source_prefers_baseline_collected_db(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    firmware_dir = tmp_path / "firmware_validation"
    (firmware_dir / "logs").mkdir(parents=True)
    baseline_results_dir = firmware_dir / "results" / "2.15.1"
    baseline_results_dir.mkdir(parents=True)

    root_log = firmware_dir / "logs" / "S1.parquet"
    pd.DataFrame([{"timestamp": pd.Timestamp("2026-01-01 09:00:00"), "event_id": 1, "parameter": 1}]).to_parquet(root_log, index=False)
    baseline_db = baseline_results_dir / "collected.db"
    con = duckdb.connect(str(baseline_db))
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
        "INSERT INTO events VALUES ('S1', 1, '2026-01-02 09:00:00', 2, 2)"
    )
    con.close()

    settings = {
        "logs_dir": "logs",
        "results_dir": "results",
        "baseline_version": "2.15.1",
    }

    baseline_source, baseline_label = firmware_validate.resolve_baseline_source("S1", firmware_dir, settings)

    assert baseline_label == "2.15.1"
    assert baseline_source == ("db", str(baseline_db))


def test_extract_collected_events_writes_versioned_logs(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    db_path = tmp_path / "shared.db"
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
        db_path,
        tmp_path / "results" / "2.17.3" / "logs",
    )

    assert exported["S1"].name == "S1.parquet"
    assert exported["S1"].exists()


def test_load_collected_events_from_duckdb_reads_single_scenario(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    db_path = tmp_path / "collected.db"
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


def test_load_baseline_events_reads_single_scenario_from_collected_db(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    db_path = tmp_path / "baseline.db"
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
            ("S1", 1, pd.Timestamp("2026-01-01 12:00:00"), 5, 1),
            ("S2", 1, pd.Timestamp("2026-01-01 12:01:00"), 9, 2),
            ("S1", 2, pd.Timestamp("2026-01-01 12:02:00"), 7, 3),
        ],
    )
    con.close()

    baseline = firmware_validate._load_baseline_events(("db", str(db_path)), "S1")

    assert baseline["device_id"].tolist() == ["S1", "S1"]
    assert baseline["run_number"].tolist() == [1, 2]
    assert baseline["event_id"].tolist() == [5, 7]


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

    db_path = tmp_path / "collected.db"
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
        db_path,
        {"S1": ("file", str(baseline_log))},
    )

    csv_path = out_dir / "S1.csv"
    combined = pd.read_csv(csv_path)

    assert list(combined.columns) == ["DeviceId", "timestamp", "EventId", "Parameter"]
    assert combined["DeviceId"].tolist() == ["baseline", "new"]
    assert combined["EventId"].tolist() == [13, 9]
    assert combined["Parameter"].tolist() == [2, 2]


def test_export_device_csv_skips_scenarios_without_collected_rows(tmp_path):
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

    db_path = tmp_path / "collected.db"
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
        db_path,
        {"S1": ("file", str(baseline_log))},
    )

    assert not (out_dir / "S1.csv").exists()


def test_select_pending_replay_batch_skips_existing_data_and_honors_replace_flag(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    suite = sr.FirmwareTestSuite(
        suite_name="suite",
        firmware_version="2.17.3",
        baseline_version="2.15.1",
        scenarios=[
            sr.TestScenario("S1", "S1.bin", "S1.parquet", sr.TestType.SIMILARITY),
            sr.TestScenario("S2", "S2.bin", "S2.parquet", sr.TestType.SIMILARITY),
            sr.TestScenario("S3", "S3.bin", "S3.parquet", sr.TestType.SIMILARITY),
        ],
        batches=[],
        output_dir=str(tmp_path / "results"),
    )
    settings = {"controller_targets": ["127.0.0.1:9701", "127.0.0.1:9702"]}
    catalog = [
        {"TSSU": "S1", "ReplaceOnRerun": "no"},
        {"TSSU": "S2", "ReplaceOnRerun": "YES"},
        {"TSSU": "S3", "ReplaceOnRerun": ""},
    ]

    batch, remaining, replace_selected, skipped_existing = firmware_validate._select_pending_replay_batch(
        suite,
        settings,
        catalog,
        existing_ids={"S1", "S2"},
    )

    assert batch is not None
    assert list(batch.assignments.keys()) == ["S2", "S3"]
    assert replace_selected == ["S2"]
    assert skipped_existing == ["S1"]
    assert remaining == []


def test_select_pending_replay_batch_returns_only_first_controller_group(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    suite = sr.FirmwareTestSuite(
        suite_name="suite",
        firmware_version="2.17.3",
        baseline_version="2.15.1",
        scenarios=[
            sr.TestScenario("S1", "S1.bin", "S1.parquet", sr.TestType.SIMILARITY),
            sr.TestScenario("S2", "S2.bin", "S2.parquet", sr.TestType.SIMILARITY),
            sr.TestScenario("S3", "S3.bin", "S3.parquet", sr.TestType.SIMILARITY),
        ],
        batches=[],
        output_dir=str(tmp_path / "results"),
    )
    settings = {"controller_targets": ["127.0.0.1:9701", "127.0.0.1:9702"]}
    catalog = [
        {"TSSU": "S1", "ReplaceOnRerun": "yes"},
        {"TSSU": "S2", "ReplaceOnRerun": ""},
        {"TSSU": "S3", "ReplaceOnRerun": ""},
    ]

    batch, remaining, replace_selected, skipped_existing = firmware_validate._select_pending_replay_batch(
        suite,
        settings,
        catalog,
        existing_ids={"S1"},
    )

    assert batch is not None
    assert list(batch.assignments.keys()) == ["S1", "S2"]
    assert [scenario.scenario_id for scenario in remaining] == ["S3"]
    assert replace_selected == ["S1"]
    assert skipped_existing == []


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

    db_path = results_dir / "2.17.3" / "collected.db"
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

    with patch.object(firmware_validate, "ProcessPoolExecutor", ThreadPoolExecutor):
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


def test_run_analysis_marks_similarity_scenarios_with_missing_collected_rows_as_errors(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    firmware_dir = tmp_path / "firmware_validation"
    logs_dir = firmware_dir / "logs"
    results_dir = firmware_dir / "results"
    logs_dir.mkdir(parents=True)
    (results_dir / "2.17.3").mkdir(parents=True)

    baseline_log = logs_dir / "S1.parquet"
    pd.DataFrame(
        [
            {"timestamp": pd.Timestamp("2026-01-01 09:00:00"), "event_id": 1, "parameter": 1},
            {"timestamp": pd.Timestamp("2026-01-01 09:00:05"), "event_id": 7, "parameter": 1},
        ]
    ).to_parquet(baseline_log, index=False)

    db_path = results_dir / "2.17.3" / "collected.db"
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
    con.close()

    with open(results_dir / "2.17.3" / "checkpoint.json", "w", encoding="utf-8") as handle:
        json.dump(
            {
                "completed_batches": ["batch_1"],
                "scenario_db_map": {"S1": str(db_path)},
            },
            handle,
        )

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
        batches=[sr.TestBatch(batch_id="batch_1", assignments={"S1": "127.0.0.1:9701:9701"})],
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

    with patch.object(firmware_validate, "ProcessPoolExecutor", ThreadPoolExecutor):
        results = firmware_validate.run_analysis(suite, settings, firmware_dir)

    assert len(results) == 1
    result = results[0]
    assert result.passed is False
    assert result.error is not None
    assert "No collected events found for S1" in result.error
    assert not (results_dir / "2.17.3" / "device_events" / "S1.csv").exists()


def test_run_analysis_can_skip_device_csv_export_in_report_only_mode(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    firmware_dir = tmp_path / "firmware_validation"
    logs_dir = firmware_dir / "logs"
    results_dir = firmware_dir / "results"
    logs_dir.mkdir(parents=True)
    (results_dir / "2.17.3").mkdir(parents=True)

    baseline_log = logs_dir / "S1.parquet"
    pd.DataFrame(
        [
            {"timestamp": pd.Timestamp("2026-01-01 09:00:00"), "event_id": 1, "parameter": 1},
            {"timestamp": pd.Timestamp("2026-01-01 09:00:05"), "event_id": 7, "parameter": 1},
        ]
    ).to_parquet(baseline_log, index=False)

    db_path = results_dir / "2.17.3" / "collected.db"
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
        batches=[sr.TestBatch(batch_id="batch_1", assignments={"S1": "127.0.0.1:9701:9701"})],
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

    with (
        patch.object(firmware_validate, "_export_device_csvs", side_effect=AssertionError("device CSV export should be skipped")),
        patch.object(firmware_validate, "ProcessPoolExecutor", ThreadPoolExecutor),
    ):
        results = firmware_validate.run_analysis(
            suite,
            settings,
            firmware_dir,
            export_device_csvs=False,
        )

    assert len(results) == 1
    assert results[0].error is not None
    assert not (results_dir / "2.17.3" / "device_events").exists()


def test_compare_one_scenario_marks_empty_chunk_similarity_as_thrown_out(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    baseline = pd.DataFrame(
        [{"timestamp": pd.Timestamp("2026-01-01 09:00:00"), "event_id": 1, "parameter": 1}]
    )
    collected = pd.DataFrame(
        [{"timestamp": pd.Timestamp("2026-01-01 09:00:01"), "event_id": 1, "parameter": 1}]
    )
    empty_timeline = pd.DataFrame(columns=["EventClass", "StartTime", "EndTime"])

    class FakeComparisonResult:
        def __init__(self):
            self.chunk_scores = []
            self.phase_call_chunk_scores = []
            self.included_chunk_count = 0
            self.excluded_chunk_count = 0
            self.divergence_windows = []
            self.match_percentage = 0.0
            self.temporal_shift_seconds = 0.0
            self.thrown_out = False
            self.thrown_out_reason = ""

        def format_summary(self):
            return "Match: thrown out\nInsufficient scored chunks remained after settling/filtering for a reliable comparison."

    with (
        patch.object(firmware_validate, "_load_baseline_events", return_value=baseline),
        patch.object(firmware_validate, "_load_collected_events_from_duckdb", return_value=collected),
        patch.object(
            firmware_validate,
            "_prepare_analysis_inputs",
            return_value=(baseline, collected, datetime(2026, 1, 1, 9, 0, 0), datetime(2026, 1, 1, 9, 0, 1)),
        ),
        patch("signal_replay.compare_runs", return_value=FakeComparisonResult()),
        patch("signal_replay.generate_timeline", side_effect=[empty_timeline.copy(), empty_timeline.copy()]),
        patch("signal_replay.render_sparkline_svg", return_value=""),
    ):
        out = firmware_validate._compare_one_scenario(
            (
                "12035",
                ("parquet", str(tmp_path / "12035.parquet")),
                "2.15.1",
                "SIMILARITY",
                str(tmp_path / "collected.db"),
                "2.17.3",
                str(tmp_path / "plots"),
                0.0,
                0.0,
                5,
                15,
                False,
                "",
                False,
                None,
                None,
                90.0,
            )
        )

    assert out["thrown_out"] is True
    assert out["thrown_out_reason"] == "Insufficient scored chunks remained after settling/filtering for a reliable comparison."
    assert "Match: thrown out" in out["summary"]


def test_main_report_only_fast_skips_device_csv_export(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    firmware_dir = tmp_path / "firmware_validation"
    firmware_dir.mkdir(parents=True)
    settings_path = firmware_dir / "settings.json"
    settings_path.write_text(
        json.dumps(
            {
                "catalog_file": "catalog.csv",
                "conflict_pairs_file": "conflict_pairs.json",
                "controller_targets": ["127.0.0.1:9701"],
                "firmware_version": "2.17.3",
                "baseline_version": "2.15.1",
                "comparison": {},
            }
        ),
        encoding="utf-8",
    )
    (firmware_dir / "catalog.csv").write_text("TSSU,Type,CycleLength,Offset,Notes\n", encoding="utf-8")
    (firmware_dir / "conflict_pairs.json").write_text("{}", encoding="utf-8")

    suite = sr.FirmwareTestSuite(
        suite_name="suite",
        firmware_version="2.17.3",
        baseline_version="2.15.1",
        scenarios=[],
        batches=[],
        output_dir=str(tmp_path / "results"),
    )
    captured = {}

    def fake_run_analysis(_suite, _settings, _firmware_dir, *, export_device_csvs=True):
        captured["export_device_csvs"] = export_device_csvs
        return []

    with (
        patch.object(firmware_validate, "__file__", str(firmware_dir / "firmware_validate.py")),
        patch.object(firmware_validate, "load_settings", return_value=json.loads(settings_path.read_text(encoding="utf-8"))),
        patch.object(firmware_validate, "read_catalog", return_value=[]),
        patch.object(firmware_validate, "build_suite", return_value=(suite, {})),
        patch.object(firmware_validate, "run_analysis", side_effect=fake_run_analysis),
        patch.object(firmware_validate, "build_report", return_value=tmp_path / "report.html"),
        patch.object(firmware_validate, "archive_and_extract"),
        patch("sys.argv", ["firmware_validate.py", "--report-only-fast"]),
    ):
        firmware_validate.main()

    assert captured["export_device_csvs"] is False


def _build_issue_timeline(rows):
    if not rows:
        return pd.DataFrame(columns=["EventClass", "EventValue", "StartTime", "EndTime", "Duration"])
    timeline = pd.DataFrame(rows)
    timeline["StartTime"] = pd.to_datetime(timeline["StartTime"])
    timeline["EndTime"] = pd.to_datetime(timeline["EndTime"])
    timeline["Duration"] = (timeline["EndTime"] - timeline["StartTime"]).dt.total_seconds()
    return timeline


def test_select_operational_issue_anchor_prefers_missing_service_cluster_over_duration_outlier():
    firmware_validate = _load_firmware_validate_module()

    timeline_a = _build_issue_timeline(
        [
            {
                "EventClass": "Ped Service",
                "EventValue": 8,
                "StartTime": "2026-02-16 10:33:00",
                "EndTime": "2026-02-16 10:33:30",
            },
            {
                "EventClass": "Ped Service",
                "EventValue": 8,
                "StartTime": "2026-02-16 10:34:00",
                "EndTime": "2026-02-16 10:34:30",
            },
            {
                "EventClass": "Ped Service",
                "EventValue": 8,
                "StartTime": "2026-02-16 12:04:58",
                "EndTime": "2026-02-16 12:05:28",
            },
        ]
    )
    timeline_b = _build_issue_timeline(
        [
            {
                "EventClass": "Ped Service",
                "EventValue": 8,
                "StartTime": "2026-02-16 12:04:58",
                "EndTime": "2026-02-16 12:05:15.400000",
            },
        ]
    )

    issue_window = firmware_validate._select_operational_issue_anchor(
        timeline_a,
        timeline_b,
        {
            "event_class": "Ped Service",
            "event_value": 8,
            "label": "Ped 8",
            "state": "Service",
        },
    )

    assert issue_window is not None
    assert issue_window["start"] == pd.Timestamp("2026-02-16 10:33:00")
    assert issue_window["end"] == pd.Timestamp("2026-02-16 10:34:30")
    assert issue_window["mismatch_seconds"] == 60.0
    assert issue_window["count_a"] == 2
    assert issue_window["count_b"] == 0


def test_generate_special_issue_plots_includes_green_phase_differences(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    timeline_a = _build_issue_timeline(
        [
            {
                "EventClass": "Green",
                "EventValue": 4,
                "StartTime": "2026-01-01 11:00:00",
                "EndTime": "2026-01-01 11:00:20",
            },
        ]
    )
    timeline_b = _build_issue_timeline(
        [
            {
                "EventClass": "Green",
                "EventValue": 4,
                "StartTime": "2026-01-01 11:00:00",
                "EndTime": "2026-01-01 11:00:40",
            },
        ]
    )

    captured = {}

    def fake_create_comparison_gantt_matplotlib(**kwargs):
        captured["divergence_start"] = kwargs["divergence_start"]
        captured["divergence_end"] = kwargs["divergence_end"]
        captured["programmed_split_timeline"] = kwargs["programmed_split_timeline"]
        Path(kwargs["output_path"]).write_bytes(b"png")
        return object()

    with (
        patch.object(
            firmware_validate,
            "_load_coord_split_schedules",
            return_value={
                "S1": [
                    {
                        "phase": 4,
                        "start_time": datetime.strptime("11:00:05", "%H:%M:%S").time(),
                        "end_time": datetime.strptime("11:00:35", "%H:%M:%S").time(),
                    }
                ]
            },
        ),
        patch.object(firmware_validate.sr, "create_comparison_gantt_matplotlib", side_effect=fake_create_comparison_gantt_matplotlib),
        patch.object(firmware_validate.plt, "close"),
    ):
        plot_paths, plot_captions = firmware_validate._generate_special_issue_plots(
            scenario_id="S1",
            timeline_a=timeline_a,
            timeline_b=timeline_b,
            aligned_timeline_a=timeline_a,
            aligned_timeline_b=timeline_b,
            phase_differences=[
                {
                    "label": "Ph 4",
                    "state": "Green",
                    "event_class": "Green",
                    "event_value": 4,
                    "count_a": 1,
                    "count_b": 1,
                    "count_delta": 0,
                    "duration_a": 20.0,
                    "duration_b": 40.0,
                    "duration_delta": 20.0,
                    "total_duration_a": 20.0,
                    "total_duration_b": 40.0,
                    "total_duration_delta": 20.0,
                }
            ],
            clearance_irregularities=[],
            operational_diffs=[],
            plots_dir=str(tmp_path / "plots"),
            label_a="2.15.1",
            label_b="2.17.3",
            window_minutes=10.0,
            time_offset_b=0.0,
            align_by_time_delta=False,
            tod_align=True,
        )

    assert len(plot_paths) == 1
    assert len(plot_captions) == 1
    assert plot_captions[0].startswith("Ph 4 Green: largest local mismatch window")
    assert "2.15.1 events 0, active 0.00s" in plot_captions[0]
    assert "2.17.3 events 1, active 20.00s" in plot_captions[0]
    assert captured["divergence_start"] == pd.Timestamp("2026-01-01 11:00:20")
    assert captured["divergence_end"] == pd.Timestamp("2026-01-01 11:00:40")
    assert captured["programmed_split_timeline"] is not None
    assert captured["programmed_split_timeline"]["Phase"].tolist() == [4]


def test_generate_special_issue_plots_passes_programmed_splits_for_tod_transition_with_base_device_fallback(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    timeline_a = _build_issue_timeline(
        [
            {
                "EventClass": "Green",
                "EventValue": 2,
                "StartTime": "2026-01-01 09:00:00",
                "EndTime": "2026-01-01 09:00:35",
            },
            {
                "EventClass": "Transition Longway",
                "EventValue": 1,
                "StartTime": "2026-01-01 09:00:35",
                "EndTime": "2026-01-01 09:00:50",
            },
        ]
    )
    timeline_b = _build_issue_timeline(
        [
            {
                "EventClass": "Green",
                "EventValue": 2,
                "StartTime": "2026-01-01 09:00:00",
                "EndTime": "2026-01-01 09:00:25",
            },
        ]
    )

    captured = {}

    def fake_create_comparison_gantt_matplotlib(**kwargs):
        captured["programmed_split_timeline"] = kwargs["programmed_split_timeline"]
        Path(kwargs["output_path"]).write_bytes(b"png")
        return object()

    with (
        patch.object(
            firmware_validate,
            "_load_coord_split_schedules",
            return_value={
                "2B045": [
                    {
                        "phase": 2,
                        "start_time": datetime.strptime("09:00:05", "%H:%M:%S").time(),
                        "end_time": datetime.strptime("09:00:40", "%H:%M:%S").time(),
                    }
                ]
            },
        ),
        patch.object(firmware_validate.sr, "create_comparison_gantt_matplotlib", side_effect=fake_create_comparison_gantt_matplotlib),
        patch.object(firmware_validate.plt, "close"),
    ):
        plot_paths, _plot_captions = firmware_validate._generate_special_issue_plots(
            scenario_id="2B045_c",
            timeline_a=timeline_a,
            timeline_b=timeline_b,
            aligned_timeline_a=timeline_a,
            aligned_timeline_b=timeline_b,
            phase_differences=[],
            clearance_irregularities=[],
            operational_diffs=[
                {
                    "label": "Transition",
                    "state": "Longway",
                    "event_class": "Transition Longway",
                    "event_value": 1,
                    "count_delta": -1,
                    "duration_delta": -15.0,
                    "total_duration_delta": -15.0,
                }
            ],
            plots_dir=str(tmp_path / "plots"),
            label_a="2.15.1",
            label_b="2.17.3",
            window_minutes=10.0,
            time_offset_b=0.0,
            align_by_time_delta=False,
            tod_align=True,
        )

    assert len(plot_paths) == 1
    assert captured["programmed_split_timeline"] is not None
    assert captured["programmed_split_timeline"]["Phase"].tolist() == [2]
    assert captured["programmed_split_timeline"]["StartTime"].iloc[0] == pd.Timestamp("2026-01-01 09:00:05")


def test_generate_special_issue_plots_skips_programmed_splits_for_overlap_green(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    timeline_a = _build_issue_timeline(
        [
            {
                "EventClass": "Overlap Green",
                "EventValue": 4,
                "StartTime": "2026-01-01 11:00:00",
                "EndTime": "2026-01-01 11:00:20",
            },
        ]
    )
    timeline_b = _build_issue_timeline(
        [
            {
                "EventClass": "Overlap Green",
                "EventValue": 4,
                "StartTime": "2026-01-01 11:00:00",
                "EndTime": "2026-01-01 11:00:40",
            },
        ]
    )

    captured = {}

    def fake_create_comparison_gantt_matplotlib(**kwargs):
        captured["programmed_split_timeline"] = kwargs["programmed_split_timeline"]
        Path(kwargs["output_path"]).write_bytes(b"png")
        return object()

    with (
        patch.object(
            firmware_validate,
            "_load_coord_split_schedules",
            return_value={
                "S1": [
                    {
                        "phase": 4,
                        "start_time": datetime.strptime("11:00:05", "%H:%M:%S").time(),
                        "end_time": datetime.strptime("11:00:35", "%H:%M:%S").time(),
                    }
                ]
            },
        ),
        patch.object(firmware_validate.sr, "create_comparison_gantt_matplotlib", side_effect=fake_create_comparison_gantt_matplotlib),
        patch.object(firmware_validate.plt, "close"),
    ):
        plot_paths, _plot_captions = firmware_validate._generate_special_issue_plots(
            scenario_id="S1",
            timeline_a=timeline_a,
            timeline_b=timeline_b,
            aligned_timeline_a=timeline_a,
            aligned_timeline_b=timeline_b,
            phase_differences=[
                {
                    "label": "Ovlp 4",
                    "state": "Green",
                    "event_class": "Overlap Green",
                    "event_value": 4,
                    "count_a": 1,
                    "count_b": 1,
                    "count_delta": 0,
                    "duration_a": 20.0,
                    "duration_b": 40.0,
                    "duration_delta": 20.0,
                    "total_duration_a": 20.0,
                    "total_duration_b": 40.0,
                    "total_duration_delta": 20.0,
                }
            ],
            clearance_irregularities=[],
            operational_diffs=[],
            plots_dir=str(tmp_path / "plots"),
            label_a="2.15.1",
            label_b="2.17.3",
            window_minutes=10.0,
            time_offset_b=0.0,
            align_by_time_delta=False,
            tod_align=True,
        )

    assert len(plot_paths) == 1
    assert captured["programmed_split_timeline"] is not None
    assert captured["programmed_split_timeline"]["Phase"].tolist() == [4]


def test_generate_special_issue_plots_groups_non_clearance_types_per_new_rules(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    timeline_a = _build_issue_timeline(
        [
            {
                "EventClass": "Ped Service",
                "EventValue": 4,
                "StartTime": "2026-01-01 09:00:00",
                "EndTime": "2026-01-01 09:00:30",
            },
            {
                "EventClass": "Ped Service",
                "EventValue": 8,
                "StartTime": "2026-01-01 09:20:00",
                "EndTime": "2026-01-01 09:20:30",
            },
            {
                "EventClass": "Ped Service",
                "EventValue": 8,
                "StartTime": "2026-01-01 09:21:00",
                "EndTime": "2026-01-01 09:21:30",
            },
            {
                "EventClass": "Preempt",
                "EventValue": 5,
                "StartTime": "2026-01-01 09:40:00",
                "EndTime": "2026-01-01 09:40:45",
            },
            {
                "EventClass": "Preempt",
                "EventValue": 6,
                "StartTime": "2026-01-01 10:00:00",
                "EndTime": "2026-01-01 10:00:50",
            },
            {
                "EventClass": "Transition Longway",
                "EventValue": 1,
                "StartTime": "2026-01-01 10:20:00",
                "EndTime": "2026-01-01 10:20:20",
            },
            {
                "EventClass": "Transition Longway",
                "EventValue": 2,
                "StartTime": "2026-01-01 10:40:00",
                "EndTime": "2026-01-01 10:41:10",
            },
            {
                "EventClass": "Transition Shortway",
                "EventValue": 3,
                "StartTime": "2026-01-01 11:00:00",
                "EndTime": "2026-01-01 11:00:25",
            },
        ]
    )
    timeline_b = _build_issue_timeline(
        [
            {
                "EventClass": "Green",
                "EventValue": 1,
                "StartTime": "2026-01-01 08:00:00",
                "EndTime": "2026-01-01 08:00:05",
            },
        ]
    )

    captured_titles = []

    def fake_create_comparison_gantt_matplotlib(**kwargs):
        captured_titles.append(kwargs["title"])
        Path(kwargs["output_path"]).write_bytes(b"png")
        return object()

    with (
        patch.object(firmware_validate.sr, "create_comparison_gantt_matplotlib", side_effect=fake_create_comparison_gantt_matplotlib),
        patch.object(firmware_validate.plt, "close"),
    ):
        plot_paths, _plot_captions = firmware_validate._generate_special_issue_plots(
            scenario_id="S1",
            timeline_a=timeline_a,
            timeline_b=timeline_b,
            aligned_timeline_a=timeline_a,
            aligned_timeline_b=timeline_b,
            phase_differences=[],
            clearance_irregularities=[],
            operational_diffs=[
                {
                    "label": "Ped 4",
                    "state": "Service",
                    "event_class": "Ped Service",
                    "event_value": 4,
                    "count_delta": -1,
                    "duration_delta": -30.0,
                    "total_duration_delta": -30.0,
                },
                {
                    "label": "Ped 8",
                    "state": "Service",
                    "event_class": "Ped Service",
                    "event_value": 8,
                    "count_delta": -2,
                    "duration_delta": -30.0,
                    "total_duration_delta": -60.0,
                },
                {
                    "label": "Preempt 5",
                    "state": "Active",
                    "event_class": "Preempt",
                    "event_value": 5,
                    "count_delta": -1,
                    "duration_delta": -45.0,
                    "total_duration_delta": -45.0,
                },
                {
                    "label": "Preempt 6",
                    "state": "Active",
                    "event_class": "Preempt",
                    "event_value": 6,
                    "count_delta": -1,
                    "duration_delta": -50.0,
                    "total_duration_delta": -50.0,
                },
                {
                    "label": "Transition",
                    "state": "Longway",
                    "event_class": "Transition Longway",
                    "event_value": 1,
                    "count_delta": -1,
                    "duration_delta": -20.0,
                    "total_duration_delta": -20.0,
                },
                {
                    "label": "Transition",
                    "state": "Longway",
                    "event_class": "Transition Longway",
                    "event_value": 2,
                    "count_delta": -1,
                    "duration_delta": -70.0,
                    "total_duration_delta": -70.0,
                },
                {
                    "label": "Transition",
                    "state": "Shortway",
                    "event_class": "Transition Shortway",
                    "event_value": 3,
                    "count_delta": -1,
                    "duration_delta": -25.0,
                    "total_duration_delta": -25.0,
                },
            ],
            plots_dir=str(tmp_path / "plots"),
            label_a="2.15.1",
            label_b="2.17.3",
            window_minutes=10.0,
            time_offset_b=0.0,
            align_by_time_delta=False,
        )

    assert len(plot_paths) == 5
    assert captured_titles == [
        "S1 Issue Focus - Transition Longway",
        "S1 Issue Focus - Ped 8 Service",
        "S1 Issue Focus - Preempt 6 Active",
        "S1 Issue Focus - Preempt 5 Active",
        "S1 Issue Focus - Transition Shortway",
    ]


def test_select_operational_issue_anchor_handles_transition_rows_with_missing_event_value():
    firmware_validate = _load_firmware_validate_module()

    timeline_a = _build_issue_timeline(
        [
            {
                "EventClass": "Transition Shortway",
                "EventValue": None,
                "StartTime": "2026-01-01 09:00:00",
                "EndTime": "2026-01-01 09:00:45",
            },
        ]
    )
    timeline_b = _build_issue_timeline(
        [
            {
                "EventClass": "Transition Shortway",
                "EventValue": None,
                "StartTime": "2026-01-01 09:10:00",
                "EndTime": "2026-01-01 09:10:00",
            },
        ]
    )

    issue_window = firmware_validate._select_operational_issue_anchor(
        timeline_a,
        timeline_b,
        {
            "event_class": "Transition Shortway",
            "event_value": 0,
            "label": "Transition",
            "state": "Shortway",
        },
    )

    assert issue_window is not None
    assert issue_window["start"] == pd.Timestamp("2026-01-01 09:00:00")
    assert issue_window["end"] == pd.Timestamp("2026-01-01 09:00:45")


def test_generate_special_issue_plots_uses_next_best_window_when_top_window_conflicts(tmp_path):
    firmware_validate = _load_firmware_validate_module()

    timeline_a = _build_issue_timeline(
        [
            {
                "EventClass": "Green",
                "EventValue": 6,
                "StartTime": "2026-01-01 09:00:00",
                "EndTime": "2026-01-01 09:10:00",
            },
            {
                "EventClass": "Ped Service",
                "EventValue": 8,
                "StartTime": "2026-01-01 09:05:00",
                "EndTime": "2026-01-01 09:06:00",
            },
            {
                "EventClass": "Ped Service",
                "EventValue": 8,
                "StartTime": "2026-01-01 09:20:00",
                "EndTime": "2026-01-01 09:20:30",
            },
        ]
    )
    timeline_b = _build_issue_timeline(
        [
            {
                "EventClass": "Green",
                "EventValue": 1,
                "StartTime": "2026-01-01 08:00:00",
                "EndTime": "2026-01-01 08:00:05",
            },
        ]
    )

    captured = []

    def fake_create_comparison_gantt_matplotlib(**kwargs):
        captured.append((kwargs["title"], kwargs["divergence_start"], kwargs["divergence_end"]))
        Path(kwargs["output_path"]).write_bytes(b"png")
        return object()

    with (
        patch.object(firmware_validate.sr, "create_comparison_gantt_matplotlib", side_effect=fake_create_comparison_gantt_matplotlib),
        patch.object(firmware_validate.plt, "close"),
    ):
        plot_paths, plot_captions = firmware_validate._generate_special_issue_plots(
            scenario_id="S1",
            timeline_a=timeline_a,
            timeline_b=timeline_b,
            aligned_timeline_a=timeline_a,
            aligned_timeline_b=timeline_b,
            phase_differences=[
                {
                    "label": "Ph 6",
                    "state": "Green",
                    "event_class": "Green",
                    "event_value": 6,
                    "count_a": 1,
                    "count_b": 0,
                    "count_delta": -1,
                    "duration_a": 600.0,
                    "duration_b": 0.0,
                    "duration_delta": -600.0,
                    "total_duration_a": 600.0,
                    "total_duration_b": 0.0,
                    "total_duration_delta": -600.0,
                }
            ],
            clearance_irregularities=[],
            operational_diffs=[
                {
                    "label": "Ped 8",
                    "state": "Service",
                    "event_class": "Ped Service",
                    "event_value": 8,
                    "count_a": 2,
                    "count_b": 0,
                    "count_delta": -2,
                    "duration_a": 45.0,
                    "duration_b": 0.0,
                    "duration_delta": -45.0,
                    "total_duration_a": 90.0,
                    "total_duration_b": 0.0,
                    "total_duration_delta": -90.0,
                }
            ],
            plots_dir=str(tmp_path / "plots"),
            label_a="2.15.1",
            label_b="2.17.3",
            window_minutes=10.0,
            time_offset_b=0.0,
            align_by_time_delta=False,
        )

    assert len(plot_paths) == 2
    assert [title for title, _start, _end in captured] == [
        "S1 Issue Focus - Ph 6 Green",
        "S1 Issue Focus - Ped 8 Service",
    ]
    ped_chart = next(item for item in captured if item[0] == "S1 Issue Focus - Ped 8 Service")
    assert ped_chart[1] == pd.Timestamp("2026-01-01 09:20:00")
    assert ped_chart[2] == pd.Timestamp("2026-01-01 09:20:30")
    assert any(caption.startswith("Ped 8 Service: largest local mismatch window") for caption in plot_captions)


def test_select_clearance_issue_spec_remains_median_based():
    firmware_validate = _load_firmware_validate_module()

    timeline_a = _build_issue_timeline(
        [
            {
                "EventClass": "Yellow",
                "EventValue": 4,
                "StartTime": "2026-01-01 09:00:00",
                "EndTime": "2026-01-01 09:00:05",
            },
            {
                "EventClass": "Yellow",
                "EventValue": 4,
                "StartTime": "2026-01-01 09:10:00",
                "EndTime": "2026-01-01 09:10:05",
            },
            {
                "EventClass": "Yellow",
                "EventValue": 4,
                "StartTime": "2026-01-01 09:20:00",
                "EndTime": "2026-01-01 09:20:09",
            },
        ]
    )
    timeline_b = _build_issue_timeline(
        [
            {
                "EventClass": "Yellow",
                "EventValue": 4,
                "StartTime": "2026-01-01 09:00:00",
                "EndTime": "2026-01-01 09:00:05",
            },
            {
                "EventClass": "Yellow",
                "EventValue": 4,
                "StartTime": "2026-01-01 09:10:00",
                "EndTime": "2026-01-01 09:10:05",
            },
            {
                "EventClass": "Yellow",
                "EventValue": 4,
                "StartTime": "2026-01-01 09:20:00",
                "EndTime": "2026-01-01 09:20:05",
            },
        ]
    )

    issue_spec = firmware_validate._select_clearance_issue_spec(
        scenario_id="S1",
        row={
            "label": "Ph 4",
            "state": "Yellow",
            "event_class": "Yellow",
            "event_value": 4,
        },
        timeline_a=timeline_a,
        timeline_b=timeline_b,
        label_a="2.15.1",
        label_b="2.17.3",
    )

    assert issue_spec is not None
    assert issue_spec["start"] == pd.Timestamp("2026-01-01 09:20:00")
    assert issue_spec["end"] == pd.Timestamp("2026-01-01 09:20:09")
    assert "2.15.1: 9.00s vs median 5.00s" in issue_spec["caption"]
