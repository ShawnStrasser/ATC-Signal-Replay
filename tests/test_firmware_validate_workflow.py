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

    suite, scenarios, _batches, _file_map = firmware_validate.build_suite(settings, firmware_dir, catalog, {})

    assert suite.baseline_version == "2.15.1 (source logs)"
    assert scenarios[0].events_source == str(root_log)


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

    current_batch, completed = firmware_validate.detect_current_batch(suite, [batch])

    assert current_batch is not None
    assert current_batch.batch_id == "batch_1"
    assert completed == {"batch_1"}