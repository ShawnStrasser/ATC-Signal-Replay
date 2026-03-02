import duckdb
from pathlib import Path
from unittest.mock import patch

import signal_replay as sr


def _build_suite(tmp_path: Path) -> sr.FirmwareTestSuite:
    scenario = sr.TestScenario(
        scenario_id="S1",
        database_name="S1.bin",
        events_source="S1.parquet",
        test_type=sr.TestType.SIMILARITY,
    )
    batch = sr.TestBatch(batch_id="batch_1", assignments={"S1": "127.0.0.1:1025"})
    return sr.FirmwareTestSuite(
        suite_name="suite",
        firmware_version="new",
        baseline_version="old",
        scenarios=[scenario],
        batches=[batch],
        output_dir=str(tmp_path),
    )


def test_failed_batch_is_not_marked_complete_and_data_is_cleared(tmp_path):
    suite = _build_suite(tmp_path)
    runner = sr.BatchRunner(suite, debug=False)

    db_path = runner.run_dir / "batch_1.duckdb"
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

    with patch.object(runner, "_run_similarity_batch", side_effect=RuntimeError("boom")):
        checkpoint = runner.run(db_loader_callback=lambda *_args: True)

    assert "batch_1" not in checkpoint.get("completed_batches", [])
    assert "S1" not in checkpoint.get("scenario_db_map", {})
    assert "batch_1" in checkpoint.get("batch_errors", {})

    con = duckdb.connect(str(db_path))
    rows = con.execute("SELECT COUNT(*) FROM events WHERE device_id = 'S1'").fetchone()[0]
    con.close()
    assert rows == 0

    for handler in runner.logger.handlers:
        handler.close()
    runner.logger.handlers.clear()
