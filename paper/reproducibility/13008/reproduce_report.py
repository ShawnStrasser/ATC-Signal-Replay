"""Regenerate the 13008 comparison report from saved controller outputs only."""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from importlib import import_module
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPOSITORY = HERE.parents[2]


def load_dependencies():
    try:
        import duckdb
        import pandas as pd
    except ImportError as exc:
        raise SystemExit("From the repository root run: py -m pip install -e . duckdb") from exc
    sys.path.insert(0, str(REPOSITORY / "firmware_validation"))
    try:
        return duckdb, pd, import_module("firmware_validate"), import_module("signal_replay")
    except ImportError as exc:
        raise SystemExit("From the repository root run: py -m pip install -e . duckdb") from exc


def write_database(duckdb, frame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    try:
        con.register("saved_events", frame)
        con.execute("""
            CREATE TABLE events AS
            SELECT '13008'::VARCHAR AS device_id, 1::INTEGER AS run_number,
                   CAST(timestamp AS TIMESTAMP) AS timestamp,
                   CAST(EventId AS INTEGER) AS event_id,
                   CAST(Parameter AS INTEGER) AS parameter
            FROM saved_events ORDER BY timestamp, EventId, Parameter
        """)
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=HERE / "reproduced-report.html")
    args = parser.parse_args()
    duckdb, pd, firmware_validate, sr = load_dependencies()
    expected = json.loads((HERE / "expected-results.json").read_text(encoding="utf-8"))
    events = pd.read_csv(HERE / "events.csv")
    required = {"DeviceId", "timestamp", "EventId", "Parameter"}
    if missing := sorted(required - set(events.columns)):
        raise SystemExit(f"events.csv is missing: {', '.join(missing)}")
    baseline = events.loc[events.DeviceId.eq("baseline")].copy()
    candidate = events.loc[events.DeviceId.eq("new")].copy()
    if baseline.empty or candidate.empty:
        raise SystemExit("events.csv must contain both baseline and new rows.")

    with tempfile.TemporaryDirectory(prefix="signal-replay-13008-") as temporary:
        workspace = Path(temporary)
        results_root = workspace / "results"
        write_database(duckdb, baseline, results_root / "2.15.1" / "collected.db")
        write_database(duckdb, candidate, results_root / "2.18.1" / "collected.db")
        comparison = expected["comparison"]
        settings = {"baseline_version": expected["baseline_version"], "results_dir": "results", "analysis_workers": 1,
                    "comparison": {"sequence_threshold": comparison["sequence_alert_threshold"],
                                   "timing_threshold": comparison["timing_alert_threshold"],
                                   "match_threshold": comparison["pass_threshold_percent"],
                                   "phase_call_similarity_threshold": comparison["phase_call_reliability_percent"],
                                   "analysis_start_time": comparison["analysis_start_time"], "analysis_end_time": "",
                                   "group_tolerance": comparison["group_tolerance_seconds"],
                                   "max_divergence_plots": 2, "divergence_window_minutes": 5.0}}
        suite = sr.FirmwareTestSuite(
            suite_name="Paper reproducibility example: 13008", firmware_version=expected["candidate_version"],
            baseline_version=expected["baseline_version"], batches=[], output_dir=str(results_root),
            scenarios=[sr.TestScenario(scenario_id="13008", database_name="not required", events_source="not required",
                                       test_type=sr.TestType.SIMILARITY, tod_align=True)],
            comparison_thresholds=sr.ComparisonThresholds(sequence_threshold=comparison["sequence_alert_threshold"],
                                                          timing_threshold=comparison["timing_alert_threshold"],
                                                          match_threshold=comparison["pass_threshold_percent"]),
            phase_call_similarity_threshold=comparison["phase_call_reliability_percent"],
            analysis_start_time=comparison["analysis_start_time"])
        results = firmware_validate.run_analysis(suite, settings, workspace, export_device_csvs=False)
        if len(results) != 1:
            raise SystemExit(f"Expected one comparison result, found {len(results)}.")
        result = results[0]
        actual = {"status": "PASS" if result.passed else "FAIL",
                  "sequence_match_percent": round(float(result.match_percentage), 1),
                  "timing_match_percent": round(float(result.timing_match_percentage), 1)}
        wanted = {key: expected[key] for key in actual}
        if actual != wanted:
            raise SystemExit("Reproduced metrics differ:\n" + json.dumps({"expected": wanted, "actual": actual}, indent=2))
        output = args.output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        sr.generate_report(results, suite, str(output))
    report = output.read_text(encoding="utf-8")
    if "13008" not in report or "Firmware Validation Report" not in report:
        raise SystemExit("Generated HTML is missing expected report content.")
    print(f"Verified {actual['status']} result and wrote self-contained report: {output}")


if __name__ == "__main__":
    main()
