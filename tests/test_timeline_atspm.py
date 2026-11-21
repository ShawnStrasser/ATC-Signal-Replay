"""The atspm timeline on real controller events, and the comparisons built on it.

1.0.1 passed ``has_data`` parameters that atspm 3 removed. atspm raised, and the
comparisons caught it and only noted "Timeline generation failed", so these
tests check that the timeline is built and used, not just that nothing raised.
"""

import warnings
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest

import signal_replay as sr
from signal_replay.comparison import generate_timeline

DEVICE = "X"


@pytest.fixture(scope="module")
def field_events():
    """40 minutes of one signal from atspm's sample data (phases, overlaps, peds, detectors)."""
    from atspm import sample_data

    raw = sample_data.data.df()
    raw = raw[raw["DeviceId"] == raw["DeviceId"].iloc[0]]
    start = raw["TimeStamp"].min()
    raw = raw[raw["TimeStamp"] < start + pd.Timedelta(minutes=40)]
    return (
        raw.drop(columns=["DeviceId"])
        .rename(columns={"TimeStamp": "timestamp", "EventId": "event_id", "Parameter": "parameter"})
        .reset_index(drop=True)
    )


def _longer_yellow(events, phases=(4, 8), from_minute=20, to_minute=30):
    """Red clearance (event 10) of ``phases`` one second later between two minutes of the log."""
    out = events.copy()
    start = out["timestamp"].min()
    late = (
        (out["event_id"] == 10)
        & out["parameter"].isin(phases)
        & (out["timestamp"] > start + timedelta(minutes=from_minute))
        & (out["timestamp"] < start + timedelta(minutes=to_minute))
    )
    out.loc[late, "timestamp"] += pd.Timedelta(seconds=1)
    return out.sort_values("timestamp", kind="stable").reset_index(drop=True)


def _suite():
    return sr.SoftwareTestSuite(
        suite_name="timeline", software_version="2.0", baseline_version="1.0",
        scenarios=[sr.TestScenario(DEVICE, f"{DEVICE}.bin", "unused.parquet",
                                   sr.TestType.SIMILARITY, tod_align=False)],
        batches=[], analysis_settle_minutes=0.0,
    )


def test_timeline_from_field_events(field_events):
    timeline = generate_timeline(field_events, device_id=DEVICE)

    assert {"StartTime", "EndTime", "Duration", "IsValid", "EventClass", "EventValue"} <= set(timeline.columns)
    assert {"Green", "Yellow", "Red", "Phase Call"} <= set(timeline["EventClass"])
    # clipped to the events' own time range
    assert timeline["StartTime"].min() >= field_events["timestamp"].min()
    assert timeline["EndTime"].max() <= field_events["timestamp"].max()
    # all three 15-minute bins count as having data, the partly covered last one included
    assert timeline["IsValid"].mean() > 0.95
    assert timeline.loc[timeline["IsValid"], "StartTime"].dt.floor("15min").nunique() == 3


def test_timeline_keeps_the_fall_back_hour():
    """atspm drops 1-2 am on the first Sunday of November unless told not to."""
    start = pd.Timestamp("2026-11-01 00:50:00")
    rows = []
    for cycle in range(100):
        t = start + timedelta(seconds=90 * cycle)
        rows += [(t, 1, 2), (t + timedelta(seconds=40), 8, 2), (t + timedelta(seconds=44), 10, 2),
                 (t + timedelta(seconds=46), 1, 4), (t + timedelta(seconds=80), 8, 4),
                 (t + timedelta(seconds=84), 10, 4)]
    events = pd.DataFrame(rows, columns=["timestamp", "event_id", "parameter"])

    timeline = generate_timeline(events, device_id=DEVICE)

    greens = timeline[timeline["EventClass"] == "Green"]
    in_hour = greens["StartTime"].between(pd.Timestamp("2026-11-01 01:00"), pd.Timestamp("2026-11-01 01:59:59"))
    assert in_hour.sum() >= 75  # 40 cycles of two greens in that hour


def test_compare_validation_builds_and_uses_the_timeline(field_events, tmp_path):
    [result] = sr.compare_validation(
        {DEVICE: field_events}, {DEVICE: _longer_yellow(field_events)}, _suite(),
        sr.ValidationSettings(settle_minutes=0.0), plots_dir=str(tmp_path),
    )

    assert result.error is None
    diagnostics = "\n".join(result.analysis_diagnostics)
    assert "Timeline generation failed" not in diagnostics
    assert "Timeline difference summary failed" not in diagnostics
    assert "Valid rows used for detailed summaries" in diagnostics
    assert result.timeline_difference_analysis_available is True
    # the one change made to the candidate is found from the timeline...
    flagged = {(item["event_class"], item["event_value"]) for item in result.clearance_irregularities
               if item["irregular_count_b"] > item["irregular_count_a"]}
    assert flagged and flagged <= {("Red", 4), ("Red", 8)}
    # ...and drawn
    assert result.plot_paths and all(Path(p).is_file() for p in result.plot_paths)


def test_compare_and_visualize_draws_the_timeline(field_events, tmp_path):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = sr.compare_and_visualize(
            field_events, _longer_yellow(field_events), device_id=DEVICE,
            output_dir=tmp_path, force_plot=True, print_summary=False,
        )

    # it warns "Failed to generate visualization" or "No timeline events generated" otherwise
    messages = [str(w.message) for w in caught if not issubclass(w.category, DeprecationWarning)]
    assert not messages, messages
    assert result.plot_path is not None and Path(result.plot_path).is_file()


def test_simulation_comparison_plot_draws_the_timeline(field_events, tmp_path):
    """ATCSimulation draws a Gantt chart from the timeline when a run comparison exceeds thresholds."""
    from signal_replay.collector import DatabaseManager

    db_path = str(tmp_path / "replay.duckdb")
    replay_input = field_events[field_events["event_id"].isin([81, 82])].assign(device_id=DEVICE)
    sim = sr.ATCSimulation(
        signals=[sr.SignalConfig(device_id=DEVICE, ip="192.0.2.1", incompatible_pairs=[])],
        events=replay_input, db_path=db_path, output_dir=tmp_path / "plots",
    )
    runs = {1: field_events, 2: _longer_yellow(field_events)}
    db = DatabaseManager(db_path)
    for run_number, events in runs.items():
        db.insert_events(
            events.rename(columns={"timestamp": "TimeStamp", "event_id": "EventTypeID", "parameter": "Parameter"}),
            DEVICE, run_number, events["timestamp"].min().to_pydatetime(),
        )
    result = sr.compare_event_sequences(runs[1], runs[2], device_id=DEVICE, print_summary=False)
    result.run_a, result.run_b = 1, 2

    sim._generate_comparison_plot(DEVICE, result)

    assert result.plot_path is not None and Path(result.plot_path).is_file()
