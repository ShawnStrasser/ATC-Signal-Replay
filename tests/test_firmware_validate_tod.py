from datetime import datetime

import pandas as pd

import firmware_validation.firmware_validate as fv
from signal_replay.comparison import ChunkScore, _filter_chunk_scores_after_settle, render_sparkline_svg


def test_tod_export_shift_preserves_time_of_day():
    original = pd.DataFrame(
        {
            "timestamp": pd.to_datetime([
                "2026-02-25 09:00:06.300",
                "2026-02-25 09:00:14.600",
            ]),
            "event_id": [82, 7],
            "parameter": [3, 2],
        }
    )
    collected = pd.DataFrame(
        {
            "timestamp": pd.to_datetime([
                "2026-03-25 09:02:43.300",
            ]),
            "event_id": [7],
            "parameter": [6],
        }
    )

    shift = fv._compute_export_shift(
        original,
        collected,
        original_ts_col="timestamp",
        collected_ts_col="timestamp",
        tod_align=True,
        group_tolerance=0.15,
    )

    shifted = original.copy()
    shifted["timestamp"] = shifted["timestamp"] + shift

    assert shift == pd.Timedelta(days=28)
    assert shifted["timestamp"].min() == pd.Timestamp("2026-03-25 09:00:06.300")
    assert shifted.loc[1, "timestamp"] == pd.Timestamp("2026-03-25 09:00:14.600")


def test_non_tod_export_shift_uses_comparison_reference_points(monkeypatch):
    original = pd.DataFrame(
        {
            "timestamp": pd.to_datetime([
                "2026-02-25 09:00:06.300",
                "2026-02-25 09:00:14.600",
            ]),
            "event_id": [82, 7],
            "parameter": [3, 2],
        }
    )
    collected = pd.DataFrame(
        {
            "timestamp": pd.to_datetime([
                "2026-03-25 09:02:43.300",
            ]),
            "event_id": [7],
            "parameter": [6],
        }
    )

    monkeypatch.setattr(fv.sr, "find_temporal_offset", lambda *args, **kwargs: 27.0)

    shift = fv._compute_export_shift(
        original,
        collected,
        original_ts_col="timestamp",
        collected_ts_col="timestamp",
        tod_align=False,
        group_tolerance=0.15,
    )

    assert shift == pd.Timedelta(days=28, minutes=2, seconds=1.7)


def test_prepare_analysis_inputs_for_tod_uses_shared_wall_clock_anchor():
    original = pd.DataFrame(
        {
            "timestamp": pd.to_datetime([
                "2026-02-25 09:00:14.600",
                "2026-02-25 09:00:18.100",
            ]),
            "event_id": [7, 9],
            "parameter": [2, 2],
        }
    )
    collected = pd.DataFrame(
        {
            "timestamp": pd.to_datetime([
                "2026-03-25 09:02:43.300",
                "2026-03-25 09:02:46.800",
            ]),
            "event_id": [7, 9],
            "parameter": [6, 6],
        }
    )

    shifted_original, shifted_collected, start_a, start_b = fv._prepare_analysis_inputs(
        original,
        collected,
        tod_align=True,
    )

    assert shifted_original["timestamp"].min() == pd.Timestamp("2026-03-25 09:00:14.600")
    assert shifted_collected["timestamp"].min() == pd.Timestamp("2026-03-25 09:02:43.300")
    assert start_a == datetime(2026, 3, 25, 9, 0, 14, 600000)
    assert start_b == datetime(2026, 3, 25, 9, 0, 14, 600000)


def test_resolve_manual_analysis_start_uses_collected_run_date():
    collected = pd.DataFrame(
        {
            "timestamp": pd.to_datetime([
                "2026-03-25 09:07:10.000",
                "2026-03-25 09:09:12.000",
            ]),
            "event_id": [44, 10],
            "parameter": [2, 2],
        }
    )

    analysis_start = fv._resolve_manual_analysis_start(
        collected,
        analysis_start_time="09:10",
    )

    assert analysis_start == pd.Timestamp("2026-03-25 09:10:00")


def test_filter_chunk_scores_after_settle_excludes_initial_window():
    chunk_scores = [
        ChunkScore(center_seconds=300.0, match_percentage=40.0, window_seconds=2700.0),
        ChunkScore(center_seconds=600.0, match_percentage=70.0, window_seconds=2700.0),
        ChunkScore(center_seconds=1200.0, match_percentage=98.0, window_seconds=2700.0),
    ]

    filtered = _filter_chunk_scores_after_settle(chunk_scores, settle_seconds=600.0)

    assert [chunk.center_seconds for chunk in filtered] == [600.0, 1200.0]


def test_trim_to_analysis_start_uses_sent_timestamp_cutoff():
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime([
                "2026-03-25 09:00:14.600",
                "2026-03-25 09:09:59.000",
                "2026-03-25 09:10:14.600",
            ]),
            "event_id": [7, 9, 11],
            "parameter": [2, 2, 2],
        }
    )

    trimmed = fv._trim_to_analysis_start(df, pd.Timestamp("2026-03-25 09:10:14.600"))

    assert list(trimmed["event_id"]) == [11]


def test_sparkline_uses_actual_wall_clock_labels():
    svg = render_sparkline_svg(
        [
            ChunkScore(center_seconds=1350.0, match_percentage=95.0, window_seconds=2700.0),
            ChunkScore(center_seconds=1950.0, match_percentage=96.0, window_seconds=2700.0),
            ChunkScore(center_seconds=2550.0, match_percentage=97.0, window_seconds=2700.0),
        ],
        base_timestamp=datetime(2026, 3, 25, 9, 10, 0),
    )

    assert "09:10" in svg
    assert "2026-03-25 09:10:00 to 09:55:00" in svg
