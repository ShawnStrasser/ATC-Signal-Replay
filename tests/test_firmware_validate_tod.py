from datetime import datetime

import pandas as pd

import firmware_validation.firmware_validate as fv


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
