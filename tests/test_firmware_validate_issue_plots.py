import importlib.util
import sys
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def _load_firmware_validate_module():
    module_name = "_firmware_validate_under_test"
    if module_name in sys.modules:
        return sys.modules[module_name]

    module_path = Path(__file__).resolve().parents[1] / "firmware_validation" / "firmware_validate.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _timeline(event_class, event_value, durations):
    base_time = datetime(2026, 4, 2, 9, 0, 0)
    rows = []
    cursor = base_time
    for duration in durations:
        start_time = cursor
        end_time = start_time + timedelta(seconds=duration)
        rows.append(
            {
                "StartTime": start_time,
                "EndTime": end_time,
                "Duration": duration,
                "EventClass": event_class,
                "EventValue": event_value,
            }
        )
        cursor = end_time + timedelta(seconds=5)
    return pd.DataFrame(rows)


def test_clearance_issue_plots_skip_old_version_only_irregularities(tmp_path, monkeypatch):
    fv = _load_firmware_validate_module()
    captured = []

    def fake_create_comparison_gantt_matplotlib(**kwargs):
        captured.append(kwargs)
        return plt.figure()

    monkeypatch.setattr(fv.sr, "create_comparison_gantt_matplotlib", fake_create_comparison_gantt_matplotlib)

    plot_paths, captions = fv._generate_special_issue_plots(
        scenario_id="03013",
        timeline_a=_timeline("Yellow", 2, [4.0, 4.0, 4.8]),
        timeline_b=_timeline("Yellow", 2, [4.0, 4.0, 4.0]),
        aligned_timeline_a=_timeline("Yellow", 2, [4.0, 4.0, 4.8]),
        aligned_timeline_b=_timeline("Yellow", 2, [4.0, 4.0, 4.0]),
        phase_differences=[],
        clearance_irregularities=[
            {
                "label": "Ph 2",
                "state": "Yellow",
                "event_class": "Yellow",
                "event_value": 2,
                "irregular_count_a": 1,
                "irregular_count_b": 0,
            }
        ],
        operational_diffs=[],
        plots_dir=str(tmp_path),
        label_a="2.15.1",
        label_b="2.17.3",
        window_minutes=5.0,
        time_offset_b=0.0,
        align_by_time_delta=False,
    )

    assert plot_paths == []
    assert captions == []
    assert captured == []


def test_clearance_issue_plots_anchor_new_version_irregularity(tmp_path, monkeypatch):
    fv = _load_firmware_validate_module()
    captured = []

    def fake_create_comparison_gantt_matplotlib(**kwargs):
        captured.append(kwargs)
        return plt.figure()

    monkeypatch.setattr(fv.sr, "create_comparison_gantt_matplotlib", fake_create_comparison_gantt_matplotlib)

    timeline_a = _timeline("Yellow", 2, [4.0, 4.0, 5.5])
    timeline_b = _timeline("Yellow", 2, [4.0, 4.0, 4.9])

    plot_paths, captions = fv._generate_special_issue_plots(
        scenario_id="03013",
        timeline_a=timeline_a,
        timeline_b=timeline_b,
        aligned_timeline_a=timeline_a,
        aligned_timeline_b=timeline_b,
        phase_differences=[],
        clearance_irregularities=[
            {
                "label": "Ph 2",
                "state": "Yellow",
                "event_class": "Yellow",
                "event_value": 2,
                "irregular_count_a": 1,
                "irregular_count_b": 1,
            }
        ],
        operational_diffs=[],
        plots_dir=str(tmp_path),
        label_a="2.15.1",
        label_b="2.17.3",
        window_minutes=5.0,
        time_offset_b=0.0,
        align_by_time_delta=False,
    )

    assert len(plot_paths) == 1
    assert len(captions) == 1
    assert len(captured) == 1
    assert captured[0]["divergence_start"] == timeline_b.iloc[2]["StartTime"]
    assert "in 2.17.3" in captions[0]


def test_mixed_use_overlap_yellow_is_treated_as_non_clearance_issue():
    fv = _load_firmware_validate_module()
    diff = {
        "event_class": "Overlap Yellow",
        "event_value": 5,
    }

    assert fv._treat_phase_diff_as_non_clearance_issue(
        diff,
        _timeline("Overlap Yellow", 5, [4.0] * 8 + [20.0, 22.0]),
        _timeline("Overlap Yellow", 5, [4.0] * 9 + [21.0]),
    )


def test_clustered_overlap_yellow_remains_clearance_for_issue_selection():
    fv = _load_firmware_validate_module()
    diff = {
        "event_class": "Overlap Yellow",
        "event_value": 5,
    }

    assert not fv._treat_phase_diff_as_non_clearance_issue(
        diff,
        _timeline("Overlap Yellow", 5, [4.0, 4.0, 4.0, 4.4]),
        _timeline("Overlap Yellow", 5, [4.0, 4.0, 4.0, 4.5]),
    )
