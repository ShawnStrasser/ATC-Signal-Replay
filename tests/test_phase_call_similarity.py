from datetime import datetime, timedelta

import pandas as pd

from signal_replay.comparison import (
    ChunkScore,
    PhaseCallChunkScore,
    compare_runs,
    generate_operational_difference_summary,
    generate_phase_difference_summary,
    render_sparkline_svg,
)
from signal_replay.report import generate_report
from signal_replay.test_suite import (
    FirmwareTestSuite,
    ScenarioResult,
    TestBatch as SuiteBatch,
    TestScenario as SuiteScenario,
    TestType as SuiteTestType,
)


def _make_events(main_offsets, phase_offsets=None):
    base_time = datetime(2026, 4, 2, 9, 0, 0)
    rows = []

    for idx, offset in enumerate(main_offsets):
        rows.append(
            {
                "timestamp": base_time + timedelta(seconds=offset),
                "event_id": 7 if idx % 2 == 0 else 9,
                "parameter": 2,
            }
        )

    for event_id, offset, parameter in phase_offsets or []:
        rows.append(
            {
                "timestamp": base_time + timedelta(seconds=offset),
                "event_id": event_id,
                "parameter": parameter,
            }
        )

    return pd.DataFrame(rows).sort_values(["timestamp", "event_id", "parameter"]).reset_index(drop=True)


def _make_timeline(rows):
    return pd.DataFrame(rows)


def test_compare_runs_excludes_chunk_when_phase_call_similarity_is_below_threshold():
    main_offsets = [0, 600, 1200, 1800, 2400, 2800]
    phase_offsets_a = [
        (43, 300, 2),
        (44, 900, 2),
        (43, 1500, 2),
        (44, 2100, 2),
    ]

    events_a = _make_events(main_offsets, phase_offsets=phase_offsets_a)
    events_b = _make_events(main_offsets, phase_offsets=[])

    result = compare_runs(
        events_a,
        events_b,
        device_id="03013",
        auto_align=False,
        phase_call_threshold=90.0,
    )

    assert len(result.chunk_scores) == 1
    assert result.chunk_scores[0].match_percentage == 100.0
    assert len(result.phase_call_chunk_scores) == 1
    assert result.phase_call_chunk_scores[0].similarity_percentage == 0.0
    assert result.phase_call_chunk_scores[0].excluded_from_match is True
    assert result.included_chunk_count == 0
    assert result.excluded_chunk_count == 1
    assert result.match_percentage == 0.0


def test_compare_runs_does_not_exclude_low_activity_phase_call_chunk():
    main_offsets = [0, 600, 1200, 1800, 2400, 2800]
    phase_offsets_a = [(43, 900, 2)]

    events_a = _make_events(main_offsets, phase_offsets=phase_offsets_a)
    events_b = _make_events(main_offsets, phase_offsets=[])

    result = compare_runs(
        events_a,
        events_b,
        device_id="03013",
        auto_align=False,
        phase_call_threshold=90.0,
    )

    assert len(result.phase_call_chunk_scores) == 1
    assert result.phase_call_chunk_scores[0].similarity_percentage is None
    assert result.phase_call_chunk_scores[0].has_activity is True
    assert result.phase_call_chunk_scores[0].excluded_from_match is False
    assert result.included_chunk_count == 1
    assert result.excluded_chunk_count == 0
    assert result.match_percentage == 100.0


def test_render_sparkline_draws_phase_call_overlay_and_exclusion_legend():
    svg = render_sparkline_svg(
        [
            ChunkScore(center_seconds=1350.0, match_percentage=97.0, window_seconds=2700.0),
            ChunkScore(center_seconds=3750.0, match_percentage=91.0, window_seconds=2700.0),
        ],
        phase_call_chunk_scores=[
            PhaseCallChunkScore(center_seconds=1350.0, window_seconds=2700.0, similarity_percentage=92.0),
            PhaseCallChunkScore(center_seconds=3750.0, window_seconds=2700.0, similarity_percentage=84.0, excluded_from_match=True),
        ],
        phase_call_threshold=90.0,
        base_timestamp=datetime(2026, 4, 2, 9, 0, 0),
    )

    assert 'stroke="#000"' in svg
    assert 'Phase-call similarity' in svg
    assert 'Excluded from match average (&lt; 90%)' in svg
    assert 'opacity="0.42"' in svg


def test_combined_timeline_chart_uses_dynamic_y_axis_without_exclusion_legend():
    svg = render_sparkline_svg(
        [],
        phase_call_chunk_scores=[
            PhaseCallChunkScore(center_seconds=1350.0, window_seconds=2700.0, similarity_percentage=91.8),
            PhaseCallChunkScore(center_seconds=3750.0, window_seconds=2700.0, similarity_percentage=93.1),
        ],
        phase_call_threshold=90.0,
        auto_scale_y=True,
        show_exclusion_legend=False,
    )

    assert 'Excluded from match average' not in svg
    assert '>0%</text>' not in svg
    assert '>100%</text>' not in svg


def test_phase_and_operational_summaries_are_split_by_event_class():
    base_time = datetime(2026, 4, 2, 9, 0, 0)
    timeline_a = _make_timeline([
        {
            "StartTime": base_time,
            "EndTime": base_time + timedelta(seconds=20),
            "EventClass": "Green",
            "EventValue": 2,
        },
        {
            "StartTime": base_time + timedelta(seconds=30),
            "EndTime": base_time + timedelta(seconds=40),
            "EventClass": "Ped Service",
            "EventValue": 2,
        },
        {
            "StartTime": base_time + timedelta(seconds=45),
            "EndTime": base_time + timedelta(seconds=50),
            "EventClass": "Preempt",
            "EventValue": 1,
        },
        {
            "StartTime": base_time + timedelta(seconds=55),
            "EndTime": base_time + timedelta(seconds=60),
            "EventClass": "Transition Longway",
            "EventValue": 0,
        },
    ])
    timeline_b = _make_timeline([
        {
            "StartTime": base_time,
            "EndTime": base_time + timedelta(seconds=25),
            "EventClass": "Green",
            "EventValue": 2,
        },
        {
            "StartTime": base_time + timedelta(seconds=30),
            "EndTime": base_time + timedelta(seconds=48),
            "EventClass": "Ped Service",
            "EventValue": 2,
        },
        {
            "StartTime": base_time + timedelta(seconds=45),
            "EndTime": base_time + timedelta(seconds=53),
            "EventClass": "Preempt",
            "EventValue": 1,
        },
        {
            "StartTime": base_time + timedelta(seconds=55),
            "EndTime": base_time + timedelta(seconds=63),
            "EventClass": "Transition Longway",
            "EventValue": 0,
        },
    ])

    phase_diffs = generate_phase_difference_summary(timeline_a, timeline_b, tolerance_seconds=0.2)
    operational_diffs = generate_operational_difference_summary(timeline_a, timeline_b, tolerance_seconds=0.2)

    assert [item["label"] for item in phase_diffs] == ["Ph 2"]
    assert {item["label"] for item in operational_diffs} == {"Ped 2", "Preempt 1", "Transition"}


def test_operational_summary_includes_transition_rows_with_null_event_value():
    base_time = datetime(2026, 4, 2, 9, 0, 0)
    timeline_a = _make_timeline([
        {
            "StartTime": base_time,
            "EndTime": base_time + timedelta(seconds=10),
            "EventClass": "Transition Longway",
            "EventValue": pd.NA,
        }
    ])
    timeline_b = _make_timeline([
        {
            "StartTime": base_time,
            "EndTime": base_time + timedelta(seconds=40),
            "EventClass": "Transition Longway",
            "EventValue": pd.NA,
        }
    ])

    operational_diffs = generate_operational_difference_summary(timeline_a, timeline_b, tolerance_seconds=0.2)

    assert len(operational_diffs) == 1
    assert operational_diffs[0]["label"] == "Transition"
    assert operational_diffs[0]["state"] == "Longway"


def test_generate_report_includes_combined_timeline_and_threshold(tmp_path):
    suite = FirmwareTestSuite(
        suite_name="Firmware Validation",
        firmware_version="2.17.3",
        baseline_version="2.15.1",
        scenarios=[
            SuiteScenario(
                scenario_id="03013",
                database_name="03013.bin",
                events_source="03013.parquet",
                test_type=SuiteTestType.SIMILARITY,
            )
        ],
        batches=[SuiteBatch(batch_id="batch_1", assignments={"03013": "127.0.0.1:9701"})],
        output_dir=str(tmp_path),
        phase_call_similarity_threshold=90.0,
    )

    result = ScenarioResult(
        scenario_id="03013",
        test_type=SuiteTestType.SIMILARITY,
        firmware_version="2.17.3",
        passed=True,
        match_percentage=97.5,
        num_divergences=0,
        runs_completed=1,
        total_runs=1,
        notes="No divergences",
        phase_differences=[
            {
                "label": "Ph 2",
                "state": "Green",
                "count_a": 1,
                "count_b": 1,
                "count_delta": 0,
                "duration_a": 10.0,
                "duration_b": 12.0,
                "duration_delta": 2.0,
            }
        ],
        operational_differences=[
            {
                "label": "Transition",
                "state": "Longway",
                "count_a": 1,
                "count_b": 2,
                "count_delta": 1,
                "duration_a": 5.0,
                "duration_b": 7.0,
                "duration_delta": 2.0,
            }
        ],
        phase_call_chunk_scores=[
            {
                "center_seconds": 1350.0,
                "window_seconds": 2700.0,
                "similarity_percentage": 96.0,
                "has_activity": True,
                "excluded_from_match": False,
            },
            {
                "center_seconds": 3750.0,
                "window_seconds": 2700.0,
                "similarity_percentage": 88.0,
                "has_activity": True,
                "excluded_from_match": True,
            },
        ],
        timeline_difference_analysis_available=True,
    )

    thrown_out = ScenarioResult(
        scenario_id="03014",
        test_type=SuiteTestType.SIMILARITY,
        firmware_version="2.17.3",
        passed=False,
        match_percentage=0.0,
        num_divergences=2,
        runs_completed=1,
        total_runs=1,
        notes="Match: thrown out",
        included_chunk_count=0,
        excluded_chunk_count=2,
        thrown_out=True,
        phase_call_chunk_scores=[
            {
                "center_seconds": 1350.0,
                "window_seconds": 2700.0,
                "similarity_percentage": 50.0,
                "has_activity": True,
                "excluded_from_match": True,
            }
        ],
        timeline_difference_analysis_available=True,
    )

    report_path = tmp_path / "report.html"
    generate_report([result, thrown_out], suite, str(report_path))

    html = report_path.read_text(encoding="utf-8")
    assert "Combined Timeline" in html
    assert "system-wide sanity check" in html
    assert "Phase-call similarity threshold" in html
    assert "90.0%" in html
    assert "Transition / Preempt / Ped Service Differences" in html
    assert "Thrown out" in html
    assert ">97.5%</div>" in html
    assert "No meaningful transition, preempt, or pedestrian-service differences were found." in html