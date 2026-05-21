from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure
import signal_replay as sr

from signal_replay.comparison import (
    ChunkScore,
    PhaseCallChunkScore,
    DivergenceWindow,
    build_included_event_periods,
    clip_timeline_to_relative_periods,
    compare_runs,
    create_comparison_gantt_matplotlib,
    filter_divergence_windows_to_periods,
    generate_clearance_irregularity_summary,
    generate_operational_difference_summary,
    generate_phase_difference_summary,
    is_overlap_clearance_interval_candidate,
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


def test_signal_replay_package_exports_clearance_summary():
    assert callable(sr.generate_clearance_irregularity_summary)


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


def test_generate_clearance_irregularity_summary_counts_off_median_events():
    base_time = datetime(2026, 4, 2, 9, 0, 0)

    def _make_clearance_timeline(durations):
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
                    "EventClass": "Yellow",
                    "EventValue": 2,
                }
            )
            cursor = end_time + timedelta(seconds=10)
        return pd.DataFrame(rows)

    timeline_a = _make_clearance_timeline([4.0, 4.0, 4.0, 4.3])
    timeline_b = _make_clearance_timeline([4.2, 4.2, 4.5, 4.2])

    rows = generate_clearance_irregularity_summary(timeline_a, timeline_b, threshold_seconds=0.2)

    assert len(rows) == 1
    row = rows[0]
    assert row["label"] == "Ph 2"
    assert row["state"] == "Yellow"
    assert row["median_a"] == 4.0
    assert row["median_b"] == 4.2
    assert row["irregular_count_a"] == 1
    assert row["irregular_count_b"] == 1
    assert row["high_count_a"] == 1
    assert row["low_count_a"] == 0
    assert row["high_count_b"] == 1
    assert row["low_count_b"] == 0


def test_generate_clearance_irregularity_summary_skips_long_overlap_yellow_red():
    base_time = datetime(2026, 4, 2, 9, 0, 0)

    def _make_overlap_clearance_timeline(durations):
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
                    "EventClass": "Overlap Yellow",
                    "EventValue": 5,
                }
            )
            cursor = end_time + timedelta(seconds=5)
        return pd.DataFrame(rows)

    timeline_a = _make_overlap_clearance_timeline([8.0, 8.0, 8.0, 12.0])
    timeline_b = _make_overlap_clearance_timeline([8.0, 8.0, 8.0, 9.0])

    rows = generate_clearance_irregularity_summary(timeline_a, timeline_b, threshold_seconds=0.2)

    assert rows == []


def test_generate_clearance_irregularity_summary_skips_mixed_use_overlap_yellow():
    base_time = datetime(2026, 4, 2, 9, 0, 0)

    def _make_overlap_yellow_timeline(durations):
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
                    "EventClass": "Overlap Yellow",
                    "EventValue": 5,
                }
            )
            cursor = end_time + timedelta(seconds=5)
        return pd.DataFrame(rows)

    timeline_a = _make_overlap_yellow_timeline([4.0] * 8 + [20.0, 22.0])
    timeline_b = _make_overlap_yellow_timeline([4.0] * 9 + [21.0])

    rows = generate_clearance_irregularity_summary(timeline_a, timeline_b, threshold_seconds=0.2)

    assert rows == []
    assert not is_overlap_clearance_interval_candidate(pd.Series([4.0] * 94 + [20.0] * 6))


def test_generate_clearance_irregularity_summary_keeps_tightly_clustered_overlap_yellow():
    base_time = datetime(2026, 4, 2, 9, 0, 0)

    def _make_overlap_yellow_timeline(durations):
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
                    "EventClass": "Overlap Yellow",
                    "EventValue": 5,
                }
            )
            cursor = end_time + timedelta(seconds=5)
        return pd.DataFrame(rows)

    timeline_a = _make_overlap_yellow_timeline([4.0, 4.0, 4.0, 4.4])
    timeline_b = _make_overlap_yellow_timeline([4.0, 4.0, 4.0, 4.5])

    rows = generate_clearance_irregularity_summary(timeline_a, timeline_b, threshold_seconds=0.2)

    assert len(rows) == 1
    assert rows[0]["label"] == "Ovlp 5"
    assert rows[0]["state"] == "Yellow"
    assert is_overlap_clearance_interval_candidate(pd.Series([4.0] * 95 + [7.0] * 5))


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


def test_compare_runs_reports_percent_timing_match_from_aligned_exact_matches():
    main_offsets = list(range(0, 3001, 60))
    shifted_offsets = [
        offset + (2.0 if idx % 5 == 0 else 0.0)
        for idx, offset in enumerate(main_offsets)
    ]

    events_a = _make_events(main_offsets)
    events_b = _make_events(shifted_offsets)

    result = compare_runs(
        events_a,
        events_b,
        device_id="03013",
        auto_align=False,
        phase_call_threshold=0.0,
    )

    assert result.match_percentage == 100.0
    assert result.timing_match_percentage is not None
    assert 0.0 <= result.timing_match_percentage < 90.0
    assert result.timing_p95_error_seconds is not None
    assert result.timing_p95_error_seconds > 0.5
    assert result.timing_max_error_seconds is not None
    assert result.timing_max_error_seconds > 0.5


def test_compare_runs_reports_timing_match_when_full_series_dtw_is_skipped():
    main_offsets = list(range(0, 432000, 60))
    shifted_offsets = [
        offset + (2.0 if idx % 5 == 0 else 0.0)
        for idx, offset in enumerate(main_offsets)
    ]

    events_a = _make_events(main_offsets)
    events_b = _make_events(shifted_offsets)

    result = compare_runs(
        events_a,
        events_b,
        device_id="03013",
        auto_align=False,
        phase_call_threshold=0.0,
    )

    assert result.sequence_dtw.warping_path == []
    assert result.match_percentage == 100.0
    assert result.timing_match_percentage is not None
    assert 0.0 <= result.timing_match_percentage < 90.0
    assert result.timing_p95_error_seconds is not None
    assert result.timing_p95_error_seconds > 0.5
    assert result.timing_max_error_seconds is not None
    assert result.timing_max_error_seconds > 0.5


def test_build_included_event_periods_merges_overlapping_good_chunks():
    periods = build_included_event_periods(
        [
            ChunkScore(center_seconds=1350.0, match_percentage=98.0, window_seconds=2700.0),
            ChunkScore(center_seconds=3750.0, match_percentage=97.0, window_seconds=2700.0),
        ],
        [
            PhaseCallChunkScore(center_seconds=1350.0, window_seconds=2700.0, similarity_percentage=96.0),
            PhaseCallChunkScore(center_seconds=3750.0, window_seconds=2700.0, similarity_percentage=94.0),
        ],
        min_start_seconds=600.0,
    )

    assert periods == [(600.0, 5100.0)]


def test_build_included_event_periods_subtracts_excluded_chunk_overlap():
    periods = build_included_event_periods(
        [
            ChunkScore(center_seconds=30.0, match_percentage=98.0, window_seconds=60.0),
            ChunkScore(center_seconds=50.0, match_percentage=60.0, window_seconds=20.0),
            ChunkScore(center_seconds=80.0, match_percentage=97.0, window_seconds=60.0),
        ],
        [
            PhaseCallChunkScore(center_seconds=30.0, window_seconds=60.0, similarity_percentage=96.0),
            PhaseCallChunkScore(
                center_seconds=50.0,
                window_seconds=20.0,
                similarity_percentage=40.0,
                excluded_from_match=True,
            ),
            PhaseCallChunkScore(center_seconds=80.0, window_seconds=60.0, similarity_percentage=94.0),
        ],
    )

    assert periods == [(0.0, 40.0), (60.0, 110.0)]


def test_clip_timeline_to_relative_periods_splits_rows_at_good_period_edges():
    base_time = datetime(2026, 4, 2, 9, 0, 0)
    timeline = _make_timeline([
        {
            "StartTime": base_time,
            "EndTime": base_time + timedelta(seconds=20),
            "Duration": 20.0,
            "EventClass": "Green",
            "EventValue": 2,
        }
    ])

    clipped = clip_timeline_to_relative_periods(timeline, [(5.0, 10.0), (12.0, 18.0)])

    assert len(clipped) == 2
    assert clipped["Duration"].tolist() == [5.0, 6.0]
    assert clipped["StartTime"].tolist() == [
        base_time + timedelta(seconds=5),
        base_time + timedelta(seconds=12),
    ]
    assert clipped["EndTime"].tolist() == [
        base_time + timedelta(seconds=10),
        base_time + timedelta(seconds=18),
    ]


def test_clip_timeline_to_relative_periods_uses_analysis_base_timestamp():
    base_time = datetime(2026, 4, 2, 9, 0, 0)
    timeline = _make_timeline([
        {
            "StartTime": base_time + timedelta(hours=1),
            "EndTime": base_time + timedelta(hours=1, seconds=20),
            "Duration": 20.0,
            "EventClass": "Green",
            "EventValue": 2,
        }
    ])

    clipped = clip_timeline_to_relative_periods(
        timeline,
        [(3605.0, 3610.0)],
        base_timestamp=base_time,
    )

    assert len(clipped) == 1
    assert clipped.iloc[0]["Duration"] == 5.0
    assert clipped.iloc[0]["StartTime"] == base_time + timedelta(hours=1, seconds=5)
    assert clipped.iloc[0]["EndTime"] == base_time + timedelta(hours=1, seconds=10)


def test_filter_divergence_windows_to_periods_keeps_only_good_period_divergences():
    divergences = [
        DivergenceWindow(
            start_index_a=0,
            end_index_a=1,
            start_index_b=0,
            end_index_b=1,
            start_time_delta_a=0.0,
            end_time_delta_a=10.0,
            start_time_delta_b=0.0,
            end_time_delta_b=10.0,
            original_start_seconds_a=50.0,
            original_end_seconds_a=60.0,
            original_start_seconds_b=50.0,
            original_end_seconds_b=60.0,
        ),
        DivergenceWindow(
            start_index_a=2,
            end_index_a=3,
            start_index_b=2,
            end_index_b=3,
            start_time_delta_a=0.0,
            end_time_delta_a=10.0,
            start_time_delta_b=0.0,
            end_time_delta_b=10.0,
            original_start_seconds_a=250.0,
            original_end_seconds_a=260.0,
            original_start_seconds_b=250.0,
            original_end_seconds_b=260.0,
        ),
    ]

    filtered = filter_divergence_windows_to_periods(divergences, [(200.0, 300.0)])

    assert len(filtered) == 1
    assert filtered[0].original_start_seconds_a == 250.0


def test_compare_runs_filters_divergences_when_all_chunks_are_bad():
    main_offsets = [0, 600, 1200, 1800, 2400, 2800]
    phase_offsets_a = [
        (43, 300, 2),
        (44, 900, 2),
        (43, 1500, 2),
        (44, 2100, 2),
    ]

    events_a = _make_events(main_offsets, phase_offsets=phase_offsets_a)
    events_b = _make_events(main_offsets + [3000], phase_offsets=[])

    result = compare_runs(
        events_a,
        events_b,
        device_id="03013",
        auto_align=False,
        phase_call_threshold=90.0,
    )

    assert result.thrown_out is True
    assert result.included_event_periods_a == []
    assert result.included_event_periods_b == []
    assert result.divergence_windows == []


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


def test_render_sparkline_places_start_time_label_at_left_edge():
    svg = render_sparkline_svg(
        [
            ChunkScore(
                center_seconds=(9 * 3600) + (50 * 60) + (45 * 60 / 2),
                match_percentage=62.7,
                window_seconds=2700.0,
            ),
        ],
        base_timestamp=datetime(2026, 5, 14, 0, 0, 0),
    )

    assert '<text x="64.0" y="200" font-size="12" fill="#5f6368" font-family="sans-serif" text-anchor="middle">09:50</text>' in svg


def test_create_comparison_gantt_matplotlib_omits_transition_section_text():
    base_time = datetime(2026, 4, 2, 9, 0, 0)
    timeline = _make_timeline([
        {
            "StartTime": base_time,
            "EndTime": base_time + timedelta(seconds=20),
            "Duration": 20.0,
            "EventClass": "Green",
            "EventValue": 2,
        },
        {
            "StartTime": base_time + timedelta(seconds=40),
            "EndTime": base_time + timedelta(seconds=60),
            "Duration": 20.0,
            "EventClass": "Transition Shortway",
            "EventValue": None,
        },
    ])

    fig = create_comparison_gantt_matplotlib(
        timeline_a=timeline,
        timeline_b=timeline.iloc[0:0].copy(),
        label_a="2.15.1",
        label_b="2.17.3",
        title="03013 Issue Focus - Transition Shortway",
        window_minutes=5.0,
        align_by_time_delta=False,
        time_offset_b=0.0,
    )

    assert fig is not None
    text_values = [text.get_text() for text in fig.axes[0].texts]
    assert "Transition / Preempt / Ped Service" not in text_values
    plt.close(fig)


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


def test_comparison_gantt_overlays_programmed_splits_without_adding_rows():
    base_time = datetime(2026, 4, 2, 9, 0, 0)
    timeline_a = _make_timeline([
        {
            "StartTime": base_time,
            "EndTime": base_time + timedelta(seconds=20),
            "EventClass": "Green",
            "EventValue": 2,
        },
    ])
    timeline_b = _make_timeline([
        {
            "StartTime": base_time,
            "EndTime": base_time + timedelta(seconds=25),
            "EventClass": "Green",
            "EventValue": 2,
        },
    ])
    programmed_split_timeline = pd.DataFrame(
        [
            {
                "Phase": 2,
                "StartTime": base_time + timedelta(seconds=5),
                "EndTime": base_time + timedelta(seconds=15),
            }
        ]
    )

    original_subplots = sr.comparison.plt.subplots

    def fake_subplots(*args, **kwargs):
        fig = Figure(figsize=kwargs.get("figsize"))
        ax = fig.add_subplot(111)
        return fig, ax

    sr.comparison.plt.subplots = fake_subplots

    try:
        fig = create_comparison_gantt_matplotlib(
            timeline_a=timeline_a,
            timeline_b=timeline_b,
            label_a="2.15.1",
            label_b="2.17.3",
            title="2B045 Issue Focus - Ph 2 Green",
            window_minutes=5.0,
            align_by_time_delta=False,
            programmed_split_timeline=programmed_split_timeline,
        )
    finally:
        sr.comparison.plt.subplots = original_subplots

    assert fig is not None
    axis = fig.axes[0]
    row_labels = [tick.get_text() for tick in axis.get_yticklabels()]
    legend = axis.get_legend()

    assert row_labels == ["Ph 2 (2.15.1)", "Ph 2 (2.17.3)"]
    assert legend is not None
    assert [text.get_text() for text in legend.get_texts()] == ["Programed Split"]
    assert len(axis.collections) >= 4
    plt.close(fig)


def test_create_multi_divergence_plots_passes_programmed_splits_through(monkeypatch, tmp_path):
    base_time = datetime(2026, 4, 2, 9, 0, 0)
    timeline_a = _make_timeline([
        {
            "StartTime": base_time,
            "EndTime": base_time + timedelta(seconds=20),
            "EventClass": "Green",
            "EventValue": 2,
        },
    ])
    timeline_b = _make_timeline([
        {
            "StartTime": base_time,
            "EndTime": base_time + timedelta(seconds=25),
            "EventClass": "Green",
            "EventValue": 2,
        },
    ])
    programmed_split_timeline = pd.DataFrame(
        [
            {
                "Phase": 2,
                "StartTime": base_time + timedelta(seconds=5),
                "EndTime": base_time + timedelta(seconds=15),
            }
        ]
    )
    captured = {}

    def fake_create_comparison_gantt_matplotlib(**kwargs):
        captured["programmed_split_timeline"] = kwargs["programmed_split_timeline"]
        return object()

    monkeypatch.setattr(sr.comparison, "create_comparison_gantt_matplotlib", fake_create_comparison_gantt_matplotlib)
    monkeypatch.setattr(plt, "close", lambda fig: None)

    result = sr.ComparisonResult(
        device_id="2B045",
        run_a="Original",
        run_b="Replay",
        sequence_dtw=sr.DTWResult(0.0, 0.0, [], 1, 1),
        timing_dtw=sr.DTWResult(0.0, 0.0, [], 1, 1),
        divergence_windows=[
            sr.DivergenceWindow(
                start_index_a=0,
                end_index_a=0,
                start_index_b=0,
                end_index_b=0,
                start_time_delta_a=0.0,
                end_time_delta_a=30.0,
                start_time_delta_b=0.0,
                end_time_delta_b=30.0,
                original_start_seconds_a=0.0,
                original_end_seconds_a=30.0,
                original_start_seconds_b=0.0,
                original_end_seconds_b=30.0,
            )
        ],
        match_percentage=100.0,
    )

    paths = sr.create_multi_divergence_plots(
        timeline_a=timeline_a,
        timeline_b=timeline_b,
        comparison_result=result,
        output_dir=tmp_path,
        programmed_split_timeline=programmed_split_timeline,
        align_by_time_delta=False,
    )

    assert len(paths) == 1
    assert captured["programmed_split_timeline"] is programmed_split_timeline


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
            ),
            SuiteScenario(
                scenario_id="03014",
                database_name="03014.bin",
                events_source="03014.parquet",
                test_type=SuiteTestType.SIMILARITY,
            ),
            SuiteScenario(
                scenario_id="03015",
                database_name="03015.bin",
                events_source="03015.parquet",
                test_type=SuiteTestType.SIMILARITY,
            ),
            SuiteScenario(
                scenario_id="03016",
                database_name="03016.bin",
                events_source="03016.parquet",
                test_type=SuiteTestType.SIMILARITY,
            ),
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
        timing_match_percentage=92.5,
        timing_p95_error_seconds=0.432,
        timing_max_error_seconds=0.876,
        num_divergences=0,
        runs_completed=1,
        total_runs=1,
        notes="No divergences",
        phase_differences=[
            {
                "label": "Ph 2",
                "state": "Green",
                "count_a": 20,
                "count_b": 19,
                "count_delta": -1,
                "duration_a": 32.0,
                "duration_b": 32.3,
                "duration_delta": 0.3,
                "total_duration_a": 640.0,
                "total_duration_b": 613.7,
                "total_duration_delta": -26.3,
            },
            {
                "label": "Ovlp 4",
                "state": "Yellow",
                "event_class": "Overlap Yellow",
                "count_a": 10,
                "count_b": 14,
                "count_delta": 4,
                "duration_a": 4.0,
                "duration_b": 4.0,
                "duration_delta": 0.0,
                "total_duration_a": 40.0,
                "total_duration_b": 56.0,
                "total_duration_delta": 16.0,
            }
        ],
        clearance_irregularities=[
            {
                "label": "Ph 2",
                "state": "Yellow",
                "median_a": 4.0,
                "median_b": 4.3,
                "median_delta": 0.3,
                "irregular_count_a": 1,
                "irregular_count_b": 4,
                "irregular_count_delta": 3,
                "high_count_a": 1,
                "high_count_b": 4,
                "low_count_a": 0,
                "low_count_b": 0,
                "high_avg_deviation_a": 0.4,
                "high_avg_deviation_b": 0.3,
                "low_avg_deviation_a": 0.0,
                "low_avg_deviation_b": 0.0,
                "sample_count_a": 20,
                "sample_count_b": 19,
            }
        ],
        invalid_clearance_irregularities=[
            {
                "label": "Ph 4",
                "state": "Red",
                "median_a": 1.5,
                "median_b": 1.4,
                "median_delta": -0.1,
                "irregular_count_a": 1,
                "irregular_count_b": 2,
                "irregular_count_delta": 1,
                "high_count_a": 0,
                "high_count_b": 0,
                "low_count_a": 1,
                "low_count_b": 2,
                "high_avg_deviation_a": 0.0,
                "high_avg_deviation_b": 0.0,
                "low_avg_deviation_a": 0.25,
                "low_avg_deviation_b": 0.35,
                "sample_count_a": 20,
                "sample_count_b": 19,
            }
        ],
        operational_differences=[
            {
                "label": "Preempt 6",
                "state": "Active",
                "count_a": 2,
                "count_b": 3,
                "count_delta": 1,
                "duration_a": 2240.9,
                "duration_b": 86.3,
                "duration_delta": -2154.6,
                "total_duration_a": 4481.8,
                "total_duration_b": 258.9,
                "total_duration_delta": -4222.9,
            },
            {
                "label": "Ped 2",
                "state": "Service",
                "count_a": 4,
                "count_b": 4,
                "count_delta": 0,
                "duration_a": 8.0,
                "duration_b": 11.0,
                "duration_delta": 3.0,
                "total_duration_a": 32.0,
                "total_duration_b": 44.0,
                "total_duration_delta": 12.0,
            },
            {
                "label": "Ovlp Ped 3",
                "state": "Service",
                "count_a": 3,
                "count_b": 3,
                "count_delta": 0,
                "duration_a": 9.0,
                "duration_b": 11.5,
                "duration_delta": 2.5,
                "total_duration_a": 27.0,
                "total_duration_b": 34.5,
                "total_duration_delta": 7.5,
            }
        ],
        invalid_operational_differences=[
            {
                "label": "Transition",
                "state": "Active",
                "count_a": 5,
                "count_b": 6,
                "count_delta": 1,
                "duration_a": 10.0,
                "duration_b": 14.0,
                "duration_delta": 4.0,
                "total_duration_a": 50.0,
                "total_duration_b": 84.0,
                "total_duration_delta": 34.0,
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

    trend_peer = ScenarioResult(
        scenario_id="03015",
        test_type=SuiteTestType.SIMILARITY,
        firmware_version="2.17.3",
        passed=False,
        match_percentage=91.2,
        num_divergences=1,
        runs_completed=1,
        total_runs=1,
        notes="Recurring preempt drift",
        phase_differences=[
            {
                "label": "Ph 6",
                "state": "Green",
                "count_a": 18,
                "count_b": 18,
                "count_delta": 0,
                "duration_a": 44.4,
                "duration_b": 43.9,
                "duration_delta": -0.5,
                "total_duration_a": 799.2,
                "total_duration_b": 790.2,
                "total_duration_delta": -9.0,
            }
        ],
        clearance_irregularities=[
            {
                "label": "Ph 6",
                "state": "Yellow",
                "median_a": 4.4,
                "median_b": 3.9,
                "median_delta": -0.5,
                "irregular_count_a": 2,
                "irregular_count_b": 1,
                "irregular_count_delta": -1,
                "high_count_a": 2,
                "high_count_b": 1,
                "low_count_a": 0,
                "low_count_b": 0,
                "high_avg_deviation_a": 0.35,
                "high_avg_deviation_b": 0.3,
                "low_avg_deviation_a": 0.0,
                "low_avg_deviation_b": 0.0,
                "sample_count_a": 18,
                "sample_count_b": 18,
            }
        ],
        invalid_clearance_irregularities=[
            {
                "label": "Ph 4",
                "state": "Red",
                "median_a": 1.6,
                "median_b": 1.5,
                "median_delta": -0.1,
                "irregular_count_a": 2,
                "irregular_count_b": 1,
                "irregular_count_delta": -1,
                "high_count_a": 0,
                "high_count_b": 0,
                "low_count_a": 2,
                "low_count_b": 1,
                "high_avg_deviation_a": 0.0,
                "high_avg_deviation_b": 0.0,
                "low_avg_deviation_a": 0.20,
                "low_avg_deviation_b": 0.30,
                "sample_count_a": 18,
                "sample_count_b": 18,
            }
        ],
        operational_differences=[
            {
                "label": "Preempt 6",
                "state": "Active",
                "count_a": 6,
                "count_b": 6,
                "count_delta": 0,
                "duration_a": 200.0,
                "duration_b": 100.0,
                "duration_delta": -100.0,
                "total_duration_a": 1200.0,
                "total_duration_b": 600.0,
                "total_duration_delta": -600.0,
            },
            {
                "label": "Ped 5",
                "state": "Service",
                "count_a": 4,
                "count_b": 4,
                "count_delta": 0,
                "duration_a": 7.5,
                "duration_b": 10.5,
                "duration_delta": 3.0,
                "total_duration_a": 30.0,
                "total_duration_b": 42.0,
                "total_duration_delta": 12.0,
            },
            {
                "label": "Ovlp Ped 7",
                "state": "Service",
                "count_a": 3,
                "count_b": 3,
                "count_delta": 0,
                "duration_a": 8.5,
                "duration_b": 11.0,
                "duration_delta": 2.5,
                "total_duration_a": 25.5,
                "total_duration_b": 33.0,
                "total_duration_delta": 7.5,
            }
        ],
        invalid_operational_differences=[
            {
                "label": "Transition",
                "state": "Active",
                "count_a": 4,
                "count_b": 5,
                "count_delta": 1,
                "duration_a": 9.0,
                "duration_b": 10.0,
                "duration_delta": 1.0,
                "total_duration_a": 36.0,
                "total_duration_b": 50.0,
                "total_duration_delta": 14.0,
            }
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
        thrown_out_reason="Insufficient scored chunks remained after settling/filtering for a reliable comparison.",
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

    unavailable = ScenarioResult(
        scenario_id="03016",
        test_type=SuiteTestType.SIMILARITY,
        firmware_version="2.17.3",
        passed=False,
        match_percentage=None,
        num_divergences=0,
        runs_completed=0,
        total_runs=1,
        notes="Match: 0.0%\nDivergences: 0",
        error="No collected events found for 03016 in collected.db. Replay data for this scenario is missing, so the comparison and device CSV export are invalid until that device is collected again.",
        analysis_diagnostics=[
            "Similarity chunks after settle/filtering: total=0, included=0, excluded=0",
            "Timeline rows after removing input-only classes: original=120, new=0",
            "Filtered timelines do not contain enough signal, overlap, transition, preempt, or pedestrian-service rows for detailed summaries.",
        ],
        timeline_difference_analysis_available=False,
    )

    report_path = tmp_path / "report.html"
    generate_report([result, thrown_out, trend_peer, unavailable], suite, str(report_path))

    html = report_path.read_text(encoding="utf-8")
    assert f"Generated {datetime.now():%Y-%m-%d}" in html
    assert f"Generated {datetime.now():%Y-%m-%d} " not in html
    assert "Combined Timeline" in html
    assert "system-wide sanity check" in html
    assert "Phase-call similarity threshold" in html
    assert "Timing match threshold" in html
    assert "90.0% within 0.5s" in html
    assert "Timing DTW" not in html
    assert "90.0%" in html
    assert "Transition / Preempt / Ped Service Differences" in html
    assert "Thrown out" in html
    assert "97.5%" in html
    assert "92.5%" in html
    assert "Avg Timing Match" in html
    assert "Similarity Tests" not in html
    assert "Conflict Tests" not in html
    assert "Timing p95 error:" in html
    assert "0.432s" in html
    assert "Timing max error:" in html
    assert "0.876s" in html
    assert "No meaningful transition, preempt, or pedestrian-service differences were found." in html
    assert "Timeline Difference Analysis" not in html
    assert "Timeline diff analysis:" not in html
    assert "original=120, new=0" not in html
    assert "Device Trends" in html
    assert "Cross-Device Trends" not in html
    assert "Clearance Irregularities" in html
    assert "Preempt 6 Active" in html
    assert "Phase Yellow" in html
    assert "Ped Service" in html
    assert "Overlap Ped Service" in html
    assert "Data Integrity" in html
    assert "Clearance Interval Checks (Valid Timeline Events)" in html
    assert "Irregular Orig" in html
    assert "Irregular New" in html
    assert "Total Dur Orig (s)" in html
    assert "Comparison Details" not in html
    assert "Affected<br>Devices" not in html
    assert "Type</th><th>Avg<br>Delta (s)</th><th>Total Count<br>Delta</th><th>Devices" in html
    assert "Type</th><th>2.15.1<br>Irregular Count</th><th>2.17.3<br>Irregular Count</th><th>&#916; Count</th><th>Devices" in html
    assert "Type</th><th>2.15.1<br>Invalid Count</th><th>2.17.3<br>Invalid Count</th><th>&#916; Count</th><th>Devices" in html
    assert "Example Scenarios" not in html
    assert "Special Device Notes" not in html
    trends_block = html.split("Device Trends", 1)[1].split("Combined Timeline", 1)[0]
    assert "03013, 03015" in trends_block
    assert ">Phase Yellow<" in trends_block
    assert ">Overlap Yellow Occurrences<" in trends_block
    assert ">Ped Service \u2191<" in trends_block
    assert ">Overlap Ped Service \u2191<" in trends_block
    assert ">3<" in trends_block
    assert ">5<" in trends_block
    assert ">-1127.3<" in trends_block
    assert ">+1<" in trends_block
    assert "Phase Red Clearance" in trends_block
    assert "Ph 4 Red Clearance" not in trends_block
    assert "Transition Active" in trends_block
    assert 'class="trend-row trend-yellow"' in trends_block
    assert 'class="trend-row trend-preempt"' in trends_block
    assert 'class="trend-row trend-ped"' in trends_block
    assert 'class="trend-row trend-overlap-ped"' in trends_block
    similarity_results_block = html.split("Similarity Results", 1)[1].split("Device Trends", 1)[0]
    similarity_summary_table = similarity_results_block.split("</table>", 1)[0]
    assert "Sequence Match" in similarity_summary_table
    assert "Timing Match" in similarity_summary_table
    assert "(events within 0.5s)" in similarity_summary_table
    assert "Description" not in similarity_summary_table
    assert "Bar" not in similarity_summary_table
    assert "Divergences" not in similarity_summary_table
    assert '&mdash;' in similarity_results_block
    assert similarity_results_block.count("THROWN OUT") == 2
    assert html.count(">1/2<") >= 1
    assert "Insufficient scored chunks remained after settling/filtering for a reliable comparison." in html
    assert "No collected events found for 03016 in collected.db." in html
