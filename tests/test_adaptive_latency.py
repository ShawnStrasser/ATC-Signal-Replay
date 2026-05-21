from datetime import datetime, timedelta

import pandas as pd

import signal_replay as sr


def test_dynamic_min_samples_scales_by_device_count_and_update_window():
    assert sr.compute_latency_min_samples(12, 5.0) == 2
    assert sr.compute_latency_min_samples(12, 10.0) == 4
    assert sr.compute_latency_min_samples(8, 15.0) == 6
    assert sr.compute_latency_min_samples(12, 5.0, configured_min_samples=7) == 7


def test_sparse_detector_events_keeps_only_isolated_event82_rows():
    df = pd.DataFrame(
        [
            ("S1", "2026-01-01 09:00:00", 82, 1),
            ("S1", "2026-01-01 09:00:05", 82, 1),
            ("S1", "2026-01-01 09:00:20", 82, 1),
            ("S1", "2026-01-01 09:00:30", 82, 1),
            ("S1", "2026-01-01 09:00:41", 82, 1),
            ("S1", "2026-01-01 09:00:10", 82, 2),
        ],
        columns=["device_id", "timestamp", "event_id", "parameter"],
    )

    sparse = sr.sparse_detector_events(df, gap_seconds=10.0)

    assert list(sparse["timestamp"].astype(str)) == [
        "2026-01-01 09:00:10",
        "2026-01-01 09:00:20",
        "2026-01-01 09:00:30",
        "2026-01-01 09:00:41",
    ]


def test_match_sparse_event82_ignores_extra_collected_events_and_measures_latency():
    source = pd.DataFrame(
        [
            ("S1", pd.Timestamp("2026-01-01 09:00:00"), 82, 1),
            ("S1", pd.Timestamp("2026-01-01 09:00:30"), 82, 1),
        ],
        columns=["device_id", "timestamp", "event_id", "parameter"],
    )
    actual = pd.DataFrame(
        [
            ("S1", 1, pd.Timestamp("2026-01-01 09:00:00.300"), 82, 1),
            ("S1", 1, pd.Timestamp("2026-01-01 09:00:10.000"), 82, 99),
            ("S1", 1, pd.Timestamp("2026-01-01 09:00:30.300"), 82, 1),
        ],
        columns=["device_id", "run_number", "timestamp", "event_id", "parameter"],
    )

    matches = sr.match_sparse_event82_latency(source, actual, offset_seconds=0.2)

    assert len(matches) == 2
    assert matches["parameter"].tolist() == [1, 1]
    assert matches["processing_latency_seconds"].round(3).tolist() == [0.5, 0.5]


def test_match_sparse_event82_allows_negative_measured_latency_from_clock_offset():
    source = pd.DataFrame(
        [
            ("S1", pd.Timestamp("2026-01-01 09:00:00.000"), 82, 1),
            ("S1", pd.Timestamp("2026-01-01 09:00:30.000"), 82, 1),
        ],
        columns=["device_id", "timestamp", "event_id", "parameter"],
    )
    actual = pd.DataFrame(
        [
            ("S1", 1, pd.Timestamp("2026-01-01 08:59:59.400"), 82, 1),
            ("S1", 1, pd.Timestamp("2026-01-01 09:00:29.400"), 82, 1),
        ],
        columns=["device_id", "run_number", "timestamp", "event_id", "parameter"],
    )

    matches = sr.match_sparse_event82_latency(source, actual, offset_seconds=0.185)

    assert len(matches) == 2
    assert matches["residual_seconds"].round(3).tolist() == [-0.6, -0.6]
    assert matches["processing_latency_seconds"].round(3).tolist() == [-0.415, -0.415]


def test_adaptive_latency_manager_updates_each_device_independently_with_transition(tmp_path):
    db = sr.DatabaseManager(str(tmp_path / "collected.db"))
    source_s1 = pd.DataFrame(
        [
            ("2026-01-01 09:00:00", 82, 1),
            ("2026-01-01 09:00:30", 82, 1),
            ("2026-01-01 09:01:00", 82, 1),
            ("2026-01-01 09:01:30", 82, 1),
        ],
        columns=["timestamp", "event_id", "parameter"],
    )
    source_s2 = pd.DataFrame(
        [
            ("2026-01-01 09:00:00", 82, 1),
            ("2026-01-01 09:06:00", 82, 1),
            ("2026-01-01 09:06:30", 82, 1),
            ("2026-01-01 09:07:00", 82, 1),
            ("2026-01-01 09:07:30", 82, 1),
        ],
        columns=["timestamp", "event_id", "parameter"],
    )
    db.insert_input_detector_events(source_s1, "S1")
    db.insert_input_detector_events(source_s2, "S2")
    collected_s1 = pd.DataFrame(
        [
            {
                "TimeStamp": pd.Timestamp("2026-01-01 09:00:00.300"),
                "EventTypeID": 82,
                "Parameter": 1,
            },
            {
                "TimeStamp": pd.Timestamp("2026-01-01 09:00:30.300"),
                "EventTypeID": 82,
                "Parameter": 1,
            },
            {
                "TimeStamp": pd.Timestamp("2026-01-01 09:01:00.300"),
                "EventTypeID": 82,
                "Parameter": 1,
            },
            {
                "TimeStamp": pd.Timestamp("2026-01-01 09:01:30.300"),
                "EventTypeID": 82,
                "Parameter": 1,
            },
        ]
    )
    collected_s2 = pd.DataFrame(
        [
            {"TimeStamp": pd.Timestamp("2026-01-01 09:00:00.900"), "EventTypeID": 82, "Parameter": 1},
            {"TimeStamp": pd.Timestamp("2026-01-01 09:06:00.600"), "EventTypeID": 82, "Parameter": 1},
            {"TimeStamp": pd.Timestamp("2026-01-01 09:06:30.600"), "EventTypeID": 82, "Parameter": 1},
            {"TimeStamp": pd.Timestamp("2026-01-01 09:07:00.600"), "EventTypeID": 82, "Parameter": 1},
            {"TimeStamp": pd.Timestamp("2026-01-01 09:07:30.600"), "EventTypeID": 82, "Parameter": 1},
        ]
    )
    db.insert_events(collected_s1, "S1", 1, datetime(2026, 1, 1, 8, 59, 0))
    db.insert_events(collected_s2, "S2", 1, datetime(2026, 1, 1, 8, 59, 0))

    manager = sr.AdaptiveLatencyOffsetManager(
        db_manager=db,
        run_number=1,
        device_ids=["S1", "S2"],
        initial_offset_seconds=0.2,
        lookback_minutes=5.0,
        min_samples=3,
    )
    manager.set_device_date_shift("S1", timedelta(0))
    manager.set_device_date_shift("S2", timedelta(0))

    now = datetime(2026, 1, 1, 9, 5, 0)
    results = manager.update_once(now=now)
    by_device = {result.device_id: result for result in results}

    assert by_device["S1"].applied is True
    assert by_device["S1"].sample_count == 4
    assert round(by_device["S1"].target_offset_seconds, 3) == 0.5
    assert by_device["S2"].applied is False
    assert by_device["S2"].sample_count == 1

    assert round(manager.get_offset("S1", now=now), 3) == 0.2
    assert round(manager.get_offset("S1", now=now + timedelta(seconds=1)), 3) == 0.35
    assert round(manager.get_offset("S1", now=now + timedelta(seconds=2)), 3) == 0.5
    assert round(manager.get_offset("S2", now=now + timedelta(seconds=2)), 3) == 0.2

    second_now = datetime(2026, 1, 1, 9, 10, 0)
    second_results = manager.update_once(now=second_now)
    second_by_device = {result.device_id: result for result in second_results}

    assert second_by_device["S1"].applied is False
    assert second_by_device["S1"].final_offset_seconds == 0.5
    assert second_by_device["S2"].applied is True
    assert second_by_device["S2"].sample_count == 4
    assert round(second_by_device["S2"].target_offset_seconds, 3) == 0.8
    assert round(manager.get_offset("S2", now=second_now + timedelta(seconds=1)), 3) == 0.5
    assert round(manager.get_offset("S2", now=second_now + timedelta(seconds=2)), 3) == 0.8
    assert round(manager.get_offset("S1", now=second_now + timedelta(seconds=2)), 3) == 0.5

    con = db._connect_with_retry()
    try:
        audit = con.execute(
            """
            SELECT device_id, sample_count, required_min_samples, applied, final_offset_seconds
            FROM latency_offset_updates
            ORDER BY updated_at, device_id
            """
        ).fetchall()
        sample_count = con.execute("SELECT COUNT(*) FROM latency_offset_samples").fetchone()[0]
    finally:
        con.close()
    assert audit == [
        ("S1", 4, 3, True, 0.5),
        ("S2", 1, 3, False, 0.2),
        ("S1", 0, 3, False, 0.5),
        ("S2", 4, 3, True, 0.8),
    ]
    assert sample_count == 9


def test_adaptive_latency_manager_skips_when_min_samples_not_met(tmp_path):
    db = sr.DatabaseManager(str(tmp_path / "collected.db"))
    source_rows = pd.DataFrame(
        [("2026-01-01 09:00:00", 82, 1)],
        columns=["timestamp", "event_id", "parameter"],
    )
    db.insert_input_detector_events(source_rows, "S1")
    collected = pd.DataFrame(
        [{"TimeStamp": pd.Timestamp("2026-01-01 09:00:00.300"), "EventTypeID": 82, "Parameter": 1}]
    )
    db.insert_events(collected, "S1", 1, datetime(2026, 1, 1, 8, 59, 0))

    manager = sr.AdaptiveLatencyOffsetManager(
        db_manager=db,
        run_number=1,
        device_ids=["S1"],
        initial_offset_seconds=0.2,
        lookback_minutes=5.0,
        min_samples=2,
    )
    manager.set_device_date_shift("S1", timedelta(0))

    now = datetime(2026, 1, 1, 9, 5, 0)
    result = manager.update_once(now=now)[0]

    assert result.applied is False
    assert result.sample_count == 1
    assert result.final_offset_seconds == 0.2
    assert manager.get_offset("S1", now=now + timedelta(seconds=2)) == 0.2
