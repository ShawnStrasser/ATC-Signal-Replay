from datetime import datetime
import threading

import pandas as pd
import pytest
import requests
from unittest.mock import patch

from signal_replay.collector import DataCollector, check_conflicts


def _sample_df(rows):
    return pd.DataFrame(rows, columns=["TimeStamp", "EventTypeID", "Parameter"])


def test_dedup_uses_timestamp_event_parameter_watermark():
    df_poll_1 = _sample_df([
        [pd.Timestamp("2026-01-01 12:00:00"), 1, 1],
    ])
    # Same timestamp, different event tuple appears on the second poll.
    df_poll_2 = _sample_df([
        [pd.Timestamp("2026-01-01 12:00:00"), 1, 1],
        [pd.Timestamp("2026-01-01 12:00:00"), 10, 1],
    ])

    inserted = []

    class FakeDB:
        def __init__(self, _path):
            pass

        def insert_events(self, df, *_args):
            inserted.append(df.copy())
            return len(df)

        def insert_conflict(self, _conflict):
            return None

    collector = DataCollector(
        db_path="ignored.duckdb",
        device_configs={"d1": (("127.0.0.1", 161), [], 80)},
    )

    with patch("signal_replay.collector.fetch_output_data", side_effect=[df_poll_1, df_poll_2]), patch(
        "signal_replay.collector.DatabaseManager", FakeDB
    ):
        collector.collect_once(1, datetime(2026, 1, 1, 11, 0, 0))
        collector.collect_once(1, datetime(2026, 1, 1, 11, 0, 0))

    assert len(inserted) == 2
    assert inserted[0]["EventTypeID"].tolist() == [1]
    assert inserted[1]["EventTypeID"].tolist() == [10]


def test_insert_failures_do_not_advance_watermark_and_abort_after_three():
    df = _sample_df([
        [pd.Timestamp("2026-01-01 12:00:00"), 1, 1],
    ])
    attempts = {"count": 0}

    class FailingDB:
        def __init__(self, _path):
            pass

        def insert_events(self, *_args):
            attempts["count"] += 1
            raise RuntimeError("simulated lock")

        def insert_conflict(self, _conflict):
            return None

    collector = DataCollector(
        db_path="ignored.duckdb",
        device_configs={"d1": (("127.0.0.1", 161), [], 80)},
    )
    err = threading.Event()

    with patch("signal_replay.collector.fetch_output_data", return_value=df), patch(
        "signal_replay.collector.DatabaseManager", FailingDB
    ):
        collector.collect_once(1, datetime(2026, 1, 1, 11, 0, 0), error_event=err)
        collector.collect_once(1, datetime(2026, 1, 1, 11, 0, 0), error_event=err)
        with pytest.raises(RuntimeError, match="Repeated DB insert failures"):
            collector.collect_once(1, datetime(2026, 1, 1, 11, 0, 0), error_event=err)

    assert attempts["count"] == 3
    assert "d1" not in collector._last_seen_event_key
    assert err.is_set()


def test_http_collection_failures_are_non_fatal_and_logged_once(capsys):
    collector = DataCollector(
        db_path="ignored.duckdb",
        device_configs={"d1": (("127.0.0.1", 161), [], 80)},
    )
    err = threading.Event()
    start_time = datetime(2026, 1, 1, 11, 0, 0)

    with patch(
        "signal_replay.collector.fetch_output_data",
        side_effect=requests.exceptions.ConnectionError("boom"),
    ):
        collector.collect_once(1, start_time, error_event=err)
        collector.collect_once(1, start_time, error_event=err)

    out = capsys.readouterr().out
    assert "*** COLLECTION WARNING:" in out
    assert out.count("*** COLLECTION WARNING:") == 1
    assert not err.is_set()


def test_single_device_fetch_failure_does_not_block_other_devices():
    collector = DataCollector(
        db_path="ignored.duckdb",
        device_configs={
            "bad": (("127.0.0.1", 161), [], 1031),
            "good": (("127.0.0.1", 162), [], 1032),
        },
    )
    inserted = {"rows": 0}

    class FakeDB:
        def __init__(self, _path):
            pass

        def insert_events(self, df, *_args):
            inserted["rows"] += len(df)
            return len(df)

        def insert_conflict(self, _conflict):
            return None

    def fake_fetch(_ip, port, since=None):
        if port == 1031:
            raise requests.exceptions.ConnectionError("boom")
        return _sample_df([[pd.Timestamp("2026-01-01 12:00:00"), 1, 1]])

    with patch("signal_replay.collector.fetch_output_data", side_effect=fake_fetch), patch(
        "signal_replay.collector.DatabaseManager", FakeDB
    ):
        collector.collect_once(1, datetime(2026, 1, 1, 11, 0, 0))

    assert inserted["rows"] == 1


def test_check_conflicts_detects_overlap_ped_without_off_event():
    df = _sample_df([
        [pd.Timestamp("2026-01-01 12:00:00"), 1, 2],
        [pd.Timestamp("2026-01-01 12:00:01"), 67, 17],
    ])

    conflicts = check_conflicts(df, [("Ph2", "OPed17")])

    assert len(conflicts) == 1
    assert conflicts.iloc[0]["Conflict_Details"] == "Ph2 & OPed17"


def test_check_conflicts_distinguishes_overlap_ped_from_standard_ped():
    df = _sample_df([
        [pd.Timestamp("2026-01-01 12:00:00"), 1, 2],
        [pd.Timestamp("2026-01-01 12:00:01"), 67, 17],
    ])

    wrong_pair_conflicts = check_conflicts(df, [("Ph2", "Ped17")])
    correct_pair_conflicts = check_conflicts(df, [("Ph2", "OPed17")])

    assert wrong_pair_conflicts.empty
    assert len(correct_pair_conflicts) == 1


def test_collect_once_does_not_check_conflicts_by_default():
    df_poll_1 = _sample_df([
        [pd.Timestamp("2026-01-01 12:00:00"), 1, 2],
        [pd.Timestamp("2026-01-01 12:00:01"), 67, 17],
    ])

    conflicts_seen = []

    class FakeDB:
        def __init__(self, _path):
            pass

        def insert_events(self, df, *_args):
            return len(df)

        def insert_conflict(self, conflict):
            conflicts_seen.append(conflict)
            return None

    collector = DataCollector(
        db_path="ignored.duckdb",
        device_configs={"d1": (("127.0.0.1", 161), [("Ph2", "OPed17")], 80)},
    )

    with patch("signal_replay.collector.fetch_output_data", return_value=df_poll_1), patch(
        "signal_replay.collector.DatabaseManager", FakeDB
    ):
        collector.collect_once(1, datetime(2026, 1, 1, 11, 0, 0))

    assert len(conflicts_seen) == 0


def test_collect_once_detects_conflict_against_full_run_when_requested():
    df_poll_2 = _sample_df([
        [pd.Timestamp("2026-01-01 12:00:01"), 67, 17],
    ])

    conflicts_seen = []
    accumulated_events = pd.DataFrame([
        {
            "device_id": "d1",
            "run_number": 1,
            "timestamp": pd.Timestamp("2026-01-01 12:00:00"),
            "event_id": 1,
            "parameter": 2,
        },
        {
            "device_id": "d1",
            "run_number": 1,
            "timestamp": pd.Timestamp("2026-01-01 12:00:01"),
            "event_id": 67,
            "parameter": 17,
        },
    ])

    class FakeDB:
        def __init__(self, _path):
            pass

        def insert_events(self, df, *_args):
            return len(df)

        def get_events(self, device_id=None, run_number=None, start_time=None, end_time=None):
            return accumulated_events.copy()

        def insert_conflict(self, conflict):
            conflicts_seen.append(conflict)
            return None

    collector = DataCollector(
        db_path="ignored.duckdb",
        device_configs={"d1": (("127.0.0.1", 161), [("Ph2", "OPed17")], 80)},
    )

    with patch("signal_replay.collector.fetch_output_data", return_value=df_poll_2), patch(
        "signal_replay.collector.DatabaseManager", FakeDB
    ):
        collector.collect_once(
            1,
            datetime(2026, 1, 1, 11, 0, 0),
            detect_conflicts=True,
        )

    assert len(conflicts_seen) == 1
    assert conflicts_seen[0].conflict_details == "Ph2 & OPed17"
