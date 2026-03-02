from datetime import datetime
import threading

import pandas as pd
import pytest
import requests
from unittest.mock import patch

from signal_replay.collector import DataCollector


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

    def fake_fetch(_ip, port):
        if port == 1031:
            raise requests.exceptions.ConnectionError("boom")
        return _sample_df([[pd.Timestamp("2026-01-01 12:00:00"), 1, 1]])

    with patch("signal_replay.collector.fetch_output_data", side_effect=fake_fetch), patch(
        "signal_replay.collector.DatabaseManager", FakeDB
    ):
        collector.collect_once(1, datetime(2026, 1, 1, 11, 0, 0))

    assert inserted["rows"] == 1
