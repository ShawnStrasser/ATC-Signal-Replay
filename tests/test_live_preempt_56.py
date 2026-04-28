"""
Live emulator tests for preempt 5/6 replay and collection.

These tests are intended to diagnose the suspected preempt-6 round-trip issue
seen in firmware-validation results. They:

1. Replay short synthetic preempt-only sequences to localhost emulators
2. Collect the controller event log back over HTTP
3. Compare the collected preempt 102/104 events against what was sent
4. Generate an operational preempt timeline diff for clearer failure output

By default the tests target:
- 2B045 on localhost ports 9701-9704
- 13010 on localhost ports 9705-9708

Override with environment variables if needed:
- PREEMPT_TEST_HOST
- PREEMPT_2B045_PORTS
- PREEMPT_13010_PORTS
- PREEMPT_TEST_SETTLE_SECONDS
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

import pandas as pd
import pytest

import signal_replay as sr


pytestmark = pytest.mark.live_device

PREEMPT_ON = 102
PREEMPT_OFF = 104
PREEMPT_PARAMS = (5, 6)
DEFAULT_SETTLE_SECONDS = float(os.environ.get("PREEMPT_TEST_SETTLE_SECONDS", "0.5"))
DEFAULT_POLL_SECONDS = float(os.environ.get("PREEMPT_TEST_POLL_SECONDS", "20"))
DEFAULT_POLL_INTERVAL_SECONDS = float(
    os.environ.get("PREEMPT_TEST_POLL_INTERVAL_SECONDS", "1.0")
)


@dataclass(frozen=True)
class EmulatorTarget:
    device_id: str
    host: str
    port: int

    @property
    def ip_port(self) -> tuple[str, int]:
        return (self.host, self.port)


@dataclass(frozen=True)
class ReplayResult:
    filtered_events: pd.DataFrame
    diagnostic_events: pd.DataFrame


def _parse_ports(env_name: str, default: list[int]) -> list[int]:
    raw = os.environ.get(env_name, "").strip()
    if not raw:
        return default
    return [int(part.strip()) for part in raw.split(",") if part.strip()]


_HOST = os.environ.get("PREEMPT_TEST_HOST", "127.0.0.1")
_TARGETS = [
    *[
        EmulatorTarget("2B045", _HOST, port)
        for port in _parse_ports("PREEMPT_2B045_PORTS", [9701, 9702, 9703, 9704])
    ],
    *[
        EmulatorTarget("13010", _HOST, port)
        for port in _parse_ports("PREEMPT_13010_PORTS", [9705, 9706, 9707, 9708])
    ],
]


PREEMPT_SCENARIOS = [
    pytest.param(
        "preempt_5_only",
        [
            (0.0, PREEMPT_ON, 5),
            (4.0, PREEMPT_OFF, 5),
        ],
        id="5_only",
    ),
    pytest.param(
        "preempt_6_only",
        [
            (0.0, PREEMPT_ON, 6),
            (4.0, PREEMPT_OFF, 6),
        ],
        id="6_only",
    ),
    pytest.param(
        "preempt_5_then_6_overlap",
        [
            (0.0, PREEMPT_ON, 5),
            (1.0, PREEMPT_ON, 6),
            (4.0, PREEMPT_OFF, 5),
            (5.0, PREEMPT_OFF, 6),
        ],
        id="5_then_6",
    ),
    pytest.param(
        "preempt_6_then_5_overlap",
        [
            (0.0, PREEMPT_ON, 6),
            (1.0, PREEMPT_ON, 5),
            (4.0, PREEMPT_OFF, 6),
            (5.0, PREEMPT_OFF, 5),
        ],
        id="6_then_5",
    ),
]


def _target_id(target: EmulatorTarget) -> str:
    return f"{target.device_id}_{target.port}"


def _make_preempt_events(
    offsets: Iterable[tuple[float, int, int]],
    *,
    device_id: str,
) -> pd.DataFrame:
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    rows = [
        {
            "timestamp": base_time + timedelta(seconds=offset_seconds),
            "event_id": event_id,
            "parameter": parameter,
            "device_id": device_id,
        }
        for offset_seconds, event_id, parameter in offsets
    ]
    return pd.DataFrame(rows)


def _normalize_preempt_events(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    col_map = {}
    for col in normalized.columns:
        col_lower = col.lower()
        if col_lower in ("timestamp", "time_stamp"):
            col_map[col] = "timestamp"
        elif col_lower in ("event_id", "eventid", "eventtypeid"):
            col_map[col] = "event_id"
        elif col_lower in ("parameter", "param"):
            col_map[col] = "parameter"
        elif col_lower in ("deviceid", "device_id"):
            col_map[col] = "device_id"

    normalized = normalized.rename(columns=col_map)
    if "timestamp" not in normalized.columns:
        raise ValueError(f"Missing timestamp column in {normalized.columns.tolist()}")
    if "event_id" not in normalized.columns or "parameter" not in normalized.columns:
        raise ValueError(f"Missing event_id/parameter columns in {normalized.columns.tolist()}")

    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"])
    normalized["event_id"] = normalized["event_id"].astype(int)
    normalized["parameter"] = normalized["parameter"].astype(int)

    filtered = normalized[
        normalized["event_id"].isin((PREEMPT_ON, PREEMPT_OFF))
        & normalized["parameter"].isin(PREEMPT_PARAMS)
    ].copy()
    return filtered.sort_values(["timestamp", "event_id", "parameter"]).reset_index(drop=True)


def _normalize_preempt_diagnostic_events(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    col_map = {}
    for col in normalized.columns:
        col_lower = col.lower()
        if col_lower in ("timestamp", "time_stamp"):
            col_map[col] = "timestamp"
        elif col_lower in ("event_id", "eventid", "eventtypeid"):
            col_map[col] = "event_id"
        elif col_lower in ("parameter", "param"):
            col_map[col] = "parameter"

    normalized = normalized.rename(columns=col_map)
    if "timestamp" not in normalized.columns:
        return pd.DataFrame(columns=["timestamp", "event_id", "parameter"])
    if "event_id" not in normalized.columns or "parameter" not in normalized.columns:
        return pd.DataFrame(columns=["timestamp", "event_id", "parameter"])

    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"])
    normalized["event_id"] = normalized["event_id"].astype(int)
    normalized["parameter"] = normalized["parameter"].astype(int)

    filtered = normalized[
        normalized["event_id"].isin((PREEMPT_ON, PREEMPT_OFF, 105, 111))
        & normalized["parameter"].isin(PREEMPT_PARAMS)
    ].copy()
    return filtered.sort_values(["timestamp", "event_id", "parameter"]).reset_index(drop=True)


def _count_map(df: pd.DataFrame) -> dict[tuple[int, int], int]:
    if df.empty:
        return {}
    grouped = (
        df.groupby(["event_id", "parameter"])
        .size()
        .reset_index(name="count")
        .sort_values(["event_id", "parameter"])
    )
    return {
        (int(row.event_id), int(row.parameter)): int(row.count)
        for row in grouped.itertuples(index=False)
    }


def _expected_count_map(offsets: Iterable[tuple[float, int, int]]) -> dict[tuple[int, int], int]:
    counts: dict[tuple[int, int], int] = {}
    for _offset_seconds, event_id, parameter in offsets:
        if event_id not in (PREEMPT_ON, PREEMPT_OFF) or parameter not in PREEMPT_PARAMS:
            continue
        key = (int(event_id), int(parameter))
        counts[key] = counts.get(key, 0) + 1
    return counts


def _preempt_differences(expected: pd.DataFrame, actual: pd.DataFrame) -> list[dict]:
    timeline_expected = sr.generate_timeline(expected, device_id="expected")
    timeline_actual = sr.generate_timeline(actual, device_id="actual")
    diffs = sr.generate_operational_difference_summary(
        timeline_expected,
        timeline_actual,
        tolerance_seconds=2.0,
    )
    return [diff for diff in diffs if diff["label"] in {"Preempt 5", "Preempt 6"}]


def _relative_events(df: pd.DataFrame) -> str:
    if df.empty:
        return "<no preempt events>"
    rel = df.copy()
    base = rel["timestamp"].min()
    rel["t_rel_s"] = (rel["timestamp"] - base).dt.total_seconds().round(3)
    return rel[["t_rel_s", "event_id", "parameter"]].to_string(index=False)


def _relative_diagnostic_events(df: pd.DataFrame) -> str:
    if df.empty:
        return "<no preempt diagnostic events>"
    rel = df.copy()
    base = rel["timestamp"].min()
    rel["t_rel_s"] = (rel["timestamp"] - base).dt.total_seconds().round(3)
    return rel[["t_rel_s", "event_id", "parameter"]].to_string(index=False)


def _format_diffs(diffs: list[dict]) -> str:
    if not diffs:
        return "<no preempt timeline diffs>"
    rows = []
    for diff in diffs:
        rows.append(
            (
                f"{diff['label']} {diff['state']}: "
                f"count {diff['count_a']}->{diff['count_b']} "
                f"(delta {diff['count_delta']:+d}), "
                f"avg {diff['duration_a']:.3f}s->{diff['duration_b']:.3f}s "
                f"(delta {diff['duration_delta']:+.3f}s)"
            )
        )
    return "\n".join(rows)


def _run_preempt_replay(
    target: EmulatorTarget,
    expected_events: pd.DataFrame,
    *,
    expected_counts: dict[tuple[int, int], int],
) -> ReplayResult:
    signal = sr.SignalConfig(
        device_id=target.device_id,
        ip=target.host,
        udp_port=target.port,
        http_port=target.port,
        cycle_length=0,
        incompatible_pairs=[],
        replay_latency_offset_seconds=0.0,
    )
    signal.events = expected_events

    replay = sr.SignalReplay(signal, simulation_speed=1.0, debug=False)
    start_time = replay.run()
    time.sleep(DEFAULT_SETTLE_SECONDS)
    since = start_time - timedelta(seconds=1)
    deadline = time.monotonic() + DEFAULT_POLL_SECONDS
    latest = pd.DataFrame(columns=["timestamp", "event_id", "parameter"])
    latest_diagnostic = pd.DataFrame(columns=["timestamp", "event_id", "parameter"])

    while True:
        raw = sr.fetch_output_data(
            target.host,
            target.port,
            since=since,
            request_timeout_seconds=15.0,
        )
        latest = _normalize_preempt_events(raw)
        latest_diagnostic = _normalize_preempt_diagnostic_events(raw)
        if _count_map(latest) == expected_counts:
            return ReplayResult(latest, latest_diagnostic)
        if time.monotonic() >= deadline:
            return ReplayResult(latest, latest_diagnostic)
        time.sleep(DEFAULT_POLL_INTERVAL_SECONDS)


@pytest.fixture(params=_TARGETS, ids=_target_id)
def emulator_target(request) -> EmulatorTarget:
    target: EmulatorTarget = request.param
    try:
        sr.send_ntcip(
            target.ip_port,
            detector_group=1,
            state_integer=0,
            detector_type="Vehicle",
            timeout=1.0,
        )
    except Exception as exc:  # pragma: no cover - live skip path
        pytest.skip(f"Emulator target {target.host}:{target.port} not reachable: {exc}")
    return target


@pytest.fixture(autouse=True)
def reset_emulator(emulator_target: EmulatorTarget):
    sr.reset_all_detectors(emulator_target.ip_port, timeout=1.0)
    time.sleep(0.25)
    yield
    try:
        sr.reset_all_detectors(emulator_target.ip_port, timeout=1.0)
        time.sleep(0.25)
    except Exception as exc:  # pragma: no cover - live cleanup path
        print(f"Warning: cleanup reset failed for {emulator_target}: {exc}")


class TestLivePreemptFiveSix:
    @pytest.mark.parametrize("scenario_name, offsets", PREEMPT_SCENARIOS)
    def test_preempt_sequences_round_trip(
        self,
        emulator_target: EmulatorTarget,
        scenario_name: str,
        offsets: list[tuple[float, int, int]],
    ):
        expected = _make_preempt_events(offsets, device_id=emulator_target.device_id)
        expected_preempt = _normalize_preempt_events(expected)
        expected_counts = _expected_count_map(offsets)
        result = _run_preempt_replay(
            emulator_target,
            expected,
            expected_counts=expected_counts,
        )
        actual_preempt = result.filtered_events
        diagnostic_preempt = result.diagnostic_events
        actual_counts = _count_map(actual_preempt)
        preempt_diffs = _preempt_differences(expected_preempt, actual_preempt)

        assert actual_counts == expected_counts, (
            f"{scenario_name} on {emulator_target.device_id}:{emulator_target.port} "
            f"recorded different preempt counts.\n"
            f"Expected counts: {expected_counts}\n"
            f"Actual counts:   {actual_counts}\n\n"
            f"Expected events:\n{_relative_events(expected_preempt)}\n\n"
            f"Actual events:\n{_relative_events(actual_preempt)}\n\n"
            f"Diagnostic events:\n{_relative_diagnostic_events(diagnostic_preempt)}\n\n"
            f"Preempt timeline diffs:\n{_format_diffs(preempt_diffs)}"
        )

        assert not preempt_diffs, (
            f"{scenario_name} on {emulator_target.device_id}:{emulator_target.port} "
            f"produced preempt timeline differences.\n"
            f"Expected events:\n{_relative_events(expected_preempt)}\n\n"
            f"Actual events:\n{_relative_events(actual_preempt)}\n\n"
            f"Diagnostic events:\n{_relative_diagnostic_events(diagnostic_preempt)}\n\n"
            f"Preempt timeline diffs:\n{_format_diffs(preempt_diffs)}"
        )
