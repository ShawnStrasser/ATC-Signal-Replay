"""Adaptive detector replay latency calibration."""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


DETECTOR_LATENCY_EVENT_ID = 82
DEFAULT_SPARSE_GAP_SECONDS = 10.0
DEFAULT_MATCH_TOLERANCE_SECONDS = 2.5
DEFAULT_OFFSET_TRANSITION_SECONDS = 2.0


def compute_latency_min_samples(
    device_count: int,
    lookback_minutes: float,
    configured_min_samples: Optional[int] = None,
) -> int:
    """Return the configured or per-device minimum sample count.

    ``device_count`` is retained for compatibility with the previous public
    signature, but adaptive latency is now calibrated independently per device.
    """
    if configured_min_samples is not None:
        return int(configured_min_samples)
    return max(1, 2 * math.ceil(float(lookback_minutes) / 5.0))


def sparse_detector_events(df: pd.DataFrame, gap_seconds: float = DEFAULT_SPARSE_GAP_SECONDS) -> pd.DataFrame:
    """Keep detector events isolated from neighboring same-device/same-detector events."""
    if df.empty:
        return df.copy()

    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"])
    out = out.sort_values(["device_id", "event_id", "parameter", "timestamp"]).reset_index(drop=True)

    parts = []
    for _, group in out.groupby(["device_id", "event_id", "parameter"], sort=False):
        group = group.sort_values("timestamp")
        prev_gap = group["timestamp"].diff().dt.total_seconds().fillna(np.inf)
        next_gap = group["timestamp"].shift(-1).sub(group["timestamp"]).dt.total_seconds().abs().fillna(np.inf)
        parts.append(group[(prev_gap >= gap_seconds) & (next_gap >= gap_seconds)])

    if not parts:
        return out.iloc[0:0].copy()
    return pd.concat(parts, ignore_index=True).sort_values(["device_id", "timestamp"]).reset_index(drop=True)


def match_sparse_event82_latency(
    source: pd.DataFrame,
    actual: pd.DataFrame,
    *,
    offset_seconds: float,
    tolerance_seconds: float = DEFAULT_MATCH_TOLERANCE_SECONDS,
) -> pd.DataFrame:
    """Match sparse source event 82 rows to collected event 82 rows."""
    columns = [
        "device_id",
        "event_id",
        "parameter",
        "source_timestamp",
        "collected_timestamp",
        "processing_latency_seconds",
        "residual_seconds",
        "match_delta_seconds",
    ]
    if source.empty or actual.empty:
        return pd.DataFrame(columns=columns)

    src = source.copy()
    act = actual.copy()
    src["timestamp"] = pd.to_datetime(src["timestamp"])
    act["timestamp"] = pd.to_datetime(act["timestamp"])
    src = src[src["event_id"] == DETECTOR_LATENCY_EVENT_ID]
    act = act[act["event_id"] == DETECTOR_LATENCY_EVENT_ID]

    rows = []
    for key, source_group in src.groupby(["device_id", "parameter"], sort=False):
        actual_group = act[
            (act["device_id"] == key[0])
            & (act["parameter"] == key[1])
        ].sort_values("timestamp")
        if actual_group.empty:
            continue

        actual_values = actual_group["timestamp"].to_list()
        used = np.zeros(len(actual_values), dtype=bool)

        for source_ts in source_group.sort_values("timestamp")["timestamp"]:
            deltas = [
                abs((actual_ts - source_ts).total_seconds())
                if not used[idx]
                else float("inf")
                for idx, actual_ts in enumerate(actual_values)
            ]
            match_idx = int(np.argmin(deltas)) if deltas else -1
            if match_idx < 0 or deltas[match_idx] > tolerance_seconds:
                continue

            used[match_idx] = True
            collected_ts = actual_values[match_idx]
            scheduled_send_ts = source_ts - pd.Timedelta(seconds=float(offset_seconds))
            processing_latency = (collected_ts - scheduled_send_ts).total_seconds()
            residual = (collected_ts - source_ts).total_seconds()

            rows.append(
                {
                    "device_id": str(key[0]),
                    "event_id": DETECTOR_LATENCY_EVENT_ID,
                    "parameter": int(key[1]),
                    "source_timestamp": source_ts.to_pydatetime(),
                    "collected_timestamp": collected_ts.to_pydatetime(),
                    "processing_latency_seconds": processing_latency,
                    "residual_seconds": residual,
                    "match_delta_seconds": abs(residual),
                }
            )

    return pd.DataFrame(rows, columns=columns)


@dataclass
class LatencyUpdateResult:
    update_id: str
    device_id: str
    updated_at: datetime
    window_start: datetime
    window_end: datetime
    device_count: int
    sample_count: int
    required_min_samples: int
    previous_offset_seconds: float
    measured_median_latency_seconds: Optional[float]
    target_offset_seconds: float
    final_offset_seconds: float
    transition_start: Optional[datetime]
    transition_end: Optional[datetime]
    applied: bool
    status: str
    reason: str
    latency_p05_seconds: Optional[float] = None
    latency_p25_seconds: Optional[float] = None
    latency_p50_seconds: Optional[float] = None
    latency_p75_seconds: Optional[float] = None
    latency_p95_seconds: Optional[float] = None


@dataclass
class _DeviceLatencyState:
    start_offset_seconds: float
    target_offset_seconds: float
    transition_start: Optional[datetime] = None
    transition_end: Optional[datetime] = None


class AdaptiveLatencyOffsetManager:
    """Per-device latency offsets calibrated from collected event 82 rows."""

    def __init__(
        self,
        *,
        db_manager,
        run_number: int,
        device_ids: List[str],
        initial_offset_seconds: float,
        lookback_minutes: float,
        min_samples: Optional[int] = None,
        match_tolerance_seconds: float = DEFAULT_MATCH_TOLERANCE_SECONDS,
        sparse_gap_seconds: float = DEFAULT_SPARSE_GAP_SECONDS,
        transition_seconds: float = DEFAULT_OFFSET_TRANSITION_SECONDS,
        debug: bool = False,
    ) -> None:
        self.db = db_manager
        self.run_number = int(run_number)
        self.device_ids = [str(device_id) for device_id in device_ids]
        self.device_count = len(self.device_ids)
        self.initial_offset_seconds = float(initial_offset_seconds)
        self.lookback_seconds = float(lookback_minutes) * 60.0
        self.required_min_samples = compute_latency_min_samples(
            self.device_count,
            float(lookback_minutes),
            min_samples,
        )
        self.match_tolerance_seconds = float(match_tolerance_seconds)
        self.sparse_gap_seconds = float(sparse_gap_seconds)
        self.transition_seconds = max(0.0, float(transition_seconds))
        self.debug = debug

        self._lock = threading.Lock()
        self._states: Dict[str, _DeviceLatencyState] = {
            device_id: _DeviceLatencyState(
                start_offset_seconds=self.initial_offset_seconds,
                target_offset_seconds=self.initial_offset_seconds,
            )
            for device_id in self.device_ids
        }
        self._date_shifts: Dict[str, timedelta] = {}

    @staticmethod
    def _offset_at(state: _DeviceLatencyState, now: datetime) -> float:
        if state.transition_start is None or state.transition_end is None:
            return float(state.target_offset_seconds)
        if now >= state.transition_end:
            return float(state.target_offset_seconds)
        if now <= state.transition_start:
            return float(state.start_offset_seconds)

        duration = (state.transition_end - state.transition_start).total_seconds()
        if duration <= 0:
            return float(state.target_offset_seconds)
        elapsed = (now - state.transition_start).total_seconds()
        fraction = min(1.0, max(0.0, elapsed / duration))
        return float(
            state.start_offset_seconds
            + (state.target_offset_seconds - state.start_offset_seconds) * fraction
        )

    def _get_or_create_state_locked(self, device_id: str) -> _DeviceLatencyState:
        device_key = str(device_id)
        if device_key not in self._states:
            self._states[device_key] = _DeviceLatencyState(
                start_offset_seconds=self.initial_offset_seconds,
                target_offset_seconds=self.initial_offset_seconds,
            )
        return self._states[device_key]

    def get_offset(self, device_id: Optional[str] = None, now: Optional[datetime] = None) -> float:
        now = now or datetime.now()
        with self._lock:
            if device_id is None:
                device_id = self.device_ids[0] if self.device_ids else ""
            state = self._get_or_create_state_locked(str(device_id))
            return self._offset_at(state, now)

    def set_device_date_shift(self, device_id: str, date_shift: timedelta) -> None:
        with self._lock:
            self._date_shifts[str(device_id)] = date_shift

    def _start_transition(
        self,
        *,
        device_id: str,
        target_offset: float,
        now: datetime,
    ) -> tuple[float, datetime, datetime]:
        with self._lock:
            state = self._get_or_create_state_locked(device_id)
            current_offset = self._offset_at(state, now)
            transition_start = now
            transition_end = now + timedelta(seconds=self.transition_seconds)
            if self.transition_seconds <= 0:
                transition_end = transition_start
                state.start_offset_seconds = float(target_offset)
                state.target_offset_seconds = float(target_offset)
                state.transition_start = None
                state.transition_end = None
            else:
                state.start_offset_seconds = float(current_offset)
                state.target_offset_seconds = float(target_offset)
                state.transition_start = transition_start
                state.transition_end = transition_end
            return current_offset, transition_start, transition_end

    def _make_result(
        self,
        *,
        device_id: str,
        now: datetime,
        window_start: datetime,
        window_end: datetime,
        sample_count: int,
        previous_offset: float,
        measured_median: Optional[float],
        target_offset: float,
        transition_start: Optional[datetime],
        transition_end: Optional[datetime],
        applied: bool,
        status: str,
        reason: str,
        matches: Optional[pd.DataFrame] = None,
    ) -> LatencyUpdateResult:
        quantiles: Dict[str, Optional[float]] = {
            "latency_p05_seconds": None,
            "latency_p25_seconds": None,
            "latency_p50_seconds": None,
            "latency_p75_seconds": None,
            "latency_p95_seconds": None,
        }
        if matches is not None and not matches.empty:
            latency = matches["processing_latency_seconds"]
            quantiles = {
                "latency_p05_seconds": float(latency.quantile(0.05)),
                "latency_p25_seconds": float(latency.quantile(0.25)),
                "latency_p50_seconds": float(latency.quantile(0.50)),
                "latency_p75_seconds": float(latency.quantile(0.75)),
                "latency_p95_seconds": float(latency.quantile(0.95)),
            }

        return LatencyUpdateResult(
            update_id=f"{self.run_number}:{device_id}:{now.isoformat(timespec='microseconds')}",
            device_id=device_id,
            updated_at=now,
            window_start=window_start,
            window_end=window_end,
            device_count=self.device_count,
            sample_count=sample_count,
            required_min_samples=self.required_min_samples,
            previous_offset_seconds=float(previous_offset),
            measured_median_latency_seconds=measured_median,
            target_offset_seconds=float(target_offset),
            final_offset_seconds=float(target_offset),
            transition_start=transition_start,
            transition_end=transition_end,
            applied=applied,
            status=status,
            reason=reason,
            **quantiles,
        )

    def _update_device_once(
        self,
        *,
        device_id: str,
        now: datetime,
        window_start: datetime,
        window_end: datetime,
        source_all: pd.DataFrame,
        actual_all: pd.DataFrame,
    ) -> LatencyUpdateResult:
        with self._lock:
            previous_offset = self._offset_at(
                self._get_or_create_state_locked(device_id),
                now,
            )
            date_shift = self._date_shifts.get(device_id)

        if date_shift is None:
            result = self._make_result(
                device_id=device_id,
                now=now,
                window_start=window_start,
                window_end=window_end,
                sample_count=0,
                previous_offset=previous_offset,
                measured_median=None,
                target_offset=previous_offset,
                transition_start=None,
                transition_end=None,
                applied=False,
                status="skipped",
                reason="missing date shift",
            )
            self.db.insert_latency_offset_update(self.run_number, result)
            return result

        source = source_all[source_all["device_id"].astype(str) == device_id].copy()
        if source.empty:
            result = self._make_result(
                device_id=device_id,
                now=now,
                window_start=window_start,
                window_end=window_end,
                sample_count=0,
                previous_offset=previous_offset,
                measured_median=None,
                target_offset=previous_offset,
                transition_start=None,
                transition_end=None,
                applied=False,
                status="skipped",
                reason="no input detector events",
            )
            self.db.insert_latency_offset_update(self.run_number, result)
            return result

        source = sparse_detector_events(source, gap_seconds=self.sparse_gap_seconds)
        if source.empty:
            result = self._make_result(
                device_id=device_id,
                now=now,
                window_start=window_start,
                window_end=window_end,
                sample_count=0,
                previous_offset=previous_offset,
                measured_median=None,
                target_offset=previous_offset,
                transition_start=None,
                transition_end=None,
                applied=False,
                status="skipped",
                reason="no sparse input detector events",
            )
            self.db.insert_latency_offset_update(self.run_number, result)
            return result

        source["timestamp"] = pd.to_datetime(source["timestamp"]) + date_shift
        source = source[
            (source["timestamp"] >= pd.Timestamp(window_start))
            & (source["timestamp"] <= pd.Timestamp(window_end))
        ].copy()
        if source.empty:
            result = self._make_result(
                device_id=device_id,
                now=now,
                window_start=window_start,
                window_end=window_end,
                sample_count=0,
                previous_offset=previous_offset,
                measured_median=None,
                target_offset=previous_offset,
                transition_start=None,
                transition_end=None,
                applied=False,
                status="skipped",
                reason="no input detector events in update window",
            )
            self.db.insert_latency_offset_update(self.run_number, result)
            return result

        actual = actual_all[actual_all["device_id"].astype(str) == device_id].copy()
        matches = match_sparse_event82_latency(
            source,
            actual,
            offset_seconds=previous_offset,
            tolerance_seconds=self.match_tolerance_seconds,
        )
        sample_count = len(matches)
        median_latency = (
            float(matches["processing_latency_seconds"].median())
            if sample_count > 0
            else None
        )

        applied = sample_count >= self.required_min_samples and median_latency is not None
        if applied:
            transition_previous, transition_start, transition_end = self._start_transition(
                device_id=device_id,
                target_offset=median_latency,
                now=now,
            )
            previous_offset = transition_previous
            target_offset = median_latency
            status = "applied"
            reason = ""
        else:
            transition_start = None
            transition_end = None
            target_offset = previous_offset
            status = "skipped"
            reason = "insufficient samples"

        result = self._make_result(
            device_id=device_id,
            now=now,
            window_start=window_start,
            window_end=window_end,
            sample_count=sample_count,
            previous_offset=previous_offset,
            measured_median=median_latency,
            target_offset=target_offset,
            transition_start=transition_start,
            transition_end=transition_end,
            applied=applied,
            status=status,
            reason=reason,
            matches=matches,
        )
        self.db.insert_latency_offset_update(self.run_number, result, samples=matches)
        return result

    def update_once(self, now: Optional[datetime] = None) -> List[LatencyUpdateResult]:
        now = now or datetime.now()
        window_end = now
        window_start = window_end - timedelta(seconds=self.lookback_seconds)

        source_all = self.db.get_input_detector_events(device_ids=self.device_ids)
        actual_all = self.db.get_events(
            run_number=self.run_number,
            start_time=window_start - timedelta(seconds=self.match_tolerance_seconds),
            end_time=window_end + timedelta(seconds=self.match_tolerance_seconds),
        )
        if not actual_all.empty:
            actual_all = actual_all[
                (actual_all["device_id"].astype(str).isin(self.device_ids))
                & (actual_all["event_id"] == DETECTOR_LATENCY_EVENT_ID)
                & (actual_all["parameter"] < 65)
            ].copy()

        results = []
        for device_id in self.device_ids:
            try:
                result = self._update_device_once(
                    device_id=device_id,
                    now=now,
                    window_start=window_start,
                    window_end=window_end,
                    source_all=source_all,
                    actual_all=actual_all,
                )
            except Exception as exc:
                previous_offset = self.get_offset(device_id, now=now)
                result = self._make_result(
                    device_id=device_id,
                    now=now,
                    window_start=window_start,
                    window_end=window_end,
                    sample_count=0,
                    previous_offset=previous_offset,
                    measured_median=None,
                    target_offset=previous_offset,
                    transition_start=None,
                    transition_end=None,
                    applied=False,
                    status="error",
                    reason=str(exc),
                )
                self.db.insert_latency_offset_update(self.run_number, result)
            results.append(result)

        if self.debug:
            applied = [result for result in results if result.applied]
            skipped = [result for result in results if not result.applied]
            print(
                "[latency] "
                f"applied={len(applied)} skipped={len(skipped)} "
                f"required_samples={self.required_min_samples}",
                flush=True,
            )
        return results

    def update_after_poll(self, now: Optional[datetime] = None) -> List[LatencyUpdateResult]:
        """Run one update after a collector poll, logging errors without raising."""
        now = now or datetime.now()
        try:
            return self.update_once(now=now)
        except Exception as exc:
            window_start = now - timedelta(seconds=self.lookback_seconds)
            results = []
            for device_id in self.device_ids:
                current_offset = self.get_offset(device_id, now=now)
                result = self._make_result(
                    device_id=device_id,
                    now=now,
                    window_start=window_start,
                    window_end=now,
                    sample_count=0,
                    previous_offset=current_offset,
                    measured_median=None,
                    target_offset=current_offset,
                    transition_start=None,
                    transition_end=None,
                    applied=False,
                    status="error",
                    reason=str(exc),
                )
                self.db.insert_latency_offset_update(self.run_number, result)
                results.append(result)
            if self.debug:
                print(f"[latency] adaptive update failed: {exc}", flush=True)
            return results
