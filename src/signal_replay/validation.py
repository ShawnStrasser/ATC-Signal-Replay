"""
Software validation comparison: baseline software output vs candidate output.

:func:`compare_validation` is the single implementation used by the
package (:func:`~signal_replay.compare_software`), by
``software_validation/software_validate.py`` and by applications. It takes
explicit inputs (no checkpoint files):

* **Similarity scenarios**: the candidate's output events are compared with
  the baseline's using DTW on event groups (:func:`compare_runs`), with the
  settle period, the manual time-of-day analysis window and the phase-call
  similarity threshold applied. Passing needs a sequence match of at least
  ``sequence_match_threshold`` and a timing match (events within 0.5 s) of
  at least ``timing_match_threshold``. Phase, clearance and operational
  differences, chunk scores, a sparkline and issue plots are produced.
* **Conflict scenarios**: conflicts are recomputed from the stored events of
  every run. The scenario passes when the baseline reproduced the configured
  conflict and the candidate never did.

Inputs for ``baseline`` and ``candidate`` (see :data:`EventsInput`):

* a path to a collected DuckDB file (``events`` table, filtered by
  ``device_id == scenario_id``);
* a path to a parquet / CSV / other event log readable by
  :func:`~signal_replay.load_events` (rows for the scenario are kept when it
  has a ``device_id`` column; otherwise the whole file is that scenario's);
* a DataFrame (same rule);
* a mapping ``{scenario_id: one of the above}``, or a callable
  ``scenario_id -> DataFrame``;
* ``('db', path)`` / ``('file', path)`` to say explicitly whether a path is
  a collected database or a source log.

Events are loaded in the calling process through read-only DuckDB
connections (one per file, closed in ``finally``); worker processes only
receive DataFrames, so several scenarios sharing one database never lock
each other out.
"""

from __future__ import annotations

import contextlib
import logging
import os
import threading
from dataclasses import dataclass, fields, replace
from datetime import datetime, time as dt_time
from concurrent.futures import FIRST_COMPLETED, wait as futures_wait
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

import duckdb
import pandas as pd

from .collector import check_conflicts
from .comparison import (
    ComparisonResult,
    clip_timeline_to_relative_periods,
    compare_runs,
    compute_timeline_offset,
    create_comparison_gantt_matplotlib,
    create_multi_divergence_plots,
    cross_invalidate_timelines,
    generate_clearance_irregularity_summary,
    generate_operational_difference_summary,
    generate_phase_difference_summary,
    generate_timeline,
    is_overlap_clearance_interval_candidate,
    load_events,
    render_sparkline_svg,
    timeline_overlaps_interval,
)
from .progress import ProgressCallback, Stage, as_reporter
from .test_suite import ScenarioResult, SoftwareTestSuite, TestScenario, TestType

logger = logging.getLogger(__name__)

__all__ = [
    "ValidationSettings",
    "EventsInput",
    "compare_validation",
    "load_collected_events",
    "load_coord_split_schedules",
]

#: Anything :func:`compare_validation` accepts as ``baseline``/``candidate``.
EventsInput = Union[
    str,
    os.PathLike,
    pd.DataFrame,
    Tuple[str, Union[str, os.PathLike]],
    Mapping[str, Any],
    Callable[[str], pd.DataFrame],
]


class _ValidationCancelled(Exception):
    """Internal: raised when the stop event is set (re-raised as OperationCancelled)."""


@dataclass
class ValidationSettings:
    """Analysis settings for :func:`compare_validation`.

    Attributes:
        settle_minutes: Minutes at the start of each similarity comparison
            that are not scored. Replaced by ``analysis_start_time`` for
            time-of-day aligned scenarios.
        analysis_start_time / analysis_end_time: Optional ``HH:MM[:SS]``
            window (on the collected run's date) used for time-of-day
            aligned scenarios.
        group_tolerance: Seconds within which events count as one group.
        phase_call_threshold: Minimum phase-call input similarity (%) for a
            chunk to count toward the match score.
        sequence_match_threshold: Minimum sequence match (%) to pass.
        timing_match_threshold: Minimum timing match (%) to pass.
        max_plots: Most plots per scenario (issue plots first).
        window_minutes: Width of each plot window.
        min_issue_context_minutes: Data needed on both sides of an issue
            for it to be plotted.
        coord_patterns_dir: Folder with ``<device>_coord_splits.csv`` files
            whose programmed splits are drawn on time-of-day plots.
        baseline_label / candidate_label: Version labels used in results
            and plots.
    """

    settle_minutes: float = 10.0
    analysis_start_time: str = ""
    analysis_end_time: str = ""
    group_tolerance: float = 0.0
    phase_call_threshold: float = 90.0
    sequence_match_threshold: float = 95.0
    timing_match_threshold: float = 90.0
    max_plots: int = 3
    window_minutes: float = 5.0
    min_issue_context_minutes: float = 5.0
    coord_patterns_dir: Optional[str] = None
    baseline_label: str = "baseline"
    candidate_label: str = "candidate"

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any], **overrides: Any) -> "ValidationSettings":
        """Settings from a dict such as ``settings.json['comparison']``.

        Understands the script's key names (``max_divergence_plots``,
        ``divergence_window_minutes``, ``phase_call_similarity_threshold`` /
        ``detector_similarity_threshold``) as well as the field names.
        """
        aliases = {
            "max_divergence_plots": "max_plots",
            "divergence_window_minutes": "window_minutes",
            "phase_call_similarity_threshold": "phase_call_threshold",
            "detector_similarity_threshold": "phase_call_threshold",
        }
        names = {f.name for f in fields(cls)}
        values: Dict[str, Any] = {}
        for key, value in data.items():
            name = aliases.get(key, key)
            if name in names and value is not None and name not in values:
                values[name] = value
        if "phase_call_similarity_threshold" in data and data["phase_call_similarity_threshold"] is not None:
            values["phase_call_threshold"] = data["phase_call_similarity_threshold"]
        for name in ("analysis_start_time", "analysis_end_time"):
            if name in values:
                values[name] = str(values[name] or "").strip()
        values.update(overrides)
        return cls(**values)

    @classmethod
    def from_suite(cls, suite: SoftwareTestSuite, **overrides: Any) -> "ValidationSettings":
        """Settings from a :class:`~signal_replay.SoftwareTestSuite`.

        Uses its settle minutes, analysis window, phase-call threshold and
        version labels.
        """
        values: Dict[str, Any] = {
            "settle_minutes": float(suite.analysis_settle_minutes or 0.0),
            "analysis_start_time": str(suite.analysis_start_time or "").strip(),
            "analysis_end_time": str(suite.analysis_end_time or "").strip(),
            "phase_call_threshold": float(suite.phase_call_similarity_threshold),
            "baseline_label": str(suite.baseline_version),
            "candidate_label": str(suite.software_version),
        }
        values.update(overrides)
        return cls(**values)


def _missing_collected_error(scenario_id: str, source_label: str) -> str:
    return (
        f"No collected events found for {scenario_id} in {source_label}. "
        "Replay data for this scenario is missing, so the comparison and device CSV export are invalid "
        "until that device is collected again."
    )


# ---------------------------------------------------------------------------
# Loading events (parent process only, read-only connections)
# ---------------------------------------------------------------------------

def _read_events_table(con: duckdb.DuckDBPyConnection, db_label: str, scenario_id: str) -> pd.DataFrame:
    columns = [row[1] for row in con.execute("PRAGMA table_info('events')").fetchall()]
    required = {"device_id", "timestamp", "event_id", "parameter"}
    missing = sorted(required - set(columns))
    if missing:
        raise ValueError(
            f"DuckDB events table at {db_label} is missing required columns: {missing}"
        )
    select_columns = [
        col for col in ("device_id", "run_number", "timestamp", "event_id", "parameter")
        if col in columns
    ]
    order_columns = [
        col for col in ("run_number", "timestamp", "event_id", "parameter")
        if col in columns
    ]
    query = (
        f"SELECT {', '.join(select_columns)} FROM events WHERE device_id = ? "
        f"ORDER BY {', '.join(order_columns)}"
    )
    return con.execute(query, [scenario_id]).df()


def _connect_read_only(path: str) -> duckdb.DuckDBPyConnection:
    try:
        return duckdb.connect(path, read_only=True)
    except duckdb.ConnectionException as exc:
        # This process already has the file open read-write.
        if "different configuration" not in str(exc).lower():
            raise
        return duckdb.connect(path)


def load_collected_events(db_path: Union[str, os.PathLike], scenario_id: str) -> pd.DataFrame:
    """Collected output events of one scenario from a DuckDB file (read-only).

    Returns columns ``device_id, run_number, timestamp, event_id,
    parameter`` ordered by run and time.
    """
    con = _connect_read_only(str(db_path))
    try:
        return _read_events_table(con, str(db_path), scenario_id)
    finally:
        con.close()


def _is_collected_db(path: Path) -> bool:
    """True when ``path`` is a DuckDB file with an ``events`` table."""
    if path.suffix.lower() in (".parquet", ".csv", ".txt", ".json"):
        return False
    try:
        con = _connect_read_only(str(path))
    except Exception:
        return False
    try:
        tables = {
            row[0].lower()
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        return "events" in tables
    except Exception:
        return False
    finally:
        con.close()


def _filter_device(df: pd.DataFrame, scenario_id: str) -> pd.DataFrame:
    for column in df.columns:
        if str(column).lower() in ("device_id", "deviceid"):
            return df[df[column].astype(str) == str(scenario_id)].reset_index(drop=True)
    return df


class _EventLoader:
    """Loads one side's events per scenario; keeps one read-only connection per DuckDB file."""

    def __init__(self, source: Any, side: str):
        self.source = source
        self.side = side
        self._connections: Dict[str, duckdb.DuckDBPyConnection] = {}
        self._kinds: Dict[str, str] = {}

    def close(self) -> None:
        connections, self._connections = self._connections, {}
        for con in connections.values():
            try:
                con.close()
            except Exception:
                logger.debug("Closing %s connection failed", self.side, exc_info=True)

    def has(self, scenario_id: str) -> bool:
        if isinstance(self.source, Mapping):
            return scenario_id in self.source and self.source[scenario_id] is not None
        return self.source is not None

    def label(self, scenario_id: str) -> str:
        source = self.source[scenario_id] if isinstance(self.source, Mapping) else self.source
        if isinstance(source, tuple) and len(source) == 2:
            source = source[1]
        if isinstance(source, (str, os.PathLike)):
            return Path(source).name
        if isinstance(source, pd.DataFrame):
            return f"{self.side} DataFrame"
        return f"{self.side} events"

    def _db(self, path: str) -> duckdb.DuckDBPyConnection:
        con = self._connections.get(path)
        if con is None:
            con = _connect_read_only(path)
            self._connections[path] = con
        return con

    def _load_path(self, path: Path, scenario_id: str, kind: Optional[str], whole_file: bool) -> pd.DataFrame:
        key = str(path)
        if kind is None:
            kind = self._kinds.get(key)
            if kind is None:
                kind = "db" if _is_collected_db(path) else "file"
                self._kinds[key] = kind
        if kind == "db":
            return _read_events_table(self._db(key), key, scenario_id)
        if kind == "file":
            df = load_events(key)
            return df if whole_file else _filter_device(df, scenario_id)
        raise ValueError(f"Unsupported events source kind: {kind!r}")

    def load(self, scenario_id: str) -> pd.DataFrame:
        source = self.source
        per_scenario = isinstance(source, Mapping)
        if per_scenario:
            source = source[scenario_id]
        if callable(source) and not isinstance(source, pd.DataFrame):
            df = source(scenario_id)
            return pd.DataFrame() if df is None else df
        if isinstance(source, tuple) and len(source) == 2:
            kind, path = source
            return self._load_path(Path(path), scenario_id, str(kind), whole_file=per_scenario)
        if isinstance(source, pd.DataFrame):
            return source.copy() if per_scenario else _filter_device(source, scenario_id)
        if isinstance(source, (str, os.PathLike)):
            return self._load_path(Path(source), scenario_id, None, whole_file=per_scenario)
        raise TypeError(f"Unsupported {self.side} events input: {type(source).__name__}")


# ---------------------------------------------------------------------------
# Coordination split schedules (programmed splits drawn on TOD plots)
# ---------------------------------------------------------------------------

def load_coord_split_schedules(coord_dir: Optional[Union[str, os.PathLike]]) -> Dict[str, List[Dict[str, object]]]:
    """Read ``<device>_coord_splits.csv`` files (columns Phase, start_time, end_time).

    Returns ``{device_id: [{'phase', 'start_time', 'end_time'}, ...]}``;
    malformed rows are skipped with a warning.
    """
    schedules: Dict[str, List[Dict[str, object]]] = {}
    if not coord_dir:
        return schedules
    path = Path(coord_dir)
    if not path.exists():
        return schedules

    for csv_path in sorted(path.glob("*.csv")):
        device_id = _extract_coord_split_device_id(csv_path)
        if device_id is None:
            continue
        try:
            df = pd.read_csv(csv_path)
        except Exception as exc:
            logger.warning("Failed to read coord split CSV %s: %s", csv_path.name, exc)
            continue

        rows: List[Dict[str, object]] = []
        for row in df.to_dict("records"):
            try:
                phase = _parse_coord_split_phase(row.get("Phase"))
                start_time = _parse_coord_split_clock_time(row.get("start_time"))
                end_time = _parse_coord_split_clock_time(row.get("end_time"))
            except Exception as exc:
                logger.warning("Skipping malformed coord split row in %s: %s", csv_path.name, exc)
                continue
            if phase is None:
                logger.warning("Skipping coord split row with missing phase in %s", csv_path.name)
                continue
            rows.append({"phase": phase, "start_time": start_time, "end_time": end_time})

        if rows:
            rows.sort(key=lambda row: (row["start_time"], row["phase"]))
            schedules[device_id] = rows

    return schedules


# ---------------------------------------------------------------------------
# Analysis helpers (moved from software_validation/software_validate.py)
# ---------------------------------------------------------------------------

def _extract_coord_split_device_id(path: Path) -> Optional[str]:
    stem = path.stem
    suffix = "_coord_splits"
    if not stem.endswith(suffix):
        return None
    device_id = stem[:-len(suffix)].strip()
    return device_id or None


def _parse_coord_split_clock_time(value: object) -> dt_time:
    text = str(value).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    raise ValueError(f"invalid coord split time: {value!r}")


def _parse_coord_split_phase(value: object) -> Optional[int]:
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    return int(digits) if digits else None

def _resolve_coord_split_device_id(
    scenario_id: str,
    schedules: Dict[str, List[Dict[str, object]]],
) -> Optional[str]:
    if scenario_id in schedules:
        return scenario_id
    base_device_id = scenario_id.split("_", 1)[0]
    if base_device_id in schedules:
        return base_device_id
    return None


def _build_programmed_split_timeline(
    schedule_rows: List[Dict[str, object]],
    anchor_time: pd.Timestamp,
) -> pd.DataFrame:
    if not schedule_rows:
        return pd.DataFrame(columns=["Phase", "StartTime", "EndTime"])

    anchor_ts = pd.Timestamp(anchor_time)
    base_date = anchor_ts.normalize().to_pydatetime().date()
    rows: List[Dict[str, object]] = []
    for schedule_row in schedule_rows:
        start_ts = pd.Timestamp(datetime.combine(base_date, schedule_row["start_time"]))
        end_ts = pd.Timestamp(datetime.combine(base_date, schedule_row["end_time"]))
        if end_ts <= start_ts:
            end_ts += pd.Timedelta(days=1)
        rows.append(
            {
                "Phase": int(schedule_row["phase"]),
                "StartTime": start_ts,
                "EndTime": end_ts,
            }
        )
    return pd.DataFrame(rows)

def _get_timestamp_col(df: pd.DataFrame) -> str:
    """Return the timestamp column name used by a raw events DataFrame."""
    return next(
        (col for col in df.columns if col.lower() in ("timestamp", "time_stamp")),
        "timestamp",
    )


def _date_only_shifted_copy(
    df: pd.DataFrame,
    ts_col: str,
    target_ts: pd.Timestamp,
) -> pd.DataFrame:
    """Shift a run onto the target date while preserving time-of-day."""
    shifted = df.copy()
    if shifted.empty:
        return shifted

    shifted[ts_col] = pd.to_datetime(shifted[ts_col])
    source_start = shifted[ts_col].min()
    date_shift = target_ts.normalize() - source_start.normalize()
    shifted[ts_col] = shifted[ts_col] + date_shift
    return shifted


def _parse_analysis_clock_time(value: Optional[str], setting_name: str) -> Optional[dt_time]:
    """Parse a manual HH:MM[:SS] analysis clock time from settings."""
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue

    raise ValueError(
        f"comparison.{setting_name} must use HH:MM or HH:MM:SS format"
    )


def _resolve_manual_analysis_window(
    collected: pd.DataFrame,
    *,
    analysis_start_time: Optional[str],
    analysis_end_time: Optional[str],
) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """Resolve manual TOD analysis bounds onto the collected run dates."""
    parsed_start = _parse_analysis_clock_time(analysis_start_time, "analysis_start_time")
    parsed_end = _parse_analysis_clock_time(analysis_end_time, "analysis_end_time")
    if collected.empty or (parsed_start is None and parsed_end is None):
        return None, None

    collected_ts_col = _get_timestamp_col(collected)
    collected_ts = pd.to_datetime(collected[collected_ts_col])
    min_date = collected_ts.min().normalize()
    max_date = collected_ts.max().normalize()

    analysis_start = None
    if parsed_start is not None:
        analysis_start = min_date + pd.Timedelta(
            hours=parsed_start.hour,
            minutes=parsed_start.minute,
            seconds=parsed_start.second,
        )

    analysis_end = None
    if parsed_end is not None:
        end_date = min_date
        if parsed_start is not None and parsed_end < parsed_start:
            end_date = max_date
        analysis_end = end_date + pd.Timedelta(
            hours=parsed_end.hour,
            minutes=parsed_end.minute,
            seconds=parsed_end.second,
        )

    return analysis_start, analysis_end


def _prepare_analysis_inputs(
    original: pd.DataFrame,
    collected: pd.DataFrame,
    *,
    tod_align: bool,
) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[datetime], Optional[datetime]]:
    """Prepare scenario inputs for comparison/reporting.

    TOD scenarios are moved onto the collected run's date and anchored to a
    shared wall-clock reference so comparison preserves real time-of-day gaps.
    """
    original_prepared = original.copy()
    collected_prepared = collected.copy()

    if original_prepared.empty or collected_prepared.empty:
        return original_prepared, collected_prepared, None, None

    original_ts_col = _get_timestamp_col(original_prepared)
    collected_ts_col = _get_timestamp_col(collected_prepared)
    original_prepared[original_ts_col] = pd.to_datetime(original_prepared[original_ts_col])
    collected_prepared[collected_ts_col] = pd.to_datetime(collected_prepared[collected_ts_col])

    if not tod_align:
        return original_prepared, collected_prepared, None, None

    collected_start = collected_prepared[collected_ts_col].min()
    original_prepared = _date_only_shifted_copy(original_prepared, original_ts_col, collected_start)
    shared_start = original_prepared[original_ts_col].min().to_pydatetime()
    return original_prepared, collected_prepared, shared_start, shared_start


def _trim_to_analysis_window(
    df: pd.DataFrame,
    analysis_start: Optional[pd.Timestamp],
    analysis_end: Optional[pd.Timestamp],
) -> pd.DataFrame:
    """Trim a raw events DataFrame to the configured analysis window."""
    if df.empty or (analysis_start is None and analysis_end is None):
        return df
    ts_col = _get_timestamp_col(df)
    trimmed = df.copy()
    trimmed[ts_col] = pd.to_datetime(trimmed[ts_col])
    mask = pd.Series(True, index=trimmed.index)
    if analysis_start is not None:
        mask &= trimmed[ts_col] >= analysis_start
    if analysis_end is not None:
        mask &= trimmed[ts_col] <= analysis_end
    return trimmed[mask].reset_index(drop=True)


def _normalize_conflict_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw event logs into the shape expected by conflict analysis."""
    if events_df.empty:
        return pd.DataFrame(columns=["run_number", "TimeStamp", "EventTypeID", "Parameter"])

    df = events_df.copy()
    rename_map: Dict[str, str] = {}

    timestamp_col = _get_timestamp_col(df)
    if timestamp_col not in df.columns:
        raise ValueError("Conflict analysis requires a timestamp column")
    if timestamp_col != "TimeStamp":
        rename_map[timestamp_col] = "TimeStamp"

    event_col = next(
        (col for col in df.columns if col.lower() in ("event_id", "eventid", "eventtypeid")),
        None,
    )
    if event_col is None:
        raise ValueError("Conflict analysis requires an event_id/EventTypeID column")
    if event_col != "EventTypeID":
        rename_map[event_col] = "EventTypeID"

    parameter_col = next(
        (col for col in df.columns if col.lower() == "parameter"),
        None,
    )
    if parameter_col is None:
        raise ValueError("Conflict analysis requires a parameter column")
    if parameter_col != "Parameter":
        rename_map[parameter_col] = "Parameter"

    if rename_map:
        df = df.rename(columns=rename_map)

    if "run_number" not in df.columns:
        df["run_number"] = 1

    df["run_number"] = pd.to_numeric(df["run_number"], errors="coerce").fillna(1).astype(int)
    df["TimeStamp"] = pd.to_datetime(df["TimeStamp"])
    df["EventTypeID"] = pd.to_numeric(df["EventTypeID"], errors="raise").astype(int)
    df["Parameter"] = pd.to_numeric(df["Parameter"], errors="raise").astype(int)

    return (
        df[["run_number", "TimeStamp", "EventTypeID", "Parameter"]]
        .sort_values(["run_number", "TimeStamp", "EventTypeID", "Parameter"])
        .reset_index(drop=True)
    )


def _normalize_issue_timeline_rows(
    timeline: pd.DataFrame,
    event_class: str,
    event_value: int,
) -> pd.DataFrame:
    """Return timeline rows for a single event class/value with numeric durations."""
    if timeline.empty:
        return pd.DataFrame(columns=["StartTime", "EndTime", "Duration"])

    filtered = timeline[timeline["EventClass"] == event_class].copy()
    if filtered.empty:
        return filtered

    filtered["EventValue"] = pd.to_numeric(filtered["EventValue"], errors="coerce")
    if event_value == 0:
        value_mask = filtered["EventValue"].fillna(0) == 0
    else:
        value_mask = filtered["EventValue"] == event_value
    filtered = filtered[value_mask].copy()
    if filtered.empty:
        return filtered

    if "Duration" not in filtered.columns or filtered["Duration"].isna().all():
        filtered["Duration"] = (filtered["EndTime"] - filtered["StartTime"]).dt.total_seconds()
    filtered["Duration"] = pd.to_numeric(filtered["Duration"], errors="coerce")
    filtered = filtered.dropna(subset=["Duration"])
    return filtered.sort_values(["StartTime", "EndTime"]).reset_index(drop=True)


def _timeline_valid_mask(timeline: pd.DataFrame) -> pd.Series:
    if timeline.empty:
        return pd.Series(dtype=bool, index=timeline.index)

    for column in ("IsValid", "is_valid"):
        if column in timeline.columns:
            return timeline[column].fillna(False).astype(bool)

    return pd.Series(True, index=timeline.index)


def _split_timeline_by_validity(
    timeline: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if timeline.empty:
        return timeline.copy(), timeline.copy()

    valid_mask = _timeline_valid_mask(timeline)
    return timeline[valid_mask].copy(), timeline[~valid_mask].copy()


def _remove_ignored_timeline_events(timeline: pd.DataFrame) -> pd.DataFrame:
    if timeline.empty:
        return timeline.copy()

    remove_events = [
        "Ped Omit",
        "Phase Hold",
        "Phase Omit",
        "Phase Call",
    ]
    return timeline[~timeline["EventClass"].isin(remove_events)].copy()


def _prepare_settled_overlap_timelines(
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    *,
    settle_minutes: float,
    tod_align: bool,
    clip_to_overlap: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Apply the settle trim (and, for non-TOD scenarios, the A/B clock alignment).

    clip_to_overlap additionally restricts both sides to the time range where
    *both* have rows (by each side's own max EndTime). That's appropriate when
    pairing up events for A-vs-B duration comparisons, but wrong when simply
    checking "is there any invalid data here" - one side (e.g. a clean
    baseline) legitimately having little/no invalid data, and thus an early
    max EndTime, would otherwise truncate away real invalid rows on the other
    side. Callers doing that kind of existence check should pass False.
    """
    left = timeline_a.copy()
    right = timeline_b.copy()

    if left.empty and right.empty:
        return left, right

    settle_td = pd.Timedelta(minutes=settle_minutes)

    if tod_align:
        if settle_minutes > 0:
            if not left.empty:
                left = left[left["StartTime"] >= left["StartTime"].min() + settle_td].copy()
            if not right.empty:
                right = right[right["StartTime"] >= right["StartTime"].min() + settle_td].copy()
    else:
        start_a = None
        if not left.empty:
            start_a = left["StartTime"].min()
            left = left[left["StartTime"] >= start_a + settle_td].copy()
        if not right.empty:
            start_b = right["StartTime"].min()
            if start_a is not None:
                right["StartTime"] = start_a + (right["StartTime"] - start_b)
                right["EndTime"] = start_a + (right["EndTime"] - start_b)
                right = right[right["StartTime"] >= start_a + settle_td].copy()
            else:
                right = right[right["StartTime"] >= start_b + settle_td].copy()

    if clip_to_overlap and not left.empty and not right.empty:
        overlap_end = min(left["EndTime"].max(), right["EndTime"].max())
        left = left[left["StartTime"] <= overlap_end].copy()
        right = right[right["StartTime"] <= overlap_end].copy()

    return left, right


def _is_significant_operational_issue(diff: Dict[str, object]) -> bool:
    """Mirror report thresholds for non-clearance issue plots, including green rows."""
    event_class = str(diff.get("event_class", "")).strip()
    avg_delta = float(diff.get("duration_delta", 0.0) or 0.0)
    total_delta = float(diff.get("total_duration_delta", 0.0) or 0.0)
    count_delta = abs(int(diff.get("count_delta", 0) or 0))

    threshold = None
    total_threshold = None
    if event_class == "Preempt":
        threshold = 3.0
        total_threshold = 60.0
    elif event_class == "Ped Service":
        threshold = 2.0
        total_threshold = 15.0
    elif event_class in {"Transition Longway", "Transition Shortway"}:
        threshold = 1.0
        total_threshold = 10.0
    elif event_class in {"Green", "Overlap Green", "Overlap Yellow", "Overlap Red"}:
        threshold = 2.0
        total_threshold = 15.0

    if threshold is None:
        return False
    return abs(avg_delta) >= threshold or abs(total_delta) >= total_threshold or count_delta >= 1


def _merge_issue_spans(
    spans: List[Dict[str, object]],
    *,
    gap_seconds: float,
) -> List[Dict[str, object]]:
    if not spans:
        return []

    ordered = sorted(spans, key=lambda span: pd.Timestamp(span["start"]))
    merged: List[Dict[str, object]] = [dict(ordered[0])]
    for span in ordered[1:]:
        gap = (
            pd.Timestamp(span["start"]) - pd.Timestamp(merged[-1]["end"])
        ).total_seconds()
        if gap <= gap_seconds:
            merged[-1]["end"] = pd.Timestamp(span["end"])
            merged[-1]["mismatch_seconds"] = float(merged[-1]["mismatch_seconds"]) + float(span["mismatch_seconds"])
            continue
        merged.append(dict(span))
    return merged


def _as_datetime_series(values: pd.Series) -> pd.Series:
    """``values`` as datetimes, skipping the conversion when they already are."""
    if pd.api.types.is_datetime64_any_dtype(values):
        return values
    return pd.to_datetime(values)


def _window_activity_stats(
    rows: pd.DataFrame,
    *,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> Tuple[int, float, float]:
    if rows.empty or window_end <= window_start:
        return 0, 0.0, 0.0

    # Only rows that can overlap the window need the per-row arithmetic below.
    # Rows with a missing time are kept so the result matches the full loop.
    starts = _as_datetime_series(rows["StartTime"])
    ends = _as_datetime_series(rows["EndTime"])
    candidate = (
        ((ends > starts) & (ends > window_start) & (starts < window_end))
        | starts.isna()
        | ends.isna()
    )
    if not candidate.any():
        return 0, 0.0, 0.0

    count = 0
    active_seconds = 0.0
    for row in rows.loc[candidate.to_numpy()].itertuples(index=False):
        row_start = pd.Timestamp(row.StartTime)
        row_end = pd.Timestamp(row.EndTime)
        overlap_start = max(row_start, window_start)
        overlap_end = min(row_end, window_end)
        if overlap_end <= overlap_start:
            continue
        count += 1
        active_seconds += (overlap_end - overlap_start).total_seconds()

    avg_duration = active_seconds / count if count else 0.0
    return count, active_seconds, avg_duration


def _rank_operational_issue_windows(
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    diff: Dict[str, object],
) -> List[Dict[str, object]]:
    """Return ranked local A-vs-B mismatch windows for a non-clearance row."""
    rows_a = _normalize_issue_timeline_rows(
        timeline_a,
        str(diff.get("event_class", "")),
        int(diff.get("event_value", 0) or 0),
    )
    rows_b = _normalize_issue_timeline_rows(
        timeline_b,
        str(diff.get("event_class", "")),
        int(diff.get("event_value", 0) or 0),
    )

    if rows_a.empty and rows_b.empty:
        return []

    boundary_deltas: Dict[pd.Timestamp, List[int]] = {}

    def _record_boundaries(rows: pd.DataFrame, run_index: int) -> None:
        for row in rows.itertuples(index=False):
            start = pd.Timestamp(row.StartTime)
            end = pd.Timestamp(row.EndTime)
            if end <= start:
                continue
            boundary_deltas.setdefault(start, [0, 0])[run_index] += 1
            boundary_deltas.setdefault(end, [0, 0])[run_index] -= 1

    _record_boundaries(rows_a, 0)
    _record_boundaries(rows_b, 1)

    boundaries = sorted(boundary_deltas)
    if len(boundaries) < 2:
        return []

    mismatch_spans: List[Dict[str, object]] = []
    active_a = 0
    active_b = 0
    for current, nxt in zip(boundaries, boundaries[1:]):
        delta_a, delta_b = boundary_deltas[current]
        active_a += delta_a
        active_b += delta_b
        if nxt <= current or bool(active_a) == bool(active_b):
            continue
        mismatch_spans.append(
            {
                "start": current,
                "end": nxt,
                "mismatch_seconds": (pd.Timestamp(nxt) - pd.Timestamp(current)).total_seconds(),
            }
        )

    if not mismatch_spans:
        return []

    durations: List[float] = []
    for rows in (rows_a, rows_b):
        if rows.empty:
            continue
        durations.extend(float(value) for value in rows["Duration"].tolist())

    typical_duration = float(pd.Series(durations).median()) if durations else 0.0
    merge_gap_seconds = min(60.0, max(10.0, typical_duration))
    candidate_windows = _merge_issue_spans(mismatch_spans, gap_seconds=merge_gap_seconds)

    ranked_windows: List[Dict[str, object]] = []
    for window in candidate_windows:
        window_start = pd.Timestamp(window["start"])
        window_end = pd.Timestamp(window["end"])
        count_a, active_seconds_a, avg_duration_a = _window_activity_stats(
            rows_a,
            window_start=window_start,
            window_end=window_end,
        )
        count_b, active_seconds_b, avg_duration_b = _window_activity_stats(
            rows_b,
            window_start=window_start,
            window_end=window_end,
        )
        ranked_windows.append(
            {
                "start": window_start,
                "end": window_end,
                "score": float(window["mismatch_seconds"]),
                "mismatch_seconds": float(window["mismatch_seconds"]),
                "count_a": count_a,
                "count_b": count_b,
                "count_imbalance": abs(count_a - count_b),
                "active_seconds_a": active_seconds_a,
                "active_seconds_b": active_seconds_b,
                "active_imbalance": abs(active_seconds_a - active_seconds_b),
                "avg_duration_a": avg_duration_a,
                "avg_duration_b": avg_duration_b,
            }
        )

    ranked_windows.sort(
        key=lambda window: (
            -float(window.get("score", 0.0) or 0.0),
            -int(window.get("count_imbalance", 0) or 0),
            -float(window.get("active_imbalance", 0.0) or 0.0),
            pd.Timestamp(window["start"]),
        ),
    )
    return ranked_windows


def _select_operational_issue_anchor(
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    diff: Dict[str, object],
) -> Optional[Dict[str, object]]:
    """Select the largest local A-vs-B mismatch window for a non-clearance row."""
    ranked_windows = _rank_operational_issue_windows(timeline_a, timeline_b, diff)
    if not ranked_windows:
        return None
    return ranked_windows[0]


def _format_operational_issue_caption(
    issue_window: Dict[str, object],
    diff: Dict[str, object],
    *,
    label_a: str,
    label_b: str,
) -> str:
    detail_label = f"{str(diff.get('label', '')).strip()} {str(diff.get('state', '')).strip()}".strip()
    parts = [
        f"{detail_label}: largest local mismatch window",
        f"mismatch {float(issue_window.get('mismatch_seconds', 0.0) or 0.0):.2f}s",
        (
            f"{label_a} events {int(issue_window.get('count_a', 0) or 0)}, "
            f"active {float(issue_window.get('active_seconds_a', 0.0) or 0.0):.2f}s"
        ),
        (
            f"{label_b} events {int(issue_window.get('count_b', 0) or 0)}, "
            f"active {float(issue_window.get('active_seconds_b', 0.0) or 0.0):.2f}s"
        ),
    ]

    count_a = int(issue_window.get("count_a", 0) or 0)
    count_b = int(issue_window.get("count_b", 0) or 0)
    if count_a > 0 and count_b > 0:
        parts.append(
            f"window avg {label_a} {float(issue_window.get('avg_duration_a', 0.0) or 0.0):.2f}s vs "
            f"{label_b} {float(issue_window.get('avg_duration_b', 0.0) or 0.0):.2f}s"
        )

    return "; ".join(parts)


def _treat_phase_diff_as_non_clearance_issue(
    diff: Dict[str, object],
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
) -> bool:
    event_class = str(diff.get("event_class", "")).strip()
    if event_class in {"Green", "Overlap Green"}:
        return True
    if event_class not in {"Overlap Yellow", "Overlap Red"}:
        return False

    rows_a = _normalize_issue_timeline_rows(
        timeline_a,
        event_class,
        int(diff.get("event_value", 0) or 0),
    )
    rows_b = _normalize_issue_timeline_rows(
        timeline_b,
        event_class,
        int(diff.get("event_value", 0) or 0),
    )
    overlap_sources = [
        rows["Duration"]
        for rows in (rows_a, rows_b)
        if not rows.empty
    ]
    if not overlap_sources:
        return False
    return not all(
        is_overlap_clearance_interval_candidate(durations)
        for durations in overlap_sources
    )


def _select_clearance_issue_spec(
    *,
    scenario_id: str,
    row: Dict[str, object],
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    label_a: str,
    label_b: str,
) -> Optional[Dict[str, object]]:
    if "irregular_count_b" in row and int(row.get("irregular_count_b", 0) or 0) <= 0:
        return None

    rows_a = _normalize_issue_timeline_rows(
        timeline_a,
        str(row.get("event_class", "")),
        int(row.get("event_value", 0) or 0),
    )
    rows_b = _normalize_issue_timeline_rows(
        timeline_b,
        str(row.get("event_class", "")),
        int(row.get("event_value", 0) or 0),
    )

    def _peak(rows: pd.DataFrame, version_label: str) -> Optional[Dict[str, object]]:
        if rows.empty:
            return None
        median = float(rows["Duration"].median())
        deviations = rows["Duration"] - median
        idx = deviations.abs().idxmax()
        return {
            "version": version_label,
            "duration": float(rows.loc[idx, "Duration"]),
            "median": median,
            "deviation": float(deviations.loc[idx]),
            "start": pd.Timestamp(rows.loc[idx, "StartTime"]),
            "end": pd.Timestamp(rows.loc[idx, "EndTime"]),
        }

    peak_a = _peak(rows_a, label_a)
    peak_b = _peak(rows_b, label_b)
    if peak_b is None:
        return None

    anchor = peak_b
    detail_label = f"{str(row.get('label', '')).strip()} {str(row.get('state', '')).strip()}".strip()
    relation = "above" if float(anchor["deviation"]) >= 0 else "below"

    version_parts: List[str] = []
    if peak_a is not None:
        version_parts.append(
            f"{label_a}: {peak_a['duration']:.2f}s vs median {peak_a['median']:.2f}s"
        )
    else:
        version_parts.append(f"{label_a}: no flagged irregular event")
    if peak_b is not None:
        version_parts.append(
            f"{label_b}: {peak_b['duration']:.2f}s vs median {peak_b['median']:.2f}s"
        )
    else:
        version_parts.append(f"{label_b}: no flagged irregular event")

    return {
        "caption": (
            f"{detail_label} shown below is {abs(float(anchor['deviation'])):.2f}s {relation} the median "
            f"in {anchor['version']} ({'; '.join(version_parts)})"
        ),
        "title": f"{scenario_id} Clearance Focus - {detail_label}",
        "start": anchor["start"],
        "end": anchor["end"],
        "score": abs(float(anchor["deviation"])),
    }


def _timeline_context_bounds(
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    if timeline_a.empty or timeline_b.empty:
        return None

    start = max(pd.Timestamp(timeline_a["StartTime"].min()), pd.Timestamp(timeline_b["StartTime"].min()))
    end = min(pd.Timestamp(timeline_a["EndTime"].max()), pd.Timestamp(timeline_b["EndTime"].max()))
    if pd.isna(start) or pd.isna(end) or end <= start:
        return None
    return start, end


def _issue_has_timeline_context(
    spec: Dict[str, object],
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    *,
    min_context_minutes: float,
) -> bool:
    if min_context_minutes <= 0:
        return True

    bounds = _timeline_context_bounds(timeline_a, timeline_b)
    if bounds is None:
        return False

    context = pd.Timedelta(minutes=min_context_minutes)
    issue_start = pd.Timestamp(spec["start"])
    issue_end = pd.Timestamp(spec.get("end", issue_start))
    if pd.isna(issue_start) or pd.isna(issue_end):
        return False
    if issue_end < issue_start:
        issue_start, issue_end = issue_end, issue_start

    data_start, data_end = bounds
    return issue_start - data_start >= context and data_end - issue_end >= context


def _is_distinct_issue_window(
    start: pd.Timestamp,
    existing_specs: List[Dict[str, object]],
    *,
    min_spacing_seconds: float = 600.0,
) -> bool:
    return all(
        abs((pd.Timestamp(start) - pd.Timestamp(spec["start"])).total_seconds()) >= min_spacing_seconds
        for spec in existing_specs
    )


def _non_clearance_issue_group_key(diff: Dict[str, object]) -> Tuple[str, ...]:
    event_class = str(diff.get("event_class", "")).strip()
    label = str(diff.get("label", "")).strip()
    event_value = int(diff.get("event_value", 0) or 0)

    if event_class == "Preempt":
        return ("preempt", str(event_value))
    if event_class in {"Transition Longway", "Transition Shortway"}:
        return ("transition", event_class)
    if event_class == "Green":
        return ("phase_green",)
    if event_class == "Overlap Green":
        return ("overlap_green",)
    if event_class == "Overlap Yellow":
        return ("overlap_yellow",)
    if event_class == "Overlap Red":
        return ("overlap_red",)
    if event_class == "Ped Service":
        if label.startswith(("Ovlp Ped", "Overlap Ped")):
            return ("overlap_ped",)
        return ("ped",)
    return ("non_clearance", event_class)


def _issue_spec_priority(spec: Dict[str, object]) -> Tuple[float, int, float, float]:
    return (
        float(spec.get("score", 0.0) or 0.0),
        int(spec.get("count_imbalance", 0) or 0),
        float(spec.get("active_imbalance", 0.0) or 0.0),
        -float(pd.Timestamp(spec["start"]).value),
    )


def _issue_spec_sort_key(spec: Dict[str, object]) -> Tuple[float, int, float, pd.Timestamp]:
    return (
        -float(spec.get("score", 0.0) or 0.0),
        -int(spec.get("count_imbalance", 0) or 0),
        -float(spec.get("active_imbalance", 0.0) or 0.0),
        pd.Timestamp(spec["start"]),
    )


def _generate_special_issue_plots(
    *,
    scenario_id: str,
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    aligned_timeline_a: pd.DataFrame,
    aligned_timeline_b: pd.DataFrame,
    phase_differences: List[dict],
    clearance_irregularities: List[dict],
    operational_diffs: List[dict],
    plots_dir: str,
    label_a: str,
    label_b: str,
    window_minutes: float,
    time_offset_b: float,
    align_by_time_delta: bool,
    tod_align: bool = False,
    min_context_minutes: float = 0.0,
    invalid_timeline_a: Optional[pd.DataFrame] = None,
    invalid_timeline_b: Optional[pd.DataFrame] = None,
    coord_split_schedules: Optional[Dict[str, List[Dict[str, object]]]] = None,
) -> Tuple[List[str], List[str]]:
    """Create labeled charts for the worst flagged clearance and operational issues.

    Candidate windows that overlap an invalid (missing/unreliable data) interval
    on either side are skipped, since such a mismatch reflects a data collection
    gap rather than an actual software behavior difference.
    """
    if timeline_a.empty or timeline_b.empty:
        return [], []

    issue_specs: List[Dict[str, object]] = []
    seen_keys = set()
    non_clearance_candidates_by_group: Dict[Tuple[str, ...], List[Dict[str, object]]] = {}
    coord_split_schedules = (coord_split_schedules or {}) if tod_align else {}
    programmed_split_device_id = _resolve_coord_split_device_id(scenario_id, coord_split_schedules) if tod_align else None

    for row in clearance_irregularities:
        if int(row.get("irregular_count_b", 0) or 0) <= 0:
            continue
        key = ("clearance", row.get("event_class"), row.get("event_value"))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        issue_spec = _select_clearance_issue_spec(
            scenario_id=scenario_id,
            row=row,
            timeline_a=aligned_timeline_a,
            timeline_b=aligned_timeline_b,
            label_a=label_a,
            label_b=label_b,
        )
        if issue_spec is None:
            continue
        if timeline_overlaps_interval(
            invalid_timeline_a, issue_spec["start"], issue_spec["end"]
        ) or timeline_overlaps_interval(
            invalid_timeline_b, issue_spec["start"], issue_spec["end"]
        ):
            continue
        if not _issue_has_timeline_context(
            issue_spec,
            aligned_timeline_a,
            aligned_timeline_b,
            min_context_minutes=min_context_minutes,
        ):
            continue
        issue_specs.append(issue_spec)

    phase_state_diffs = [
        diff for diff in phase_differences
        if _treat_phase_diff_as_non_clearance_issue(diff, aligned_timeline_a, aligned_timeline_b)
    ]

    for diff in [*operational_diffs, *phase_state_diffs]:
        if not _is_significant_operational_issue(diff):
            continue
        key = ("non_clearance", diff.get("event_class"), diff.get("event_value"))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        ranked_issue_windows = _rank_operational_issue_windows(aligned_timeline_a, aligned_timeline_b, diff)
        if not ranked_issue_windows:
            continue
        detail_label = f"{diff.get('label', '').strip()} {diff.get('state', '').strip()}".strip()
        group_key = _non_clearance_issue_group_key(diff)
        non_clearance_candidates_by_group.setdefault(group_key, [])
        for issue_window in ranked_issue_windows:
            if timeline_overlaps_interval(
                invalid_timeline_a, issue_window["start"], issue_window["end"]
            ) or timeline_overlaps_interval(
                invalid_timeline_b, issue_window["start"], issue_window["end"]
            ):
                continue
            if not _issue_has_timeline_context(
                issue_window,
                aligned_timeline_a,
                aligned_timeline_b,
                min_context_minutes=min_context_minutes,
            ):
                continue
            non_clearance_candidates_by_group[group_key].append(
                {
                    "caption": _format_operational_issue_caption(
                        issue_window,
                        diff,
                        label_a=label_a,
                        label_b=label_b,
                    ),
                    "title": f"{scenario_id} Issue Focus - {detail_label}",
                    "start": issue_window["start"],
                    "end": issue_window["end"],
                    "score": float(issue_window.get("score", 0.0) or 0.0),
                    "count_imbalance": int(issue_window.get("count_imbalance", 0) or 0),
                    "active_imbalance": float(issue_window.get("active_imbalance", 0.0) or 0.0),
                    "group_key": group_key,
                    "show_programmed_splits": programmed_split_device_id is not None,
                }
            )

    selected_so_far = list(issue_specs)
    ranked_groups = []
    for specs in non_clearance_candidates_by_group.values():
        if not specs:
            continue
        specs.sort(key=_issue_spec_sort_key)
        ranked_groups.append(specs)

    ranked_groups.sort(key=lambda specs: _issue_spec_sort_key(specs[0]))
    for specs in ranked_groups:
        for spec in specs:
            if not _is_distinct_issue_window(pd.Timestamp(spec["start"]), selected_so_far):
                continue
            issue_specs.append(spec)
            selected_so_far.append(spec)
            break

    issue_specs.sort(key=_issue_spec_sort_key)
    deduped_issue_specs: List[Dict[str, object]] = []
    for spec in issue_specs:
        if not _is_distinct_issue_window(pd.Timestamp(spec["start"]), deduped_issue_specs):
            continue
        deduped_issue_specs.append(spec)

    output_dir = Path(plots_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    plot_paths: List[str] = []
    plot_captions: List[str] = []
    for index, spec in enumerate(deduped_issue_specs, start=1):
        output_path = output_dir / f"{scenario_id}_issue_{index}.png"
        programmed_split_timeline = None
        if spec.get("show_programmed_splits") and programmed_split_device_id is not None:
            programmed_split_timeline = _build_programmed_split_timeline(
                coord_split_schedules[programmed_split_device_id],
                pd.Timestamp(spec["start"]),
            )
        fig = create_comparison_gantt_matplotlib(
            timeline_a=timeline_a,
            timeline_b=timeline_b,
            label_a=label_a,
            label_b=label_b,
            title=str(spec["title"]),
            divergence_start=spec["start"],
            divergence_end=spec["end"],
            output_path=output_path,
            window_minutes=window_minutes,
            dpi=150,
            align_by_time_delta=align_by_time_delta,
            time_offset_b=time_offset_b,
            programmed_split_timeline=programmed_split_timeline,
        )
        if fig is None:
            continue
        plot_paths.append(str(output_path))
        plot_captions.append(str(spec["caption"]))

    return plot_paths, plot_captions


# ---------------------------------------------------------------------------
# Conflict scenarios
# ---------------------------------------------------------------------------

def _summarize_conflicts_from_saved_events(
    events_df: pd.DataFrame,
    incompatible_pairs: Optional[List[Tuple[str, str]]],
) -> Tuple[List[dict], int]:
    """Compute per-run conflict records directly from saved event logs."""
    if events_df.empty:
        return [], 0
    if not incompatible_pairs:
        normalized = _normalize_conflict_events(events_df)
        run_count = int(normalized["run_number"].nunique()) if not normalized.empty else 0
        return [], run_count

    normalized = _normalize_conflict_events(events_df)
    if normalized.empty:
        return [], 0

    conflicts_found: List[dict] = []
    run_numbers = normalized["run_number"].drop_duplicates().tolist()
    for run_number in run_numbers:
        run_events = normalized.loc[
            normalized["run_number"] == run_number,
            ["TimeStamp", "EventTypeID", "Parameter"],
        ]
        run_conflicts = check_conflicts(run_events, incompatible_pairs)
        if run_conflicts.empty:
            continue

        run_conflicts = run_conflicts.sort_values("TimeStamp")
        for row in run_conflicts.itertuples(index=False):
            last = getattr(row, "Last_TimeStamp", None)
            duration = getattr(row, "Duration_Seconds", None)
            conflicts_found.append({
                "run_number": int(run_number),
                "timestamp": pd.Timestamp(row.TimeStamp).isoformat(sep=" "),
                "conflict_details": row.Conflict_Details,
                "last_timestamp": None if last is None or pd.isna(last) else pd.Timestamp(last).isoformat(sep=" "),
                "occurrences": int(getattr(row, "Occurrences", 1) or 1),
                "duration_seconds": None if duration is None or pd.isna(duration) else float(duration),
            })

    return conflicts_found, len(run_numbers)


def _analyze_conflict_scenario(
    scenario: TestScenario,
    baseline_events: pd.DataFrame,
    collected_events: pd.DataFrame,
    *,
    baseline_label: str,
    software_version: str,
    collected_label: str,
) -> ScenarioResult:
    """Build a conflict result from the baseline and candidate event logs."""
    if collected_events.empty:
        return ScenarioResult(
            scenario_id=scenario.scenario_id,
            test_type=TestType.CONFLICT,
            software_version=software_version,
            passed=False,
            runs_completed=0,
            total_runs=scenario.replays,
            error=_missing_collected_error(scenario.scenario_id, collected_label),
            notes_column=scenario.notes_column,
        )

    baseline_conflicts, baseline_runs = _summarize_conflicts_from_saved_events(
        baseline_events,
        scenario.incompatible_pairs,
    )
    new_conflicts, runs_completed = _summarize_conflicts_from_saved_events(
        collected_events,
        scenario.incompatible_pairs,
    )

    baseline_has_conflict = bool(baseline_conflicts)
    new_has_conflict = bool(new_conflicts)
    configured_pairs = bool(scenario.incompatible_pairs)

    notes: List[str] = []
    if not configured_pairs:
        notes.append("No incompatible_pairs configured for this conflict scenario.")
    elif baseline_has_conflict:
        notes.append(
            f"{baseline_label} reproduced {len(baseline_conflicts)} conflict signature(s) across {baseline_runs} run(s)."
        )
    else:
        notes.append(f"{baseline_label} did not reproduce the configured conflict; test validity warning.")

    if new_has_conflict:
        conflict_runs = sorted({record["run_number"] for record in new_conflicts})
        notes.append(
            f"Conflict observed on {software_version} in run(s): {', '.join(str(run) for run in conflict_runs)}."
        )
    else:
        notes.append(f"No conflicts detected on {software_version} across {runs_completed} completed run(s).")

    passed = configured_pairs and baseline_has_conflict and not new_has_conflict

    return ScenarioResult(
        scenario_id=scenario.scenario_id,
        test_type=TestType.CONFLICT,
        software_version=software_version,
        passed=passed,
        conflicts_found=new_conflicts,
        runs_completed=runs_completed,
        total_runs=scenario.replays,
        notes=" ".join(notes),
        notes_column=scenario.notes_column,
    )


# ---------------------------------------------------------------------------
# Similarity scenarios
# ---------------------------------------------------------------------------

def _truncated_summary(result: ComparisonResult, max_div_shown: int = 5) -> str:
    """``result.format_summary()`` with at most ``max_div_shown`` divergence lines."""
    summary_lines = result.format_summary().split("\n")
    truncated_lines = []
    div_count = 0
    for line in summary_lines:
        if line.startswith("  ") and div_count >= max_div_shown:
            continue  # skip excess divergence lines
        truncated_lines.append(line)
        if line.startswith("  "):
            div_count += 1
            if div_count == max_div_shown and len(result.divergence_windows) > max_div_shown:
                truncated_lines.append(f"  ... and {len(result.divergence_windows) - max_div_shown} more divergences")
    return "\n".join(truncated_lines)


@contextlib.contextmanager
def _quiet(enabled: bool):
    """Silence stdout (atspm prints progress); used inside worker processes only."""
    if not enabled:
        yield
        return
    with open(os.devnull, "w") as devnull, contextlib.redirect_stdout(devnull):
        yield


def _compare_similarity(job: Dict[str, Any]) -> ScenarioResult:
    """Compare one similarity scenario. Runs in a worker process or in-process.

    ``job`` holds plain data and DataFrames only: ``scenario_id``,
    ``baseline``/``candidate`` (DataFrames), ``candidate_source`` (label for
    errors), ``settings`` (ValidationSettings), ``notes_column``,
    ``tod_align``, ``plots_dir`` (str or None), ``coord_split_schedules``
    and ``quiet``.
    """
    scenario_id: str = job["scenario_id"]
    settings: ValidationSettings = job["settings"]
    baseline: pd.DataFrame = job["baseline"]
    collected: pd.DataFrame = job["candidate"]
    tod_align: bool = bool(job["tod_align"])
    notes_column: str = job.get("notes_column") or ""
    plots_dir_str: Optional[str] = job.get("plots_dir")
    coord_split_schedules = job.get("coord_split_schedules") or {}
    quiet: bool = bool(job.get("quiet"))
    baseline_label = settings.baseline_label
    software_version = settings.candidate_label
    group_tolerance = settings.group_tolerance
    phase_call_threshold = settings.phase_call_threshold
    window_minutes = settings.window_minutes
    max_plots = int(settings.max_plots)

    if collected.empty:
        return ScenarioResult(
            scenario_id=scenario_id,
            test_type=TestType.SIMILARITY,
            software_version=software_version,
            passed=False,
            num_divergences=0,
            error=_missing_collected_error(scenario_id, job.get("candidate_source") or "candidate events"),
            notes_column=notes_column,
            runs_completed=0,
            total_runs=1,
        )
    baseline_for_analysis, collected_for_analysis, start_time_a, start_time_b = _prepare_analysis_inputs(
        baseline,
        collected,
        tod_align=tod_align,
    )

    sparkline_base_timestamp: Optional[datetime] = None
    compare_settle_minutes = settings.settle_minutes
    manual_analysis_start: Optional[pd.Timestamp] = None
    manual_analysis_end: Optional[pd.Timestamp] = None
    if tod_align:
        manual_analysis_start, manual_analysis_end = _resolve_manual_analysis_window(
            collected_for_analysis,
            analysis_start_time=settings.analysis_start_time,
            analysis_end_time=settings.analysis_end_time,
        )
        if manual_analysis_start is not None or manual_analysis_end is not None:
            baseline_for_analysis = _trim_to_analysis_window(
                baseline_for_analysis,
                manual_analysis_start,
                manual_analysis_end,
            )
            collected_for_analysis = _trim_to_analysis_window(
                collected_for_analysis,
                manual_analysis_start,
                manual_analysis_end,
            )
            if manual_analysis_start is not None:
                start_time_a = manual_analysis_start.to_pydatetime()
                start_time_b = manual_analysis_start.to_pydatetime()
                sparkline_base_timestamp = manual_analysis_start.to_pydatetime()
            elif not baseline_for_analysis.empty:
                start_time_a = pd.to_datetime(
                    baseline_for_analysis[_get_timestamp_col(baseline_for_analysis)]
                ).min().to_pydatetime()
                start_time_b = start_time_a
                sparkline_base_timestamp = start_time_a
            compare_settle_minutes = 0.0
        else:
            sparkline_base_timestamp = start_time_a

    result = compare_runs(
        events_a=baseline_for_analysis, events_b=collected_for_analysis,
        device_id=scenario_id,
        run_a_label=baseline_label, run_b_label=software_version,
        start_time_a=start_time_a,
        start_time_b=start_time_b,
        auto_align=not tod_align, settle_minutes=compare_settle_minutes,
        group_tolerance=group_tolerance,
        phase_call_threshold=phase_call_threshold,
    )

    if not result.thrown_out and not result.chunk_scores:
        result.thrown_out = True
        result.thrown_out_reason = (
            getattr(result, "thrown_out_reason", "")
            or "Insufficient scored chunks remained after settling/filtering for a reliable comparison."
        )

    plot_paths: list = []
    plot_captions: list = []
    timeline_a = timeline_b = None
    valid_timeline_a = valid_timeline_b = None
    invalid_timeline_a = invalid_timeline_b = None
    tl_a_invalid = tl_b_invalid = None
    analysis_diagnostics = []
    chart_time_offset_b = 0.0
    chart_align_by_time_delta = not tod_align
    analysis_diagnostics.append(
        "Similarity chunks after settle/filtering: "
        f"total={len(result.chunk_scores)}, included={result.included_chunk_count}, excluded={result.excluded_chunk_count}"
    )
    if not result.chunk_scores:
        analysis_diagnostics.append(
            "Comparison produced no scored chunks; match likely fell back to full-sequence DTW or a very short overlap."
        )
    if not baseline_for_analysis.empty and not collected_for_analysis.empty:
        try:
            with _quiet(quiet):
                timeline_a = generate_timeline(baseline_for_analysis, device_id=scenario_id)
                timeline_b = generate_timeline(collected_for_analysis, device_id=scenario_id)
            timeline_a = _remove_ignored_timeline_events(timeline_a)
            timeline_b = _remove_ignored_timeline_events(timeline_b)
            rows_before_chunk_clip_a = len(timeline_a)
            rows_before_chunk_clip_b = len(timeline_b)
            timeline_a = clip_timeline_to_relative_periods(
                timeline_a,
                result.included_event_periods_a,
                base_timestamp=start_time_a,
            )
            timeline_b = clip_timeline_to_relative_periods(
                timeline_b,
                result.included_event_periods_b,
                base_timestamp=start_time_b,
            )
            timeline_a, timeline_b = cross_invalidate_timelines(timeline_a, timeline_b)
            valid_timeline_a, invalid_timeline_a = _split_timeline_by_validity(timeline_a)
            valid_timeline_b, invalid_timeline_b = _split_timeline_by_validity(timeline_b)
            if tod_align or valid_timeline_a.empty or valid_timeline_b.empty:
                chart_time_offset_b = 0.0
            else:
                chart_time_offset_b = compute_timeline_offset(valid_timeline_a, valid_timeline_b)
            analysis_diagnostics.append(
                "Timeline rows after removing input-only classes: "
                f"original={rows_before_chunk_clip_a}, new={rows_before_chunk_clip_b}"
            )
            analysis_diagnostics.append(
                "Timeline rows after phase-call chunk filtering: "
                f"original={len(timeline_a)}, new={len(timeline_b)}"
            )
            analysis_diagnostics.append(
                "Valid rows used for detailed summaries: "
                f"original={len(valid_timeline_a)}, new={len(valid_timeline_b)}; "
                "invalid rows reserved for Data Integrity: "
                f"original={len(invalid_timeline_a)}, new={len(invalid_timeline_b)}"
            )
            if valid_timeline_a.empty or valid_timeline_b.empty:
                analysis_diagnostics.append(
                    "Filtered valid timelines do not contain enough signal, overlap, transition, preempt, or pedestrian-service rows for detailed summaries."
                )
        except Exception as e:
            analysis_diagnostics.append(f"Timeline generation failed: {e}")
            logger.debug("Timeline generation failed for %s", scenario_id, exc_info=True)

    timing_match = getattr(result, "timing_match_percentage", None)
    sequence_passed = result.match_percentage >= settings.sequence_match_threshold
    timing_passed = timing_match is not None and timing_match >= settings.timing_match_threshold
    passed = (not result.thrown_out) and sequence_passed and timing_passed
    phase_diffs: list = []
    clearance_irregularities: list = []
    operational_diffs: list = []
    invalid_clearance_irregularities: list = []
    invalid_operational_diffs: list = []
    timeline_difference_analysis_available = False
    if (
        valid_timeline_a is not None
        and valid_timeline_b is not None
        and not valid_timeline_a.empty
        and not valid_timeline_b.empty
    ):
        try:
            tl_a_settled, tl_b_settled = _prepare_settled_overlap_timelines(
                valid_timeline_a,
                valid_timeline_b,
                settle_minutes=compare_settle_minutes,
                tod_align=tod_align,
            )
            analysis_diagnostics.append(
                "Settled overlap rows used for timeline summaries: "
                f"original={len(tl_a_settled)}, new={len(tl_b_settled)}"
            )
            if tl_a_settled.empty or tl_b_settled.empty:
                analysis_diagnostics.append(
                    "No overlapping settled timeline remained after alignment and overlap clipping."
                )

            phase_diffs = generate_phase_difference_summary(tl_a_settled, tl_b_settled, tolerance_seconds=0.2)
            clearance_irregularities = generate_clearance_irregularity_summary(
                tl_a_settled,
                tl_b_settled,
                threshold_seconds=0.1,
            )
            operational_diffs = generate_operational_difference_summary(tl_a_settled, tl_b_settled, tolerance_seconds=0.2)
            timeline_difference_analysis_available = True

            tl_a_invalid, tl_b_invalid = _prepare_settled_overlap_timelines(
                invalid_timeline_a,
                invalid_timeline_b,
                settle_minutes=compare_settle_minutes,
                tod_align=tod_align,
            )
            # Unclipped (no A/B overlap-range trim) version for the "does invalid
            # data touch this window at all" checks used to discard divergences.
            # Using tl_a_invalid/tl_b_invalid there would be wrong: if one side
            # (e.g. a clean baseline) has little invalid data ending early, its
            # short EndTime range would truncate away real invalid rows on the
            # other side that fall later in the day.
            invalid_context_a, invalid_context_b = _prepare_settled_overlap_timelines(
                invalid_timeline_a,
                invalid_timeline_b,
                settle_minutes=compare_settle_minutes,
                tod_align=tod_align,
                clip_to_overlap=False,
            )

            if plots_dir_str:
                try:
                    with _quiet(quiet):
                        programmed_split_timeline = None
                        if tod_align:
                            programmed_split_device_id = _resolve_coord_split_device_id(scenario_id, coord_split_schedules)
                            if programmed_split_device_id is not None:
                                programmed_split_timeline = _build_programmed_split_timeline(
                                    coord_split_schedules[programmed_split_device_id],
                                    valid_timeline_a["StartTime"].min(),
                                )

                        issue_plot_paths, issue_plot_captions = _generate_special_issue_plots(
                            scenario_id=scenario_id,
                            timeline_a=timeline_a,
                            timeline_b=timeline_b,
                            aligned_timeline_a=tl_a_settled,
                            aligned_timeline_b=tl_b_settled,
                            invalid_timeline_a=invalid_context_a,
                            invalid_timeline_b=invalid_context_b,
                            phase_differences=phase_diffs,
                            clearance_irregularities=clearance_irregularities,
                            operational_diffs=operational_diffs,
                            plots_dir=plots_dir_str,
                            label_a=baseline_label,
                            label_b=software_version,
                            window_minutes=window_minutes,
                            time_offset_b=chart_time_offset_b,
                            align_by_time_delta=chart_align_by_time_delta,
                            tod_align=tod_align,
                            min_context_minutes=settings.min_issue_context_minutes,
                            coord_split_schedules=coord_split_schedules,
                        )

                        remaining_divergence_plots = max(0, max_plots - len(issue_plot_paths))
                        divergence_paths = create_multi_divergence_plots(
                            timeline_a=timeline_a,
                            timeline_b=timeline_b,
                            comparison_result=result,
                            output_dir=plots_dir_str,
                            label_a=baseline_label,
                            label_b=software_version,
                            max_plots=remaining_divergence_plots,
                            window_minutes=window_minutes,
                            time_offset_b=chart_time_offset_b,
                            align_by_time_delta=chart_align_by_time_delta,
                            programmed_split_timeline=programmed_split_timeline,
                            min_context_minutes=settings.min_issue_context_minutes,
                            invalid_timeline_a=invalid_timeline_a,
                            invalid_timeline_b=invalid_timeline_b,
                        ) if remaining_divergence_plots > 0 else []

                    plot_paths = issue_plot_paths + divergence_paths
                    plot_captions = issue_plot_captions + [
                        f"Divergence {index}" for index in range(1, len(divergence_paths) + 1)
                    ]
                except Exception:
                    logger.warning("Chart generation failed for %s", scenario_id, exc_info=True)
        except Exception as e:
            analysis_diagnostics.append(f"Timeline difference summary failed: {e}")
            logger.warning("Phase breakdown failed for %s: %s", scenario_id, e, exc_info=True)

    if tl_a_invalid is not None and tl_b_invalid is not None:
        try:
            if not tl_a_invalid.empty or not tl_b_invalid.empty:
                invalid_clearance_irregularities = generate_clearance_irregularity_summary(
                    tl_a_invalid,
                    tl_b_invalid,
                    threshold_seconds=0.1,
                )
                invalid_operational_diffs = generate_operational_difference_summary(
                    tl_a_invalid,
                    tl_b_invalid,
                    tolerance_seconds=0.2,
                )
        except Exception as e:
            analysis_diagnostics.append(f"Invalid-event timeline summary failed: {e}")
            logger.warning("Invalid-event breakdown failed for %s: %s", scenario_id, e, exc_info=True)

    if not timeline_difference_analysis_available and not analysis_diagnostics:
        analysis_diagnostics.append("Detailed timeline analysis was unavailable for this scenario.")

    # Keep the result small: the warping paths are only needed during compare_runs.
    for dtw_result in (getattr(result, "sequence_dtw", None), getattr(result, "timing_dtw", None)):
        if dtw_result is not None:
            dtw_result.warping_path = []

    return ScenarioResult(
        scenario_id=scenario_id,
        test_type=TestType.SIMILARITY,
        software_version=software_version,
        passed=passed,
        match_percentage=result.match_percentage,
        timing_match_percentage=timing_match if sequence_passed else None,
        timing_p95_error_seconds=(
            getattr(result, "timing_p95_error_seconds", None) if sequence_passed else None
        ),
        timing_max_error_seconds=(
            getattr(result, "timing_max_error_seconds", None) if sequence_passed else None
        ),
        num_divergences=len(result.divergence_windows),
        notes=_truncated_summary(result),
        notes_column=notes_column,
        plot_paths=plot_paths,
        plot_captions=plot_captions,
        phase_differences=phase_diffs,
        clearance_irregularities=clearance_irregularities,
        operational_differences=operational_diffs,
        invalid_clearance_irregularities=invalid_clearance_irregularities,
        invalid_operational_differences=invalid_operational_diffs,
        runs_completed=1,
        total_runs=1,
        chunk_scores=[
            {"center_seconds": c.center_seconds,
             "match_percentage": c.match_percentage,
             "window_seconds": c.window_seconds}
            for c in result.chunk_scores
        ],
        phase_call_chunk_scores=[
            {"center_seconds": c.center_seconds,
             "window_seconds": c.window_seconds,
             "similarity_percentage": c.similarity_percentage,
             "has_activity": c.has_activity,
             "excluded_from_match": c.excluded_from_match}
            for c in result.phase_call_chunk_scores
        ],
        included_chunk_count=result.included_chunk_count,
        excluded_chunk_count=result.excluded_chunk_count,
        thrown_out=result.thrown_out,
        thrown_out_reason=getattr(result, "thrown_out_reason", ""),
        analysis_diagnostics=analysis_diagnostics,
        timeline_difference_analysis_available=timeline_difference_analysis_available,
        sparkline_svg=render_sparkline_svg(
            result.chunk_scores,
            pass_threshold=95.0,
            base_timestamp=sparkline_base_timestamp,
            phase_call_chunk_scores=result.phase_call_chunk_scores,
            phase_call_threshold=phase_call_threshold,
        ) if result.chunk_scores else "",
        temporal_shift_seconds=result.temporal_shift_seconds,
        comparison=result,
    )


def _timed_compare(job: Dict[str, Any]) -> ScenarioResult:
    """Worker entry point: compare one scenario and record how long it took."""
    started = datetime.now()
    result = _compare_similarity(job)
    result.duration_seconds = (datetime.now() - started).total_seconds()
    return result


def _make_pool(processes: int):
    """Worker pool for similarity comparisons (a function so tests can swap it).

    A ``ProcessPoolExecutor`` is used rather than ``multiprocessing.Pool``
    because it reports a worker that dies (out of memory, native crash,
    killed) as ``BrokenProcessPool`` instead of leaving that job pending
    forever.
    """
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor

    return ProcessPoolExecutor(max_workers=processes, mp_context=multiprocessing.get_context("spawn"))


def _terminate_pool(pool: Any) -> None:
    """Stop a worker pool now, killing jobs that are still running."""
    terminate = getattr(pool, "terminate_workers", None)  # Python 3.14+
    if callable(terminate):
        with contextlib.suppress(Exception):
            terminate()
    else:
        processes = list((getattr(pool, "_processes", None) or {}).values())
        for process in processes:
            with contextlib.suppress(Exception):
                process.terminate()
        for process in processes:
            with contextlib.suppress(Exception):
                process.join(5)
    with contextlib.suppress(Exception):
        pool.shutdown(wait=False, cancel_futures=True)


_WORKER_DIED = "worker process died while comparing this scenario"
_NOT_RUN = object()


def _run_pool_batch(
    pool: Any,
    scenarios: Sequence[TestScenario],
    *,
    workers: int,
    make_job: Callable[[TestScenario, bool], Dict[str, Any]],
    check_stop: Callable[[], None],
) -> Dict[str, Tuple[TestScenario, Any]]:
    """Run similarity comparisons on ``pool``.

    Returns ``{scenario_id: (scenario, outcome)}`` where outcome is the
    ScenarioResult or the exception the job raised. When the pool breaks (a
    worker died), every job that was queued or running gets
    ``BrokenProcessPool``; scenarios not yet submitted get ``_NOT_RUN``.
    """
    pending = list(scenarios)
    in_flight: Dict[Any, TestScenario] = {}
    outcomes: Dict[str, Tuple[TestScenario, Any]] = {}
    # Load events just before a job is submitted so at most about two jobs
    # per worker are held in memory at once.
    limit = max(1, workers) * 2
    broken: Optional[BaseException] = None
    while (pending and broken is None) or in_flight:
        check_stop()
        while pending and broken is None and len(in_flight) < limit:
            scenario = pending.pop(0)
            try:
                job = make_job(scenario, True)
            except Exception as exc:
                outcomes[scenario.scenario_id] = (scenario, exc)
                continue
            try:
                in_flight[pool.submit(_timed_compare, job)] = scenario
            except BrokenProcessPool as exc:
                broken = exc
                outcomes[scenario.scenario_id] = (scenario, exc)
            del job
        if not in_flight:
            continue
        done, _ = futures_wait(list(in_flight), timeout=0.25, return_when=FIRST_COMPLETED)
        for future in done:
            scenario = in_flight.pop(future)
            try:
                outcomes[scenario.scenario_id] = (scenario, future.result())
            except BrokenProcessPool as exc:
                broken = exc
                outcomes[scenario.scenario_id] = (scenario, exc)
            except Exception as exc:
                outcomes[scenario.scenario_id] = (scenario, exc)
    for scenario in pending:
        outcomes[scenario.scenario_id] = (scenario, _NOT_RUN)
    return outcomes


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def _coerce_settings(settings: Any, suite: Optional[SoftwareTestSuite]) -> ValidationSettings:
    if isinstance(settings, ValidationSettings):
        return settings
    if isinstance(settings, SoftwareTestSuite):
        return ValidationSettings.from_suite(settings)
    if isinstance(settings, Mapping):
        return ValidationSettings.from_mapping(settings)
    if settings is None:
        return ValidationSettings.from_suite(suite) if suite is not None else ValidationSettings()
    raise TypeError(f"settings must be ValidationSettings, a SoftwareTestSuite or a dict, not {type(settings).__name__}")


def compare_validation(
    baseline: EventsInput,
    candidate: EventsInput,
    scenarios: Union[Sequence[TestScenario], SoftwareTestSuite],
    settings: Union[ValidationSettings, SoftwareTestSuite, Mapping[str, Any], None] = None,
    *,
    plots_dir: Optional[Union[str, os.PathLike]] = None,
    max_workers: Optional[int] = 1,
    on_progress: Optional[ProgressCallback] = None,
    stop_event: Optional[threading.Event] = None,
    baseline_label: Optional[str] = None,
    candidate_label: Optional[str] = None,
) -> List[ScenarioResult]:
    """Compare candidate software output with baseline output, scenario by scenario.

    Args:
        baseline: Baseline (old software) events; see :data:`EventsInput`.
        candidate: Candidate (new software) events; same forms.
        scenarios: Scenarios to compare (or a suite, whose scenarios and
            settings are used).
        settings: :class:`ValidationSettings`, a suite (its analysis
            settings), or a dict like ``settings.json['comparison']``.
        plots_dir: Folder for issue and divergence plots; None makes none.
        max_workers: Worker processes for similarity scenarios. 0 or 1 (the
            default) compares in this process, one after another; None uses
            one per CPU. Workers receive DataFrames only. A frozen Windows
            app that uses workers must call
            ``multiprocessing.freeze_support()``.
        on_progress: Optional callback: one COMPARE event per scenario
            (``scenario_id``, ``index``, ``total``, ``extra['passed']``),
            then DONE, or CANCELLED. Called on the calling thread.
        stop_event: Optional ``threading.Event``; when set, workers are
            terminated within about 0.25 s and
            :class:`~signal_replay.OperationCancelled` is raised.
        baseline_label / candidate_label: Override the version labels.

    Returns:
        One :class:`~signal_replay.ScenarioResult` per scenario, in the
        order given. A scenario without baseline or candidate events gets
        ``passed=False`` and ``error`` set. Similarity results carry the
        underlying ``comparison`` (with absolute divergence timestamps).
    """
    from .batch_runner import OperationCancelled

    suite = scenarios if isinstance(scenarios, SoftwareTestSuite) else None
    scenario_list = list(suite.scenarios if suite is not None else scenarios)
    cfg = _coerce_settings(settings, suite)
    overrides = {}
    if baseline_label is not None:
        overrides["baseline_label"] = baseline_label
    if candidate_label is not None:
        overrides["candidate_label"] = candidate_label
    if overrides:
        cfg = replace(cfg, **overrides)

    reporter = as_reporter(on_progress, log=logger)
    total = len(scenario_list)
    done = [0]
    plots = str(plots_dir) if plots_dir is not None else None
    if plots is not None:
        Path(plots).mkdir(parents=True, exist_ok=True)
    schedules = load_coord_split_schedules(cfg.coord_patterns_dir)

    def _stopped() -> bool:
        return stop_event is not None and stop_event.is_set()

    def _check_stop() -> None:
        if _stopped():
            raise _ValidationCancelled()

    def _report(result: ScenarioResult) -> None:
        done[0] += 1
        outcome = "error" if result.error else ("passed" if result.passed else "failed")
        match = "n/a" if result.match_percentage is None else f"{result.match_percentage:.1f}%"
        reporter.emit(
            Stage.COMPARE,
            f"Compared {result.scenario_id} ({done[0]}/{total}): {outcome} (match {match}, "
            f"{result.num_divergences} divergences)",
            scenario_id=result.scenario_id,
            index=done[0],
            total=total,
            extra={"passed": bool(result.passed), "test_type": result.test_type.value,
                   "error": result.error},
        )

    results: Dict[str, ScenarioResult] = {}
    base_loader = _EventLoader(baseline, "baseline")
    cand_loader = _EventLoader(candidate, "candidate")
    pool = None
    try:
        _check_stop()
        similarity: List[TestScenario] = []
        conflict: List[TestScenario] = []
        for scenario in scenario_list:
            sid = scenario.scenario_id
            missing = [side for side, loader in (("baseline", base_loader), ("candidate", cand_loader)) if not loader.has(sid)]
            if missing:
                results[sid] = ScenarioResult(
                    scenario_id=sid,
                    test_type=scenario.test_type,
                    software_version=cfg.candidate_label,
                    passed=False,
                    error=f"No {' or '.join(missing)} events source for {sid}",
                    notes_column=scenario.notes_column,
                )
                _report(results[sid])
                continue
            (conflict if scenario.test_type == TestType.CONFLICT else similarity).append(scenario)

        def _job(scenario: TestScenario, quiet: bool) -> Dict[str, Any]:
            sid = scenario.scenario_id
            return {
                "scenario_id": sid,
                "baseline": base_loader.load(sid),
                "candidate": cand_loader.load(sid),
                "candidate_source": cand_loader.label(sid),
                "settings": cfg,
                "notes_column": scenario.notes_column,
                "tod_align": scenario.tod_align,
                "plots_dir": plots,
                "coord_split_schedules": schedules,
                "quiet": quiet,
            }

        def _error_result(scenario: TestScenario, exc: BaseException) -> ScenarioResult:
            logger.warning("Comparison failed for %s", scenario.scenario_id, exc_info=exc)
            return ScenarioResult(
                scenario_id=scenario.scenario_id,
                test_type=scenario.test_type,
                software_version=cfg.candidate_label,
                passed=False,
                error=f"{type(exc).__name__}: {exc}",
                notes_column=scenario.notes_column,
            )

        workers = max_workers if max_workers is not None else (os.cpu_count() or 1)
        workers = max(0, min(int(workers), len(similarity)))
        if workers <= 1:
            for scenario in similarity:
                _check_stop()
                try:
                    result = _timed_compare(_job(scenario, quiet=False))
                except Exception as exc:
                    result = _error_result(scenario, exc)
                results[scenario.scenario_id] = result
                _report(result)
        elif similarity:
            pending = list(similarity)
            # Scenarios that were queued or running when a worker died. Any of
            # them may have killed it, so each is re-run alone in a fresh
            # pool; one that kills its worker again becomes an error result.
            suspects: List[TestScenario] = []
            while pending or suspects:
                isolate = not pending
                batch = [suspects.pop(0)] if isolate else pending
                pending = []
                batch_workers = 1 if isolate else workers
                pool = _make_pool(batch_workers)
                outcomes = _run_pool_batch(
                    pool, batch, workers=batch_workers, make_job=_job, check_stop=_check_stop,
                )
                pool.shutdown(wait=True)
                pool = None
                for sid, (scenario, outcome) in outcomes.items():
                    if outcome is _NOT_RUN:
                        pending.append(scenario)
                        continue
                    if isinstance(outcome, ScenarioResult):
                        results[sid] = outcome
                    elif isinstance(outcome, BrokenProcessPool) and not isolate:
                        suspects.append(scenario)
                        continue
                    else:
                        if isinstance(outcome, BrokenProcessPool):
                            outcome = RuntimeError(_WORKER_DIED)
                        results[sid] = _error_result(scenario, outcome)
                    _report(results[sid])
                if suspects and not isolate:
                    logger.warning(
                        "A comparison worker process died; re-running %d scenario(s) one at a time",
                        len(suspects),
                    )

        for scenario in conflict:
            _check_stop()
            sid = scenario.scenario_id
            try:
                started = datetime.now()
                result = _analyze_conflict_scenario(
                    scenario,
                    base_loader.load(sid),
                    cand_loader.load(sid),
                    baseline_label=cfg.baseline_label,
                    software_version=cfg.candidate_label,
                    collected_label=cand_loader.label(sid),
                )
                result.duration_seconds = (datetime.now() - started).total_seconds()
            except Exception as exc:
                result = _error_result(scenario, exc)
            results[sid] = result
            _report(result)
    except (_ValidationCancelled, KeyboardInterrupt) as exc:
        if pool is not None:
            _terminate_pool(pool)
            pool = None
        reporter.emit(
            Stage.CANCELLED, "Software comparison cancelled",
            level=logging.WARNING, log=False, total=total, index=done[0],
            extra={"final": True},
        )
        if isinstance(exc, KeyboardInterrupt):
            raise
        raise OperationCancelled("Software comparison cancelled") from None
    finally:
        if pool is not None:
            _terminate_pool(pool)
        base_loader.close()
        cand_loader.close()

    ordered = [results[s.scenario_id] for s in scenario_list if s.scenario_id in results]
    passed = sum(1 for r in ordered if r.passed)
    reporter.emit(
        Stage.DONE,
        f"Software comparison complete: {passed}/{len(ordered)} scenario(s) passed",
        log=False,
        total=len(ordered),
        index=len(ordered),
        extra={"passed": passed, "failed": len(ordered) - passed, "final": True},
    )
    return ordered
