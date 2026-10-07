"""
Typed, JSON-safe results and tidy DataFrames for an application's own database.

:meth:`ATCSimulation.run` returns a :class:`ReplicationResult`;
:func:`~signal_replay.compare_validation` returns a list of
:class:`~signal_replay.ScenarioResult`. Both serialise with ``to_dict()``
(ISO-8601 timestamps, ``inf``/NaN as None) and turn into fixed-column
DataFrames with :func:`results_to_frames`, ready for
``INSERT INTO app_table SELECT ... FROM df`` on the application's own
DuckDB connection.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Union

import pandas as pd

from ._serialize import known_fields, parse_datetime, to_jsonable
from .collector import ConflictRecord, SCHEMA_VERSION
from .comparison import ComparisonResult
from .test_suite import ScenarioResult

__all__ = [
    "RunRecord",
    "ReplicationResult",
    "FRAME_COLUMNS",
    "results_to_frames",
    "new_run_uuid",
]


def new_run_uuid() -> str:
    """A new random run identifier (hex UUID4)."""
    return uuid.uuid4().hex


@dataclass
class RunRecord:
    """One device's part of one replay run.

    Attributes:
        device_id: Device (signal / scenario) replayed.
        run_number: Run number.
        status: ``completed``, ``incomplete`` (output events not confirmed
            complete), ``cancelled`` or ``failed``.
        replay_start / replay_end: PC time sending started / ended.
        source_start / source_end: Source-log timestamps of the first and
            last event sent.
        date_shift_seconds: Replay time minus source time (whole days for
            time-of-day replays). Subtract it from a controller timestamp to
            get the matching moment in the source log.
        events_sent / events_total: Detector commands sent / scheduled.
        mode: ``'tod'`` (time-of-day aligned) or ``'relative'``.
        speed: Simulation speed multiplier.
        detectors_reset: Whether the end-of-replay detector reset was
            confirmed (None when unknown).
    """

    device_id: str
    run_number: int
    status: str
    replay_start: Optional[datetime] = None
    replay_end: Optional[datetime] = None
    source_start: Optional[datetime] = None
    source_end: Optional[datetime] = None
    date_shift_seconds: Optional[float] = None
    events_sent: Optional[int] = None
    events_total: Optional[int] = None
    mode: Optional[str] = None
    speed: float = 1.0
    detectors_reset: Optional[bool] = None

    _TIMESTAMP_FIELDS = ("replay_start", "replay_end", "source_start", "source_end")

    def source_time(self, timestamp: Optional[datetime]) -> Optional[datetime]:
        """Map a collected (controller) timestamp of this run to the source log's clock."""
        if timestamp is None or self.date_shift_seconds is None:
            return None
        ts = pd.Timestamp(timestamp)
        if self.mode == "relative" and self.speed not in (0, 1.0) and self.replay_start is not None:
            replay_start = pd.Timestamp(self.replay_start)
            anchor = replay_start - pd.Timedelta(seconds=self.date_shift_seconds)
            return (anchor + (ts - replay_start) * self.speed).to_pydatetime()
        return (ts - pd.Timedelta(seconds=self.date_shift_seconds)).to_pydatetime()

    def to_dict(self) -> Dict[str, Any]:
        """JSON-safe dict."""
        return to_jsonable(self, drop_keys=("_TIMESTAMP_FIELDS",))

    @classmethod
    def from_dict(cls, data: Mapping) -> "RunRecord":
        kwargs = known_fields(cls, data)
        for name in cls._TIMESTAMP_FIELDS:
            kwargs[name] = parse_datetime(kwargs.get(name))
        return cls(**kwargs)


# Keys of the 0.x ``run()`` dict, served by ReplicationResult[...] unchanged.
_LEGACY_KEYS = (
    "completed_runs",
    "incomplete_runs",
    "conflicts",
    "failed_signals_by_run",
    "stopped_early",
    "stop_reason",
    "cancelled",
    "cancel_reason",
    "cancelled_run",
    "collection_error",
    "detectors_reset",
    "collection_health",
    "collection_health_by_run",
    "comparison_summary",
)
# New scalar keys also available through ReplicationResult[...].
_NEW_KEYS = (
    "run_uuid",
    "replicated",
    "first_conflict_run",
    "runs_attempted",
    "runs_completed",
    "conflict_store_errors",
    "db_path",
    "work_dir",
)


@dataclass(eq=False)
class ReplicationResult(Mapping):
    """Result of :meth:`ATCSimulation.run`.

    The attributes are the typed result. For 0.x code the object is also a
    read-only mapping with the old dict keys (``result['completed_runs']``,
    ``result['conflicts']`` as a list of dicts, ...), plus ``run_uuid``,
    ``replicated``, ``first_conflict_run``, ``runs_attempted``,
    ``runs_completed``, ``conflict_store_errors``, ``db_path`` and
    ``work_dir``. ``dict(result)`` gives that dict.

    Attributes:
        run_uuid: Identifier of this ``run()`` call (also written to the
            working database's ``meta`` table and each run row).
        stop_reason: completed | conflict | cancelled | collection_error |
            all_signals_failed.
        replicated: True when any conflict was found (the failure was
            reproduced), including conflicts stored by an earlier call for
            runs a resumed simulation did not run again.
        first_conflict_run: Lowest run number with a conflict, or None.
        runs_attempted: Runs started by this call.
        runs_completed: Runs whose replay finished and whose conflict check
            ran (``len(completed_runs)``; includes incomplete runs).
        runs: One :class:`RunRecord` per device per run attempted.
        conflicts: :class:`~signal_replay.ConflictRecord` list, with first
            and last timestamp, occurrences, duration and source-equivalent
            timestamp. On a resume it also holds the conflicts already
            stored for the runs that were done before this call.
        prior_completed_runs: Runs that were already done for every device
            when this call started (a resumed simulation); empty otherwise.
        comparisons: :class:`~signal_replay.ComparisonResult` list (empty
            when comparison was skipped).
        conflict_store_errors: Conflicts that were found but could not be
            written to the working database (the in-memory result still has
            them).
    """

    run_uuid: str
    stop_reason: str
    replicated: bool = False
    first_conflict_run: Optional[int] = None
    runs_attempted: int = 0
    runs_completed: int = 0
    runs: List[RunRecord] = field(default_factory=list)
    conflicts: List[ConflictRecord] = field(default_factory=list)
    comparisons: List[ComparisonResult] = field(default_factory=list)
    completed_runs: List[int] = field(default_factory=list)
    incomplete_runs: List[int] = field(default_factory=list)
    failed_signals_by_run: Dict[int, List[str]] = field(default_factory=dict)
    stopped_early: bool = False
    cancelled: bool = False
    cancel_reason: Optional[str] = None
    cancelled_run: Optional[int] = None
    collection_error: bool = False
    detectors_reset: Dict[str, bool] = field(default_factory=dict)
    collection_health: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    collection_health_by_run: Dict[int, Dict[str, Dict[str, Any]]] = field(default_factory=dict)
    conflict_store_errors: List[str] = field(default_factory=list)
    prior_completed_runs: List[int] = field(default_factory=list)
    comparison_summary: str = ""
    db_path: Optional[str] = None
    work_dir: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    package_version: Optional[str] = None
    schema_version: int = SCHEMA_VERSION

    # -- 0.x dict view -----------------------------------------------------

    def _legacy_value(self, key: str) -> Any:
        if key == "conflicts":
            return [c.as_legacy_dict() for c in self.conflicts]
        return getattr(self, key)

    def __getitem__(self, key: str) -> Any:
        if key in _LEGACY_KEYS or key in _NEW_KEYS:
            return self._legacy_value(key)
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return iter(_LEGACY_KEYS + _NEW_KEYS)

    def __len__(self) -> int:
        return len(_LEGACY_KEYS) + len(_NEW_KEYS)

    def __repr__(self) -> str:
        return (
            f"ReplicationResult(run_uuid={self.run_uuid!r}, stop_reason={self.stop_reason!r}, "
            f"replicated={self.replicated}, first_conflict_run={self.first_conflict_run}, "
            f"runs_completed={self.runs_completed}/{self.runs_attempted}, conflicts={len(self.conflicts)})"
        )

    # -- serialisation -----------------------------------------------------

    def to_dict(self, include_warping_path: bool = False) -> Dict[str, Any]:
        """JSON-safe dict of every attribute (``json.dumps(..., allow_nan=False)`` works)."""
        data = to_jsonable(self, drop_keys=("runs", "conflicts", "comparisons"))
        data["runs"] = [r.to_dict() for r in self.runs]
        data["conflicts"] = [c.to_dict() for c in self.conflicts]
        data["comparisons"] = [c.to_dict(include_warping_path) for c in self.comparisons]
        return data

    def to_json(self, **kwargs: Any) -> str:
        """``json.dumps(self.to_dict(), allow_nan=False, ...)``."""
        kwargs.setdefault("allow_nan", False)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(cls, data: Mapping) -> "ReplicationResult":
        """Rebuild a result from :meth:`to_dict` output."""
        kwargs = known_fields(cls, data)
        kwargs["runs"] = [RunRecord.from_dict(r) for r in data.get("runs") or []]
        kwargs["conflicts"] = [ConflictRecord.from_dict(c) for c in data.get("conflicts") or []]
        kwargs["comparisons"] = [ComparisonResult.from_dict(c) for c in data.get("comparisons") or []]
        kwargs["failed_signals_by_run"] = {
            int(k): list(v) for k, v in (data.get("failed_signals_by_run") or {}).items()
        }
        kwargs["collection_health_by_run"] = {
            int(k): v for k, v in (data.get("collection_health_by_run") or {}).items()
        }
        for name in ("started_at", "finished_at"):
            kwargs[name] = parse_datetime(kwargs.get(name))
        return cls(**kwargs)


# ---------------------------------------------------------------------------
# Tidy frames
# ---------------------------------------------------------------------------

#: Column lists of the frames returned by :func:`results_to_frames`. Every
#: frame starts with ``run_uuid``; columns ending in ``_timestamp``,
#: ``_start``/``_end`` (runs) are ``datetime64[ns]`` (naive, local PC time),
#: counts are ``Int64``, flags are ``boolean``, scores are ``float64``.
FRAME_COLUMNS: Dict[str, List[str]] = {
    "runs": [
        "run_uuid", "device_id", "run_number", "status", "replay_start", "replay_end",
        "source_start", "source_end", "date_shift_seconds", "events_sent", "events_total",
        "mode", "speed", "detectors_reset",
    ],
    "conflicts": [
        "run_uuid", "scenario_id", "device_id", "run_number", "conflict_details",
        "first_timestamp", "last_timestamp", "duration_seconds", "occurrences",
        "source_equivalent_timestamp", "stored",
    ],
    "comparison_scores": [
        "run_uuid", "comparison_key", "scenario_id", "device_id", "run_a", "run_b",
        "match_percentage", "timing_match_percentage", "timing_p95_error_seconds",
        "timing_max_error_seconds", "sequence_dtw_distance", "timing_dtw_distance",
        "num_divergences", "thrown_out", "thrown_out_reason", "included_chunk_count",
        "excluded_chunk_count", "temporal_shift_seconds", "exceeds_threshold",
        "threshold_reason", "plot_path",
    ],
    "divergence_windows": [
        "run_uuid", "comparison_key", "scenario_id", "device_id", "window_index",
        "start_seconds_a", "end_seconds_a", "start_seconds_b", "end_seconds_b",
        "start_timestamp_a", "end_timestamp_a", "start_timestamp_b", "end_timestamp_b",
        "description",
    ],
    "chunk_scores": [
        "run_uuid", "comparison_key", "scenario_id", "device_id", "kind", "chunk_index",
        "center_seconds", "window_seconds", "match_percentage", "similarity_percentage",
        "has_activity", "excluded_from_match",
    ],
    "scenario_results": [
        "run_uuid", "scenario_id", "test_type", "software_version", "passed", "error",
        "match_percentage", "timing_match_percentage", "timing_p95_error_seconds",
        "timing_max_error_seconds", "num_divergences", "conflicts_found", "runs_completed",
        "total_runs", "thrown_out", "thrown_out_reason", "included_chunk_count",
        "excluded_chunk_count", "temporal_shift_seconds", "timeline_difference_analysis_available",
        "duration_seconds", "notes", "notes_column",
    ],
    "scenario_findings": [
        "run_uuid", "scenario_id", "kind", "item_index", "payload",
    ],
    "plots": [
        "run_uuid", "scenario_id", "plot_index", "path", "caption",
    ],
}

_DATETIME_COLUMNS = {
    "replay_start", "replay_end", "source_start", "source_end", "first_timestamp",
    "last_timestamp", "source_equivalent_timestamp", "start_timestamp_a", "end_timestamp_a",
    "start_timestamp_b", "end_timestamp_b",
}
_INT_COLUMNS = {
    "run_number", "events_sent", "events_total", "occurrences", "num_divergences",
    "included_chunk_count", "excluded_chunk_count", "window_index", "chunk_index",
    "conflicts_found", "runs_completed", "total_runs", "item_index", "plot_index",
}
_BOOL_COLUMNS = {
    "detectors_reset", "stored", "thrown_out", "exceeds_threshold", "passed",
    "timeline_difference_analysis_available", "has_activity", "excluded_from_match",
}
_FLOAT_COLUMNS = {
    "date_shift_seconds", "speed", "duration_seconds", "match_percentage",
    "timing_match_percentage", "timing_p95_error_seconds", "timing_max_error_seconds",
    "sequence_dtw_distance", "timing_dtw_distance", "temporal_shift_seconds",
    "start_seconds_a", "end_seconds_a", "start_seconds_b", "end_seconds_b",
    "center_seconds", "window_seconds", "similarity_percentage",
}

#: ``scenario_findings.kind`` values and the ScenarioResult lists they come from.
FINDING_KINDS = {
    "phase_difference": "phase_differences",
    "clearance_irregularity": "clearance_irregularities",
    "operational_difference": "operational_differences",
    "invalid_clearance_irregularity": "invalid_clearance_irregularities",
    "invalid_operational_difference": "invalid_operational_differences",
    "analysis_diagnostic": "analysis_diagnostics",
}


def _frame(name: str, rows: List[Dict[str, Any]]) -> pd.DataFrame:
    columns = FRAME_COLUMNS[name]
    df = pd.DataFrame(rows, columns=columns)
    for column in columns:
        if column in _DATETIME_COLUMNS:
            df[column] = pd.to_datetime(df[column], errors="coerce").astype("datetime64[ns]")
        elif column in _INT_COLUMNS:
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
        elif column in _BOOL_COLUMNS:
            df[column] = df[column].astype("boolean")
        elif column in _FLOAT_COLUMNS:
            values = pd.to_numeric(df[column], errors="coerce").astype("float64")
            df[column] = values.where(values.abs() != float("inf"))
        else:
            # Text columns use the pandas string dtype so even an empty frame
            # maps to VARCHAR (an all-null object column becomes INTEGER in DuckDB).
            df[column] = df[column].astype("object").where(df[column].notna(), None).astype("string")
    return df


def _finite(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return number


def _comparison_key(scenario_id: Optional[str], comparison: ComparisonResult) -> str:
    prefix = f"{scenario_id}:" if scenario_id else ""
    return f"{prefix}{comparison.device_id}:{comparison.run_a}:{comparison.run_b}"


def _comparison_rows(
    run_uuid: str,
    scenario_id: Optional[str],
    comparison: ComparisonResult,
    rows: Dict[str, List[Dict[str, Any]]],
) -> None:
    key = _comparison_key(scenario_id, comparison)
    common = {"run_uuid": run_uuid, "comparison_key": key, "scenario_id": scenario_id,
              "device_id": str(comparison.device_id)}
    rows["comparison_scores"].append({
        **common,
        "run_a": str(comparison.run_a),
        "run_b": str(comparison.run_b),
        "match_percentage": _finite(comparison.match_percentage),
        "timing_match_percentage": _finite(comparison.timing_match_percentage),
        "timing_p95_error_seconds": _finite(comparison.timing_p95_error_seconds),
        "timing_max_error_seconds": _finite(comparison.timing_max_error_seconds),
        "sequence_dtw_distance": _finite(comparison.sequence_dtw.normalized_distance),
        "timing_dtw_distance": _finite(comparison.timing_dtw.normalized_distance),
        "num_divergences": len(comparison.divergence_windows),
        "thrown_out": bool(comparison.thrown_out),
        "thrown_out_reason": comparison.thrown_out_reason or None,
        "included_chunk_count": comparison.included_chunk_count,
        "excluded_chunk_count": comparison.excluded_chunk_count,
        "temporal_shift_seconds": _finite(comparison.temporal_shift_seconds),
        "exceeds_threshold": bool(comparison.exceeds_threshold),
        "threshold_reason": comparison.threshold_reason or None,
        "plot_path": comparison.plot_path,
    })
    for index, window in enumerate(comparison.divergence_windows, start=1):
        rows["divergence_windows"].append({
            **common,
            "window_index": index,
            "start_seconds_a": window.original_start_seconds_a,
            "end_seconds_a": window.original_end_seconds_a,
            "start_seconds_b": window.original_start_seconds_b,
            "end_seconds_b": window.original_end_seconds_b,
            "start_timestamp_a": window.start_timestamp_a,
            "end_timestamp_a": window.end_timestamp_a,
            "start_timestamp_b": window.start_timestamp_b,
            "end_timestamp_b": window.end_timestamp_b,
            "description": window.description or None,
        })
    _chunk_rows(common, comparison.chunk_scores, comparison.phase_call_chunk_scores, rows)


def _chunk_rows(
    common: Dict[str, Any],
    chunk_scores: Iterable[Any],
    phase_call_chunk_scores: Iterable[Any],
    rows: Dict[str, List[Dict[str, Any]]],
) -> None:
    def _get(item: Any, name: str) -> Any:
        return item.get(name) if isinstance(item, Mapping) else getattr(item, name, None)

    for index, chunk in enumerate(chunk_scores, start=1):
        rows["chunk_scores"].append({
            **common, "kind": "match", "chunk_index": index,
            "center_seconds": _get(chunk, "center_seconds"),
            "window_seconds": _get(chunk, "window_seconds"),
            "match_percentage": _finite(_get(chunk, "match_percentage")),
            "similarity_percentage": None, "has_activity": None, "excluded_from_match": None,
        })
    for index, chunk in enumerate(phase_call_chunk_scores, start=1):
        has_activity = _get(chunk, "has_activity")
        excluded = _get(chunk, "excluded_from_match")
        rows["chunk_scores"].append({
            **common, "kind": "phase_call", "chunk_index": index,
            "center_seconds": _get(chunk, "center_seconds"),
            "window_seconds": _get(chunk, "window_seconds"),
            "match_percentage": None,
            "similarity_percentage": _finite(_get(chunk, "similarity_percentage")),
            "has_activity": None if has_activity is None else bool(has_activity),
            "excluded_from_match": None if excluded is None else bool(excluded),
        })


def _conflict_row(run_uuid: str, scenario_id: Optional[str], conflict: ConflictRecord) -> Dict[str, Any]:
    return {
        "run_uuid": run_uuid,
        "scenario_id": scenario_id,
        "device_id": str(conflict.device_id),
        "run_number": conflict.run_number,
        "conflict_details": conflict.conflict_details,
        "first_timestamp": conflict.timestamp,
        "last_timestamp": conflict.last_timestamp,
        "duration_seconds": conflict.duration_seconds,
        "occurrences": conflict.occurrences,
        "source_equivalent_timestamp": conflict.source_equivalent_timestamp,
        "stored": conflict.stored,
    }


def _scenario_rows(run_uuid: str, result: ScenarioResult, rows: Dict[str, List[Dict[str, Any]]]) -> None:
    sid = result.scenario_id
    test_type = getattr(result.test_type, "value", result.test_type)
    rows["scenario_results"].append({
        "run_uuid": run_uuid,
        "scenario_id": sid,
        "test_type": str(test_type),
        "software_version": result.software_version,
        "passed": bool(result.passed),
        "error": result.error,
        "match_percentage": _finite(result.match_percentage),
        "timing_match_percentage": _finite(result.timing_match_percentage),
        "timing_p95_error_seconds": _finite(result.timing_p95_error_seconds),
        "timing_max_error_seconds": _finite(result.timing_max_error_seconds),
        "num_divergences": result.num_divergences,
        "conflicts_found": len(result.conflicts_found or []),
        "runs_completed": result.runs_completed,
        "total_runs": result.total_runs,
        "thrown_out": bool(result.thrown_out),
        "thrown_out_reason": result.thrown_out_reason or None,
        "included_chunk_count": result.included_chunk_count,
        "excluded_chunk_count": result.excluded_chunk_count,
        "temporal_shift_seconds": _finite(result.temporal_shift_seconds),
        "timeline_difference_analysis_available": bool(result.timeline_difference_analysis_available),
        "duration_seconds": _finite(result.duration_seconds),
        "notes": result.notes or None,
        "notes_column": result.notes_column or None,
    })
    for kind, attribute in FINDING_KINDS.items():
        for index, item in enumerate(getattr(result, attribute, None) or [], start=1):
            payload = item if isinstance(item, Mapping) else {"text": item}
            rows["scenario_findings"].append({
                "run_uuid": run_uuid, "scenario_id": sid, "kind": kind, "item_index": index,
                "payload": json.dumps(to_jsonable(payload), allow_nan=False, sort_keys=True),
            })
    for index, path in enumerate(result.plot_paths or [], start=1):
        captions = result.plot_captions or []
        rows["plots"].append({
            "run_uuid": run_uuid, "scenario_id": sid, "plot_index": index, "path": str(path),
            "caption": captions[index - 1] if index - 1 < len(captions) else None,
        })
    for conflict in result.conflicts_found or []:
        record = conflict if isinstance(conflict, ConflictRecord) else ConflictRecord.from_dict({
            "device_id": sid,
            "run_number": conflict.get("run_number") or 1,
            "conflict_details": conflict.get("conflict_details", ""),
            **{k: conflict.get(k) for k in (
                "timestamp", "last_timestamp", "occurrences", "duration_seconds",
                "source_equivalent_timestamp",
            ) if conflict.get(k) is not None},
        })
        rows["conflicts"].append(_conflict_row(run_uuid, sid, record))
    if result.comparison is not None:
        _comparison_rows(run_uuid, sid, result.comparison, rows)
    elif result.chunk_scores or result.phase_call_chunk_scores:
        common = {"run_uuid": run_uuid, "comparison_key": f"{sid}:scenario", "scenario_id": sid,
                  "device_id": sid}
        _chunk_rows(common, result.chunk_scores or [], result.phase_call_chunk_scores or [], rows)


def results_to_frames(
    result: Union[ReplicationResult, ScenarioResult, Sequence[ScenarioResult], ComparisonResult, Sequence[ComparisonResult]],
    run_uuid: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """Tidy DataFrames of a result, with fixed columns (see :data:`FRAME_COLUMNS`).

    Every frame named in :data:`FRAME_COLUMNS` is returned (empty when the
    result has no such rows), and every frame carries ``run_uuid``:

    * ``runs``: one row per device per run (replication results).
    * ``conflicts``: one row per conflict signature per device and run.
    * ``comparison_scores``: one row per comparison (``comparison_key`` is
      ``[scenario_id:]device_id:run_a:run_b``).
    * ``divergence_windows``: one row per divergence window, with seconds
      from each side's start and absolute timestamps.
    * ``chunk_scores``: rolling-window scores; ``kind`` is ``match`` or
      ``phase_call``.
    * ``scenario_results``: one row per validation scenario (flat scalars).
    * ``scenario_findings``: long format; ``kind`` is one of
      :data:`FINDING_KINDS` and ``payload`` is the item as JSON text.
    * ``plots``: plot files with captions.

    Args:
        result: A :class:`ReplicationResult`, one or more
            :class:`~signal_replay.ScenarioResult`, or one or more
            :class:`~signal_replay.ComparisonResult`.
        run_uuid: Identifier written to every row. Defaults to the
            replication result's ``run_uuid``; otherwise a new UUID.

    Example (application side)::

        frames = results_to_frames(result)
        for name, df in frames.items():
            con.register("df", df)
            con.execute(f"INSERT INTO replay_{name} SELECT * FROM df")
            con.unregister("df")
    """
    rows: Dict[str, List[Dict[str, Any]]] = {name: [] for name in FRAME_COLUMNS}
    if isinstance(result, ReplicationResult):
        run_uuid = run_uuid or result.run_uuid
        for run in result.runs:
            rows["runs"].append({"run_uuid": run_uuid, **{k: v for k, v in run.to_dict().items()}})
            rows["runs"][-1].update({k: getattr(run, k) for k in RunRecord._TIMESTAMP_FIELDS})
        for conflict in result.conflicts:
            rows["conflicts"].append(_conflict_row(run_uuid, None, conflict))
        for comparison in result.comparisons:
            _comparison_rows(run_uuid, None, comparison, rows)
    else:
        run_uuid = run_uuid or new_run_uuid()
        items = [result] if isinstance(result, (ScenarioResult, ComparisonResult)) else list(result)
        for item in items:
            if isinstance(item, ScenarioResult):
                _scenario_rows(run_uuid, item, rows)
            elif isinstance(item, ComparisonResult):
                _comparison_rows(run_uuid, None, item, rows)
            else:
                raise TypeError(f"Cannot build frames from {type(item).__name__}")
    return {name: _frame(name, rows[name]) for name in FRAME_COLUMNS}
