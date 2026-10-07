"""
Progress events and status snapshots for embedding signal_replay in an app.

Two ways to follow a run from outside:

* **Push** - pass ``on_progress=callback`` to :class:`~signal_replay.ATCSimulation`,
  :class:`~signal_replay.BatchRunner` or :func:`~signal_replay.compare_software`.
  The callback receives :class:`ProgressEvent` objects.
* **Poll** - call ``get_status()`` on the simulation or runner from any thread.
  It returns a plain, JSON-safe dict (see :class:`StatusTracker`), suitable
  for a REST endpoint.

Threading contract
------------------
Callbacks run on the package's worker threads (replay threads, the
collection thread and the thread that called ``run()``), never on the app's
UI thread. A GUI must marshal each event to its UI thread itself, for
example with ``queue.put(event)`` or a Qt signal. Events are frozen
dataclasses holding only plain values, so they are safe to queue.

A callback should return quickly. An exception raised by a callback is
logged with ``logger.exception`` and otherwise ignored: it never stops a
replay or a collection thread. High-frequency replay progress is throttled
to about one event per second per device; ``get_status()`` always reflects
the latest values, including throttled ones.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)

#: Minimum seconds between two throttled events with the same key
#: (replay progress, wait countdowns). Read when a reporter is created.
DEFAULT_MIN_INTERVAL_SECONDS = 1.0

#: Values of ``get_status()['state']``.
STATES = ("idle", "running", "stopping", "cancelled", "completed", "failed")

# Most recent conflicts kept in a status snapshot.
_MAX_STATUS_CONFLICTS = 100


class Stage(str, Enum):
    """What a :class:`ProgressEvent` reports."""

    SETUP = "setup"                        # simulation being configured
    STORE_INPUT = "store_input"            # source events stored for comparison
    RUN_START = "run_start"                # replay run N of M starting
    DETECTOR_RESET = "detector_reset"      # detectors reset before / after a replay
    WAITING = "waiting"                    # TOD, cycle-align or settle wait (seconds_until_start)
    REPLAY = "replay"                      # events_sent / events_total for one device
    COLLECT = "collect"                    # one output-event poll for one device
    FINAL_COLLECTION = "final_collection"  # waiting for the source to report complete data
    CONFLICT = "conflict"                  # conflicting outputs detected
    SIGNAL_FAILED = "signal_failed"        # one device's replay raised
    RUN_COMPLETE = "run_complete"          # replay run N finished
    COMPARE = "compare"                    # comparison analysis
    PLOT = "plot"                          # comparison plot written (or failed)
    AWAITING_DB_LOAD = "awaiting_db_load"  # BatchRunner waits for a controller database load
    BATCH = "batch"                        # BatchRunner batch / scenario start and end
    CANCELLED = "cancelled"                # the operation was cancelled (terminal)
    ERROR = "error"                        # an error (terminal when the operation failed)
    DONE = "done"                          # the operation finished (terminal)


def _json_safe(value: Any) -> Any:
    """Convert ``value`` to plain JSON types (datetimes become ISO strings)."""
    if isinstance(value, Enum):
        return _json_safe(value.value)
    if value is None or isinstance(value, (bool, str)):
        return value if not isinstance(value, str) else str(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return value if value == value and value not in (float("inf"), float("-inf")) else None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(v) for v in value]
    # numpy / pandas scalars
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _json_safe(item())
        except Exception:
            pass
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except Exception:
            pass
    return str(value)


@dataclass(frozen=True)
class ProgressEvent:
    """One progress report.

    Every field except ``stage`` and ``message`` is optional; which ones
    are set depends on the stage. Callbacks run on worker threads (see the
    module docstring).

    Attributes:
        stage: What is being reported (:class:`Stage`).
        message: Human-readable ASCII text (also written to the log).
        level: ``logging`` level (INFO, WARNING, ERROR).
        run_number: Replay run this event belongs to.
        total_runs: Number of runs requested.
        device_id: Device (signal or scenario) the event is about.
        events_sent: REPLAY: commands sent so far for ``device_id``.
        events_total: REPLAY: commands this replay will send.
        seconds_until_start: WAITING: seconds until the replay starts sending.
        batch_id: BatchRunner batch.
        scenario_id: BatchRunner / comparison scenario.
        index: 1-based position in a sequence (batches, DB loads, comparisons).
        total: Length of that sequence.
        timestamp: When the event was created (local, naive).
        extra: Stage-specific detail with plain values (for example
            ``rows`` for COLLECT or ``conflicts`` for CONFLICT).
    """

    stage: Stage
    message: str
    level: int = logging.INFO
    run_number: Optional[int] = None
    total_runs: Optional[int] = None
    device_id: Optional[str] = None
    events_sent: Optional[int] = None
    events_total: Optional[int] = None
    seconds_until_start: Optional[float] = None
    batch_id: Optional[str] = None
    scenario_id: Optional[str] = None
    index: Optional[int] = None
    total: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.now)
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "stage", Stage(self.stage))
        object.__setattr__(self, "extra", dict(self.extra or {}))

    @property
    def fraction(self) -> Optional[float]:
        """Progress in [0, 1]: events_sent/events_total, else index/total, else None."""
        for done, total in ((self.events_sent, self.events_total), (self.index, self.total)):
            if done is not None and total:
                return max(0.0, min(1.0, float(done) / float(total)))
        return None

    @property
    def level_name(self) -> str:
        return logging.getLevelName(self.level)

    def to_dict(self) -> Dict[str, Any]:
        """The event as a JSON-safe dict (adds ``fraction`` and ``level_name``)."""
        data = _json_safe(asdict(self))
        data["fraction"] = self.fraction
        data["level_name"] = self.level_name
        return data


ProgressCallback = Callable[[ProgressEvent], None]


class StatusTracker:
    """Thread-safe latest-status record, fed by :class:`ProgressReporter`.

    :meth:`snapshot` (exposed as ``get_status()`` on the simulation and the
    batch runner) returns a JSON-safe dict:

    * ``state``: one of ``idle``, ``running``, ``stopping``, ``cancelled``,
      ``completed``, ``failed``
    * ``stage``, ``message``, ``level``, ``updated_at``: the latest event
    * ``run_number``, ``total_runs``, ``batch_id``, ``scenario_id``,
      ``index``, ``total``: latest known position
    * ``devices``: {device_id: {stage, events_sent, events_total,
      seconds_until_start, message}} for the current run
    * ``seconds_until_start``: longest remaining start wait over all devices
      (counts down live), or None
    * ``collection``: {device_id: health of the latest poll (rows,
      total_rows, polls, failures, consecutive_failures, complete_through,
      last_success, last_error, degraded)}
    * ``final_collection``: ``{waiting, needed, devices: {device_id:
      complete_through}, seconds_left}`` while the run waits for its source
      to report complete data; ``waiting`` is False once that wait ends
    * ``conflicts_found`` and ``conflicts`` (latest 100)
    * ``failed_signals``: devices whose replay failed in the current run
    * ``started_at``, ``finished_at``, ``elapsed_seconds``
    * ``stop_reason`` and ``error`` (when set)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._reset(state="idle")

    def _reset(self, state: str) -> None:
        self._state = state
        self._started_at: Optional[datetime] = None
        self._started_mono: Optional[float] = None
        self._finished_at: Optional[datetime] = None
        self._finished_mono: Optional[float] = None
        self._latest: Dict[str, Any] = {"stage": None, "message": None, "level": None, "updated_at": None}
        self._position: Dict[str, Any] = {
            "run_number": None, "total_runs": None, "batch_id": None,
            "scenario_id": None, "index": None, "total": None,
        }
        self._devices: Dict[str, Dict[str, Any]] = {}
        self._wait_deadlines: Dict[str, float] = {}
        self._collection: Dict[str, Dict[str, Any]] = {}
        self._final: Optional[Dict[str, Any]] = None
        self._final_deadline: Optional[float] = None
        self._conflicts: List[Dict[str, Any]] = []
        self._conflicts_found = 0
        self._failed_signals: List[str] = []
        self._stop_reason: Optional[str] = None
        self._error: Optional[str] = None

    # -- lifecycle (called by the owning object) ---------------------------

    def start(self, **position: Any) -> None:
        """Mark the operation running and clear the previous one's details."""
        with self._lock:
            self._reset(state="running")
            self._started_at = datetime.now()
            self._started_mono = time.monotonic()
            for key, value in position.items():
                if key in self._position:
                    self._position[key] = value

    def set_state(self, state: str, *, stop_reason: Optional[str] = None, error: Optional[str] = None) -> None:
        """Set ``state``; terminal states also record the finish time."""
        if state not in STATES:
            raise ValueError(f"Unknown state {state!r}; expected one of {STATES}")
        with self._lock:
            if state == "stopping" and self._state != "running":
                return
            self._state = state
            if stop_reason is not None:
                self._stop_reason = stop_reason
            if error is not None:
                self._error = error
            if state in ("cancelled", "completed", "failed"):
                self._finished_at = datetime.now()
                self._finished_mono = time.monotonic()

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    # -- event feed --------------------------------------------------------

    def update(self, ev: ProgressEvent) -> None:
        """Fold one event into the status (never changes ``state``)."""
        now = time.monotonic()
        with self._lock:
            self._latest = {
                "stage": ev.stage.value,
                "message": ev.message,
                "level": logging.getLevelName(ev.level),
                "updated_at": ev.timestamp,
            }
            for key in self._position:
                value = getattr(ev, key)
                if value is not None:
                    self._position[key] = value

            stage = ev.stage
            extra = ev.extra
            if stage == Stage.RUN_START:
                self._devices = {}
                self._wait_deadlines = {}
                self._collection = {}
                self._final = None
                self._final_deadline = None
                self._failed_signals = []

            if ev.device_id is not None:
                dev = self._devices.setdefault(str(ev.device_id), {
                    "stage": None, "events_sent": None, "events_total": None,
                    "seconds_until_start": None, "message": None,
                })
                dev["stage"] = stage.value
                dev["message"] = ev.message
                if ev.events_sent is not None:
                    dev["events_sent"] = ev.events_sent
                if ev.events_total is not None:
                    dev["events_total"] = ev.events_total
                if stage == Stage.WAITING and ev.seconds_until_start is not None:
                    self._wait_deadlines[str(ev.device_id)] = now + max(0.0, float(ev.seconds_until_start))
                elif stage in (Stage.REPLAY, Stage.SIGNAL_FAILED):
                    self._wait_deadlines.pop(str(ev.device_id), None)

            if stage == Stage.COLLECT and ev.device_id is not None:
                self._collection[str(ev.device_id)] = dict(extra)
            elif stage == Stage.FINAL_COLLECTION:
                final = dict(self._final or {})
                final["waiting"] = bool(extra.get("waiting", True))
                for key in ("needed", "status"):
                    if key in extra:
                        final[key] = extra[key]
                if "devices" in extra:
                    final["devices"] = dict(extra["devices"])
                seconds_left = extra.get("seconds_left")
                if final["waiting"] and seconds_left is not None:
                    self._final_deadline = now + max(0.0, float(seconds_left))
                elif not final["waiting"]:
                    self._final_deadline = None
                self._final = final
            elif stage == Stage.CONFLICT:
                conflicts = list(extra.get("conflicts") or [])
                self._conflicts_found += len(conflicts) or 1
                self._conflicts.extend(conflicts)
                del self._conflicts[:-_MAX_STATUS_CONFLICTS]
            elif stage == Stage.SIGNAL_FAILED and ev.device_id is not None:
                if str(ev.device_id) not in self._failed_signals:
                    self._failed_signals.append(str(ev.device_id))

    # -- read --------------------------------------------------------------

    def snapshot(self) -> Dict[str, Any]:
        """JSON-safe copy of the current status (see the class docstring)."""
        now = time.monotonic()
        with self._lock:
            devices = {}
            for device_id, dev in self._devices.items():
                dev = dict(dev)
                deadline = self._wait_deadlines.get(device_id)
                dev["seconds_until_start"] = max(0.0, deadline - now) if deadline is not None else None
                devices[device_id] = dev
            waits = [d["seconds_until_start"] for d in devices.values() if d["seconds_until_start"] is not None]
            final = None
            if self._final is not None:
                final = dict(self._final)
                final["seconds_left"] = (
                    max(0.0, self._final_deadline - now) if self._final_deadline is not None else None
                )
            if self._started_mono is None:
                elapsed = None
            else:
                end = self._finished_mono if self._finished_mono is not None else now
                elapsed = max(0.0, end - self._started_mono)
            data = {
                "state": self._state,
                **self._latest,
                **self._position,
                "devices": devices,
                "seconds_until_start": max(waits) if waits else None,
                "collection": {d: dict(h) for d, h in self._collection.items()},
                "final_collection": final,
                "conflicts_found": self._conflicts_found,
                "conflicts": list(self._conflicts),
                "failed_signals": list(self._failed_signals),
                "started_at": self._started_at,
                "finished_at": self._finished_at,
                "elapsed_seconds": elapsed,
                "stop_reason": self._stop_reason,
                "error": self._error,
            }
        return _json_safe(data)


class ProgressReporter:
    """Internal: logs a progress event, updates the status and calls the callback.

    Not part of the public API. ``emit`` never raises because of the
    callback; a failing callback is logged with ``logger.exception``.
    """

    def __init__(
        self,
        on_progress: Optional[ProgressCallback] = None,
        *,
        log: Optional[logging.Logger] = None,
        status: Optional[StatusTracker] = None,
        min_interval: Optional[float] = None,
    ) -> None:
        self._callback = on_progress
        self._log = log or logger
        self.status = status if status is not None else StatusTracker()
        self.min_interval = DEFAULT_MIN_INTERVAL_SECONDS if min_interval is None else float(min_interval)
        self._context: Dict[str, Any] = {}
        self._last_sent: Dict[Any, float] = {}
        self._lock = threading.Lock()

    @property
    def has_callback(self) -> bool:
        return self._callback is not None

    def set_context(self, **fields: Any) -> None:
        """Default field values for later events (None removes a default)."""
        with self._lock:
            for key, value in fields.items():
                if value is None:
                    self._context.pop(key, None)
                else:
                    self._context[key] = value

    def emit(
        self,
        stage: Stage,
        message: str = "",
        *,
        level: int = logging.INFO,
        throttle_key: Any = None,
        force: bool = False,
        log: bool = True,
        log_to: Optional[logging.Logger] = None,
        **fields: Any,
    ) -> Optional[ProgressEvent]:
        """Build an event, log it, record it and pass it to the callback.

        Args:
            throttle_key: Events sharing a key reach the callback at most
                once per ``min_interval`` seconds (``force`` overrides this).
                The status is updated either way.
            log: Write ``message`` to the log at ``level`` (False when the
                caller already logged it).
            log_to: Logger to write to instead of the reporter's own.

        Returns:
            The event, or None when it was throttled.
        """
        with self._lock:
            for key, value in self._context.items():
                if fields.get(key) is None:
                    fields[key] = value
        ev = ProgressEvent(stage=stage, message=message, level=level, **fields)
        if log and message:
            (log_to or self._log).log(level, "%s", message)
        self.status.update(ev)
        if throttle_key is not None:
            now = time.monotonic()
            with self._lock:
                last = self._last_sent.get(throttle_key)
                if not force and last is not None and now - last < self.min_interval:
                    return None
                self._last_sent[throttle_key] = now
        self.deliver(ev)
        return ev

    def forward(self, ev: ProgressEvent) -> None:
        """Record and deliver an event produced by a nested reporter."""
        self.status.update(ev)
        self.deliver(ev)

    def deliver(self, ev: ProgressEvent) -> None:
        """Call the callback with ``ev``; log and swallow any exception it raises."""
        callback = self._callback
        if callback is None:
            return
        try:
            callback(ev)
        except Exception:
            logger.exception("on_progress callback failed for %s event", ev.stage.value)


def as_reporter(
    on_progress: Any,
    *,
    log: Optional[logging.Logger] = None,
) -> ProgressReporter:
    """Return ``on_progress`` if it is already a reporter, else wrap the callback."""
    if isinstance(on_progress, ProgressReporter):
        return on_progress
    if on_progress is not None and not callable(on_progress):
        raise TypeError("on_progress must be a callable taking a ProgressEvent")
    return ProgressReporter(on_progress, log=log)
