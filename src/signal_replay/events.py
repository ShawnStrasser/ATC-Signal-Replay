"""
Output-event sources: where the package gets the events a test controller logged.

During a replay the package needs the controller's own high-resolution event
log (its *output* events) to detect conflicts, calibrate replay latency and
compare runs. MAXTIME controllers serve that log over HTTP, and the built-in
:class:`MaxtimeHttpEventSource` reads it. For any other controller, pass your
own source as ``event_source=`` to :class:`~signal_replay.ATCSimulation`,
:class:`~signal_replay.SimulationConfig` or :class:`~signal_replay.BatchRunner`.

Output-event schema
-------------------
Canonical columns (see :data:`OUTPUT_EVENT_COLUMNS`):

* ``TimeStamp``: ``datetime64[ns]``, naive, in the controller's local
  wall-clock time. The controller clock is assumed to match the clock of
  the PC running the replay (see ``SignalConfig.clock_offset_seconds`` when
  it does not). Timezone-aware values are converted to the PC's local time
  and then made naive. Naive values are taken as local time unless the
  source or signal declares ``source_timezone`` (for example ``'UTC'``).
  Timestamps are never rounded.
* ``EventTypeID``: int, Indiana high-resolution event code.
* ``Parameter``: int, the phase, overlap or detector number (1-based).

A source may return a pandas DataFrame or a list of dicts. Column names are
matched without regard to case, and common aliases are accepted (``EventId``,
``event_id``, ``timestamp``, ``param`` and so on). Extra columns are ignored.
If a device id column (``DeviceId`` / ``device_id``) is present, rows for
other devices are dropped. Rows need not be sorted, and re-delivering rows
already returned is harmless: stored events are de-duplicated on
(device, run, TimeStamp, EventTypeID, Parameter).

Source contract
---------------
A source is any of:

* a plain function ``fetch(target, since)``;
* an ``async def`` function with the same signature (it runs on a private
  event loop owned by the package, in a background thread). The loop is
  kept for as long as the source object lives, so every simulation and
  batch that uses the same source runs it on the same loop, and a
  loop-bound client the source caches (``httpx.AsyncClient``, an aiohttp
  session) keeps working;
* an object with a ``fetch(target, since)`` method (sync or async), see
  :class:`OutputEventSource`.

``target`` is a :class:`CollectionTarget`. ``since`` is a hint (naive
datetime, same clock and timezone convention as the timestamps the source
returns) that the package already holds every event before it; returning
more than asked is fine. The function returns the events, or a
:class:`FetchResult` that also says how far its data is known to be
complete (``complete_through``). Plain events count as complete up to the
moment the call started.

Any exception raised by a source is a counted, non-fatal failure for that
poll (raise :class:`EventSourceError` for expected ones). The package never
handles credentials: a source that needs them keeps them itself.

Options can be attached to a function with :class:`EventSource`, or set as
attributes on a source object:

* ``ordered`` (default False): True when the source returns events in
  append order and never delivers a late event older than one it already
  returned. The package then drops rows at or before the last stored event
  before writing.
* ``source_timezone`` (default None): timezone of naive timestamps (and of
  ``complete_through``) the source returns, for example ``'UTC'``.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import time
import weakref
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Set,
    Tuple,
    Union,
    runtime_checkable,
)

import numpy as np
import pandas as pd

from ._logging import run_in_log_context

logger = logging.getLogger(__name__)

#: Canonical output-event columns, in order.
OUTPUT_EVENT_COLUMNS: Tuple[str, str, str] = ("TimeStamp", "EventTypeID", "Parameter")

_COLUMN_ALIASES: Dict[str, Tuple[str, ...]] = {
    "TimeStamp": ("timestamp", "time_stamp", "time", "datetime"),
    "EventTypeID": ("eventtypeid", "eventid", "event_id", "event_type_id", "eventcode", "event_code", "event"),
    "Parameter": ("parameter", "param", "eventparam", "event_param", "eventparameter", "event_parameter"),
}
_DEVICE_ID_ALIASES: Tuple[str, ...] = ("deviceid", "device_id")

# Event codes each feature needs in the collected output.
DETECTOR_ON_EVENT_ID = 82
_CONFLICT_CODES_BY_PREFIX: Dict[str, Tuple[int, ...]] = {
    "Ph": (1, 10),
    "O": (61, 63, 65),
    "Ped": (21, 23),
    "OPed": (67, 65),
}

# How often a waiting caller checks its stop flag.
_STOP_POLL_SECONDS = 0.1

EventsLike = Union[pd.DataFrame, Sequence[Mapping[str, Any]], None]


class EventSourceError(Exception):
    """A poll of an output-event source failed.

    Raise it from a source for expected failures (controller offline, file
    not ready). Any exception from a source is treated the same way: the
    failure is counted in ``collection_health`` and the next poll tries
    again. It never stops a replay on its own.
    """


class _FetchAbandoned(Exception):
    """Internal: the caller stopped waiting for a source call (stop or abort)."""


def normalize_device_id(value: Any) -> str:
    """Return the canonical string form of a device id.

    Device ids may be given as int or str (for example ``12`` or ``'12'``).
    The package stores them as strings: integers (including integral floats
    such as ``12.0``) become ``'12'``; strings are stripped.
    """
    if value is None:
        raise ValueError("device_id must not be None")
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    try:
        import numpy as np

        if isinstance(value, np.integer):
            return str(int(value))
        if isinstance(value, np.floating) and float(value).is_integer():
            return str(int(value))
    except ImportError:  # pragma: no cover - numpy ships with pandas
        pass
    return str(value).strip()


@dataclass(frozen=True)
class CollectionTarget:
    """One controller to collect output events from, as passed to a source.

    Attributes:
        device_id: The package's device id for this signal, always a string
            (see :func:`normalize_device_id`).
        ip: Controller IP address (the SNMP replay target).
        http_port: HTTP port of the MAXTIME event-log endpoint, or None.
        extra: Read-only mapping of caller-defined values copied from
            ``SignalConfig.collection_extra`` (for a BatchRunner scenario:
            ``scenario_id``, ``database_name``, ``assignment`` plus
            ``TestScenario.collection_extra``). Use ``extra['source_device_id']``
            when the device id in the source's rows differs from ``device_id``.
    """

    device_id: str
    ip: str
    http_port: Optional[int] = None
    extra: Mapping[str, Any] = field(default_factory=dict, compare=False, hash=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "device_id", normalize_device_id(self.device_id))
        object.__setattr__(self, "extra", MappingProxyType(dict(self.extra or {})))

    @property
    def source_device_id(self) -> str:
        """Device id expected in a source's device id column."""
        value = self.extra.get("source_device_id")
        return normalize_device_id(value) if value is not None else self.device_id


@dataclass(frozen=True)
class FetchResult:
    """Events from one source call plus how far they are known to be complete.

    Attributes:
        events: DataFrame or list of dicts (see the module schema).
        complete_through: The source holds every event up to this time
            (same clock/timezone convention as its timestamps). For a
            file-based log this is the end time of the newest closed file.
            None means "complete up to when the call started".
    """

    events: EventsLike = None
    complete_through: Optional[datetime] = None


@runtime_checkable
class OutputEventSource(Protocol):
    """Class form of an output-event source.

    ``fetch`` may be a normal or an ``async`` method. Optional attributes
    ``ordered`` and ``source_timezone`` are read as described in the module
    documentation.
    """

    def fetch(self, target: CollectionTarget, since: Optional[datetime]) -> Union[EventsLike, FetchResult]:
        ...


class EventSource:
    """Attach options to a source function.

    Example::

        source = EventSource(my_fetch, source_timezone='UTC')

    Args:
        fetch: Function ``(target, since)``, sync or async.
        ordered: See the module documentation (default False).
        source_timezone: Timezone of naive timestamps the function returns.
        name: Label used in log messages.
    """

    def __init__(
        self,
        fetch: Callable[..., Any],
        *,
        ordered: bool = False,
        source_timezone: Optional[str] = None,
        name: Optional[str] = None,
    ):
        if not callable(fetch):
            raise TypeError("fetch must be callable")
        if source_timezone is not None:
            _validate_timezone(source_timezone, "source_timezone")
        self._fetch = fetch
        self.ordered = bool(ordered)
        self.source_timezone = source_timezone
        self.name = name or getattr(fetch, "__name__", type(fetch).__name__)

    def fetch(self, target: CollectionTarget, since: Optional[datetime]) -> Any:
        return self._fetch(target, since)

    def __repr__(self) -> str:
        return f"EventSource({self.name!r}, ordered={self.ordered}, source_timezone={self.source_timezone!r})"


class MaxtimeHttpEventSource:
    """Built-in source: the MAXTIME ``/v1/asclog/xml/full`` HTTP event log.

    Wraps :func:`signal_replay.fetch_output_data`. Targets without an
    ``http_port`` are skipped (collection is off for them). MAXTIME appends
    events in order, so this source is ``ordered``; its data counts as
    complete at the time of each fetch.

    Args:
        request_timeout_seconds: HTTP read timeout (None keeps the default).
        connect_timeout_seconds: HTTP connect timeout (None keeps the default).
    """

    ordered = True
    source_timezone: Optional[str] = None
    requires_http_port = True
    name = "MAXTIME HTTP"

    def __init__(
        self,
        request_timeout_seconds: Optional[float] = None,
        connect_timeout_seconds: Optional[float] = None,
    ):
        self.request_timeout_seconds = request_timeout_seconds
        self.connect_timeout_seconds = connect_timeout_seconds

    def fetch(self, target: CollectionTarget, since: Optional[datetime]) -> pd.DataFrame:
        if target.http_port is None:
            raise EventSourceError(f"No http_port configured for {target.device_id}")
        # Looked up at call time so tests (and callers) can patch
        # signal_replay.collector.fetch_output_data.
        from . import collector as _collector

        kwargs: Dict[str, Any] = {}
        if self.request_timeout_seconds is not None:
            kwargs["request_timeout_seconds"] = self.request_timeout_seconds
        if self.connect_timeout_seconds is not None:
            kwargs["connect_timeout_seconds"] = self.connect_timeout_seconds
        return _collector.fetch_output_data(target.ip, target.http_port, since=since, **kwargs)

    def __repr__(self) -> str:
        return "MaxtimeHttpEventSource()"


# ---------------------------------------------------------------------------
# Timestamp and schema normalisation
# ---------------------------------------------------------------------------

def _local_tz():
    from dateutil import tz

    return tz.tzlocal()


def _validate_timezone(value: str, name: str) -> None:
    try:
        pd.Timestamp("2026-01-01").tz_localize(value)
    except Exception as exc:
        raise ValueError(f"{name} {value!r} is not a known timezone") from exc


def _resolve_tz(value: Optional[str]):
    return _local_tz() if value is None else value


def _to_local_naive_series(
    values: pd.Series,
    source_timezone: Optional[str],
    local_timezone: Optional[str],
) -> pd.Series:
    try:
        ts = pd.to_datetime(values)
    except (ValueError, TypeError):
        # Mixed UTC offsets in an object column.
        ts = pd.to_datetime(values, utc=True)
    local = _resolve_tz(local_timezone)
    if getattr(ts.dt, "tz", None) is not None:
        return ts.dt.tz_convert(local).dt.tz_localize(None)
    if source_timezone is not None:
        return _localize_naive_series(ts, source_timezone).dt.tz_convert(local).dt.tz_localize(None)
    return ts


def _localize_naive_series(ts: pd.Series, source_timezone: str) -> pd.Series:
    """Attach ``source_timezone`` to naive timestamps without dropping any.

    Wall-clock times repeated by a DST change (the autumn hour) are resolved
    from the row order when possible (``ambiguous='infer'``), otherwise taken
    as the first occurrence (daylight time). Times skipped by the spring
    change are moved forward to the first valid time. Either case is logged
    at WARNING with the number of rows affected.
    """
    try:
        return ts.dt.tz_localize(source_timezone)
    except Exception:  # AmbiguousTimeError / NonExistentTimeError
        pass
    flagged = ts.dt.tz_localize(source_timezone, ambiguous="NaT", nonexistent="NaT")
    affected = int((flagged.isna() & ts.notna()).sum())
    try:
        out = ts.dt.tz_localize(source_timezone, ambiguous="infer", nonexistent="shift_forward")
        how = "resolved from the row order"
    except Exception:
        dst = np.ones(len(ts), dtype=bool)
        out = ts.dt.tz_localize(source_timezone, ambiguous=dst, nonexistent="shift_forward")
        how = "taken as daylight time"
    logger.warning(
        "%d output event timestamp(s) fall in a DST change of source_timezone %r; repeated "
        "times were %s and skipped times moved forward",
        affected, source_timezone, how,
    )
    return out


def to_local_naive(
    value: Any,
    *,
    source_timezone: Optional[str] = None,
    clock_offset_seconds: float = 0.0,
    local_timezone: Optional[str] = None,
) -> Optional[datetime]:
    """Convert one source timestamp to the canonical clock (naive local time).

    Applies the same rules as :func:`normalize_output_events`. Returns None
    for None/NaT.
    """
    if value is None:
        return None
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        return None
    local = _resolve_tz(local_timezone)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(local).tz_localize(None)
    elif source_timezone is not None:
        # A wall-clock time repeated by the autumn DST change is taken as its
        # first occurrence (the earlier instant), so a complete_through in
        # that hour never overstates how far the data is complete.
        ts = ts.tz_localize(source_timezone, ambiguous=True, nonexistent="shift_forward")
        ts = ts.tz_convert(local).tz_localize(None)
    if clock_offset_seconds:
        ts = ts + pd.Timedelta(seconds=float(clock_offset_seconds))
    return ts.to_pydatetime()


def from_local_naive(
    value: Optional[datetime],
    *,
    source_timezone: Optional[str] = None,
    clock_offset_seconds: float = 0.0,
    local_timezone: Optional[str] = None,
) -> Optional[datetime]:
    """Inverse of :func:`to_local_naive`: express a canonical time in a source's convention."""
    if value is None:
        return None
    ts = pd.Timestamp(value)
    if clock_offset_seconds:
        ts = ts - pd.Timedelta(seconds=float(clock_offset_seconds))
    if source_timezone is not None:
        ts = ts.tz_localize(_resolve_tz(local_timezone), ambiguous=True, nonexistent="shift_forward")
        ts = ts.tz_convert(source_timezone).tz_localize(None)
    return ts.to_pydatetime()


def _find_column(columns: Iterable[Any], canonical: str, aliases: Tuple[str, ...]) -> Optional[Any]:
    columns = list(columns)
    if canonical in columns:
        return canonical
    lowered = {str(col).lower(): col for col in columns}
    for alias in (canonical.lower(),) + aliases:
        if alias in lowered:
            return lowered[alias]
    return None


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "TimeStamp": pd.Series([], dtype="datetime64[ns]"),
            "EventTypeID": pd.Series([], dtype="int64"),
            "Parameter": pd.Series([], dtype="int64"),
        }
    )


def normalize_output_events(
    events: EventsLike,
    *,
    device_id: Any = None,
    source_timezone: Optional[str] = None,
    clock_offset_seconds: float = 0.0,
    local_timezone: Optional[str] = None,
) -> pd.DataFrame:
    """Convert source output events to the canonical schema.

    Args:
        events: DataFrame, list of dicts, or None.
        device_id: When given and the data has a device id column, rows for
            other devices are dropped (ids compared with
            :func:`normalize_device_id`).
        source_timezone: Timezone of naive timestamps (None = local time).
        clock_offset_seconds: Seconds added to every timestamp to bring the
            controller clock onto the PC clock.
        local_timezone: Target timezone (None = this PC's local timezone).

    Returns:
        DataFrame with exactly :data:`OUTPUT_EVENT_COLUMNS`; rows with a
        missing value are dropped.

    Raises:
        ValueError: A required column is missing or values cannot be parsed.
        TypeError: ``events`` is not a DataFrame or a list of mappings.
    """
    if events is None:
        return _empty_events()
    if isinstance(events, pd.DataFrame):
        df = events
    elif isinstance(events, (list, tuple)):
        if not events:
            return _empty_events()
        df = pd.DataFrame.from_records(list(events))
    else:
        raise TypeError(
            "Output events must be a pandas DataFrame or a list of dicts, "
            f"got {type(events).__name__}"
        )

    if df.empty and len(df.columns) == 0:
        return _empty_events()

    found: Dict[str, Any] = {}
    for canonical, aliases in _COLUMN_ALIASES.items():
        col = _find_column(df.columns, canonical, aliases)
        if col is not None:
            found[canonical] = col
    missing = [name for name in OUTPUT_EVENT_COLUMNS if name not in found]
    if missing:
        raise ValueError(
            f"Output events are missing column(s) {missing}. "
            f"Expected {list(OUTPUT_EVENT_COLUMNS)} (or aliases); got {[str(c) for c in df.columns]}"
        )

    device_col = _find_column(df.columns, "DeviceId", _DEVICE_ID_ALIASES)
    if device_col is not None and device_id is not None and not df.empty:
        wanted = normalize_device_id(device_id)
        ids = df[device_col].map(lambda v: None if pd.isna(v) else normalize_device_id(v))
        df = df[ids == wanted]

    out = pd.DataFrame(
        {
            "TimeStamp": df[found["TimeStamp"]],
            "EventTypeID": df[found["EventTypeID"]],
            "Parameter": df[found["Parameter"]],
        }
    )
    if out.empty:
        return _empty_events()

    out["TimeStamp"] = _to_local_naive_series(out["TimeStamp"], source_timezone, local_timezone)
    if clock_offset_seconds:
        out["TimeStamp"] = out["TimeStamp"] + pd.Timedelta(seconds=float(clock_offset_seconds))
    for column in ("EventTypeID", "Parameter"):
        out[column] = pd.to_numeric(out[column], errors="raise")

    before = len(out)
    out = out.dropna()
    if len(out) < before:
        logger.debug("Dropped %d output event row(s) with missing values", before - len(out))
    if out.empty:
        return _empty_events()
    out["EventTypeID"] = out["EventTypeID"].astype("int64")
    out["Parameter"] = out["Parameter"].astype("int64")
    out["TimeStamp"] = out["TimeStamp"].astype("datetime64[ns]")
    return out.reset_index(drop=True)


def required_event_codes(
    incompatible_pairs: Optional[Iterable[Tuple[str, str]]] = None,
    adaptive_latency: bool = False,
) -> Dict[str, Set[int]]:
    """Event codes the collected output must contain for the enabled features.

    Returns ``{feature_name: {codes}}``: ``'conflict detection'`` (codes for
    the phase/overlap/ped types named in ``incompatible_pairs``) and
    ``'adaptive latency'`` (event 82, detector on).
    """
    required: Dict[str, Set[int]] = {}
    codes: Set[int] = set()
    for pair in incompatible_pairs or []:
        for name in pair:
            prefix = str(name).rstrip("0123456789")
            codes.update(_CONFLICT_CODES_BY_PREFIX.get(prefix, ()))
    if codes:
        required["conflict detection"] = codes
    if adaptive_latency:
        required["adaptive latency"] = {DETECTOR_ON_EVENT_ID}
    return required


# ---------------------------------------------------------------------------
# Internal adapter: one calling convention for every source form
# ---------------------------------------------------------------------------

def _wait_for_stop(stop: Any, seconds: float) -> bool:
    """Wait up to ``seconds``; return True as soon as ``stop.is_set()``.

    ``stop`` is anything with ``is_set()`` (a threading.Event or a view of
    one), or None.
    """
    deadline = time.monotonic() + max(0.0, seconds)
    while True:
        if stop is not None and stop.is_set():
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        step = min(remaining, 0.25)
        waiter = getattr(stop, "wait", None)
        if waiter is not None:
            waiter(step)
        else:
            time.sleep(step)


class _AnyStop:
    """Stop view that is set when any of several events is set."""

    def __init__(self, *events: Any):
        self._events = [e for e in events if e is not None]

    def is_set(self) -> bool:
        return any(e.is_set() for e in self._events)


class _SourceLoop:
    """A background event loop shared by every adapter of one source object."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

    def get(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._loop is None or self._loop.is_closed() or not (
                self._thread is not None and self._thread.is_alive()
            ):
                loop = asyncio.new_event_loop()
                thread = threading.Thread(
                    target=loop.run_forever, name="event-source-loop", daemon=True
                )
                thread.start()
                self._loop, self._thread = loop, thread
            return self._loop

    def stop(self) -> None:
        with self._lock:
            loop, thread = self._loop, self._thread
            self._loop = self._thread = None
        if loop is None or loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(loop.stop)
        except RuntimeError:  # pragma: no cover - closed between the checks
            return
        if thread is None or thread is threading.current_thread():
            return
        thread.join(2.0)
        if not loop.is_running() and not loop.is_closed():
            loop.close()


# One loop per source object, kept until that object is garbage collected.
# Sources that cannot be weakly referenced share one process-wide loop.
_SOURCE_LOOPS: "weakref.WeakKeyDictionary[Any, _SourceLoop]" = weakref.WeakKeyDictionary()
_SHARED_SOURCE_LOOP = _SourceLoop()
_SOURCE_LOOPS_LOCK = threading.Lock()


def _loop_owner(source: Any) -> Any:
    """The object whose lifetime the async loop of ``source`` follows.

    An :class:`EventSource` wrapper or a bound method is looked through to
    the function or object it wraps, so a new wrapper around the same
    function still gets the same loop.
    """
    owner = source
    if isinstance(owner, EventSource):
        owner = owner._fetch
    bound_to = getattr(owner, "__self__", None)
    if inspect.ismethod(owner) and bound_to is not None:
        owner = bound_to
    return owner


def _source_loop_for(source: Any) -> _SourceLoop:
    owner = _loop_owner(source)
    with _SOURCE_LOOPS_LOCK:
        try:
            holder = _SOURCE_LOOPS.get(owner)
        except TypeError:  # not weak-referenceable or not hashable
            return _SHARED_SOURCE_LOOP
        if holder is None:
            holder = _SourceLoop()
            try:
                _SOURCE_LOOPS[owner] = holder
                weakref.finalize(owner, holder.stop)
            except TypeError:
                return _SHARED_SOURCE_LOOP
        return holder


class _SourceAdapter:
    """Calls a user or built-in source in a uniform, stoppable way."""

    def __init__(self, source: Any = None):
        if source is None:
            source = MaxtimeHttpEventSource()
        self.source = source
        if callable(getattr(source, "fetch", None)) and not inspect.isroutine(source):
            self._call = source.fetch
        elif callable(source):
            self._call = source
        else:
            raise TypeError(
                "event_source must be a function (target, since), an async function, "
                "or an object with a fetch(target, since) method"
            )
        self.is_default = isinstance(source, MaxtimeHttpEventSource)
        self.ordered = bool(getattr(source, "ordered", False))
        self.source_timezone: Optional[str] = getattr(source, "source_timezone", None)
        if self.source_timezone is not None:
            _validate_timezone(self.source_timezone, "source_timezone")
        self.requires_http_port = bool(getattr(source, "requires_http_port", False))
        self.name = str(
            getattr(source, "name", None) or getattr(self._call, "__qualname__", None) or type(source).__name__
        )
        self._source_loop: Optional[_SourceLoop] = None

    def enabled_for(self, target: CollectionTarget) -> bool:
        return not (self.requires_http_port and target.http_port is None)

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        if self._source_loop is None:
            self._source_loop = _source_loop_for(self.source)
        return self._source_loop.get()

    def fetch(
        self,
        target: CollectionTarget,
        since: Optional[datetime],
        *,
        stop: Any = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Call the source and return its raw result.

        The call runs on a helper thread (async sources on the private event
        loop) so the caller can stop waiting at once.

        Raises:
            EventSourceError: The source raised, or exceeded ``timeout``.
            _FetchAbandoned: ``stop`` was set before the call finished; any
                result it produces later is discarded.
        """
        box: Dict[str, Any] = {}
        done = threading.Event()

        def _worker() -> None:
            try:
                result = self._call(target, since)
                if inspect.isawaitable(result):
                    future = asyncio.run_coroutine_threadsafe(
                        _as_coroutine(result), self._ensure_loop()
                    )
                    box["future"] = future
                    result = future.result()
                box["result"] = result
            except BaseException as exc:  # reported to the caller below
                box["error"] = exc
            finally:
                done.set()

        thread = threading.Thread(
            target=run_in_log_context(_worker), name=f"event-source-{target.device_id}", daemon=True
        )
        thread.start()
        deadline = None if timeout is None else time.monotonic() + timeout
        while not done.wait(_STOP_POLL_SECONDS):
            if stop is not None and stop.is_set():
                _cancel_future(box)
                raise _FetchAbandoned()
            if deadline is not None and time.monotonic() >= deadline:
                _cancel_future(box)
                raise EventSourceError(f"source call timed out after {timeout:.0f}s")
        if stop is not None and stop.is_set():
            raise _FetchAbandoned()
        error = box.get("error")
        if error is not None:
            if isinstance(error, EventSourceError):
                raise error
            raise EventSourceError(f"{type(error).__name__}: {error}") from error
        return box.get("result")

    def close(self) -> None:
        """Release this adapter.

        The async loop belongs to the source object, not to the adapter, so
        it is left running for the next collector that uses the same source
        (a later simulation or batch). It stops when the source object is
        garbage collected.
        """
        self._source_loop = None


async def _as_coroutine(awaitable: Any) -> Any:
    return await awaitable


def _cancel_future(box: Dict[str, Any]) -> None:
    future = box.get("future")
    if future is not None:
        future.cancel()


def split_fetch_result(result: Any) -> Tuple[EventsLike, Optional[datetime], bool]:
    """Return (events, complete_through, reported) from a source's return value."""
    if isinstance(result, FetchResult):
        return result.events, result.complete_through, result.complete_through is not None
    return result, None, False


__all__ = [
    "OUTPUT_EVENT_COLUMNS",
    "CollectionTarget",
    "FetchResult",
    "EventSourceError",
    "OutputEventSource",
    "EventSource",
    "MaxtimeHttpEventSource",
    "normalize_output_events",
    "normalize_device_id",
    "required_event_codes",
    "to_local_naive",
    "from_local_naive",
]
