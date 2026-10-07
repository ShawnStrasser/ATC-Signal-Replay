"""
Main orchestrator for ATC Signal Replay simulations.
"""

import logging
import threading
import time
from contextlib import nullcontext
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Union, Tuple, Set
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
import pandas as pd

from .config import SimulationConfig, SignalConfig
from .replay import SignalReplay, source_time_from_info
from .collector import ConflictRecord, DatabaseManager, DataCollector, _log_memory
from .events import CollectionTarget, required_event_codes
from .latency import AdaptiveLatencyOffsetManager
from .comparison import (
    compare_all_runs,
    format_comparison_summary,
    ComparisonResult,
    ComparisonThresholds,
    store_comparison_result,
    generate_timeline,
    prepare_events_for_comparison,
)
from ._logging import debug_level, log_to_file, run_in_log_context
from .progress import ProgressCallback, ProgressReporter, Stage, StatusTracker
from .results import ReplicationResult, RunRecord, new_run_uuid
from .workspace import PLOTS_DIR_NAME, REPLAY_DB_NAME, RUN_LOG_NAME, resolve_work_dir, write_manifest

logger = logging.getLogger(__name__)

#: Values of ``results['stop_reason']`` returned by :meth:`ATCSimulation.run`.
STOP_REASONS = ('completed', 'conflict', 'cancelled', 'collection_error', 'all_signals_failed')

# How long a cancelled run waits for the background collection thread before
# abandoning it (it is a daemon thread and discards anything it fetches later).
_CANCEL_COLLECTION_JOIN_SECONDS = 1.0
# How often the main thread wakes while waiting, so Ctrl+C is seen on Windows.
_MAIN_WAIT_SECONDS = 0.25
# SimulationConfig's default database path (relative to the current directory).
_DEFAULT_DB_PATH = "./atc_replay.db"
# Run status -> simulation_runs / RunRecord status.
_RECORD_STATUS = {
    'completed': 'completed',
    'incomplete': 'incomplete',
    'cancelled': 'cancelled',
    'collection_error': 'failed',
    'all_signals_failed': 'failed',
}


def _load_events(events: Union[pd.DataFrame, str, Path]) -> pd.DataFrame:
    """Load events from DataFrame or file path."""
    if isinstance(events, pd.DataFrame):
        return events
    elif isinstance(events, (str, Path)):
        path = Path(events)
        if path.suffix.lower() == '.parquet':
            return pd.read_parquet(path)
        else:
            return pd.read_csv(path)
    else:
        raise ValueError(f"events must be DataFrame or file path, got {type(events)}")


def _normalize_device_id_column(df: pd.DataFrame) -> str:
    """Find and return the device_id column name (normalized)."""
    for col in df.columns:
        if col.lower() in ('device_id', 'deviceid'):
            return col
    raise ValueError(
        "Centralized events must have a 'device_id' column to map events to signals. "
        f"Found columns: {list(df.columns)}"
    )


def _distribute_events(
    signals: List[SignalConfig],
    centralized_events: Union[pd.DataFrame, str, Path]
) -> None:
    """
    Distribute centralized events to all signals by filtering on device_id.
    
    Args:
        signals: List of SignalConfig objects (modified in place)
        centralized_events: DataFrame or path with device_id column
    """
    # Load the centralized events
    all_events = _load_events(centralized_events)
    device_id_col = _normalize_device_id_column(all_events)
    
    # Get unique device_ids in the events
    available_device_ids = set(all_events[device_id_col].astype(str).unique())
    
    for signal in signals:
        # Filter centralized events for this device
        device_id_str = str(signal.device_id)
        if device_id_str not in available_device_ids:
            raise ValueError(
                f"Signal '{signal.device_id}' not found in events. "
                f"Available device_ids: {sorted(available_device_ids)}"
            )
        
        # Filter events for this device
        mask = all_events[device_id_col].astype(str) == device_id_str
        signal_events = all_events[mask].copy()
        
        if signal_events.empty:
            raise ValueError(
                f"No events found for device_id '{signal.device_id}'"
            )
        
        # Assign events directly (using object.__setattr__ for frozen dataclass compatibility)
        object.__setattr__(signal, 'events', signal_events)


def _signals_have_events(signals: List[SignalConfig]) -> bool:
    """Return True when every signal already has an assigned event source."""
    return all(signal.events is not None for signal in signals)


def _is_missing(value: Any) -> bool:
    """True for None, NaN and NaT (values read back from a DataFrame row)."""
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


class _CancelView:
    """Stop flag for the final collection: set once the simulation is cancelled.

    Reading it also picks up the caller's shared stop event.
    """

    def __init__(self, sim: "ATCSimulation"):
        self._sim = sim

    def is_set(self) -> bool:
        return self._sim._is_cancelled()

    def wait(self, timeout: float) -> bool:
        step = min(timeout, _MAIN_WAIT_SECONDS)
        if self._sim._stop_event.is_set():
            # Set by a non-cancel stop (conflict): do not spin.
            time.sleep(step)
        else:
            self._sim._stop_event.wait(step)
        return self.is_set()


class ATCSimulation:
    """
    Main orchestrator for multi-signal ATC replay simulations.
    
    Manages replay processes, data collection, conflict detection,
    and post-simulation comparison analysis.
    
    Can be initialized in two ways:
    
    1. Legacy (with SimulationConfig):
        config = SimulationConfig(signals=[...], simulation_replays=5)
        sim = ATCSimulation(config)
    
    2. Streamlined (with kwargs):
        sim = ATCSimulation(
            signals=[...],
            events='all_events.csv',  # Centralized, filtered by device_id
            replays=5
        )
    
    Comparison Thresholds:
        Set comparison_thresholds to control when plots are generated.
        If a comparison exceeds thresholds and output_dir is set, 
        Gantt charts will be automatically generated.
    """
    
    def __init__(
        self,
        config: Optional[SimulationConfig] = None,
        *,
        signals: Optional[List[SignalConfig]] = None,
        events: Union[pd.DataFrame, str, Path, None] = None,
        replays: int = 1,
        stop_on_conflict: bool = True,
        db_path: Optional[str] = None,
        simulation_speed: float = 1.0,
        collection_interval_minutes: float = 5.0,
        post_replay_settle_seconds: float = 10.0,
        snmp_timeout_seconds: float = 2.0,
        snmp_send_retries: int = 0,
        snmp_retry_backoff_seconds: float = 0.25,
        show_progress_logs: bool = False,
        progress_log_interval_seconds: float = 60.0,
        replay_latency_offset_lookback_min: Optional[float] = None,
        replay_latency_offset_update_min: Optional[float] = None,
        replay_latency_offset_min_samples: Optional[int] = None,
        comparison_thresholds: Optional[ComparisonThresholds] = None,
        output_dir: Optional[Union[str, Path]] = None,
        skip_comparison: bool = False,
        debug: bool = False,
        stop_event: Optional[threading.Event] = None,
        stop_grace_seconds: float = 8.0,
        detector_reset_timeout_seconds: float = 5.0,
        cancel_final_poll_seconds: float = 3.0,
        event_source: Any = None,
        final_collection_timeout_seconds: float = 900.0,
        final_collection_poll_seconds: float = 20.0,
        on_progress: Optional[ProgressCallback] = None,
        work_dir: Optional[Union[str, Path]] = None,
        run_log: bool = False,
    ):
        """
        Initialize the ATC simulation.
        
        Args:
            config: SimulationConfig with all simulation parameters (legacy pattern)
            signals: List of SignalConfig objects (streamlined pattern)
            events: REQUIRED. Centralized event source with device_id column.
                Events are automatically filtered and distributed to signals by device_id.
            replays: Number of simulation runs (streamlined pattern)
            stop_on_conflict: Stop before the next run when a conflict is detected after final end-of-run collection
            db_path: Path to the working DuckDB database. Default:
                ``<work_dir>/replay.duckdb`` when ``work_dir`` is given,
                else ``./atc_replay.db`` (relative to the current directory)
            simulation_speed: Speed multiplier (1.0 = real-time)
            collection_interval_minutes: How often to poll controller event logs
            post_replay_settle_seconds: Wait after replay before final collection
            snmp_timeout_seconds: SNMP response timeout for replay sends
            snmp_send_retries: Additional replay send attempts after the first try
            snmp_retry_backoff_seconds: Delay between replay retry attempts
            show_progress_logs: If True, print periodic replay send progress
            progress_log_interval_seconds: Seconds between progress log lines
            comparison_thresholds: Thresholds for triggering comparison alerts.
                If None, uses defaults (sequence=0.05, timing=0.02, match=95%).
            output_dir: Directory to save comparison plots when thresholds exceeded.
                If None, no plots are generated.
            skip_comparison: If True, skip the post-replay comparison analysis.
                Useful when comparison is done separately (e.g., in a report step).
            debug: Enable debug output
            stop_event: Optional ``threading.Event`` shared with the caller (for
                example a GUI Cancel button or a BatchRunner). Setting it cancels
                the simulation exactly like :meth:`request_stop`, and
                :meth:`request_stop` sets it, so one event can cover several
                simulations. A conflict stop does not set it.
            stop_grace_seconds: Streamlined-pattern value for
                ``SimulationConfig.stop_grace_seconds``
            detector_reset_timeout_seconds: Streamlined-pattern value for
                ``SimulationConfig.detector_reset_timeout_seconds``
            cancel_final_poll_seconds: Streamlined-pattern value for
                ``SimulationConfig.cancel_final_poll_seconds``
            event_source: Streamlined-pattern value for ``SimulationConfig.event_source``:
                where output events come from (None = MAXTIME HTTP event log)
            final_collection_timeout_seconds: Streamlined-pattern value for
                ``SimulationConfig.final_collection_timeout_seconds``
            final_collection_poll_seconds: Streamlined-pattern value for
                ``SimulationConfig.final_collection_poll_seconds``
            on_progress: Optional callback receiving
                :class:`~signal_replay.ProgressEvent` objects (SETUP,
                STORE_INPUT, RUN_START, DETECTOR_RESET, WAITING, REPLAY,
                COLLECT, FINAL_COLLECTION, CONFLICT, SIGNAL_FAILED,
                RUN_COMPLETE, COMPARE, PLOT, then one of DONE, CANCELLED or
                ERROR). It is called on the package's worker threads, not the
                caller's; a GUI must hand events to its UI thread itself.
                Exceptions raised by the callback are logged and ignored.
                :meth:`get_status` gives the same information on demand.
            work_dir: Optional working folder owned by the caller. Every
                file goes under it with a fixed name (``replay.duckdb``,
                ``plots/``, ``run.log`` when ``run_log`` is True, and
                ``manifest.json``); nothing is derived from the current
                directory. No file stays open after :meth:`run` returns,
                so the folder can be deleted. See
                :mod:`signal_replay.workspace`.
            run_log: With ``work_dir``, copy this package's log records to
                ``<work_dir>/run.log`` while :meth:`run` executes.
        """
        self._status = StatusTracker()
        self._progress = ProgressReporter(on_progress, log=logger, status=self._status)
        self.debug = debug
        self.skip_comparison = skip_comparison
        self.comparison_thresholds = comparison_thresholds or ComparisonThresholds()
        #: Identifier of this simulation's run() (see ReplicationResult.run_uuid).
        self.run_uuid: str = new_run_uuid()
        self.work_dir: Optional[Path] = resolve_work_dir(work_dir) if work_dir is not None else None
        self.run_log = bool(run_log)
        if output_dir is None and self.work_dir is not None:
            output_dir = self.work_dir / PLOTS_DIR_NAME
        self.output_dir = Path(output_dir) if output_dir else None
        if db_path is None:
            db_path = str(self.work_dir / REPLAY_DB_NAME) if self.work_dir is not None else _DEFAULT_DB_PATH
        
        # Handle legacy vs streamlined initialization
        if config is not None:
            # Legacy pattern: SimulationConfig provided
            if signals is not None:
                raise ValueError("Cannot specify both 'config' and 'signals'. Use one or the other.")
            if config.events is not None:
                # Distribute events to all signals
                _distribute_events(config.signals, config.events)
            elif not _signals_have_events(config.signals):
                raise ValueError(
                    "SimulationConfig.events is None, but one or more signals are missing an event source"
                )
            if self.work_dir is not None and config.db_path == _DEFAULT_DB_PATH:
                config.db_path = db_path
            self.config = config
        else:
            # Streamlined pattern: kwargs provided
            if signals is None:
                raise ValueError("Must provide either 'config' or 'signals'")
            if events is None and not _signals_have_events(signals):
                raise ValueError(
                    "Must provide 'events' unless every signal already has an event source assigned"
                )

            if events is not None:
                # Distribute centralized events to all signals
                _distribute_events(signals, events)
            
            # Create SimulationConfig internally
            self.config = SimulationConfig(
                signals=signals,
                events=events,
                simulation_replays=replays,
                stop_on_conflict=stop_on_conflict,
                db_path=db_path,
                simulation_speed=simulation_speed,
                collection_interval_minutes=collection_interval_minutes,
                post_replay_settle_seconds=post_replay_settle_seconds,
                snmp_timeout_seconds=snmp_timeout_seconds,
                snmp_send_retries=snmp_send_retries,
                snmp_retry_backoff_seconds=snmp_retry_backoff_seconds,
                show_progress_logs=show_progress_logs,
                progress_log_interval_seconds=progress_log_interval_seconds,
                replay_latency_offset_lookback_min=replay_latency_offset_lookback_min,
                replay_latency_offset_update_min=replay_latency_offset_update_min,
                replay_latency_offset_min_samples=replay_latency_offset_min_samples,
                stop_grace_seconds=stop_grace_seconds,
                detector_reset_timeout_seconds=detector_reset_timeout_seconds,
                cancel_final_poll_seconds=cancel_final_poll_seconds,
                event_source=event_source,
                final_collection_timeout_seconds=final_collection_timeout_seconds,
                final_collection_poll_seconds=final_collection_poll_seconds,
            )

        if any(sig.tod_align for sig in self.config.signals) and self.config.simulation_speed != 1.0:
            raise ValueError("simulation_speed must be 1.0 when any signal uses tod_align=True")

        self._progress.set_context(total_runs=self.config.simulation_replays)
        self._progress.emit(
            Stage.SETUP,
            f"Setting up simulation: {len(self.config.signals)} signal(s), "
            f"{self.config.simulation_replays} run(s)",
            log=False,
            extra={"device_ids": [str(sig.device_id) for sig in self.config.signals],
                   "db_path": str(self.config.db_path)},
        )

        # State tracking used by replay setup and run-time shutdown.
        self._current_run: int = 0
        self._simulation_start_time: Optional[datetime] = None
        # _stop_event stops this simulation's replays (cancel, conflict or
        # collection error). _external_stop_event is the caller's cancel token.
        self._stop_event: threading.Event = threading.Event()
        self._external_stop_event: Optional[threading.Event] = stop_event
        self._stop_lock = threading.Lock()
        self._stop_reason: Optional[str] = None
        self._cancel_requested = False
        self._run_stop_event: Optional[threading.Event] = None
        self._run_ctx: Optional[Dict[str, Any]] = None
        self._active_replays: Dict[str, SignalReplay] = {}
        self._detectors_reset: Dict[str, bool] = {}
        self._cancelled_run: Optional[int] = None
        self.last_results: Optional[Dict[str, Any]] = None
        self._conflicts_found: List[Dict[str, Any]] = []
        self._conflict_keys: Set[Tuple[str, int, str]] = set()
        self._completed_runs: List[int] = []
        self._incomplete_runs: List[int] = []
        self._collection_health_by_run: Dict[int, Dict[str, Dict[str, Any]]] = {}
        self._failed_signals_by_run: Dict[int, List[str]] = {}
        self._comparison_results: Optional[Dict[str, List[ComparisonResult]]] = None
        self._conflict_records: List[ConflictRecord] = []
        # Conflicts stored by an earlier call for runs that were already done.
        self._prior_conflict_records: List[ConflictRecord] = []
        self._prior_completed_runs: List[int] = []
        # Devices recorded for the current run (None: all signals).
        self._run_devices: Optional[List[str]] = None
        self._run_records: Dict[Tuple[str, int], RunRecord] = {}
        self._runs_attempted = 0
        self._conflict_store_errors: List[str] = []
        self._started_at: Optional[datetime] = None
        #: The typed result of the last :meth:`run` (also returned by it).
        self.result: Optional[ReplicationResult] = None

        # Initialize database
        self.db = DatabaseManager(self.config.db_path)
        self._safe_db_call('set_meta', 'run_uuid', self.run_uuid)
        
        # Determine starting run number using completed runs so interrupted runs
        # resume to the requested total instead of adding a fresh batch.
        self._run_offset = self.db.get_max_run_number(device_ids=[sig.device_id for sig in self.config.signals])
        if self._run_offset > 0:
            logger.info("Existing completed runs found in database: %d", self._run_offset)
            self._progress.set_context(run_number=self._run_offset)
        
        # Store input events for comparison
        t0 = time.time()
        self._store_input_events()
        logger.info("Input events stored in %.1fs", time.time() - t0)

        # Free centralized events DataFrame (individual signals have their own sources)
        if isinstance(self.config.events, pd.DataFrame):
            self.config.events = None
    
    def _store_input_events(self) -> None:
        """Store source comparison events (phase/overlap events) for each signal.
        
        This stores the original phase/overlap events from the source data,
        not the detector actuations. This allows meaningful comparison between
        the source data and the replay output events.
        
        Also caches run duration from each replay to avoid recreating them later.
        """
        self._cached_durations: Dict[str, float] = {}
        
        total = len(self.config.signals)
        for index, signal_config in enumerate(self.config.signals, start=1):
            self._progress.emit(
                Stage.STORE_INPUT,
                f"[{signal_config.device_id}] Preparing replay and storing input events",
                device_id=signal_config.device_id,
                index=index,
                total=total,
                log=False,
            )
            replay = SignalReplay(
                signal_config,
                simulation_speed=self.config.simulation_speed,
                snmp_timeout_seconds=self.config.snmp_timeout_seconds,
                snmp_send_retries=self.config.snmp_send_retries,
                snmp_retry_backoff_seconds=self.config.snmp_retry_backoff_seconds,
                show_progress_logs=self.config.show_progress_logs,
                progress_log_interval_seconds=self.config.progress_log_interval_seconds,
                stop_event=self._stop_event,
                debug=self.debug
            )

            try:
                # Cache duration so _get_estimated_duration doesn't recreate replays
                self._cached_durations[signal_config.device_id] = replay.get_run_duration()

                # Get source comparison events (phase/overlap events)
                comparison_events = replay.get_source_comparison_events()

                if comparison_events is not None and not comparison_events.empty:
                    # Data is already in the correct format (timestamp, event_id, parameter)
                    self.db.insert_input_events(comparison_events, signal_config.device_id)

                detector_events = replay.get_source_detector_events(event_ids=[82])
                self.db.insert_input_detector_events(detector_events, signal_config.device_id)
            finally:
                replay.release_cached_data(keep_activation_feed=False)
    
    def _run_single_signal(
        self,
        signal_config: SignalConfig,
        latency_offset_provider: Optional[AdaptiveLatencyOffsetManager] = None,
    ) -> datetime:
        """Run replay for a single signal and return start time."""
        replay = SignalReplay(
            signal_config,
            simulation_speed=self.config.simulation_speed,
            snmp_timeout_seconds=self.config.snmp_timeout_seconds,
            snmp_send_retries=self.config.snmp_send_retries,
            snmp_retry_backoff_seconds=self.config.snmp_retry_backoff_seconds,
            show_progress_logs=self.config.show_progress_logs,
            progress_log_interval_seconds=self.config.progress_log_interval_seconds,
            stop_event=self._stop_event,
            latency_offset_provider=latency_offset_provider,
            detector_reset_timeout_seconds=self.config.detector_reset_timeout_seconds,
            debug=self.debug,
            on_progress=self._progress,
        )
        self._active_replays[signal_config.device_id] = replay
        try:
            return replay.run()
        finally:
            replay.release_cached_data(keep_activation_feed=False)

    def request_stop(self, reason: str = 'user') -> None:
        """Cancel the simulation. Thread-safe; returns immediately.

        Replays stop within about 0.25 s, queued SNMP sends are dropped, every
        detector group that was driven is reset to 0, and :meth:`run` returns
        a result with ``cancelled=True`` and ``stop_reason='cancelled'``
        (normally within a second; at most about ``stop_grace_seconds`` plus
        ``detector_reset_timeout_seconds``). The run in progress is recorded
        as 'cancelled', not completed, so a resume runs it again.

        Args:
            reason: Free-text cancel reason reported as ``cancel_reason``
                (``'user'`` by default; Ctrl+C uses ``'keyboard_interrupt'``).
        """
        self._set_stop(reason, cancel=True)

    def _set_stop(self, reason: str, cancel: bool) -> None:
        """Record the first stop reason and set every stop flag for this simulation."""
        with self._stop_lock:
            if self._stop_reason is None:
                self._stop_reason = reason
                self._cancel_requested = cancel
                if cancel:
                    logger.warning("Stop requested (%s)", reason)
        self._stop_event.set()
        if cancel:
            self._status.set_state('stopping', stop_reason=reason)
        if cancel and self._external_stop_event is not None:
            self._external_stop_event.set()
        run_stop_event = self._run_stop_event
        if run_stop_event is not None and (cancel or reason == 'collection_error'):
            run_stop_event.set()

    def _should_stop(self) -> bool:
        """Return True once any stop was requested, picking up the caller's event."""
        external = self._external_stop_event
        if external is not None and external.is_set() and not self._stop_event.is_set():
            self.request_stop('user')
        return self._stop_event.is_set()

    def _is_cancelled(self) -> bool:
        """True when the stop came from request_stop / Ctrl+C / the shared event."""
        self._should_stop()
        return self._cancel_requested

    def _on_collection_fatal_error(self, exc: BaseException) -> None:
        """Collector callback: stop the replay as soon as collection has failed."""
        self._set_stop('collection_error', cancel=False)

    def _wait_stoppable(self, seconds: float) -> bool:
        """Sleep up to ``seconds``; return False early if a stop is requested."""
        deadline = time.monotonic() + max(0.0, seconds)
        while True:
            if self._should_stop():
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            self._stop_event.wait(min(remaining, _MAIN_WAIT_SECONDS))

    def _join_bounded(self, thread: threading.Thread, timeout: float, stop_aware: bool = True) -> bool:
        """Join ``thread`` in short steps for at most ``timeout`` seconds.

        Short steps keep Ctrl+C responsive on Windows. With ``stop_aware``,
        the join also ends early once the simulation is cancelled. Returns
        True when the thread has finished.
        """
        deadline = time.monotonic() + max(0.0, timeout)
        while thread.is_alive():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if stop_aware and self._is_cancelled():
                break
            thread.join(min(remaining, _MAIN_WAIT_SECONDS))
        return not thread.is_alive()

    def _safe_db_call(self, method_name: str, *args: Any, **kwargs: Any) -> None:
        """Call a DatabaseManager method, logging instead of raising on failure."""
        method = getattr(self.db, method_name, None)
        if method is None:
            return
        try:
            method(*args, **kwargs)
        except Exception:
            logger.warning("Database update %s failed", method_name, exc_info=True)

    def _all_device_ids(self) -> List[str]:
        return [str(sig.device_id) for sig in self.config.signals]

    def _device_ids(self) -> List[str]:
        """Devices recorded for the current run (all, unless a resume re-runs some)."""
        if self._run_devices is not None:
            return list(self._run_devices)
        return self._all_device_ids()

    def _done_runs_by_device(self) -> Dict[str, Set[int]]:
        """Run numbers already done per device in the working database."""
        devices = self._all_device_ids()
        reader = getattr(self.db, 'get_done_runs_by_device', None)
        if callable(reader):
            try:
                return {str(d): set(runs) for d, runs in reader(devices).items()}
            except Exception:
                logger.warning("Could not read done runs per device", exc_info=True)
        return {d: set(range(1, self._run_offset + 1)) for d in devices}

    def _run_plan(self, done: Dict[str, Set[int]]) -> List[Tuple[int, List[str]]]:
        """(run_number, devices to run) for every run up to the target not done for some device."""
        devices = self._all_device_ids()
        plan = []
        for run_num in range(1, self.config.simulation_replays + 1):
            needed = [d for d in devices if run_num not in done.get(d, set())]
            if needed:
                plan.append((run_num, needed))
        return plan

    def _load_prior_conflicts(self, done: Dict[str, Set[int]]) -> None:
        """Load conflicts stored by earlier calls for runs that are already done."""
        self._prior_conflict_records = []
        getter = getattr(self.db, 'get_conflicts', None)
        if not callable(getter):
            return
        for device_id, runs in done.items():
            if not runs:
                continue
            try:
                frame = getter(device_id=device_id)
            except Exception:
                logger.warning("Could not read stored conflicts for %s", device_id, exc_info=True)
                continue
            if frame is None or frame.empty:
                continue
            for row in frame.to_dict('records'):
                if row.get('run_number') is None or int(row['run_number']) not in runs:
                    continue
                clean = {k: (None if _is_missing(v) else v) for k, v in row.items()}
                clean = {k: v for k, v in clean.items() if v is not None}
                try:
                    record = ConflictRecord.from_dict(clean)
                except Exception:
                    logger.debug("Skipping unreadable stored conflict %r", row, exc_info=True)
                    continue
                record.run_number = int(record.run_number)
                if record.occurrences is not None:
                    record.occurrences = int(record.occurrences)
                key = (str(record.device_id), record.run_number, record.conflict_details)
                if key in self._conflict_keys:
                    continue
                self._conflict_keys.add(key)
                self._prior_conflict_records.append(record)
        if self._prior_conflict_records:
            logger.info(
                "%d conflict(s) already stored from earlier runs in %s",
                len(self._prior_conflict_records), self.config.db_path,
            )

    def _source_time(self, device_id: str, run_number: int, timestamp: datetime) -> Optional[datetime]:
        """Collected timestamp -> matching moment in the source log (None if unknown)."""
        record = self._run_records.get((str(device_id), int(run_number)))
        if record is not None:
            return record.source_time(timestamp)
        if int(run_number) == self._current_run:
            for key, replay in list(self._active_replays.items()):
                if str(key) == str(device_id):
                    return source_time_from_info(getattr(replay, 'replay_info', None) or {}, timestamp)
        return None

    def _record_run(self, run_num: int, status: str) -> None:
        """Keep one RunRecord per device for ``run_num`` and store its timing."""
        record_status = _RECORD_STATUS.get(status, 'failed')
        failed = {str(d) for d in self._failed_signals_by_run.get(run_num, [])} & set(self._device_ids())
        replays = {str(k): v for k, v in self._active_replays.items()} if self._current_run == run_num else {}
        for device_id in self._device_ids():
            info = dict(getattr(replays.get(device_id), 'replay_info', None) or {})
            reset = self._detectors_reset.get(device_id)
            record = RunRecord(
                device_id=device_id,
                run_number=run_num,
                status='failed' if device_id in failed else record_status,
                replay_start=info.get('replay_start'),
                replay_end=info.get('replay_end'),
                source_start=info.get('source_start'),
                source_end=info.get('source_end'),
                date_shift_seconds=info.get('date_shift_seconds'),
                events_sent=info.get('events_sent'),
                events_total=info.get('events_total'),
                mode=info.get('mode'),
                speed=float(info.get('speed') or self.config.simulation_speed),
                detectors_reset=None if reset is None else bool(reset),
            )
            self._run_records[(device_id, run_num)] = record
            self._safe_db_call(
                'update_run_details', run_num, device_id,
                run_uuid=self.run_uuid,
                replay_start=record.replay_start,
                replay_end=record.replay_end,
                source_start=record.source_start,
                source_end=record.source_end,
                date_shift_seconds=record.date_shift_seconds,
                events_sent=record.events_sent,
                events_total=record.events_total,
            )
        if failed and record_status in ('completed', 'incomplete'):
            # A device whose replay failed did not run: resume runs it again.
            self._safe_db_call('mark_run_failed', run_num, device_ids=sorted(failed))

    def _run_all_signals(
        self,
        latency_offset_provider: Optional[AdaptiveLatencyOffsetManager] = None,
    ) -> Tuple[Dict[str, datetime], List[str]]:
        """
        Run replay for all signals in parallel and return start times.

        Blocks until every replay has finished, but the main thread wakes
        every 0.25 s, so Ctrl+C and stop requests are seen promptly (a
        wait with no timeout cannot be interrupted on Windows). After a stop,
        workers get ``stop_grace_seconds`` to finish (they reset their
        detectors on the way out); any still running are then abandoned and
        a safety-net detector reset is sent for them.

        The first Ctrl+C becomes ``request_stop('keyboard_interrupt')``; a
        second one stops waiting for the workers and goes straight to the
        safety-net reset.

        Individual signal failures are logged but do not abort other signals.
        """
        start_times: Dict[str, datetime] = {}
        failed_signals: List[str] = []
        signals = self.config.signals
        self._active_replays = {}

        executor = ThreadPoolExecutor(max_workers=len(signals), thread_name_prefix='replay')
        futures: Dict[Any, str] = {}
        pending: set = set()
        interrupts = 0
        try:
            for sig in signals:
                futures[executor.submit(run_in_log_context(self._run_single_signal), sig, latency_offset_provider)] = sig.device_id
            pending = set(futures)
            deadline: Optional[float] = None
            while pending:
                try:
                    done, pending = wait(pending, timeout=_MAIN_WAIT_SECONDS, return_when=FIRST_COMPLETED)
                except KeyboardInterrupt:
                    interrupts += 1
                    if interrupts == 1:
                        logger.warning("Keyboard interrupt received. Stopping active replays...")
                        self.request_stop('keyboard_interrupt')
                        continue
                    logger.warning("Second keyboard interrupt; no longer waiting for replay workers")
                    break

                for future in done:
                    device_id = futures[future]
                    try:
                        start_times[device_id] = future.result()
                    except Exception as exc:
                        failed_signals.append(device_id)
                        self._progress.emit(
                            Stage.SIGNAL_FAILED,
                            f"*** Signal {device_id} failed: {exc}",
                            level=logging.ERROR,
                            device_id=device_id,
                            extra={"error": str(exc)},
                        )

                if pending and self._should_stop():
                    now = time.monotonic()
                    if deadline is None:
                        deadline = now + self.config.stop_grace_seconds
                    elif now >= deadline:
                        logger.warning(
                            "%d replay worker(s) did not stop within %.1fs; abandoning them",
                            len(pending), self.config.stop_grace_seconds,
                        )
                        break
        finally:
            abandoned = [futures[f] for f in pending if not f.done()]
            executor.shutdown(wait=False, cancel_futures=True)
            self._record_detector_resets(abandoned)

        if failed_signals:
            logger.warning(
                "*** %d/%d signals failed: %s. Continuing with %d successful signals.",
                len(failed_signals), len(self.config.signals),
                ", ".join(failed_signals), len(start_times),
            )

        return start_times, failed_signals

    def _record_detector_resets(self, abandoned: List[str]) -> None:
        """Record each replay's end-of-run reset; run a safety-net reset where it never ran.

        A replay that finished reports True/False itself. One that was
        abandoned (or died before its reset) reports None; for those the
        touched groups are reset from here, in parallel, within
        ``detector_reset_timeout_seconds``.
        """
        needs_reset: List[Tuple[str, SignalReplay]] = []
        for device_id, replay in list(self._active_replays.items()):
            status = replay.detectors_reset
            if status is None:
                needs_reset.append((device_id, replay))
            else:
                self._detectors_reset[device_id] = bool(status)

        if not needs_reset:
            return

        budget = self.config.detector_reset_timeout_seconds
        outcome: Dict[str, bool] = {}

        def _reset(device_id: str, replay: SignalReplay) -> None:
            outcome[device_id] = replay.reset_touched_groups_blocking(budget=budget)

        threads = []
        for device_id, replay in needs_reset:
            if device_id in abandoned:
                logger.warning("[%s] Replay worker abandoned; sending safety-net detector reset", device_id)
            thread = threading.Thread(
                target=run_in_log_context(_reset), args=(device_id, replay), name=f"safety-reset-{device_id}", daemon=True
            )
            thread.start()
            threads.append(thread)
        deadline = time.monotonic() + budget + 1.5
        for thread in threads:
            while thread.is_alive() and time.monotonic() < deadline:
                try:
                    thread.join(_MAIN_WAIT_SECONDS)
                except KeyboardInterrupt:
                    self.request_stop('keyboard_interrupt')
                    logger.warning("Keyboard interrupt during detector reset; finishing reset first")
        for device_id, _replay in needs_reset:
            self._detectors_reset[device_id] = outcome.get(device_id, False)
    
    def _get_estimated_duration(self) -> float:
        """Get estimated simulation duration in seconds.
        
        Uses cached durations from _store_input_events to avoid
        recreating expensive SignalReplay instances.
        """
        if self._cached_durations:
            return max(self._cached_durations.values()) if self._cached_durations else 0.0
        
        # Fallback: create replays (should not normally be reached)
        max_duration = 0.0
        
        for signal_config in self.config.signals:
            replay = SignalReplay(
                signal_config,
                simulation_speed=self.config.simulation_speed,
                snmp_timeout_seconds=self.config.snmp_timeout_seconds,
                snmp_send_retries=self.config.snmp_send_retries,
                snmp_retry_backoff_seconds=self.config.snmp_retry_backoff_seconds,
                show_progress_logs=self.config.show_progress_logs,
                progress_log_interval_seconds=self.config.progress_log_interval_seconds,
                debug=self.debug
            )
            duration = replay.get_run_duration()
            replay.release_cached_data(keep_activation_feed=False)
            if duration > max_duration:
                max_duration = duration
        
        return max_duration
    
    def _on_conflict_detected(self, conflicts: List) -> None:
        """Callback when conflicts are detected."""
        new_by_device: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
        for conflict in conflicts:
            conflict_key = (
                conflict.device_id,
                conflict.run_number,
                conflict.conflict_details,
            )
            if conflict_key in self._conflict_keys:
                continue

            if getattr(conflict, 'source_equivalent_timestamp', None) is None and hasattr(conflict, 'source_equivalent_timestamp'):
                try:
                    conflict.source_equivalent_timestamp = self._source_time(
                        conflict.device_id, conflict.run_number, conflict.timestamp
                    )
                except Exception:
                    logger.debug("Source time mapping failed", exc_info=True)

            as_legacy = getattr(conflict, 'as_legacy_dict', None)
            conflict_dict = as_legacy() if callable(as_legacy) else {
                'device_id': conflict.device_id,
                'run_number': conflict.run_number,
                'timestamp': conflict.timestamp,
                'conflict_details': conflict.conflict_details
            }

            self._conflict_keys.add(conflict_key)
            self._conflicts_found.append(conflict_dict)
            self._conflict_records.append(conflict)
            new_by_device.setdefault((conflict.device_id, conflict.run_number), []).append(conflict_dict)

        for (device_id, run_number), found in new_by_device.items():
            self._progress.emit(
                Stage.CONFLICT,
                f"[{device_id}] {len(found)} conflict(s) in run {run_number}: "
                + "; ".join(str(c['conflict_details']) for c in found[:3])
                + (" ..." if len(found) > 3 else ""),
                level=logging.WARNING,
                device_id=device_id,
                run_number=run_number,
                log=False,
                extra={"conflicts": found, "stop_on_conflict": bool(self.config.stop_on_conflict)},
            )

        if self.config.stop_on_conflict:
            self._set_stop('conflict', cancel=False)

    def get_status(self) -> Dict[str, Any]:
        """Thread-safe, JSON-safe snapshot of the simulation's progress.

        Safe to call from any thread at any time (for example from a REST
        handler while :meth:`run` executes on a worker thread). Keys are
        described in :class:`~signal_replay.progress.StatusTracker`;
        ``state`` is one of ``idle``, ``running``, ``stopping``,
        ``cancelled``, ``completed`` or ``failed``.
        """
        status = self._status.snapshot()
        status["kind"] = "simulation"
        return status

    def run(self) -> ReplicationResult:
        """
        Run the complete simulation.

        Executes all configured replay runs, collects data,
        checks for conflicts, and runs comparison analysis.

        Cancelling: call :meth:`request_stop` from another thread (or set the
        ``stop_event`` passed to the constructor). ``run()`` then returns
        normally with ``cancelled=True``. Ctrl+C in the thread running
        ``run()`` triggers the same clean stop (replays stopped, detectors
        reset, run recorded as cancelled) and then re-raises
        ``KeyboardInterrupt``; the result is still available as
        :attr:`last_results`.

        Returns:
            A :class:`~signal_replay.ReplicationResult` (also kept as
            :attr:`result`). Its attributes are the typed result
            (``replicated``, ``first_conflict_run``, ``runs`` per device,
            ``conflicts`` as ConflictRecord objects, ``comparisons``, ...).
            For 0.x code it is also a read-only mapping with the old keys:
            - completed_runs: Run numbers whose replay finished and whose
              conflict check ran (includes incomplete runs)
            - incomplete_runs: Completed runs whose output events were not
              confirmed complete within ``final_collection_timeout_seconds``
              (recorded as 'incomplete'; conflict detection used the events
              received)
            - conflicts: List of detected conflicts
            - failed_signals_by_run: Dict of run_number -> failed device IDs
            - stopped_early: True when the simulation ended before all runs
              (for any reason)
            - stop_reason: One of 'completed', 'conflict', 'cancelled',
              'collection_error', 'all_signals_failed'
            - cancelled: True when request_stop / Ctrl+C / the shared stop
              event cancelled the simulation
            - cancel_reason: The reason given to request_stop, or None
            - cancelled_run: Run number that was cut short by the cancel, or None
            - collection_error: True when data collection failed (or every
              signal failed)
            - detectors_reset: {device_id: bool}, whether the end-of-replay
              detector reset of the last run was confirmed for each device
            - collection_health: {device_id: {...}} for the last run: polls,
              failures, consecutive_failures, rows, first_timestamp,
              last_timestamp, last_success, last_error, complete_through,
              degraded
            - collection_health_by_run: {run_number: collection_health}
            - comparison_summary: DTW comparison summary string (comparison
              is skipped when the simulation was cancelled)
        """
        self._status.start(total_runs=self.config.simulation_replays)
        self._started_at = datetime.now()
        log_scope = (
            log_to_file(self.work_dir / RUN_LOG_NAME)
            if self.run_log and self.work_dir is not None else nullcontext()
        )
        try:
            with log_scope:
                return self._run()
        except KeyboardInterrupt:
            if self._status.state in ('running', 'stopping'):
                self._status.set_state('cancelled', stop_reason='keyboard_interrupt')
                self._progress.emit(
                    Stage.CANCELLED, "Simulation cancelled (keyboard_interrupt)",
                    level=logging.WARNING, log=False,
                )
            raise
        except BaseException as exc:
            if self._status.state in ('running', 'stopping'):
                self._status.set_state('failed', error=f"{type(exc).__name__}: {exc}")
                self._progress.emit(
                    Stage.ERROR, f"Simulation failed: {type(exc).__name__}: {exc}",
                    level=logging.ERROR, log=False,
                    extra={"final": True, "error": f"{type(exc).__name__}: {exc}"},
                )
            raise
        finally:
            self._write_manifest()

    def _write_manifest(self) -> None:
        """Refresh ``manifest.json`` in the working folder (if there is one)."""
        if self.work_dir is None:
            return
        result = self.result
        try:
            write_manifest(
                self.work_dir,
                run_uuid=self.run_uuid,
                kind="replication",
                extra={
                    "db_path": str(self.config.db_path),
                    "stop_reason": result.stop_reason if result is not None else None,
                    "state": self._status.state,
                },
            )
        except Exception:
            logger.warning("Could not write manifest.json in %s", self.work_dir, exc_info=True)

    def _run(self) -> Dict[str, Any]:
        """Body of :meth:`run`."""
        logger.info(
            "Starting ATC simulation with %d signals, %d replays",
            len(self.config.signals), self.config.simulation_replays,
        )

        tod_mode = any(sig.tod_align for sig in self.config.signals)
        t0 = time.time()
        estimated_duration = self._get_estimated_duration()
        duration_str = f"{int(estimated_duration // 3600):d}h {int((estimated_duration % 3600) // 60):02d}m"
        if tod_mode:
            logger.info(
                "Replay data spans ~%s (TOD-align: events sent at real wall-clock times)",
                duration_str,
            )
        else:
            logger.info(
                "Estimated duration per run: %s (computed in %.1fs)",
                duration_str, time.time() - t0,
            )

        adaptive_latency_enabled = bool(self.config.replay_latency_offset_lookback_min)
        if adaptive_latency_enabled and not tod_mode:
            raise ValueError("replay_latency_offset_lookback_min requires tod_align=True")

        collector = self._create_collector(adaptive_latency_enabled)

        stopped_early = False
        collection_error = False
        stop_reason = 'completed'

        done = self._done_runs_by_device()
        all_devices = self._all_device_ids()
        self._prior_completed_runs = sorted(
            set.intersection(*(done.get(d, set()) for d in all_devices)) if all_devices else set()
        )
        self._load_prior_conflicts(done)
        plan = self._run_plan(done)
        if not plan:
            logger.info(
                "Requested total runs already satisfied: %d completed, target was %d.",
                self._run_offset, self.config.simulation_replays,
            )

        for run_num, run_devices in plan:
            if len(run_devices) < len(all_devices):
                logger.info(
                    "Run %d is done for %s; replaying it again and recording it only for %s",
                    run_num, ", ".join(d for d in all_devices if d not in run_devices),
                    ", ".join(run_devices),
                )
                self._run_devices = list(run_devices)
            else:
                self._run_devices = None
            limit = getattr(collector, 'limit_to_devices', None)
            if callable(limit):
                limit(self._run_devices)

            if self._should_stop():
                stopped_early = True
                stop_reason = self._stop_reason_for_result()
                logger.warning(
                    "Simulation stopped before run %d (%s)", run_num, self._stop_reason
                )
                break

            status = 'failed'
            try:
                status = self._execute_run(run_num, collector, adaptive_latency_enabled)
            except KeyboardInterrupt:
                # Ctrl+C outside the replay wait (e.g. during collection or DB work).
                logger.warning("Keyboard interrupt received. Stopping simulation...")
                self.request_stop('keyboard_interrupt')
                self._abandon_run_after_interrupt(run_num)
                status = 'cancelled'
            finally:
                self._record_collection_health(run_num, collector)
                self._record_run(run_num, status)

            if status in ('completed', 'incomplete'):
                # Check if we should stop
                if self._conflicts_found and self.config.stop_on_conflict:
                    stopped_early = True
                    stop_reason = 'conflict'
                    logger.warning("Conflict detected! Stopping simulation.")
                    break
                continue

            stopped_early = True
            stop_reason = status
            if status in ('collection_error', 'all_signals_failed'):
                collection_error = True
            break

        self._run_devices = None
        limit = getattr(collector, 'limit_to_devices', None)
        if callable(limit):
            limit(None)

        cancelled = self._is_cancelled()
        if cancelled:
            stop_reason = 'cancelled'
            stopped_early = True

        # Run comparison analysis
        if collection_error:
            logger.warning("--- Skipping comparison (collection failed) ---")
        elif cancelled:
            logger.warning("--- Skipping comparison (simulation cancelled) ---")
        elif self.skip_comparison:
            pass  # comparison deferred to report step
        else:
            self._progress.emit(Stage.COMPARE, "--- Running Comparison Analysis ---")
            self._run_comparison()

        close = getattr(collector, 'close', None)
        if close is not None:
            close()
        self._conflict_store_errors = list(getattr(collector, 'conflict_store_errors', None) or [])

        results = self._build_result(stopped_early, stop_reason, cancelled, collection_error)
        self.last_results = results
        self.result = results

        # Print summary
        self._print_summary()
        self._report_finished(results)

        if cancelled and self._stop_reason == 'keyboard_interrupt':
            # Cleanup is done; let the CLI see the Ctrl+C.
            raise KeyboardInterrupt

        return results

    def _build_result(
        self,
        stopped_early: bool,
        stop_reason: str,
        cancelled: bool,
        collection_error: bool,
    ) -> ReplicationResult:
        """Assemble the ReplicationResult of this run()."""
        from . import __version__ as package_version

        last_health_run = max(self._collection_health_by_run) if self._collection_health_by_run else None
        all_conflicts = sorted(
            list(self._prior_conflict_records) + list(self._conflict_records),
            key=lambda c: (int(c.run_number), str(c.device_id), str(c.timestamp)),
        )
        conflict_runs = [int(c.run_number) for c in all_conflicts]
        comparisons: List[ComparisonResult] = []
        for device_results in (self._comparison_results or {}).values():
            comparisons.extend(device_results)
        runs = [self._run_records[key] for key in sorted(self._run_records, key=lambda k: (k[1], k[0]))]
        return ReplicationResult(
            run_uuid=self.run_uuid,
            stop_reason=stop_reason,
            replicated=bool(all_conflicts),
            first_conflict_run=min(conflict_runs) if conflict_runs else None,
            runs_attempted=self._runs_attempted,
            runs_completed=len(self._completed_runs),
            runs=runs,
            conflicts=all_conflicts,
            prior_completed_runs=list(self._prior_completed_runs),
            comparisons=comparisons,
            completed_runs=self._completed_runs,
            incomplete_runs=self._incomplete_runs,
            failed_signals_by_run=self._failed_signals_by_run,
            stopped_early=stopped_early,
            cancelled=cancelled,
            cancel_reason=self._stop_reason if cancelled else None,
            cancelled_run=self._cancelled_run,
            collection_error=collection_error,
            detectors_reset=dict(self._detectors_reset),
            collection_health=(
                self._collection_health_by_run[last_health_run] if last_health_run is not None else {}
            ),
            collection_health_by_run=self._collection_health_by_run,
            conflict_store_errors=list(self._conflict_store_errors),
            comparison_summary=self.get_comparison_summary(),
            db_path=str(self.config.db_path),
            work_dir=str(self.work_dir) if self.work_dir is not None else None,
            started_at=self._started_at,
            finished_at=datetime.now(),
            package_version=package_version,
        )

    def _report_finished(self, results: Dict[str, Any]) -> None:
        """Set the final status and emit the terminal DONE / CANCELLED / ERROR event."""
        extra = {
            "stop_reason": results['stop_reason'],
            "completed_runs": list(results['completed_runs']),
            "incomplete_runs": list(results['incomplete_runs']),
            "conflicts_found": len(results['conflicts']),
            "replicated": bool(results.get('replicated')),
            "first_conflict_run": results.get('first_conflict_run'),
            "run_uuid": results.get('run_uuid'),
            "final": True,
        }
        if results['cancelled']:
            self._status.set_state('cancelled', stop_reason=results['cancel_reason'])
            self._progress.emit(
                Stage.CANCELLED, f"Simulation cancelled ({results['cancel_reason']})",
                level=logging.WARNING, log=False, extra=extra,
            )
        elif results['stop_reason'] in ('collection_error', 'all_signals_failed'):
            self._status.set_state('failed', stop_reason=results['stop_reason'], error=results['stop_reason'])
            self._progress.emit(
                Stage.ERROR, f"Simulation failed ({results['stop_reason']})",
                level=logging.ERROR, log=False, extra=extra,
            )
        else:
            self._status.set_state('completed', stop_reason=results['stop_reason'])
            self._progress.emit(
                Stage.DONE,
                f"Simulation complete: {len(results['completed_runs'])} run(s), "
                f"{len(results['conflicts'])} conflict(s)",
                log=False, extra=extra,
            )

    def _stop_reason_for_result(self) -> str:
        """Map the recorded stop reason onto a results['stop_reason'] value."""
        if self._cancel_requested:
            return 'cancelled'
        if self._stop_reason in STOP_REASONS:
            return self._stop_reason
        return 'cancelled'

    def _execute_run(self, run_num: int, collector: DataCollector, adaptive_latency_enabled: bool) -> str:
        """Run one replay pass and return its status.

        Returns 'completed', 'cancelled', 'collection_error' or
        'all_signals_failed'.
        """
        self._progress.set_context(run_number=run_num)
        self._progress.emit(
            Stage.RUN_START,
            f"Working on run {run_num} of {self.config.simulation_replays}",
            run_number=run_num,
        )
        _log_memory(f"[start run {run_num}]")
        self._current_run = run_num
        self._runs_attempted += 1
        self._active_replays = {}
        # Scope run cleanup to the active devices only so a later batch that
        # reuses run number 1 does not erase previously collected devices.
        # On a resume only the devices that are not done for this run are
        # cleared, so stored results of devices that finished it are kept.
        self.db.clear_run_data(run_num, device_ids=self._device_ids())
        self.db.mark_run_started(run_num, device_ids=self._device_ids(), run_uuid=self.run_uuid)

        # Per-run collection flags. request_stop() also sets run_stop_event.
        run_stop_event = threading.Event()
        collection_abort = threading.Event()
        collection_error_event = threading.Event()
        ctx: Dict[str, Any] = {
            'run_num': run_num,
            'run_stop_event': run_stop_event,
            'collection_abort': collection_abort,
            'collection_thread': None,
            'finished': False,
        }
        self._run_ctx = ctx
        self._run_stop_event = run_stop_event
        if self._is_cancelled():
            run_stop_event.set()

        # Start data collection in background thread
        latency_manager = None
        if adaptive_latency_enabled:
            latency_manager = AdaptiveLatencyOffsetManager(
                db_manager=self.db,
                run_number=run_num,
                device_ids=[sig.device_id for sig in self.config.signals],
                initial_offset_seconds=self.config.signals[0].replay_latency_offset_seconds,
                lookback_minutes=float(self.config.replay_latency_offset_lookback_min),
                min_samples=self.config.replay_latency_offset_min_samples,
                debug=self.debug,
            )

        collection_kwargs: Dict[str, Any] = {
            "error_event": collection_error_event,
            "abort_event": collection_abort,
            "on_fatal_error": self._on_collection_fatal_error,
        }
        if latency_manager is not None:
            def _after_poll(now: datetime, _manager=latency_manager, _collector=collector) -> Any:
                # End the matching window where the collected output events
                # are complete, not at the PC clock (file sources lag).
                snapshot = getattr(_collector, "complete_through_snapshot", None)
                return _manager.update_after_poll(
                    now, data_complete_through=snapshot() if callable(snapshot) else None,
                )

            collection_kwargs["after_collect_callback"] = _after_poll

        collection_thread = threading.Thread(
            target=run_in_log_context(collector.run_collection_loop),
            args=(run_num, datetime.now(), run_stop_event, self._on_conflict_detected),
            kwargs=collection_kwargs,
            daemon=True,
            name=f"collect-run{run_num}",
        )
        collection_thread.start()
        ctx['collection_thread'] = collection_thread

        # Run all signals - this blocks until all replays complete or stop.
        start_times, failed_signals = self._run_all_signals(latency_manager)
        replay_end = datetime.now()
        if failed_signals:
            self._failed_signals_by_run[run_num] = failed_signals
            logger.warning("Run %d signal failures: %s", run_num, ", ".join(failed_signals))
        valid_starts = [t for t in start_times.values() if t is not None]
        self._simulation_start_time = min(valid_starts) if valid_starts else datetime.now()

        if self._is_cancelled():
            self._finish_cancelled_run(run_num, collector, ctx, have_data=bool(valid_starts))
            return 'cancelled'

        # Check if collection hit a fatal error during replay (it also stopped the replay)
        if collection_error_event.is_set() or self._stop_reason == 'collection_error':
            logger.error("*** Aborting run %d: data collection failed.", run_num)
            run_stop_event.set()
            if not self._join_bounded(collection_thread, 5.0, stop_aware=False):
                collection_abort.set()
            self._safe_db_call('mark_run_failed', run_num, device_ids=self._device_ids())
            return 'collection_error'

        if not start_times:
            logger.error("*** Aborting run %d: all signals failed.", run_num)
            run_stop_event.set()
            if not self._join_bounded(collection_thread, 60):
                collection_abort.set()
            self._safe_db_call('mark_run_failed', run_num, device_ids=self._device_ids())
            return 'all_signals_failed'

        # Stop collection for this run
        run_stop_event.set()
        if not self._join_bounded(collection_thread, 60):
            if not self._is_cancelled():
                # Ensure collection thread has fully released file handles
                logger.warning("Collection thread still running, waiting for it to finish...")
                self._join_bounded(collection_thread, 120)

        if self._is_cancelled():
            self._finish_cancelled_run(run_num, collector, ctx, have_data=True)
            return 'cancelled'

        if self.config.post_replay_settle_seconds > 0:
            self._progress.emit(
                Stage.WAITING,
                f"Waiting {self.config.post_replay_settle_seconds:.0f}s for the controller to settle",
                log=False,
                extra={"reason": "settle", "seconds": self.config.post_replay_settle_seconds},
            )
            self._wait_stoppable(self.config.post_replay_settle_seconds)
            if self._is_cancelled():
                self._finish_cancelled_run(run_num, collector, ctx, have_data=True)
                return 'cancelled'

        # Final collection: poll until every source reports its events
        # complete through the end of the replay (plus settle), then check the
        # whole run for conflicts. A cancel ends the wait promptly.
        complete_target = replay_end + timedelta(seconds=self.config.post_replay_settle_seconds)
        outcome = collector.finalize_run(
            run_num,
            self._simulation_start_time,
            complete_target,
            conflict_callback=self._on_conflict_detected,
            stop_event=_CancelView(self),
        ) or {}
        status = outcome.get('status', 'complete')
        if latency_manager is not None:
            try:
                latency_manager.warn_if_never_applied()
            except Exception:
                logger.debug("Adaptive latency summary failed", exc_info=True)
        if status == 'stopped' or self._is_cancelled():
            self._finish_cancelled_run(run_num, collector, ctx, have_data=False)
            return 'cancelled'

        self._completed_runs.append(run_num)
        ctx['finished'] = True
        _log_memory(f"[end run {run_num}]")
        if status == 'incomplete':
            self._incomplete_runs.append(run_num)
            self._safe_db_call('mark_run_incomplete', run_num, device_ids=self._device_ids())
            self._progress.emit(
                Stage.RUN_COMPLETE,
                f"Completed run {run_num} of {self.config.simulation_replays} with INCOMPLETE "
                f"output events for: {', '.join(outcome.get('incomplete_devices', []))}",
                level=logging.WARNING,
                run_number=run_num,
                extra={"status": "incomplete",
                       "incomplete_devices": list(outcome.get('incomplete_devices', []))},
            )
            return 'incomplete'
        self.db.mark_run_completed(run_num, device_ids=self._device_ids())
        self._progress.emit(
            Stage.RUN_COMPLETE,
            f"Completed run {run_num} of {self.config.simulation_replays}",
            run_number=run_num,
            extra={"status": "completed"},
        )
        return 'completed'

    def _create_collector(self, adaptive_latency_enabled: bool) -> DataCollector:
        """Build the DataCollector for this simulation's signals and event source."""
        targets = {
            sig.device_id: CollectionTarget(
                device_id=sig.device_id,
                ip=sig.ip,
                http_port=sig.http_port,
                extra=dict(sig.collection_extra or {}),
            )
            for sig in self.config.signals
        }
        return DataCollector(
            db_path=self.config.db_path,
            device_configs=targets,
            collection_interval_minutes=self.config.collection_interval_minutes,
            stop_on_conflict=self.config.stop_on_conflict,
            debug=self.debug,
            event_source=self.config.event_source,
            incompatible_pairs={sig.device_id: sig.incompatible_pairs for sig in self.config.signals},
            clock_offsets={sig.device_id: sig.clock_offset_seconds for sig in self.config.signals},
            source_timezones={sig.device_id: sig.source_timezone for sig in self.config.signals},
            required_codes={
                sig.device_id: required_event_codes(sig.incompatible_pairs, adaptive_latency_enabled)
                for sig in self.config.signals
            },
            final_collection_timeout_seconds=self.config.final_collection_timeout_seconds,
            final_collection_poll_seconds=self.config.final_collection_poll_seconds,
            on_progress=self._progress,
            source_time=self._source_time,
            run_uuid=self.run_uuid,
        )

    def _record_collection_health(self, run_num: int, collector: DataCollector) -> None:
        """Keep the collector's per-device health for ``run_num`` in the results."""
        snapshot = getattr(collector, 'health_snapshot', None)
        if snapshot is None or getattr(collector, '_health_run', None) != run_num:
            return
        try:
            self._collection_health_by_run[run_num] = snapshot()
        except Exception:
            logger.warning("Could not read collection health for run %d", run_num, exc_info=True)

    def _finish_cancelled_run(
        self,
        run_num: int,
        collector: DataCollector,
        ctx: Dict[str, Any],
        have_data: bool,
    ) -> None:
        """Wind down a cancelled run quickly and record it as 'cancelled'.

        Skips the settle wait and conflict detection. If the background
        collection thread finishes within about a second, one best-effort
        poll (capped at ``cancel_final_poll_seconds``) keeps the events up
        to the stop; otherwise the thread is abandoned and anything it
        fetches later is discarded.
        """
        logger.warning(
            "Run %d cancelled (%s); skipping settle wait and comparison",
            run_num, self._stop_reason,
        )
        ctx['run_stop_event'].set()
        thread = ctx.get('collection_thread')
        joined = thread is None or self._join_bounded(
            thread, _CANCEL_COLLECTION_JOIN_SECONDS, stop_aware=False
        )
        if not joined:
            ctx['collection_abort'].set()
            logger.warning(
                "Collection poll for run %d still in progress; abandoning it", run_num
            )
        elif have_data and self.config.cancel_final_poll_seconds > 0:
            self._bounded_final_poll(collector, run_num, self.config.cancel_final_poll_seconds)
        self._safe_db_call('mark_run_cancelled', run_num, device_ids=self._device_ids())
        self._cancelled_run = run_num
        ctx['finished'] = True

    def _bounded_final_poll(self, collector: DataCollector, run_num: int, budget: float) -> None:
        """Run one collection poll on a helper thread; abandon it after ``budget`` seconds."""
        abort = threading.Event()

        def _poll() -> None:
            try:
                collector.collect_once(run_num, self._simulation_start_time, abort_event=abort)
            except Exception:
                logger.warning("Final poll after cancel failed", exc_info=True)

        thread = threading.Thread(target=run_in_log_context(_poll), name=f"final-poll-run{run_num}", daemon=True)
        thread.start()
        if not self._join_bounded(thread, budget, stop_aware=False):
            abort.set()
            logger.warning(
                "Final poll after cancel did not finish within %.1fs; results discarded", budget
            )

    def _abandon_run_after_interrupt(self, run_num: int) -> None:
        """Best-effort cleanup when Ctrl+C escaped from the middle of a run."""
        ctx = self._run_ctx
        if ctx is None or ctx.get('run_num') != run_num or ctx.get('finished'):
            return
        ctx['run_stop_event'].set()
        ctx['collection_abort'].set()
        self._safe_db_call('mark_run_cancelled', run_num, device_ids=self._device_ids())
        self._cancelled_run = run_num
        ctx['finished'] = True

    def _reader(self) -> Any:
        """Read-only DatabaseManager for post-run reads (falls back to self.db)."""
        try:
            return DatabaseManager(self.config.db_path, read_only=True)
        except TypeError:
            return self.db

    def _run_comparison(self) -> None:
        """Run DTW comparison analysis on all collected data with threshold checks."""
        device_ids = [sig.device_id for sig in self.config.signals]
        
        # Retry with delay to handle file-lock release lag on network shares
        for attempt in range(5):
            try:
                self._comparison_results = compare_all_runs(
                    self._reader(),
                    device_ids,
                    self._completed_runs,
                    include_input_comparison=True
                )
                break
            except Exception as e:
                if attempt < 4 and "being used by another process" in str(e):
                    wait = 3 * (attempt + 1)
                    logger.info(
                        "Database locked, retrying in %ds... (attempt %d/5)", wait, attempt + 1
                    )
                    if not self._wait_stoppable(wait) and self._is_cancelled():
                        logger.warning("Comparison abandoned: simulation cancelled")
                        return
                else:
                    raise
        
        # Check thresholds and generate plots for each comparison
        self._check_thresholds_and_plot()
    
    def _check_thresholds_and_plot(self) -> None:
        """Check comparison thresholds and generate plots when exceeded."""
        if not self._comparison_results:
            return
        
        for device_id, comparisons in self._comparison_results.items():
            for result in comparisons:
                # Add threshold info to result
                result.thresholds = self.comparison_thresholds
                exceeded, reason = self.comparison_thresholds.exceeds_threshold(
                    result.sequence_dtw.normalized_distance,
                    result.timing_dtw.normalized_distance,
                    result.match_percentage
                )
                result.exceeds_threshold = exceeded
                result.threshold_reason = reason
                
                # Generate plot if threshold exceeded and output_dir configured
                if exceeded and self.output_dir:
                    self._generate_comparison_plot(device_id, result)
                elif exceeded:
                    logger.warning(
                        "WARNING: Threshold exceeded for %s (%s vs %s): %s. "
                        "Set output_dir to generate comparison plots.",
                        device_id, result.run_a, result.run_b, reason,
                    )

                # Store result in database (after the plot so plot_path is set)
                try:
                    store_comparison_result(self.config.db_path, result, run_uuid=self.run_uuid)
                except Exception:
                    logger.warning("Failed to store comparison result", exc_info=True)
    
    def _generate_comparison_plot(self, device_id: str, result: ComparisonResult) -> None:
        """Generate Gantt chart for a comparison that exceeded thresholds."""
        try:
            # Get events for both runs
            reader = self._reader()
            if result.run_a == "input":
                events_a = reader.get_input_events(device_id=device_id)
            else:
                events_a = reader.get_events(device_id=device_id, run_number=int(result.run_a))

            events_b = reader.get_events(device_id=device_id, run_number=int(result.run_b))
            
            if events_a.empty or events_b.empty:
                logger.log(debug_level(self.debug), "No events to plot for %s", device_id)
                return
            
            # Find divergence time
            divergence_start = None
            divergence_end = None
            
            if result.divergence_windows:
                df_a_prep = prepare_events_for_comparison(events_a)
                if not df_a_prep.empty:
                    first_div = result.divergence_windows[0]
                    if first_div.start_index_a < len(df_a_prep):
                        divergence_start = df_a_prep.iloc[first_div.start_index_a]['timestamp']
                    if first_div.end_index_a < len(df_a_prep):
                        divergence_end = df_a_prep.iloc[first_div.end_index_a]['timestamp']
            
            # Generate timelines
            timeline_a = generate_timeline(events_a, device_id=device_id)
            timeline_b = generate_timeline(events_b, device_id=device_id)
            
            # Create output filename
            label_a = str(result.run_a)
            label_b = str(result.run_b)
            output_name = f"{device_id}_{label_a}_vs_{label_b}".replace(' ', '_')
            self.output_dir.mkdir(parents=True, exist_ok=True)
            output_path = self.output_dir / f"{output_name}.png"
            
            # Create Gantt chart
            from .comparison import create_comparison_gantt_matplotlib

            create_comparison_gantt_matplotlib(
                timeline_a=timeline_a,
                timeline_b=timeline_b,
                label_a=f"Run {label_a}" if label_a != "input" else "Input",
                label_b=f"Run {label_b}",
                title=f"Device {device_id}: {label_a} vs {label_b}",
                divergence_start=divergence_start,
                divergence_end=divergence_end,
                output_path=output_path,
                window_minutes=5.0
            )

            result.plot_path = str(output_path)

            logger.log(debug_level(self.debug), "Generated comparison plot: %s", output_path)
            self._progress.emit(
                Stage.PLOT,
                f"[{device_id}] Comparison plot written: {output_path}",
                device_id=device_id,
                log=False,
                extra={"path": str(output_path), "run_a": str(result.run_a), "run_b": str(result.run_b)},
            )

        except Exception as exc:
            logger.warning("Failed to generate plot for %s", device_id, exc_info=True)
            self._progress.emit(
                Stage.PLOT,
                f"[{device_id}] Comparison plot failed: {exc}",
                level=logging.WARNING,
                device_id=device_id,
                log=False,
                extra={"error": str(exc), "run_a": str(result.run_a), "run_b": str(result.run_b)},
            )
    
    def format_summary(self) -> str:
        """Return the final simulation summary as plain ASCII text."""
        header = "SIMULATION CANCELLED" if self._cancel_requested else "SIMULATION COMPLETE"
        lines = ["", "=" * 60, header, "=" * 60, ""]
        lines.append(f"Completed Runs: {len(self._completed_runs)}")
        if self._conflict_records:
            first_run = min(int(c.run_number) for c in self._conflict_records)
            lines.append(f"Failure Replicated: yes (first conflict in run {first_run})")
        if self._conflict_store_errors:
            lines.append(
                f"WARNING: {len(self._conflict_store_errors)} conflict(s) could not be written "
                "to the working database (they are in the returned result)"
            )
        if self._incomplete_runs:
            lines.append(
                "WARNING: Output events incomplete for run(s): "
                + ", ".join(str(r) for r in self._incomplete_runs)
                + " (conflict check used the events received)"
            )
        if self._collection_health_by_run:
            last = self._collection_health_by_run[max(self._collection_health_by_run)]
            degraded = sorted(d for d, h in last.items() if h.get('degraded'))
            if degraded:
                lines.append("WARNING: Output collection degraded for: " + ", ".join(degraded))
        if self._stop_reason is not None:
            lines.append(f"Stop Reason: {self._stop_reason}")
        if self._cancelled_run is not None:
            lines.append(f"Cancelled Run: {self._cancelled_run} (recorded as cancelled)")
        not_reset = sorted(d for d, ok in self._detectors_reset.items() if not ok)
        if not_reset:
            lines.append(
                "WARNING: Detector reset not confirmed for: "
                + ", ".join(not_reset)
                + " (check the controller for inputs left ON)"
            )
        lines.append(f"Conflicts Found: {len(self._conflicts_found)}")
        if self._failed_signals_by_run:
            lines.append("Signal Failures by Run:")
            for run_num in sorted(self._failed_signals_by_run):
                failed = ", ".join(self._failed_signals_by_run[run_num])
                lines.append(f"  Run {run_num}: {failed}")

        if self._conflicts_found:
            lines.append("")
            lines.append("Conflicts:")
            for conflict in self._conflicts_found:
                line = (
                    f"  [{conflict['device_id']}] Run {conflict['run_number']}: "
                    f"{conflict['conflict_details']} at {conflict['timestamp']}"
                )
                if conflict.get('source_equivalent_timestamp') is not None:
                    line += f" (source log time {conflict['source_equivalent_timestamp']})"
                lines.append(line)

        if self._comparison_results:
            lines.append("")
            lines.append(self.get_comparison_summary())

            alerts = []
            for device_id, comparisons in self._comparison_results.items():
                for result in comparisons:
                    if hasattr(result, 'exceeds_threshold') and result.exceeds_threshold:
                        alerts.append((device_id, result))

            if alerts:
                lines.append("")
                lines.append("WARNING: THRESHOLD ALERTS:")
                for device_id, result in alerts:
                    lines.append(
                        f"  [{device_id}] {result.run_a} vs {result.run_b}: "
                        f"{result.threshold_reason}"
                    )
                    if result.plot_path:
                        lines.append(f"      Plot: {result.plot_path}")
        return "\n".join(lines)

    def _print_summary(self) -> None:
        """Log the final simulation summary at INFO level."""
        logger.info("%s", self.format_summary())

    def get_events(
        self,
        device_id: Optional[str] = None,
        run_number: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get collected events from the database.
        
        Args:
            device_id: Optional filter by device
            run_number: Optional filter by run number
        
        Returns:
            DataFrame of events
        """
        return self.db.get_events(device_id=device_id, run_number=run_number)
    
    def get_conflicts(
        self,
        device_id: Optional[str] = None,
        run_number: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get detected conflicts from the database.
        
        Args:
            device_id: Optional filter by device
            run_number: Optional filter by run number
        
        Returns:
            DataFrame of conflicts
        """
        return self.db.get_conflicts(device_id=device_id, run_number=run_number)
    
    def get_comparison_results(self) -> Optional[Dict[str, List[ComparisonResult]]]:
        """Get the raw comparison results."""
        return self._comparison_results
    
    def get_comparison_summary(self) -> str:
        """Get formatted comparison summary."""
        if not self._comparison_results:
            return "No comparison results available."
        return format_comparison_summary(self._comparison_results)
    
    def get_input_events(self, device_id: Optional[str] = None) -> pd.DataFrame:
        """Get stored input events for comparison."""
        return self.db.get_input_events(device_id=device_id)
