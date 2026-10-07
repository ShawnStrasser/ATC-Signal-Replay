"""
Replay module for generating activation feeds and sending SNMP commands.
"""

import duckdb
import pandas as pd
import asyncio
import logging
import threading
import time
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union, Optional, Tuple, List, Dict, Any
from importlib import resources
from jinja2 import Template

import pyarrow as pa

from ._logging import debug_level, run_in_log_context
from ._threads import TrackedThread
from .progress import ProgressCallback, Stage, as_reporter
from .ntcip import async_send_ntcip, async_reset_all_detectors
from .config import SignalConfig
from pysnmp.hlapi.v3arch.asyncio import SnmpEngine

logger = logging.getLogger(__name__)

# Longest sleep between stop-flag checks while the replay waits for its next event.
_STOP_POLL_SECONDS = 0.25
# How often a send in progress checks whether a stop was requested.
_STOP_WATCH_SECONDS = 0.1
# Detector types in the order they are reset; preempt inputs are cleared first.
_RESET_ORDER = {'Preempt': 0, 'Vehicle': 1, 'Ped': 2}


async def reset_detector_groups(
    ip_port: Tuple[str, int],
    keys: List[Tuple[str, int]],
    *,
    snmp_engine: SnmpEngine,
    timeout: float = 2.0,
    budget: float = 5.0,
    community: str = "public",
) -> bool:
    """Send state 0 to each (detector_type, group) in ``keys``, in order.

    Every key is tried even if an earlier one fails. The whole reset is
    capped at ``budget`` seconds. Returns True only when every SET succeeded.
    """
    all_ok = True

    async def _send_all() -> None:
        nonlocal all_ok
        for detector_type, group_number in keys:
            try:
                await async_send_ntcip(
                    ip_port, group_number, 0, detector_type, community,
                    timeout=timeout, snmp_engine=snmp_engine,
                )
            except Exception as exc:
                all_ok = False
                logger.warning(
                    "Reset of %s group %s at %s failed: %s",
                    detector_type, group_number, ip_port, exc,
                )

    try:
        await asyncio.wait_for(_send_all(), timeout=max(0.0, budget))
    except asyncio.TimeoutError:
        logger.warning("Detector reset at %s timed out after %.1fs", ip_port, budget)
        return False
    return all_ok


def _get_sql_template(filename: str) -> str:
    """Load a SQL template from the package's sql directory."""
    sql_dir = Path(__file__).parent / "sql"
    with open(sql_dir / filename, 'r') as f:
        return f.read()


class SignalReplay:
    """
    Handles replay of hi-res events for a single signal.
    
    Generates activation feeds from input events and sends SNMP commands
    to the controller at the appropriate times.
    """
    
    def __init__(
        self,
        config: SignalConfig,
        simulation_speed: float = 1.0,
        limit_minutes: Optional[float] = None,
        buffer_minutes: Optional[float] = None,
        snmp_timeout_seconds: float = 2.0,
        snmp_send_retries: int = 0,
        snmp_retry_backoff_seconds: float = 0.25,
        show_progress_logs: bool = False,
        progress_log_interval_seconds: float = 60.0,
        stop_event: Optional[threading.Event] = None,
        latency_offset_provider: Optional[Any] = None,
        detector_reset_timeout_seconds: float = 5.0,
        debug: bool = False,
        on_progress: Optional[ProgressCallback] = None,
    ):
        """
        Initialize the SignalReplay.
        
        Args:
            config: SignalConfig with device settings and events
            simulation_speed: Speed multiplier for playback (1.0 = real-time)
            limit_minutes: Limit input events to the last N minutes (0 = no limit)
            buffer_minutes: Include buffer minutes before the last N minutes (0 = no buffer)
            snmp_timeout_seconds: SNMP response timeout in seconds for replay sends
            snmp_send_retries: Additional replay send attempts after the first try
            snmp_retry_backoff_seconds: Delay between replay retry attempts
            show_progress_logs: If True, print periodic "Sent x/y events" updates
            progress_log_interval_seconds: Seconds between periodic progress updates
            stop_event: Optional shared event; when set, the replay stops within
                about 0.25 s, drops queued sends and cancels the send in progress
            latency_offset_provider: Optional live latency offset source (TOD mode)
            detector_reset_timeout_seconds: Time budget for the detector reset that
                always runs when the replay ends (completed, stopped or failed)
            debug: Enable debug output
            on_progress: Optional callback receiving
                :class:`~signal_replay.progress.ProgressEvent` objects
                (DETECTOR_RESET, WAITING, REPLAY). It runs on the replay
                thread; REPLAY events are throttled to about one per second.
        """
        self.config = config
        self._progress = as_reporter(on_progress, log=logger)
        self.device_id = config.device_id
        self.ip_port = config.ip_port
        self.snmp_community = getattr(config, "snmp_community", None) or "public"
        self.cycle_length = config.cycle_length
        self.cycle_offset = config.cycle_offset
        self.tod_align = config.tod_align
        self.replay_latency_offset_seconds = config.replay_latency_offset_seconds
        self.incompatible_pairs = config.incompatible_pairs
        self.simulation_speed = simulation_speed
        self.limit_minutes = config.limit_minutes if limit_minutes is None else limit_minutes
        self.buffer_minutes = config.buffer_minutes if buffer_minutes is None else buffer_minutes
        self.snmp_timeout_seconds = snmp_timeout_seconds
        self.snmp_send_retries = snmp_send_retries
        self.snmp_retry_backoff_seconds = snmp_retry_backoff_seconds
        self.show_progress_logs = show_progress_logs
        self.progress_log_interval_seconds = progress_log_interval_seconds
        self.debug = debug
        self._stop_event = stop_event or threading.Event()
        self.latency_offset_provider = latency_offset_provider
        
        self.input_data: Optional[pd.DataFrame] = None
        self.activation_feed: Optional[pd.DataFrame] = None
        self.original_start_time: Optional[datetime] = None
        self.simulation_start_time: Optional[datetime] = None
        self.input_window_start: Optional[datetime] = None
        self.input_window_end: Optional[datetime] = None
        self.input_buffer_start: Optional[datetime] = None
        self._send_queue: Optional[asyncio.Queue] = None
        self._send_worker_task: Optional[asyncio.Task] = None
        self._final_send_failures: Dict[Tuple[str, int], int] = {}
        self._first_send_logged = False
        self.detector_reset_timeout_seconds = detector_reset_timeout_seconds
        # (detector_type, group) pairs this replay has queued a state for.
        self.touched_keys: set = set()
        # None until the end-of-replay reset has run; then True/False.
        self.detectors_reset: Optional[bool] = None
        #: How this replay mapped source time onto wall-clock time; filled
        #: while it runs. Keys: ``mode`` ('tod' or 'relative'), ``speed``,
        #: ``replay_start``/``replay_end`` (PC time sending started/ended),
        #: ``source_start``/``source_end`` (source timestamps of the first
        #: and last event sent), ``date_shift_seconds`` (replay time minus
        #: source time at speed 1), ``events_sent``, ``events_total``.
        self.replay_info: Dict[str, Any] = {
            "mode": "tod" if self.tod_align else "relative",
            "speed": float(simulation_speed),
            "replay_start": None,
            "replay_end": None,
            "source_start": None,
            "source_end": None,
            "date_shift_seconds": None,
            "events_sent": 0,
            "events_total": None,
        }
        
        # Load and process events
        self._load_events()
        self._generate_activation_feed()

    def release_cached_data(self, keep_activation_feed: bool = True) -> None:
        """Drop replay DataFrames that are no longer needed."""
        self.input_data = None
        if not keep_activation_feed:
            self.activation_feed = None

    def request_stop(self) -> None:
        """Request cooperative replay shutdown."""
        self._stop_event.set()

    async def _sleep_interruptibly(self, delay: float) -> bool:
        """Sleep in short chunks so stop requests can interrupt waits quickly.

        The wait runs to an absolute deadline on the loop clock, so the small
        oversleep of each chunk (about 15 ms on Windows) does not add up over
        a long gap between events.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, delay)
        while True:
            if self._stop_event.is_set():
                return False
            remaining = deadline - loop.time()
            if remaining <= 0:
                return True
            await asyncio.sleep(min(remaining, _STOP_POLL_SECONDS))

    async def _wait_for_stop(self) -> None:
        """Return once the stop event is set (polled, so it never blocks the loop)."""
        while not self._stop_event.is_set():
            await asyncio.sleep(_STOP_WATCH_SECONDS)

    async def _run_unless_stopped(self, coro) -> Tuple[bool, Any]:
        """Run ``coro`` but cancel it if a stop is requested first.

        Returns ``(True, result)`` when the coroutine finished, or
        ``(False, None)`` when the stop won and the coroutine was cancelled.
        Exceptions raised by the coroutine propagate.
        """
        task = asyncio.ensure_future(coro)
        watcher = asyncio.ensure_future(self._wait_for_stop())
        try:
            await asyncio.wait({task, watcher}, return_when=asyncio.FIRST_COMPLETED)
        except BaseException:
            task.cancel()
            raise
        finally:
            watcher.cancel()
        if task.done():
            return True, task.result()
        task.cancel()
        await asyncio.wait({task})
        if not task.cancelled():
            task.exception()  # mark retrieved; the stop outcome wins
        return False, None

    @staticmethod
    def _command_key(group_number: int, detector_type: str) -> Tuple[str, int]:
        """Return the per-controller state key for a detector group/type pair."""
        return (detector_type, int(group_number))
    
    def _load_events(self) -> None:
        """Load events from the configured source."""
        events = self.config.events
        
        if isinstance(events, pd.DataFrame):
            self._load_from_dataframe(events)
        elif isinstance(events, pa.Table):
            self._load_from_arrow(events)
        elif isinstance(events, (str, Path)):
            self._load_from_path(str(events))
        else:
            raise ValueError(f"Unsupported events type: {type(events)}")
        
        # Add device_id if not present
        if 'DeviceId' not in self.input_data.columns:
            self.input_data['DeviceId'] = self.device_id

        # Static mode shifts timestamps once up front. Adaptive TOD mode keeps
        # source timestamps intact and subtracts the live offset while scheduling.
        if self.latency_offset_provider is None:
            self._apply_replay_latency_offset()

        # Apply time-window slicing if specified
        self._apply_time_window()
        
        logger.log(
            debug_level(self.debug), "[%s] Loaded %d events", self.device_id, len(self.input_data)
        )

    def _apply_replay_latency_offset(self) -> None:
        """Advance detector event timestamps by the configured latency compensation."""
        if (
            self.input_data is None
            or self.input_data.empty
            or self.replay_latency_offset_seconds <= 0
        ):
            return

        if not pd.api.types.is_datetime64_any_dtype(self.input_data['TimeStamp']):
            self.input_data['TimeStamp'] = pd.to_datetime(self.input_data['TimeStamp'])

        self.input_data['TimeStamp'] = (
            self.input_data['TimeStamp']
            - pd.to_timedelta(self.replay_latency_offset_seconds, unit='s')
        )

        logger.log(
            debug_level(self.debug),
            "[%s] Advanced replay timestamps by %.1f ms",
            self.device_id, self.replay_latency_offset_seconds * 1000,
        )
    
    def _load_from_dataframe(self, df: pd.DataFrame) -> None:
        """Load events from a pandas DataFrame."""
        # Expected columns: timestamp, event_id, parameter (or variations)
        df = df.copy()
        
        # Normalize column names
        col_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ('timestamp', 'time_stamp', 'time'):
                col_map[col] = 'TimeStamp'
            elif col_lower in ('event_id', 'eventid', 'event_type_id', 'eventtypeid'):
                col_map[col] = 'EventId'
            elif col_lower in ('parameter', 'param', 'detector'):
                col_map[col] = 'Detector'
            elif col_lower in ('device_id', 'deviceid'):
                col_map[col] = 'DeviceId'
        
        df = df.rename(columns=col_map)
        
        # Filter to detector events only
        detector_events = [81, 82, 89, 90, 102, 104]
        df = df[df['EventId'].isin(detector_events)].copy()
        
        # Add DetectorType
        df['DetectorType'] = df['EventId'].apply(lambda x: 
            'Vehicle' if x in (81, 82) else 
            'Ped' if x in (89, 90) else 
            'Preempt'
        )
        
        # Filter out dummy detectors
        df = df[df['Detector'] < 65]
        
        # Ensure timestamp is datetime
        if not pd.api.types.is_datetime64_any_dtype(df['TimeStamp']):
            df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
        
        # Keep only the expected columns to avoid duplicate/extra column issues
        expected_cols = ['TimeStamp', 'DeviceId', 'EventId', 'Detector', 'DetectorType']
        df = df[expected_cols]
        
        self.input_data = df.sort_values('TimeStamp').reset_index(drop=True)
    
    def _load_from_arrow(self, table) -> None:
        """Load events from an Arrow Table."""
        df = table.to_pandas()
        self._load_from_dataframe(df)
    
    def _load_from_path(self, path: str) -> None:
        """Load events from a file path."""
        logger.log(debug_level(self.debug), "[%s] Loading data from %s", self.device_id, path)
        
        # Check if it's a SQLite database
        suffix = Path(path).suffix.lower()
        if suffix in ('.db', '.sqlite', '.sqlite3'):
            self._load_from_sqlite(path)
        elif suffix == '.parquet':
            self._load_from_dataframe(pd.read_parquet(path))
        elif suffix == '.csv':
            self._load_from_dataframe(pd.read_csv(path))
        else:
            # Use SQL template for other file types
            template_vars = {
                'timestamp': 'timestamp',
                'eventid': 'event_id',
                'parameter': 'parameter',
                'from_path': path
            }
            template = Template(_get_sql_template('load_from_path.sql'))
            sql = template.render(**template_vars)
            
            con = duckdb.connect()
            try:
                self.input_data = con.execute(sql).df()
            finally:
                con.close()
    
    def _load_from_sqlite(self, db_path: str) -> None:
        """Load events from a MAXTIME SQLite database."""
        con = duckdb.connect()
        try:
            con.execute(f"ATTACH DATABASE '{db_path}' AS LastFail (TYPE SQLITE)")
            con.execute("USE LastFail")

            sql = _get_sql_template('load_maxtime_db.sql')
            self.input_data = con.execute(sql).df()
        finally:
            con.close()

    def _apply_time_window(self) -> None:
        """Slice input data to the last N minutes with optional buffer."""
        if self.input_data is None or self.input_data.empty:
            return

        # Ensure timestamp is datetime
        if not pd.api.types.is_datetime64_any_dtype(self.input_data['TimeStamp']):
            self.input_data['TimeStamp'] = pd.to_datetime(self.input_data['TimeStamp'])

        # Default window is full range
        end_time = self.input_data['TimeStamp'].max()
        start_time = self.input_data['TimeStamp'].min()
        buffer_start = start_time

        if self.limit_minutes and self.limit_minutes > 0:
            start_time = end_time - timedelta(minutes=self.limit_minutes)
            buffer_start = start_time - timedelta(minutes=self.buffer_minutes)

        self.input_window_start = start_time
        self.input_window_end = end_time
        self.input_buffer_start = buffer_start

        if self.limit_minutes and self.limit_minutes > 0:
            self.input_data = (
                self.input_data[self.input_data['TimeStamp'] >= buffer_start]
                .sort_values('TimeStamp')
                .reset_index(drop=True)
            )
    
    def _generate_activation_feed(self) -> None:
        """Generate the activation feed from input data."""
        logger.log(debug_level(self.debug), "[%s] Generating activation feed", self.device_id)

        con = duckdb.connect()
        try:
            con.register('raw_data', self.input_data)

            # Impute missing actuations
            sql_impute = _get_sql_template('impute_actuations.sql')
            imputed = con.execute(sql_impute).df()
            con.register('imputed', imputed)

            # Generate activation feed
            sql_feed = _get_sql_template('generate_activation_feed.sql')
            self.activation_feed = con.execute(sql_feed).df()
        finally:
            con.close()

        if self.activation_feed is None or self.activation_feed.empty:
            raise ValueError(
                f"Scenario {self.device_id} has no replayable detector events - check the log file"
            )
        
        # Add cumulative sleep time (adjusted for simulation speed)
        self.activation_feed['sleep_time_cumulative'] = (
            self.activation_feed['sleep_time'].cumsum() / self.simulation_speed
        )
        
        # Store original start time for cycle sync
        self.original_start_time = self.activation_feed['TimeStamp'].min()
        
        logger.log(
            debug_level(self.debug),
            "[%s] Generated %d activation commands",
            self.device_id, len(self.activation_feed),
        )

        self.input_data = None
    
    def get_run_duration(self) -> float:
        """Get the total duration of the simulation in seconds."""
        if self.activation_feed is None:
            return 0.0
        return self.activation_feed['sleep_time_cumulative'].max()
    
    def get_source_comparison_events(self) -> pd.DataFrame:
        """Load phase/overlap events from original source for comparison.
        
        This loads the same source data but filters to phase/overlap events
        (COMPARISON_EVENT_IDS) instead of detector actuations. This allows
        meaningful comparison between source and replay outputs.
        
        Returns:
            DataFrame with timestamp, event_id, parameter columns containing
            phase/overlap events from the original source data.
        """
        from .comparison import COMPARISON_EVENT_IDS
        
        events = self.config.events
        comparison_df = None
        
        # Load based on event source type
        if isinstance(events, pd.DataFrame):
            comparison_df = self._load_comparison_from_dataframe(events)
        elif isinstance(events, pa.Table):
            comparison_df = self._load_comparison_from_dataframe(events.to_pandas())
        elif isinstance(events, (str, Path)):
            comparison_df = self._load_comparison_from_path(str(events))
        else:
            raise ValueError(f"Unsupported events type: {type(events)}")
        
        if comparison_df is None or comparison_df.empty:
            return pd.DataFrame(columns=['timestamp', 'event_id', 'parameter'])
        
        # Filter to COMPARISON_EVENT_IDS
        comparison_df = comparison_df[
            comparison_df['event_id'].isin(COMPARISON_EVENT_IDS)
        ].copy()
        
        # Apply same time window as input data
        if self.input_buffer_start is not None:
            comparison_df = comparison_df[
                comparison_df['timestamp'] >= self.input_buffer_start
            ].copy()
        
        return comparison_df.sort_values('timestamp').reset_index(drop=True)

    def get_source_detector_events(self, event_ids: Optional[List[int]] = None) -> pd.DataFrame:
        """Load source detector input events for adaptive latency calibration."""
        if event_ids is None:
            event_ids = [82]

        events = self.config.events
        if isinstance(events, pd.DataFrame):
            detector_df = self._load_comparison_from_dataframe(events)
        elif isinstance(events, pa.Table):
            detector_df = self._load_comparison_from_dataframe(events.to_pandas())
        elif isinstance(events, (str, Path)):
            detector_df = self._load_comparison_from_path(str(events))
        else:
            raise ValueError(f"Unsupported events type: {type(events)}")

        if detector_df.empty:
            return pd.DataFrame(columns=['timestamp', 'event_id', 'parameter'])

        detector_df = detector_df[detector_df['event_id'].isin(event_ids)].copy()
        detector_df["parameter"] = pd.to_numeric(detector_df["parameter"], errors="coerce")
        detector_df = detector_df[detector_df["parameter"] < 65].copy()

        if self.input_buffer_start is not None:
            detector_df = detector_df[
                detector_df['timestamp'] >= self.input_buffer_start
            ].copy()

        return detector_df.sort_values('timestamp').reset_index(drop=True)
    
    def _load_comparison_from_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Load comparison events from a DataFrame."""
        df = df.copy()
        
        # Normalize column names
        col_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ('timestamp', 'time_stamp'):
                col_map[col] = 'timestamp'
            elif col_lower in ('event_id', 'eventid', 'eventtypeid'):
                col_map[col] = 'event_id'
            elif col_lower in ('parameter', 'param', 'detector'):
                col_map[col] = 'parameter'
        
        df = df.rename(columns=col_map)
        
        # Ensure we have the required columns
        if 'timestamp' not in df.columns or 'event_id' not in df.columns:
            raise ValueError("DataFrame must have timestamp and event_id columns")
        
        if 'parameter' not in df.columns:
            df['parameter'] = 0
        
        # Ensure timestamp is datetime
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        return df[['timestamp', 'event_id', 'parameter']]
    
    def _load_comparison_from_path(self, path: str) -> pd.DataFrame:
        """Load comparison events from a file path."""
        suffix = Path(path).suffix.lower()
        
        if suffix in ('.db', '.sqlite', '.sqlite3'):
            # Load from SQLite database
            con = duckdb.connect()
            try:
                con.execute(f"ATTACH DATABASE '{path}' AS SourceDB (TYPE SQLITE)")
                con.execute("USE SourceDB")

                # Query all events (not just detector events)
                sql = """
                    SELECT
                        TO_TIMESTAMP(Timestamp + (Tick / 10))::TIMESTAMP AS timestamp,
                        EventTypeID AS event_id,
                        Parameter AS parameter
                    FROM Event
                    ORDER BY timestamp
                """
                df = con.execute(sql).df()
            finally:
                con.close()
            return df
        if suffix == '.parquet':
            return self._load_comparison_from_dataframe(pd.read_parquet(path))
        if suffix == '.csv':
            return self._load_comparison_from_dataframe(pd.read_csv(path))

        raise ValueError(f"Unsupported file type: {suffix}. Use .csv, .parquet, or .db")
    
    async def _send_state_with_retries(
        self,
        key: Tuple[str, int],
        state_integer: int,
        snmp_engine: SnmpEngine,
    ) -> bool:
        """Send the latest desired state, retrying a few times on failure."""
        detector_type, group_number = key
        attempts = self.snmp_send_retries + 1

        for attempt in range(1, attempts + 1):
            if self._stop_event.is_set():
                return False
            try:
                # Race the send against the stop flag so a silent controller
                # cannot hold up a stop for a full SNMP timeout.
                finished, _ = await self._run_unless_stopped(async_send_ntcip(
                    self.ip_port,
                    group_number,
                    state_integer,
                    detector_type,
                    self.snmp_community,
                    timeout=self.snmp_timeout_seconds,
                    snmp_engine=snmp_engine,
                ))
                if not finished:
                    return False
                self._final_send_failures[key] = 0
                return True
            except Exception as exc:
                is_final_attempt = attempt >= attempts
                if is_final_attempt:
                    self._final_send_failures[key] = self._final_send_failures.get(key, 0) + 1
                    logger.warning(
                        "[%s] SNMP send failed for group %s type %s state %s after "
                        "%s attempt(s): %s",
                        self.device_id, group_number, detector_type, state_integer,
                        attempts, exc,
                    )
                    return False

                logger.log(
                    debug_level(self.debug),
                    "[%s] Retrying group %s type %s state %s, attempt %d/%d: %s",
                    self.device_id, group_number, detector_type, state_integer,
                    attempt + 1, attempts, exc,
                )

                if self.snmp_retry_backoff_seconds > 0:
                    if not await self._sleep_interruptibly(self.snmp_retry_backoff_seconds):
                        return False

        return False

    async def _ensure_send_worker(self, snmp_engine: SnmpEngine) -> None:
        """Start a per-device worker that drains queued SNMP sends sequentially."""
        if self._send_queue is None:
            self._send_queue = asyncio.Queue()

        if self._send_worker_task is not None and not self._send_worker_task.done():
            return

        async def _worker() -> None:
            assert self._send_queue is not None

            while True:
                item = await self._send_queue.get()
                if item is None:
                    self._send_queue.task_done()
                    return

                key, state_integer = item
                try:
                    # After a stop, queued commands are dropped; the end-of-replay
                    # reset puts every touched group back to 0 instead.
                    if not self._stop_event.is_set():
                        await self._send_state_with_retries(key, state_integer, snmp_engine)
                finally:
                    self._send_queue.task_done()

        self._send_worker_task = asyncio.create_task(_worker())

    async def _enqueue_state_send(
        self,
        key: Tuple[str, int],
        state_integer: int,
        snmp_engine: SnmpEngine,
    ) -> None:
        """Queue a state send on the per-device worker so every command is preserved."""
        await self._ensure_send_worker(snmp_engine)

        assert self._send_queue is not None
        self.touched_keys.add(key)
        await self._send_queue.put((key, state_integer))

    async def _send_command(self, row, snmp_engine: SnmpEngine) -> None:
        """Queue a single SNMP command asynchronously on the per-device send worker."""
        if self._stop_event.is_set():
            return

        key = self._command_key(row.group_number, row.DetectorType)
        state_integer = int(row.state_integer)

        if not self._first_send_logged and self.debug:
            self._first_send_logged = True
            logger.info(
                "[%s] First command dispatched at %s for target %s group %s type %s state %s",
                self.device_id,
                f"{datetime.now():%H:%M:%S}",
                f"{pd.to_datetime(row.TimeStamp):%H:%M:%S}",
                row.group_number, row.DetectorType, state_integer,
            )

        await self._enqueue_state_send(key, state_integer, snmp_engine)

    def _note_sent(self, row, sent_count: int) -> None:
        """Record the source timestamp of a sent event in :attr:`replay_info`."""
        info = self.replay_info
        info["events_sent"] = int(sent_count)
        try:
            source_ts = pd.Timestamp(row.TimeStamp).to_pydatetime()
        except (AttributeError, TypeError, ValueError):
            return
        if info["source_start"] is None:
            info["source_start"] = source_ts
        info["source_end"] = source_ts

    def source_time_for(self, timestamp: datetime) -> Optional[datetime]:
        """Map a wall-clock (controller) timestamp of this replay back to the source log's clock.

        TOD replays subtract the whole-day date shift; relative replays
        subtract the offset between the replay start and the first source
        event (scaled by the simulation speed). None before the replay ran.
        """
        return source_time_from_info(self.replay_info, timestamp)

    def _report_sent(self, sent: int, total: int, *, final: bool = False, message: str = "") -> None:
        """REPLAY progress for the callback/status (throttled unless ``final``)."""
        self._progress.emit(
            Stage.REPLAY,
            message,
            device_id=self.device_id,
            events_sent=int(sent),
            events_total=int(total),
            throttle_key=("replay", self.device_id),
            force=final,
            log=False,
            extra={"complete": bool(final and sent >= total), "stopped": self._stop_event.is_set()},
        )

    async def _wait_with_countdown(self, delay: float, reason: str, message: str) -> bool:
        """Sleep ``delay`` seconds interruptibly, reporting WAITING with a countdown.

        Returns False when a stop was requested.
        """
        deadline = time.monotonic() + max(0.0, delay)
        force = True
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return not self._stop_event.is_set()
            self._progress.emit(
                Stage.WAITING,
                message,
                device_id=self.device_id,
                seconds_until_start=remaining,
                throttle_key=("waiting", self.device_id),
                force=force,
                log=False,
                extra={"reason": reason},
            )
            force = False
            if not await self._sleep_interruptibly(min(remaining, 1.0)):
                return False

    def _print_send_summary(self) -> None:
        """Emit a concise summary when replay ended with unresolved send failures."""
        failed_keys = [
            (key, count)
            for key, count in self._final_send_failures.items()
            if count > 0
        ]
        if not failed_keys:
            return

        summary = ", ".join(
            f"{detector_type} group {group_number} ({count})"
            for (detector_type, group_number), count in failed_keys
        )
        self._progress.emit(
            Stage.REPLAY,
            f"[{self.device_id}] Replay completed with unresolved SNMP send failures: {summary}",
            level=logging.WARNING,
            device_id=self.device_id,
            extra={"send_failures": {f"{t} {g}": c for (t, g), c in failed_keys}},
            log_to=logger,
        )

    async def _wait_for_pending_sends(self) -> None:
        """Wait until the per-device SNMP send queue has drained."""
        if self._send_queue is None:
            return

        worker = self._send_worker_task
        drained = False
        if worker is not None and not worker.done():
            # Drain normally, but give up as soon as a stop is requested so a
            # backlog of queued commands is never sent after the stop.
            drained, _ = await self._run_unless_stopped(self._send_queue.join())

        if worker is not None and not worker.done():
            if drained:
                await self._send_queue.put(None)
                await worker
            else:
                worker.cancel()
                await asyncio.wait({worker})

        self._send_worker_task = None
        self._send_queue = None

    async def _run_async_inner(self, activation_feed, snmp_engine: SnmpEngine) -> None:
        """Inner replay loop that uses a shared SnmpEngine."""
        if self._stop_event.is_set():
            logger.info("[%s] Stop requested before replay start", self.device_id)
            return

        if self.tod_align:
            if self.simulation_start_time is None:
                self.simulation_start_time = datetime.now()

            # Shift all event timestamps so the earliest event's date maps to
            # today.  Events on subsequent dates (e.g. after midnight) naturally
            # land on tomorrow, the day after, etc.
            min_event_date = pd.to_datetime(activation_feed['TimeStamp'].min()).date()
            date_shift = self.simulation_start_time.date() - min_event_date
            self.replay_info.update(
                replay_start=self.simulation_start_time,
                date_shift_seconds=float(date_shift.total_seconds()),
            )

            replay_start = self.simulation_start_time
            skipped_events = 0
            first_target = None

            # Pre-compute shifted timestamps to avoid slow per-row iteration
            if self.latency_offset_provider is not None:
                self.latency_offset_provider.set_device_date_shift(self.device_id, date_shift)

            base_ts = pd.to_datetime(activation_feed['TimeStamp']) + date_shift
            if self.latency_offset_provider is None:
                shifted_ts = base_ts
            else:
                shifted_ts = base_ts - pd.to_timedelta(
                    self.latency_offset_provider.get_offset(self.device_id),
                    unit='s',
                )
            mask = shifted_ts >= replay_start
            first_valid_idx = mask.idxmax() if mask.any() else None

            if first_valid_idx is not None:
                first_target = shifted_ts.loc[first_valid_idx]
                skipped_events = int((shifted_ts < replay_start).sum())
                logger.info(
                    "[%s] TOD align: date_shift=%dd, skipping %d/%d old events, "
                    "first event at %s",
                    self.device_id, date_shift.days, skipped_events, len(activation_feed),
                    f"{first_target:%H:%M:%S}",
                )
            else:
                logger.warning(
                    "[%s] TOD align: ALL %d events are before %s - nothing to send!",
                    self.device_id, len(activation_feed), f"{replay_start:%H:%M:%S}",
                )
                return

            # Slice to only the events we need (avoids iterating through skipped rows)
            active_feed = activation_feed.loc[first_valid_idx:]
            active_shifted = shifted_ts.loc[first_valid_idx:]
            total_to_send = len(active_feed)
            self.replay_info["events_total"] = int(total_to_send)

            # Show wait time if first event is in the future
            first_delay = (first_target.to_pydatetime() - datetime.now()).total_seconds()
            if first_delay > 5:
                logger.info(
                    "[%s] Waiting %.0fs until first event...", self.device_id, first_delay
                )
            if first_delay > 1.0:
                # Leave the last second to the per-event wait (the live latency
                # offset may move the target slightly).
                if not await self._wait_with_countdown(
                    first_delay - 1.0,
                    "tod_first_event",
                    f"[{self.device_id}] Waiting until first event at {first_target:%H:%M:%S}",
                ):
                    logger.info("[%s] Stop requested while waiting for first event", self.device_id)
                    self._report_sent(0, total_to_send, final=True)
                    return

            sent_count = 0
            self._report_sent(0, total_to_send, final=True)
            last_progress = time.time()
            for idx, row in active_feed.iterrows():
                if self._stop_event.is_set():
                    logger.info(
                        "[%s] Stop requested - halting replay after %d events",
                        self.device_id, sent_count,
                    )
                    break

                if self.latency_offset_provider is None:
                    target_time = active_shifted.loc[idx].to_pydatetime()

                    delay = (target_time - datetime.now()).total_seconds()
                    if delay > 0:
                        if not await self._sleep_interruptibly(delay):
                            logger.info(
                                "[%s] Stop requested while waiting for next event", self.device_id
                            )
                            break
                else:
                    source_target = base_ts.loc[idx]
                    while not self._stop_event.is_set():
                        live_offset = self.latency_offset_provider.get_offset(self.device_id)
                        target_time = (
                            source_target
                            - pd.to_timedelta(live_offset, unit='s')
                        ).round('us').to_pydatetime()
                        delay = (target_time - datetime.now()).total_seconds()
                        if delay <= 0:
                            break
                        await self._sleep_interruptibly(min(delay, 1.0))
                    if self._stop_event.is_set():
                        # Same handling as the static path: fall through to the
                        # drain (which drops the queue) and the detector reset.
                        logger.info(
                            "[%s] Stop requested while waiting for next event", self.device_id
                        )
                        break

                await self._send_command(row, snmp_engine)
                sent_count += 1
                self._note_sent(row, sent_count)
                self._report_sent(sent_count, total_to_send)

                # Print progress every 60 seconds
                now = time.time()
                if self.show_progress_logs and now - last_progress >= self.progress_log_interval_seconds:
                    logger.info(
                        "[%s] Sent %d/%d events", self.device_id, sent_count, total_to_send
                    )
                    last_progress = now

            await self._wait_for_pending_sends()
            self.replay_info["replay_end"] = datetime.now()
            self._print_send_summary()
            self._report_sent(
                sent_count, total_to_send, final=True,
                message=f"[{self.device_id}] Complete - sent {sent_count} events",
            )
            if self.show_progress_logs or self.debug:
                logger.info("[%s] Complete - sent %d events", self.device_id, sent_count)
            return

        loop = asyncio.get_running_loop()
        start_time = loop.time()
        total_events = len(activation_feed)
        sent_count = 0
        replay_start = self.simulation_start_time or datetime.now()
        source_anchor = pd.Timestamp(self.original_start_time) if self.original_start_time is not None else None
        self.replay_info.update(
            replay_start=replay_start,
            events_total=int(total_events),
            date_shift_seconds=(
                float((pd.Timestamp(replay_start) - source_anchor).total_seconds())
                if source_anchor is not None else None
            ),
        )
        last_progress = time.time()
        self._report_sent(0, total_events, final=True)

        for _, row in activation_feed.iterrows():
            if self._stop_event.is_set():
                logger.info(
                    "[%s] Stop requested - halting replay after %d events",
                    self.device_id, sent_count,
                )
                break

            # Calculate delay from start
            current_time = loop.time()
            delay = row.sleep_time_cumulative - (current_time - start_time)

            if delay > 0:
                if not await self._sleep_interruptibly(delay):
                    logger.info(
                        "[%s] Stop requested while waiting for next event", self.device_id
                    )
                    break

            # Send command (non-blocking via executor)
            await self._send_command(row, snmp_engine)
            sent_count += 1
            self._note_sent(row, sent_count)
            self._report_sent(sent_count, total_events)

            # Print progress every 60 seconds
            now = time.time()
            if self.show_progress_logs and now - last_progress >= self.progress_log_interval_seconds:
                logger.info("[%s] Sent %d/%d events", self.device_id, sent_count, total_events)
                last_progress = now

        await self._wait_for_pending_sends()
        self.replay_info["replay_end"] = datetime.now()
        self._print_send_summary()
        self._report_sent(
            sent_count, total_events, final=True,
            message=f"[{self.device_id}] Complete - sent {sent_count} events",
        )
        if self.show_progress_logs or self.debug:
            logger.info("[%s] Complete - sent %d events", self.device_id, sent_count)
    
    def _run_in_thread(self) -> None:
        """Run the async replay in a new event loop in a separate thread."""
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        new_loop.run_until_complete(self._reset_and_run())
        new_loop.close()

    async def _reset_and_run(self) -> None:
        """Create one SnmpEngine, reset detectors, wait for cycle, then replay.

        Every group this replay touched is reset to 0 when it ends, whether
        it completed, was stopped or failed.
        """
        snmp_engine = SnmpEngine()
        try:
            try:
                if self._stop_event.is_set():
                    logger.info("[%s] Stop requested before replay start", self.device_id)
                    return
                self._progress.emit(
                    Stage.DETECTOR_RESET,
                    f"[{self.device_id}] Resetting all detectors before replay",
                    device_id=self.device_id,
                    log=False,
                    extra={"when": "before_replay"},
                )
                await self._run_unless_stopped(async_reset_all_detectors(
                    self.ip_port,
                    community=self.snmp_community,
                    debug=self.debug,
                    timeout=self.snmp_timeout_seconds,
                    snmp_engine=snmp_engine,
                ))
                await self._wait_until_next_cycle()
                self.simulation_start_time = datetime.now()
                await self._run_async_inner(self.activation_feed, snmp_engine)
            finally:
                await self._cancel_send_worker()
                self.detectors_reset = await self._reset_touched_groups(snmp_engine)
                self._progress.emit(
                    Stage.DETECTOR_RESET,
                    f"[{self.device_id}] Detector reset after replay "
                    + ("confirmed" if self.detectors_reset else "NOT confirmed"),
                    level=logging.INFO if self.detectors_reset else logging.WARNING,
                    device_id=self.device_id,
                    log=False,
                    extra={"when": "after_replay", "ok": bool(self.detectors_reset),
                           "groups": len(self.touched_keys)},
                )
        finally:
            snmp_engine.close_dispatcher()

    async def _cancel_send_worker(self) -> None:
        """Stop the send worker if the replay left early (error or cancel)."""
        worker = self._send_worker_task
        if worker is not None and not worker.done():
            worker.cancel()
            await asyncio.wait({worker})
        self._send_worker_task = None
        self._send_queue = None

    @staticmethod
    def _reset_keys_in_order(keys) -> List[Tuple[str, int]]:
        """Return (type, group) keys with preempt first, then vehicle, then ped."""
        return sorted(keys, key=lambda k: (_RESET_ORDER.get(k[0], 99), k[1]))

    async def _reset_touched_groups(self, snmp_engine: SnmpEngine) -> bool:
        """Send state 0 to every group this replay touched, within a time budget.

        Runs even after a stop. Returns True when every reset was acknowledged
        (or nothing was touched) and False on any failure or timeout.
        """
        keys = self._reset_keys_in_order(self.touched_keys)
        if not keys:
            return True
        ok = await reset_detector_groups(
            self.ip_port,
            keys,
            snmp_engine=snmp_engine,
            timeout=self.snmp_timeout_seconds,
            budget=self.detector_reset_timeout_seconds,
            community=self.snmp_community,
        )
        if ok:
            logger.log(
                debug_level(self.debug),
                "[%s] Reset %d detector group(s) to 0", self.device_id, len(keys),
            )
        else:
            logger.warning(
                "[%s] Detector reset after replay did not complete; check the controller "
                "for detector or preempt inputs left ON",
                self.device_id,
            )
        return ok

    def reset_touched_groups_blocking(self, budget: Optional[float] = None) -> bool:
        """Synchronous, time-boxed reset of the touched groups (orchestrator safety net).

        Safe to call from any thread, including one with a running event loop:
        the reset runs on its own short-lived thread and event loop.
        """
        keys = self._reset_keys_in_order(self.touched_keys)
        if not keys:
            return True
        budget = self.detector_reset_timeout_seconds if budget is None else budget
        outcome: Dict[str, bool] = {}

        async def _do() -> bool:
            engine = SnmpEngine()
            try:
                return await reset_detector_groups(
                    self.ip_port, keys, snmp_engine=engine,
                    timeout=self.snmp_timeout_seconds, budget=budget,
                    community=self.snmp_community,
                )
            finally:
                engine.close_dispatcher()

        def _target() -> None:
            try:
                outcome['ok'] = asyncio.run(_do())
            except Exception:
                logger.warning(
                    "[%s] Safety-net detector reset failed", self.device_id, exc_info=True
                )
                outcome['ok'] = False

        thread = TrackedThread(target=run_in_log_context(_target), name=f"reset-{self.device_id}", daemon=True)
        thread.start()
        thread.join(budget + 1.0)
        return bool(outcome.get('ok', False))

    async def _wait_until_next_cycle(self) -> None:
        """Wait until the next cycle boundary for coordinated signals."""
        if self.tod_align:
            return

        if self.cycle_length == 0 or self.original_start_time is None:
            return

        offset = self.cycle_offset or 0.0
        if offset < 0:
            offset = 0.0
        offset = offset % self.cycle_length

        delta_seconds = (datetime.now() - self.original_start_time).total_seconds()
        cycle_pos = delta_seconds % self.cycle_length
        sleep_time = (offset - cycle_pos) % self.cycle_length

        if sleep_time > 0:
            logger.log(
                debug_level(self.debug),
                "[%s] Waiting %.1fs to align with cycle offset %.1fs",
                self.device_id, sleep_time, offset,
            )
            await self._wait_with_countdown(
                sleep_time,
                "cycle_align",
                f"[{self.device_id}] Waiting {sleep_time:.1f}s to align with cycle offset {offset:.1f}s",
            )
    
    def _join_after_interrupt(self, thread: threading.Thread) -> None:
        """Wait (bounded) for a stopped replay thread to finish its reset."""
        budget = float(self.detector_reset_timeout_seconds) + float(self.snmp_timeout_seconds) + 2.0
        deadline = time.monotonic() + budget
        try:
            while thread.is_alive() and time.monotonic() < deadline:
                thread.join(0.2)
        except BaseException:  # a second interrupt stops the wait
            pass
        if thread.is_alive():
            logger.warning(
                "[%s] Replay thread still running %.0fs after the interrupt; detectors may not be reset",
                self.device_id, budget,
            )

    def run(self) -> datetime:
        """
        Run the SNMP command replay.
        
        Returns:
            The simulation start time
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No event loop running, safe to use asyncio.run
            asyncio.run(self._reset_and_run())
        else:
            # Event loop already running, use thread
            thread = TrackedThread(target=run_in_log_context(self._run_in_thread), daemon=True)
            thread.start()
            # Join in short steps so Ctrl+C can still reach the caller on Windows.
            try:
                while thread.is_alive():
                    thread.join(0.5)
            except BaseException:
                # Ctrl+C or a kernel interrupt: stop the replay thread and give
                # it time to reset the detectors it touched before re-raising.
                self.request_stop()
                self._join_after_interrupt(thread)
                raise
        
        return self.simulation_start_time


def source_time_from_info(info: Dict[str, Any], timestamp: datetime) -> Optional[datetime]:
    """Map ``timestamp`` to source time using a :attr:`SignalReplay.replay_info` dict."""
    if not info or timestamp is None:
        return None
    shift = info.get("date_shift_seconds")
    if shift is None:
        return None
    ts = pd.Timestamp(timestamp)
    speed = float(info.get("speed") or 1.0)
    if info.get("mode") == "relative" and speed != 1.0 and info.get("replay_start") is not None:
        replay_start = pd.Timestamp(info["replay_start"])
        source_anchor = replay_start - pd.Timedelta(seconds=shift)
        # Scaling can leave nanoseconds that datetime cannot hold; truncate them.
        return (source_anchor + (ts - replay_start) * speed).floor("us").to_pydatetime()
    return (ts - pd.Timedelta(seconds=float(shift))).to_pydatetime()


def create_replays(
    configs: List[SignalConfig],
    simulation_speed: float = 1.0,
    snmp_timeout_seconds: float = 2.0,
    snmp_send_retries: int = 0,
    snmp_retry_backoff_seconds: float = 0.25,
    show_progress_logs: bool = False,
    progress_log_interval_seconds: float = 60.0,
    stop_event: Optional[threading.Event] = None,
    debug: bool = False,
    on_progress: Optional[ProgressCallback] = None,
) -> List[SignalReplay]:
    """
    Create SignalReplay instances for multiple signals.
    
    Args:
        configs: List of SignalConfig objects
        simulation_speed: Speed multiplier for playback
        snmp_timeout_seconds: SNMP response timeout in seconds
        snmp_send_retries: Additional replay send attempts after the first try
        snmp_retry_backoff_seconds: Delay between replay retry attempts
        show_progress_logs: If True, print periodic "Sent x/y events" updates
        progress_log_interval_seconds: Seconds between periodic progress updates
        stop_event: Optional shared stop event for every replay
        debug: Enable debug output
        on_progress: Optional progress callback shared by every replay
    
    Returns:
        List of SignalReplay instances
    """
    return [
        SignalReplay(
            config,
            simulation_speed=simulation_speed,
            snmp_timeout_seconds=snmp_timeout_seconds,
            snmp_send_retries=snmp_send_retries,
            snmp_retry_backoff_seconds=snmp_retry_backoff_seconds,
            show_progress_logs=show_progress_logs,
            progress_log_interval_seconds=progress_log_interval_seconds,
            stop_event=stop_event,
            debug=debug,
            on_progress=on_progress,
        )
        for config in configs
    ]

