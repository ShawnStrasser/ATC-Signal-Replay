"""
Replay module for generating activation feeds and sending SNMP commands.
"""

import duckdb
import pandas as pd
import asyncio
import threading
import time
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union, Optional, Tuple, List, Dict, Any
from importlib import resources
from jinja2 import Template

import pyarrow as pa

from .ntcip import async_send_ntcip, async_reset_all_detectors
from .config import SignalConfig
from pysnmp.hlapi.v3arch.asyncio import SnmpEngine


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
        debug: bool = False
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
            debug: Enable debug output
        """
        self.config = config
        self.device_id = config.device_id
        self.ip_port = config.ip_port
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
        """Sleep in short chunks so stop requests can interrupt waits quickly."""
        remaining = max(0.0, delay)
        while remaining > 0:
            if self._stop_event.is_set():
                return False
            chunk = min(remaining, 1.0)
            await asyncio.sleep(chunk)
            remaining -= chunk
        return not self._stop_event.is_set()

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
        
        if self.debug:
            print(f"[{self.device_id}] Loaded {len(self.input_data)} events")

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

        if self.debug:
            print(
                f"[{self.device_id}] Advanced replay timestamps by "
                f"{self.replay_latency_offset_seconds * 1000:.1f} ms"
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
        if self.debug:
            print(f"[{self.device_id}] Loading data from {path}")
        
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
        con.execute(f"ATTACH DATABASE '{db_path}' AS LastFail (TYPE SQLITE)")
        con.execute("USE LastFail")
        
        sql = _get_sql_template('load_maxtime_db.sql')
        self.input_data = con.execute(sql).df()
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
        if self.debug:
            print(f"[{self.device_id}] Generating activation feed")

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
        
        if self.debug:
            print(f"[{self.device_id}] Generated {len(self.activation_feed)} activation commands")

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
            try:
                await async_send_ntcip(
                    self.ip_port,
                    group_number,
                    state_integer,
                    detector_type,
                    timeout=self.snmp_timeout_seconds,
                    snmp_engine=snmp_engine,
                )
                self._final_send_failures[key] = 0
                return True
            except Exception as exc:
                is_final_attempt = attempt >= attempts
                if is_final_attempt:
                    self._final_send_failures[key] = self._final_send_failures.get(key, 0) + 1
                    print(
                        f"[{self.device_id}] SNMP send failed for group {group_number} "
                        f"type {detector_type} state {state_integer} after "
                        f"{attempts} attempt(s): {exc}",
                        flush=True,
                    )
                    return False

                if self.debug:
                    print(
                        f"[{self.device_id}] Retrying group {group_number} type {detector_type} "
                        f"state {state_integer}, attempt {attempt + 1}/{attempts}: {exc}",
                        flush=True,
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
        await self._send_queue.put((key, state_integer))

    async def _send_command(self, row, snmp_engine: SnmpEngine) -> None:
        """Queue a single SNMP command asynchronously on the per-device send worker."""
        if self._stop_event.is_set():
            return

        key = self._command_key(row.group_number, row.DetectorType)
        state_integer = int(row.state_integer)

        if not self._first_send_logged and self.debug:
            self._first_send_logged = True
            print(
                f"[{self.device_id}] First command dispatched at {datetime.now():%H:%M:%S} "
                f"for target {pd.to_datetime(row.TimeStamp):%H:%M:%S} "
                f"group {row.group_number} type {row.DetectorType} state {state_integer}",
                flush=True,
            )

        await self._enqueue_state_send(key, state_integer, snmp_engine)

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
        print(f"[{self.device_id}] Replay completed with unresolved SNMP send failures: {summary}", flush=True)

    async def _wait_for_pending_sends(self) -> None:
        """Wait until the per-device SNMP send queue has drained."""
        if self._send_queue is None:
            return

        await self._send_queue.join()

        if self._send_worker_task is not None and not self._send_worker_task.done():
            await self._send_queue.put(None)
            await self._send_worker_task

        self._send_worker_task = None
        self._send_queue = None

    async def _run_async_inner(self, activation_feed, snmp_engine: SnmpEngine) -> None:
        """Inner replay loop that uses a shared SnmpEngine."""
        if self._stop_event.is_set():
            print(f"[{self.device_id}] Stop requested before replay start", flush=True)
            return

        if self.tod_align:
            if self.simulation_start_time is None:
                self.simulation_start_time = datetime.now()

            # Shift all event timestamps so the earliest event's date maps to
            # today.  Events on subsequent dates (e.g. after midnight) naturally
            # land on tomorrow, the day after, etc.
            min_event_date = pd.to_datetime(activation_feed['TimeStamp'].min()).date()
            date_shift = self.simulation_start_time.date() - min_event_date

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
                print(f"[{self.device_id}] TOD align: date_shift={date_shift.days}d, "
                      f"skipping {skipped_events}/{len(activation_feed)} old events, "
                    f"first event at {first_target:%H:%M:%S}", flush=True)
            else:
                print(f"[{self.device_id}] TOD align: ALL {len(activation_feed)} events "
                    f"are before {replay_start:%H:%M:%S} — nothing to send!", flush=True)
                return

            # Slice to only the events we need (avoids iterating through skipped rows)
            active_feed = activation_feed.loc[first_valid_idx:]
            active_shifted = shifted_ts.loc[first_valid_idx:]
            total_to_send = len(active_feed)

            # Show wait time if first event is in the future
            first_delay = (first_target.to_pydatetime() - datetime.now()).total_seconds()
            if first_delay > 5:
                print(f"[{self.device_id}] Waiting {first_delay:.0f}s until first event...", flush=True)

            sent_count = 0
            last_progress = time.time()
            for idx, row in active_feed.iterrows():
                if self._stop_event.is_set():
                    print(f"[{self.device_id}] Stop requested — halting replay after {sent_count} events", flush=True)
                    break

                if self.latency_offset_provider is None:
                    target_time = active_shifted.loc[idx].to_pydatetime()

                    delay = (target_time - datetime.now()).total_seconds()
                    if delay > 0:
                        if not await self._sleep_interruptibly(delay):
                            print(f"[{self.device_id}] Stop requested while waiting for next event", flush=True)
                            break
                else:
                    source_target = base_ts.loc[idx]
                    while True:
                        if self._stop_event.is_set():
                            print(f"[{self.device_id}] Stop requested while waiting for next event", flush=True)
                            return

                        live_offset = self.latency_offset_provider.get_offset(self.device_id)
                        target_time = (
                            source_target
                            - pd.to_timedelta(live_offset, unit='s')
                        ).round('us').to_pydatetime()
                        delay = (target_time - datetime.now()).total_seconds()
                        if delay <= 0:
                            break
                        if not await self._sleep_interruptibly(min(delay, 1.0)):
                            print(f"[{self.device_id}] Stop requested while waiting for next event", flush=True)
                            return

                await self._send_command(row, snmp_engine)
                sent_count += 1

                # Print progress every 60 seconds
                now = time.time()
                if self.show_progress_logs and now - last_progress >= self.progress_log_interval_seconds:
                    print(f"[{self.device_id}] Sent {sent_count}/{total_to_send} events", flush=True)
                    last_progress = now

            await self._wait_for_pending_sends()
            self._print_send_summary()
            if self.show_progress_logs or self.debug:
                print(f"[{self.device_id}] Complete — sent {sent_count} events", flush=True)
            return

        start_time = asyncio.get_event_loop().time()
        total_events = len(activation_feed)
        sent_count = 0
        last_progress = time.time()

        for _, row in activation_feed.iterrows():
            if self._stop_event.is_set():
                print(f"[{self.device_id}] Stop requested — halting replay after {sent_count} events", flush=True)
                break

            # Calculate delay from start
            current_time = asyncio.get_event_loop().time()
            delay = row.sleep_time_cumulative - (current_time - start_time)

            if delay > 0:
                if not await self._sleep_interruptibly(delay):
                    print(f"[{self.device_id}] Stop requested while waiting for next event", flush=True)
                    break

            # Send command (non-blocking via executor)
            await self._send_command(row, snmp_engine)
            sent_count += 1

            # Print progress every 60 seconds
            now = time.time()
            if self.show_progress_logs and now - last_progress >= self.progress_log_interval_seconds:
                print(f"[{self.device_id}] Sent {sent_count}/{total_events} events", flush=True)
                last_progress = now

        await self._wait_for_pending_sends()
        self._print_send_summary()
        if self.show_progress_logs or self.debug:
            print(f"[{self.device_id}] Complete — sent {sent_count} events", flush=True)
    
    def _run_in_thread(self) -> None:
        """Run the async replay in a new event loop in a separate thread."""
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        new_loop.run_until_complete(self._reset_and_run())
        new_loop.close()

    async def _reset_and_run(self) -> None:
        """Create one SnmpEngine, reset detectors, wait for cycle, then replay."""
        snmp_engine = SnmpEngine()
        try:
            await async_reset_all_detectors(
                self.ip_port,
                debug=self.debug,
                timeout=self.snmp_timeout_seconds,
                snmp_engine=snmp_engine,
            )
            self._wait_until_next_cycle()
            self.simulation_start_time = datetime.now()
            await self._run_async_inner(self.activation_feed, snmp_engine)
        finally:
            snmp_engine.close_dispatcher()
    
    def _wait_until_next_cycle(self) -> None:
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
            if self.debug:
                print(
                    f"[{self.device_id}] Waiting {sleep_time:.1f}s to align with cycle offset {offset:.1f}s"
                )
            remaining = sleep_time
            while remaining > 0 and not self._stop_event.is_set():
                chunk = min(remaining, 1.0)
                time.sleep(chunk)
                remaining -= chunk
    
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
            thread = threading.Thread(target=self._run_in_thread)
            thread.start()
            thread.join()
        
        return self.simulation_start_time


def create_replays(
    configs: List[SignalConfig],
    simulation_speed: float = 1.0,
    snmp_timeout_seconds: float = 2.0,
    snmp_send_retries: int = 0,
    snmp_retry_backoff_seconds: float = 0.25,
    show_progress_logs: bool = False,
    progress_log_interval_seconds: float = 60.0,
    stop_event: Optional[threading.Event] = None,
    debug: bool = False
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
        debug: Enable debug output
    
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
        )
        for config in configs
    ]

