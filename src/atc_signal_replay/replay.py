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
from typing import Union, Optional, Tuple, List
from importlib import resources
from jinja2 import Template

try:
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    pa = None

from .ntcip import send_ntcip, reset_all_detectors
from .config import SignalConfig


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
        limit_rows: int = 0,
        debug: bool = False
    ):
        """
        Initialize the SignalReplay.
        
        Args:
            config: SignalConfig with device settings and events
            simulation_speed: Speed multiplier for playback (1.0 = real-time)
            limit_rows: Limit number of input rows (0 = no limit)
            debug: Enable debug output
        """
        self.config = config
        self.device_id = config.device_id
        self.ip_port = config.ip_port
        self.cycle_length = config.cycle_length
        self.incompatible_pairs = config.incompatible_pairs
        self.simulation_speed = simulation_speed
        self.limit_rows = limit_rows
        self.debug = debug
        
        self.input_data: Optional[pd.DataFrame] = None
        self.activation_feed: Optional[pd.DataFrame] = None
        self.original_start_time: Optional[datetime] = None
        self.simulation_start_time: Optional[datetime] = None
        
        # Load and process events
        self._load_events()
        self._generate_activation_feed()
    
    def _load_events(self) -> None:
        """Load events from the configured source."""
        events = self.config.events
        
        if isinstance(events, pd.DataFrame):
            self._load_from_dataframe(events)
        elif HAS_PYARROW and isinstance(events, pa.Table):
            self._load_from_arrow(events)
        elif isinstance(events, (str, Path)):
            self._load_from_path(str(events))
        else:
            raise ValueError(f"Unsupported events type: {type(events)}")
        
        # Add device_id if not present
        if 'DeviceId' not in self.input_data.columns:
            self.input_data['DeviceId'] = self.device_id
        
        # Apply row limit if specified
        if self.limit_rows > 0:
            self.input_data = self.input_data.tail(self.limit_rows)
        
        if self.debug:
            print(f"[{self.device_id}] Loaded {len(self.input_data)} events")
    
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
        if path.endswith('.db'):
            self._load_from_sqlite(path)
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
            
            self.input_data = duckdb.sql(sql).df()
    
    def _load_from_sqlite(self, db_path: str) -> None:
        """Load events from a MAXTIME SQLite database."""
        con = duckdb.connect()
        con.execute(f"ATTACH DATABASE '{db_path}' AS LastFail (TYPE SQLITE)")
        con.execute("USE LastFail")
        
        sql = _get_sql_template('load_maxtime_db.sql')
        self.input_data = con.execute(sql).df()
        con.close()
    
    def _generate_activation_feed(self) -> None:
        """Generate the activation feed from input data."""
        if self.debug:
            print(f"[{self.device_id}] Generating activation feed")
        
        con = duckdb.connect()
        con.register('raw_data', self.input_data)
        
        # Impute missing actuations
        sql_impute = _get_sql_template('impute_actuations.sql')
        imputed = con.execute(sql_impute).df()
        con.register('imputed', imputed)
        
        # Generate activation feed
        sql_feed = _get_sql_template('generate_activation_feed.sql')
        self.activation_feed = con.execute(sql_feed).df()
        con.close()
        
        # Add cumulative sleep time (adjusted for simulation speed)
        self.activation_feed['sleep_time_cumulative'] = (
            self.activation_feed['sleep_time'].cumsum() / self.simulation_speed
        )
        
        # Store original start time for cycle sync
        self.original_start_time = self.activation_feed['TimeStamp'].min()
        
        if self.debug:
            print(f"[{self.device_id}] Generated {len(self.activation_feed)} activation commands")
    
    def get_run_duration(self) -> float:
        """Get the total duration of the simulation in seconds."""
        if self.activation_feed is None:
            return 0.0
        return self.activation_feed['sleep_time_cumulative'].max()
    
    async def _send_command(self, row, start_time: float) -> None:
        """Send a single SNMP command after waiting for the required delay."""
        current_time = asyncio.get_event_loop().time()
        delay = row.sleep_time_cumulative - (current_time - start_time)
        
        if delay > 0:
            await asyncio.sleep(delay)
        
        # Send command in executor to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            send_ntcip,
            self.ip_port,
            row.group_number,
            row.state_integer,
            row.DetectorType
        )
    
    async def _run_async(self) -> None:
        """Run the async SNMP command replay."""
        activation_feed = self.activation_feed.copy()
        start_time = asyncio.get_event_loop().time()
        
        tasks = [
            asyncio.create_task(self._send_command(row, start_time))
            for _, row in activation_feed.iterrows()
        ]
        
        await asyncio.gather(*tasks)
    
    def _run_in_thread(self) -> None:
        """Run the async replay in a new event loop in a separate thread."""
        reset_all_detectors(self.ip_port, debug=self.debug)
        self._wait_until_next_cycle()
        
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        new_loop.run_until_complete(self._run_async())
        new_loop.close()
    
    def _wait_until_next_cycle(self) -> None:
        """Wait until the next cycle boundary for coordinated signals."""
        if self.cycle_length == 0:
            return
        
        delta = datetime.now() - self.original_start_time
        delta_seconds = delta.total_seconds()
        
        cycles_passed = delta_seconds / self.cycle_length
        next_cycle = math.ceil(cycles_passed) * self.cycle_length
        next_time = self.original_start_time + timedelta(seconds=next_cycle)
        
        sleep_time = (next_time - datetime.now()).total_seconds()
        
        if sleep_time > 0:
            if self.debug:
                print(f"[{self.device_id}] Waiting {sleep_time:.1f}s until next cycle")
            time.sleep(sleep_time)
    
    def run(self) -> datetime:
        """
        Run the SNMP command replay.
        
        Returns:
            The simulation start time
        """
        self.simulation_start_time = datetime.now()
        
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No event loop running, safe to use asyncio.run
            reset_all_detectors(self.ip_port, debug=self.debug)
            self._wait_until_next_cycle()
            asyncio.run(self._run_async())
        else:
            # Event loop already running, use thread
            thread = threading.Thread(target=self._run_in_thread)
            thread.start()
            thread.join()
        
        return self.simulation_start_time


def create_replays(
    configs: List[SignalConfig],
    simulation_speed: float = 1.0,
    debug: bool = False
) -> List[SignalReplay]:
    """
    Create SignalReplay instances for multiple signals.
    
    Args:
        configs: List of SignalConfig objects
        simulation_speed: Speed multiplier for playback
        debug: Enable debug output
    
    Returns:
        List of SignalReplay instances
    """
    return [
        SignalReplay(config, simulation_speed=simulation_speed, debug=debug)
        for config in configs
    ]
