"""
Data collector module for polling controllers and storing results in DuckDB.
"""

import gc
import duckdb
import logging
import os
import pandas as pd
import psutil
import requests
import warnings
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any, Set
from pathlib import Path
import time
import multiprocessing as mp
from dataclasses import dataclass
import threading

logger = logging.getLogger(__name__)

from .config import SignalConfig


def _log_memory(label: str = "") -> None:
    """Log current process RSS memory usage."""
    proc = psutil.Process(os.getpid())
    rss_mb = proc.memory_info().rss / (1024 * 1024)
    logger.info("Memory RSS: %.1f MB %s", rss_mb, label)


def _get_sql_template(filename: str) -> str:
    """Load a SQL template from the package's sql directory."""
    sql_dir = Path(__file__).parent / "sql"
    with open(sql_dir / filename, 'r') as f:
        return f.read()


@dataclass
class ConflictRecord:
    """Record of a detected conflict."""
    device_id: str
    run_number: int
    timestamp: datetime
    conflict_details: str


def fetch_output_data(ip: str, http_port: int = 80, since: Optional[datetime] = None) -> pd.DataFrame:
    """
    Fetch the event log from a MAXTIME controller.
    
    Args:
        ip: IP address of the controller
        http_port: HTTP port for data collection (default: 80)
        since: If provided, only fetch events after this timestamp.
            The controller supports a ``?since=`` query parameter.
    
    Returns:
        DataFrame with columns: TimeStamp, EventTypeID, Parameter
    """
    url = f'http://{ip}:{http_port}/v1/asclog/xml/full'
    if since is not None:
        since_str = since.strftime('%m-%d-%Y %H:%M:%S.0')
        url = f'{url}?since={since_str}'
    
    response = requests.get(url, verify=False, timeout=30)
    response.raise_for_status()
    
    # Parse XML incrementally to minimise peak memory.  Use .content
    # (bytes) instead of .text (decoded str) to avoid an extra copy, then
    # clear the tree as we extract attributes.
    from io import BytesIO
    data = []
    for _event, elem in ET.iterparse(BytesIO(response.content), events=('end',)):
        if elem.tag == 'Event':
            attrib = elem.attrib
            data.append({k: attrib[k] for k in attrib if k != 'ID'})
            elem.clear()
    del response  # release HTTP body
    
    if not data:
        return pd.DataFrame(columns=['TimeStamp', 'EventTypeID', 'Parameter'])
    
    df = pd.DataFrame(data)
    del data
    
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
        for w in caught:
            samples = df['TimeStamp'].head(5).tolist()
            logger.warning(
                f"Datetime warning from {ip}:{http_port} ({len(df)} events): "
                f"{w.message}\n"
                f"  Sample raw values: {samples}"
            )
    except Exception:
        samples = df['TimeStamp'].head(5).tolist()
        logger.warning(
            f"Datetime parsing failed for {ip}:{http_port}. "
            f"Sample TimeStamp values: {samples}"
        )
        raise
    df['EventTypeID'] = df['EventTypeID'].astype(int)
    df['Parameter'] = df['Parameter'].astype(int)
    
    return df


def check_conflicts(
    events_df: pd.DataFrame,
    incompatible_pairs: List[Tuple[str, str]]
) -> pd.DataFrame:
    """
    Check for phase/overlap conflicts in event data.
    
    Args:
        events_df: DataFrame with TimeStamp, EventTypeID, Parameter columns
        incompatible_pairs: List of (signal1, signal2) tuples that conflict
    
    Returns:
        DataFrame with TimeStamp, Conflict_Details for each conflict found
    """
    if events_df.empty or not incompatible_pairs:
        return pd.DataFrame(columns=['TimeStamp', 'Conflict_Details'])
    
    con = duckdb.connect()
    con.register('Event', events_df)
    
    sql = _get_sql_template('load_conflict_events.sql')
    raw_data = con.sql(sql).df()
    con.close()
    
    if raw_data.empty:
        return pd.DataFrame(columns=['TimeStamp', 'Conflict_Details'])
    
    conflict_df = _build_conflict_dataframe(raw_data, incompatible_pairs)
    return conflict_df


def _build_conflict_dataframe(
    raw_data: pd.DataFrame,
    incompatible_pairs: List[Tuple[str, str]],
) -> pd.DataFrame:
    """Detect conflicts from a full ordered event stream for one run."""
    if raw_data.empty:
        return pd.DataFrame(columns=['TimeStamp', 'Conflict_Details'])

    current_states: Dict[str, int] = {}
    for param in raw_data['Parameter'].unique().tolist():
        current_states.setdefault(param, 0)

    state_records = []
    for _, row in raw_data.iterrows():
        param = row['Parameter']
        state = row['state_integer']
        current_states[param] = state

        snapshot = {'TimeStamp': row['TimeStamp']}
        for tracked_param, tracked_state in current_states.items():
            snapshot[tracked_param] = tracked_state
        state_records.append(snapshot)

    if not state_records:
        return pd.DataFrame(columns=['TimeStamp', 'Conflict_Details'])

    final_df = pd.DataFrame(state_records)

    def check_incompatibilities(row):
        conflicts = []
        for (param1, param2) in incompatible_pairs:
            if row.get(param1, 0) == 1 and row.get(param2, 0) == 1:
                conflicts.append((param1, param2))
        return conflicts

    final_df['Conflicts'] = final_df.apply(
        lambda row: check_incompatibilities(row), axis=1
    )
    final_df['Has_Conflict'] = final_df['Conflicts'].apply(lambda x: len(x) > 0)
    final_df['Conflict_Details'] = final_df['Conflicts'].apply(
        lambda x: '; '.join([f"{a} & {b}" for a, b in x]) if x else ""
    )

    final_df = final_df.drop_duplicates(subset='TimeStamp', keep='last')
    conflict_df = final_df[final_df['Has_Conflict']][['TimeStamp', 'Conflict_Details']].copy()
    if conflict_df.empty:
        return conflict_df

    conflict_df = conflict_df[conflict_df['Conflict_Details'] != ""]
    if conflict_df.empty:
        return conflict_df

    # Report the first timestamp for each distinct conflict signature in a run.
    conflict_df = (
        conflict_df.sort_values('TimeStamp')
        .groupby('Conflict_Details', as_index=False, sort=False)
        .first()
        .sort_values('TimeStamp')
        .reset_index(drop=True)
    )

    return conflict_df[['TimeStamp', 'Conflict_Details']]


class DatabaseManager:
    """Manages DuckDB database for storing simulation data."""

    # Retry settings for DuckDB file lock issues on network shares
    _MAX_RETRIES = 5
    _BASE_DELAY = 2.0  # seconds
    
    def __init__(self, db_path: str):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = db_path
        self._init_database()

    def _connect_with_retry(self) -> duckdb.DuckDBPyConnection:
        """Open a DuckDB connection with retry logic for file-lock errors on network shares."""
        last_err = None
        for attempt in range(1, self._MAX_RETRIES + 1):
            try:
                return duckdb.connect(self.db_path)
            except Exception as e:
                err_msg = str(e).lower()
                if "being used by another process" in err_msg or "ioexception" in err_msg:
                    last_err = e
                    delay = self._BASE_DELAY * attempt
                    time.sleep(delay)
                else:
                    raise
        raise last_err  # type: ignore[misc]
    
    def _init_database(self) -> None:
        """Initialize database tables if they don't exist."""
        con = self._connect_with_retry()
        try:
            # Events table must include run_number in the primary key so
            # repeated runs with identical timestamps do not overwrite data.
            con.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    device_id VARCHAR,
                    run_number INTEGER,
                    timestamp TIMESTAMP,
                    event_id INTEGER,
                    parameter INTEGER,
                    PRIMARY KEY (device_id, run_number, timestamp, event_id, parameter)
                )
            """)

            # Migrate legacy schema where run_number was not in the PK.
            info = con.execute("PRAGMA table_info('events')").fetchall()
            run_number_in_pk = any(
                row[1] == 'run_number' and int(row[5]) > 0
                for row in info
            )
            if info and not run_number_in_pk:
                con.execute("""
                    CREATE TABLE events_new (
                        device_id VARCHAR,
                        run_number INTEGER,
                        timestamp TIMESTAMP,
                        event_id INTEGER,
                        parameter INTEGER,
                        PRIMARY KEY (device_id, run_number, timestamp, event_id, parameter)
                    )
                """)
                con.execute("""
                    INSERT INTO events_new
                    SELECT device_id, run_number, timestamp, event_id, parameter
                    FROM events
                """)
                con.execute("DROP TABLE events")
                con.execute("ALTER TABLE events_new RENAME TO events")

            # Conflicts table
            con.execute("""
                CREATE TABLE IF NOT EXISTS conflicts (
                    device_id VARCHAR,
                    run_number INTEGER,
                    timestamp TIMESTAMP,
                    conflict_details VARCHAR
                )
            """)

            # Input events table (for comparison)
            con.execute("""
                CREATE TABLE IF NOT EXISTS input_events (
                    device_id VARCHAR,
                    timestamp TIMESTAMP,
                    event_id INTEGER,
                    parameter INTEGER
                )
            """)

            con.execute("""
                CREATE TABLE IF NOT EXISTS simulation_runs (
                    run_number INTEGER PRIMARY KEY,
                    status VARCHAR,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP
                )
            """)
        finally:
            con.close()
    
    def get_max_run_number(self) -> int:
        """Get the maximum run number currently in the database."""
        completed_runs = self.get_completed_run_numbers()
        if completed_runs:
            return completed_runs[-1]

        con = self._connect_with_retry()
        try:
            # Check both events and conflicts tables efficiently
            res = con.execute("""
                SELECT MAX(max_run) FROM (
                    SELECT MAX(run_number) as max_run FROM events
                    UNION ALL
                    SELECT MAX(run_number) as max_run FROM conflicts
                )
            """).fetchone()
            max_run = res[0] if res and res[0] is not None else 0
            return int(max_run)
        except Exception:
            return 0
        finally:
            con.close()

    def get_completed_run_numbers(self) -> List[int]:
        """Return completed run numbers in ascending order."""
        con = self._connect_with_retry()
        try:
            simulation_runs_exists = con.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE lower(table_name) = 'simulation_runs'
            """).fetchone()[0] > 0

            if simulation_runs_exists:
                completed = con.execute("""
                    SELECT run_number
                    FROM simulation_runs
                    WHERE status = 'completed'
                    ORDER BY run_number
                """).fetchall()
                if completed:
                    return [int(row[0]) for row in completed]

            fallback = con.execute("""
                SELECT DISTINCT run_number
                FROM (
                    SELECT run_number FROM events
                    UNION
                    SELECT run_number FROM conflicts
                )
                WHERE run_number IS NOT NULL
                ORDER BY run_number
            """).fetchall()
            return [int(row[0]) for row in fallback]
        finally:
            con.close()

    def mark_run_started(self, run_number: int) -> None:
        """Mark a run as started so interrupted runs can be resumed cleanly."""
        con = self._connect_with_retry()
        try:
            con.execute("DELETE FROM simulation_runs WHERE run_number = ?", [run_number])
            con.execute(
                """
                INSERT INTO simulation_runs (run_number, status, started_at, completed_at)
                VALUES (?, 'running', ?, NULL)
                """,
                [run_number, datetime.now()],
            )
        finally:
            con.close()

    def mark_run_completed(self, run_number: int) -> None:
        """Mark a run as completed."""
        con = self._connect_with_retry()
        try:
            started_row = con.execute(
                "SELECT started_at FROM simulation_runs WHERE run_number = ?",
                [run_number],
            ).fetchone()
            started_at = started_row[0] if started_row and started_row[0] is not None else datetime.now()
            con.execute("DELETE FROM simulation_runs WHERE run_number = ?", [run_number])
            con.execute(
                """
                INSERT INTO simulation_runs (run_number, status, started_at, completed_at)
                VALUES (?, 'completed', ?, ?)
                """,
                [run_number, started_at, datetime.now()],
            )
        finally:
            con.close()
    
    def insert_events(
        self,
        df: pd.DataFrame,
        device_id: str,
        run_number: int,
        simulation_start_time: datetime
    ) -> int:
        """
        Insert events into the database with deduplication.
        
        Args:
            df: DataFrame with TimeStamp, EventTypeID, Parameter columns
            device_id: Device identifier
            run_number: Current simulation run number
            simulation_start_time: Start time of simulation (filter events before this)
        
        Returns:
            Number of rows inserted
        """
        if df.empty:
            return 0
        
        # Filter to events >= simulation start time
        df = df[df['TimeStamp'] >= simulation_start_time].copy()
        
        if df.empty:
            return 0
        
        # Prepare data for insertion
        df['device_id'] = device_id
        df['run_number'] = run_number
        df = df.rename(columns={
            'TimeStamp': 'timestamp',
            'EventTypeID': 'event_id',
            'Parameter': 'parameter'
        })
        
        df = df[['device_id', 'run_number', 'timestamp', 'event_id', 'parameter']]
        
        con = self._connect_with_retry()
        try:
            # Use INSERT OR REPLACE for deduplication
            con.register('new_events', df)
            con.execute("""
                INSERT OR REPLACE INTO events
                SELECT * FROM new_events
            """)
            rows_inserted = len(df)
        finally:
            con.close()
        
        return rows_inserted
    
    def insert_conflict(self, conflict: ConflictRecord) -> None:
        """Insert a conflict record into the database if it doesn't already exist."""
        con = self._connect_with_retry()
        try:
            # Check if this exact conflict already exists
            existing = con.execute("""
                SELECT COUNT(*) as count FROM conflicts 
                WHERE device_id = ? 
                AND run_number = ? 
                AND timestamp = ? 
                AND conflict_details = ?
            """, [conflict.device_id, conflict.run_number, conflict.timestamp, conflict.conflict_details]).fetchone()
            
            # Only insert if it doesn't exist
            if existing[0] == 0:
                con.execute("""
                    INSERT INTO conflicts (device_id, run_number, timestamp, conflict_details)
                    VALUES (?, ?, ?, ?)
                """, [conflict.device_id, conflict.run_number, conflict.timestamp, conflict.conflict_details])
        finally:
            con.close()
    
    def insert_input_events(self, df: pd.DataFrame, device_id: str) -> None:
        """Store input events for later comparison."""
        if df.empty:
            return
        
        df = df.copy()
        df['device_id'] = device_id
        
        # Normalize column names
        col_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ('timestamp', 'time_stamp'):
                col_map[col] = 'timestamp'
            elif col_lower in ('event_id', 'eventid', 'eventtypeid'):
                col_map[col] = 'event_id'
            elif col_lower in ('parameter', 'detector'):
                col_map[col] = 'parameter'
        
        df = df.rename(columns=col_map)
        df = df[['device_id', 'timestamp', 'event_id', 'parameter']]
        
        con = self._connect_with_retry()
        try:
            con.execute("DELETE FROM input_events WHERE device_id = ?", [device_id])
            con.register('input_df', df)
            con.execute("""
                INSERT INTO input_events
                SELECT * FROM input_df
            """)
        finally:
            con.close()
    
    def get_events(
        self,
        device_id: Optional[str] = None,
        run_number: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> pd.DataFrame:
        """Retrieve events from the database with optional filters."""
        con = self._connect_with_retry()
        try:
            query = "SELECT * FROM events WHERE 1=1"
            params = []
            
            if device_id is not None:
                query += " AND device_id = ?"
                params.append(device_id)
            
            if run_number is not None:
                query += " AND run_number = ?"
                params.append(run_number)
            
            if start_time is not None:
                query += " AND timestamp >= ?"
                params.append(start_time)
            
            if end_time is not None:
                query += " AND timestamp <= ?"
                params.append(end_time)
            
            query += " ORDER BY timestamp"
            
            df = con.execute(query, params).df()
        finally:
            con.close()
        
        return df
    
    def get_conflicts(
        self,
        device_id: Optional[str] = None,
        run_number: Optional[int] = None
    ) -> pd.DataFrame:
        """Retrieve conflicts from the database."""
        con = self._connect_with_retry()
        try:
            query = "SELECT * FROM conflicts WHERE 1=1"
            params = []
            
            if device_id is not None:
                query += " AND device_id = ?"
                params.append(device_id)
            
            if run_number is not None:
                query += " AND run_number = ?"
                params.append(run_number)
            
            query += " ORDER BY timestamp"
            
            df = con.execute(query, params).df()
        finally:
            con.close()
        
        return df
    
    def get_input_events(self, device_id: Optional[str] = None) -> pd.DataFrame:
        """Retrieve stored input events."""
        con = self._connect_with_retry()
        try:
            if device_id:
                df = con.execute(
                    "SELECT * FROM input_events WHERE device_id = ? ORDER BY timestamp",
                    [device_id]
                ).df()
            else:
                df = con.execute("SELECT * FROM input_events ORDER BY timestamp").df()
        finally:
            con.close()
        return df
    
    def clear_run_data(self, run_number: Optional[int] = None) -> None:
        """Clear data for a specific run or all runs."""
        con = self._connect_with_retry()
        try:
            if run_number is not None:
                con.execute("DELETE FROM events WHERE run_number = ?", [run_number])
                con.execute("DELETE FROM conflicts WHERE run_number = ?", [run_number])
            else:
                con.execute("DELETE FROM events")
                con.execute("DELETE FROM conflicts")
        finally:
            con.close()

    def clear_device_data(self, device_ids: List[str]) -> None:
        """Clear all stored data for one or more device IDs."""
        if not device_ids:
            return

        placeholders = ",".join(["?"] * len(device_ids))
        con = self._connect_with_retry()
        try:
            con.execute(f"DELETE FROM events WHERE device_id IN ({placeholders})", device_ids)
            con.execute(f"DELETE FROM conflicts WHERE device_id IN ({placeholders})", device_ids)
            con.execute(f"DELETE FROM input_events WHERE device_id IN ({placeholders})", device_ids)

            has_comparison = con.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE lower(table_name) = 'comparison_results'
            """).fetchone()[0] > 0
            if has_comparison:
                con.execute(
                    f"DELETE FROM comparison_results WHERE device_id IN ({placeholders})",
                    device_ids,
                )
        finally:
            con.close()


class DataCollector:
    """
    Collects data from controllers and checks for conflicts.
    
    Runs as a separate process, polling controllers at regular intervals.
    """
    
    def __init__(
        self,
        db_path: str,
        device_configs: Dict[str, Tuple[Tuple[str, int], List[Tuple[str, str]], Optional[int]]],
        collection_interval_minutes: float = 5.0,
        stop_on_conflict: bool = False,
        debug: bool = False
    ):
        """
        Initialize the data collector.
        
        Args:
            db_path: Path to DuckDB database
            device_configs: Dict mapping device_id to (ip_port, incompatible_pairs, http_port)
            collection_interval_minutes: How often to collect data
            stop_on_conflict: Whether to stop before the next run when final conflict detection finds a conflict
            debug: Enable debug output
        """
        self.db_path = db_path
        self.device_configs = device_configs
        self.collection_interval = collection_interval_minutes * 60  # Convert to seconds
        self.stop_on_conflict = stop_on_conflict
        self.debug = debug

        self._stop_event = mp.Event()
        self._collector_process = None
        self._db: Optional[DatabaseManager] = None
        self._last_seen_event_key: Dict[str, Tuple[pd.Timestamp, int, int]] = {}
        self._consecutive_insert_failures: Dict[str, int] = {}
        self._fetch_error_logged: Set[str] = set()

    @staticmethod
    def _new_rows_since_watermark(
        df: pd.DataFrame,
        last_key: Optional[Tuple[pd.Timestamp, int, int]]
    ) -> pd.DataFrame:
        """Return rows strictly after the last seen (timestamp, event_id, parameter) key."""
        if df.empty:
            return df

        df = df.sort_values(['TimeStamp', 'EventTypeID', 'Parameter']).copy()
        if last_key is None:
            return df

        last_ts, last_event_id, last_parameter = last_key
        mask = (
            (df['TimeStamp'] > last_ts)
            | (
                (df['TimeStamp'] == last_ts)
                & (
                    (df['EventTypeID'] > last_event_id)
                    | (
                        (df['EventTypeID'] == last_event_id)
                        & (df['Parameter'] > last_parameter)
                    )
                )
            )
        )
        return df[mask].copy()
    
    def run_collection_loop(
        self,
        run_number: int,
        simulation_start_time: datetime,
        stop_event: threading.Event,
        conflict_callback: Optional[callable] = None,
        error_event: Optional[threading.Event] = None,
    ) -> None:
        """
        Run continuous collection in a loop until stopped.
        
        Args:
            run_number: Current simulation run number
            simulation_start_time: Start time of simulation
            stop_event: Threading event to stop the loop
            conflict_callback: Optional callback when conflicts found
            error_event: Optional threading event set on fatal collection errors
        """
        if self.debug:
            print(f"Starting collection loop for run {run_number}")
        
        while not stop_event.is_set():
            try:
                self.collect_once(
                    run_number,
                    simulation_start_time,
                    conflict_callback=conflict_callback,
                    error_event=error_event,
                )
            except Exception as exc:
                print(f"\n*** COLLECTION ERROR: {exc}")
                print("Stopping data collection for this run.")
                if error_event is not None:
                    error_event.set()
                stop_event.set()
                return
            
            _log_memory(f"[after collect run={run_number}]")
            gc.collect()

            # Sleep in small intervals so we can check stop_event frequently
            sleep_elapsed = 0
            while sleep_elapsed < self.collection_interval and not stop_event.is_set():
                time.sleep(1)  # Check every 1 second
                sleep_elapsed += 1
        
        if self.debug:
            print(f"Collection loop stopped for run {run_number}")
    
    def collect_once(
        self,
        run_number: int,
        simulation_start_time: datetime,
        detect_conflicts: bool = False,
        conflict_callback: Optional[callable] = None,
        error_event: Optional[threading.Event] = None,
    ) -> None:
        """
        Collect data once from all devices.
        
        Args:
            run_number: Current simulation run number
            simulation_start_time: Start time of simulation
            detect_conflicts: Whether to check conflicts after storing events
            conflict_callback: Optional callback when conflicts found
            error_event: Optional event set when repeated DB insert failures occur
        """
        if self.debug:
            print(f"Collecting data for run {run_number}...")
        
        # Fetch data for each device
        for device_id, (ip_port, incompatible_pairs, http_port) in self.device_configs.items():
            # Skip collection if http_port is None
            if http_port is None:
                if self.debug:
                    print(f"HTTP collection disabled for {device_id}")
                continue
            
            ip = ip_port[0]
            # Use the watermark timestamp (minus 10s buffer) to ask the
            # controller for only recent events, avoiding fetching the
            # entire log on every poll.
            since = None
            last_key = self._last_seen_event_key.get(device_id)
            if last_key is not None:
                since = (last_key[0] - pd.Timedelta(seconds=10)).to_pydatetime()
            try:
                df = fetch_output_data(ip, http_port, since=since)
            except (
                requests.exceptions.RequestException,
                ET.ParseError,
                ValueError,
            ) as exc:
                # HTTP collection failures should not abort replay. Controllers may still
                # be reachable via SNMP and this endpoint may recover on later polls.
                if device_id not in self._fetch_error_logged:
                    print(
                        f"\n*** COLLECTION WARNING: Cannot collect HTTP events for "
                        f"{device_id} at {ip}:{http_port} ({type(exc).__name__}: {exc}). "
                        "Continuing replay without aborting."
                    )
                    self._fetch_error_logged.add(device_id)
                continue
            else:
                self._fetch_error_logged.discard(device_id)
            
            if df.empty:
                if self.debug:
                    print(f"No data returned for {device_id}")
                continue

            last_seen_key = self._last_seen_event_key.get(device_id)
            df = self._new_rows_since_watermark(df, last_seen_key)

            if df.empty:
                if self.debug:
                    print(f"No new events for {device_id}")
                continue

            # Insert events into DB
            try:
                if self._db is None:
                    self._db = DatabaseManager(self.db_path)
                db = self._db
                rows = db.insert_events(df, device_id, run_number, simulation_start_time)
                self._consecutive_insert_failures[device_id] = 0

                # Advance watermark only after a successful insert.
                last_row = df.iloc[-1]
                self._last_seen_event_key[device_id] = (
                    pd.Timestamp(last_row['TimeStamp']),
                    int(last_row['EventTypeID']),
                    int(last_row['Parameter']),
                )
                if self.debug:
                    print(f"Inserted {rows} events for {device_id}")
            except Exception as e:
                fail_count = self._consecutive_insert_failures.get(device_id, 0) + 1
                self._consecutive_insert_failures[device_id] = fail_count
                if self.debug:
                    print(f"DB insert failed for {device_id} (attempt {fail_count}): {e}")
                if fail_count >= 3:
                    if error_event is not None:
                        error_event.set()
                    raise RuntimeError(
                        f"Repeated DB insert failures for {device_id} ({fail_count} consecutive)"
                    ) from e
                continue
            if not detect_conflicts:
                continue

            start = time.time()
            run_events = db.get_events(device_id=device_id, run_number=run_number)
            if not run_events.empty:
                run_events = run_events.rename(columns={
                    'timestamp': 'TimeStamp',
                    'event_id': 'EventTypeID',
                    'parameter': 'Parameter',
                })

            conflicts = check_conflicts(run_events, incompatible_pairs)
            if self.debug:
                print(f"Conflict check for {device_id} took {time.time() - start:.2f} seconds")
            if conflicts.empty:
                continue

            if self.debug:
                print(f"Conflicts found for {device_id}!")

            conflict_records = []
            for _, row in conflicts.iterrows():
                conflict_records.append(ConflictRecord(
                    device_id=device_id,
                    run_number=run_number,
                    timestamp=row['TimeStamp'],
                    conflict_details=row['Conflict_Details']
                ))

            try:
                for conflict in conflict_records:
                    db.insert_conflict(conflict)
            except Exception as e:
                if self.debug:
                    print(f"DB conflict insert failed for {device_id}: {e}")

            if conflict_callback:
                conflict_callback(conflict_records)
    
    def stop(self) -> None:
        """Stop the collection loop."""
        self._stop_event.set()
        if self._collector_process:
            self._collector_process.terminate()
            self._collector_process.join()
            self._collector_process = None
