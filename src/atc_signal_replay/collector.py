"""
Data collector module for polling controllers and storing results in DuckDB.
"""

import duckdb
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
import time
import multiprocessing as mp
from dataclasses import dataclass
import threading

from .config import SignalConfig


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


def fetch_output_data(ip_port: Tuple[str, int]) -> pd.DataFrame:
    """
    Fetch the event log from a MAXTIME controller.
    
    Args:
        ip_port: Tuple of (IP address, port)
    
    Returns:
        DataFrame with columns: TimeStamp, EventTypeID, Parameter
    """
    url = f'http://{ip_port[0]}:{ip_port[1]}/v1/asclog/xml/full'
    
    response = requests.get(url, verify=False, timeout=30)
    response.raise_for_status()
    
    root = ET.fromstring(response.text)
    data = [event.attrib for event in root.findall('.//Event')]
    
    if not data:
        return pd.DataFrame(columns=['TimeStamp', 'EventTypeID', 'Parameter'])
    
    df = pd.DataFrame(data)
    
    # Drop ID column if present
    if 'ID' in df.columns:
        df = df.drop(columns='ID')
    
    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
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
    
    parameters = raw_data['Parameter'].unique().tolist()
    current_states = {param: 0 for param in parameters}
    state_records = []
    
    for _, row in raw_data.iterrows():
        param = row['Parameter']
        state = row['state_integer']
        current_states[param] = state
        
        snapshot = {'TimeStamp': row['TimeStamp']}
        for p in parameters:
            snapshot[p] = current_states[p]
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
    conflict_df = final_df[final_df['Has_Conflict']][['TimeStamp', 'Conflict_Details']]
    
    return conflict_df


class DatabaseManager:
    """Manages DuckDB database for storing simulation data."""
    
    def __init__(self, db_path: str):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self) -> None:
        """Initialize database tables if they don't exist."""
        con = duckdb.connect(self.db_path)
        
        # Events table
        con.execute("""
            CREATE TABLE IF NOT EXISTS events (
                device_id VARCHAR,
                run_number INTEGER,
                timestamp TIMESTAMP,
                event_id INTEGER,
                parameter INTEGER,
                PRIMARY KEY (device_id, timestamp, event_id, parameter)
            )
        """)
        
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
        
        con = duckdb.connect(self.db_path)
        
        # Use INSERT OR REPLACE for deduplication
        con.register('new_events', df)
        con.execute("""
            INSERT OR REPLACE INTO events
            SELECT * FROM new_events
        """)
        
        rows_inserted = len(df)
        con.close()
        
        return rows_inserted
    
    def insert_conflict(self, conflict: ConflictRecord) -> None:
        """Insert a conflict record into the database."""
        con = duckdb.connect(self.db_path)
        con.execute("""
            INSERT INTO conflicts (device_id, run_number, timestamp, conflict_details)
            VALUES (?, ?, ?, ?)
        """, [conflict.device_id, conflict.run_number, conflict.timestamp, conflict.conflict_details])
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
        
        con = duckdb.connect(self.db_path)
        con.register('input_df', df)
        con.execute("""
            INSERT INTO input_events
            SELECT * FROM input_df
        """)
        con.close()
    
    def get_events(
        self,
        device_id: Optional[str] = None,
        run_number: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> pd.DataFrame:
        """Retrieve events from the database with optional filters."""
        con = duckdb.connect(self.db_path)
        
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
        con.close()
        
        return df
    
    def get_conflicts(
        self,
        device_id: Optional[str] = None,
        run_number: Optional[int] = None
    ) -> pd.DataFrame:
        """Retrieve conflicts from the database."""
        con = duckdb.connect(self.db_path)
        
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
        con.close()
        
        return df
    
    def get_input_events(self, device_id: Optional[str] = None) -> pd.DataFrame:
        """Retrieve stored input events."""
        con = duckdb.connect(self.db_path)
        
        if device_id:
            df = con.execute(
                "SELECT * FROM input_events WHERE device_id = ? ORDER BY timestamp",
                [device_id]
            ).df()
        else:
            df = con.execute("SELECT * FROM input_events ORDER BY timestamp").df()
        
        con.close()
        return df
    
    def clear_run_data(self, run_number: Optional[int] = None) -> None:
        """Clear data for a specific run or all runs."""
        con = duckdb.connect(self.db_path)
        
        if run_number is not None:
            con.execute("DELETE FROM events WHERE run_number = ?", [run_number])
            con.execute("DELETE FROM conflicts WHERE run_number = ?", [run_number])
        else:
            con.execute("DELETE FROM events")
            con.execute("DELETE FROM conflicts")
        
        con.close()


class DataCollector:
    """
    Collects data from controllers and checks for conflicts.
    
    Runs as a separate process, polling controllers at regular intervals.
    """
    
    def __init__(
        self,
        db_path: str,
        device_configs: Dict[str, Tuple[Tuple[str, int], List[Tuple[str, str]]]],
        collection_interval_minutes: float = 5.0,
        stop_on_conflict: bool = False,
        debug: bool = False
    ):
        """
        Initialize the data collector.
        
        Args:
            db_path: Path to DuckDB database
            device_configs: Dict mapping device_id to (ip_port, incompatible_pairs)
            collection_interval_minutes: How often to collect data
            stop_on_conflict: Whether to signal stop when conflict detected
            debug: Enable debug output
        """
        self.db_path = db_path
        self.device_configs = device_configs
        self.collection_interval = collection_interval_minutes * 60  # Convert to seconds
        self.stop_on_conflict = stop_on_conflict
        self.debug = debug
        
        self._stop_event = None
        self._conflict_event = None
        self._run_number = None
        self._simulation_start: Optional[datetime] = None
    
    def collect_once(
        self,
        run_number: int,
        simulation_start_time: datetime,
        check_from_time: Optional[datetime] = None
    ) -> List[ConflictRecord]:
        """
        Perform a single collection cycle for all devices.
        
        Args:
            run_number: Current simulation run number
            simulation_start_time: Start time of simulation
            check_from_time: Check conflicts from this time (defaults to 5 min before now)
        
        Returns:
            List of any conflicts detected
        """
        db = DatabaseManager(self.db_path)
        conflicts = []
        
        if check_from_time is None:
            check_from_time = datetime.now() - timedelta(minutes=5)
        
        for device_id, (ip_port, incompatible_pairs) in self.device_configs.items():
            try:
                # Fetch data from controller
                df = fetch_output_data(ip_port)
                
                if self.debug:
                    print(f"[Collector] Fetched {len(df)} events from {device_id}")
                
                # Insert into database
                rows = db.insert_events(df, device_id, run_number, simulation_start_time)
                
                if self.debug:
                    print(f"[Collector] Inserted {rows} events for {device_id}")
                
                # Check for conflicts in recent window
                if incompatible_pairs:
                    recent_df = df[df['TimeStamp'] >= check_from_time]
                    conflict_df = check_conflicts(recent_df, incompatible_pairs)
                    
                    for _, row in conflict_df.iterrows():
                        conflict = ConflictRecord(
                            device_id=device_id,
                            run_number=run_number,
                            timestamp=row['TimeStamp'],
                            conflict_details=row['Conflict_Details']
                        )
                        db.insert_conflict(conflict)
                        conflicts.append(conflict)
                        
                        if self.debug:
                            print(f"[Collector] Conflict detected at {device_id}: "
                                  f"{row['Conflict_Details']}")
            
            except Exception as e:
                print(f"[Collector] Error collecting from {device_id}: {e}")
        
        return conflicts
    
    def run_collection_loop(
        self,
        run_number: int,
        simulation_start_time: datetime,
        stop_event: threading.Event,
        conflict_callback: Optional[callable] = None
    ) -> None:
        """
        Run the collection loop until stop_event is set.
        
        Args:
            run_number: Current simulation run number
            simulation_start_time: Start time of simulation
            stop_event: Event to signal when to stop
            conflict_callback: Optional callback when conflict detected
        """
        last_check_time = simulation_start_time
        
        while not stop_event.is_set():
            # Wait for collection interval
            stop_event.wait(timeout=self.collection_interval)
            
            if stop_event.is_set():
                break
            
            # Collect data
            conflicts = self.collect_once(
                run_number,
                simulation_start_time,
                check_from_time=last_check_time
            )
            last_check_time = datetime.now()
            
            # Handle conflicts
            if conflicts and self.stop_on_conflict:
                if conflict_callback:
                    conflict_callback(conflicts)
                break
        
        # Final collection when stopping
        self.collect_once(run_number, simulation_start_time, check_from_time=last_check_time)
