"""
Data collector: polls controller output events, stores them in DuckDB and
checks them for conflicts. The MAXTIME HTTP reader lives here
(:func:`fetch_output_data`); other sources plug in through
:mod:`signal_replay.events`.
"""

import gc
import duckdb
import logging
import os
import pandas as pd
import warnings
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Callable, Dict, Iterable, List, Mapping, Tuple, Optional, Any, Set
from pathlib import Path
import time
from dataclasses import dataclass
import threading

from ._logging import debug_level
from ._serialize import known_fields, parse_datetime, to_jsonable
from .progress import ProgressCallback, Stage, as_reporter
from .events import (
    CollectionTarget,
    EventSourceError,
    _FetchAbandoned,
    _SourceAdapter,
    _wait_for_stop,
    from_local_naive,
    normalize_device_id,
    normalize_output_events,
    split_fetch_result,
    to_local_naive,
)

logger = logging.getLogger(__name__)

_CONTROLLER_TIMESTAMP_FORMAT = "%m-%d-%Y %H:%M:%S.%f"
_CONTROLLER_TIMESTAMP_FORMAT_NO_FRACTION = "%m-%d-%Y %H:%M:%S"

# Each poll asks for events from this long before the newest stored event.
_WATERMARK_OVERLAP_SECONDS = 10
# The first poll of a collector asks for events from this long before the run.
_FIRST_POLL_BUFFER_SECONDS = 60

#: Version of the DuckDB layout written by :class:`DatabaseManager`. Stored in
#: the ``meta`` table as ``schema_version``. 2 = simulation_runs keyed by
#: (device_id, run_number), meta table, conflict episode columns.
SCHEMA_VERSION = 2

#: Values of ``simulation_runs.status``.
RUN_STATUSES = ('running', 'completed', 'incomplete', 'cancelled', 'failed')
# Statuses that count as done for resume (the replay itself finished).
_DONE_STATUSES = ('completed', 'incomplete')
# Worst-first order used when one status must summarise several devices.
_STATUS_PRIORITY = ('running', 'failed', 'cancelled', 'incomplete', 'completed')


def _log_memory(label: str = "") -> None:
    """Log current process RSS memory usage at DEBUG level.

    psutil is an optional dependency; the helper does nothing without it.
    """
    if not logger.isEnabledFor(logging.DEBUG):
        return
    try:
        import psutil
    except ImportError:
        return
    proc = psutil.Process(os.getpid())
    rss_mb = proc.memory_info().rss / (1024 * 1024)
    logger.debug("Memory RSS: %.1f MB %s", rss_mb, label)


def _get_sql_template(filename: str) -> str:
    """Load a SQL template from the package's sql directory."""
    sql_dir = Path(__file__).parent / "sql"
    with open(sql_dir / filename, 'r') as f:
        return f.read()


def _parse_controller_timestamps(values: pd.Series) -> pd.Series:
    """Parse MAXTIME controller timestamps without relying on pandas inference."""
    raw = values.astype("string")
    parsed = pd.to_datetime(
        raw,
        format=_CONTROLLER_TIMESTAMP_FORMAT,
        errors="coerce",
    )

    # Expected controller output includes fractional seconds, normally tenths.
    # Keep an exact-second fallback for logs that omit the trailing ".0".
    missing = parsed.isna() & raw.notna()
    if missing.any():
        parsed_no_fraction = pd.to_datetime(
            raw[missing],
            format=_CONTROLLER_TIMESTAMP_FORMAT_NO_FRACTION,
            errors="coerce",
        )
        parsed.loc[missing] = parsed_no_fraction

    bad = parsed.isna() & raw.notna()
    if bad.any():
        samples = raw[bad].head(5).tolist()
        raise ValueError(
            "Could not parse controller timestamps with known formats. "
            f"Sample raw values: {samples}"
        )

    return parsed


@dataclass
class ConflictRecord:
    """One conflict signature found in one device's run.

    Attributes:
        device_id: Device (scenario) the conflict was found on.
        run_number: Run it was found in.
        timestamp: First time the conflict state was seen (controller time,
            corrected by the signal's clock offset). Also available as
            :attr:`first_timestamp`.
        conflict_details: Signature, e.g. ``"Ph2 & Ph6"`` (pairs joined by
            ``"; "`` when several are active together).
        last_timestamp: When the last occurrence of the state ended (or was
            last seen, if the log ends while it is active).
        occurrences: Separate times the state was entered during the run.
        duration_seconds: Total time spent in the state.
        source_equivalent_timestamp: :attr:`timestamp` mapped back to the
            source log's clock using the replay's date shift / start offset
            (the moment in the original incident this corresponds to), or
            None when the mapping is unknown.
        stored: False when writing the record to the database failed.
    """
    device_id: str
    run_number: int
    timestamp: datetime
    conflict_details: str
    last_timestamp: Optional[datetime] = None
    occurrences: int = 1
    duration_seconds: Optional[float] = None
    source_equivalent_timestamp: Optional[datetime] = None
    stored: bool = True

    _TIMESTAMP_FIELDS = ('timestamp', 'last_timestamp', 'source_equivalent_timestamp')

    @property
    def first_timestamp(self) -> datetime:
        """Alias of :attr:`timestamp`."""
        return self.timestamp

    @property
    def pairs(self) -> List[Tuple[str, str]]:
        """The conflicting pairs in :attr:`conflict_details` as tuples."""
        result = []
        for part in str(self.conflict_details).split(';'):
            names = [n.strip() for n in part.split('&')]
            if len(names) == 2 and all(names):
                result.append((names[0], names[1]))
        return result

    def to_dict(self) -> Dict[str, Any]:
        """JSON-safe dict (timestamps as ISO-8601 strings)."""
        return to_jsonable(self, drop_keys=('_TIMESTAMP_FIELDS',))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ConflictRecord":
        kwargs = known_fields(cls, data)
        if 'timestamp' not in kwargs and 'first_timestamp' in data:
            kwargs['timestamp'] = data['first_timestamp']
        for name in cls._TIMESTAMP_FIELDS:
            kwargs[name] = parse_datetime(kwargs.get(name))
        return cls(**kwargs)

    def as_legacy_dict(self) -> Dict[str, Any]:
        """The 0.x ``results['conflicts']`` entry, plus the new fields."""
        return {
            'device_id': self.device_id,
            'run_number': self.run_number,
            'timestamp': self.timestamp,
            'conflict_details': self.conflict_details,
            'last_timestamp': self.last_timestamp,
            'occurrences': self.occurrences,
            'duration_seconds': self.duration_seconds,
            'source_equivalent_timestamp': self.source_equivalent_timestamp,
        }


def fetch_output_data(
    ip: str,
    http_port: int = 80,
    since: Optional[datetime] = None,
    request_timeout_seconds: float = 30,
    connect_timeout_seconds: float = 5,
) -> pd.DataFrame:
    """
    Fetch the event log from a MAXTIME controller.
    
    Args:
        ip: IP address of the controller
        http_port: HTTP port for data collection (default: 80)
        since: If provided, only fetch events after this timestamp.
            The controller supports a ``?since=`` query parameter.
        request_timeout_seconds: HTTP read timeout in seconds.
        connect_timeout_seconds: HTTP connect timeout in seconds, so an
            unreachable controller fails fast.
    
    Returns:
        DataFrame with columns: TimeStamp, EventTypeID, Parameter
    """
    import requests

    url = f'http://{ip}:{http_port}/v1/asclog/xml/full'
    if since is not None:
        since_str = since.strftime('%m-%d-%Y %H:%M:%S.0')
        url = f'{url}?since={since_str}'
    
    response = requests.get(
        url, verify=False, timeout=(connect_timeout_seconds, request_timeout_seconds)
    )
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
    
    raw_timestamps = df['TimeStamp'].copy()
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            df['TimeStamp'] = _parse_controller_timestamps(raw_timestamps)
        for w in caught:
            samples = raw_timestamps.head(5).tolist()
            logger.warning(
                "Datetime warning from %s:%s (%d events): %s\n  Sample raw values: %s",
                ip, http_port, len(df), w.message, samples,
            )
    except Exception:
        samples = raw_timestamps.head(5).tolist()
        logger.warning(
            "Datetime parsing failed for %s:%s. Sample TimeStamp values: %s",
            ip, http_port, samples,
        )
        raise
    df['EventTypeID'] = df['EventTypeID'].astype(int)
    df['Parameter'] = df['Parameter'].astype(int)
    
    return df


#: Columns returned by :func:`check_conflicts`.
_CONFLICT_COLUMNS = ['TimeStamp', 'Conflict_Details', 'Last_TimeStamp', 'Occurrences', 'Duration_Seconds']


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
        DataFrame with one row per conflict signature: TimeStamp (first
        seen), Conflict_Details, Last_TimeStamp (end of the last
        occurrence), Occurrences and Duration_Seconds
    """
    if events_df.empty or not incompatible_pairs:
        return pd.DataFrame(columns=_CONFLICT_COLUMNS)

    con = duckdb.connect()
    try:
        con.register('Event', events_df)
        sql = _get_sql_template('load_conflict_events.sql')
        raw_data = con.sql(sql).df()
    finally:
        con.close()

    if raw_data.empty:
        return pd.DataFrame(columns=_CONFLICT_COLUMNS)
    
    conflict_df = _build_conflict_dataframe(raw_data, incompatible_pairs)
    return conflict_df


def _build_conflict_dataframe(
    raw_data: pd.DataFrame,
    incompatible_pairs: List[Tuple[str, str]],
) -> pd.DataFrame:
    """Detect conflicts from a full ordered event stream for one run."""
    if raw_data.empty:
        return pd.DataFrame(columns=_CONFLICT_COLUMNS)

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
        return pd.DataFrame(columns=_CONFLICT_COLUMNS)

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
    final_df = final_df.sort_values('TimeStamp', kind='stable').reset_index(drop=True)
    # Each snapshot holds until the next one; an episode is a run of
    # consecutive snapshots with the same signature.
    final_df['_Next'] = final_df['TimeStamp'].shift(-1)
    details = final_df['Conflict_Details']
    final_df['_Episode'] = (details != details.shift(1)).cumsum()
    conflict_df = final_df[final_df['Has_Conflict'] & (details != "")].copy()
    if conflict_df.empty:
        return pd.DataFrame(columns=_CONFLICT_COLUMNS)

    conflict_df['_End'] = conflict_df['_Next'].fillna(conflict_df['TimeStamp'])
    conflict_df['_Seconds'] = (conflict_df['_End'] - conflict_df['TimeStamp']).dt.total_seconds()

    # One row per distinct conflict signature in the run: first time seen,
    # end of the last occurrence, number of occurrences and total duration.
    summary = (
        conflict_df.groupby('Conflict_Details', sort=False)
        .agg(
            TimeStamp=('TimeStamp', 'min'),
            Last_TimeStamp=('_End', 'max'),
            Occurrences=('_Episode', 'nunique'),
            Duration_Seconds=('_Seconds', 'sum'),
        )
        .reset_index()
        .sort_values('TimeStamp')
        .reset_index(drop=True)
    )
    summary['Occurrences'] = summary['Occurrences'].astype(int)
    return summary[_CONFLICT_COLUMNS]


class DatabaseManager:
    """DuckDB store for one replay working database.

    Every method opens its own connection and closes it before returning
    (in ``finally``), so the file is never held open between calls. The
    database is private to the run that writes it: do not point it at an
    application's own database, and do not open it from elsewhere while a
    run is writing it (DuckDB refuses a read-only connection from the same
    process while a read-write one is open).

    Tables (schema version :data:`SCHEMA_VERSION`):

    * ``events(device_id, run_number, timestamp, event_id, parameter)``:
      collected output events, primary key on all five columns.
    * ``conflicts(device_id, run_number, timestamp, conflict_details,
      last_timestamp, occurrences, duration_seconds,
      source_equivalent_timestamp, run_uuid)``: one row per conflict
      signature per device and run.
    * ``input_events`` / ``input_detector_events(device_id, timestamp,
      event_id, parameter)``: source events used for comparison and
      latency calibration.
    * ``latency_offset_updates`` / ``latency_offset_samples``: adaptive
      latency calibration history.
    * ``simulation_runs(device_id, run_number, status, started_at,
      completed_at, run_uuid, replay_start, replay_end, source_start,
      source_end, date_shift_seconds, events_sent, events_total)``, primary
      key ``(device_id, run_number)``; ``status`` is one of
      :data:`RUN_STATUSES`.
    * ``meta(key, value)``: ``schema_version``, ``package_version``,
      ``created_at`` and the latest ``run_uuid``.

    Args:
        db_path: Path to the DuckDB file.
        read_only: Open every connection read-only and skip schema creation
            and migration (for reading a finished run).
    """

    # Retry settings for DuckDB file lock issues on network shares
    _MAX_RETRIES = 5
    _BASE_DELAY = 2.0  # seconds

    def __init__(self, db_path: str, read_only: bool = False):
        self.db_path = str(db_path)
        self.read_only = bool(read_only)
        if not self.read_only:
            self._init_database()

    def _connect_with_retry(self, read_only: Optional[bool] = None) -> duckdb.DuckDBPyConnection:
        """Open a DuckDB connection with retry logic for file-lock errors on network shares.

        A read-only request falls back to a normal connection when this
        process already has the file open read-write (DuckDB refuses to mix
        the two configurations in one process).
        """
        read_only = self.read_only if read_only is None else read_only
        last_err = None
        for attempt in range(1, self._MAX_RETRIES + 1):
            try:
                if read_only:
                    try:
                        return duckdb.connect(self.db_path, read_only=True)
                    except duckdb.ConnectionException as exc:
                        if "different configuration" not in str(exc).lower():
                            raise
                        return duckdb.connect(self.db_path)
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

    def _read(self) -> duckdb.DuckDBPyConnection:
        """Connection for a read: read-only when this manager is read-only."""
        con = self._connect_with_retry(read_only=self.read_only)
        if self.read_only:
            try:
                self._shadow_legacy_simulation_runs(con)
            except Exception:
                con.close()
                raise
        return con

    @staticmethod
    def _shadow_legacy_simulation_runs(con: duckdb.DuckDBPyConnection) -> None:
        """Present a 0.x ``simulation_runs`` table in the per-device layout.

        A read-only manager cannot migrate the file, so a temporary view with
        the same rows the migration would write (see
        :meth:`_init_simulation_runs`) hides the old table for this
        connection.
        """
        tables = {
            row[0].lower()
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        if "simulation_runs" not in tables:
            return
        columns = {str(row[1]).lower() for row in con.execute("PRAGMA table_info('simulation_runs')").fetchall()}
        if "device_id" in columns:
            return
        catalog = str(con.execute("SELECT current_database()").fetchone()[0]).replace('"', '""')
        sources = [
            f'SELECT DISTINCT device_id, run_number FROM "{catalog}".main.{name}'
            for name in ("events", "conflicts") if name in tables
        ]
        devices_sql = " UNION ".join(sources) if sources else (
            "SELECT CAST(NULL AS VARCHAR) AS device_id, CAST(NULL AS INTEGER) AS run_number"
        )
        con.execute(
            f"""
            CREATE TEMP VIEW simulation_runs AS
            SELECT COALESCE(d.device_id, '') AS device_id, r.run_number, r.status,
                   r.started_at, r.completed_at
            FROM "{catalog}".main.simulation_runs r
            LEFT JOIN ({devices_sql}) d ON d.run_number = r.run_number
            WHERE r.run_number IS NOT NULL
            """
        )

    @staticmethod
    def _ensure_columns(
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        columns: Dict[str, str],
    ) -> None:
        """Add missing columns for pre-release DuckDB schemas."""
        existing = {
            row[1]
            for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        }
        for column_name, column_type in columns.items():
            if column_name not in existing:
                con.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
                )

    #: Columns of ``simulation_runs`` besides the (device_id, run_number) key.
    _RUN_COLUMNS = {
        "status": "VARCHAR",
        "started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "run_uuid": "VARCHAR",
        "replay_start": "TIMESTAMP",
        "replay_end": "TIMESTAMP",
        "source_start": "TIMESTAMP",
        "source_end": "TIMESTAMP",
        "date_shift_seconds": "DOUBLE",
        "events_sent": "BIGINT",
        "events_total": "BIGINT",
    }

    #: Conflict columns added in schema version 2.
    _CONFLICT_EXTRA_COLUMNS = {
        "last_timestamp": "TIMESTAMP",
        "occurrences": "INTEGER",
        "duration_seconds": "DOUBLE",
        "source_equivalent_timestamp": "TIMESTAMP",
        "run_uuid": "VARCHAR",
    }

    def _init_database(self) -> None:
        """Initialize database tables if they don't exist."""
        con = self._connect_with_retry(read_only=False)
        try:
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

            info = con.execute("PRAGMA table_info('events')").fetchall()
            expected_columns = ["device_id", "run_number", "timestamp", "event_id", "parameter"]
            actual_columns = [row[1] for row in info]
            pk_markers = [row[5] for row in info]
            if all(type(marker) is bool for marker in pk_markers):
                valid_pk = all(pk_markers)
            else:
                valid_pk = [int(marker) for marker in pk_markers] == [1, 2, 3, 4, 5]

            valid_schema = actual_columns == expected_columns and valid_pk
            if not valid_schema:
                raise RuntimeError(
                    f"Unsupported events schema in {self.db_path}. "
                    "Delete the existing DuckDB and rerun with the current pre-release schema."
                )

            # Conflicts table
            con.execute("""
                CREATE TABLE IF NOT EXISTS conflicts (
                    device_id VARCHAR,
                    run_number INTEGER,
                    timestamp TIMESTAMP,
                    conflict_details VARCHAR
                )
            """)
            self._ensure_columns(con, "conflicts", self._CONFLICT_EXTRA_COLUMNS)

            # Input events table (for comparison)
            con.execute("""
                CREATE TABLE IF NOT EXISTS input_events (
                    device_id VARCHAR,
                    timestamp TIMESTAMP,
                    event_id INTEGER,
                    parameter INTEGER
                )
            """)

            # Detector input reference table used by adaptive latency calibration.
            con.execute("""
                CREATE TABLE IF NOT EXISTS input_detector_events (
                    device_id VARCHAR,
                    timestamp TIMESTAMP,
                    event_id INTEGER,
                    parameter INTEGER
                )
            """)

            latency_update_columns = {
                "run_number": "INTEGER",
                "update_id": "VARCHAR",
                "device_id": "VARCHAR",
                "updated_at": "TIMESTAMP",
                "window_start": "TIMESTAMP",
                "window_end": "TIMESTAMP",
                "device_count": "INTEGER",
                "sample_count": "INTEGER",
                "required_min_samples": "INTEGER",
                "previous_offset_seconds": "DOUBLE",
                "measured_median_latency_seconds": "DOUBLE",
                "target_offset_seconds": "DOUBLE",
                "final_offset_seconds": "DOUBLE",
                "transition_start": "TIMESTAMP",
                "transition_end": "TIMESTAMP",
                "applied": "BOOLEAN",
                "status": "VARCHAR",
                "reason": "VARCHAR",
                "latency_p05_seconds": "DOUBLE",
                "latency_p25_seconds": "DOUBLE",
                "latency_p50_seconds": "DOUBLE",
                "latency_p75_seconds": "DOUBLE",
                "latency_p95_seconds": "DOUBLE",
            }
            con.execute(
                "CREATE TABLE IF NOT EXISTS latency_offset_updates ("
                + ", ".join(
                    f"{column_name} {column_type}"
                    for column_name, column_type in latency_update_columns.items()
                )
                + ")"
            )
            self._ensure_columns(con, "latency_offset_updates", latency_update_columns)

            latency_sample_columns = {
                "run_number": "INTEGER",
                "update_id": "VARCHAR",
                "device_id": "VARCHAR",
                "updated_at": "TIMESTAMP",
                "source_timestamp": "TIMESTAMP",
                "collected_timestamp": "TIMESTAMP",
                "event_id": "INTEGER",
                "parameter": "INTEGER",
                "previous_offset_seconds": "DOUBLE",
                "residual_seconds": "DOUBLE",
                "measured_latency_seconds": "DOUBLE",
                "match_delta_seconds": "DOUBLE",
            }
            con.execute(
                "CREATE TABLE IF NOT EXISTS latency_offset_samples ("
                + ", ".join(
                    f"{column_name} {column_type}"
                    for column_name, column_type in latency_sample_columns.items()
                )
                + ")"
            )
            self._ensure_columns(con, "latency_offset_samples", latency_sample_columns)

            self._init_simulation_runs(con)

            con.execute("CREATE TABLE IF NOT EXISTS meta (key VARCHAR PRIMARY KEY, value VARCHAR)")
            from . import __version__ as package_version
            now = datetime.now().isoformat(timespec="seconds")
            con.execute(
                "INSERT INTO meta (key, value) VALUES ('created_at', ?) ON CONFLICT DO NOTHING",
                [now],
            )
            for key, value in (("schema_version", str(SCHEMA_VERSION)), ("package_version", package_version)):
                con.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", [key, value]
                )
        finally:
            con.close()

    def _init_simulation_runs(self, con: duckdb.DuckDBPyConnection) -> None:
        """Create ``simulation_runs`` keyed by (device_id, run_number); migrate the 0.x table.

        The 0.x table had one row per run number. Each old row is copied to
        every device that has events (or conflicts) for that run number, or
        to device ``''`` when there are none, inside one transaction.
        """
        column_sql = ", ".join(f"{name} {kind}" for name, kind in self._RUN_COLUMNS.items())
        create_sql = (
            "CREATE TABLE {name} (device_id VARCHAR NOT NULL, run_number INTEGER NOT NULL, "
            + column_sql
            + ", PRIMARY KEY (device_id, run_number))"
        )
        tables = {
            row[0].lower()
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        if "simulation_runs" not in tables:
            con.execute(create_sql.format(name="simulation_runs"))
            return

        columns = {
            row[1] for row in con.execute("PRAGMA table_info('simulation_runs')").fetchall()
        }
        if "device_id" in columns:
            self._ensure_columns(con, "simulation_runs", self._RUN_COLUMNS)
            return

        logger.info("Migrating simulation_runs in %s to one row per device", self.db_path)
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute("DROP TABLE IF EXISTS simulation_runs_v2")
            con.execute(create_sql.format(name="simulation_runs_v2"))
            con.execute(
                """
                INSERT INTO simulation_runs_v2 (device_id, run_number, status, started_at, completed_at)
                SELECT COALESCE(d.device_id, ''), r.run_number, r.status, r.started_at, r.completed_at
                FROM simulation_runs r
                LEFT JOIN (
                    SELECT DISTINCT device_id, run_number FROM events
                    UNION
                    SELECT DISTINCT device_id, run_number FROM conflicts
                ) d ON d.run_number = r.run_number
                WHERE r.run_number IS NOT NULL
                """
            )
            con.execute("DROP TABLE simulation_runs")
            con.execute("ALTER TABLE simulation_runs_v2 RENAME TO simulation_runs")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    # -- meta ----------------------------------------------------------------

    def set_meta(self, key: str, value: Any) -> None:
        """Write one ``meta`` row (for example the current ``run_uuid``)."""
        con = self._connect_with_retry(read_only=False)
        try:
            con.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                [str(key), None if value is None else str(value)],
            )
        finally:
            con.close()

    def get_meta(self) -> Dict[str, Optional[str]]:
        """All ``meta`` rows as a dict (empty for a 0.x database)."""
        con = self._read()
        try:
            try:
                rows = con.execute("SELECT key, value FROM meta").fetchall()
            except duckdb.CatalogException:
                return {}
            return {row[0]: row[1] for row in rows}
        finally:
            con.close()

    # -- run status -----------------------------------------------------------

    @staticmethod
    def _device_list(device_ids: Optional[List[str]]) -> Optional[List[str]]:
        if device_ids is None:
            return None
        return [str(d) for d in device_ids]

    def _done_runs_for_device(self, con: duckdb.DuckDBPyConnection, device_id: str) -> Set[int]:
        """Run numbers that count as done for one device.

        From ``simulation_runs`` (status completed or incomplete). A device
        with no ``simulation_runs`` rows at all (data written by a 0.x
        version) falls back to the runs that have stored events, minus runs
        a legacy row marks cancelled or failed.
        """
        rows = con.execute(
            "SELECT run_number, status FROM simulation_runs WHERE device_id = ?", [device_id]
        ).fetchall()
        if rows:
            return {int(r[0]) for r in rows if r[1] in _DONE_STATUSES}
        legacy = con.execute(
            """
            SELECT DISTINCT run_number FROM (
                SELECT run_number FROM events WHERE device_id = ?
                UNION
                SELECT run_number FROM conflicts WHERE device_id = ?
            )
            WHERE run_number IS NOT NULL
              AND run_number NOT IN (
                  SELECT run_number FROM simulation_runs
                  WHERE device_id = '' AND status NOT IN ('completed', 'incomplete')
              )
            """,
            [device_id, device_id],
        ).fetchall()
        return {int(r[0]) for r in legacy}

    def get_done_runs_by_device(self, device_ids: List[str]) -> Dict[str, List[int]]:
        """Per device, the run numbers that are done (completed or incomplete), ascending."""
        con = self._read()
        try:
            return {
                str(d): sorted(self._done_runs_for_device(con, str(d)))
                for d in self._device_list(device_ids) or []
            }
        finally:
            con.close()

    def get_completed_run_numbers(self, device_ids: Optional[List[str]] = None) -> List[int]:
        """Run numbers that are done, ascending.

        With ``device_ids``: runs done for every one of those devices (status
        completed or incomplete in ``simulation_runs``). Without: runs whose
        every ``simulation_runs`` row is completed or incomplete.
        """
        devices = self._device_list(device_ids)
        con = self._read()
        try:
            if devices:
                done: Optional[Set[int]] = None
                for device_id in devices:
                    runs = self._done_runs_for_device(con, device_id)
                    done = runs if done is None else done & runs
                return sorted(done or set())

            rows = con.execute(
                """
                SELECT run_number
                FROM simulation_runs
                GROUP BY run_number
                HAVING bool_and(status IN ('completed', 'incomplete'))
                ORDER BY run_number
                """
            ).fetchall()
            return [int(row[0]) for row in rows]
        finally:
            con.close()

    def get_max_run_number(self, device_ids: Optional[List[str]] = None) -> int:
        """Highest run number that is done (see :meth:`get_completed_run_numbers`).

        A resumed simulation runs every run number up to its target that is
        not done for some device (a cancelled, failed or interrupted
        ('running') run, including one below this number), see
        :meth:`get_done_runs_by_device`.
        """
        try:
            completed_runs = self.get_completed_run_numbers(device_ids=device_ids)
        except Exception:
            logger.warning("Could not read completed runs from %s", self.db_path, exc_info=True)
            return 0
        return completed_runs[-1] if completed_runs else 0

    def mark_run_started(
        self,
        run_number: int,
        device_ids: Optional[List[str]] = None,
        run_uuid: Optional[str] = None,
    ) -> None:
        """Record a run as 'running' for each device so an interrupted run is resumed.

        Without ``device_ids`` the run is recorded under device ``''`` (the
        0.x behaviour, one row per run number).
        """
        devices = self._device_list(device_ids) or ['']
        now = datetime.now()
        con = self._connect_with_retry(read_only=False)
        try:
            for device_id in devices:
                con.execute(
                    "DELETE FROM simulation_runs WHERE device_id = ? AND run_number = ?",
                    [device_id, run_number],
                )
                con.execute(
                    """
                    INSERT INTO simulation_runs (device_id, run_number, status, started_at, completed_at, run_uuid)
                    VALUES (?, ?, 'running', ?, NULL, ?)
                    """,
                    [device_id, run_number, now, run_uuid],
                )
        finally:
            con.close()

    def mark_run_completed(self, run_number: int, device_ids: Optional[List[str]] = None) -> None:
        """Mark a run as completed."""
        self._mark_run_finished(run_number, 'completed', device_ids)

    def mark_run_incomplete(self, run_number: int, device_ids: Optional[List[str]] = None) -> None:
        """Mark a run as finished but with output events not confirmed complete.

        Resume treats it as done (the replay ran); results flag it.
        """
        self._mark_run_finished(run_number, 'incomplete', device_ids)

    def mark_run_cancelled(self, run_number: int, device_ids: Optional[List[str]] = None) -> None:
        """Mark a run as cancelled so resume runs it again."""
        self._mark_run_finished(run_number, 'cancelled', device_ids)

    def mark_run_failed(self, run_number: int, device_ids: Optional[List[str]] = None) -> None:
        """Mark a run as failed (for example, data collection failed)."""
        self._mark_run_finished(run_number, 'failed', device_ids)

    def get_run_status(self, run_number: int, device_id: Optional[str] = None) -> Optional[str]:
        """Return the ``simulation_runs`` status of a run, or None if unknown.

        Without ``device_id``, the least finished status over all devices of
        that run (running, then failed, cancelled, incomplete, completed).
        """
        con = self._read()
        try:
            if device_id is not None:
                row = con.execute(
                    "SELECT status FROM simulation_runs WHERE device_id = ? AND run_number = ?",
                    [str(device_id), run_number],
                ).fetchone()
                return row[0] if row else None
            statuses = [
                row[0]
                for row in con.execute(
                    "SELECT status FROM simulation_runs WHERE run_number = ?", [run_number]
                ).fetchall()
            ]
        finally:
            con.close()
        if not statuses:
            return None
        for status in _STATUS_PRIORITY:
            if status in statuses:
                return status
        return statuses[0]

    def get_runs(self, device_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """``simulation_runs`` rows (optionally for some devices), ordered by device and run."""
        devices = self._device_list(device_ids)
        con = self._read()
        try:
            if devices:
                placeholders = ",".join(["?"] * len(devices))
                return con.execute(
                    f"SELECT * FROM simulation_runs WHERE device_id IN ({placeholders}) "
                    "ORDER BY device_id, run_number",
                    devices,
                ).df()
            return con.execute("SELECT * FROM simulation_runs ORDER BY device_id, run_number").df()
        finally:
            con.close()

    def update_run_details(self, run_number: int, device_id: str, **details: Any) -> None:
        """Store replay timing for one device's run (columns of ``simulation_runs``).

        Accepted keys: ``run_uuid``, ``replay_start``, ``replay_end``,
        ``source_start``, ``source_end``, ``date_shift_seconds``,
        ``events_sent``, ``events_total``. Unknown keys are ignored.
        """
        allowed = {k: v for k, v in details.items() if k in self._RUN_COLUMNS and k not in ('status', 'started_at', 'completed_at')}
        if not allowed:
            return
        assignments = ", ".join(f"{name} = ?" for name in allowed)
        con = self._connect_with_retry(read_only=False)
        try:
            con.execute(
                f"UPDATE simulation_runs SET {assignments} WHERE device_id = ? AND run_number = ?",
                list(allowed.values()) + [str(device_id), run_number],
            )
        finally:
            con.close()

    def _mark_run_finished(
        self,
        run_number: int,
        status: str,
        device_ids: Optional[List[str]] = None,
    ) -> None:
        devices = self._device_list(device_ids)
        now = datetime.now()
        con = self._connect_with_retry(read_only=False)
        try:
            if devices is None:
                # 0.x form: every row of this run number, or a new '' row.
                existing = [
                    row[0]
                    for row in con.execute(
                        "SELECT device_id FROM simulation_runs WHERE run_number = ?", [run_number]
                    ).fetchall()
                ]
                devices = existing or ['']
            for device_id in devices:
                updated = con.execute(
                    """
                    UPDATE simulation_runs SET status = ?, completed_at = ?
                    WHERE device_id = ? AND run_number = ?
                    RETURNING run_number
                    """,
                    [status, now, device_id, run_number],
                ).fetchall()
                if not updated:
                    con.execute(
                        """
                        INSERT INTO simulation_runs (device_id, run_number, status, started_at, completed_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        [device_id, run_number, status, now, now],
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
    
    def count_events(self, device_id: str, run_number: int) -> int:
        """Number of stored output events for one device and run."""
        con = self._read()
        try:
            return int(con.execute(
                "SELECT COUNT(*) FROM events WHERE device_id = ? AND run_number = ?",
                [device_id, run_number],
            ).fetchone()[0])
        finally:
            con.close()

    def insert_conflict(self, conflict: ConflictRecord, run_uuid: Optional[str] = None) -> None:
        """Insert a conflict record, or update the stored one with the same signature.

        One row per (device, run, first timestamp, signature); a later
        detection of the same signature refreshes its last timestamp,
        occurrences and duration.
        """
        con = self._connect_with_retry(read_only=False)
        try:
            key = [conflict.device_id, conflict.run_number, conflict.timestamp, conflict.conflict_details]
            values = [
                conflict.last_timestamp,
                conflict.occurrences,
                conflict.duration_seconds,
                conflict.source_equivalent_timestamp,
                run_uuid,
            ]
            existing = con.execute("""
                SELECT COUNT(*) as count FROM conflicts
                WHERE device_id = ?
                AND run_number = ?
                AND timestamp = ?
                AND conflict_details = ?
            """, key).fetchone()

            if existing[0] == 0:
                con.execute("""
                    INSERT INTO conflicts (
                        device_id, run_number, timestamp, conflict_details,
                        last_timestamp, occurrences, duration_seconds,
                        source_equivalent_timestamp, run_uuid
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, key + values)
            else:
                con.execute("""
                    UPDATE conflicts SET
                        last_timestamp = ?, occurrences = ?, duration_seconds = ?,
                        source_equivalent_timestamp = ?, run_uuid = COALESCE(?, run_uuid)
                    WHERE device_id = ? AND run_number = ? AND timestamp = ? AND conflict_details = ?
                """, values + key)
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

    def insert_input_detector_events(self, df: pd.DataFrame, device_id: str) -> None:
        """Store source detector input events for adaptive latency calibration."""
        df = df.copy()
        if not df.empty:
            df['device_id'] = device_id

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
            con.execute("DELETE FROM input_detector_events WHERE device_id = ?", [device_id])
            if not df.empty:
                con.register('input_detector_df', df)
                con.execute("""
                    INSERT INTO input_detector_events
                    SELECT * FROM input_detector_df
                """)
        finally:
            con.close()

    def insert_latency_offset_update(
        self,
        run_number: int,
        update,
        samples: Optional[pd.DataFrame] = None,
    ) -> None:
        """Store one adaptive latency offset update attempt and its matched samples."""
        con = self._connect_with_retry()
        try:
            con.execute(
                """
                INSERT INTO latency_offset_updates (
                    run_number,
                    update_id,
                    device_id,
                    updated_at,
                    window_start,
                    window_end,
                    device_count,
                    sample_count,
                    required_min_samples,
                    previous_offset_seconds,
                    measured_median_latency_seconds,
                    target_offset_seconds,
                    final_offset_seconds,
                    transition_start,
                    transition_end,
                    applied,
                    status,
                    reason,
                    latency_p05_seconds,
                    latency_p25_seconds,
                    latency_p50_seconds,
                    latency_p75_seconds,
                    latency_p95_seconds
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_number,
                    getattr(update, "update_id", None),
                    getattr(update, "device_id", None),
                    update.updated_at,
                    update.window_start,
                    update.window_end,
                    update.device_count,
                    update.sample_count,
                    update.required_min_samples,
                    update.previous_offset_seconds,
                    update.measured_median_latency_seconds,
                    getattr(update, "target_offset_seconds", update.final_offset_seconds),
                    update.final_offset_seconds,
                    getattr(update, "transition_start", None),
                    getattr(update, "transition_end", None),
                    update.applied,
                    update.status,
                    update.reason,
                    getattr(update, "latency_p05_seconds", None),
                    getattr(update, "latency_p25_seconds", None),
                    getattr(update, "latency_p50_seconds", None),
                    getattr(update, "latency_p75_seconds", None),
                    getattr(update, "latency_p95_seconds", None),
                ],
            )
            if samples is not None and not samples.empty:
                sample_df = samples.copy()
                sample_df["run_number"] = run_number
                sample_df["update_id"] = getattr(update, "update_id", None)
                sample_df["device_id"] = getattr(update, "device_id", None)
                sample_df["updated_at"] = update.updated_at
                sample_df["previous_offset_seconds"] = update.previous_offset_seconds
                sample_df = sample_df.rename(
                    columns={"processing_latency_seconds": "measured_latency_seconds"}
                )
                sample_df = sample_df[
                    [
                        "run_number",
                        "update_id",
                        "device_id",
                        "updated_at",
                        "source_timestamp",
                        "collected_timestamp",
                        "event_id",
                        "parameter",
                        "previous_offset_seconds",
                        "residual_seconds",
                        "measured_latency_seconds",
                        "match_delta_seconds",
                    ]
                ]
                con.register("latency_samples_df", sample_df)
                con.execute(
                    """
                    INSERT INTO latency_offset_samples (
                        run_number,
                        update_id,
                        device_id,
                        updated_at,
                        source_timestamp,
                        collected_timestamp,
                        event_id,
                        parameter,
                        previous_offset_seconds,
                        residual_seconds,
                        measured_latency_seconds,
                        match_delta_seconds
                    )
                    SELECT
                        run_number,
                        update_id,
                        device_id,
                        updated_at,
                        source_timestamp,
                        collected_timestamp,
                        event_id,
                        parameter,
                        previous_offset_seconds,
                        residual_seconds,
                        measured_latency_seconds,
                        match_delta_seconds
                    FROM latency_samples_df
                    """
                )
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
        con = self._read()
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
        con = self._read()
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
        con = self._read()
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

    def get_input_detector_events(
        self,
        device_ids: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Retrieve source detector input events used for latency calibration."""
        con = self._read()
        try:
            if device_ids:
                placeholders = ",".join(["?"] * len(device_ids))
                df = con.execute(
                    f"""
                    SELECT * FROM input_detector_events
                    WHERE device_id IN ({placeholders})
                    ORDER BY device_id, timestamp, event_id, parameter
                    """,
                    device_ids,
                ).df()
            else:
                df = con.execute(
                    "SELECT * FROM input_detector_events ORDER BY device_id, timestamp, event_id, parameter"
                ).df()
        finally:
            con.close()
        return df
    
    def clear_run_data(
        self,
        run_number: Optional[int] = None,
        device_ids: Optional[List[str]] = None,
    ) -> None:
        """Clear data for a specific run or all runs, optionally scoped to devices."""
        if device_ids is not None and not device_ids:
            return

        con = self._connect_with_retry()
        try:
            if device_ids:
                placeholders = ",".join(["?"] * len(device_ids))
                params: List[object] = []
                where_clauses: List[str] = []
                if run_number is not None:
                    where_clauses.append("run_number = ?")
                    params.append(run_number)
                where_clauses.append(f"device_id IN ({placeholders})")
                params.extend(device_ids)
                where_sql = " WHERE " + " AND ".join(where_clauses)
                con.execute(f"DELETE FROM events{where_sql}", params)
                con.execute(f"DELETE FROM conflicts{where_sql}", params)
                con.execute(f"DELETE FROM latency_offset_updates{where_sql}", params)
                con.execute(f"DELETE FROM latency_offset_samples{where_sql}", params)
                con.execute(f"DELETE FROM simulation_runs{where_sql}", params)
            elif run_number is not None:
                con.execute("DELETE FROM events WHERE run_number = ?", [run_number])
                con.execute("DELETE FROM conflicts WHERE run_number = ?", [run_number])
                con.execute("DELETE FROM latency_offset_updates WHERE run_number = ?", [run_number])
                con.execute("DELETE FROM latency_offset_samples WHERE run_number = ?", [run_number])
                con.execute("DELETE FROM simulation_runs WHERE run_number = ?", [run_number])
            else:
                con.execute("DELETE FROM events")
                con.execute("DELETE FROM conflicts")
                con.execute("DELETE FROM latency_offset_updates")
                con.execute("DELETE FROM latency_offset_samples")
                con.execute("DELETE FROM simulation_runs")
        finally:
            con.close()

    def clear_device_data(self, device_ids: List[str]) -> None:
        """Clear all stored data for one or more device IDs."""
        self.clear_collected_device_data(device_ids)

        if not device_ids:
            return

        placeholders = ",".join(["?"] * len(device_ids))
        con = self._connect_with_retry()
        try:
            con.execute(f"DELETE FROM input_events WHERE device_id IN ({placeholders})", device_ids)
            con.execute(f"DELETE FROM input_detector_events WHERE device_id IN ({placeholders})", device_ids)
        finally:
            con.close()

    def clear_collected_device_data(self, device_ids: List[str]) -> None:
        """Clear collected output data for one or more device IDs."""
        if not device_ids:
            return

        placeholders = ",".join(["?"] * len(device_ids))
        con = self._connect_with_retry()
        try:
            con.execute(f"DELETE FROM events WHERE device_id IN ({placeholders})", device_ids)
            con.execute(f"DELETE FROM conflicts WHERE device_id IN ({placeholders})", device_ids)
            con.execute(f"DELETE FROM latency_offset_updates WHERE device_id IN ({placeholders})", device_ids)
            con.execute(f"DELETE FROM latency_offset_samples WHERE device_id IN ({placeholders})", device_ids)
            con.execute(f"DELETE FROM simulation_runs WHERE device_id IN ({placeholders})", device_ids)

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
    Collects controller output events, stores them and checks for conflicts.

    Events come from an output-event source (see :mod:`signal_replay.events`):
    the MAXTIME HTTP log by default, or any ``event_source`` the caller
    supplies. During a replay, :meth:`run_collection_loop` runs on a
    background thread and polls every ``collection_interval_minutes``. After
    the replay, :meth:`finalize_run` keeps polling until each source reports
    its data complete through the end of the replay (or a timeout), then
    runs conflict detection over the whole run.

    Each step is also usable on its own: :meth:`collect_once` polls every
    device once, :meth:`ingest` stores events the caller already has, and
    :meth:`detect_conflicts` checks one device's run.
    """

    def __init__(
        self,
        db_path: str,
        device_configs: Mapping[str, Any],
        collection_interval_minutes: float = 5.0,
        stop_on_conflict: bool = False,
        debug: bool = False,
        *,
        event_source: Any = None,
        incompatible_pairs: Optional[Mapping[str, List[Tuple[str, str]]]] = None,
        clock_offsets: Optional[Mapping[str, float]] = None,
        source_timezones: Optional[Mapping[str, Optional[str]]] = None,
        required_codes: Optional[Mapping[str, Mapping[str, Set[int]]]] = None,
        final_collection_timeout_seconds: float = 900.0,
        final_collection_poll_seconds: float = 20.0,
        source_timeout_seconds: Optional[float] = 120.0,
        max_consecutive_failures: int = 3,
        clock: Optional[Callable[[], datetime]] = None,
        sleep: Optional[Callable[[float], None]] = None,
        on_progress: Optional[ProgressCallback] = None,
        source_time: Optional[Callable[[str, int, datetime], Optional[datetime]]] = None,
        run_uuid: Optional[str] = None,
    ):
        """
        Initialize the data collector.

        Args:
            db_path: Path to DuckDB database
            device_configs: Dict mapping device_id to a
                :class:`~signal_replay.CollectionTarget`. The 0.x form
                ``(ip_port, incompatible_pairs, http_port)`` is still accepted.
            collection_interval_minutes: How often to poll during a replay
            stop_on_conflict: Kept for compatibility; the orchestrator decides
                whether a conflict stops the simulation
            debug: Log per-poll detail at INFO instead of DEBUG
            event_source: Output-event source; None uses the MAXTIME HTTP log
            incompatible_pairs: device_id -> pairs checked for conflicts
                (overrides pairs given in the legacy tuple form)
            clock_offsets: device_id -> seconds added to collected timestamps
            source_timezones: device_id -> timezone of naive source timestamps
                (overrides the source's own ``source_timezone``)
            required_codes: device_id -> {feature: codes}; a warning is logged
                once per run when a code never appears
            final_collection_timeout_seconds: Longest :meth:`finalize_run`
                waits for a source to report its data complete
            final_collection_poll_seconds: Poll interval during that wait
            source_timeout_seconds: A single source call taking longer than
                this counts as a failed poll (None = no limit)
            max_consecutive_failures: Failed polls in a row before a device is
                flagged ``degraded`` in ``collection_health``
            clock: Returns the current naive local time (tests inject a fake)
            sleep: Called with seconds to wait between final polls (tests
                inject a fake that advances ``clock``)
            on_progress: Optional callback receiving COLLECT (one per device
                per poll, with ``extra['rows']`` and the device's health),
                FINAL_COLLECTION and ERROR progress events. It runs on the
                collection thread.
            source_time: Optional ``(device_id, run_number, timestamp) ->
                datetime`` mapping a collected timestamp to the source log's
                clock; fills ``ConflictRecord.source_equivalent_timestamp``
            run_uuid: Stored with each conflict row
        """
        self.db_path = db_path
        self.collection_interval = collection_interval_minutes * 60  # Convert to seconds
        self.stop_on_conflict = stop_on_conflict
        self.debug = debug

        self.targets: Dict[str, CollectionTarget] = {}
        self.incompatible_pairs: Dict[str, List[Tuple[str, str]]] = {}
        for raw_id, config in device_configs.items():
            device_id = normalize_device_id(raw_id)
            if isinstance(config, CollectionTarget):
                target = config
                if target.device_id != device_id:
                    target = CollectionTarget(device_id, target.ip, target.http_port, target.extra)
                pairs: List[Tuple[str, str]] = []
            else:
                ip_port, pairs, http_port = config
                ip = ip_port[0] if isinstance(ip_port, (tuple, list)) else str(ip_port)
                target = CollectionTarget(device_id=device_id, ip=ip, http_port=http_port)
            self.targets[device_id] = target
            self.incompatible_pairs[device_id] = list(pairs or [])
        for raw_id, pairs in (incompatible_pairs or {}).items():
            self.incompatible_pairs[normalize_device_id(raw_id)] = list(pairs or [])

        self.source = _SourceAdapter(event_source)
        self.clock_offsets = {normalize_device_id(k): float(v or 0.0) for k, v in (clock_offsets or {}).items()}
        self.source_timezones = {normalize_device_id(k): v for k, v in (source_timezones or {}).items()}
        self.required_codes = {normalize_device_id(k): dict(v) for k, v in (required_codes or {}).items()}
        self.final_collection_timeout_seconds = float(final_collection_timeout_seconds)
        self.final_collection_poll_seconds = max(0.0, float(final_collection_poll_seconds))
        self.source_timeout_seconds = source_timeout_seconds
        self.max_consecutive_failures = max(1, int(max_consecutive_failures))
        self._clock = clock or datetime.now
        self._sleep = sleep
        self._progress = as_reporter(on_progress, log=logger)
        self._source_time = source_time
        self.run_uuid = run_uuid
        #: Messages for conflict rows that could not be written to the database.
        self.conflict_store_errors: List[str] = []

        self._db: Optional[DatabaseManager] = None
        self._lock = threading.RLock()
        self._last_seen_event_key: Dict[str, Tuple[pd.Timestamp, int, int]] = {}
        # When set, only these devices are collected (a resumed run that is
        # re-run for some devices only).
        self._device_filter: Optional[Set[str]] = None
        # Latest complete_through each source reported (canonical clock).
        self._reported_complete_through: Dict[str, datetime] = {}
        self._consecutive_insert_failures: Dict[str, int] = {}
        self._fetch_error_logged: Set[str] = set()
        self._health_run: Optional[int] = None
        self.collection_health: Dict[str, Dict[str, Any]] = {}
        self._codes_seen: Dict[str, Set[int]] = {}
        self._reports_completeness: Dict[str, bool] = {}

        disabled = [d for d, t in self.targets.items() if not self.source.enabled_for(t)]
        for device_id in disabled:
            logger.log(debug_level(self.debug), "Output collection disabled for %s (no http_port)", device_id)

    # -- bookkeeping -------------------------------------------------------

    @property
    def active_device_ids(self) -> List[str]:
        """Devices the source collects from (MAXTIME skips those without an http_port).

        Limited further by :meth:`limit_to_devices` while that is set.
        """
        devices = [d for d, t in self.targets.items() if self.source.enabled_for(t)]
        if self._device_filter is not None:
            devices = [d for d in devices if d in self._device_filter]
        return devices

    def limit_to_devices(self, device_ids: Optional[Iterable[Any]]) -> None:
        """Collect only from ``device_ids`` (None: every device) from the next run on."""
        with self._lock:
            self._device_filter = (
                None if device_ids is None else {normalize_device_id(d) for d in device_ids}
            )
            self._health_run = None

    @staticmethod
    def _new_health() -> Dict[str, Any]:
        return {
            "polls": 0,
            "failures": 0,
            "consecutive_failures": 0,
            "rows": 0,
            "first_timestamp": None,
            "last_timestamp": None,
            "last_success": None,
            "last_error": None,
            "complete_through": None,
            "degraded": False,
        }

    def _ensure_run(self, run_number: int) -> None:
        """Reset per-run health and code tracking when a new run starts."""
        with self._lock:
            if self._health_run == run_number:
                return
            self._health_run = run_number
            self.collection_health = {d: self._new_health() for d in self.active_device_ids}
            self._codes_seen = {d: set() for d in self.active_device_ids}
            self._reports_completeness = {}
            self._fetch_error_logged.clear()

    def _health(self, device_id: str) -> Dict[str, Any]:
        return self.collection_health.setdefault(device_id, self._new_health())

    def complete_through_snapshot(self) -> Dict[str, datetime]:
        """Per device, the time through which stored output events are complete.

        Only devices with a successful poll in the current run are included.
        """
        with self._lock:
            return {
                d: h["complete_through"]
                for d, h in self.collection_health.items()
                if h.get("complete_through") is not None
            }

    def health_snapshot(self) -> Dict[str, Dict[str, Any]]:
        """Copy of ``collection_health`` for the current run.

        Per device: ``polls``, ``failures``, ``consecutive_failures``,
        ``rows`` (rows stored), ``first_timestamp``/``last_timestamp`` (of
        stored rows), ``last_success`` (PC time), ``last_error``,
        ``complete_through`` and ``degraded``.
        """
        with self._lock:
            return {d: dict(h) for d, h in self.collection_health.items()}

    def _db_manager(self) -> "DatabaseManager":
        if self._db is None:
            self._db = DatabaseManager(self.db_path)
        return self._db

    def _source_timezone(self, device_id: str) -> Optional[str]:
        tz_name = self.source_timezones.get(device_id)
        return tz_name if tz_name is not None else self.source.source_timezone

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

    def _report_poll(
        self,
        device_id: str,
        run_number: int,
        rows: int,
        *,
        error: Optional[str] = None,
        rows_written: Optional[int] = None,
    ) -> None:
        """COLLECT progress event for one device's poll (health in ``extra``).

        ``rows`` is the number of new distinct rows stored by this poll;
        ``rows_written`` counts re-delivered rows that replaced themselves too.
        """
        with self._lock:
            health = dict(self._health(device_id))
        extra = dict(health)
        extra["total_rows"] = health.get("rows", 0)
        extra["rows"] = int(rows)
        extra["rows_written"] = int(rows if rows_written is None else rows_written)
        if error is not None:
            extra["error"] = error
            message = f"[{device_id}] Output event poll failed: {error}"
        else:
            message = f"[{device_id}] Collected {int(rows)} new output events"
        self._progress.emit(
            Stage.COLLECT,
            message,
            level=logging.WARNING if error is not None else logging.INFO,
            device_id=device_id,
            run_number=run_number,
            log=False,
            extra=extra,
        )

    # -- fetch / ingest / detect ------------------------------------------

    def _since_for(self, device_id: str, simulation_start_time: datetime) -> Optional[datetime]:
        """The ``since`` hint for the next poll, in the source's own clock convention.

        It trails the newest stored event by a small overlap. When the source
        reported its data complete only up to an earlier time (rows from a
        file still being written, or an unordered source), it trails that
        ``complete_through`` instead, so late rows in the gap are asked for
        again. It never goes earlier than the first-poll hint for the run.
        """
        floor = simulation_start_time - timedelta(seconds=_FIRST_POLL_BUFFER_SECONDS)
        last_key = self._last_seen_event_key.get(device_id)
        if last_key is not None:
            since = (last_key[0] - pd.Timedelta(seconds=_WATERMARK_OVERLAP_SECONDS)).to_pydatetime()
            reported_through = self._reported_complete_through.get(device_id)
            if reported_through is not None:
                since = min(since, reported_through - timedelta(seconds=_WATERMARK_OVERLAP_SECONDS))
            since = max(since, floor)
        else:
            since = floor
        return from_local_naive(
            since,
            source_timezone=self._source_timezone(device_id),
            clock_offset_seconds=self.clock_offsets.get(device_id, 0.0),
        )

    def fetch(
        self,
        device_id: str,
        since: Optional[datetime],
        *,
        stop: Any = None,
    ) -> Tuple[pd.DataFrame, datetime]:
        """Poll the source once for one device.

        Returns the normalised events and the time through which the data
        is complete (canonical clock). Any source failure is counted in
        ``collection_health`` and re-raised as :class:`EventSourceError`.
        """
        device_id = normalize_device_id(device_id)
        target = self.targets[device_id]
        called_at = self._clock()
        with self._lock:
            health = self._health(device_id)
            health["polls"] += 1
        try:
            raw = self.source.fetch(target, since, stop=stop, timeout=self.source_timeout_seconds)
            events, complete_through, reported = split_fetch_result(raw)
            tz_name = self._source_timezone(device_id)
            offset = self.clock_offsets.get(device_id, 0.0)
            df = normalize_output_events(
                events,
                device_id=target.source_device_id,
                source_timezone=tz_name,
                clock_offset_seconds=offset,
            )
            if reported:
                complete_through = to_local_naive(
                    complete_through, source_timezone=tz_name, clock_offset_seconds=offset
                )
            else:
                complete_through = called_at
        except _FetchAbandoned:
            with self._lock:
                health["polls"] -= 1
            raise
        except Exception as exc:
            error = exc if isinstance(exc, EventSourceError) else EventSourceError(f"{type(exc).__name__}: {exc}")
            with self._lock:
                health["failures"] += 1
                health["consecutive_failures"] += 1
                health["last_error"] = str(error)
                if health["consecutive_failures"] >= self.max_consecutive_failures and not health["degraded"]:
                    health["degraded"] = True
                    logger.warning(
                        "*** COLLECTION WARNING: %d consecutive failed polls for %s (%s); last error: %s",
                        health["consecutive_failures"], device_id, self.source.name, error,
                    )
            if error is exc:
                raise
            raise error from exc
        with self._lock:
            if reported:
                self._reports_completeness[device_id] = True
                previous = self._reported_complete_through.get(device_id)
                if previous is None or complete_through > previous:
                    self._reported_complete_through[device_id] = complete_through
        return df, complete_through

    def ingest(
        self,
        device_id: str,
        events: Any,
        run_number: int,
        simulation_start_time: datetime,
        *,
        normalized: bool = False,
        until: Optional[datetime] = None,
    ) -> int:
        """Store output events for one device and run.

        The push path: tests, offline re-processing, or a caller that already
        holds events can call this directly. ``events`` follows the schema
        in :mod:`signal_replay.events` (DataFrame or list of dicts); the
        signal's clock offset and source timezone are applied unless
        ``normalized`` is True. Rows before ``simulation_start_time`` are
        dropped, and so are rows after ``until`` when it is given (the final
        collection passes the end of the replay plus the settle time, so a
        file that runs past the run does not add free-running controller
        events to it); re-delivered rows are de-duplicated.

        Returns:
            Number of rows written.

        Raises:
            Exception: Whatever the database raised (the caller decides
                whether it is fatal).
        """
        device_id = normalize_device_id(device_id)
        self._ensure_run(run_number)
        if normalized:
            df = events
        else:
            target = self.targets.get(device_id)
            df = normalize_output_events(
                events,
                device_id=target.source_device_id if target is not None else device_id,
                source_timezone=self._source_timezone(device_id),
                clock_offset_seconds=self.clock_offsets.get(device_id, 0.0),
            )
        if df.empty:
            return 0

        with self._lock:
            last_key = self._last_seen_event_key.get(device_id)
            if self.source.ordered:
                df = self._new_rows_since_watermark(df, last_key)
            else:
                df = df.sort_values(['TimeStamp', 'EventTypeID', 'Parameter'])
            if df.empty:
                return 0

            to_store = df
            if until is not None:
                to_store = df[df['TimeStamp'] <= pd.Timestamp(until)]
            db = self._db_manager()
            rows = db.insert_events(to_store, device_id, run_number, simulation_start_time) if not to_store.empty else 0

            # Advance the watermark only after a successful insert.
            last_row = df.iloc[-1]
            new_key = (
                pd.Timestamp(last_row['TimeStamp']),
                int(last_row['EventTypeID']),
                int(last_row['Parameter']),
            )
            if last_key is None or new_key > last_key:
                self._last_seen_event_key[device_id] = new_key

            kept = to_store[to_store['TimeStamp'] >= simulation_start_time]
            if not kept.empty:
                health = self._health(device_id)
                counter = getattr(db, "count_events", None)
                try:
                    # Distinct stored rows: re-delivered rows replace themselves.
                    health["rows"] = counter(device_id, run_number) if counter else health["rows"] + int(rows)
                except Exception:
                    health["rows"] += int(rows)
                first_ts = kept['TimeStamp'].min().to_pydatetime()
                last_ts = kept['TimeStamp'].max().to_pydatetime()
                if health["first_timestamp"] is None or first_ts < health["first_timestamp"]:
                    health["first_timestamp"] = first_ts
                if health["last_timestamp"] is None or last_ts > health["last_timestamp"]:
                    health["last_timestamp"] = last_ts
                self._codes_seen.setdefault(device_id, set()).update(
                    int(code) for code in kept['EventTypeID'].unique()
                )
        return int(rows)

    def detect_conflicts(
        self,
        device_id: str,
        run_number: int,
        conflict_callback: Optional[Callable[[List[ConflictRecord]], None]] = None,
    ) -> List[ConflictRecord]:
        """Check one device's stored events for a run against its incompatible pairs.

        New conflicts are written to the ``conflicts`` table and passed to
        ``conflict_callback``.
        """
        device_id = normalize_device_id(device_id)
        incompatible_pairs = self.incompatible_pairs.get(device_id) or []
        if not incompatible_pairs:
            return []
        log_level = debug_level(self.debug)
        db = self._db_manager()
        start = time.time()
        run_events = db.get_events(device_id=device_id, run_number=run_number)
        if not run_events.empty:
            run_events = run_events.rename(columns={
                'timestamp': 'TimeStamp',
                'event_id': 'EventTypeID',
                'parameter': 'Parameter',
            })

        conflicts = check_conflicts(run_events, incompatible_pairs)
        logger.log(
            log_level, "Conflict check for %s took %.2f seconds",
            device_id, time.time() - start,
        )
        if conflicts.empty:
            return []

        logger.warning("Conflicts found for %s!", device_id)
        conflict_records = []
        for _, row in conflicts.iterrows():
            first = pd.Timestamp(row['TimeStamp']).to_pydatetime()
            last = row.get('Last_TimeStamp')
            duration = row.get('Duration_Seconds')
            record = ConflictRecord(
                device_id=device_id,
                run_number=run_number,
                timestamp=first,
                conflict_details=row['Conflict_Details'],
                last_timestamp=None if last is None or pd.isna(last) else pd.Timestamp(last).to_pydatetime(),
                occurrences=int(row.get('Occurrences') or 1),
                duration_seconds=None if duration is None or pd.isna(duration) else float(duration),
            )
            if self._source_time is not None:
                try:
                    record.source_equivalent_timestamp = self._source_time(device_id, run_number, first)
                except Exception:
                    logger.debug("Source time mapping failed for %s", device_id, exc_info=True)
            conflict_records.append(record)

        for conflict in conflict_records:
            try:
                if self.run_uuid is not None:
                    db.insert_conflict(conflict, run_uuid=self.run_uuid)
                else:
                    db.insert_conflict(conflict)
            except Exception as e:
                conflict.stored = False
                message = (
                    f"{device_id} run {run_number}: could not store conflict "
                    f"'{conflict.conflict_details}' at {conflict.timestamp}: {type(e).__name__}: {e}"
                )
                self.conflict_store_errors.append(message)
                logger.warning("*** DB conflict insert failed: %s", message)

        if conflict_callback:
            conflict_callback(conflict_records)
        return conflict_records

    def _collect_device(
        self,
        device_id: str,
        run_number: int,
        simulation_start_time: datetime,
        *,
        stop: Any = None,
        error_event: Optional[threading.Event] = None,
        until: Optional[datetime] = None,
    ) -> Optional[datetime]:
        """Fetch and ingest one device. Returns complete_through, or None on failure.

        ``until`` drops rows after it (see :meth:`ingest`) for a source that
        reports ``complete_through``; it is ignored for plain sources.

        Raises _FetchAbandoned when ``stop`` is set, and RuntimeError after
        repeated database insert failures.
        """
        log_level = debug_level(self.debug)
        target = self.targets[device_id]
        since = self._since_for(device_id, simulation_start_time)
        try:
            df, complete_through = self.fetch(device_id, since, stop=stop)
        except EventSourceError as exc:
            # Fetch failures never abort the replay: the controller may still
            # take SNMP commands, and the source may recover on a later poll.
            if device_id not in self._fetch_error_logged:
                logger.warning(
                    "*** COLLECTION WARNING: Cannot collect output events for %s at %s (%s: %s). "
                    "Continuing replay without aborting.",
                    device_id, target.ip, self.source.name, exc,
                )
                self._fetch_error_logged.add(device_id)
            self._report_poll(device_id, run_number, 0, error=str(exc))
            return None
        self._fetch_error_logged.discard(device_id)

        if stop is not None and stop.is_set():
            raise _FetchAbandoned()

        rows = 0
        with self._lock:
            rows_before = int(self._health(device_id).get("rows") or 0)
            # The upper bound applies to sources that report complete_through
            # (whole files can run well past the run). A plain source is polled
            # once right after the settle time, so its short tail is kept.
            if not self._reports_completeness.get(device_id):
                until = None
        if df.empty:
            logger.log(log_level, "No data returned for %s", device_id)
        else:
            try:
                rows = self.ingest(
                    device_id, df, run_number, simulation_start_time, normalized=True, until=until,
                )
                self._consecutive_insert_failures[device_id] = 0
                logger.log(log_level, "Inserted %s events for %s", rows, device_id)
            except Exception as e:
                fail_count = self._consecutive_insert_failures.get(device_id, 0) + 1
                self._consecutive_insert_failures[device_id] = fail_count
                logger.warning(
                    "DB insert failed for %s (attempt %d): %s", device_id, fail_count, e
                )
                if fail_count >= 3:
                    if error_event is not None:
                        error_event.set()
                    raise RuntimeError(
                        f"Repeated DB insert failures for {device_id} ({fail_count} consecutive)"
                    ) from e
                self._report_poll(device_id, run_number, 0, error=f"database insert failed: {e}")
                return None

        with self._lock:
            health = self._health(device_id)
            health["consecutive_failures"] = 0
            health["last_error"] = None
            health["last_success"] = self._clock()
            health["complete_through"] = complete_through
            rows_after = int(health.get("rows") or 0)
        self._report_poll(device_id, run_number, max(0, rows_after - rows_before), rows_written=rows)
        return complete_through

    # -- polling -----------------------------------------------------------

    def run_collection_loop(
        self,
        run_number: int,
        simulation_start_time: datetime,
        stop_event: threading.Event,
        conflict_callback: Optional[callable] = None,
        error_event: Optional[threading.Event] = None,
        after_collect_callback: Optional[callable] = None,
        abort_event: Optional[threading.Event] = None,
        on_fatal_error: Optional[Callable[[BaseException], None]] = None,
    ) -> None:
        """
        Run continuous collection in a loop until stopped.

        Args:
            run_number: Current simulation run number
            simulation_start_time: Start time of simulation
            stop_event: Threading event to stop the loop
            conflict_callback: Optional callback when conflicts found
            error_event: Optional threading event set on fatal collection errors
            after_collect_callback: Optional callback run after each successful poll
            abort_event: Optional event; once set, a poll still in progress
                discards its results instead of writing them to the database
                (used when the caller has stopped waiting for this thread)
            on_fatal_error: Optional callback called with the exception when
                collection stops on a fatal error, so the caller can stop the replay
        """
        logger.log(debug_level(self.debug), "Starting collection loop for run %s", run_number)
        self._ensure_run(run_number)

        while not stop_event.is_set():
            try:
                self.collect_once(
                    run_number,
                    simulation_start_time,
                    conflict_callback=conflict_callback,
                    error_event=error_event,
                    abort_event=abort_event,
                )
                if (
                    after_collect_callback is not None
                    and not (abort_event is not None and abort_event.is_set())
                    and not stop_event.is_set()
                ):
                    after_collect_callback(datetime.now())
            except Exception as exc:
                self._progress.emit(
                    Stage.ERROR,
                    f"*** COLLECTION ERROR: {exc}. Stopping data collection for this run.",
                    level=logging.ERROR,
                    run_number=run_number,
                    extra={"error": str(exc), "source": "collection"},
                    log_to=logger,
                )
                if error_event is not None:
                    error_event.set()
                stop_event.set()
                if on_fatal_error is not None:
                    try:
                        on_fatal_error(exc)
                    except Exception:
                        logger.warning("Collection error callback failed", exc_info=True)
                return

            _log_memory(f"[after collect run={run_number}]")
            gc.collect()

            # Wake as soon as stop is requested.
            stop_event.wait(self.collection_interval)

        logger.log(debug_level(self.debug), "Collection loop stopped for run %s", run_number)

    def collect_once(
        self,
        run_number: int,
        simulation_start_time: datetime,
        detect_conflicts: bool = False,
        conflict_callback: Optional[callable] = None,
        error_event: Optional[threading.Event] = None,
        abort_event: Optional[Any] = None,
    ) -> Dict[str, Optional[datetime]]:
        """
        Poll every device once and store the new events.

        Args:
            run_number: Current simulation run number
            simulation_start_time: Start time of simulation
            detect_conflicts: Whether to check conflicts after storing events
            conflict_callback: Optional callback when conflicts found
            error_event: Optional event set when repeated DB insert failures occur
            abort_event: Optional event; when set, the poll stops and anything
                fetched but not yet written is discarded

        Returns:
            device_id -> complete_through for each device polled successfully
        """
        log_level = debug_level(self.debug)
        logger.log(log_level, "Collecting data for run %s...", run_number)
        self._ensure_run(run_number)

        def _aborted() -> bool:
            if abort_event is not None and abort_event.is_set():
                logger.log(log_level, "Collection for run %s abandoned; results discarded", run_number)
                return True
            return False

        complete: Dict[str, Optional[datetime]] = {}
        for device_id in self.active_device_ids:
            if _aborted():
                return complete
            try:
                result = self._collect_device(
                    device_id, run_number, simulation_start_time,
                    stop=abort_event, error_event=error_event,
                )
            except _FetchAbandoned:
                _aborted()
                return complete
            if result is not None:
                complete[device_id] = result

        if detect_conflicts:
            for device_id in self.active_device_ids:
                if _aborted():
                    return complete
                self.detect_conflicts(device_id, run_number, conflict_callback)
        return complete

    def _wait(self, seconds: float, stop: Any) -> bool:
        """Wait between final polls; True if stopped."""
        if self._sleep is not None:
            if stop is not None and stop.is_set():
                return True
            self._sleep(seconds)
            return stop is not None and stop.is_set()
        return _wait_for_stop(stop, seconds)

    def finalize_run(
        self,
        run_number: int,
        simulation_start_time: datetime,
        complete_through_target: datetime,
        *,
        conflict_callback: Optional[Callable[[List[ConflictRecord]], None]] = None,
        stop_event: Any = None,
        error_event: Optional[threading.Event] = None,
    ) -> Dict[str, Any]:
        """Final collection for a run, then conflict detection.

        Polls every device until its source reports data complete through
        ``complete_through_target`` (normally the end of the replay plus the
        settle time). For a source that reports ``complete_through``, rows
        after the target are not stored for the run, so a log delivered in
        whole files does not add the controller's free-running events after
        the replay to it. Sources that return plain events are complete as of
        each successful call, so one poll is usually enough for them; if
        such a source keeps failing, the device is given up after
        ``max_consecutive_failures`` attempts. Sources that report
        ``complete_through`` (for example a log written in 5-minute files)
        are polled every ``final_collection_poll_seconds`` for up to
        ``final_collection_timeout_seconds``.

        ``stop_event`` (anything with ``is_set()``) ends the wait promptly;
        conflict detection is then skipped.

        Returns:
            dict with ``status`` ('complete', 'incomplete' or 'stopped'),
            ``incomplete_devices``, ``waited_seconds`` and ``health``.
        """
        self._ensure_run(run_number)
        log_level = debug_level(self.debug)
        pending = set(self.active_device_ids)
        incomplete: List[str] = []
        plain_failures: Dict[str, int] = {}
        started = self._clock()
        deadline = started + timedelta(seconds=self.final_collection_timeout_seconds)

        def _final_event(message: str, waiting: bool, level: int = logging.INFO, **extra: Any) -> None:
            with self._lock:
                devices = {
                    d: self.collection_health.get(d, {}).get("complete_through")
                    for d in self.active_device_ids
                }
            self._progress.emit(
                Stage.FINAL_COLLECTION,
                message,
                level=level,
                run_number=run_number,
                log=False,
                extra={"waiting": waiting, "needed": complete_through_target, "devices": devices,
                       "pending": sorted(pending), **extra},
            )

        _final_event(
            f"Final collection for run {run_number}: waiting for output events through "
            f"{complete_through_target:%H:%M:%S}",
            True,
            seconds_left=self.final_collection_timeout_seconds,
        )

        def _stopped() -> Dict[str, Any]:
            logger.warning("Final collection for run %d stopped", run_number)
            _final_event(f"Final collection for run {run_number} stopped", False, status="stopped")
            return {
                "status": "stopped",
                "incomplete_devices": sorted(pending | set(incomplete)),
                "waited_seconds": (self._clock() - started).total_seconds(),
                "health": self.health_snapshot(),
            }

        while pending:
            for device_id in sorted(pending):
                if stop_event is not None and stop_event.is_set():
                    return _stopped()
                try:
                    complete_through = self._collect_device(
                        device_id, run_number, simulation_start_time,
                        stop=stop_event, error_event=error_event,
                        until=complete_through_target,
                    )
                except _FetchAbandoned:
                    return _stopped()
                if complete_through is not None and complete_through >= complete_through_target:
                    pending.discard(device_id)
                elif complete_through is None and not self._reports_completeness.get(device_id):
                    plain_failures[device_id] = plain_failures.get(device_id, 0) + 1
                    if plain_failures[device_id] >= self.max_consecutive_failures:
                        logger.warning(
                            "Final collection for %s failed %d times; giving up (run %d is incomplete)",
                            device_id, plain_failures[device_id], run_number,
                        )
                        pending.discard(device_id)
                        incomplete.append(device_id)
            if not pending:
                break

            now = self._clock()
            if now >= deadline:
                for device_id in sorted(pending):
                    logger.warning(
                        "*** Output events for %s are complete only through %s, not %s; "
                        "gave up after %.0fs. Run %d is incomplete.",
                        device_id, self.collection_health.get(device_id, {}).get("complete_through"),
                        complete_through_target, self.final_collection_timeout_seconds, run_number,
                    )
                incomplete.extend(sorted(pending))
                pending.clear()
                break

            remaining = (deadline - now).total_seconds()
            for device_id in sorted(pending):
                logger.info(
                    "Waiting for output events from %s: complete through %s, need %s (%.0fs left)",
                    device_id, self.collection_health.get(device_id, {}).get("complete_through"),
                    complete_through_target, remaining,
                )
            _final_event(
                f"Waiting for output events from {', '.join(sorted(pending))} "
                f"({remaining:.0f}s left)",
                True,
                seconds_left=remaining,
            )
            if self._wait(min(self.final_collection_poll_seconds, remaining), stop_event):
                return _stopped()

        self._warn_missing_codes(run_number)

        for device_id in self.active_device_ids:
            self.detect_conflicts(device_id, run_number, conflict_callback)

        waited = (self._clock() - started).total_seconds()
        _final_event(
            f"Final collection for run {run_number} "
            + ("INCOMPLETE for " + ", ".join(sorted(incomplete)) if incomplete else "complete"),
            False,
            level=logging.WARNING if incomplete else logging.INFO,
            status="incomplete" if incomplete else "complete",
            incomplete_devices=sorted(incomplete),
            waited_seconds=waited,
        )
        if incomplete:
            logger.warning(
                "Run %d output events incomplete for: %s. Conflict detection ran on the events received.",
                run_number, ", ".join(sorted(incomplete)),
            )
        else:
            logger.log(log_level, "Final collection for run %d complete after %.1fs", run_number, waited)
        return {
            "status": "incomplete" if incomplete else "complete",
            "incomplete_devices": sorted(incomplete),
            "waited_seconds": waited,
            "health": self.health_snapshot(),
        }

    def _warn_missing_codes(self, run_number: int) -> None:
        """Warn once per run when required event codes never appeared."""
        for device_id in self.active_device_ids:
            health = self.collection_health.get(device_id) or {}
            if not health.get("rows"):
                logger.warning(
                    "No output events were stored for %s in run %d (source: %s)",
                    device_id, run_number, self.source.name,
                )
                continue
            seen = self._codes_seen.get(device_id, set())
            for feature, codes in sorted(self.required_codes.get(device_id, {}).items()):
                missing = sorted(set(codes) - seen)
                if missing:
                    logger.warning(
                        "Output events for %s in run %d never included event code(s) %s, needed for %s. "
                        "Check that the controller logs them.",
                        device_id, run_number, ", ".join(str(c) for c in missing), feature,
                    )

    def close(self) -> None:
        """Release the event source adapter (the source's async loop outlives it)."""
        self.source.close()

    def stop(self) -> None:
        """Kept for compatibility; the collector holds no background process."""
        self.close()
