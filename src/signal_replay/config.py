"""
Configuration classes for ATC Signal Replay.
"""

from dataclasses import dataclass, field
from typing import Union, List, Tuple, Optional
from pathlib import Path
import pandas as pd

try:
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    pa = None

# Sentinel value to indicate "use default"
_USE_DEFAULT = object()


@dataclass
class SignalConfig:
    """
    Configuration for a single signal/intersection.
    
    Attributes:
        device_id: Unique identifier for the signal device
        ip: IP address for NTCIP/SNMP communication
        events: Hi-res event data as Arrow table, pandas DataFrame, or file path (str/Path)
        udp_port: UDP port for SNMP communication. Required for localhost, defaults to 161 for remote hosts.
        cycle_length: Cycle length in seconds for coordinated signals (0 = disabled, default)
        incompatible_pairs: List of phase/overlap pairs that should never be active together. Optional - if not provided, conflict checking is disabled.
        cycle_offset: Offset in seconds within the cycle to start playback (0 = cycle boundary)
        limit_minutes: Limit input events to the last N minutes (0 = no limit)
        buffer_minutes: Include additional buffer minutes before the last N minutes (0 = no buffer)
        http_port: Port for HTTP data collection. Defaults to udp_port for localhost, 80 for remote hosts. Use None to disable.
    """
    device_id: str
    ip: str
    events: Union[pd.DataFrame, str, Path]  # Also accepts pyarrow.Table at runtime
    udp_port: Optional[int] = None
    cycle_length: int = 0
    incompatible_pairs: Optional[List[Tuple[str, str]]] = None
    cycle_offset: float = 0.0
    limit_minutes: float = 0.0
    buffer_minutes: float = 0.0
    http_port: Optional[int] = field(default_factory=lambda: _USE_DEFAULT)
    
    def __post_init__(self):
        # Detect if this is localhost
        is_localhost = self.ip.lower() in ('127.0.0.1', 'localhost')
        
        # Validate/default udp_port
        if self.udp_port is None:
            if is_localhost:
                raise ValueError(
                    f"udp_port is required when ip is localhost/127.0.0.1. "
                    f"For emulators, specify the port configured in the emulator settings."
                )
            else:
                self.udp_port = 161  # Default SNMP port for remote hosts
        
        # Validate udp_port range
        if not isinstance(self.udp_port, int) or self.udp_port < 1 or self.udp_port > 65535:
            raise ValueError(f"udp_port must be an integer between 1 and 65535, got {self.udp_port}")
        
        # Validate IP address
        if not isinstance(self.ip, str) or not self.ip:
            raise ValueError(f"ip must be a non-empty string, got {self.ip}")
        
        # Handle http_port defaulting
        # If http_port is the sentinel value, apply defaults based on IP
        if self.http_port is _USE_DEFAULT:
            if is_localhost:
                self.http_port = self.udp_port  # Match UDP port for localhost
            else:
                self.http_port = 80  # Default HTTP port for remote hosts
        # If user explicitly passed None, keep it as None (disables HTTP collection)
        # If user passed an int, keep that value
        
        # Validate cycle_length
        if not isinstance(self.cycle_length, int) or self.cycle_length < 0:
            raise ValueError(f"cycle_length must be a non-negative integer, got {self.cycle_length}")

        # Validate cycle_offset
        if not isinstance(self.cycle_offset, (int, float)) or self.cycle_offset < 0:
            raise ValueError(f"cycle_offset must be a non-negative number, got {self.cycle_offset}")
        
        # Normalize and validate incompatible_pairs
        # None means no conflict checking (convert to empty list for internal use)
        if self.incompatible_pairs is None:
            self.incompatible_pairs = []
        elif not isinstance(self.incompatible_pairs, list):
            raise ValueError(f"incompatible_pairs must be a list or None, got {type(self.incompatible_pairs)}")
        else:
            for pair in self.incompatible_pairs:
                if not isinstance(pair, tuple) or len(pair) != 2:
                    raise ValueError(f"Each incompatible pair must be a tuple of 2 strings, got {pair}")

        # Validate limit_minutes and buffer_minutes
        if not isinstance(self.limit_minutes, (int, float)) or self.limit_minutes < 0:
            raise ValueError(f"limit_minutes must be a non-negative number, got {self.limit_minutes}")
        if not isinstance(self.buffer_minutes, (int, float)) or self.buffer_minutes < 0:
            raise ValueError(f"buffer_minutes must be a non-negative number, got {self.buffer_minutes}")
        
        # Validate events type
        valid_types = [pd.DataFrame, str, Path]
        if HAS_PYARROW:
            valid_types.append(pa.Table)
        
        if not any(isinstance(self.events, t) for t in valid_types):
            raise ValueError(
                f"events must be a pandas DataFrame, Arrow Table, or file path, "
                f"got {type(self.events)}"
            )
    
    @property
    def ip_port(self) -> Tuple[str, int]:
        """Get (ip, udp_port) tuple for SNMP communication."""
        return (self.ip, self.udp_port)


@dataclass
class SimulationConfig:
    """
    Global configuration for the simulation.
    
    Attributes:
        signals: List of SignalConfig objects for each signal to simulate
        simulation_replays: Number of times to replay the simulation
        stop_on_conflict: If True, stop simulation when a conflict is detected
        db_path: Path to DuckDB database file (defaults to ./atc_replay.duckdb)
        controller_type: Type of controller (currently only "MAXTIME" supported)
        simulation_speed: Speed multiplier for the simulation (1.0 = real-time)
        collection_interval_minutes: How often to collect data from controllers (default: 5)
    """
    signals: List[SignalConfig]
    simulation_replays: int = 1
    stop_on_conflict: bool = False
    db_path: str = "./atc_replay.duckdb"
    controller_type: str = "MAXTIME"
    simulation_speed: float = 1.0
    collection_interval_minutes: float = 5.0
    
    def __post_init__(self):
        # Validate signals list
        if not self.signals or not isinstance(self.signals, list):
            raise ValueError("signals must be a non-empty list of SignalConfig objects")
        
        for i, signal in enumerate(self.signals):
            if not isinstance(signal, SignalConfig):
                raise ValueError(f"signals[{i}] must be a SignalConfig object, got {type(signal)}")
        
        # Validate all non-zero cycle lengths match (0 means disabled/uncoordinated)
        non_zero_lengths = set(s.cycle_length for s in self.signals if s.cycle_length > 0)
        if len(non_zero_lengths) > 1:
            raise ValueError(
                f"All signals with coordination enabled must have the same cycle_length. "
                f"Found different values: {non_zero_lengths}"
            )
        
        # Validate simulation_replays
        if not isinstance(self.simulation_replays, int) or self.simulation_replays < 1:
            raise ValueError(f"simulation_replays must be a positive integer, got {self.simulation_replays}")
        
        # Validate controller_type
        if self.controller_type != "MAXTIME":
            raise ValueError(f"controller_type must be 'MAXTIME', got {self.controller_type}")
        
        # Validate simulation_speed
        if not isinstance(self.simulation_speed, (int, float)) or self.simulation_speed <= 0:
            raise ValueError(f"simulation_speed must be a positive number, got {self.simulation_speed}")
        
        # Validate collection_interval_minutes
        if not isinstance(self.collection_interval_minutes, (int, float)) or self.collection_interval_minutes <= 0:
            raise ValueError(f"collection_interval_minutes must be a positive number, got {self.collection_interval_minutes}")
        
        # Validate db_path
        if not isinstance(self.db_path, str):
            raise ValueError(f"db_path must be a string, got {type(self.db_path)}")
        
        # Validate unique device_ids
        device_ids = [s.device_id for s in self.signals]
        if len(device_ids) != len(set(device_ids)):
            raise ValueError(f"All device_ids must be unique. Found duplicates in: {device_ids}")
    
    @property
    def cycle_length(self) -> int:
        """Get the common cycle length for all signals."""
        return self.signals[0].cycle_length if self.signals else 0
    
    @property
    def device_mapping(self) -> dict:
        """Get a mapping of device_id to ip_port for all signals."""
        return {s.device_id: s.ip_port for s in self.signals}
