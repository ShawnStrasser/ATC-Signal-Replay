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


@dataclass
class SignalConfig:
    """
    Configuration for a single signal/intersection.
    
    Attributes:
        device_id: Unique identifier for the signal device
        ip_port: Tuple of (IP address, port) for NTCIP communication
        cycle_length: Cycle length in seconds for coordinated signals (0 = disabled)
        cycle_offset: Offset in seconds within the cycle to start playback (0 = cycle boundary)
        incompatible_pairs: List of phase/overlap pairs that should never be active together
        events: Hi-res event data as Arrow table, pandas DataFrame, or file path (str/Path)
        limit_minutes: Limit input events to the last N minutes (0 = no limit)
        buffer_minutes: Include additional buffer minutes before the last N minutes (0 = no buffer)
    """
    device_id: str
    ip_port: Tuple[str, int]
    cycle_length: int
    incompatible_pairs: List[Tuple[str, str]]
    events: Union[pd.DataFrame, str, Path]  # Also accepts pyarrow.Table at runtime
    cycle_offset: float = 0.0
    limit_minutes: float = 0.0
    buffer_minutes: float = 0.0
    
    def __post_init__(self):
        # Validate ip_port format
        if not isinstance(self.ip_port, tuple) or len(self.ip_port) != 2:
            raise ValueError(f"ip_port must be a tuple of (host, port), got {self.ip_port}")
        
        host, port = self.ip_port
        if not isinstance(host, str):
            raise ValueError(f"IP address must be a string, got {type(host)}")
        if not isinstance(port, int) or port < 1 or port > 65535:
            raise ValueError(f"Port must be an integer between 1 and 65535, got {port}")
        
        # Validate cycle_length
        if not isinstance(self.cycle_length, int) or self.cycle_length < 0:
            raise ValueError(f"cycle_length must be a non-negative integer, got {self.cycle_length}")

        # Validate cycle_offset
        if not isinstance(self.cycle_offset, (int, float)) or self.cycle_offset < 0:
            raise ValueError(f"cycle_offset must be a non-negative number, got {self.cycle_offset}")
        
        # Validate incompatible_pairs
        if not isinstance(self.incompatible_pairs, list):
            raise ValueError(f"incompatible_pairs must be a list, got {type(self.incompatible_pairs)}")
        
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
        
        # Validate all cycle lengths match
        cycle_lengths = set(s.cycle_length for s in self.signals)
        if len(cycle_lengths) > 1:
            raise ValueError(
                f"All signals must have the same cycle_length. "
                f"Found different values: {cycle_lengths}"
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
