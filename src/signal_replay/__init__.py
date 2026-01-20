"""
Signal Replay - Python package for replaying high-resolution event logs
from ATC signal controllers back to test controllers using NTCIP.
"""

from .config import SignalConfig, SimulationConfig
from .orchestrator import ATCSimulation, quick_run
from .replay import SignalReplay, create_replays
from .collector import (
    DatabaseManager,
    DataCollector,
    ConflictRecord,
    fetch_output_data,
    check_conflicts,
)
from .comparison import (
    COMPARISON_EVENT_IDS,
    DTWResult,
    DivergenceWindow,
    ComparisonResult,
    prepare_events_for_comparison,
    encode_categorical_sequence,
    compute_dtw,
    compare_runs,
    compare_all_runs,
    format_comparison_summary,
)
from .ntcip import send_ntcip, reset_all_detectors
from . import collector, comparison, config, ntcip, orchestrator, replay

__version__ = "0.1.0"
__all__ = [
    "ATCSimulation",
    "SimulationConfig",
    "SignalConfig",
    "quick_run",
    "SignalReplay",
    "create_replays",
    "DatabaseManager",
    "DataCollector",
    "ConflictRecord",
    "fetch_output_data",
    "check_conflicts",
    "COMPARISON_EVENT_IDS",
    "DTWResult",
    "DivergenceWindow",
    "ComparisonResult",
    "prepare_events_for_comparison",
    "encode_categorical_sequence",
    "compute_dtw",
    "compare_runs",
    "compare_all_runs",
    "format_comparison_summary",
    "send_ntcip",
    "reset_all_detectors",
    "collector",
    "comparison",
    "config",
    "ntcip",
    "orchestrator",
    "replay",
]
