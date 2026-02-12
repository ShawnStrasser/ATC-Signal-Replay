"""
Signal Replay - Python package for replaying high-resolution event logs
from ATC signal controllers back to test controllers using NTCIP.
"""

from .config import SignalConfig, SimulationConfig
from .orchestrator import ATCSimulation
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
    DEFAULT_SEQUENCE_THRESHOLD,
    DEFAULT_TIMING_THRESHOLD,
    DTWResult,
    DivergenceWindow,
    ComparisonResult,
    ComparisonThresholds,
    ComparisonAnalysis,
    prepare_events_for_comparison,
    encode_categorical_sequence,
    compute_dtw,
    compare_runs,
    compare_all_runs,
    compare_event_sequences,
    compare_and_visualize,
    format_comparison_summary,
    load_events,
    generate_timeline,
    create_comparison_gantt_matplotlib,
    store_comparison_result,
    find_alignment_offset,
    calculate_timeline_offset,
    compute_timeline_offset,
)
from .ntcip import send_ntcip, reset_all_detectors
from . import collector, comparison, config, ntcip, orchestrator, replay

__version__ = "1.0.0"
__all__ = [
    "ATCSimulation",
    "SimulationConfig",
    "SignalConfig",
    "SignalReplay",
    "create_replays",
    "DatabaseManager",
    "DataCollector",
    "ConflictRecord",
    "fetch_output_data",
    "check_conflicts",
    "COMPARISON_EVENT_IDS",
    "DEFAULT_SEQUENCE_THRESHOLD",
    "DEFAULT_TIMING_THRESHOLD",
    "DTWResult",
    "DivergenceWindow",
    "ComparisonResult",
    "ComparisonThresholds",
    "ComparisonAnalysis",
    "prepare_events_for_comparison",
    "encode_categorical_sequence",
    "compute_dtw",
    "compare_runs",
    "compare_all_runs",
    "compare_event_sequences",
    "compare_and_visualize",
    "format_comparison_summary",
    "load_events",
    "generate_timeline",
    "create_comparison_gantt_matplotlib",
    "store_comparison_result",
    "send_ntcip",
    "reset_all_detectors",
    "collector",
    "comparison",
    "config",
    "ntcip",
    "orchestrator",
    "replay",
]
