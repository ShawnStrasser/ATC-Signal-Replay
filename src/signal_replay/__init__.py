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
    generate_phase_difference_summary,
    format_phase_differences,
    create_comparison_gantt_matplotlib,
    create_multi_divergence_plots,
    store_comparison_result,
    find_alignment_offset,
    calculate_timeline_offset,
    compute_timeline_offset,
)
from .test_suite import (
    TestType,
    TestScenario,
    TestBatch,
    FirmwareTestSuite,
    ScenarioResult,
    save_to_yaml,
    load_from_yaml,
)
from .batch_runner import BatchRunner, compare_firmware
from .report import generate_report, load_annotations
from .ntcip import send_ntcip, reset_all_detectors, async_send_ntcip, async_reset_all_detectors
from . import collector, comparison, config, ntcip, orchestrator, replay, test_suite, batch_runner, report

__version__ = "0.0.0"
__all__ = [
    # Core simulation
    "ATCSimulation",
    "SimulationConfig",
    "SignalConfig",
    "SignalReplay",
    "create_replays",
    # Data collection
    "DatabaseManager",
    "DataCollector",
    "ConflictRecord",
    "fetch_output_data",
    "check_conflicts",
    # Comparison — types and constants
    "COMPARISON_EVENT_IDS",
    "DEFAULT_SEQUENCE_THRESHOLD",
    "DEFAULT_TIMING_THRESHOLD",
    "DTWResult",
    "DivergenceWindow",
    "ComparisonResult",
    "ComparisonThresholds",
    "ComparisonAnalysis",
    # Comparison — public functions
    "prepare_events_for_comparison",
    "compare_runs",
    "compare_all_runs",
    "compare_event_sequences",
    "compare_and_visualize",
    "format_comparison_summary",
    "generate_timeline",
    "generate_phase_difference_summary",
    "format_phase_differences",
    # Firmware validation
    "TestType",
    "TestScenario",
    "TestBatch",
    "FirmwareTestSuite",
    "ScenarioResult",
    "save_to_yaml",
    "load_from_yaml",
    "BatchRunner",
    "compare_firmware",
    "generate_report",
    "load_annotations",
    # NTCIP / SNMP
    "send_ntcip",
    "reset_all_detectors",
    "async_send_ntcip",
    "async_reset_all_detectors",
]
