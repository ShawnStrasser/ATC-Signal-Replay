"""
Signal Replay - Python package for replaying high-resolution event logs
from ATC signal controllers back to test controllers using NTCIP.

The package logs through the standard ``logging`` module under the
``signal_replay`` logger and never configures handlers on import. Call
``signal_replay.enable_console_logging()`` in scripts and notebooks to see
progress output on the console.
"""

import logging as _logging_module

# Library logging convention: a NullHandler only. Applications decide where
# records go (console, file, GUI) through the standard logging configuration.
_logging_module.getLogger(__name__).addHandler(_logging_module.NullHandler())

# Single source of the package version. pyproject.toml reads it through
# [tool.setuptools.dynamic] version = {attr = "signal_replay.__version__"}.
__version__ = "1.0.0"

from ._logging import enable_console_logging, disable_console_logging, log_to_file
from .progress import Stage, ProgressEvent, ProgressCallback
from .config import (
    DEFAULT_REPLAY_LATENCY_OFFSET_SECONDS,
    SignalConfig,
    SimulationConfig,
)
from .orchestrator import ATCSimulation
from .replay import SignalReplay, create_replays
from .collector import (
    SCHEMA_VERSION,
    DatabaseManager,
    DataCollector,
    ConflictRecord,
    fetch_output_data,
    check_conflicts,
)
from .events import (
    OUTPUT_EVENT_COLUMNS,
    CollectionTarget,
    FetchResult,
    EventSourceError,
    OutputEventSource,
    EventSource,
    MaxtimeHttpEventSource,
    normalize_output_events,
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
    ChunkScore,
    prepare_events_for_comparison,
    compare_runs,
    compare_all_runs,
    compare_event_sequences,
    compare_and_visualize,
    format_comparison_summary,
    load_events,
    generate_timeline,
    cross_invalidate_timelines,
    generate_phase_difference_summary,
    generate_clearance_irregularity_summary,
    generate_operational_difference_summary,
    format_phase_differences,
    find_temporal_offset,
    build_included_event_periods,
    filter_divergence_windows_to_periods,
    clip_timeline_to_relative_periods,
    timeline_overlaps_interval,
    render_sparkline_svg,
)
from .test_suite import (
    TestType,
    TestScenario,
    TestBatch,
    SoftwareTestSuite,
    ScenarioResult,
)
from .latency import (
    AdaptiveLatencyOffsetManager,
    compute_latency_min_samples,
    match_sparse_event82_latency,
    sparse_detector_events,
)
from .batch_runner import (
    BatchRunner,
    DbLoadRequest,
    OperationCancelled,
    compare_software,
    console_db_loader,
)
from .results import FRAME_COLUMNS, ReplicationResult, RunRecord, results_to_frames
from .validation import (
    ValidationSettings,
    compare_validation,
    load_collected_events,
    load_coord_split_schedules,
)
from .workspace import read_manifest
from .report import generate_report, load_annotations
from .ntcip import send_ntcip, reset_all_detectors, async_send_ntcip, async_reset_all_detectors
from . import (
    batch_runner,
    collector,
    comparison,
    config,
    events,
    latency,
    ntcip,
    orchestrator,
    progress,
    replay,
    report,
    results,
    test_suite,
    validation,
    workspace,
)

__all__ = [
    # Logging helpers
    "enable_console_logging",
    "disable_console_logging",
    "log_to_file",
    # Progress reporting
    "Stage",
    "ProgressEvent",
    "ProgressCallback",
    # Core simulation
    "ATCSimulation",
    "SimulationConfig",
    "SignalConfig",
    "DEFAULT_REPLAY_LATENCY_OFFSET_SECONDS",
    "SignalReplay",
    "create_replays",
    # Results
    "ReplicationResult",
    "RunRecord",
    "results_to_frames",
    "FRAME_COLUMNS",
    "read_manifest",
    "SCHEMA_VERSION",
    # Data collection
    "DatabaseManager",
    "DataCollector",
    "ConflictRecord",
    "fetch_output_data",
    "check_conflicts",
    # Output-event sources
    "OUTPUT_EVENT_COLUMNS",
    "CollectionTarget",
    "FetchResult",
    "EventSourceError",
    "OutputEventSource",
    "EventSource",
    "MaxtimeHttpEventSource",
    "normalize_output_events",
    # Comparison — types and constants
    "COMPARISON_EVENT_IDS",
    "DEFAULT_SEQUENCE_THRESHOLD",
    "DEFAULT_TIMING_THRESHOLD",
    "DTWResult",
    "DivergenceWindow",
    "ComparisonResult",
    "ComparisonThresholds",
    "ComparisonAnalysis",
    "ChunkScore",
    # Comparison — public functions
    "prepare_events_for_comparison",
    "compare_runs",
    "compare_all_runs",
    "compare_event_sequences",
    "compare_and_visualize",
    "format_comparison_summary",
    "load_events",
    "generate_timeline",
    "cross_invalidate_timelines",
    "generate_phase_difference_summary",
    "generate_clearance_irregularity_summary",
    "generate_operational_difference_summary",
    "format_phase_differences",
    "find_temporal_offset",
    "build_included_event_periods",
    "filter_divergence_windows_to_periods",
    "clip_timeline_to_relative_periods",
    "timeline_overlaps_interval",
    "render_sparkline_svg",
    # Software validation
    "TestType",
    "TestScenario",
    "TestBatch",
    "SoftwareTestSuite",
    "ScenarioResult",
    "BatchRunner",
    "OperationCancelled",
    "DbLoadRequest",
    "console_db_loader",
    "compare_software",
    "compare_validation",
    "ValidationSettings",
    "load_collected_events",
    "load_coord_split_schedules",
    "generate_report",
    "load_annotations",
    "AdaptiveLatencyOffsetManager",
    "compute_latency_min_samples",
    "match_sparse_event82_latency",
    "sparse_detector_events",
    # NTCIP / SNMP
    "send_ntcip",
    "reset_all_detectors",
    "async_send_ntcip",
    "async_reset_all_detectors",
]

# Comparison internals that 0.x exposed at package level without listing them
# in __all__. They still resolve (with a DeprecationWarning) so old code keeps
# working; import them from signal_replay.comparison instead.
_DEPRECATED_COMPARISON_ALIASES = frozenset({
    "encode_categorical_sequence",
    "compute_dtw",
    "create_comparison_gantt_matplotlib",
    "create_multi_divergence_plots",
    "store_comparison_result",
    "find_alignment_offset",
    "calculate_timeline_offset",
    "compute_timeline_offset",
})


def __getattr__(name):
    if name in _DEPRECATED_COMPARISON_ALIASES:
        import warnings

        warnings.warn(
            f"signal_replay.{name} is not part of the public API; "
            f"import it from signal_replay.comparison instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return getattr(comparison, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
