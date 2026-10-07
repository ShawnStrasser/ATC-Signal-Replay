"""
Public API guard for the 1.x series.

``signal_replay.__all__`` is the stable surface: removing or renaming one of
these names is a breaking change (major version). Adding a name means adding
it to ``EXPECTED_ALL`` here and to the CHANGELOG.
"""

import subprocess
import sys
import textwrap
import types
import warnings

import pytest

import signal_replay as sr

EXPECTED_ALL = sorted([
    # Logging helpers
    "enable_console_logging", "disable_console_logging", "log_to_file",
    # Progress reporting
    "Stage", "ProgressEvent", "ProgressCallback",
    # Core simulation
    "ATCSimulation", "SimulationConfig", "SignalConfig",
    "DEFAULT_REPLAY_LATENCY_OFFSET_SECONDS", "SignalReplay", "create_replays",
    # Results
    "ReplicationResult", "RunRecord", "results_to_frames", "FRAME_COLUMNS",
    "read_manifest", "SCHEMA_VERSION",
    # Data collection
    "DatabaseManager", "DataCollector", "ConflictRecord", "fetch_output_data",
    "check_conflicts",
    # Output-event sources
    "OUTPUT_EVENT_COLUMNS", "CollectionTarget", "FetchResult", "EventSourceError",
    "OutputEventSource", "EventSource", "MaxtimeHttpEventSource",
    "normalize_output_events",
    # Comparison types and constants
    "COMPARISON_EVENT_IDS", "DEFAULT_SEQUENCE_THRESHOLD", "DEFAULT_TIMING_THRESHOLD",
    "DTWResult", "DivergenceWindow", "ComparisonResult", "ComparisonThresholds",
    "ComparisonAnalysis", "ChunkScore",
    # Comparison functions
    "prepare_events_for_comparison", "compare_runs", "compare_all_runs",
    "compare_event_sequences", "compare_and_visualize", "format_comparison_summary",
    "load_events", "generate_timeline", "cross_invalidate_timelines",
    "generate_phase_difference_summary", "generate_clearance_irregularity_summary",
    "generate_operational_difference_summary", "format_phase_differences",
    "find_temporal_offset", "build_included_event_periods",
    "filter_divergence_windows_to_periods", "clip_timeline_to_relative_periods",
    "timeline_overlaps_interval", "render_sparkline_svg",
    # Software validation
    "TestType", "TestScenario", "TestBatch", "SoftwareTestSuite", "ScenarioResult",
    "BatchRunner", "OperationCancelled", "DbLoadRequest", "console_db_loader",
    "compare_software", "compare_validation", "ValidationSettings",
    "load_collected_events", "load_coord_split_schedules", "generate_report",
    "load_annotations", "AdaptiveLatencyOffsetManager", "compute_latency_min_samples",
    "match_sparse_event82_latency", "sparse_detector_events",
    # NTCIP / SNMP
    "send_ntcip", "reset_all_detectors", "async_send_ntcip", "async_reset_all_detectors",
])

DEPRECATED_ALIASES = [
    "encode_categorical_sequence",
    "compute_dtw",
    "create_comparison_gantt_matplotlib",
    "create_multi_divergence_plots",
    "store_comparison_result",
    "find_alignment_offset",
    "calculate_timeline_offset",
    "compute_timeline_offset",
]


def test_all_snapshot():
    assert len(sr.__all__) == len(set(sr.__all__)), "duplicate names in __all__"
    assert sorted(sr.__all__) == EXPECTED_ALL


def test_every_public_name_resolves():
    for name in sr.__all__:
        assert getattr(sr, name, None) is not None, name


def test_namespace_matches_all():
    """Every public non-module attribute of the package is listed in __all__."""
    public = {
        name for name, value in vars(sr).items()
        if not name.startswith("_") and not isinstance(value, types.ModuleType)
    }
    assert public == set(sr.__all__)


def test_version_is_1_0_0():
    assert sr.__version__ == "1.0.0"


@pytest.mark.parametrize("name", DEPRECATED_ALIASES)
def test_old_package_level_aliases_warn_and_resolve(name):
    with pytest.warns(DeprecationWarning, match="signal_replay.comparison"):
        value = getattr(sr, name)
    assert value is getattr(sr.comparison, name)
    assert name not in sr.__all__


def test_unknown_attribute_still_raises():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(AttributeError):
            sr.no_such_name  # noqa: B018


def test_star_import_gives_exactly_all():
    namespace = {}
    exec("from signal_replay import *", namespace)
    namespace.pop("__builtins__", None)
    assert sorted(namespace) == EXPECTED_ALL


def test_consumer_pytest_collects_nothing_from_the_package(tmp_path):
    """A consumer test module importing every public name gets no collection warnings."""
    (tmp_path / "test_consumer.py").write_text(
        textwrap.dedent(
            """
            from signal_replay import *  # noqa: F401,F403
            from signal_replay import TestBatch, TestScenario, TestType, SoftwareTestSuite  # noqa: F401
            import signal_replay.test_suite  # noqa: F401


            def test_placeholder():
                assert True
            """
        ),
        encoding="utf-8",
    )
    proc = subprocess.run(
        [
            sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
            "-W", "error::pytest.PytestCollectionWarning",
            "-o", "addopts=", "--rootdir", str(tmp_path), str(tmp_path / "test_consumer.py"),
        ],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        timeout=180,
    )
    output = proc.stdout + proc.stderr
    assert proc.returncode == 0, output
    assert "1 passed" in output, output
    assert "PytestCollectionWarning" not in output, output
    assert "warning" not in output.lower(), output
