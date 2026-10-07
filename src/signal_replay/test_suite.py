import logging
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Tuple

from ._serialize import known_fields, to_jsonable
from .comparison import ComparisonResult, ComparisonThresholds
from .config import DEFAULT_REPLAY_LATENCY_OFFSET_SECONDS

logger = logging.getLogger(__name__)


class TestType(str, Enum):
    __test__ = False  # not a pytest test class

    SIMILARITY = "similarity"
    CONFLICT = "conflict"


@dataclass
class TestScenario:
    __test__ = False  # not a pytest test class

    scenario_id: str
    database_name: str
    events_source: str
    test_type: TestType
    replays: int = 1
    incompatible_pairs: Optional[List[Tuple[str, str]]] = None
    description: str = ""
    notes_column: str = ""
    tod_align: bool = True
    cycle_length: int = 0
    cycle_offset: float = 0.0
    # Passed to the output-event source as CollectionTarget.extra (merged
    # over scenario_id, database_name and assignment).
    collection_extra: Optional[Mapping[str, Any]] = None
    # SNMP community for this scenario's controller; None uses the suite's.
    snmp_community: Optional[str] = None


@dataclass
class TestBatch:
    __test__ = False  # not a pytest test class

    batch_id: str
    assignments: Dict[str, str]
    description: str = ""


@dataclass
class SoftwareTestSuite:
    __test__ = False  # not a pytest test class

    suite_name: str
    software_version: str
    baseline_version: str
    scenarios: List[TestScenario]
    batches: List[TestBatch]
    output_dir: str = "./software_test_results"
    comparison_thresholds: Optional[ComparisonThresholds] = None
    phase_call_similarity_threshold: float = 90.0
    analysis_settle_minutes: float = 0.0
    analysis_start_time: str = ""
    analysis_end_time: str = ""
    replay_latency_offset_seconds: float = DEFAULT_REPLAY_LATENCY_OFFSET_SECONDS
    collection_interval_minutes: float = 5.0
    post_replay_settle_seconds: float = 10.0
    snmp_timeout_seconds: float = 2.0
    snmp_send_retries: int = 1
    snmp_retry_backoff_seconds: float = 0.25
    show_progress_logs: bool = False
    progress_log_interval_seconds: float = 60.0
    replay_latency_offset_lookback_min: Optional[float] = None
    replay_latency_offset_update_min: Optional[float] = None
    replay_latency_offset_min_samples: Optional[int] = None
    # Output-event source for every scenario (None = MAXTIME HTTP log);
    # see signal_replay.events. BatchRunner(event_source=...) overrides it.
    event_source: Any = None
    # SNMP community used for every controller unless a scenario sets its own.
    snmp_community: str = "public"
    final_collection_timeout_seconds: float = 900.0
    final_collection_poll_seconds: float = 20.0

    @property
    def detector_similarity_threshold(self) -> float:
        return self.phase_call_similarity_threshold

    @detector_similarity_threshold.setter
    def detector_similarity_threshold(self, value: float) -> None:
        self.phase_call_similarity_threshold = value


@dataclass
class ScenarioResult:
    """Outcome of comparing one scenario (see :func:`signal_replay.compare_validation`).

    ``comparison`` holds the underlying :class:`~signal_replay.ComparisonResult`
    for similarity scenarios (warping paths removed), with absolute
    divergence timestamps. :meth:`to_dict` is JSON-safe.
    """

    scenario_id: str
    test_type: TestType
    software_version: str
    passed: bool
    match_percentage: Optional[float] = None
    timing_match_percentage: Optional[float] = None
    timing_p95_error_seconds: Optional[float] = None
    timing_max_error_seconds: Optional[float] = None
    num_divergences: int = 0
    conflicts_found: List[dict] = field(default_factory=list)
    runs_completed: int = 0
    total_runs: int = 0
    plot_paths: List[str] = field(default_factory=list)
    plot_captions: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0
    error: Optional[str] = None
    notes: str = ""
    notes_column: str = ""
    phase_differences: List[dict] = field(default_factory=list)
    clearance_irregularities: List[dict] = field(default_factory=list)
    operational_differences: List[dict] = field(default_factory=list)
    invalid_clearance_irregularities: List[dict] = field(default_factory=list)
    invalid_operational_differences: List[dict] = field(default_factory=list)
    chunk_scores: List[dict] = field(default_factory=list)
    phase_call_chunk_scores: List[dict] = field(default_factory=list)
    included_chunk_count: int = 0
    excluded_chunk_count: int = 0
    thrown_out: bool = False
    thrown_out_reason: str = ""
    analysis_diagnostics: List[str] = field(default_factory=list)
    timeline_difference_analysis_available: bool = False
    sparkline_svg: str = ""
    temporal_shift_seconds: float = 0.0
    comparison: Optional[ComparisonResult] = None

    @property
    def detector_chunk_scores(self) -> List[dict]:
        return self.phase_call_chunk_scores

    @detector_chunk_scores.setter
    def detector_chunk_scores(self, value: List[dict]) -> None:
        self.phase_call_chunk_scores = value

    def to_dict(self, include_sparkline: bool = True) -> Dict[str, Any]:
        """JSON-safe dict: timestamps as ISO-8601 strings, ``inf``/NaN as None.

        ``comparison`` is included without DTW warping paths. Pass
        ``include_sparkline=False`` to leave out the (large) SVG text.
        """
        data = to_jsonable(self, drop_keys=("comparison",))
        data["test_type"] = self.test_type.value if isinstance(self.test_type, TestType) else str(self.test_type)
        data["comparison"] = self.comparison.to_dict() if self.comparison is not None else None
        if not include_sparkline:
            data["sparkline_svg"] = ""
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ScenarioResult":
        """Rebuild a result from :meth:`to_dict` output."""
        kwargs = known_fields(cls, data)
        kwargs["test_type"] = TestType(data.get("test_type", TestType.SIMILARITY.value))
        comparison = data.get("comparison")
        kwargs["comparison"] = ComparisonResult.from_dict(comparison) if comparison else None
        return cls(**kwargs)
