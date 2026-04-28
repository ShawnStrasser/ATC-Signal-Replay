from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Dict, List, Optional, Tuple

from .comparison import ComparisonThresholds
from .config import DEFAULT_REPLAY_LATENCY_OFFSET_SECONDS


class TestType(str, Enum):
    SIMILARITY = "similarity"
    CONFLICT = "conflict"


@dataclass
class TestScenario:
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


@dataclass
class TestBatch:
    batch_id: str
    assignments: Dict[str, str]
    description: str = ""


@dataclass
class FirmwareTestSuite:
    suite_name: str
    firmware_version: str
    baseline_version: str
    scenarios: List[TestScenario]
    batches: List[TestBatch]
    output_dir: str = "./firmware_test_results"
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

    @property
    def detector_similarity_threshold(self) -> float:
        return self.phase_call_similarity_threshold

    @detector_similarity_threshold.setter
    def detector_similarity_threshold(self, value: float) -> None:
        self.phase_call_similarity_threshold = value


@dataclass
class ScenarioResult:
    scenario_id: str
    test_type: TestType
    firmware_version: str
    passed: bool
    match_percentage: Optional[float] = None
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

    @property
    def detector_chunk_scores(self) -> List[dict]:
        return self.phase_call_chunk_scores

    @detector_chunk_scores.setter
    def detector_chunk_scores(self, value: List[dict]) -> None:
        self.phase_call_chunk_scores = value
