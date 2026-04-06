from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

from .comparison import ComparisonThresholds


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
    collection_interval_minutes: float = 5.0
    post_replay_settle_seconds: float = 10.0
    snmp_timeout_seconds: float = 2.0
    snmp_send_retries: int = 1
    snmp_retry_backoff_seconds: float = 0.25
    heartbeat_enabled: bool = True
    heartbeat_interval_seconds: float = 5.0
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
    duration_seconds: float = 0.0
    error: Optional[str] = None
    notes: str = ""
    notes_column: str = ""
    phase_differences: List[dict] = field(default_factory=list)
    operational_differences: List[dict] = field(default_factory=list)
    chunk_scores: List[dict] = field(default_factory=list)
    phase_call_chunk_scores: List[dict] = field(default_factory=list)
    included_chunk_count: int = 0
    excluded_chunk_count: int = 0
    thrown_out: bool = False
    timeline_difference_analysis_available: bool = False
    sparkline_svg: str = ""
    temporal_shift_seconds: float = 0.0

    @property
    def detector_chunk_scores(self) -> List[dict]:
        return self.phase_call_chunk_scores

    @detector_chunk_scores.setter
    def detector_chunk_scores(self, value: List[dict]) -> None:
        self.phase_call_chunk_scores = value


def _serialize_suite(suite: FirmwareTestSuite) -> dict:
    data = asdict(suite)

    def _to_plain(value):
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, dict):
            return {k: _to_plain(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_to_plain(v) for v in value]
        if isinstance(value, tuple):
            return [_to_plain(v) for v in value]
        return value

    if suite.comparison_thresholds is not None:
        data["comparison_thresholds"] = asdict(suite.comparison_thresholds)

    return _to_plain(data)


def save_to_yaml(suite: FirmwareTestSuite, path: str) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(_serialize_suite(suite), f, sort_keys=False)


def load_from_yaml(path: str) -> FirmwareTestSuite:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    thresholds_data = data.get("comparison_thresholds")
    thresholds = ComparisonThresholds(**thresholds_data) if thresholds_data else None

    scenarios = []
    for item in data.get("scenarios", []):
        raw_pairs = item.get("incompatible_pairs")
        normalized_pairs = None
        if raw_pairs is not None:
            normalized_pairs = [tuple(pair) for pair in raw_pairs]

        scenarios.append(
            TestScenario(
                scenario_id=item["scenario_id"],
                database_name=item["database_name"],
                events_source=item["events_source"],
                test_type=TestType(item["test_type"]),
                replays=item.get("replays", 1),
                incompatible_pairs=normalized_pairs,
                description=item.get("description", ""),
                notes_column=item.get("notes_column", ""),
                tod_align=item.get("tod_align", True),
                cycle_length=item.get("cycle_length", 0),
                cycle_offset=item.get("cycle_offset", 0.0),
            )
        )

    batches = [
        TestBatch(
            batch_id=item["batch_id"],
            assignments=item.get("assignments", {}),
            description=item.get("description", ""),
        )
        for item in data.get("batches", [])
    ]

    return FirmwareTestSuite(
        suite_name=data["suite_name"],
        firmware_version=data["firmware_version"],
        baseline_version=data["baseline_version"],
        scenarios=scenarios,
        batches=batches,
        output_dir=data.get("output_dir", "./firmware_test_results"),
        comparison_thresholds=thresholds,
        phase_call_similarity_threshold=data.get("phase_call_similarity_threshold", data.get("detector_similarity_threshold", 90.0)),
        analysis_settle_minutes=data.get("analysis_settle_minutes", 0.0),
        analysis_start_time=data.get("analysis_start_time", ""),
        collection_interval_minutes=data.get("collection_interval_minutes", 5.0),
        post_replay_settle_seconds=data.get("post_replay_settle_seconds", 10.0),
        snmp_timeout_seconds=data.get("snmp_timeout_seconds", 2.0),
        snmp_send_retries=data.get("snmp_send_retries", 1),
        snmp_retry_backoff_seconds=data.get("snmp_retry_backoff_seconds", 0.25),
        heartbeat_enabled=data.get("heartbeat_enabled", True),
        heartbeat_interval_seconds=data.get("heartbeat_interval_seconds", 5.0),
        show_progress_logs=data.get("show_progress_logs", False),
        progress_log_interval_seconds=data.get("progress_log_interval_seconds", 60.0),
    )
