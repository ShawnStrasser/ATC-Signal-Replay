import json
import logging
from dataclasses import asdict
from datetime import datetime
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import duckdb
import pandas as pd

from .comparison import compare_runs
from .config import SignalConfig
from .orchestrator import ATCSimulation
from .test_suite import FirmwareTestSuite, ScenarioResult, TestType, TestScenario


def _parse_assignment(value: str) -> Tuple[str, Optional[int], Optional[int]]:
    """
    Parse assignment target into host + UDP/SNMP + HTTP ports.

    Supported formats:
      - host                    -> udp=None, http=None
      - host:port               -> localhost uses udp=http=port; remote uses udp=None, http=port
      - host:udp_port:http_port -> explicit ports for both protocols
    """
    parts = value.split(":")
    if len(parts) == 1:
        return value, None, None
    if len(parts) == 2:
        host, port_text = parts
        port = int(port_text)
        if host.lower() in ("localhost", "127.0.0.1"):
            return host, port, port
        return host, None, port
    if len(parts) == 3:
        host, udp_text, http_text = parts
        return host, int(udp_text), int(http_text)
    raise ValueError(
        f"Invalid assignment '{value}'. Use host, host:port, or host:udp_port:http_port."
    )

def _serialize_result(result: ScenarioResult) -> dict:
    data = asdict(result)
    data["test_type"] = result.test_type.value
    return data


def _comparison_worker(args: Tuple[dict, str, str, str, str, float]) -> dict:
    scenario_data, baseline_db, new_db, baseline_version, firmware_version, trim_edges_minutes = args
    scenario_id = scenario_data["scenario_id"]

    con_base = duckdb.connect(baseline_db)
    con_new = duckdb.connect(new_db)
    try:
        baseline_run = con_base.execute(
            "SELECT MAX(run_number) FROM events WHERE device_id = ?",
            [scenario_id],
        ).fetchone()[0]
        new_run = con_new.execute(
            "SELECT MAX(run_number) FROM events WHERE device_id = ?",
            [scenario_id],
        ).fetchone()[0]

        if baseline_run is None or new_run is None:
            return {
                "scenario_id": scenario_id,
                "match_percentage": 0.0,
                "num_divergences": 0,
                "sequence_dtw": float("inf"),
                "timing_dtw": float("inf"),
                "summary": "Missing run data in baseline or new database",
            }

        baseline_events = con_base.execute(
            """
            SELECT * FROM events
            WHERE device_id = ? AND run_number = ?
            ORDER BY timestamp, event_id, parameter
            """,
            [scenario_id, int(baseline_run)],
        ).df()
        new_events = con_new.execute(
            """
            SELECT * FROM events
            WHERE device_id = ? AND run_number = ?
            ORDER BY timestamp, event_id, parameter
            """,
            [scenario_id, int(new_run)],
        ).df()
    finally:
        con_base.close()
        con_new.close()

    result = compare_runs(
        events_a=baseline_events,
        events_b=new_events,
        device_id=scenario_id,
        run_a_label=baseline_version,
        run_b_label=firmware_version,
        auto_align=True,
        trim_edges_minutes=trim_edges_minutes,
    )

    return {
        "scenario_id": scenario_id,
        "match_percentage": result.match_percentage,
        "num_divergences": len(result.divergence_windows),
        "sequence_dtw": result.sequence_dtw.normalized_distance,
        "timing_dtw": result.timing_dtw.normalized_distance,
        "summary": result.format_summary(),
    }


class BatchRunner:
    def __init__(self, suite: FirmwareTestSuite, debug: bool = False):
        self.suite = suite
        self.debug = debug
        self.run_dir = Path(suite.output_dir) / suite.firmware_version
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path = self.run_dir / "checkpoint.json"
        self._active_simulation: Optional[ATCSimulation] = None

        self.logger = logging.getLogger(f"signal_replay.batch_runner.{suite.firmware_version}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        stream_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(self.run_dir / "run.log", encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        stream_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)
        self.logger.addHandler(file_handler)

    def _default_checkpoint(self) -> dict:
        return {
            "suite_name": self.suite.suite_name,
            "firmware_version": self.suite.firmware_version,
            "completed_batches": [],
            "scenario_db_map": {},
            "started_at": datetime.utcnow().isoformat(),
            "last_updated": datetime.utcnow().isoformat(),
        }

    def _load_checkpoint(self) -> dict:
        if not self.checkpoint_path.exists():
            return self._default_checkpoint()
        with open(self.checkpoint_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data

    def _save_checkpoint(self, checkpoint: dict) -> None:
        checkpoint["last_updated"] = datetime.utcnow().isoformat()
        with open(self.checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, indent=2)

    def _get_scenario(self, scenario_id: str) -> TestScenario:
        for scenario in self.suite.scenarios:
            if scenario.scenario_id == scenario_id:
                return scenario
        raise ValueError(f"Scenario '{scenario_id}' not found in suite")

    def _clear_scenario_data(self, db_path: Path, scenario_ids: List[str]) -> None:
        """Delete persisted rows for the provided scenarios from a DuckDB file."""
        if not scenario_ids or not db_path.exists():
            return

        placeholders = ",".join(["?"] * len(scenario_ids))
        con = duckdb.connect(str(db_path))
        try:
            tables = {
                row[0].lower()
                for row in con.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                ).fetchall()
            }
            for table in ("events", "conflicts", "input_events", "comparison_results"):
                if table in tables:
                    con.execute(
                        f"DELETE FROM {table} WHERE device_id IN ({placeholders})",
                        scenario_ids,
                    )
        finally:
            con.close()

    def _prompt_database_load(self, batch_id: str, scenario_ids: List[str], assignments: Dict[str, str]) -> None:
        lines = [f"Batch {batch_id}: load these databases before continuing:"]
        for scenario_id in scenario_ids:
            scenario = self._get_scenario(scenario_id)
            lines.append(
                f" - {scenario.scenario_id}: {scenario.database_name} -> {assignments.get(scenario_id, 'UNASSIGNED')}"
            )
        message = "\n".join(lines)
        self.logger.info(message)
        input(f"\n{message}\n\nPress Enter when database loading is complete...\n")

    def _run_similarity_batch(
        self,
        batch,
        similarity_ids: List[str],
        db_loader_callback: Optional[Callable[[str, str], bool]] = None,
    ) -> Optional[Path]:
        if not similarity_ids:
            return None

        if db_loader_callback is None:
            self._prompt_database_load(batch.batch_id, similarity_ids, batch.assignments)
        else:
            for scenario_id in similarity_ids:
                scenario = self._get_scenario(scenario_id)
                target = batch.assignments[scenario_id]
                if not db_loader_callback(scenario.database_name, target):
                    raise RuntimeError(f"Database load callback failed for {scenario_id}")

        signals: List[SignalConfig] = []

        for scenario_id in similarity_ids:
            scenario = self._get_scenario(scenario_id)
            ip, udp_port, http_port = _parse_assignment(batch.assignments[scenario_id])
            signal_cfg = SignalConfig(
                device_id=scenario_id,
                ip=ip,
                udp_port=udp_port,
                http_port=http_port,
                incompatible_pairs=scenario.incompatible_pairs,
                cycle_length=scenario.cycle_length,
                cycle_offset=scenario.cycle_offset,
                tod_align=scenario.tod_align,
            )
            object.__setattr__(signal_cfg, "events", scenario.events_source)
            signals.append(signal_cfg)

        db_path = self.run_dir / f"{batch.batch_id}.duckdb"
        self._clear_scenario_data(db_path, similarity_ids)

        sim = ATCSimulation(
            signals=signals,
            events=None,
            replays=1,
            stop_on_conflict=False,
            db_path=str(db_path),
            simulation_speed=1.0,
            collection_interval_minutes=self.suite.collection_interval_minutes,
            post_replay_settle_seconds=self.suite.post_replay_settle_seconds,
            snmp_timeout_seconds=self.suite.snmp_timeout_seconds,
            show_progress_logs=self.suite.show_progress_logs,
            progress_log_interval_seconds=self.suite.progress_log_interval_seconds,
            comparison_thresholds=self.suite.comparison_thresholds,
            output_dir=str(self.run_dir / "plots"),
            debug=self.debug,
            skip_comparison=True,
        )
        self._active_simulation = sim
        try:
            self.logger.info(f"Starting similarity batch {batch.batch_id} with {len(similarity_ids)} scenarios")
            sim.run()
            self.logger.info(f"Completed similarity batch {batch.batch_id}")
        finally:
            self._active_simulation = None
        return db_path

    def _run_conflict_scenario(
        self,
        batch,
        scenario_id: str,
        db_loader_callback: Optional[Callable[[str, str], bool]] = None,
    ) -> Path:
        scenario = self._get_scenario(scenario_id)
        target = batch.assignments[scenario_id]

        if db_loader_callback is None:
            self.logger.info(
                f"Load conflict scenario {scenario_id}: {scenario.database_name} -> {target}"
            )
            input(
                f"Load conflict database for {scenario_id} ({scenario.database_name}) and press Enter...\n"
            )
        else:
            if not db_loader_callback(scenario.database_name, target):
                raise RuntimeError(f"Database load callback failed for {scenario_id}")

        ip, udp_port, http_port = _parse_assignment(target)
        signal_cfg = SignalConfig(
            device_id=scenario_id,
            ip=ip,
            udp_port=udp_port,
            http_port=http_port,
            incompatible_pairs=scenario.incompatible_pairs,
            cycle_length=scenario.cycle_length,
            cycle_offset=scenario.cycle_offset,
            tod_align=scenario.tod_align,
        )
        object.__setattr__(signal_cfg, "events", scenario.events_source)

        db_path = self.run_dir / f"conflict_{scenario_id}.duckdb"
        self._clear_scenario_data(db_path, [scenario_id])
        sim = ATCSimulation(
            signals=[signal_cfg],
            events=None,
            replays=scenario.replays,
            stop_on_conflict=True,
            db_path=str(db_path),
            simulation_speed=1.0,
            collection_interval_minutes=self.suite.collection_interval_minutes,
            post_replay_settle_seconds=self.suite.post_replay_settle_seconds,
            snmp_timeout_seconds=self.suite.snmp_timeout_seconds,
            show_progress_logs=self.suite.show_progress_logs,
            progress_log_interval_seconds=self.suite.progress_log_interval_seconds,
            comparison_thresholds=self.suite.comparison_thresholds,
            output_dir=str(self.run_dir / "plots"),
            debug=self.debug,
            skip_comparison=True,
        )
        self._active_simulation = sim
        try:
            self.logger.info(f"Starting conflict scenario {scenario_id}")
            sim.run()
            self.logger.info(f"Completed conflict scenario {scenario_id}")
        finally:
            self._active_simulation = None
        return db_path

    def stop(self) -> None:
        """Request cooperative shutdown of the active simulation, if any."""
        if self._active_simulation is not None:
            self.logger.warning("Stop requested for active simulation")
            self._active_simulation.request_stop()

    def run(
        self,
        db_loader_callback: Optional[Callable[[str, str], bool]] = None,
        batch_ids: Optional[List[str]] = None,
    ) -> dict:
        """Run replay batches.

        Parameters
        ----------
        db_loader_callback : callable, optional
            Called with (database_name, target) before each batch.
        batch_ids : list[str], optional
            If provided, only run batches whose ``batch_id`` is in this list.
            Batches already completed (per checkpoint) are still skipped.
        """
        checkpoint = self._load_checkpoint()
        completed = set(checkpoint.get("completed_batches", []))
        scenario_db_map = checkpoint.get("scenario_db_map", {})

        for batch in self.suite.batches:
            if batch_ids is not None and batch.batch_id not in batch_ids:
                continue
            if batch.batch_id in completed:
                self.logger.info(f"Skipping completed batch {batch.batch_id}")
                continue

            scenario_ids = list(batch.assignments.keys())
            similarity_ids = [
                sid for sid in scenario_ids if self._get_scenario(sid).test_type == TestType.SIMILARITY
            ]
            conflict_ids = [
                sid for sid in scenario_ids if self._get_scenario(sid).test_type == TestType.CONFLICT
            ]

            batch_errors = []
            batch_failed = False

            try:
                similarity_db = self._run_similarity_batch(batch, similarity_ids, db_loader_callback)
                if similarity_db is not None:
                    for sid in similarity_ids:
                        scenario_db_map[sid] = str(similarity_db)
            except Exception as exc:
                self.logger.error(f"Similarity batch {batch.batch_id} failed: {exc}")
                batch_errors.append(f"similarity: {exc}")
                batch_failed = True

            if not batch_failed:
                for sid in conflict_ids:
                    try:
                        conflict_db = self._run_conflict_scenario(batch, sid, db_loader_callback)
                        scenario_db_map[sid] = str(conflict_db)
                    except Exception as exc:
                        self.logger.error(f"Conflict scenario {sid} failed: {exc}")
                        batch_errors.append(f"{sid}: {exc}")
                        batch_failed = True
                        break

            if batch_failed:
                self._clear_scenario_data(self.run_dir / f"{batch.batch_id}.duckdb", similarity_ids)
                for sid in conflict_ids:
                    self._clear_scenario_data(self.run_dir / f"conflict_{sid}.duckdb", [sid])
                for sid in scenario_ids:
                    scenario_db_map.pop(sid, None)
            else:
                completed.add(batch.batch_id)

            checkpoint["completed_batches"] = sorted(completed)
            checkpoint["scenario_db_map"] = scenario_db_map
            if batch_errors:
                errors_map = checkpoint.setdefault("batch_errors", {})
                errors_map[batch.batch_id] = batch_errors
            elif "batch_errors" in checkpoint:
                checkpoint["batch_errors"].pop(batch.batch_id, None)
            self._save_checkpoint(checkpoint)

            if batch_errors:
                self.logger.warning(
                    f"Batch {batch.batch_id} failed and was reset: {batch_errors}"
                )
            else:
                self.logger.info(f"Batch {batch.batch_id} complete")

        return checkpoint


def compare_firmware(
    baseline_run_dir: str,
    new_run_dir: str,
    suite: FirmwareTestSuite,
    output_dir: Optional[str] = None,
    max_workers: Optional[int] = None,
    trim_edges_minutes: float = 2.0,
) -> List[ScenarioResult]:
    baseline_checkpoint_path = Path(baseline_run_dir) / "checkpoint.json"
    new_checkpoint_path = Path(new_run_dir) / "checkpoint.json"

    with open(baseline_checkpoint_path, "r", encoding="utf-8") as f:
        baseline_checkpoint = json.load(f)
    with open(new_checkpoint_path, "r", encoding="utf-8") as f:
        new_checkpoint = json.load(f)

    baseline_map = baseline_checkpoint.get("scenario_db_map", {})
    new_map = new_checkpoint.get("scenario_db_map", {})

    results: List[ScenarioResult] = []

    similarity_scenarios = [s for s in suite.scenarios if s.test_type == TestType.SIMILARITY]
    conflict_scenarios = [s for s in suite.scenarios if s.test_type == TestType.CONFLICT]

    similarity_jobs = []
    for scenario in similarity_scenarios:
        baseline_db = baseline_map.get(scenario.scenario_id)
        new_db = new_map.get(scenario.scenario_id)
        if not baseline_db or not new_db:
            results.append(
                ScenarioResult(
                    scenario_id=scenario.scenario_id,
                    test_type=TestType.SIMILARITY,
                    firmware_version=suite.firmware_version,
                    passed=False,
                    error="Missing baseline or new database mapping",
                )
            )
            continue

        similarity_jobs.append(
            (
                {
                    "scenario_id": scenario.scenario_id,
                },
                baseline_db,
                new_db,
                suite.baseline_version,
                suite.firmware_version,
                trim_edges_minutes,
            )
        )

    if similarity_jobs:
        worker_count = max_workers or min(cpu_count(), max(1, len(similarity_jobs)))
        with Pool(processes=worker_count) as pool:
            similarity_outputs = pool.map(_comparison_worker, similarity_jobs)

        thresholds = suite.comparison_thresholds
        for output in similarity_outputs:
            if thresholds is None:
                passed = output["num_divergences"] == 0
            else:
                exceeded, _ = thresholds.exceeds_threshold(
                    output["sequence_dtw"],
                    output["timing_dtw"],
                    output["match_percentage"],
                )
                passed = not exceeded and output["num_divergences"] == 0

            results.append(
                ScenarioResult(
                    scenario_id=output["scenario_id"],
                    test_type=TestType.SIMILARITY,
                    firmware_version=suite.firmware_version,
                    passed=passed,
                    match_percentage=output["match_percentage"],
                    num_divergences=output["num_divergences"],
                    notes=output["summary"],
                    runs_completed=1,
                    total_runs=1,
                )
            )

    for scenario in conflict_scenarios:
        baseline_db = baseline_map.get(scenario.scenario_id)
        new_db = new_map.get(scenario.scenario_id)
        if not baseline_db or not new_db:
            results.append(
                ScenarioResult(
                    scenario_id=scenario.scenario_id,
                    test_type=TestType.CONFLICT,
                    firmware_version=suite.firmware_version,
                    passed=False,
                    error="Missing baseline or new database mapping",
                )
            )
            continue

        con_base = duckdb.connect(baseline_db)
        con_new = duckdb.connect(new_db)
        try:
            base_run = con_base.execute(
                "SELECT MAX(run_number) FROM events WHERE device_id = ?",
                [scenario.scenario_id],
            ).fetchone()[0]
            new_run = con_new.execute(
                "SELECT MAX(run_number) FROM events WHERE device_id = ?",
                [scenario.scenario_id],
            ).fetchone()[0]

            if base_run is None or new_run is None:
                base_conflicts = pd.DataFrame(columns=["timestamp", "conflict_details"])
                new_conflicts = pd.DataFrame(columns=["timestamp", "conflict_details"])
            else:
                base_conflicts = con_base.execute(
                    """
                    SELECT timestamp, conflict_details
                    FROM conflicts
                    WHERE device_id = ? AND run_number = ?
                    ORDER BY timestamp
                    """,
                    [scenario.scenario_id, int(base_run)],
                ).df()
                new_conflicts = con_new.execute(
                    """
                    SELECT timestamp, conflict_details
                    FROM conflicts
                    WHERE device_id = ? AND run_number = ?
                    ORDER BY timestamp
                    """,
                    [scenario.scenario_id, int(new_run)],
                ).df()

            runs_df = con_new.execute(
                "SELECT MAX(run_number) AS max_run FROM events WHERE device_id = ?",
                [scenario.scenario_id],
            ).df()
        finally:
            con_base.close()
            con_new.close()

        baseline_has_conflict = not base_conflicts.empty
        new_has_conflict = not new_conflicts.empty
        runs_completed = (
            int(runs_df.iloc[0]["max_run"])
            if not runs_df.empty and pd.notna(runs_df.iloc[0]["max_run"])
            else 0
        )

        passed = baseline_has_conflict and (not new_has_conflict)
        note_parts = []
        if not baseline_has_conflict:
            note_parts.append("Baseline did not reproduce conflict; test validity warning")
        if new_has_conflict:
            note_parts.append("Conflict observed on new firmware")

        results.append(
            ScenarioResult(
                scenario_id=scenario.scenario_id,
                test_type=TestType.CONFLICT,
                firmware_version=suite.firmware_version,
                passed=passed,
                conflicts_found=new_conflicts.to_dict("records") if not new_conflicts.empty else [],
                runs_completed=runs_completed,
                total_runs=scenario.replays,
                notes="; ".join(note_parts),
            )
        )

    if output_dir:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        with open(out / "comparison_results.json", "w", encoding="utf-8") as f:
            json.dump([_serialize_result(r) for r in results], f, indent=2, default=str)

    return results

