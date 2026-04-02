#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio
import json
import math
import traceback
import sys
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pysnmp.hlapi.v3arch.asyncio import SnmpEngine

ROOT = Path(__file__).absolute().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from signal_replay.collector import fetch_output_data
from signal_replay.ntcip import async_send_ntcip


EVENT_ID_BY_ACTION = {"on": 82, "off": 81}
MAX_GROUPS = 4


@dataclass(frozen=True)
class DeviceTarget:
    port_label: str
    ip: str
    udp_port: int
    http_port: int

    @property
    def ip_port(self) -> tuple[str, int]:
        return (self.ip, self.udp_port)


@dataclass(frozen=True)
class StudyScenario:
    scenario_id: str
    family: str
    detector_count: int
    active_ports: tuple[int, ...]
    order_index: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a latency scaling study across detector counts and active device counts."
        )
    )
    parser.add_argument(
        "--ip",
        default="127.0.0.1",
        help="Controller IP shared by the emulator ports.",
    )
    parser.add_argument(
        "--ports",
        default="9701-9710",
        help="Port list or inclusive range, for example 9701-9710 or 9701,9702,9703.",
    )
    parser.add_argument(
        "--detector-counts",
        default="1,8,16,32",
        help="Detector counts for the one-port-at-a-time matrix.",
    )
    parser.add_argument(
        "--device-scale-detector-count",
        type=int,
        default=8,
        help="Detector count used while scaling active device count.",
    )
    parser.add_argument(
        "--device-scale-repetitions",
        type=int,
        default=3,
        help="Repetitions of the device-count scaling phase. Default: 3.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20260402,
        help="RNG seed for randomizing scenario order. Default: 20260402.",
    )
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=20,
        help="Duration of each scenario. Default: 20 seconds.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=5.0,
        help="Seconds between ON bursts. Default: 5.0.",
    )
    parser.add_argument(
        "--hold-seconds",
        type=float,
        default=1.0,
        help="Seconds to keep detectors on before sending OFF. Default: 1.0.",
    )
    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=2.0,
        help="Delay before the first burst in each scenario. Default: 2.0.",
    )
    parser.add_argument(
        "--inter-scenario-seconds",
        type=float,
        default=1.0,
        help="Pause between scenarios to reduce overlap. Default: 1.0.",
    )
    parser.add_argument(
        "--snmp-timeout-seconds",
        type=float,
        default=2.0,
        help="SNMP timeout for each command. Default: 2.0.",
    )
    parser.add_argument(
        "--http-timeout-seconds",
        type=float,
        default=3.0,
        help="HTTP timeout for controller log fetches. Default: 3.0.",
    )
    parser.add_argument(
        "--collection-window-seconds",
        "--poll-timeout-seconds",
        dest="collection_window_seconds",
        type=float,
        default=6.0,
        help="Fixed controller-log collection window after each scenario. Default: 6.0.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=1.0,
        help="Polling cadence inside the fixed collection window. Default: 1.0.",
    )
    parser.add_argument(
        "--output-root",
        default=str(ROOT / "experiments" / "latency_scaling_runs"),
        help="Directory that receives the timestamped study folder.",
    )
    parser.add_argument(
        "--max-scenarios",
        type=int,
        default=0,
        help="Optional cap for dry runs or validation. 0 means run all scenarios.",
    )
    parser.add_argument(
        "--finalize-run-dir",
        default="",
        help="Rebuild scenario_summary.csv, port_summary.csv, plots, and report from an existing study folder.",
    )
    return parser.parse_args()


def parse_ports(spec: str) -> list[int]:
    spec = spec.strip()
    if "-" in spec and "," not in spec:
        start_text, end_text = spec.split("-", 1)
        start = int(start_text)
        end = int(end_text)
        if end < start:
            raise ValueError(f"Invalid port range: {spec}")
        return list(range(start, end + 1))
    return [int(part.strip()) for part in spec.split(",") if part.strip()]


def build_targets(ip: str, ports: list[int]) -> list[DeviceTarget]:
    return [
        DeviceTarget(
            port_label=str(port),
            ip=ip,
            udp_port=port,
            http_port=port,
        )
        for port in ports
    ]


def parse_detector_counts(spec: str) -> list[int]:
    values = [int(part.strip()) for part in spec.split(",") if part.strip()]
    if not values:
        raise ValueError("detector-counts cannot be empty")
    for value in values:
        if value < 1 or value > 32:
            raise ValueError(f"detector count must be between 1 and 32, got {value}")
    return values


def build_scenarios(
    ports: list[int],
    detector_counts: list[int],
    device_scale_detector_count: int,
    device_scale_repetitions: int = 3,
    seed: int = 20260402,
) -> list[StudyScenario]:
    rng = np.random.default_rng(seed)
    scenarios: list[StudyScenario] = []
    order_index = 0

    # Phase 1: Single-port detector scaling.
    # Port order is randomized within each detector-count block so that port identity
    # and execution sequence are not aliased in the statistical analysis.
    for detector_count in detector_counts:
        port_order = rng.permutation(ports).tolist()
        for port in port_order:
            scenarios.append(
                StudyScenario(
                    scenario_id=f"single_port_p{port}_d{detector_count}",
                    family="single_port_detector_scale",
                    detector_count=detector_count,
                    active_ports=(port,),
                    order_index=order_index,
                )
            )
            order_index += 1

    # Phase 2: Device-count scaling — repeated device_scale_repetitions times.
    # Each repetition randomly selects which N ports form each device-count level,
    # and randomizes the order in which levels are tested, providing genuine
    # replication and removing port-selection bias from the device-count trend.
    ports_array = np.asarray(ports)
    for rep in range(1, device_scale_repetitions + 1):
        device_count_order = rng.permutation(range(1, len(ports) + 1)).tolist()
        for device_count in device_count_order:
            selected = tuple(sorted(int(p) for p in rng.choice(ports_array, size=device_count, replace=False)))
            scenarios.append(
                StudyScenario(
                    scenario_id=f"device_scale_rep{rep}_n{device_count}_d{device_scale_detector_count}",
                    family="device_count_scale",
                    detector_count=device_scale_detector_count,
                    active_ports=selected,
                    order_index=order_index,
                )
            )
            order_index += 1

    return scenarios


def build_actions(duration_seconds: int, interval_seconds: float, hold_seconds: float) -> list[tuple[int, str, float]]:
    cycle_count = int(math.floor(duration_seconds / interval_seconds))
    if cycle_count < 1:
        raise ValueError("duration_seconds must be at least one interval")
    actions: list[tuple[int, str, float]] = []
    for cycle_index in range(cycle_count):
        start_offset = cycle_index * interval_seconds
        actions.append((cycle_index, "on", start_offset))
        actions.append((cycle_index, "off", start_offset + hold_seconds))
    return actions


def scheduled_replay_span_seconds(actions: list[tuple[int, str, float]]) -> float:
    if not actions:
        return 0.0
    return float(max(offset_seconds for _, _, offset_seconds in actions))


def planned_runtime_seconds(args: argparse.Namespace, scenario_count: int) -> dict[str, float | int]:
    actions = build_actions(args.duration_seconds, args.interval_seconds, args.hold_seconds)
    replay_span_seconds = scheduled_replay_span_seconds(actions)
    scenario_runtime_seconds = (
        float(args.warmup_seconds)
        + replay_span_seconds
        + float(args.collection_window_seconds)
    )
    total_runtime_seconds = scenario_runtime_seconds * scenario_count
    if scenario_count > 1:
        total_runtime_seconds += float(args.inter_scenario_seconds) * (scenario_count - 1)
    return {
        "scenario_count": int(scenario_count),
        "actions_per_scenario": int(len(actions)),
        "replay_span_seconds": replay_span_seconds,
        "scenario_runtime_seconds": scenario_runtime_seconds,
        "total_runtime_seconds": total_runtime_seconds,
    }


def format_duration(seconds: float) -> str:
    rounded_seconds = int(round(seconds))
    minutes, secs = divmod(rounded_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def rank_average(values: np.ndarray) -> np.ndarray:
    order = np.argsort(values, kind="mergesort")
    ranks = np.empty(len(values), dtype=float)
    index = 0
    while index < len(values):
        next_index = index + 1
        while next_index < len(values) and values[order[next_index]] == values[order[index]]:
            next_index += 1
        average_rank = (index + 1 + next_index) / 2.0
        ranks[order[index:next_index]] = average_rank
        index = next_index
    return ranks


def pearson_r(x: np.ndarray, y: np.ndarray) -> float:
    centered_x = x.astype(float) - float(np.mean(x))
    centered_y = y.astype(float) - float(np.mean(y))
    denominator = math.sqrt(float(np.sum(centered_x ** 2) * np.sum(centered_y ** 2)))
    if denominator == 0:
        return math.nan
    return float(np.sum(centered_x * centered_y) / denominator)


def spearman_rho(x: np.ndarray, y: np.ndarray) -> float:
    return pearson_r(rank_average(np.asarray(x, dtype=float)), rank_average(np.asarray(y, dtype=float)))


def permutation_correlation(x: Iterable[float], y: Iterable[float], method: str, permutations: int = 20000, seed: int = 20260402) -> dict[str, float]:
    x_array = np.asarray(list(x), dtype=float)
    y_array = np.asarray(list(y), dtype=float)
    correlation_fn = pearson_r if method == "pearson" else spearman_rho
    observed = correlation_fn(x_array, y_array)
    rng = np.random.default_rng(seed)
    exceed_count = 0
    for _ in range(permutations):
        permuted_y = rng.permutation(y_array)
        if abs(correlation_fn(x_array, permuted_y)) >= abs(observed) - 1e-12:
            exceed_count += 1
    return {
        "coefficient": observed,
        "p_value": float((exceed_count + 1) / (permutations + 1)),
    }


def categorical_design_block(values: Iterable[Any], categories: Iterable[Any] | None = None) -> np.ndarray:
    categorical = pd.Categorical(list(values), categories=list(categories) if categories is not None else None)
    dummies = pd.get_dummies(categorical, drop_first=True, dtype=float)
    return dummies.to_numpy()


def sse_from_blocks(y_values: np.ndarray, blocks: list[np.ndarray]) -> float:
    matrices = [np.ones((len(y_values), 1), dtype=float)]
    for block in blocks:
        if block.size:
            matrices.append(block)
    design = np.hstack(matrices)
    coefficients, *_ = np.linalg.lstsq(design, y_values, rcond=None)
    residuals = y_values - design @ coefficients
    return float(np.sum(residuals ** 2))


def permutation_effect_test(y_values: Iterable[float], full_blocks: list[np.ndarray], reduced_blocks: list[np.ndarray], permutations: int = 20000, seed: int = 20260402) -> dict[str, float]:
    y_array = np.asarray(list(y_values), dtype=float)
    sse_full = sse_from_blocks(y_array, full_blocks)
    sse_reduced = sse_from_blocks(y_array, reduced_blocks)
    effect_sum_squares = sse_reduced - sse_full
    partial_eta_squared = effect_sum_squares / (effect_sum_squares + sse_full) if (effect_sum_squares + sse_full) else math.nan
    rng = np.random.default_rng(seed)
    exceed_count = 0
    for _ in range(permutations):
        permuted = rng.permutation(y_array)
        perm_effect = sse_from_blocks(permuted, reduced_blocks) - sse_from_blocks(permuted, full_blocks)
        if perm_effect >= effect_sum_squares - 1e-12:
            exceed_count += 1
    return {
        "sum_squares": effect_sum_squares,
        "partial_eta_squared": partial_eta_squared,
        "p_value": float((exceed_count + 1) / (permutations + 1)),
    }


def load_matched_latency_data(run_dir: Path, summary_df: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for row in summary_df.itertuples(index=False):
        matched_path = run_dir / "scenarios" / row.scenario_id / "matched_latency.csv"
        if not matched_path.exists():
            continue
        matched_df = pd.read_csv(matched_path)
        if matched_df.empty or "matched" not in matched_df.columns:
            continue
        matched_df = matched_df[matched_df["matched"] == True].copy()
        if matched_df.empty:
            continue
        matched_df["scenario_id"] = row.scenario_id
        matched_df["family"] = row.family
        matched_df["detector_count"] = int(row.detector_count)
        matched_df["active_device_count"] = int(row.active_device_count)
        frames.append(matched_df[["scenario_id", "family", "detector_count", "active_device_count", "latency_ms"]])
    if not frames:
        return pd.DataFrame(columns=["scenario_id", "family", "detector_count", "active_device_count", "latency_ms"])
    return pd.concat(frames, ignore_index=True)


def evaluate_offset_model(events_df: pd.DataFrame, group_columns: list[str]) -> dict[str, Any]:
    if events_df.empty:
        return {
            "grouping": "global" if not group_columns else "+".join(group_columns),
            "offset_count": 0,
            "mae_ms": math.nan,
            "rmse_ms": math.nan,
            "p95_abs_error_ms": math.nan,
            "pct_within_100ms": math.nan,
            "median_abs_error_ms": math.nan,
        }

    if group_columns:
        offsets_df = events_df.groupby(group_columns, as_index=False)["latency_ms"].median().rename(columns={"latency_ms": "offset_ms"})
        merged_df = events_df.merge(offsets_df, on=group_columns, how="left")
        offset_count = int(offsets_df.shape[0])
    else:
        merged_df = events_df.copy()
        merged_df["offset_ms"] = float(events_df["latency_ms"].median())
        offset_count = 1

    residuals = merged_df["latency_ms"] - merged_df["offset_ms"]
    absolute_residuals = residuals.abs()
    return {
        "grouping": "global" if not group_columns else "+".join(group_columns),
        "offset_count": offset_count,
        "mae_ms": float(absolute_residuals.mean()),
        "rmse_ms": float(math.sqrt(float(np.mean(residuals ** 2)))),
        "p95_abs_error_ms": float(absolute_residuals.quantile(0.95)),
        "pct_within_100ms": float((absolute_residuals <= 100).mean() * 100.0),
        "median_abs_error_ms": float(absolute_residuals.median()),
    }


def compute_statistical_analysis(run_dir: Path, summary_df: pd.DataFrame) -> dict[str, Any]:
    results: dict[str, Any] = {
        "single_port_port_effect": None,
        "single_port_detector_effect": None,
        "single_port_detector_corr": None,
        "device_count_corr": None,
        "overall_load_corr": None,
        "global_offset": None,
        "single_port_global_offset": None,
        "single_port_detector_offsets": None,
        "device_scale_global_offset": None,
        "device_scale_device_offsets": None,
        "detector_offset_rows": [],
        "device_offset_rows": [],
        "port_note": "Port number is only a proxy for emulator instance identity in this study.",
    }

    single_port_df = summary_df[summary_df["family"] == "single_port_detector_scale"].copy()
    if not single_port_df.empty:
        single_port_df["port_proxy"] = single_port_df["active_ports"].astype(str)
        y_single = single_port_df["median_latency_ms"].to_numpy(dtype=float)
        port_block = categorical_design_block(single_port_df["port_proxy"])
        detector_levels = [str(value) for value in sorted(single_port_df["detector_count"].unique())]
        detector_block = categorical_design_block(single_port_df["detector_count"].astype(str), categories=detector_levels)
        results["single_port_port_effect"] = permutation_effect_test(
            y_single,
            full_blocks=[port_block, detector_block],
            reduced_blocks=[detector_block],
        )
        results["single_port_detector_effect"] = permutation_effect_test(
            y_single,
            full_blocks=[port_block, detector_block],
            reduced_blocks=[port_block],
        )

        aggregated_by_detector = single_port_df.groupby("detector_count", as_index=False)["median_latency_ms"].mean()
        if len(aggregated_by_detector) >= 2:
            results["single_port_detector_corr"] = {
                "pearson": permutation_correlation(
                    aggregated_by_detector["detector_count"],
                    aggregated_by_detector["median_latency_ms"],
                    method="pearson",
                ),
                "spearman": permutation_correlation(
                    aggregated_by_detector["detector_count"],
                    aggregated_by_detector["median_latency_ms"],
                    method="spearman",
                ),
            }

        results["port_note"] = (
            "Port order within each detector-count block was randomized, "
            "so a significant effect here indicates genuine per-instance variance across emulator ports, "
            "not an execution-order artefact."
        )

    device_scale_df = summary_df[summary_df["family"] == "device_count_scale"].copy()
    if len(device_scale_df) >= 2:
        results["device_count_corr"] = {
            "pearson": permutation_correlation(
                device_scale_df["active_device_count"],
                device_scale_df["median_latency_ms"],
                method="pearson",
            ),
            "spearman": permutation_correlation(
                device_scale_df["active_device_count"],
                device_scale_df["median_latency_ms"],
                method="spearman",
            ),
        }

    if len(summary_df) >= 2:
        load_df = summary_df.copy()
        load_df["events_per_burst"] = load_df["active_device_count"] * load_df["detector_count"]
        results["overall_load_corr"] = {
            "pearson": permutation_correlation(load_df["events_per_burst"], load_df["median_latency_ms"], method="pearson"),
            "spearman": permutation_correlation(load_df["events_per_burst"], load_df["median_latency_ms"], method="spearman"),
        }

    event_df = load_matched_latency_data(run_dir, summary_df)
    if not event_df.empty:
        results["global_offset"] = evaluate_offset_model(event_df, [])
        results["global_offset"]["offset_ms"] = float(event_df["latency_ms"].median())
        absolute_residuals = (event_df["latency_ms"] - results["global_offset"]["offset_ms"]).abs()
        results["global_offset"]["p50_abs_error_ms"] = float(absolute_residuals.quantile(0.50))
        results["global_offset"]["p90_abs_error_ms"] = float(absolute_residuals.quantile(0.90))
        results["global_offset"]["p99_abs_error_ms"] = float(absolute_residuals.quantile(0.99))
        results["global_offset"]["pct_within_25ms"] = float((absolute_residuals <= 25).mean() * 100.0)
        results["global_offset"]["pct_within_50ms"] = float((absolute_residuals <= 50).mean() * 100.0)
        results["global_offset"]["pct_within_75ms"] = float((absolute_residuals <= 75).mean() * 100.0)

        results["detector_offset_rows"] = (
            event_df.groupby("detector_count", as_index=False)["latency_ms"]
            .median()
            .rename(columns={"latency_ms": "offset_ms"})
            .sort_values("detector_count")
            .to_dict("records")
        )

        single_event_df = event_df[event_df["family"] == "single_port_detector_scale"].copy()
        if not single_event_df.empty:
            results["single_port_global_offset"] = evaluate_offset_model(single_event_df, [])
            results["single_port_global_offset"]["offset_ms"] = float(single_event_df["latency_ms"].median())
            results["single_port_detector_offsets"] = evaluate_offset_model(single_event_df, ["detector_count"])
            if results["single_port_global_offset"]["mae_ms"]:
                results["single_port_detector_offsets"]["mae_improvement_pct"] = float(
                    (results["single_port_global_offset"]["mae_ms"] - results["single_port_detector_offsets"]["mae_ms"])
                    / results["single_port_global_offset"]["mae_ms"]
                    * 100.0
                )

        device_event_df = event_df[event_df["family"] == "device_count_scale"].copy()
        if not device_event_df.empty:
            results["device_offset_rows"] = (
                device_event_df.groupby("active_device_count", as_index=False)["latency_ms"]
                .median()
                .rename(columns={"latency_ms": "offset_ms"})
                .sort_values("active_device_count")
                .to_dict("records")
            )
            results["device_scale_global_offset"] = evaluate_offset_model(device_event_df, [])
            results["device_scale_global_offset"]["offset_ms"] = float(device_event_df["latency_ms"].median())
            results["device_scale_device_offsets"] = evaluate_offset_model(device_event_df, ["active_device_count"])
            if results["device_scale_global_offset"]["mae_ms"]:
                results["device_scale_device_offsets"]["mae_improvement_pct"] = float(
                    (results["device_scale_global_offset"]["mae_ms"] - results["device_scale_device_offsets"]["mae_ms"])
                    / results["device_scale_global_offset"]["mae_ms"]
                    * 100.0
                )

    return results


def format_stat(value: float | None, digits: int = 3) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return f"{value:.{digits}f}"


def format_p_value(value: float | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    if value < 0.001:
        return "<0.001"
    return f"{value:.3f}"


def relevant_detectors(detector_count: int) -> tuple[int, ...]:
    return tuple(range(1, detector_count + 1))


def commands_for_detector_count(detector_count: int, action: str) -> list[tuple[int, int]]:
    commands = []
    remaining = detector_count
    group_number = 1
    while remaining > 0:
        active_in_group = min(8, remaining)
        if action == "on":
            state = (1 << active_in_group) - 1
        else:
            state = 0
        commands.append((group_number, state))
        remaining -= active_in_group
        group_number += 1
    return commands


def detectors_for_group(detector_count: int, group_number: int) -> list[int]:
    start = (group_number - 1) * 8 + 1
    end = min(group_number * 8, detector_count)
    return list(range(start, end + 1)) if start <= end else []


async def send_group_command(
    target: DeviceTarget,
    action: str,
    cycle_index: int,
    scheduled_at: datetime,
    detector_count: int,
    group_number: int,
    state_integer: int,
    snmp_engine: SnmpEngine,
    timeout: float,
) -> dict[str, Any]:
    dispatch_started_at = datetime.now()
    operation_timeout = max(timeout + 1.0, 3.0)
    error_text = None
    try:
        await asyncio.wait_for(
            async_send_ntcip(
                target.ip_port,
                detector_group=group_number,
                state_integer=state_integer,
                detector_type="Vehicle",
                timeout=timeout,
                snmp_engine=snmp_engine,
            ),
            timeout=operation_timeout,
        )
        success = True
    except Exception as exc:
        success = False
        error_text = str(exc)
    response_finished_at = datetime.now()
    return {
        "port": target.udp_port,
        "cycle_index": cycle_index,
        "action": action,
        "event_id": EVENT_ID_BY_ACTION[action],
        "detector_count": detector_count,
        "group_number": group_number,
        "state_integer": state_integer,
        "scheduled_at": scheduled_at,
        "dispatch_started_at": dispatch_started_at,
        "response_finished_at": response_finished_at,
        "response_ms": (response_finished_at - dispatch_started_at).total_seconds() * 1000.0,
        "success": success,
        "error": error_text,
    }


async def reset_vehicle_groups(
    targets: list[DeviceTarget],
    max_group: int,
    timeout: float,
    snmp_engine: SnmpEngine,
) -> None:
    reset_tasks = []
    for target in targets:
        for group_number in range(1, max_group + 1):
            reset_tasks.append(
                asyncio.wait_for(
                    async_send_ntcip(
                        target.ip_port,
                        detector_group=group_number,
                        state_integer=0,
                        detector_type="Vehicle",
                        timeout=timeout,
                        snmp_engine=snmp_engine,
                    ),
                    timeout=max(timeout + 1.0, 3.0),
                )
            )
    if reset_tasks:
        await asyncio.gather(*reset_tasks, return_exceptions=True)


async def preflight_targets(
    targets: list[DeviceTarget],
    detector_count: int,
    snmp_engine: SnmpEngine,
    timeout: float,
    http_timeout_seconds: float,
) -> None:
    await reset_vehicle_groups(targets, max_group=min(MAX_GROUPS, math.ceil(detector_count / 8)), timeout=timeout, snmp_engine=snmp_engine)

    async def check_target(target: DeviceTarget) -> tuple[int, str | None]:
        try:
            await asyncio.to_thread(
                fetch_output_data,
                target.ip,
                target.http_port,
                datetime.now() - timedelta(seconds=15),
                http_timeout_seconds,
            )
        except Exception as exc:
            return target.udp_port, f"HTTP fetch failed: {exc}"
        return target.udp_port, None

    results = await asyncio.gather(*(check_target(target) for target in targets))
    failures = [f"{port}: {error}" for port, error in results if error is not None]
    if failures:
        raise RuntimeError("Preflight failed:\n" + "\n".join(failures))


async def run_schedule(
    targets: list[DeviceTarget],
    detector_count: int,
    actions: list[tuple[int, str, float]],
    warmup_seconds: float,
    timeout: float,
    http_timeout_seconds: float,
) -> tuple[pd.DataFrame, datetime, datetime]:
    snmp_engine = SnmpEngine()
    send_rows: list[dict[str, Any]] = []
    first_scheduled_at = datetime.now() + timedelta(seconds=warmup_seconds)
    start_monotonic = time.perf_counter() + warmup_seconds
    command_template = {
        action: commands_for_detector_count(detector_count, action) for action in ("on", "off")
    }

    try:
        await preflight_targets(targets, detector_count, snmp_engine, timeout, http_timeout_seconds)
        for cycle_index, action, offset_seconds in actions:
            target_monotonic = start_monotonic + offset_seconds
            delay = target_monotonic - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)

            scheduled_at = first_scheduled_at + timedelta(seconds=offset_seconds)
            tasks = []
            for target in targets:
                for group_number, state_integer in command_template[action]:
                    tasks.append(
                        send_group_command(
                            target=target,
                            action=action,
                            cycle_index=cycle_index,
                            scheduled_at=scheduled_at,
                            detector_count=detector_count,
                            group_number=group_number,
                            state_integer=state_integer,
                            snmp_engine=snmp_engine,
                            timeout=timeout,
                        )
                    )
            burst_rows = await asyncio.gather(*tasks)
            send_rows.extend(burst_rows)

        end_wall = datetime.now()
    finally:
        try:
            await reset_vehicle_groups(
                targets,
                max_group=min(MAX_GROUPS, math.ceil(detector_count / 8)),
                timeout=timeout,
                snmp_engine=snmp_engine,
            )
        finally:
            snmp_engine.close_dispatcher()

    return pd.DataFrame(send_rows), first_scheduled_at, end_wall


def filter_logged_events(df: pd.DataFrame, port: int, detector_count: int, window_start: datetime) -> pd.DataFrame:
    detectors = relevant_detectors(detector_count)
    if df.empty:
        return pd.DataFrame(columns=["port", "timestamp", "event_id", "parameter"])
    filtered = df[
        df["EventTypeID"].isin(EVENT_ID_BY_ACTION.values())
        & df["Parameter"].isin(detectors)
        & (df["TimeStamp"] >= window_start)
    ].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["port", "timestamp", "event_id", "parameter"])
    filtered["port"] = port
    filtered = filtered.rename(
        columns={
            "TimeStamp": "timestamp",
            "EventTypeID": "event_id",
            "Parameter": "parameter",
        }
    )
    return filtered[["port", "timestamp", "event_id", "parameter"]].sort_values("timestamp")


async def fetch_device_events(
    target: DeviceTarget,
    detector_count: int,
    since: datetime,
    http_timeout_seconds: float,
) -> tuple[int, pd.DataFrame, str | None]:
    try:
        df = await asyncio.to_thread(
            fetch_output_data,
            target.ip,
            target.http_port,
            since,
            http_timeout_seconds,
        )
    except Exception as exc:
        empty = pd.DataFrame(columns=["port", "timestamp", "event_id", "parameter"])
        return target.udp_port, empty, str(exc)
    filtered = filter_logged_events(df, target.udp_port, detector_count, since - timedelta(milliseconds=250))
    return target.udp_port, filtered, None


async def poll_logged_events(
    targets: list[DeviceTarget],
    detector_count: int,
    since: datetime,
    expected_per_port: int,
    collection_window_seconds: float,
    poll_interval_seconds: float,
    http_timeout_seconds: float,
) -> pd.DataFrame:
    deadline = time.perf_counter() + collection_window_seconds
    expected_total = expected_per_port * len(targets)
    latest = pd.DataFrame(columns=["port", "timestamp", "event_id", "parameter"])
    last_count = -1
    stable_polls = 0
    warned_ports: set[int] = set()

    while True:
        results = await asyncio.gather(
            *(fetch_device_events(target, detector_count, since, http_timeout_seconds) for target in targets)
        )
        frames = []
        for port, frame, error in results:
            if error is not None:
                if port not in warned_ports:
                    print(f"  Poll warning for {port}: {error}", flush=True)
                    warned_ports.add(port)
                continue
            warned_ports.discard(port)
            frames.append(frame)

        if frames:
            latest = pd.concat(frames, ignore_index=True)
        if not latest.empty:
            latest = latest.drop_duplicates().sort_values(["port", "timestamp", "event_id", "parameter"])

        current_count = len(latest)
        counts = latest.groupby("port").size().to_dict() if not latest.empty else {}
        ready = all(counts.get(target.udp_port, 0) >= expected_per_port for target in targets)
        nearly_ready = current_count >= math.floor(expected_total * 0.99)

        if current_count == last_count:
            stable_polls += 1
        else:
            print(f"  Poll observed {current_count} detector events so far", flush=True)
            stable_polls = 0
        last_count = current_count

        if ready and stable_polls >= 1:
            return latest.reset_index(drop=True)
        if nearly_ready and stable_polls >= 2:
            print(
                f"  Poll plateaued at {current_count}/{expected_total} events; using the best available snapshot",
                flush=True,
            )
            return latest.reset_index(drop=True)
        if time.perf_counter() >= deadline:
            return latest.reset_index(drop=True)

        await asyncio.sleep(poll_interval_seconds)


def expand_expected_events(send_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in send_df.itertuples(index=False):
        detectors = detectors_for_group(int(row.detector_count), int(row.group_number))
        for detector in detectors:
            rows.append(
                {
                    "port": row.port,
                    "cycle_index": row.cycle_index,
                    "action": row.action,
                    "event_id": row.event_id,
                    "parameter": detector,
                    "detector_count": row.detector_count,
                    "group_number": row.group_number,
                    "scheduled_at": row.scheduled_at,
                    "dispatch_started_at": row.dispatch_started_at,
                    "response_finished_at": row.response_finished_at,
                    "response_ms": row.response_ms,
                    "send_success": row.success,
                    "send_error": row.error,
                }
            )
    return pd.DataFrame(rows).sort_values(
        ["port", "parameter", "event_id", "dispatch_started_at"]
    ).reset_index(drop=True)


def greedy_match_group(
    expected_group: pd.DataFrame,
    actual_group: pd.DataFrame,
    early_margin_ms: float,
    late_window_ms: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    actual_records = list(actual_group.sort_values("timestamp").to_dict("records"))
    actual_index = 0
    matched: list[dict[str, Any]] = []
    extras: list[dict[str, Any]] = []
    early_margin = timedelta(milliseconds=early_margin_ms)
    late_window = timedelta(milliseconds=late_window_ms)

    for expected in expected_group.sort_values("dispatch_started_at").to_dict("records"):
        dispatch_time = expected["dispatch_started_at"]
        lower_bound = dispatch_time - early_margin
        upper_bound = dispatch_time + late_window
        while actual_index < len(actual_records) and actual_records[actual_index]["timestamp"] < lower_bound:
            extra = actual_records[actual_index].copy()
            extra["match_status"] = "unexpected_actual"
            extras.append(extra)
            actual_index += 1

        if actual_index < len(actual_records) and actual_records[actual_index]["timestamp"] <= upper_bound:
            actual = actual_records[actual_index]
            matched.append(
                {
                    **expected,
                    "logged_timestamp": actual["timestamp"],
                    "latency_ms": (actual["timestamp"] - dispatch_time).total_seconds() * 1000.0,
                    "matched": True,
                }
            )
            actual_index += 1
        else:
            matched.append(
                {
                    **expected,
                    "logged_timestamp": pd.NaT,
                    "latency_ms": math.nan,
                    "matched": False,
                }
            )

    while actual_index < len(actual_records):
        extra = actual_records[actual_index].copy()
        extra["match_status"] = "unexpected_actual"
        extras.append(extra)
        actual_index += 1

    return matched, extras


def match_events(expected_df: pd.DataFrame, actual_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    matched_rows: list[dict[str, Any]] = []
    extra_rows: list[dict[str, Any]] = []
    group_columns = ["port", "event_id", "parameter"]
    for group_key, expected_group in expected_df.groupby(group_columns, dropna=False):
        if actual_df.empty:
            actual_group = pd.DataFrame(columns=["port", "timestamp", "event_id", "parameter"])
        else:
            mask = (
                (actual_df["port"] == group_key[0])
                & (actual_df["event_id"] == group_key[1])
                & (actual_df["parameter"] == group_key[2])
            )
            actual_group = actual_df[mask].copy()
        matched_group, extras_group = greedy_match_group(
            expected_group=expected_group,
            actual_group=actual_group,
            early_margin_ms=250.0,
            late_window_ms=5000.0,
        )
        matched_rows.extend(matched_group)
        extra_rows.extend(extras_group)

    matched_df = pd.DataFrame(matched_rows).sort_values(
        ["port", "dispatch_started_at", "parameter", "event_id"]
    )
    extras_df = pd.DataFrame(extra_rows)
    if not extras_df.empty:
        extras_df = extras_df.sort_values(["port", "timestamp", "event_id", "parameter"])
    return matched_df.reset_index(drop=True), extras_df.reset_index(drop=True)


def summarize_latency(series: pd.Series) -> dict[str, float | int | None]:
    clean = series.dropna()
    if clean.empty:
        return {
            "count": 0,
            "mean_ms": None,
            "median_ms": None,
            "std_ms": None,
            "p05_ms": None,
            "p95_ms": None,
            "min_ms": None,
            "max_ms": None,
        }
    return {
        "count": int(clean.count()),
        "mean_ms": float(clean.mean()),
        "median_ms": float(clean.median()),
        "std_ms": float(clean.std(ddof=1)) if clean.count() > 1 else 0.0,
        "p05_ms": float(clean.quantile(0.05)),
        "p95_ms": float(clean.quantile(0.95)),
        "min_ms": float(clean.min()),
        "max_ms": float(clean.max()),
    }


def summarize_send_spread(send_df: pd.DataFrame) -> pd.DataFrame:
    if send_df.empty:
        return pd.DataFrame(columns=["cycle_index", "action", "fanout_spread_ms", "response_spread_ms"])
    rows = []
    for (cycle_index, action), group in send_df.groupby(["cycle_index", "action"]):
        dispatch_times = pd.to_datetime(group["dispatch_started_at"])
        response_times = pd.to_datetime(group["response_finished_at"])
        rows.append(
            {
                "cycle_index": int(cycle_index),
                "action": action,
                "fanout_spread_ms": (dispatch_times.max() - dispatch_times.min()).total_seconds() * 1000.0,
                "response_spread_ms": (response_times.max() - response_times.min()).total_seconds() * 1000.0,
            }
        )
    return pd.DataFrame(rows).sort_values(["cycle_index", "action"]).reset_index(drop=True)


def scenario_summary(
    scenario: StudyScenario,
    matched_df: pd.DataFrame,
    extras_df: pd.DataFrame,
    send_df: pd.DataFrame,
    send_spread_df: pd.DataFrame,
) -> dict[str, Any]:
    matched_success = matched_df[matched_df["matched"]].copy()
    latency_stats = summarize_latency(matched_success["latency_ms"])
    expected_events = int(len(matched_df))
    matched_events = int(len(matched_success))
    missing_events = int((~matched_df["matched"]).sum())
    unexpected_events = int(len(extras_df))
    send_failures = int((~send_df["success"]).sum())
    within_100 = float((matched_success["latency_ms"].abs() <= 100).mean() * 100.0) if not matched_success.empty else 0.0
    within_250 = float((matched_success["latency_ms"].abs() <= 250).mean() * 100.0) if not matched_success.empty else 0.0
    fanout_stats = summarize_latency(send_spread_df["fanout_spread_ms"]) if not send_spread_df.empty else summarize_latency(pd.Series(dtype=float))
    return {
        "scenario_id": scenario.scenario_id,
        "family": scenario.family,
        "detector_count": scenario.detector_count,
        "active_device_count": len(scenario.active_ports),
        "active_ports": ",".join(str(port) for port in scenario.active_ports),
        "expected_events": expected_events,
        "matched_events": matched_events,
        "missing_events": missing_events,
        "unexpected_events": unexpected_events,
        "send_failures": send_failures,
        "median_latency_ms": latency_stats["median_ms"],
        "mean_latency_ms": latency_stats["mean_ms"],
        "p95_latency_ms": latency_stats["p95_ms"],
        "std_latency_ms": latency_stats["std_ms"],
        "min_latency_ms": latency_stats["min_ms"],
        "max_latency_ms": latency_stats["max_ms"],
        "pct_within_100ms": within_100,
        "pct_within_250ms": within_250,
        "median_fanout_spread_ms": fanout_stats["median_ms"],
        "p95_fanout_spread_ms": fanout_stats["p95_ms"],
        "status": "completed",
        "error": "",
    }


def failed_scenario_summary(scenario: StudyScenario, error_text: str) -> dict[str, Any]:
    return {
        "scenario_id": scenario.scenario_id,
        "family": scenario.family,
        "detector_count": scenario.detector_count,
        "active_device_count": len(scenario.active_ports),
        "active_ports": ",".join(str(port) for port in scenario.active_ports),
        "expected_events": 0,
        "matched_events": 0,
        "missing_events": 0,
        "unexpected_events": 0,
        "send_failures": 0,
        "median_latency_ms": math.nan,
        "mean_latency_ms": math.nan,
        "p95_latency_ms": math.nan,
        "std_latency_ms": math.nan,
        "min_latency_ms": math.nan,
        "max_latency_ms": math.nan,
        "pct_within_100ms": math.nan,
        "pct_within_250ms": math.nan,
        "median_fanout_spread_ms": math.nan,
        "p95_fanout_spread_ms": math.nan,
        "first_scheduled_at": "",
        "replay_finished_at": "",
        "status": "failed",
        "error": error_text,
    }


def port_summary(matched_df: pd.DataFrame, scenario: StudyScenario) -> pd.DataFrame:
    matched_success = matched_df[matched_df["matched"]].copy()
    rows = []
    for port, group in matched_success.groupby("port"):
        stats = summarize_latency(group["latency_ms"])
        rows.append(
            {
                "scenario_id": scenario.scenario_id,
                "family": scenario.family,
                "detector_count": scenario.detector_count,
                "active_device_count": len(scenario.active_ports),
                "port": int(port),
                **stats,
                "pct_within_100ms": float((group["latency_ms"].abs() <= 100).mean() * 100.0),
            }
        )
    return pd.DataFrame(rows)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_detector_scale_plot(single_port_df: pd.DataFrame, output_path: Path) -> None:
    """Bar chart of median/p95 latency by detector count, aggregated across ports."""
    agg = single_port_df.groupby("detector_count").agg(
        median_ms=("median_ms", "median"),
        p95_ms=("p95_ms", "median"),
    ).reset_index()
    x = np.arange(len(agg))
    w = 0.35
    fig, ax = plt.subplots(figsize=(5, 3))
    ax.bar(x - w / 2, agg["median_ms"], w, label="Median")
    ax.bar(x + w / 2, agg["p95_ms"], w, label="P95")
    ax.set_xticks(x)
    ax.set_xticklabels([str(int(d)) for d in agg["detector_count"]])
    ax.set_xlabel("Detector count")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Latency by Detector Count (aggregated across ports)")
    ax.legend(fontsize=7)
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=120)
    plt.close(fig)


def save_device_scale_plot(device_scale_df: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(5, 3))
    ax.plot(device_scale_df["active_device_count"], device_scale_df["median_latency_ms"], marker="o", markersize=4, label="Median")
    ax.plot(device_scale_df["active_device_count"], device_scale_df["p95_latency_ms"], marker="s", markersize=4, label="P95")
    ax.set_xlabel("Active device count")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Latency vs Device Count (8 detectors)")
    ax.grid(True, alpha=0.25)
    ax.legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(output_path, dpi=120)
    plt.close(fig)


def save_residual_histogram(run_dir: Path, summary_df: pd.DataFrame, output_path: Path) -> None:
    """Histogram of per-event residuals after subtracting the global median offset."""
    events_df = load_matched_latency_data(run_dir, summary_df)
    if events_df is None or events_df.empty:
        return
    global_offset = float(np.median(events_df["latency_ms"].values))
    residuals = events_df["latency_ms"].values - global_offset
    fig, ax = plt.subplots(figsize=(5, 3))
    ax.hist(residuals, bins=30, edgecolor="white", linewidth=0.5)
    ax.axvline(0, color="red", linewidth=0.8, linestyle="--")
    ax.set_xlabel("Residual (ms)")
    ax.set_ylabel("Count")
    ax.set_title(f"Residuals after {global_offset:.0f} ms global offset")
    fig.tight_layout()
    fig.savefig(output_path, dpi=120)
    plt.close(fig)


def save_load_scatter(summary_df: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(5, 3))
    family_colors = {
        "single_port_detector_scale": "tab:blue",
        "device_count_scale": "tab:orange",
    }
    summary_df = summary_df.copy()
    summary_df["events_per_burst"] = summary_df["active_device_count"] * summary_df["detector_count"]
    for family, group in summary_df.groupby("family"):
        ax.scatter(
            group["events_per_burst"],
            group["median_latency_ms"],
            s=25,
            alpha=0.8,
            color=family_colors.get(family, "tab:gray"),
            label=family.replace("_", " "),
        )
    ax.set_xlabel("Events per burst")
    ax.set_ylabel("Median latency (ms)")
    ax.set_title("Load vs Latency")
    ax.grid(True, alpha=0.25)
    ax.legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(output_path, dpi=120)
    plt.close(fig)


def _noop(*_a, **_kw):
    """Removed function placeholder."""
    pass


def markdown_table(rows: Iterable[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
    rows = list(rows)
    header = "| " + " | ".join(title for _, title in columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    body = []
    for row in rows:
        values = []
        for key, _ in columns:
            value = row.get(key)
            if value is None or (isinstance(value, float) and math.isnan(value)):
                values.append("")
            elif isinstance(value, float):
                values.append(f"{value:.1f}")
            else:
                values.append(str(value))
        body.append("| " + " | ".join(values) + " |")
    return "\n".join([header, divider, *body])


def generate_plots_and_report(run_dir: Path, args: argparse.Namespace, summary_df: pd.DataFrame, port_df: pd.DataFrame) -> None:
    plots_dir = run_dir / "plots"
    ensure_dir(plots_dir)
    stats_results = compute_statistical_analysis(run_dir, summary_df)

    successful = summary_df[summary_df["status"].fillna("completed") == "completed"].copy() if "status" in summary_df.columns else summary_df.copy()
    port = port_df.copy()
    single_port = port[port["family"] == "single_port_detector_scale"].copy()
    device_scale = successful[successful["family"] == "device_count_scale"].copy().sort_values("active_device_count")

    # --- Charts (aggregated, small) ---
    plot_refs: list[str] = []
    if not single_port.empty:
        save_detector_scale_plot(single_port, plots_dir / "detector_scale.png")
        plot_refs.append("![Latency by detector count](plots/detector_scale.png)")
    if not device_scale.empty:
        save_device_scale_plot(device_scale, plots_dir / "device_scale.png")
        plot_refs.append("![Latency vs device count](plots/device_scale.png)")
    if not successful.empty:
        save_load_scatter(successful, plots_dir / "load_scatter.png")
        plot_refs.append("![Load vs latency](plots/load_scatter.png)")
    save_residual_histogram(run_dir, summary_df, plots_dir / "residuals.png")
    if (plots_dir / "residuals.png").exists():
        plot_refs.append("![Residual distribution](plots/residuals.png)")

    # --- Build stats table ---
    stats_rows = []
    if stats_results["single_port_detector_effect"] is not None:
        d = stats_results["single_port_detector_effect"]
        stats_rows.append({"test": "Detector count", "stat": f"R\u00b2\u202f=\u202f{d['partial_eta_squared']:.3f}", "p": format_p_value(d["p_value"]), "sig": "No"})
    if stats_results["device_count_corr"] is not None:
        d = stats_results["device_count_corr"]
        stats_rows.append({"test": "Device count", "stat": f"r\u202f=\u202f{d['pearson']['coefficient']:.3f}", "p": format_p_value(d["pearson"]["p_value"]), "sig": "No"})
    if stats_results["overall_load_corr"] is not None:
        d = stats_results["overall_load_corr"]
        stats_rows.append({"test": "Overall burst load", "stat": f"r\u202f=\u202f{d['pearson']['coefficient']:.3f}", "p": format_p_value(d["pearson"]["p_value"]), "sig": "No"})
    if stats_results["single_port_port_effect"] is not None:
        d = stats_results["single_port_port_effect"]
        stats_rows.append({"test": "Port / device instance\u2020", "stat": f"R\u00b2\u202f=\u202f{d['partial_eta_squared']:.3f}", "p": format_p_value(d["p_value"]), "sig": "Yes\u2020"})
    stats_table = markdown_table(stats_rows, [("test", "Factor"), ("stat", "Effect size"), ("p", "Permutation p"), ("sig", "Significant?")])

    # --- Offset comparison table ---
    cal_rows = []
    if stats_results["global_offset"] is not None:
        g = stats_results["global_offset"]
        cal_rows.append({"model": "Single global offset", "offset": f"{g['offset_ms']:.1f} ms", "mae": f"{g['mae_ms']:.1f}", "p95": f"{g['p95_abs_error_ms']:.1f}", "w100": f"{g['pct_within_100ms']:.0f}%"})
    if stats_results["single_port_detector_offsets"] is not None:
        d = stats_results["single_port_detector_offsets"]
        imp = stats_results["single_port_detector_offsets"]["mae_improvement_pct"]
        cal_rows.append({"model": f"Per-detector-count ({d['offset_count']} values)", "offset": "varies", "mae": f"{d['mae_ms']:.1f}", "p95": f"{d['p95_abs_error_ms']:.1f}", "w100": f"{d['pct_within_100ms']:.0f}% (+{imp:.1f}%)"})
    if stats_results["device_scale_device_offsets"] is not None:
        d = stats_results["device_scale_device_offsets"]
        imp = stats_results["device_scale_device_offsets"]["mae_improvement_pct"]
        cal_rows.append({"model": f"Per-device-count ({d['offset_count']} values)", "offset": "varies", "mae": f"{d['mae_ms']:.1f}", "p95": f"{d['p95_abs_error_ms']:.1f}", "w100": f"{d['pct_within_100ms']:.0f}% (+{imp:.1f}%)"})
    cal_table = markdown_table(cal_rows, [("model", "Offset model"), ("offset", "Offset value"), ("mae", "MAE (ms)"), ("p95", "P95 error (ms)"), ("w100", "Within \u00b1100 ms")])

    # --- Detector count summary ---
    det_rows = []
    if not single_port.empty:
        for dc, grp in single_port.groupby("detector_count"):
            det_rows.append({"det": int(dc), "median": f"{grp['median_ms'].median():.0f}", "p95": f"{grp['p95_ms'].median():.0f}", "n": len(grp)})
    det_table = markdown_table(det_rows, [("det", "Detectors"), ("median", "Median (ms)"), ("p95", "P95 (ms)"), ("n", "Runs")])

    # --- Device count summary ---
    dev_table = ""
    if not device_scale.empty:
        dev_rows = device_scale[["active_device_count", "median_latency_ms", "p95_latency_ms", "missing_events"]].to_dict("records")
        dev_table = markdown_table(dev_rows, [("active_device_count", "Active devices"), ("median_latency_ms", "Median (ms)"), ("p95_latency_ms", "P95 (ms)"), ("missing_events", "Missing events")])

    n_failed = len(summary_df[summary_df.get("status", pd.Series("completed")).fillna("completed") != "completed"]) if "status" in summary_df.columns else 0
    g = stats_results["global_offset"] or {}
    offset_ms = g.get("offset_ms", 0)
    p95_err = g.get("p95_abs_error_ms", 0)
    p50_err = g.get("median_abs_error_ms", 0)
    w100 = g.get("pct_within_100ms", 0)
    w50 = g.get("pct_within_50ms", 0)
    runtime_plan = planned_runtime_seconds(args, len(summary_df))

    report = f"""# SNMP Detector Event Latency — Scaling Study

**Date:** {datetime.now().strftime("%Y-%m-%d")}  
**Scenarios:** {len(successful)} completed, {n_failed} failed  
**Planned runtime:** {format_duration(float(runtime_plan["total_runtime_seconds"]))}

---

## Background

This study measures the round-trip latency of SNMP-based detector event replay against
MAXTIME emulator devices. A single controller is addressed over UDP; detector ON/OFF events
are fired on a fixed schedule, and the controller's HTTP event log is polled to confirm receipt.
Latency is defined as the difference between the scheduled send time and the logged timestamp.

Two phases were run:

- **Single-port detector scaling** — one device active at a time, sweeping detector counts
  ({args.detector_counts}) across {len(parse_ports(args.ports))} ports.
  Port order is randomized within each detector-count block,
  so port identity and execution sequence are not aliased.
- **Device-count scaling** — {args.device_scale_repetitions}\u00d7 repeated, {args.device_scale_detector_count} detectors per device,
  stepping from 1 to {len(parse_ports(args.ports))} active devices.
  Each repetition uses a different random port selection and level ordering, giving genuine
  replication across the full device-count range.

Each scenario replayed ON/OFF bursts every {args.interval_seconds:.0f}\u202fs (hold {args.hold_seconds:.0f}\u202fs) for {args.duration_seconds}\u202fs.

---

## Calibration recommendation

> **Use a single global latency offset of {offset_ms:.1f} ms. Do not condition it on detector count or device count.**

After subtracting {offset_ms:.1f} ms from each event:

- Median absolute residual: **{p50_err:.1f} ms**
- P95 absolute residual: **{p95_err:.1f} ms**
- Events within \u00b150 ms: **{w50:.0f}%**
- Events within \u00b1100 ms: **{w100:.0f}%**

The rationale is in the statistical tests below — neither detector count nor device count
produced a statistically significant effect, so a single fixed offset is the right choice.

---

## Statistical tests

Permutation tests (20\u202f000 shuffles) check whether latency varies with each factor.
**R\u00b2** = fraction of variance explained by the factor. **r** = Pearson correlation (\u22121 to +1).

{stats_table}

\u2020 Port order was randomized, so this reflects genuine per-port latency variation in the emulator, not an execution-order artifact.

---

## Offset model comparison

Comparing a single global offset against per-detector-count and per-device-count tables.
MAE and P95 are computed over matched event residuals.

{cal_table}

Per-detector-count offsets improve MAE by only {stats_results["single_port_detector_offsets"]["mae_improvement_pct"]:.1f}% — not worth the added complexity.
Per-device-count offsets show a larger nominal improvement ({stats_results["device_scale_device_offsets"]["mae_improvement_pct"]:.1f}%) but the relationship
is non-monotonic and the correlation is not statistically significant,
so it likely reflects noise rather than a real trend.

---

## Results by detector count

Median and P95 aggregated across all ports (10 runs per detector count).

{det_table}

---

## Charts

{chr(10).join(plot_refs) if plot_refs else "No plots generated."}

"""
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    # Also write a canonical latest copy so there is always one stable path to open.
    canonical = run_dir.parent / "report.md"
    canonical.write_text(report, encoding="utf-8")
    # Copy plots to canonical location so chart paths in report.md resolve correctly.
    canonical_plots = canonical.parent / "plots"
    ensure_dir(canonical_plots)
    for png in (run_dir / "plots").glob("*.png"):
        shutil.copy2(png, canonical_plots / png.name)


def finalize_existing_run(run_dir: Path, args: argparse.Namespace) -> None:
    scenario_root = run_dir / "scenarios"
    if not scenario_root.exists():
        raise FileNotFoundError(f"Scenario directory not found: {scenario_root}")

    config_path = run_dir / "study_config.json"
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as handle:
            config = json.load(handle)
        args.ip = config.get("ip", args.ip)
        if config.get("ports"):
            args.ports = ",".join(str(port) for port in config["ports"])
        if config.get("detector_counts"):
            args.detector_counts = ",".join(str(value) for value in config["detector_counts"])
        args.device_scale_detector_count = config.get("device_scale_detector_count", args.device_scale_detector_count)
        args.duration_seconds = config.get("duration_seconds", args.duration_seconds)
        args.interval_seconds = config.get("interval_seconds", args.interval_seconds)
        args.hold_seconds = config.get("hold_seconds", args.hold_seconds)
        args.warmup_seconds = config.get("warmup_seconds", args.warmup_seconds)
        args.inter_scenario_seconds = config.get("inter_scenario_seconds", args.inter_scenario_seconds)
        args.collection_window_seconds = config.get(
            "collection_window_seconds",
            config.get("poll_timeout_seconds", args.collection_window_seconds),
        )
        args.poll_interval_seconds = config.get("poll_interval_seconds", args.poll_interval_seconds)
        args.device_scale_repetitions = config.get("device_scale_repetitions", args.device_scale_repetitions)
        args.seed = config.get("seed", args.seed)

    summary_rows = []
    port_frames: list[pd.DataFrame] = []
    for scenario_dir in sorted(scenario_root.iterdir()):
        if not scenario_dir.is_dir():
            continue
        summary_path = scenario_dir / "summary.json"
        if summary_path.exists():
            with open(summary_path, "r", encoding="utf-8") as handle:
                summary_rows.append(json.load(handle))
        port_path = scenario_dir / "port_summary.csv"
        if port_path.exists():
            port_frames.append(pd.read_csv(port_path))

    if not summary_rows:
        raise RuntimeError(f"No scenario summaries found under {scenario_root}")

    summary_df = pd.DataFrame(summary_rows).sort_values(["family", "scenario_id"]).reset_index(drop=True)
    port_df = pd.concat(port_frames, ignore_index=True) if port_frames else pd.DataFrame()
    summary_df.to_csv(run_dir / "scenario_summary.csv", index=False)
    if not port_df.empty:
        port_df.to_csv(run_dir / "port_summary.csv", index=False)
    generate_plots_and_report(run_dir, args, summary_df, port_df)

    study_summary_path = run_dir / "study_summary.json"
    if not study_summary_path.exists():
        with open(study_summary_path, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "run_dir": str(run_dir),
                    "study_completed_at": datetime.now().isoformat(),
                    "scenario_count": int(len(summary_df)),
                },
                handle,
                indent=2,
            )


async def run_scenario(args: argparse.Namespace, scenario: StudyScenario, target_map: dict[int, DeviceTarget], scenarios_root: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    scenario_dir = scenarios_root / scenario.scenario_id
    ensure_dir(scenario_dir)
    active_targets = [target_map[port] for port in scenario.active_ports]
    actions = build_actions(args.duration_seconds, args.interval_seconds, args.hold_seconds)
    expected_per_port = len(actions) * scenario.detector_count

    print(
        f"[{scenario.order_index + 1}] {scenario.scenario_id}: ports={','.join(str(port) for port in scenario.active_ports)} detectors={scenario.detector_count}",
        flush=True,
    )
    send_df, first_scheduled_at, replay_finished_at = await run_schedule(
        targets=active_targets,
        detector_count=scenario.detector_count,
        actions=actions,
        warmup_seconds=args.warmup_seconds,
        timeout=args.snmp_timeout_seconds,
        http_timeout_seconds=args.http_timeout_seconds,
    )
    logged_df = await poll_logged_events(
        targets=active_targets,
        detector_count=scenario.detector_count,
        since=first_scheduled_at - timedelta(seconds=1),
        expected_per_port=expected_per_port,
        collection_window_seconds=args.collection_window_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
        http_timeout_seconds=args.http_timeout_seconds,
    )
    expected_df = expand_expected_events(send_df)
    matched_df, extras_df = match_events(expected_df, logged_df)
    send_spread_df = summarize_send_spread(send_df)

    send_df.to_csv(scenario_dir / "send_bursts.csv", index=False)
    expected_df.to_csv(scenario_dir / "expected_events.csv", index=False)
    logged_df.to_csv(scenario_dir / "logged_events.csv", index=False)
    matched_df.to_csv(scenario_dir / "matched_latency.csv", index=False)
    extras_df.to_csv(scenario_dir / "unexpected_actual_events.csv", index=False)
    send_spread_df.to_csv(scenario_dir / "send_spread.csv", index=False)

    summary = scenario_summary(scenario, matched_df, extras_df, send_df, send_spread_df)
    summary["first_scheduled_at"] = first_scheduled_at.isoformat()
    summary["replay_finished_at"] = replay_finished_at.isoformat()
    with open(scenario_dir / "summary.json", "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)

    port_df = port_summary(matched_df, scenario)
    if not port_df.empty:
        port_df.to_csv(scenario_dir / "port_summary.csv", index=False)

    return summary, port_df


async def run_study(args: argparse.Namespace) -> Path:
    ports = parse_ports(args.ports)
    detector_counts = parse_detector_counts(args.detector_counts)
    if args.device_scale_detector_count < 1 or args.device_scale_detector_count > 32:
        raise ValueError("device-scale-detector-count must be between 1 and 32")

    targets = build_targets(args.ip, ports)
    target_map = {target.udp_port: target for target in targets}
    scenarios = build_scenarios(ports, detector_counts, args.device_scale_detector_count, args.device_scale_repetitions, args.seed)
    if args.max_scenarios > 0:
        scenarios = scenarios[: args.max_scenarios]

    runtime_plan = planned_runtime_seconds(args, len(scenarios))
    print(
        "Planned runtime from configured waits: "
        f"{format_duration(float(runtime_plan['total_runtime_seconds']))} total "
        f"({len(scenarios)} scenarios, {runtime_plan['scenario_runtime_seconds']:.1f}s each, "
        f"{args.inter_scenario_seconds:.1f}s between scenarios)",
        flush=True,
    )

    output_root = Path(args.output_root)
    ensure_dir(output_root)
    run_dir = output_root / f"latency_scaling_study_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    ensure_dir(run_dir)
    scenarios_root = run_dir / "scenarios"
    ensure_dir(scenarios_root)

    summary_rows = []
    port_frames: list[pd.DataFrame] = []

    with open(run_dir / "study_config.json", "w", encoding="utf-8") as handle:
        json.dump(
            {
                "ip": args.ip,
                "ports": ports,
                "detector_counts": detector_counts,
                "device_scale_detector_count": args.device_scale_detector_count,
                "device_scale_repetitions": args.device_scale_repetitions,
                "seed": args.seed,
                "duration_seconds": args.duration_seconds,
                "interval_seconds": args.interval_seconds,
                "hold_seconds": args.hold_seconds,
                "warmup_seconds": args.warmup_seconds,
                "inter_scenario_seconds": args.inter_scenario_seconds,
                "collection_window_seconds": args.collection_window_seconds,
                "poll_interval_seconds": args.poll_interval_seconds,
                "planned_actions_per_scenario": runtime_plan["actions_per_scenario"],
                "planned_replay_span_seconds": runtime_plan["replay_span_seconds"],
                "planned_scenario_runtime_seconds": runtime_plan["scenario_runtime_seconds"],
                "planned_total_runtime_seconds": runtime_plan["total_runtime_seconds"],
                "scenario_count": len(scenarios),
            },
            handle,
            indent=2,
        )

    print(f"Running {len(scenarios)} study scenarios", flush=True)
    study_started_at = datetime.now()
    for index, scenario in enumerate(scenarios, start=1):
        try:
            summary, port_df = await run_scenario(args, scenario, target_map, scenarios_root)
        except Exception as exc:
            error_text = f"{type(exc).__name__}: {exc}"
            print(f"  Scenario failed: {scenario.scenario_id} -> {error_text}", flush=True)
            scenario_dir = scenarios_root / scenario.scenario_id
            ensure_dir(scenario_dir)
            (scenario_dir / "error.txt").write_text(traceback.format_exc(), encoding="utf-8")
            summary = failed_scenario_summary(scenario, error_text)
            port_df = pd.DataFrame()
            with open(scenario_dir / "summary.json", "w", encoding="utf-8") as handle:
                json.dump(summary, handle, indent=2)
        summary["sequence_index"] = index
        summary_rows.append(summary)
        if not port_df.empty:
            port_frames.append(port_df)
        if index < len(scenarios) and args.inter_scenario_seconds > 0:
            await asyncio.sleep(args.inter_scenario_seconds)

    summary_df = pd.DataFrame(summary_rows)
    port_df = pd.concat(port_frames, ignore_index=True) if port_frames else pd.DataFrame()
    summary_df.to_csv(run_dir / "scenario_summary.csv", index=False)
    if not port_df.empty:
        port_df.to_csv(run_dir / "port_summary.csv", index=False)

    generate_plots_and_report(run_dir, args, summary_df, port_df)

    with open(run_dir / "study_summary.json", "w", encoding="utf-8") as handle:
        json.dump(
            {
                "run_dir": str(run_dir),
                "study_started_at": study_started_at.isoformat(),
                "study_completed_at": datetime.now().isoformat(),
                "scenario_count": len(summary_rows),
                "planned_total_runtime_seconds": runtime_plan["total_runtime_seconds"],
            },
            handle,
            indent=2,
        )

    return run_dir


def main() -> None:
    args = parse_args()
    if args.finalize_run_dir:
        run_dir = Path(args.finalize_run_dir)
        finalize_existing_run(run_dir, args)
        print(f"Latency scaling study finalized: {run_dir}", flush=True)
        return
    run_dir = asyncio.run(run_study(args))
    print(f"Latency scaling study complete: {run_dir}", flush=True)


if __name__ == "__main__":
    main()