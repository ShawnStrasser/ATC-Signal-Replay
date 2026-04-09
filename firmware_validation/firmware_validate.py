#!/usr/bin/env python
"""
firmware_validate.py — Standalone firmware validation script.

Replays source logs to controllers with new firmware, compares collected output
to the configured baseline, and generates an HTML report with divergence charts.

Usage:
    python firmware_validate.py                  # interactive, uses settings.json
    python firmware_validate.py --verbose        # extra debug output
    python firmware_validate.py --report-only    # skip replay, just run analysis
    python firmware_validate.py --settings custom_settings.json

Settings are loaded from settings.json (editable JSON file in the same folder).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from datetime import datetime, time as dt_time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import pandas as pd
import requests
from openpyxl import load_workbook

import signal_replay as sr
from signal_replay.ntcip import send_ntcip
from signal_replay.report import generate_report

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
_VERBOSE = False


def log(msg: str, *, always: bool = True) -> None:
    """Print a message. If always=False, only prints in verbose mode."""
    if always or _VERBOSE:
        print(msg, flush=True)


def vlog(msg: str) -> None:
    """Verbose-only log."""
    log(msg, always=False)


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
def load_settings(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_results_dir(firmware_dir: Path, settings: dict) -> Path:
    return firmware_dir / settings["results_dir"]


def get_version_logs_dir(firmware_dir: Path, settings: dict, version: str) -> Path:
    return get_results_dir(firmware_dir, settings) / version / "logs"


def resolve_baseline_logs_dir(firmware_dir: Path, settings: dict) -> Tuple[Optional[Path], str]:
    baseline_dir = get_version_logs_dir(firmware_dir, settings, settings["baseline_version"])
    if baseline_dir.exists():
        return baseline_dir, settings["baseline_version"]
    return None, f"{settings['baseline_version']} (source logs)"


def resolve_baseline_log(
    tssu: str,
    firmware_dir: Path,
    settings: dict,
) -> Tuple[Optional[Path], str]:
    baseline_dir, baseline_label = resolve_baseline_logs_dir(firmware_dir, settings)
    if baseline_dir is not None:
        return find_log(tssu, baseline_dir), baseline_label
    return find_log(tssu, firmware_dir / settings["logs_dir"]), baseline_label


# ---------------------------------------------------------------------------
# Catalog helpers
# ---------------------------------------------------------------------------
def read_catalog(catalog_path: Path) -> List[dict]:
    wb = load_workbook(catalog_path, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    header = [str(h).strip().lower() for h in rows[0]]
    catalog: List[dict] = []
    for row in rows[1:]:
        if not any(row):
            continue
        r = dict(zip(header, [str(v).strip() if v is not None else "" for v in row]))
        raw_cl = r.get("cyclelength", "")
        raw_off = r.get("offset", "")
        cycle_length = int(float(raw_cl)) if raw_cl not in ("", "None") else 0
        offset = float(raw_off) if raw_off not in ("", "None") else 0.0
        catalog.append({
            "TSSU": r.get("tssu", ""),
            "Version": r.get("version", ""),
            "Type": r.get("type", "Similarity") or "Similarity",
            "CycleLength": cycle_length,
            "Offset": offset,
            "Notes": r.get("notes", ""),
            "ReplaceOnRerun": r.get("replace on rerun", ""),
        })
    return catalog


def find_log(tssu: str, logs_dir: Path) -> Optional[Path]:
    for ext in (".parquet", ".csv", ".db"):
        p = logs_dir / f"{tssu}{ext}"
        if p.exists():
            return p
    matches = sorted(logs_dir.glob(f"{tssu}*"))
    return matches[0] if matches else None


def find_database(tssu: str, databases_dir: Path) -> Optional[Path]:
    matches = sorted(databases_dir.glob(f"{tssu}*"))
    return matches[0] if matches else None


def normalize_target(target: str) -> str:
    parts = target.split(":")
    if len(parts) == 2:
        host, port_text = parts
        port = int(port_text)
        return f"{host}:{port}:{port}"
    if len(parts) == 3:
        host, udp_text, http_text = parts
        return f"{host}:{int(udp_text)}:{int(http_text)}"
    raise ValueError(f"Invalid controller target: {target}")


# ---------------------------------------------------------------------------
# Build suite
# ---------------------------------------------------------------------------
def build_suite(
    settings: dict,
    firmware_dir: Path,
    catalog: List[dict],
    conflict_pairs: dict,
) -> Tuple[sr.FirmwareTestSuite, dict]:
    """Build the full test suite and return it with the discovered input file map."""

    logs_dir = firmware_dir / settings["logs_dir"]
    databases_dir = firmware_dir / settings["databases_dir"]
    firmware_version = settings["firmware_version"]
    _baseline_logs_dir, baseline_version = resolve_baseline_logs_dir(firmware_dir, settings)

    file_map: Dict[str, dict] = {}
    for r in catalog:
        tssu = r["TSSU"]
        file_map[tssu] = {
            "log": find_log(tssu, logs_dir),
            "db": find_database(tssu, databases_dir),
        }

    similarity_scenarios: List[sr.TestScenario] = []
    conflict_scenarios: List[sr.TestScenario] = []
    skipped: List[str] = []

    for r in catalog:
        tssu = r["TSSU"]
        log_path = file_map[tssu]["log"]
        db_path = file_map[tssu]["db"]
        if not log_path:
            skipped.append(tssu)
            continue

        test_type = sr.TestType.CONFLICT if r["Type"].strip().lower() == "conflict" else sr.TestType.SIMILARITY
        cycle_length = r.get("CycleLength", 0) or 0
        offset = r.get("Offset", 0.0) or 0.0
        tod_align = cycle_length == 0

        kwargs: dict = {}
        if test_type == sr.TestType.CONFLICT:
            kwargs["replays"] = 25
            pairs_key = tssu if tssu in conflict_pairs else tssu.rstrip("_c") if tssu.endswith("_c") else tssu
            if pairs_key in conflict_pairs:
                kwargs["incompatible_pairs"] = conflict_pairs[pairs_key]

        scenario = sr.TestScenario(
            scenario_id=tssu,
            database_name=str(db_path) if db_path else f"{tssu}.bin",
            events_source=str(log_path),
            test_type=test_type,
            description=f"{r['Type']} | {r['Notes']}" if r["Notes"] else r["Type"],
            notes_column=r["Notes"],
            cycle_length=cycle_length,
            cycle_offset=offset,
            tod_align=tod_align,
            **kwargs,
        )
        if test_type == sr.TestType.CONFLICT:
            conflict_scenarios.append(scenario)
        else:
            similarity_scenarios.append(scenario)

    scenarios = similarity_scenarios + conflict_scenarios

    if skipped:
        log(f"Skipped (no log): {', '.join(skipped)}")

    comp = settings.get("comparison", {})
    configured_settle_minutes = comp.get("settle_minutes", 10.0)
    analysis_start_time = str(comp.get("analysis_start_time", "")).strip()
    analysis_end_time = str(comp.get("analysis_end_time", "")).strip()
    effective_settle_minutes = configured_settle_minutes
    if analysis_start_time and scenarios and all(s.tod_align for s in scenarios):
        effective_settle_minutes = 0.0

    suite = sr.FirmwareTestSuite(
        suite_name="Firmware Validation",
        firmware_version=firmware_version,
        baseline_version=baseline_version,
        scenarios=scenarios,
        batches=[],
        output_dir=str(firmware_dir / settings["results_dir"]),
        comparison_thresholds=sr.ComparisonThresholds(
            sequence_threshold=comp.get("sequence_threshold", 0.05),
            timing_threshold=comp.get("timing_threshold", 0.02),
            match_threshold=comp.get("match_threshold", 95.0),
        ),
        phase_call_similarity_threshold=comp.get("phase_call_similarity_threshold", comp.get("detector_similarity_threshold", 90.0)),
        analysis_settle_minutes=effective_settle_minutes,
        analysis_start_time=analysis_start_time,
        analysis_end_time=analysis_end_time,
    )

    return suite, file_map


def _replace_on_rerun_enabled(value: object) -> bool:
    return str(value).strip().lower() == "yes"


def _shared_collected_db_path(suite: sr.FirmwareTestSuite) -> Path:
    return Path(suite.output_dir) / suite.firmware_version / "collected.duckdb"


def _get_collected_device_ids(db_path: Path) -> set[str]:
    if not db_path.exists():
        return set()

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {
            row[0].lower()
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }

        device_ids: set[str] = set()
        if "events" in tables:
            device_ids.update(
                row[0]
                for row in con.execute(
                    "SELECT DISTINCT device_id FROM events WHERE device_id IS NOT NULL"
                ).fetchall()
            )
        if "conflicts" in tables:
            device_ids.update(
                row[0]
                for row in con.execute(
                    "SELECT DISTINCT device_id FROM conflicts WHERE device_id IS NOT NULL"
                ).fetchall()
            )
        return device_ids
    finally:
        con.close()


def _select_pending_replay_batch(
    suite: sr.FirmwareTestSuite,
    settings: dict,
    catalog: List[dict],
    existing_ids: set[str],
) -> Tuple[Optional[sr.TestBatch], List[sr.TestScenario], List[str], List[str]]:
    replace_flags = {
        row["TSSU"]: _replace_on_rerun_enabled(row.get("ReplaceOnRerun", ""))
        for row in catalog
    }
    pending_scenarios = [
        scenario
        for scenario in suite.scenarios
        if replace_flags.get(scenario.scenario_id, False) or scenario.scenario_id not in existing_ids
    ]
    if not pending_scenarios:
        return None, [], [], []

    normalized_targets = [normalize_target(target) for target in settings["controller_targets"]]
    selected = pending_scenarios[:len(normalized_targets)]
    batch = sr.TestBatch(
        batch_id="pending_replay",
        assignments={scenario.scenario_id: target for scenario, target in zip(selected, normalized_targets)},
        description=f"Pending replay batch: {len(selected)} scenario(s)",
    )
    replace_selected = [
        scenario.scenario_id
        for scenario in selected
        if replace_flags.get(scenario.scenario_id, False)
    ]
    skipped_existing = [
        scenario.scenario_id
        for scenario in suite.scenarios
        if scenario.scenario_id in existing_ids and not replace_flags.get(scenario.scenario_id, False)
    ]
    return batch, pending_scenarios[len(selected):], replace_selected, skipped_existing


def _clear_selected_devices_for_rerun(db_path: Path, scenario_ids: List[str]) -> None:
    if not scenario_ids or not db_path.exists():
        return

    sr.DatabaseManager(str(db_path)).clear_device_data(scenario_ids)


# ---------------------------------------------------------------------------
# Controller check
# ---------------------------------------------------------------------------
def parse_target(target: str) -> Tuple[str, int, int]:
    parts = target.split(":")
    if len(parts) == 2:
        ip, port_text = parts
        port = int(port_text)
        return ip, port, port
    if len(parts) == 3:
        ip, udp_text, http_text = parts
        return ip, int(udp_text), int(http_text)
    raise ValueError(f"Invalid target format: {target}")


def check_controller(target: str) -> bool:
    ip, udp_port, http_port = parse_target(target)
    try:
        send_ntcip((ip, udp_port), 1, 1, "Vehicle", timeout=1.0)
        send_ntcip((ip, udp_port), 1, 0, "Vehicle", timeout=1.0)
        snmp_ok = True
    except Exception:
        snmp_ok = False
    try:
        r = requests.get(f"http://{ip}:{http_port}/v1/asclog/xml/full", timeout=2.0, verify=False)
        http_ok = r.status_code == 200
    except Exception:
        http_ok = False
    return snmp_ok and http_ok


def wait_for_controllers(targets: List[str], labels: List[str]) -> None:
    """Poll controllers until all respond OK. Print status each cycle."""
    log("Polling controllers...")
    while True:
        with ThreadPoolExecutor() as ex:
            statuses = list(ex.map(check_controller, targets))
        parts = []
        for ok, lbl in zip(statuses, labels):
            parts.append(f"  OK   {lbl}" if ok else f"  FAIL {lbl}")
        print("\r\033[K" + "\n".join(parts), flush=True)
        if all(statuses):
            log("All controllers responding.")
            break
        time.sleep(3)
        # Move cursor up to overwrite
        print(f"\033[{len(parts)}A", end="", flush=True)


# ---------------------------------------------------------------------------
# Replay operations
# ---------------------------------------------------------------------------
def run_batch(suite: sr.FirmwareTestSuite, batch: sr.TestBatch) -> Path:
    runner = sr.BatchRunner(suite, debug=_VERBOSE)

    def auto_db_loader(db_name: str, target: str) -> bool:
        vlog(f"  [auto] DB {db_name} -> {target}")
        return True

    try:
        return runner.run_batch_once(batch, db_loader_callback=auto_db_loader)
    except KeyboardInterrupt:
        log("\nKeyboard interrupt received. Requesting replay shutdown...")
        runner.stop()
        raise


# ---------------------------------------------------------------------------
# Analysis (comparison) — designed for multiprocessing
# ---------------------------------------------------------------------------
def _extract_collected_events(
    suite: sr.FirmwareTestSuite,
    collected_db_path: Path,
    output_dir: Path,
) -> Dict[str, Path]:
    """
    Extract collected events from DuckDB into versioned parquet files.
    Returns a mapping of scenario_id -> parquet path.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    if not collected_db_path.exists():
        return {}

    extracted: Dict[str, Path] = {}
    vlog(f"  Extracting from {collected_db_path.name}: {len(suite.scenarios)} scenarios")
    con = duckdb.connect(str(collected_db_path), read_only=True)
    try:
        for scenario in suite.scenarios:
            df = con.execute(
                "SELECT * FROM events WHERE device_id = ? ORDER BY timestamp",
                [scenario.scenario_id],
            ).df()
            out_path = output_dir / f"{scenario.scenario_id}.parquet"
            df.to_parquet(out_path, index=False)
            extracted[scenario.scenario_id] = out_path
    finally:
        con.close()
    return extracted


def _load_collected_events_from_duckdb(db_path: Path | str, scenario_id: str) -> pd.DataFrame:
    """Load collected events for a single scenario directly from DuckDB."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        columns = [row[1] for row in con.execute("PRAGMA table_info('events')").fetchall()]
        required = {"device_id", "timestamp", "event_id", "parameter"}
        missing = sorted(required - set(columns))
        if missing:
            raise ValueError(
                f"DuckDB events table at {db_path} is missing required columns: {missing}"
            )

        select_columns = [
            col for col in ("device_id", "run_number", "timestamp", "event_id", "parameter")
            if col in columns
        ]
        order_columns = [
            col for col in ("run_number", "timestamp", "event_id", "parameter")
            if col in columns
        ]
        query = (
            f"SELECT {', '.join(select_columns)} FROM events WHERE device_id = ? "
            f"ORDER BY {', '.join(order_columns)}"
        )
        return con.execute(query, [scenario_id]).df()
    finally:
        con.close()


def _missing_collected_error(scenario_id: str, collected_db_path: Path | str) -> str:
    return (
        f"No collected events found for {scenario_id} in {Path(collected_db_path).name}. "
        "Replay data for this scenario is missing, so the comparison and device CSV export are invalid "
        "until that device is collected again."
    )


def _get_timestamp_col(df: pd.DataFrame) -> str:
    """Return the timestamp column name used by a raw events DataFrame."""
    return next(
        (col for col in df.columns if col.lower() in ("timestamp", "time_stamp")),
        "timestamp",
    )


def _date_only_shifted_copy(
    df: pd.DataFrame,
    ts_col: str,
    target_ts: pd.Timestamp,
) -> pd.DataFrame:
    """Shift a run onto the target date while preserving time-of-day."""
    shifted = df.copy()
    if shifted.empty:
        return shifted

    shifted[ts_col] = pd.to_datetime(shifted[ts_col])
    source_start = shifted[ts_col].min()
    date_shift = target_ts.normalize() - source_start.normalize()
    shifted[ts_col] = shifted[ts_col] + date_shift
    return shifted


def _parse_analysis_clock_time(value: Optional[str], setting_name: str) -> Optional[dt_time]:
    """Parse a manual HH:MM[:SS] analysis clock time from settings."""
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue

    raise ValueError(
        f"comparison.{setting_name} must use HH:MM or HH:MM:SS format"
    )


def _resolve_manual_analysis_window(
    collected: pd.DataFrame,
    *,
    analysis_start_time: Optional[str],
    analysis_end_time: Optional[str],
) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """Resolve manual TOD analysis bounds onto the collected run dates."""
    parsed_start = _parse_analysis_clock_time(analysis_start_time, "analysis_start_time")
    parsed_end = _parse_analysis_clock_time(analysis_end_time, "analysis_end_time")
    if collected.empty or (parsed_start is None and parsed_end is None):
        return None, None

    collected_ts_col = _get_timestamp_col(collected)
    collected_ts = pd.to_datetime(collected[collected_ts_col])
    min_date = collected_ts.min().normalize()
    max_date = collected_ts.max().normalize()

    analysis_start = None
    if parsed_start is not None:
        analysis_start = min_date + pd.Timedelta(
            hours=parsed_start.hour,
            minutes=parsed_start.minute,
            seconds=parsed_start.second,
        )

    analysis_end = None
    if parsed_end is not None:
        end_date = min_date
        if parsed_start is not None and parsed_end < parsed_start:
            end_date = max_date
        analysis_end = end_date + pd.Timedelta(
            hours=parsed_end.hour,
            minutes=parsed_end.minute,
            seconds=parsed_end.second,
        )

    return analysis_start, analysis_end


def _prepare_analysis_inputs(
    original: pd.DataFrame,
    collected: pd.DataFrame,
    *,
    tod_align: bool,
) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[datetime], Optional[datetime]]:
    """Prepare scenario inputs for comparison/reporting.

    TOD scenarios are moved onto the collected run's date and anchored to a
    shared wall-clock reference so comparison preserves real time-of-day gaps.
    """
    original_prepared = original.copy()
    collected_prepared = collected.copy()

    if original_prepared.empty or collected_prepared.empty:
        return original_prepared, collected_prepared, None, None

    original_ts_col = _get_timestamp_col(original_prepared)
    collected_ts_col = _get_timestamp_col(collected_prepared)
    original_prepared[original_ts_col] = pd.to_datetime(original_prepared[original_ts_col])
    collected_prepared[collected_ts_col] = pd.to_datetime(collected_prepared[collected_ts_col])

    if not tod_align:
        return original_prepared, collected_prepared, None, None

    collected_start = collected_prepared[collected_ts_col].min()
    original_prepared = _date_only_shifted_copy(original_prepared, original_ts_col, collected_start)
    shared_start = original_prepared[original_ts_col].min().to_pydatetime()
    return original_prepared, collected_prepared, shared_start, shared_start


def _trim_to_analysis_window(
    df: pd.DataFrame,
    analysis_start: Optional[pd.Timestamp],
    analysis_end: Optional[pd.Timestamp],
) -> pd.DataFrame:
    """Trim a raw events DataFrame to the configured analysis window."""
    if df.empty or (analysis_start is None and analysis_end is None):
        return df
    ts_col = _get_timestamp_col(df)
    trimmed = df.copy()
    trimmed[ts_col] = pd.to_datetime(trimmed[ts_col])
    mask = pd.Series(True, index=trimmed.index)
    if analysis_start is not None:
        mask &= trimmed[ts_col] >= analysis_start
    if analysis_end is not None:
        mask &= trimmed[ts_col] <= analysis_end
    return trimmed[mask].reset_index(drop=True)


def _normalize_conflict_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw event logs into the shape expected by conflict analysis."""
    if events_df.empty:
        return pd.DataFrame(columns=["run_number", "TimeStamp", "EventTypeID", "Parameter"])

    df = events_df.copy()
    rename_map: Dict[str, str] = {}

    timestamp_col = _get_timestamp_col(df)
    if timestamp_col not in df.columns:
        raise ValueError("Conflict analysis requires a timestamp column")
    if timestamp_col != "TimeStamp":
        rename_map[timestamp_col] = "TimeStamp"

    event_col = next(
        (col for col in df.columns if col.lower() in ("event_id", "eventid", "eventtypeid")),
        None,
    )
    if event_col is None:
        raise ValueError("Conflict analysis requires an event_id/EventTypeID column")
    if event_col != "EventTypeID":
        rename_map[event_col] = "EventTypeID"

    parameter_col = next(
        (col for col in df.columns if col.lower() == "parameter"),
        None,
    )
    if parameter_col is None:
        raise ValueError("Conflict analysis requires a parameter column")
    if parameter_col != "Parameter":
        rename_map[parameter_col] = "Parameter"

    if rename_map:
        df = df.rename(columns=rename_map)

    if "run_number" not in df.columns:
        df["run_number"] = 1

    df["run_number"] = pd.to_numeric(df["run_number"], errors="coerce").fillna(1).astype(int)
    df["TimeStamp"] = pd.to_datetime(df["TimeStamp"])
    df["EventTypeID"] = pd.to_numeric(df["EventTypeID"], errors="raise").astype(int)
    df["Parameter"] = pd.to_numeric(df["Parameter"], errors="raise").astype(int)

    return (
        df[["run_number", "TimeStamp", "EventTypeID", "Parameter"]]
        .sort_values(["run_number", "TimeStamp", "EventTypeID", "Parameter"])
        .reset_index(drop=True)
    )


def _normalize_device_csv_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw events into a single human-readable CSV schema."""
    if events_df.empty:
        return pd.DataFrame(columns=["timestamp", "EventId", "Parameter"])

    df = events_df.copy()
    rename_map: Dict[str, str] = {}

    timestamp_col = _get_timestamp_col(df)
    if timestamp_col != "timestamp":
        rename_map[timestamp_col] = "timestamp"

    event_col = next(
        (col for col in df.columns if col.lower() in ("event_id", "eventid", "eventtypeid")),
        None,
    )
    if event_col is None:
        raise ValueError("Device CSV export requires an event_id/EventId/EventTypeID column")
    if event_col != "EventId":
        rename_map[event_col] = "EventId"

    parameter_col = next(
        (col for col in df.columns if col.lower() in ("parameter", "param")),
        None,
    )
    if parameter_col is None:
        raise ValueError("Device CSV export requires a parameter column")
    if parameter_col != "Parameter":
        rename_map[parameter_col] = "Parameter"

    if rename_map:
        df = df.rename(columns=rename_map)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["EventId"] = pd.to_numeric(df["EventId"], errors="raise").astype(int)
    df["Parameter"] = pd.to_numeric(df["Parameter"], errors="raise").astype(int)

    return df[["timestamp", "EventId", "Parameter"]].copy()


def _summarize_conflicts_from_saved_events(
    events_df: pd.DataFrame,
    incompatible_pairs: Optional[List[Tuple[str, str]]],
) -> Tuple[List[dict], int]:
    """Compute per-run conflict records directly from saved event logs."""
    if events_df.empty:
        return [], 0
    if not incompatible_pairs:
        normalized = _normalize_conflict_events(events_df)
        run_count = int(normalized["run_number"].nunique()) if not normalized.empty else 0
        return [], run_count

    normalized = _normalize_conflict_events(events_df)
    if normalized.empty:
        return [], 0

    conflicts_found: List[dict] = []
    run_numbers = normalized["run_number"].drop_duplicates().tolist()
    for run_number in run_numbers:
        run_events = normalized.loc[
            normalized["run_number"] == run_number,
            ["TimeStamp", "EventTypeID", "Parameter"],
        ]
        run_conflicts = sr.check_conflicts(run_events, incompatible_pairs)
        if run_conflicts.empty:
            continue

        run_conflicts = run_conflicts.sort_values("TimeStamp")
        for row in run_conflicts.itertuples(index=False):
            conflicts_found.append({
                "run_number": int(run_number),
                "timestamp": pd.Timestamp(row.TimeStamp).isoformat(sep=" "),
                "conflict_details": row.Conflict_Details,
            })

    return conflicts_found, len(run_numbers)


def _analyze_conflict_scenario(
    scenario: sr.TestScenario,
    baseline_source: Path,
    baseline_label: str,
    collected_db_path: Path,
    firmware_version: str,
) -> sr.ScenarioResult:
    """Build a conflict result from persisted baseline/new logs."""
    baseline_events = sr.load_events(str(baseline_source))
    collected_events = _load_collected_events_from_duckdb(collected_db_path, scenario.scenario_id)

    if collected_events.empty:
        return sr.ScenarioResult(
            scenario_id=scenario.scenario_id,
            test_type=sr.TestType.CONFLICT,
            firmware_version=firmware_version,
            passed=False,
            runs_completed=0,
            total_runs=scenario.replays,
            error=_missing_collected_error(scenario.scenario_id, collected_db_path),
            notes_column=scenario.notes_column,
        )

    baseline_conflicts, baseline_runs = _summarize_conflicts_from_saved_events(
        baseline_events,
        scenario.incompatible_pairs,
    )
    new_conflicts, runs_completed = _summarize_conflicts_from_saved_events(
        collected_events,
        scenario.incompatible_pairs,
    )

    baseline_has_conflict = bool(baseline_conflicts)
    new_has_conflict = bool(new_conflicts)
    configured_pairs = bool(scenario.incompatible_pairs)

    notes: List[str] = []
    if not configured_pairs:
        notes.append("No incompatible_pairs configured for this conflict scenario.")
    elif baseline_has_conflict:
        notes.append(
            f"{baseline_label} reproduced {len(baseline_conflicts)} conflict signature(s) across {baseline_runs} run(s)."
        )
    else:
        notes.append(f"{baseline_label} did not reproduce the configured conflict; test validity warning.")

    if new_has_conflict:
        conflict_runs = sorted({record["run_number"] for record in new_conflicts})
        notes.append(
            f"Conflict observed on {firmware_version} in run(s): {', '.join(str(run) for run in conflict_runs)}."
        )
    else:
        notes.append(f"No conflicts detected on {firmware_version} across {runs_completed} completed run(s).")

    passed = configured_pairs and baseline_has_conflict and not new_has_conflict

    return sr.ScenarioResult(
        scenario_id=scenario.scenario_id,
        test_type=sr.TestType.CONFLICT,
        firmware_version=firmware_version,
        passed=passed,
        conflicts_found=new_conflicts,
        runs_completed=runs_completed,
        total_runs=scenario.replays,
        notes=" ".join(notes),
        notes_column=scenario.notes_column,
    )


def _compute_export_shift(
    original: pd.DataFrame,
    collected: pd.DataFrame,
    *,
    original_ts_col: str,
    collected_ts_col: str,
    tod_align: bool,
    group_tolerance: float,
) -> pd.Timedelta:
    """Return the timestamp shift to apply to original events for export."""
    if original.empty or collected.empty:
        return pd.Timedelta(0)

    original[original_ts_col] = pd.to_datetime(original[original_ts_col])
    collected[collected_ts_col] = pd.to_datetime(collected[collected_ts_col])

    if tod_align:
        return collected[collected_ts_col].min().normalize() - original[original_ts_col].min().normalize()

    prep_a = sr.prepare_events_for_comparison(original)
    prep_b = sr.prepare_events_for_comparison(collected)
    if prep_a.empty or prep_b.empty:
        return pd.Timedelta(0)

    shift_sec = sr.find_temporal_offset(
        prep_a,
        prep_b,
        group_tolerance=group_tolerance,
    )
    original_start = prep_a["timestamp"].min()
    collected_start = prep_b["timestamp"].min()
    return (collected_start - original_start) - pd.Timedelta(seconds=shift_sec)


def _compare_one_scenario(args: Tuple) -> dict:
    """
    Worker function for ProcessPoolExecutor.
    All arguments are plain strings/numbers to avoid pickle issues.
    Reads collected output directly from DuckDB.
    """
    (scenario_id, baseline_events_source, baseline_label, test_type_str, collected_db_path,
     firmware_version, plots_dir_str, settle_minutes, group_tolerance,
      max_plots, window_minutes, verbose, notes_column, tod_align,
          analysis_start_time, analysis_end_time, phase_call_threshold) = args

    import signal_replay as sr
    import pandas as pd
    import os, contextlib

    test_type = sr.TestType.CONFLICT if test_type_str == "CONFLICT" else sr.TestType.SIMILARITY

    baseline = sr.load_events(baseline_events_source)
    collected = _load_collected_events_from_duckdb(collected_db_path, scenario_id)
    if collected.empty:
        return {
            "scenario_id": scenario_id,
            "passed": False,
            "match_percentage": None,
            "num_divergences": 0,
            "summary": "",
            "error": _missing_collected_error(scenario_id, collected_db_path),
            "plot_paths": [],
            "phase_diffs": [],
            "operational_diffs": [],
            "timeline_difference_analysis_available": False,
            "notes_column": notes_column,
            "chunk_scores": [],
            "phase_call_chunk_scores": [],
            "included_chunk_count": 0,
            "excluded_chunk_count": 0,
            "thrown_out": False,
            "temporal_shift_seconds": 0.0,
            "sparkline_svg": "",
            "runs_completed": 0,
            "total_runs": 1,
        }
    baseline_for_analysis, collected_for_analysis, start_time_a, start_time_b = _prepare_analysis_inputs(
        baseline,
        collected,
        tod_align=tod_align,
    )

    sparkline_base_timestamp: Optional[datetime] = None
    compare_settle_minutes = settle_minutes
    manual_analysis_start: Optional[pd.Timestamp] = None
    manual_analysis_end: Optional[pd.Timestamp] = None
    if tod_align:
        manual_analysis_start, manual_analysis_end = _resolve_manual_analysis_window(
            collected_for_analysis,
            analysis_start_time=analysis_start_time,
            analysis_end_time=analysis_end_time,
        )
        if manual_analysis_start is not None or manual_analysis_end is not None:
            baseline_for_analysis = _trim_to_analysis_window(
                baseline_for_analysis,
                manual_analysis_start,
                manual_analysis_end,
            )
            collected_for_analysis = _trim_to_analysis_window(
                collected_for_analysis,
                manual_analysis_start,
                manual_analysis_end,
            )
            if manual_analysis_start is not None:
                start_time_a = manual_analysis_start.to_pydatetime()
                start_time_b = manual_analysis_start.to_pydatetime()
                sparkline_base_timestamp = manual_analysis_start.to_pydatetime()
            elif not baseline_for_analysis.empty:
                start_time_a = pd.to_datetime(
                    baseline_for_analysis[_get_timestamp_col(baseline_for_analysis)]
                ).min().to_pydatetime()
                start_time_b = start_time_a
                sparkline_base_timestamp = start_time_a
            compare_settle_minutes = 0.0
        else:
            sparkline_base_timestamp = start_time_a

    result = sr.compare_runs(
        events_a=baseline_for_analysis, events_b=collected_for_analysis,
        device_id=scenario_id,
        run_a_label=baseline_label, run_b_label=firmware_version,
        start_time_a=start_time_a,
        start_time_b=start_time_b,
        auto_align=not tod_align, settle_minutes=compare_settle_minutes,
        group_tolerance=group_tolerance,
        phase_call_threshold=phase_call_threshold,
    )

    plot_paths: list = []
    timeline_a = timeline_b = None
    if not baseline_for_analysis.empty and not collected_for_analysis.empty:
        try:
            # Suppress atspm's verbose stdout unless --verbose
            _devnull = open(os.devnull, "w") if not verbose else None
            _ctx = contextlib.redirect_stdout(_devnull) if _devnull else contextlib.nullcontext()
            with _ctx:
                timeline_a = sr.generate_timeline(baseline_for_analysis, device_id=scenario_id)
                timeline_b = sr.generate_timeline(collected_for_analysis, device_id=scenario_id)
            if _devnull:
                _devnull.close()
            remove_events = [
                "Ped Omit", "Phase Hold", "Phase Omit", "Phase Call",
            ]
            timeline_a = timeline_a[~timeline_a["EventClass"].isin(remove_events)]
            timeline_b = timeline_b[~timeline_b["EventClass"].isin(remove_events)]
        except Exception as e:
            if verbose:
                print(f"    Timeline generation failed for {scenario_id}: {e}", flush=True)

    if result.divergence_windows and timeline_a is not None and not timeline_a.empty and not timeline_b.empty:
        try:
            _devnull2 = open(os.devnull, "w") if not verbose else None
            _ctx2 = contextlib.redirect_stdout(_devnull2) if _devnull2 else contextlib.nullcontext()
            with _ctx2:
                time_offset_b = 0.0 if tod_align else sr.compute_timeline_offset(timeline_a, timeline_b)
                plot_paths = sr.create_multi_divergence_plots(
                    timeline_a=timeline_a, timeline_b=timeline_b,
                    comparison_result=result, output_dir=plots_dir_str,
                    label_a=baseline_label, label_b=firmware_version,
                    max_plots=max_plots, window_minutes=window_minutes,
                    time_offset_b=time_offset_b,
                    align_by_time_delta=not tod_align,
                )
            if _devnull2:
                _devnull2.close()
        except Exception as e:
            if verbose:
                print(f"    Chart generation failed for {scenario_id}: {e}", flush=True)
                import traceback as _tb
                _tb.print_exc()

    passed = (not result.thrown_out) and result.match_percentage >= 95.0
    phase_diffs: list = []
    operational_diffs: list = []
    timeline_difference_analysis_available = False
    if test_type == sr.TestType.SIMILARITY and timeline_a is not None and not timeline_a.empty and not timeline_b.empty:
        try:
            settle_td = pd.Timedelta(minutes=compare_settle_minutes)
            if tod_align:
                tl_a_settled = timeline_a.copy()
                tl_b_settled = timeline_b.copy()
            else:
                # The two timelines come from different absolute dates (original vs.
                # collected replay). We need to align them by relative time from
                # their respective starts before settle-trimming and overlap-clipping.
                start_a = timeline_a["StartTime"].min()
                start_b = timeline_b["StartTime"].min()

                tl_a_rel = timeline_a.copy()
                tl_b_rel = timeline_b.copy()

                # Convert to a common epoch (use start_a as the reference)
                tl_b_rel["StartTime"] = start_a + (tl_b_rel["StartTime"] - start_b)
                tl_b_rel["EndTime"] = start_a + (tl_b_rel["EndTime"] - start_b)

                tl_a_settled = tl_a_rel[tl_a_rel["StartTime"] >= start_a + settle_td].copy()
                tl_b_settled = tl_b_rel[tl_b_rel["StartTime"] >= start_a + settle_td].copy()

            overlap_end = min(tl_a_settled["EndTime"].max(), tl_b_settled["EndTime"].max())
            tl_a_settled = tl_a_settled[tl_a_settled["StartTime"] <= overlap_end]
            tl_b_settled = tl_b_settled[tl_b_settled["StartTime"] <= overlap_end]

            if verbose:
                signal_classes = {'Green', 'Yellow', 'Red', 'Overlap Green', 'Overlap Trail Green', 'Overlap Yellow', 'Overlap Red'}
                sig_a = tl_a_settled[tl_a_settled["EventClass"].isin(signal_classes)]
                sig_b = tl_b_settled[tl_b_settled["EventClass"].isin(signal_classes)]
                print(f"    [diag] timeline_a range: {timeline_a['StartTime'].min()} to {timeline_a['EndTime'].max()}", flush=True)
                print(f"    [diag] timeline_b range: {timeline_b['StartTime'].min()} to {timeline_b['EndTime'].max()}", flush=True)
                print(f"    [diag] after settle+overlap: tl_a={len(tl_a_settled)} ({len(sig_a)} signal), "
                      f"tl_b={len(tl_b_settled)} ({len(sig_b)} signal), overlap_end={overlap_end}", flush=True)

            phase_diffs = sr.generate_phase_difference_summary(tl_a_settled, tl_b_settled, tolerance_seconds=0.2)
            operational_diffs = sr.generate_operational_difference_summary(tl_a_settled, tl_b_settled, tolerance_seconds=0.2)
            timeline_difference_analysis_available = True
        except Exception as e:
            if verbose:
                print(f"    Phase breakdown failed for {scenario_id}: {e}", flush=True)
                import traceback as _tb
                _tb.print_exc()

    # Truncate the summary to show at most 10 divergences
    raw_summary = result.format_summary()
    summary_lines = raw_summary.split("\n")
    # Find where divergence list starts (indented lines after "Divergences: N")
    truncated_lines = []
    div_count = 0
    max_div_shown = 5
    for line in summary_lines:
        if line.startswith("  ") and div_count >= max_div_shown:
            continue  # skip excess divergence lines
        truncated_lines.append(line)
        if line.startswith("  "):
            div_count += 1
            if div_count == max_div_shown and len(result.divergence_windows) > max_div_shown:
                truncated_lines.append(f"  ... and {len(result.divergence_windows) - max_div_shown} more divergences")

    return {
        "scenario_id": scenario_id,
        "test_type_str": test_type_str,
        "passed": passed,
        "match_percentage": result.match_percentage,
        "num_divergences": len(result.divergence_windows),
        "summary": "\n".join(truncated_lines),
        "plot_paths": plot_paths,
        "phase_diffs": phase_diffs,
        "operational_diffs": operational_diffs,
        "timeline_difference_analysis_available": timeline_difference_analysis_available,
        "notes_column": notes_column,
        "chunk_scores": [
            {"center_seconds": c.center_seconds,
             "match_percentage": c.match_percentage,
             "window_seconds": c.window_seconds}
            for c in result.chunk_scores
        ],
        "phase_call_chunk_scores": [
            {"center_seconds": c.center_seconds,
             "window_seconds": c.window_seconds,
             "similarity_percentage": c.similarity_percentage,
             "has_activity": c.has_activity,
             "excluded_from_match": c.excluded_from_match}
            for c in result.phase_call_chunk_scores
        ],
        "included_chunk_count": result.included_chunk_count,
        "excluded_chunk_count": result.excluded_chunk_count,
        "thrown_out": result.thrown_out,
        "temporal_shift_seconds": result.temporal_shift_seconds,
        "runs_completed": 1,
        "total_runs": 1,
        "sparkline_svg": sr.render_sparkline_svg(
            result.chunk_scores,
            pass_threshold=95.0,
            base_timestamp=sparkline_base_timestamp,
            phase_call_chunk_scores=result.phase_call_chunk_scores,
            phase_call_threshold=phase_call_threshold,
        ) if result.chunk_scores else "",
    }


def _export_device_csvs(
    suite: sr.FirmwareTestSuite,
    collected_db_path: Path,
    baseline_sources: Dict[str, Path],
    group_tolerance: float = 0.0,
) -> Path:
    """Export a combined CSV per device with baseline + collected events.

    Each CSV lives in results/<fw_version>/device_events/<scenario_id>.csv
    and contains a ``DeviceId`` column (``baseline`` vs ``new``)
    to distinguish the two runs. Baseline timestamps are shifted onto the
    new run's absolute timeline using the same temporal offset logic used
    by the comparison / Gantt chart alignment.
    """
    out_dir = Path(suite.output_dir) / suite.firmware_version / "device_events"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not collected_db_path.exists():
        log(f"Collected DuckDB not found for device CSV export: {collected_db_path}")
        return out_dir

    exported = 0
    for scenario in suite.scenarios:
        baseline_source = baseline_sources.get(scenario.scenario_id)
        if baseline_source is None:
            log(
                f"WARNING: Missing baseline log for {scenario.scenario_id}; "
                "skipping device CSV export for this scenario."
            )
            continue

        baseline = sr.load_events(str(baseline_source))
        collected = _load_collected_events_from_duckdb(collected_db_path, scenario.scenario_id)

        if baseline.empty:
            log(
                f"WARNING: Baseline log for {scenario.scenario_id} has no rows; "
                "skipping device CSV export for this scenario."
            )
            continue
        if collected.empty:
            log(
                f"WARNING: No collected events found in DuckDB for {scenario.scenario_id}; "
                "skipping device CSV export for this scenario."
            )
            continue

        original_ts_col = _get_timestamp_col(baseline)
        collected_ts_col = _get_timestamp_col(collected)
        baseline[original_ts_col] = pd.to_datetime(baseline[original_ts_col])
        collected[collected_ts_col] = pd.to_datetime(collected[collected_ts_col])

        original_shift = _compute_export_shift(
            baseline,
            collected,
            original_ts_col=original_ts_col,
            collected_ts_col=collected_ts_col,
            tod_align=scenario.tod_align,
            group_tolerance=group_tolerance,
        )
        if original_shift != pd.Timedelta(0):
            baseline[original_ts_col] = baseline[original_ts_col] + original_shift
        if scenario.tod_align:
            vlog(
                f"  {scenario.scenario_id}: TOD export date shift "
                f"{original_shift.total_seconds():+.2f}s"
            )
        elif original_shift != pd.Timedelta(0):
            vlog(
                f"  {scenario.scenario_id}: shifted original timestamps by "
                f"{original_shift.total_seconds():+.2f}s"
            )

        baseline = _normalize_device_csv_events(baseline)
        collected = _normalize_device_csv_events(collected)

        if baseline.empty or collected.empty:
            missing_side = "baseline" if baseline.empty else "collected"
            log(
                f"WARNING: Normalized device CSV export for {scenario.scenario_id} is missing "
                f"{missing_side} rows; skipping export."
            )
            continue

        baseline["DeviceId"] = "baseline"
        collected["DeviceId"] = "new"

        combined = pd.concat([baseline, collected], ignore_index=True)
        combined = combined[["DeviceId", "timestamp", "EventId", "Parameter"]]
        combined = combined.sort_values(["timestamp", "EventId", "Parameter", "DeviceId"]).reset_index(drop=True)
        csv_path = out_dir / f"{scenario.scenario_id}.csv"
        combined.to_csv(csv_path, index=False)
        exported += 1
        vlog(f"  {scenario.scenario_id} -> {csv_path.name} ({len(combined)} rows)")

    log(f"Exported {exported} device CSV(s) to {out_dir}")
    return out_dir


def run_analysis(
    suite: sr.FirmwareTestSuite,
    settings: dict,
    firmware_dir: Path,
) -> List[sr.ScenarioResult]:
    """Run comparisons with multiprocessing. Returns list of ScenarioResult."""
    firmware_version = suite.firmware_version
    comp = settings.get("comparison", {})
    settle_minutes = comp.get("settle_minutes", 10.0)
    group_tolerance = comp.get("group_tolerance", 0.0)
    max_plots = comp.get("max_divergence_plots", 3)
    window_minutes = comp.get("divergence_window_minutes", 5.0)
    phase_call_threshold = comp.get("phase_call_similarity_threshold", comp.get("detector_similarity_threshold", 90.0))
    max_workers = settings.get("analysis_workers", 4)

    plots_dir = Path(suite.output_dir) / firmware_version / "divergence_plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    collected_db_path = _shared_collected_db_path(suite)

    baseline_logs_dir, baseline_label = resolve_baseline_logs_dir(firmware_dir, settings)
    if baseline_logs_dir is not None:
        log(f"Baseline logs: {baseline_logs_dir}")
    else:
        log(f"Baseline logs folder not found for {settings['baseline_version']}; using source logs/ as fallback.")

    if collected_db_path.exists():
        log(f"Reading collected output directly from DuckDB: {collected_db_path}")
    else:
        log(f"Collected DuckDB not found: {collected_db_path}")

    baseline_sources: Dict[str, Path] = {}
    for scenario in suite.scenarios:
        baseline_path, _ = resolve_baseline_log(scenario.scenario_id, firmware_dir, settings)
        if baseline_path is None:
            log(
                f"WARNING: Missing baseline log for {scenario.scenario_id}; "
                "this scenario will be skipped during analysis."
            )
            continue
        baseline_sources[scenario.scenario_id] = baseline_path

    # Export per-device CSVs (baseline + collected combined)
    log("Exporting per-device CSV files...")
    _export_device_csvs(suite, collected_db_path, baseline_sources, group_tolerance=group_tolerance)

    analysis_start_time = comp.get("analysis_start_time")
    analysis_end_time = comp.get("analysis_end_time")
    if analysis_start_time:
        log(f"TOD manual analysis start time: {analysis_start_time}")
    if analysis_end_time:
        log(f"TOD manual analysis end time: {analysis_end_time}")

    # Build job list — all plain types for pickling (no DuckDB paths)
    jobs: list = []
    conflict_jobs: List[Tuple[sr.TestScenario, Path, Path]] = []
    for scenario in suite.scenarios:
        baseline_source = baseline_sources.get(scenario.scenario_id)
        if not collected_db_path.exists():
            log(f"  No output for {scenario.scenario_id}, skipping")
            continue
        if baseline_source is None:
            continue
        if scenario.test_type == sr.TestType.CONFLICT:
            conflict_jobs.append((scenario, baseline_source, collected_db_path))
            continue
        test_type_str = "CONFLICT" if scenario.test_type == sr.TestType.CONFLICT else "SIMILARITY"
        jobs.append((
            scenario.scenario_id,
            str(baseline_source),
            baseline_label,
            test_type_str,
            str(collected_db_path),
            firmware_version,
            str(plots_dir),
            settle_minutes,
            group_tolerance,
            max_plots,
            window_minutes,
            _VERBOSE,
            scenario.notes_column,
            scenario.tod_align,
            analysis_start_time,
            analysis_end_time,
            phase_call_threshold,
        ))

    if not jobs and not conflict_jobs:
        log("No scenarios to analyze.")
        return []

    results: List[sr.ScenarioResult] = []
    done = 0
    total_jobs = len(jobs) + len(conflict_jobs)

    if jobs:
        n_workers = min(max_workers, len(jobs))
        log(f"\nAnalyzing {len(jobs)} similarity scenario(s) with {n_workers} workers...")
        sys.stdout.flush()

        with ProcessPoolExecutor(max_workers=n_workers) as executor:
            futures = {
                executor.submit(_compare_one_scenario, job): job[0]
                for job in jobs
            }
            for future in as_completed(futures):
                sid = futures[future]
                done += 1
                try:
                    out = future.result()
                    results.append(sr.ScenarioResult(
                        scenario_id=out["scenario_id"],
                        test_type=sr.TestType.SIMILARITY,
                        firmware_version=firmware_version,
                        passed=out["passed"],
                        match_percentage=out["match_percentage"],
                        num_divergences=out["num_divergences"],
                        error=out.get("error"),
                        notes=out["summary"],
                        notes_column=out.get("notes_column", ""),
                        plot_paths=out["plot_paths"],
                        phase_differences=out["phase_diffs"],
                        operational_differences=out.get("operational_diffs", []),
                        runs_completed=out.get("runs_completed", 1),
                        total_runs=out.get("total_runs", 1),
                        chunk_scores=out.get("chunk_scores", []),
                        phase_call_chunk_scores=out.get("phase_call_chunk_scores", out.get("detector_chunk_scores", [])),
                        included_chunk_count=out.get("included_chunk_count", 0),
                        excluded_chunk_count=out.get("excluded_chunk_count", 0),
                        thrown_out=out.get("thrown_out", False),
                        timeline_difference_analysis_available=out.get("timeline_difference_analysis_available", False),
                        sparkline_svg=out.get("sparkline_svg", ""),
                        temporal_shift_seconds=out.get("temporal_shift_seconds", 0.0),
                    ))
                    status = "ERROR" if out.get("error") else (
                        "THROWN OUT" if out.get("thrown_out") else ("PASS" if out["passed"] else "FAIL")
                    )
                    match_text = "n/a" if out.get("match_percentage") is None else f"{out['match_percentage']:.1f}%"
                    plots_msg = f", {len(out['plot_paths'])} charts" if out["plot_paths"] else ""
                    diffs_msg = ""
                    if out["phase_diffs"]:
                        diffs_msg = f"\n    Phase/overlap differences ({len(out['phase_diffs'])} phases):\n"
                        diffs_msg += sr.format_phase_differences(
                            out["phase_diffs"], label_a=baseline_label, label_b=firmware_version
                        )
                    if out.get("operational_diffs"):
                        diffs_msg += f"\n    Transition/preempt/ped service differences ({len(out['operational_diffs'])} rows):\n"
                        diffs_msg += sr.format_phase_differences(
                            out["operational_diffs"], label_a=baseline_label, label_b=firmware_version
                        )
                    error_msg = f"\n    {out['error']}" if out.get("error") else ""
                    log(f"  [{done}/{total_jobs}] {out['scenario_id']}: {match_text}  {status}  ({out['num_divergences']} divergences{plots_msg}){diffs_msg}{error_msg}")
                except Exception as e:
                    log(f"  [{done}/{total_jobs}] {sid}: ERROR — {e}")
                    if _VERBOSE:
                        traceback.print_exc()

    if conflict_jobs:
        log(f"\nAnalyzing {len(conflict_jobs)} conflict scenario(s) from saved output logs...")
        for scenario, baseline_source, collected_db_path in conflict_jobs:
            done += 1
            try:
                result = _analyze_conflict_scenario(
                    scenario,
                    baseline_source,
                    baseline_label,
                    collected_db_path,
                    firmware_version,
                )
                results.append(result)
                status = "ERROR" if result.error else ("PASS" if result.passed else "FAIL")
                log(
                    f"  [{done}/{total_jobs}] {result.scenario_id}: {status}  "
                    f"({len(result.conflicts_found)} conflict signature(s), "
                    f"{result.runs_completed}/{result.total_runs} runs completed)"
                )
            except Exception as e:
                log(f"  [{done}/{total_jobs}] {scenario.scenario_id}: ERROR — {e}")
                if _VERBOSE:
                    traceback.print_exc()

    results.sort(key=lambda r: (0 if r.test_type == sr.TestType.SIMILARITY else 1, r.scenario_id))
    return results


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
def build_report(
    results: List[sr.ScenarioResult],
    suite: sr.FirmwareTestSuite,
) -> Path:
    report_dir = Path(suite.output_dir) / suite.firmware_version
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "report.html"
    generate_report(results, suite, str(report_path))
    return report_path


# ---------------------------------------------------------------------------
# Refresh versioned collected logs
# ---------------------------------------------------------------------------
def archive_and_extract(suite: sr.FirmwareTestSuite, firmware_dir: Path, settings: dict) -> None:
    fw_ver = suite.firmware_version
    collected_db_path = _shared_collected_db_path(suite)
    if not collected_db_path.exists():
        log(f"No collected DuckDB found to export: {collected_db_path}")
        return
    output_dir = get_version_logs_dir(firmware_dir, settings, fw_ver)
    extracted_map = _extract_collected_events(suite, collected_db_path, output_dir)
    log(f"Collected logs refreshed in {output_dir} ({len(extracted_map)} scenario(s)).")


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------
def main() -> None:
    global _VERBOSE

    parser = argparse.ArgumentParser(description="Firmware validation: replay, compare, report.")
    parser.add_argument("--settings", default="settings.json", help="Path to settings JSON file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    parser.add_argument("--report-only", action="store_true", help="Skip replay, just run analysis + report")
    parser.add_argument("--archive", action="store_true", help="Refresh the versioned collected logs after report generation")
    parser.add_argument(
        "--settle-minutes",
        type=float,
        default=None,
        help="Override the initial minutes excluded from similarity analysis/reporting",
    )
    args = parser.parse_args()

    _VERBOSE = args.verbose

    # Preserve the invoked workspace path on Windows rather than resolving a
    # mapped drive into its UNC share, which can break temp parquet access.
    firmware_dir = Path(__file__).parent
    settings_path = firmware_dir / args.settings
    if not settings_path.exists():
        print(f"ERROR: Settings file not found: {settings_path}", file=sys.stderr)
        sys.exit(1)

    settings = load_settings(settings_path)
    if args.settle_minutes is not None:
        settings.setdefault("comparison", {})["settle_minutes"] = args.settle_minutes
    log(f"signal_replay version: {sr.__version__}")
    log(f"Firmware dir:  {firmware_dir}")
    log(f"Settings:      {settings_path}")

    # --- Read catalog ---
    catalog_path = firmware_dir / settings["catalog_file"]
    catalog = read_catalog(catalog_path)
    log(f"Catalog:       {len(catalog)} rows from {catalog_path.name}")

    # --- Load conflict pairs ---
    conflict_pairs_path = firmware_dir / settings["conflict_pairs_file"]
    conflict_pairs: dict = {}
    if conflict_pairs_path.exists():
        with open(conflict_pairs_path, "r") as f:
            conflict_pairs = json.load(f)
        conflict_pairs = {k: [tuple(pair) for pair in v] for k, v in conflict_pairs.items()}
        vlog(f"Loaded conflict pairs for {len(conflict_pairs)} devices")

    # --- Build suite ---
    suite, file_map = build_suite(settings, firmware_dir, catalog, conflict_pairs)
    scenario_lookup = {scenario.scenario_id: scenario for scenario in suite.scenarios}

    if suite.analysis_start_time:
        if suite.scenarios and all(s.tod_align for s in suite.scenarios):
            log(
                f"TOD manual analysis start time: {suite.analysis_start_time} "
                "(replaces settle window for all scenarios)"
            )
        elif any(s.tod_align for s in suite.scenarios):
            log(f"Analysis settle window: {suite.analysis_settle_minutes} minutes")
            log(
                f"TOD manual analysis start time: {suite.analysis_start_time} "
                "(replaces settle window only for TOD-aligned scenarios)"
            )
        else:
            log(f"Analysis settle window: {suite.analysis_settle_minutes} minutes")
            log(
                f"TOD manual analysis start time: {suite.analysis_start_time} "
                "(no TOD-aligned scenarios currently use it)"
            )
    else:
        log(f"Analysis settle window: {suite.analysis_settle_minutes} minutes")
    if suite.analysis_end_time:
        log(f"TOD manual analysis end time: {suite.analysis_end_time}")

    log(f"Scenarios: {len(suite.scenarios)}  |  Controllers: {len(settings['controller_targets'])}")

    # --- File readiness ---
    ready = sum(1 for r in catalog if file_map[r["TSSU"]]["log"])
    log(f"Logs ready: {ready}/{len(catalog)}")

    if _VERBOSE:
        for r in catalog:
            tssu = r["TSSU"]
            lf = file_map[tssu]["log"]
            df = file_map[tssu]["db"]
            status = "OK" if lf else "--"
            log(f"  {status:>2s}  {tssu:>8s}  log={lf.name if lf else 'MISSING':<30s}  db={df.name if df else '--'}")

    try:
        # ======================================================================
        # REPLAY PHASE
        # ======================================================================
        if not args.report_only:
            existing_ids = _get_collected_device_ids(_shared_collected_db_path(suite))
            current_batch, remaining_pending, replace_selected, skipped_existing = _select_pending_replay_batch(
                suite,
                settings,
                catalog,
                existing_ids,
            )

            if skipped_existing:
                vlog(
                    "Already collected; skipping unless Replace on Rerun = yes: "
                    + ", ".join(skipped_existing)
                )

            if current_batch is None:
                log("\nNo pending replay devices in the current catalog. Proceeding to analysis.")
            else:
                shared_db_path = _shared_collected_db_path(suite)
                if replace_selected:
                    log(
                        "Replace on Rerun = yes; clearing existing data for: "
                        + ", ".join(replace_selected)
                    )
                    _clear_selected_devices_for_rerun(shared_db_path, replace_selected)

                log(f"\n{'='*70}")
                log(f"REPLAY: Next Pending Batch  ({len(current_batch.assignments)} scenarios)")
                log(f"{'='*70}")
                log("\nLoad these databases onto the controllers:")
                for sid, tgt in current_batch.assignments.items():
                    s = scenario_lookup[sid]
                    db_name = Path(s.database_name).name
                    extras = []
                    if s.test_type == sr.TestType.CONFLICT:
                        extras.append("CONFLICT" + (" pairs=configured" if s.incompatible_pairs else " (no pairs)"))
                    if s.cycle_length:
                        extras.append(f"CL={s.cycle_length}")
                    if s.cycle_offset:
                        extras.append(f"Off={s.cycle_offset}")
                    if not s.tod_align:
                        extras.append("tod_align=OFF")
                    extra_str = "  " + ", ".join(extras) if extras else ""
                    log(f"  {tgt:<22s}  <-  {db_name:<20s}{extra_str}")

                log("")
                input("Press ENTER when databases are loaded and controllers are ready...")

                log("\nChecking controllers...")
                db_lookup = {sid: Path(s.database_name).name for sid, s in scenario_lookup.items()}
                b_targets = list(current_batch.assignments.values())
                b_labels = [f"{tgt} / {db_lookup[sid]}" for sid, tgt in current_batch.assignments.items()]

                try:
                    wait_for_controllers(b_targets, b_labels)
                except KeyboardInterrupt:
                    log("\nController check interrupted. Continuing anyway...")

                log("\nRunning replay for the next pending devices...")
                try:
                    run_batch(suite, current_batch)
                except KeyboardInterrupt:
                    log("Replay interrupted by user. Exiting.")
                    sys.exit(130)

                if remaining_pending:
                    log(
                        "\nReplay batch complete. Remaining pending devices from the current catalog: "
                        + ", ".join(s.scenario_id for s in remaining_pending)
                    )
                    log("Run this script again to continue with the next pending devices.")
                    sys.exit(0)

                log("\nAll pending devices from the current catalog have been collected. Proceeding to analysis.")

        # ======================================================================
        # ANALYSIS PHASE
        # ======================================================================
        log(f"\n{'='*70}")
        log(f"ANALYSIS: Comparing {suite.firmware_version} output to {suite.baseline_version}")
        log(f"{'='*70}")

        results = run_analysis(suite, settings, firmware_dir)
        passed = sum(1 for r in results if r.passed)
        log(f"\nResults: {passed}/{len(results)} passed")

        # ======================================================================
        # REPORT
        # ======================================================================
        log(f"\n{'='*70}")
        log("REPORT: Generating HTML report")
        log(f"{'='*70}")

        report_path = build_report(results, suite)
        log(f"Report saved to: {report_path}")
        log(f"Total images embedded: {sum(len(r.plot_paths) for r in results)}")

        # ======================================================================
        # VERSIONED LOG EXPORT (optional refresh)
        # ======================================================================
        if args.archive:
            log(f"\n{'='*70}")
            log("EXPORT: Refreshing versioned collected logs")
            log(f"{'='*70}")
            archive_and_extract(suite, firmware_dir, settings)

        log("\nDone.")
    finally:
        pass


if __name__ == "__main__":
    main()
