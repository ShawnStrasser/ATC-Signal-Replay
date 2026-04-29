#!/usr/bin/env python
"""
firmware_validate.py — Standalone firmware validation script.

Replays source logs to controllers with new firmware, compares collected output
to the configured baseline, and generates an HTML report with divergence charts.

Usage:
    python firmware_validate.py                  # interactive, uses settings.json
    python firmware_validate.py --verbose        # extra debug output
    python firmware_validate.py --report-only    # skip replay, just run analysis + report
    python firmware_validate.py --report-only-fast
    python firmware_validate.py --settings custom_settings.json

Settings are loaded from settings.json (editable JSON file in the same folder).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
import traceback
from datetime import datetime, time as dt_time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
import requests
from openpyxl import load_workbook

import signal_replay as sr
from signal_replay.ntcip import send_ntcip
from signal_replay.report import generate_report

COLLECTED_DB_FILENAME = "collected.db"
BaselineSource = Tuple[str, str]
COORD_PATTERNS_DIR = Path(__file__).resolve().parent / "coord_patterns"

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


def _extract_coord_split_device_id(path: Path) -> Optional[str]:
    stem = path.stem
    suffix = "_coord_splits"
    if not stem.endswith(suffix):
        return None
    device_id = stem[:-len(suffix)].strip()
    return device_id or None


def _parse_coord_split_clock_time(value: object) -> dt_time:
    text = str(value).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    raise ValueError(f"invalid coord split time: {value!r}")


def _parse_coord_split_phase(value: object) -> Optional[int]:
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    return int(digits) if digits else None


def _refresh_coord_split_csvs(coord_dir: Path) -> None:
    module_path = coord_dir.parent / "coord_split_schedule.py"
    if not module_path.exists():
        log(f"WARNING: coord split generator not found: {module_path}")
        return

    try:
        spec = importlib.util.spec_from_file_location("coord_split_schedule_runtime", module_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Unable to load module spec for {module_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.convert_all_pattern_files(coord_dir)
    except Exception as exc:
        log(f"WARNING: Failed to refresh coord split CSVs from JSON files: {exc}")


def _load_coord_split_schedules(coord_dir: str) -> Dict[str, List[Dict[str, object]]]:
    schedules: Dict[str, List[Dict[str, object]]] = {}
    path = Path(coord_dir)
    if not path.exists():
        return schedules

    _refresh_coord_split_csvs(path)

    for csv_path in sorted(path.glob("*.csv")):
        device_id = _extract_coord_split_device_id(csv_path)
        if device_id is None:
            continue
        try:
            df = pd.read_csv(csv_path)
        except Exception as exc:
            log(f"WARNING: Failed to read coord split CSV {csv_path.name}: {exc}")
            continue

        rows: List[Dict[str, object]] = []
        for row in df.to_dict("records"):
            try:
                phase = _parse_coord_split_phase(row.get("Phase"))
                start_time = _parse_coord_split_clock_time(row.get("start_time"))
                end_time = _parse_coord_split_clock_time(row.get("end_time"))
            except Exception as exc:
                log(f"WARNING: Skipping malformed coord split row in {csv_path.name}: {exc}")
                continue
            if phase is None:
                log(f"WARNING: Skipping coord split row with missing phase in {csv_path.name}")
                continue
            rows.append(
                {
                    "phase": phase,
                    "start_time": start_time,
                    "end_time": end_time,
                }
            )

        if rows:
            rows.sort(key=lambda row: (row["start_time"], row["phase"]))
            schedules[device_id] = rows

    return schedules


def _resolve_coord_split_device_id(
    scenario_id: str,
    schedules: Dict[str, List[Dict[str, object]]],
) -> Optional[str]:
    if scenario_id in schedules:
        return scenario_id
    base_device_id = scenario_id.split("_", 1)[0]
    if base_device_id in schedules:
        return base_device_id
    return None


def _build_programmed_split_timeline(
    schedule_rows: List[Dict[str, object]],
    anchor_time: pd.Timestamp,
) -> pd.DataFrame:
    if not schedule_rows:
        return pd.DataFrame(columns=["Phase", "StartTime", "EndTime"])

    anchor_ts = pd.Timestamp(anchor_time)
    base_date = anchor_ts.normalize().to_pydatetime().date()
    rows: List[Dict[str, object]] = []
    for schedule_row in schedule_rows:
        start_ts = pd.Timestamp(datetime.combine(base_date, schedule_row["start_time"]))
        end_ts = pd.Timestamp(datetime.combine(base_date, schedule_row["end_time"]))
        if end_ts <= start_ts:
            end_ts += pd.Timedelta(days=1)
        rows.append(
            {
                "Phase": int(schedule_row["phase"]),
                "StartTime": start_ts,
                "EndTime": end_ts,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
def load_settings(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_results_dir(firmware_dir: Path, settings: dict) -> Path:
    return firmware_dir / settings["results_dir"]


def get_version_collected_db_path(firmware_dir: Path, settings: dict, version: str) -> Path:
    return get_results_dir(firmware_dir, settings) / version / COLLECTED_DB_FILENAME


def get_version_logs_dir(firmware_dir: Path, settings: dict, version: str) -> Path:
    return get_results_dir(firmware_dir, settings) / version / "logs"


def resolve_baseline_db_path(firmware_dir: Path, settings: dict) -> Tuple[Optional[Path], str]:
    baseline_db_path = get_version_collected_db_path(
        firmware_dir,
        settings,
        settings["baseline_version"],
    )
    if baseline_db_path.exists():
        return baseline_db_path, settings["baseline_version"]
    return None, f"{settings['baseline_version']} (source logs)"


def resolve_baseline_source(
    tssu: str,
    firmware_dir: Path,
    settings: dict,
) -> Tuple[Optional[BaselineSource], str]:
    baseline_db_path, baseline_label = resolve_baseline_db_path(firmware_dir, settings)
    if baseline_db_path is not None:
        return ("db", str(baseline_db_path)), baseline_label

    baseline_log = find_log(tssu, firmware_dir / settings["logs_dir"])
    if baseline_log is None:
        return None, baseline_label
    return ("file", str(baseline_log)), baseline_label


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
    _baseline_db_path, baseline_version = resolve_baseline_db_path(firmware_dir, settings)

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
    return Path(suite.output_dir) / suite.firmware_version / COLLECTED_DB_FILENAME


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


def _load_baseline_events(baseline_source: BaselineSource, scenario_id: str) -> pd.DataFrame:
    """Load baseline events from either a collected DuckDB or a source log file."""
    source_kind, source_path = baseline_source
    if source_kind == "db":
        return _load_collected_events_from_duckdb(source_path, scenario_id)
    if source_kind == "file":
        return sr.load_events(source_path)
    raise ValueError(f"Unsupported baseline source kind: {source_kind}")


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
    baseline_source: BaselineSource,
    baseline_label: str,
    collected_db_path: Path,
    firmware_version: str,
) -> sr.ScenarioResult:
    """Build a conflict result from persisted baseline/new logs."""
    baseline_events = _load_baseline_events(baseline_source, scenario.scenario_id)
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


def _normalize_issue_timeline_rows(
    timeline: pd.DataFrame,
    event_class: str,
    event_value: int,
) -> pd.DataFrame:
    """Return timeline rows for a single event class/value with numeric durations."""
    if timeline.empty:
        return pd.DataFrame(columns=["StartTime", "EndTime", "Duration"])

    filtered = timeline[timeline["EventClass"] == event_class].copy()
    if filtered.empty:
        return filtered

    filtered["EventValue"] = pd.to_numeric(filtered["EventValue"], errors="coerce")
    if event_value == 0:
        value_mask = filtered["EventValue"].fillna(0) == 0
    else:
        value_mask = filtered["EventValue"] == event_value
    filtered = filtered[value_mask].copy()
    if filtered.empty:
        return filtered

    if "Duration" not in filtered.columns or filtered["Duration"].isna().all():
        filtered["Duration"] = (filtered["EndTime"] - filtered["StartTime"]).dt.total_seconds()
    filtered["Duration"] = pd.to_numeric(filtered["Duration"], errors="coerce")
    filtered = filtered.dropna(subset=["Duration"])
    return filtered.sort_values(["StartTime", "EndTime"]).reset_index(drop=True)


def _timeline_valid_mask(timeline: pd.DataFrame) -> pd.Series:
    if timeline.empty:
        return pd.Series(dtype=bool, index=timeline.index)

    for column in ("IsValid", "is_valid"):
        if column in timeline.columns:
            return timeline[column].fillna(False).astype(bool)

    return pd.Series(True, index=timeline.index)


def _split_timeline_by_validity(
    timeline: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if timeline.empty:
        return timeline.copy(), timeline.copy()

    valid_mask = _timeline_valid_mask(timeline)
    return timeline[valid_mask].copy(), timeline[~valid_mask].copy()


def _remove_ignored_timeline_events(timeline: pd.DataFrame) -> pd.DataFrame:
    if timeline.empty:
        return timeline.copy()

    remove_events = [
        "Ped Omit",
        "Phase Hold",
        "Phase Omit",
        "Phase Call",
    ]
    return timeline[~timeline["EventClass"].isin(remove_events)].copy()


def _prepare_settled_overlap_timelines(
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    *,
    settle_minutes: float,
    tod_align: bool,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    left = timeline_a.copy()
    right = timeline_b.copy()

    if left.empty and right.empty:
        return left, right

    settle_td = pd.Timedelta(minutes=settle_minutes)

    if tod_align:
        if settle_minutes > 0:
            if not left.empty:
                left = left[left["StartTime"] >= left["StartTime"].min() + settle_td].copy()
            if not right.empty:
                right = right[right["StartTime"] >= right["StartTime"].min() + settle_td].copy()
    else:
        start_a = None
        if not left.empty:
            start_a = left["StartTime"].min()
            left = left[left["StartTime"] >= start_a + settle_td].copy()
        if not right.empty:
            start_b = right["StartTime"].min()
            if start_a is not None:
                right["StartTime"] = start_a + (right["StartTime"] - start_b)
                right["EndTime"] = start_a + (right["EndTime"] - start_b)
                right = right[right["StartTime"] >= start_a + settle_td].copy()
            else:
                right = right[right["StartTime"] >= start_b + settle_td].copy()

    if not left.empty and not right.empty:
        overlap_end = min(left["EndTime"].max(), right["EndTime"].max())
        left = left[left["StartTime"] <= overlap_end].copy()
        right = right[right["StartTime"] <= overlap_end].copy()

    return left, right


def _is_significant_operational_issue(diff: Dict[str, object]) -> bool:
    """Mirror report thresholds for non-clearance issue plots, including green rows."""
    event_class = str(diff.get("event_class", "")).strip()
    avg_delta = float(diff.get("duration_delta", 0.0) or 0.0)
    total_delta = float(diff.get("total_duration_delta", 0.0) or 0.0)
    count_delta = abs(int(diff.get("count_delta", 0) or 0))

    threshold = None
    total_threshold = None
    if event_class == "Preempt":
        threshold = 3.0
        total_threshold = 60.0
    elif event_class == "Ped Service":
        threshold = 2.0
        total_threshold = 15.0
    elif event_class in {"Transition Longway", "Transition Shortway"}:
        threshold = 1.0
        total_threshold = 10.0
    elif event_class in {"Green", "Overlap Green", "Overlap Yellow", "Overlap Red"}:
        threshold = 2.0
        total_threshold = 15.0

    if threshold is None:
        return False
    return abs(avg_delta) >= threshold or abs(total_delta) >= total_threshold or count_delta >= 1


def _merge_issue_spans(
    spans: List[Dict[str, object]],
    *,
    gap_seconds: float,
) -> List[Dict[str, object]]:
    if not spans:
        return []

    ordered = sorted(spans, key=lambda span: pd.Timestamp(span["start"]))
    merged: List[Dict[str, object]] = [dict(ordered[0])]
    for span in ordered[1:]:
        gap = (
            pd.Timestamp(span["start"]) - pd.Timestamp(merged[-1]["end"])
        ).total_seconds()
        if gap <= gap_seconds:
            merged[-1]["end"] = pd.Timestamp(span["end"])
            merged[-1]["mismatch_seconds"] = float(merged[-1]["mismatch_seconds"]) + float(span["mismatch_seconds"])
            continue
        merged.append(dict(span))
    return merged


def _window_activity_stats(
    rows: pd.DataFrame,
    *,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> Tuple[int, float, float]:
    if rows.empty or window_end <= window_start:
        return 0, 0.0, 0.0

    count = 0
    active_seconds = 0.0
    for row in rows.itertuples(index=False):
        row_start = pd.Timestamp(row.StartTime)
        row_end = pd.Timestamp(row.EndTime)
        overlap_start = max(row_start, window_start)
        overlap_end = min(row_end, window_end)
        if overlap_end <= overlap_start:
            continue
        count += 1
        active_seconds += (overlap_end - overlap_start).total_seconds()

    avg_duration = active_seconds / count if count else 0.0
    return count, active_seconds, avg_duration


def _rank_operational_issue_windows(
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    diff: Dict[str, object],
) -> List[Dict[str, object]]:
    """Return ranked local A-vs-B mismatch windows for a non-clearance row."""
    rows_a = _normalize_issue_timeline_rows(
        timeline_a,
        str(diff.get("event_class", "")),
        int(diff.get("event_value", 0) or 0),
    )
    rows_b = _normalize_issue_timeline_rows(
        timeline_b,
        str(diff.get("event_class", "")),
        int(diff.get("event_value", 0) or 0),
    )

    if rows_a.empty and rows_b.empty:
        return []

    boundary_deltas: Dict[pd.Timestamp, List[int]] = {}

    def _record_boundaries(rows: pd.DataFrame, run_index: int) -> None:
        for row in rows.itertuples(index=False):
            start = pd.Timestamp(row.StartTime)
            end = pd.Timestamp(row.EndTime)
            if end <= start:
                continue
            boundary_deltas.setdefault(start, [0, 0])[run_index] += 1
            boundary_deltas.setdefault(end, [0, 0])[run_index] -= 1

    _record_boundaries(rows_a, 0)
    _record_boundaries(rows_b, 1)

    boundaries = sorted(boundary_deltas)
    if len(boundaries) < 2:
        return []

    mismatch_spans: List[Dict[str, object]] = []
    active_a = 0
    active_b = 0
    for current, nxt in zip(boundaries, boundaries[1:]):
        delta_a, delta_b = boundary_deltas[current]
        active_a += delta_a
        active_b += delta_b
        if nxt <= current or bool(active_a) == bool(active_b):
            continue
        mismatch_spans.append(
            {
                "start": current,
                "end": nxt,
                "mismatch_seconds": (pd.Timestamp(nxt) - pd.Timestamp(current)).total_seconds(),
            }
        )

    if not mismatch_spans:
        return []

    durations: List[float] = []
    for rows in (rows_a, rows_b):
        if rows.empty:
            continue
        durations.extend(float(value) for value in rows["Duration"].tolist())

    typical_duration = float(pd.Series(durations).median()) if durations else 0.0
    merge_gap_seconds = min(60.0, max(10.0, typical_duration))
    candidate_windows = _merge_issue_spans(mismatch_spans, gap_seconds=merge_gap_seconds)

    ranked_windows: List[Dict[str, object]] = []
    for window in candidate_windows:
        window_start = pd.Timestamp(window["start"])
        window_end = pd.Timestamp(window["end"])
        count_a, active_seconds_a, avg_duration_a = _window_activity_stats(
            rows_a,
            window_start=window_start,
            window_end=window_end,
        )
        count_b, active_seconds_b, avg_duration_b = _window_activity_stats(
            rows_b,
            window_start=window_start,
            window_end=window_end,
        )
        ranked_windows.append(
            {
                "start": window_start,
                "end": window_end,
                "score": float(window["mismatch_seconds"]),
                "mismatch_seconds": float(window["mismatch_seconds"]),
                "count_a": count_a,
                "count_b": count_b,
                "count_imbalance": abs(count_a - count_b),
                "active_seconds_a": active_seconds_a,
                "active_seconds_b": active_seconds_b,
                "active_imbalance": abs(active_seconds_a - active_seconds_b),
                "avg_duration_a": avg_duration_a,
                "avg_duration_b": avg_duration_b,
            }
        )

    ranked_windows.sort(
        key=lambda window: (
            -float(window.get("score", 0.0) or 0.0),
            -int(window.get("count_imbalance", 0) or 0),
            -float(window.get("active_imbalance", 0.0) or 0.0),
            pd.Timestamp(window["start"]),
        ),
    )
    return ranked_windows


def _select_operational_issue_anchor(
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    diff: Dict[str, object],
) -> Optional[Dict[str, object]]:
    """Select the largest local A-vs-B mismatch window for a non-clearance row."""
    ranked_windows = _rank_operational_issue_windows(timeline_a, timeline_b, diff)
    if not ranked_windows:
        return None
    return ranked_windows[0]


def _format_operational_issue_caption(
    issue_window: Dict[str, object],
    diff: Dict[str, object],
    *,
    label_a: str,
    label_b: str,
) -> str:
    detail_label = f"{str(diff.get('label', '')).strip()} {str(diff.get('state', '')).strip()}".strip()
    parts = [
        f"{detail_label}: largest local mismatch window",
        f"mismatch {float(issue_window.get('mismatch_seconds', 0.0) or 0.0):.2f}s",
        (
            f"{label_a} events {int(issue_window.get('count_a', 0) or 0)}, "
            f"active {float(issue_window.get('active_seconds_a', 0.0) or 0.0):.2f}s"
        ),
        (
            f"{label_b} events {int(issue_window.get('count_b', 0) or 0)}, "
            f"active {float(issue_window.get('active_seconds_b', 0.0) or 0.0):.2f}s"
        ),
    ]

    count_a = int(issue_window.get("count_a", 0) or 0)
    count_b = int(issue_window.get("count_b", 0) or 0)
    if count_a > 0 and count_b > 0:
        parts.append(
            f"window avg {label_a} {float(issue_window.get('avg_duration_a', 0.0) or 0.0):.2f}s vs "
            f"{label_b} {float(issue_window.get('avg_duration_b', 0.0) or 0.0):.2f}s"
        )

    return "; ".join(parts)


def _treat_phase_diff_as_non_clearance_issue(
    diff: Dict[str, object],
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    *,
    overlap_clearance_median_threshold: float = 6.0,
) -> bool:
    event_class = str(diff.get("event_class", "")).strip()
    if event_class in {"Green", "Overlap Green"}:
        return True
    if event_class not in {"Overlap Yellow", "Overlap Red"}:
        return False

    rows_a = _normalize_issue_timeline_rows(
        timeline_a,
        event_class,
        int(diff.get("event_value", 0) or 0),
    )
    rows_b = _normalize_issue_timeline_rows(
        timeline_b,
        event_class,
        int(diff.get("event_value", 0) or 0),
    )
    medians: List[float] = []
    if not rows_a.empty:
        medians.append(float(rows_a["Duration"].median()))
    if not rows_b.empty:
        medians.append(float(rows_b["Duration"].median()))
    return bool(medians) and max(medians) >= overlap_clearance_median_threshold


def _select_clearance_issue_spec(
    *,
    scenario_id: str,
    row: Dict[str, object],
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    label_a: str,
    label_b: str,
) -> Optional[Dict[str, object]]:
    rows_a = _normalize_issue_timeline_rows(
        timeline_a,
        str(row.get("event_class", "")),
        int(row.get("event_value", 0) or 0),
    )
    rows_b = _normalize_issue_timeline_rows(
        timeline_b,
        str(row.get("event_class", "")),
        int(row.get("event_value", 0) or 0),
    )

    def _peak(rows: pd.DataFrame, version_label: str) -> Optional[Dict[str, object]]:
        if rows.empty:
            return None
        median = float(rows["Duration"].median())
        deviations = rows["Duration"] - median
        idx = deviations.abs().idxmax()
        return {
            "version": version_label,
            "duration": float(rows.loc[idx, "Duration"]),
            "median": median,
            "deviation": float(deviations.loc[idx]),
            "start": pd.Timestamp(rows.loc[idx, "StartTime"]),
            "end": pd.Timestamp(rows.loc[idx, "EndTime"]),
        }

    peak_a = _peak(rows_a, label_a)
    peak_b = _peak(rows_b, label_b)
    peaks = [peak for peak in (peak_a, peak_b) if peak is not None]
    if not peaks:
        return None

    anchor = max(peaks, key=lambda item: abs(float(item["deviation"])))
    detail_label = f"{str(row.get('label', '')).strip()} {str(row.get('state', '')).strip()}".strip()
    relation = "above" if float(anchor["deviation"]) >= 0 else "below"

    version_parts: List[str] = []
    if peak_a is not None:
        version_parts.append(
            f"{label_a}: {peak_a['duration']:.2f}s vs median {peak_a['median']:.2f}s"
        )
    else:
        version_parts.append(f"{label_a}: no flagged irregular event")
    if peak_b is not None:
        version_parts.append(
            f"{label_b}: {peak_b['duration']:.2f}s vs median {peak_b['median']:.2f}s"
        )
    else:
        version_parts.append(f"{label_b}: no flagged irregular event")

    return {
        "caption": (
            f"{detail_label} shown below is {abs(float(anchor['deviation'])):.2f}s {relation} the median "
            f"in {anchor['version']} ({'; '.join(version_parts)})"
        ),
        "title": f"{scenario_id} Clearance Focus - {detail_label}",
        "start": anchor["start"],
        "end": anchor["end"],
        "score": abs(float(anchor["deviation"])),
    }


def _is_distinct_issue_window(
    start: pd.Timestamp,
    existing_specs: List[Dict[str, object]],
    *,
    min_spacing_seconds: float = 600.0,
) -> bool:
    return all(
        abs((pd.Timestamp(start) - pd.Timestamp(spec["start"])).total_seconds()) >= min_spacing_seconds
        for spec in existing_specs
    )


def _non_clearance_issue_group_key(diff: Dict[str, object]) -> Tuple[str, ...]:
    event_class = str(diff.get("event_class", "")).strip()
    label = str(diff.get("label", "")).strip()
    event_value = int(diff.get("event_value", 0) or 0)

    if event_class == "Preempt":
        return ("preempt", str(event_value))
    if event_class in {"Transition Longway", "Transition Shortway"}:
        return ("transition", event_class)
    if event_class == "Green":
        return ("phase_green",)
    if event_class == "Overlap Green":
        return ("overlap_green",)
    if event_class == "Overlap Yellow":
        return ("overlap_yellow",)
    if event_class == "Overlap Red":
        return ("overlap_red",)
    if event_class == "Ped Service":
        if label.startswith(("Ovlp Ped", "Overlap Ped")):
            return ("overlap_ped",)
        return ("ped",)
    return ("non_clearance", event_class)


def _issue_spec_priority(spec: Dict[str, object]) -> Tuple[float, int, float, float]:
    return (
        float(spec.get("score", 0.0) or 0.0),
        int(spec.get("count_imbalance", 0) or 0),
        float(spec.get("active_imbalance", 0.0) or 0.0),
        -float(pd.Timestamp(spec["start"]).value),
    )


def _issue_spec_sort_key(spec: Dict[str, object]) -> Tuple[float, int, float, pd.Timestamp]:
    return (
        -float(spec.get("score", 0.0) or 0.0),
        -int(spec.get("count_imbalance", 0) or 0),
        -float(spec.get("active_imbalance", 0.0) or 0.0),
        pd.Timestamp(spec["start"]),
    )


def _generate_special_issue_plots(
    *,
    scenario_id: str,
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    aligned_timeline_a: pd.DataFrame,
    aligned_timeline_b: pd.DataFrame,
    phase_differences: List[dict],
    clearance_irregularities: List[dict],
    operational_diffs: List[dict],
    plots_dir: str,
    label_a: str,
    label_b: str,
    window_minutes: float,
    time_offset_b: float,
    align_by_time_delta: bool,
    tod_align: bool = False,
) -> Tuple[List[str], List[str]]:
    """Create labeled charts for the worst flagged clearance and operational issues."""
    if timeline_a.empty or timeline_b.empty:
        return [], []

    issue_specs: List[Dict[str, object]] = []
    seen_keys = set()
    non_clearance_candidates_by_group: Dict[Tuple[str, ...], List[Dict[str, object]]] = {}
    coord_split_schedules = _load_coord_split_schedules(str(COORD_PATTERNS_DIR)) if tod_align else {}
    programmed_split_device_id = _resolve_coord_split_device_id(scenario_id, coord_split_schedules) if tod_align else None

    for row in clearance_irregularities:
        if int(row.get("irregular_count_a", 0) or 0) <= 0 and int(row.get("irregular_count_b", 0) or 0) <= 0:
            continue
        key = ("clearance", row.get("event_class"), row.get("event_value"))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        issue_spec = _select_clearance_issue_spec(
            scenario_id=scenario_id,
            row=row,
            timeline_a=aligned_timeline_a,
            timeline_b=aligned_timeline_b,
            label_a=label_a,
            label_b=label_b,
        )
        if issue_spec is None:
            continue
        issue_specs.append(issue_spec)

    phase_state_diffs = [
        diff for diff in phase_differences
        if _treat_phase_diff_as_non_clearance_issue(diff, aligned_timeline_a, aligned_timeline_b)
    ]

    for diff in [*operational_diffs, *phase_state_diffs]:
        if not _is_significant_operational_issue(diff):
            continue
        key = ("non_clearance", diff.get("event_class"), diff.get("event_value"))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        ranked_issue_windows = _rank_operational_issue_windows(aligned_timeline_a, aligned_timeline_b, diff)
        if not ranked_issue_windows:
            continue
        detail_label = f"{diff.get('label', '').strip()} {diff.get('state', '').strip()}".strip()
        group_key = _non_clearance_issue_group_key(diff)
        non_clearance_candidates_by_group.setdefault(group_key, [])
        for issue_window in ranked_issue_windows:
            event_class = str(diff.get("event_class", "")).strip()
            non_clearance_candidates_by_group[group_key].append(
                {
                    "caption": _format_operational_issue_caption(
                        issue_window,
                        diff,
                        label_a=label_a,
                        label_b=label_b,
                    ),
                    "title": f"{scenario_id} Issue Focus - {detail_label}",
                    "start": issue_window["start"],
                    "end": issue_window["end"],
                    "score": float(issue_window.get("score", 0.0) or 0.0),
                    "count_imbalance": int(issue_window.get("count_imbalance", 0) or 0),
                    "active_imbalance": float(issue_window.get("active_imbalance", 0.0) or 0.0),
                    "group_key": group_key,
                    "show_programmed_splits": programmed_split_device_id is not None,
                }
            )

    selected_so_far = list(issue_specs)
    ranked_groups = []
    for specs in non_clearance_candidates_by_group.values():
        specs.sort(key=_issue_spec_sort_key)
        ranked_groups.append(specs)

    ranked_groups.sort(key=lambda specs: _issue_spec_sort_key(specs[0]))
    for specs in ranked_groups:
        for spec in specs:
            if not _is_distinct_issue_window(pd.Timestamp(spec["start"]), selected_so_far):
                continue
            issue_specs.append(spec)
            selected_so_far.append(spec)
            break

    issue_specs.sort(key=_issue_spec_sort_key)
    deduped_issue_specs: List[Dict[str, object]] = []
    for spec in issue_specs:
        if not _is_distinct_issue_window(pd.Timestamp(spec["start"]), deduped_issue_specs):
            continue
        deduped_issue_specs.append(spec)

    output_dir = Path(plots_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    plot_paths: List[str] = []
    plot_captions: List[str] = []
    for index, spec in enumerate(deduped_issue_specs, start=1):
        output_path = output_dir / f"{scenario_id}_issue_{index}.png"
        programmed_split_timeline = None
        if spec.get("show_programmed_splits") and programmed_split_device_id is not None:
            programmed_split_timeline = _build_programmed_split_timeline(
                coord_split_schedules[programmed_split_device_id],
                pd.Timestamp(spec["start"]),
            )
        fig = sr.create_comparison_gantt_matplotlib(
            timeline_a=timeline_a,
            timeline_b=timeline_b,
            label_a=label_a,
            label_b=label_b,
            title=str(spec["title"]),
            divergence_start=spec["start"],
            divergence_end=spec["end"],
            output_path=output_path,
            window_minutes=window_minutes,
            dpi=150,
            align_by_time_delta=align_by_time_delta,
            time_offset_b=time_offset_b,
            programmed_split_timeline=programmed_split_timeline,
        )
        if fig is None:
            continue
        plot_paths.append(str(output_path))
        plot_captions.append(str(spec["caption"]))
        plt.close(fig)

    return plot_paths, plot_captions


def _compare_one_scenario(args: Tuple) -> dict:
    """
    Worker function for ProcessPoolExecutor.
    All arguments are plain strings/numbers to avoid pickle issues.
    Reads collected output directly from DuckDB.
    """
    (scenario_id, baseline_source, baseline_label, test_type_str, collected_db_path,
     firmware_version, plots_dir_str, settle_minutes, group_tolerance,
      max_plots, window_minutes, verbose, notes_column, tod_align,
          analysis_start_time, analysis_end_time, phase_call_threshold) = args

    import signal_replay as sr
    import pandas as pd
    import os, contextlib

    test_type = sr.TestType.CONFLICT if test_type_str == "CONFLICT" else sr.TestType.SIMILARITY

    baseline = _load_baseline_events(baseline_source, scenario_id)
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

    if test_type == sr.TestType.SIMILARITY and not result.thrown_out and not result.chunk_scores:
        result.thrown_out = True
        result.thrown_out_reason = (
            getattr(result, "thrown_out_reason", "")
            or "Insufficient scored chunks remained after settling/filtering for a reliable comparison."
        )

    plot_paths: list = []
    plot_captions: list = []
    timeline_a = timeline_b = None
    valid_timeline_a = valid_timeline_b = None
    invalid_timeline_a = invalid_timeline_b = None
    analysis_diagnostics = []
    chart_time_offset_b = 0.0
    chart_align_by_time_delta = not tod_align
    if test_type == sr.TestType.SIMILARITY:
        analysis_diagnostics.append(
            "Similarity chunks after settle/filtering: "
            f"total={len(result.chunk_scores)}, included={result.included_chunk_count}, excluded={result.excluded_chunk_count}"
        )
        if not result.chunk_scores:
            analysis_diagnostics.append(
                "Comparison produced no scored chunks; match likely fell back to full-sequence DTW or a very short overlap."
            )
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
            timeline_a = _remove_ignored_timeline_events(timeline_a)
            timeline_b = _remove_ignored_timeline_events(timeline_b)
            valid_timeline_a, invalid_timeline_a = _split_timeline_by_validity(timeline_a)
            valid_timeline_b, invalid_timeline_b = _split_timeline_by_validity(timeline_b)
            if tod_align or valid_timeline_a.empty or valid_timeline_b.empty:
                chart_time_offset_b = 0.0
            else:
                chart_time_offset_b = sr.compute_timeline_offset(valid_timeline_a, valid_timeline_b)
            analysis_diagnostics.append(
                "Timeline rows after removing input-only classes: "
                f"original={len(timeline_a)}, new={len(timeline_b)}"
            )
            analysis_diagnostics.append(
                "Valid rows used for detailed summaries: "
                f"original={len(valid_timeline_a)}, new={len(valid_timeline_b)}; "
                "invalid rows reserved for Data Integrity: "
                f"original={len(invalid_timeline_a)}, new={len(invalid_timeline_b)}"
            )
            if valid_timeline_a.empty or valid_timeline_b.empty:
                analysis_diagnostics.append(
                    "Filtered valid timelines do not contain enough signal, overlap, transition, preempt, or pedestrian-service rows for detailed summaries."
                )
        except Exception as e:
            analysis_diagnostics.append(f"Timeline generation failed: {e}")
            if verbose:
                print(f"    Timeline generation failed for {scenario_id}: {e}", flush=True)

    passed = (not result.thrown_out) and result.match_percentage >= 95.0
    phase_diffs: list = []
    clearance_irregularities: list = []
    operational_diffs: list = []
    invalid_clearance_irregularities: list = []
    invalid_operational_diffs: list = []
    timeline_difference_analysis_available = False
    if (
        test_type == sr.TestType.SIMILARITY
        and valid_timeline_a is not None
        and valid_timeline_b is not None
        and not valid_timeline_a.empty
        and not valid_timeline_b.empty
    ):
        try:
            tl_a_settled, tl_b_settled = _prepare_settled_overlap_timelines(
                valid_timeline_a,
                valid_timeline_b,
                settle_minutes=compare_settle_minutes,
                tod_align=tod_align,
            )
            analysis_diagnostics.append(
                "Settled overlap rows used for timeline summaries: "
                f"original={len(tl_a_settled)}, new={len(tl_b_settled)}"
            )
            if tl_a_settled.empty or tl_b_settled.empty:
                analysis_diagnostics.append(
                    "No overlapping settled timeline remained after alignment and overlap clipping."
                )

            if verbose:
                signal_classes = {'Green', 'Yellow', 'Red', 'Overlap Green', 'Overlap Trail Green', 'Overlap Yellow', 'Overlap Red'}
                sig_a = tl_a_settled[tl_a_settled["EventClass"].isin(signal_classes)]
                sig_b = tl_b_settled[tl_b_settled["EventClass"].isin(signal_classes)]
                print(f"    [diag] valid_timeline_a range: {valid_timeline_a['StartTime'].min()} to {valid_timeline_a['EndTime'].max()}", flush=True)
                print(f"    [diag] valid_timeline_b range: {valid_timeline_b['StartTime'].min()} to {valid_timeline_b['EndTime'].max()}", flush=True)
                overlap_end = min(tl_a_settled["EndTime"].max(), tl_b_settled["EndTime"].max())
                print(f"    [diag] after settle+overlap: tl_a={len(tl_a_settled)} ({len(sig_a)} signal), "
                      f"tl_b={len(tl_b_settled)} ({len(sig_b)} signal), overlap_end={overlap_end}", flush=True)

            phase_diffs = sr.generate_phase_difference_summary(tl_a_settled, tl_b_settled, tolerance_seconds=0.2)
            clearance_irregularities = sr.generate_clearance_irregularity_summary(
                tl_a_settled,
                tl_b_settled,
                threshold_seconds=0.1,
            )
            operational_diffs = sr.generate_operational_difference_summary(tl_a_settled, tl_b_settled, tolerance_seconds=0.2)
            timeline_difference_analysis_available = True

            try:
                _devnull2 = open(os.devnull, "w") if not verbose else None
                _ctx2 = contextlib.redirect_stdout(_devnull2) if _devnull2 else contextlib.nullcontext()
                with _ctx2:
                    programmed_split_timeline = None
                    if tod_align:
                        coord_split_schedules = _load_coord_split_schedules(str(COORD_PATTERNS_DIR))
                        programmed_split_device_id = _resolve_coord_split_device_id(scenario_id, coord_split_schedules)
                        if programmed_split_device_id is not None:
                            programmed_split_timeline = _build_programmed_split_timeline(
                                coord_split_schedules[programmed_split_device_id],
                                valid_timeline_a["StartTime"].min(),
                            )

                    issue_plot_paths, issue_plot_captions = _generate_special_issue_plots(
                        scenario_id=scenario_id,
                        timeline_a=valid_timeline_a,
                        timeline_b=valid_timeline_b,
                        aligned_timeline_a=tl_a_settled,
                        aligned_timeline_b=tl_b_settled,
                        phase_differences=phase_diffs,
                        clearance_irregularities=clearance_irregularities,
                        operational_diffs=operational_diffs,
                        plots_dir=plots_dir_str,
                        label_a=baseline_label,
                        label_b=firmware_version,
                        window_minutes=window_minutes,
                        time_offset_b=chart_time_offset_b,
                        align_by_time_delta=chart_align_by_time_delta,
                        tod_align=tod_align,
                    )

                    remaining_divergence_plots = max(0, max_plots - len(issue_plot_paths))
                    divergence_paths = sr.create_multi_divergence_plots(
                        timeline_a=valid_timeline_a,
                        timeline_b=valid_timeline_b,
                        comparison_result=result,
                        output_dir=plots_dir_str,
                        label_a=baseline_label,
                        label_b=firmware_version,
                        max_plots=remaining_divergence_plots,
                        window_minutes=window_minutes,
                        time_offset_b=chart_time_offset_b,
                        align_by_time_delta=chart_align_by_time_delta,
                        programmed_split_timeline=programmed_split_timeline,
                    ) if remaining_divergence_plots > 0 else []

                plot_paths = issue_plot_paths + divergence_paths
                plot_captions = issue_plot_captions + [
                    f"Divergence {index}" for index in range(1, len(divergence_paths) + 1)
                ]
                if _devnull2:
                    _devnull2.close()
            except Exception as e:
                if verbose:
                    print(f"    Chart generation failed for {scenario_id}: {e}", flush=True)
                    import traceback as _tb
                    _tb.print_exc()
        except Exception as e:
            analysis_diagnostics.append(f"Timeline difference summary failed: {e}")
            if verbose:
                print(f"    Phase breakdown failed for {scenario_id}: {e}", flush=True)
                import traceback as _tb
                _tb.print_exc()

    if test_type == sr.TestType.SIMILARITY and invalid_timeline_a is not None and invalid_timeline_b is not None:
        try:
            tl_a_invalid, tl_b_invalid = _prepare_settled_overlap_timelines(
                invalid_timeline_a,
                invalid_timeline_b,
                settle_minutes=compare_settle_minutes,
                tod_align=tod_align,
            )
            if not tl_a_invalid.empty or not tl_b_invalid.empty:
                invalid_clearance_irregularities = sr.generate_clearance_irregularity_summary(
                    tl_a_invalid,
                    tl_b_invalid,
                    threshold_seconds=0.1,
                )
                invalid_operational_diffs = sr.generate_operational_difference_summary(
                    tl_a_invalid,
                    tl_b_invalid,
                    tolerance_seconds=0.2,
                )
        except Exception as e:
            analysis_diagnostics.append(f"Invalid-event timeline summary failed: {e}")
            if verbose:
                print(f"    Invalid-event breakdown failed for {scenario_id}: {e}", flush=True)
                import traceback as _tb
                _tb.print_exc()

    if test_type == sr.TestType.SIMILARITY and not timeline_difference_analysis_available and not analysis_diagnostics:
        analysis_diagnostics.append("Detailed timeline analysis was unavailable for this scenario.")

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
        "plot_captions": plot_captions,
        "phase_diffs": phase_diffs,
        "clearance_irregularities": clearance_irregularities,
        "operational_diffs": operational_diffs,
        "invalid_clearance_irregularities": invalid_clearance_irregularities,
        "invalid_operational_diffs": invalid_operational_diffs,
        "analysis_diagnostics": analysis_diagnostics,
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
        "thrown_out_reason": getattr(result, "thrown_out_reason", ""),
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
    baseline_sources: Dict[str, BaselineSource],
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
                f"WARNING: Missing baseline source for {scenario.scenario_id}; "
                "skipping device CSV export for this scenario."
            )
            continue

        baseline = _load_baseline_events(baseline_source, scenario.scenario_id)
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
    *,
    export_device_csvs: bool = True,
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

    baseline_db_path, baseline_label = resolve_baseline_db_path(firmware_dir, settings)
    if baseline_db_path is not None:
        log(f"Baseline collected DB: {baseline_db_path}")
    else:
        log(
            f"Baseline collected DB not found for {settings['baseline_version']}; "
            "using source logs/ as fallback."
        )

    if collected_db_path.exists():
        log(f"Reading collected output directly from DuckDB: {collected_db_path}")
    else:
        log(f"Collected DuckDB not found: {collected_db_path}")

    baseline_sources: Dict[str, BaselineSource] = {}
    for scenario in suite.scenarios:
        baseline_source, _ = resolve_baseline_source(scenario.scenario_id, firmware_dir, settings)
        if baseline_source is None:
            log(
                f"WARNING: Missing baseline source for {scenario.scenario_id}; "
                "this scenario will be skipped during analysis."
            )
            continue
        baseline_sources[scenario.scenario_id] = baseline_source

    if export_device_csvs:
        # Export per-device CSVs (baseline + collected combined)
        log("Exporting per-device CSV files...")
        _export_device_csvs(suite, collected_db_path, baseline_sources, group_tolerance=group_tolerance)
    else:
        log("Skipping per-device CSV export.")

    analysis_start_time = comp.get("analysis_start_time")
    analysis_end_time = comp.get("analysis_end_time")
    if analysis_start_time:
        log(f"TOD manual analysis start time: {analysis_start_time}")
    if analysis_end_time:
        log(f"TOD manual analysis end time: {analysis_end_time}")

    # Build job list — all plain types for pickling.
    jobs: list = []
    conflict_jobs: List[Tuple[sr.TestScenario, BaselineSource, Path]] = []
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
            baseline_source,
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
                        plot_captions=out.get("plot_captions", []),
                        phase_differences=out["phase_diffs"],
                        clearance_irregularities=out.get("clearance_irregularities", []),
                        operational_differences=out.get("operational_diffs", []),
                        invalid_clearance_irregularities=out.get("invalid_clearance_irregularities", []),
                        invalid_operational_differences=out.get("invalid_operational_diffs", []),
                        runs_completed=out.get("runs_completed", 1),
                        total_runs=out.get("total_runs", 1),
                        chunk_scores=out.get("chunk_scores", []),
                        phase_call_chunk_scores=out.get("phase_call_chunk_scores", out.get("detector_chunk_scores", [])),
                        included_chunk_count=out.get("included_chunk_count", 0),
                        excluded_chunk_count=out.get("excluded_chunk_count", 0),
                        thrown_out=out.get("thrown_out", False),
                        thrown_out_reason=out.get("thrown_out_reason", ""),
                        analysis_diagnostics=out.get("analysis_diagnostics", []),
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
    report_mode = parser.add_mutually_exclusive_group()
    report_mode.add_argument("--report-only", action="store_true", help="Skip replay, just run analysis + report")
    report_mode.add_argument(
        "--report-only-fast",
        action="store_true",
        help="Skip replay and rebuild the report without refreshing device CSV exports",
    )
    parser.add_argument("--archive", action="store_true", help="Refresh the versioned collected logs after report generation")
    parser.add_argument(
        "--settle-minutes",
        type=float,
        default=None,
        help="Override the initial minutes excluded from similarity analysis/reporting",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=None,
        metavar="N",
        help="Limit the report to the first N scenarios (by scenario ID). Useful for quick local testing.",
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
        if not args.report_only and not args.report_only_fast:
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

        if args.top_n is not None:
            suite.scenarios = suite.scenarios[:args.top_n]
            log(f"--top-n {args.top_n}: limiting analysis to {len(suite.scenarios)} scenario(s): {', '.join(s.scenario_id for s in suite.scenarios)}")

        export_device_csvs = not args.report_only_fast
        if args.report_only_fast:
            log("Report-only-fast mode: skipping device CSV refresh.")

        results = run_analysis(
            suite,
            settings,
            firmware_dir,
            export_device_csvs=export_device_csvs,
        )
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
