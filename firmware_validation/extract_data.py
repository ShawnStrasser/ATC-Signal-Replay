#!/usr/bin/env python
"""
extract_data.py — Extract collected event logs from a firmware validation run.

This script reads the DuckDB files recorded for a firmware run and exports one
parquet file per device into firmware_validation/logs_<firmware_version>/.

Usage:
    python extract_data.py --firmware-version 2.3.1.0
    python extract_data.py --firmware-version 2.3.1.0 --settings custom_settings.json

Notes:
- Firmware versions containing periods are supported as-is.
- Output folder will be named exactly logs_<firmware_version>.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict

import duckdb
import pandas as pd


def load_settings(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_logs(firmware_dir: Path, firmware_version: str, settings_path: Path) -> Path:
    settings = load_settings(settings_path)
    results_dir = firmware_dir / settings["results_dir"]
    run_dir = results_dir / firmware_version
    checkpoint_path = run_dir / "checkpoint.json"

    if not checkpoint_path.exists():
        raise FileNotFoundError(
            f"Checkpoint not found for firmware version '{firmware_version}': {checkpoint_path}"
        )

    with open(checkpoint_path, "r", encoding="utf-8") as f:
        checkpoint = json.load(f)

    scenario_db_map: Dict[str, str] = checkpoint.get("scenario_db_map", {})
    if not scenario_db_map:
        raise ValueError(f"No scenario_db_map found in {checkpoint_path}")

    output_dir = firmware_dir / f"logs_{firmware_version}"
    output_dir.mkdir(parents=True, exist_ok=True)

    extracted = 0
    for scenario_id, db_path_str in sorted(scenario_db_map.items()):
        db_path = Path(db_path_str)
        if not db_path.exists():
            print(f"Skipping {scenario_id}: DuckDB file not found: {db_path}", flush=True)
            continue

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            df = con.execute(
                "SELECT * FROM events WHERE device_id = ? ORDER BY timestamp",
                [scenario_id],
            ).df()
        finally:
            con.close()

        if df.empty:
            print(f"Skipping {scenario_id}: no events found", flush=True)
            continue

        out_path = output_dir / f"{scenario_id}.parquet"
        df.to_parquet(out_path, index=False)
        extracted += 1
        print(f"Extracted {scenario_id} -> {out_path.name} ({len(df)} events)", flush=True)

    print(f"Done. Extracted {extracted} parquet file(s) to {output_dir}", flush=True)
    return output_dir


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract device event logs from a firmware validation run into parquet files."
    )
    parser.add_argument(
        "--firmware-version",
        required=True,
        help="Firmware/test version folder name to extract from, e.g. 2.3.1.0",
    )
    parser.add_argument(
        "--settings",
        default="settings.json",
        help="Settings JSON file relative to firmware_validation/",
    )
    args = parser.parse_args()

    firmware_dir = Path(__file__).resolve().parent
    settings_path = firmware_dir / args.settings
    if not settings_path.exists():
        print(f"ERROR: Settings file not found: {settings_path}", file=sys.stderr)
        sys.exit(1)

    try:
        extract_logs(
            firmware_dir=firmware_dir,
            firmware_version=args.firmware_version,
            settings_path=settings_path,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
