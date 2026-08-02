from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import platform
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    import duckdb
except ImportError as exc:
    raise SystemExit("duckdb is required: py -3 -m pip install duckdb") from exc

ROOT = Path(__file__).resolve().parents[2]
CONFIG = Path(__file__).resolve().parent / "paper_config.toml"
SCENARIOS = [
    "01066", "03013", "05018", "08042", "08404", "08411", "12035", "12036",
    "12059", "13008", "13010", "2B009", "2B045", "2B049", "2B052", "2B054",
    "2B085", "2B094", "2B337", "2B339", "2B349", "2B530", "2C009", "2C042", "2C043",
]


def parse_simple_toml(path: Path) -> dict:
    result: dict[str, dict[str, object]] = {}
    section = None
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            result[section] = {}
            continue
        if section is None or "=" not in line:
            continue
        key, value = [x.strip() for x in line.split("=", 1)]
        if value.startswith('"'):
            parsed: object = value.strip('"')
        elif value.startswith("["):
            parsed = [int(x.strip()) if x.strip().isdigit() else x.strip().strip('"') for x in value[1:-1].split(",") if x.strip()]
        elif value.lower() in {"true", "false"}:
            parsed = value.lower() == "true"
        else:
            try:
                parsed = float(value) if "." in value else int(value)
            except ValueError:
                parsed = value
        result[section][key] = parsed
    return result


def resolve(config: dict, section: str, key: str) -> Path:
    return (Path(__file__).resolve().parent / str(config[section][key])).resolve()


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def db_summary(path: Path, label: str) -> dict:
    con = duckdb.connect(str(path), read_only=True)
    tables = {row[0] for row in con.execute("show tables").fetchall()}
    out: dict[str, object] = {"label": label, "database": str(path), "sha256": sha256(path), "tables": ",".join(sorted(tables))}
    for table in ["events", "input_events", "input_detector_events", "latency_offset_samples", "latency_offset_updates"]:
        if table in tables:
            out[f"{table}_count"] = int(con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
        else:
            out[f"{table}_count"] = 0
    if "events" in tables:
        out["event_devices"] = int(con.execute('SELECT count(DISTINCT device_id) FROM events').fetchone()[0])
        lo, hi = con.execute("SELECT min(timestamp), max(timestamp) FROM events").fetchone()
        out["event_start"] = lo.isoformat() if lo else ""
        out["event_end"] = hi.isoformat() if hi else ""
        if lo and hi:
            out["event_duration_hours"] = round((hi - lo).total_seconds() / 3600, 3)
    if "input_events" in tables:
        lo, hi = con.execute("SELECT min(timestamp), max(timestamp) FROM input_events").fetchone()
        out["input_start"] = lo.isoformat() if lo else ""
        out["input_end"] = hi.isoformat() if hi else ""
    if "simulation_runs" in tables:
        out["run_count"] = int(con.execute("SELECT count(*) FROM simulation_runs").fetchone()[0])
        out["run_statuses"] = ";".join(f"{s}:{n}" for s, n in con.execute("SELECT status,count(*) FROM simulation_runs GROUP BY status ORDER BY status").fetchall())
    con.close()
    return out


def parse_report(path: Path) -> tuple[dict, list[dict]]:
    text = path.read_text(encoding="utf-8", errors="replace")
    def tile(label: str) -> str:
        m = re.search(r'<div class="value">([^<]+)</div>\s*<div class="label">' + re.escape(label), text)
        return html.unescape(m.group(1).strip()) if m else ""
    summary = {
        "report": str(path),
        "generated": (re.search(r'<div class="generated">([^<]+)</div>', text) or ["", ""])[1],
        "scenarios_passed": tile("Scenarios Passed"),
        "avg_sequence_match": tile("Avg Sequence Match").rstrip("%"),
        "avg_timing_match": tile("Avg Timing Match").rstrip("%"),
    }
    start = text.find('<h2>Similarity Results</h2>')
    end = text.find('</table>', start)
    table = text[start:end if end > start else len(text)]
    rows = []
    pattern = re.compile(
        r'<tr>\s*<td><a href="#(?P<scenario>[^"]+)">[^<]+</a></td>\s*'
        r'<td[^>]*>\s*(?P<sequence>[0-9.]+)%\s*</td>\s*'
        r'<td[^>]*>\s*(?:(?P<timing>[0-9.]+)%|&mdash;)\s*</td>\s*'
        r'<td><span class="badge (?P<status>[^"]+)">(?P<status_text>[^<]+)</span>', re.S)
    for match in pattern.finditer(table):
        row = match.groupdict()
        row["scenario"] = row["scenario"].upper()
        row["sequence_match_percent"] = float(row.pop("sequence"))
        row["timing_match_percent"] = float(row.pop("timing")) if row.get("timing") else None
        row["status"] = row.pop("status_text").strip().upper()
        row["timing_available"] = row["timing_match_percent"] is not None
        rows.append(row)
    if len(rows) != len(SCENARIOS):
        raise RuntimeError(f"Expected {len(SCENARIOS)} rows in {path}, found {len(rows)}")
    return summary, rows


def source_summary(log_dir: Path) -> dict:
    rows = []
    for path in sorted(log_dir.glob("*.parquet")):
        if path.stem not in SCENARIOS:
            continue
        frame = pd.read_parquet(path)
        timestamp_column = "timestamp" if "timestamp" in frame.columns else "TimeStamp"
        timestamp = pd.to_datetime(frame[timestamp_column], errors="coerce")
        rows.append({"scenario": path.stem, "rows": len(frame), "start": timestamp.min().isoformat() if not timestamp.isna().all() else "", "end": timestamp.max().isoformat() if not timestamp.isna().all() else "", "duration_hours": round((timestamp.max() - timestamp.min()).total_seconds()/3600, 3) if not timestamp.isna().all() else 0})
    return {"files": len(rows), "rows": int(sum(x["rows"] for x in rows)), "duration_hours_sum": round(sum(x["duration_hours"] for x in rows), 3), "rows_by_scenario": rows}


def main() -> None:
    parser = argparse.ArgumentParser(description="Reproduce the stored comparison/reporting summaries used by the TRB paper.")
    parser.add_argument("--config", type=Path, default=CONFIG)
    args = parser.parse_args()
    config = parse_simple_toml(args.config.resolve())
    output = ROOT / "paper" / "generated" / "results"
    output.mkdir(parents=True, exist_ok=True)
    db15 = resolve(config, "paths", "database_2_15_1")
    db18 = resolve(config, "paths", "database_2_18_1")
    dbtrail = resolve(config, "paths", "database_2_18_1_parameter_modified")
    for db in (db15, db18, dbtrail):
        if not db.exists():
            raise FileNotFoundError(db)
    source = source_summary(resolve(config, "paths", "source_logs"))
    summaries = [db_summary(db15, "2.15.1"), db_summary(db18, "2.18.1"), db_summary(dbtrail, "2.18.1_parameter_modified")]
    pd.DataFrame(summaries).to_csv(output / "dataset_summary.csv", index=False)
    software_summary, software_rows = parse_report(resolve(config, "paths", "report_2_18_1"))
    parameter_summary, parameter_rows = parse_report(resolve(config, "paths", "report_2_18_1_parameter_modified"))
    for row in software_rows:
        row.update({"comparison": "software_release", "baseline": "2.15.1", "candidate": "2.18.1", "source_report": software_summary["report"]})
    for row in parameter_rows:
        row.update({"comparison": "parameter_intervention", "baseline": "2.18.1", "candidate": "2.18.1_trailing", "source_report": parameter_summary["report"]})
    pd.DataFrame(software_rows).sort_values("scenario").to_csv(output / "software_release_results.csv", index=False)
    manifest = pd.read_csv(Path(__file__).resolve().parent / "intervention_manifest.csv", dtype=str)
    param = pd.DataFrame(parameter_rows).rename(columns={"scenario": "scenario_id"})
    joined = manifest.merge(param[["scenario_id", "sequence_match_percent", "timing_match_percent", "status", "timing_available"]], on="scenario_id", how="left")
    joined["automatically_flagged"] = joined["status"].ne("PASS")
    joined.to_csv(output / "parameter_intervention_results.csv", index=False)
    detection = []
    for group, frame in joined.groupby("expected_comparison_group", sort=True):
        detection.append({"group": group, "configurations": len(frame), "flagged": int(frame["automatically_flagged"].sum()), "passed": int((~frame["automatically_flagged"]).sum()), "flag_rate_percent": round(100 * frame["automatically_flagged"].mean(), 1)})
    pd.DataFrame(detection).to_csv(output / "parameter_detection_summary.csv", index=False)
    sensitivity_rows = []
    for factor, values in [("grouping_tolerance_seconds", [0.10, 0.25, 0.50]), ("phase_call_reliability_percent", [80, 85, 90]), ("timing_tolerance_seconds", [0.25, 0.50, 1.00])]:
        for value in values:
            sensitivity_rows.append({"factor": factor, "value": value, "software_candidate_failures": "not_run", "parameter_changed_flagged": "not_run", "parameter_unchanged_flagged": "not_run", "status": "not_run_from_archived_reports", "note": "Requires rerunning comparison engine; archived HTML summaries do not contain parameter-sensitivity variants."})
    pd.DataFrame(sensitivity_rows).to_csv(output / "sensitivity_results.csv", index=False)
    metadata = {"generated_utc": datetime.now(timezone.utc).isoformat(), "python": platform.python_version(), "platform": platform.platform(), "source_commit": config.get("source", {}).get("source_commit", ""), "source_summary": source, "database_summaries": summaries, "software_report_summary": software_summary, "parameter_report_summary": parameter_summary, "comparison_method": "Archived HTML report rows cross-checked against stored DuckDB counts; no controller replay is performed by this script."}
    (output / "run_metadata.json").write_text(json.dumps(metadata, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"software": software_summary, "parameter": parameter_summary, "source_files": source["files"], "source_rows": source["rows"]}, indent=2))

if __name__ == "__main__":
    main()
