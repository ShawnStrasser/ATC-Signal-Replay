import base64
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from jinja2 import Template

from .comparison import PhaseCallChunkScore, render_sparkline_svg
from .test_suite import FirmwareTestSuite, ScenarioResult, TestType


def load_annotations(path: str) -> Dict[str, str]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return {str(k): str(v) for k, v in data.items()}


def _image_to_base64(path: str) -> Optional[str]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return None
    with open(p, "rb") as f:
        content = f.read()
    return base64.b64encode(content).decode("utf-8")


def _coerce_phase_call_chunk_scores(raw_scores: List[dict]) -> List[PhaseCallChunkScore]:
    scores: List[PhaseCallChunkScore] = []
    for item in raw_scores or []:
        if isinstance(item, PhaseCallChunkScore):
            scores.append(item)
            continue
        scores.append(
            PhaseCallChunkScore(
                center_seconds=float(item.get("center_seconds", 0.0)),
                window_seconds=float(item.get("window_seconds", 0.0)),
                similarity_percentage=item.get("similarity_percentage"),
                has_activity=bool(item.get("has_activity", False)),
                excluded_from_match=bool(item.get("excluded_from_match", False)),
            )
        )
    return scores


def _build_combined_phase_call_timeline(
    results: List[ScenarioResult],
    threshold: float,
) -> str:
    aggregate: Dict[tuple, List[float]] = {}

    for row in results:
        if row.test_type != TestType.SIMILARITY:
            continue
        raw_scores = getattr(row, "phase_call_chunk_scores", getattr(row, "detector_chunk_scores", []))
        for score in _coerce_phase_call_chunk_scores(raw_scores):
            if score.similarity_percentage is None:
                continue
            key = (round(score.center_seconds, 6), round(score.window_seconds, 6))
            aggregate.setdefault(key, []).append(float(score.similarity_percentage))

    combined_scores = [
      PhaseCallChunkScore(
            center_seconds=center_seconds,
            window_seconds=window_seconds,
            similarity_percentage=sum(values) / len(values),
            has_activity=True,
            excluded_from_match=(sum(values) / len(values)) < threshold,
        )
        for (center_seconds, window_seconds), values in sorted(aggregate.items())
        if values
    ]

    if not combined_scores:
        return ""

    return render_sparkline_svg(
        [],
        phase_call_chunk_scores=combined_scores,
        phase_call_threshold=threshold,
        auto_scale_y=True,
        show_exclusion_legend=False,
    )


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_difference_rows(diffs: List[dict]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for item in diffs or []:
        row = dict(item)
        count_a = _as_int(row.get("count_a"))
        count_b = _as_int(row.get("count_b"))
        duration_a = _as_float(row.get("duration_a"))
        duration_b = _as_float(row.get("duration_b"))
        total_duration_a = row.get("total_duration_a")
        total_duration_b = row.get("total_duration_b")
        if total_duration_a is None:
            total_duration_a = count_a * duration_a
        if total_duration_b is None:
            total_duration_b = count_b * duration_b

        row["count_a"] = count_a
        row["count_b"] = count_b
        row["count_delta"] = _as_int(row.get("count_delta"), count_b - count_a)
        row["duration_a"] = duration_a
        row["duration_b"] = duration_b
        row["duration_delta"] = _as_float(row.get("duration_delta"), duration_b - duration_a)
        row["total_duration_a"] = _as_float(total_duration_a)
        row["total_duration_b"] = _as_float(total_duration_b)
        row["total_duration_delta"] = _as_float(
            row.get("total_duration_delta"),
            row["total_duration_b"] - row["total_duration_a"],
        )
        normalized.append(row)
    return normalized


def _normalize_clearance_rows(rows: List[dict]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for item in rows or []:
        row = dict(item)
        row["median_a"] = _as_float(row.get("median_a"))
        row["median_b"] = _as_float(row.get("median_b"))
        row["median_delta"] = _as_float(row.get("median_delta"), row["median_b"] - row["median_a"])
        row["irregular_count_a"] = _as_int(row.get("irregular_count_a"))
        row["irregular_count_b"] = _as_int(row.get("irregular_count_b"))
        row["irregular_count_delta"] = _as_int(
            row.get("irregular_count_delta"),
            row["irregular_count_b"] - row["irregular_count_a"],
        )
        row["high_count_a"] = _as_int(row.get("high_count_a"))
        row["high_count_b"] = _as_int(row.get("high_count_b"))
        row["low_count_a"] = _as_int(row.get("low_count_a"))
        row["low_count_b"] = _as_int(row.get("low_count_b"))
        row["high_avg_deviation_a"] = _as_float(row.get("high_avg_deviation_a"))
        row["high_avg_deviation_b"] = _as_float(row.get("high_avg_deviation_b"))
        row["low_avg_deviation_a"] = _as_float(row.get("low_avg_deviation_a"))
        row["low_avg_deviation_b"] = _as_float(row.get("low_avg_deviation_b"))
        row["avg_deviation_a"] = _signed_clearance_avg_deviation(row, "a")
        row["avg_deviation_b"] = _signed_clearance_avg_deviation(row, "b")
        row["sample_count_a"] = _as_int(row.get("sample_count_a"))
        row["sample_count_b"] = _as_int(row.get("sample_count_b"))
        if row["irregular_count_b"] <= 0:
            continue
        normalized.append(row)
    return normalized


def _choose_primary_diagnostic(diagnostics: List[str]) -> str:
    if not diagnostics:
        return ""

    preferred_markers = (
        "Timeline rows after removing input-only classes",
        "Filtered timelines do not contain enough",
        "No overlapping settled timeline remained",
        "Timeline generation failed",
        "Timeline difference summary failed",
        "Comparison produced no scored chunks",
    )
    for marker in preferred_markers:
        for line in diagnostics:
            if marker in line:
                return line
    return diagnostics[0]


def _resolve_similarity_thrown_out(row: ScenarioResult) -> Tuple[bool, str]:
  if row.test_type != TestType.SIMILARITY:
    return False, ""

  error_text = str(getattr(row, "error", "") or "").strip()
  missing_data_markers = (
    "No collected events found",
    "Replay data for this scenario is missing",
  )
  if error_text and any(marker in error_text for marker in missing_data_markers):
    return True, error_text

  reason = str(getattr(row, "thrown_out_reason", "") or "").strip()
  if getattr(row, "thrown_out", False):
    return True, reason
  if reason:
    return True, reason

  diagnostics = [
    str(line).strip()
    for line in (getattr(row, "analysis_diagnostics", []) or [])
    if str(line).strip()
  ]
  throw_out_markers = (
    "Similarity chunks after settle/filtering: total=0",
    "Comparison produced no scored chunks",
    "Insufficient scored chunks remained after settling/filtering",
  )
  if any(marker in line for line in diagnostics for marker in throw_out_markers):
    return True, "Insufficient scored chunks remained after settling/filtering for a reliable comparison."

  return False, ""


def _trend_row_class(trend_type: str, pattern: str) -> str:
    if "Yellow" in pattern:
        return "trend-yellow"
    if "Red" in pattern:
        return "trend-red"
    if "Overlap Ped" in pattern:
        return "trend-overlap-ped"
    if pattern.startswith("Ped Service"):
        return "trend-ped"
    if "Preempt" in pattern:
        return "trend-preempt"
    if "Transition" in pattern:
        return "trend-transition"
    return "trend-operational"


def _signed_clearance_avg_deviation(row: Dict[str, Any], suffix: str) -> float:
    high_count = _as_int(row.get(f"high_count_{suffix}"))
    low_count = _as_int(row.get(f"low_count_{suffix}"))
    total_count = high_count + low_count
    if total_count <= 0:
        return 0.0

    high_total = high_count * _as_float(row.get(f"high_avg_deviation_{suffix}"))
    low_total = low_count * _as_float(row.get(f"low_avg_deviation_{suffix}"))
    return (high_total - low_total) / total_count


def _flag_clearance_irregularity(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return one flagged dict for a clearance row using total irregular counts."""
    label = str(row.get("label", "")).strip()
    state = str(row.get("state", "")).strip()

    if label.startswith("Ph "):
        base_pattern = f"Phase {state}"
    elif label.startswith("Ovlp "):
        base_pattern = f"Overlap {state}"
    else:
        base_pattern = f"{label} {state}".strip()

    detail_label = f"{label} {state}".strip()
    count_a = _as_int(row.get("irregular_count_a"))
    count_b = _as_int(row.get("irregular_count_b"))
    if count_b <= 0:
        return None

    avg_deviation_a = _signed_clearance_avg_deviation(row, "a")
    avg_deviation_b = _signed_clearance_avg_deviation(row, "b")
    return {
        "type": "Clearance irregularities",
        "pattern": base_pattern,
        "message": (
          f"{detail_label}: {count_a}\u2192{count_b} irregular events "
          f"(new avg deviation {avg_deviation_b:+.2f}s from median)"
        ),
        "count_a": count_a,
        "count_b": count_b,
        "count_delta": count_b - count_a,
        "avg_deviation_a": avg_deviation_a,
        "avg_deviation_b": avg_deviation_b,
    }


def _average(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _flag_operational_difference(diff: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    label = str(diff.get("label", "")).strip()
    state = str(diff.get("state", "")).strip()
    avg_delta = _as_float(diff.get("duration_delta"))
    total_delta = _as_float(diff.get("total_duration_delta"))
    count_delta = abs(_as_int(diff.get("count_delta")))

    threshold = None
    total_threshold = None
    if label.startswith("Preempt"):
        threshold = 3.0
        total_threshold = 60.0
    elif label.startswith(("Ped", "Ovlp Ped", "Overlap Ped")):
        threshold = 2.0
        total_threshold = 15.0
    elif label == "Transition":
        threshold = 1.0
        total_threshold = 10.0

    if threshold is None:
        return None

    if abs(avg_delta) >= threshold or abs(total_delta) >= total_threshold or count_delta >= 1:
        detail_label = f"{label} {state}".strip()
        if label.startswith(("Ovlp Ped ", "Overlap Ped ")):
            base_pattern = "Overlap Ped Service"
        elif label.startswith("Ped "):
            base_pattern = "Ped Service"
        else:
            base_pattern = detail_label
        direction = "\u2191" if avg_delta >= 0 else "\u2193"
        pattern = f"{base_pattern} {direction}"
        return {
            "type": "Operational drift",
            "pattern": pattern,
            "message": f"{detail_label} avg {avg_delta:+.1f}s, total {total_delta:+.1f}s",
            "avg_duration_delta": avg_delta,
            "total_duration_delta": total_delta,
            "count_delta": _as_int(diff.get("count_delta")),
        }
    return None


def _flag_overlap_signal_occurrence_difference(diff: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    label = str(diff.get("label", "")).strip()
    state = str(diff.get("state", "")).strip()
    event_class = str(diff.get("event_class", "")).strip()

    is_overlap = label.startswith(("Ovlp ", "Overlap ")) or event_class.startswith("Overlap ")
    if not is_overlap or state not in {"Yellow", "Red"}:
        return None

    count_delta = _as_int(diff.get("count_delta"))
    if count_delta == 0:
        return None

    pattern = f"Overlap {state} Occurrences"
    count_a = _as_int(diff.get("count_a"))
    count_b = _as_int(diff.get("count_b"))
    detail_label = f"{label} {state}".strip()
    return {
        "type": "Signal occurrence change",
        "pattern": pattern,
        "message": f"{detail_label} count {count_a}\u2192{count_b} ({count_delta:+d})",
        "avg_duration_delta": None,
        "count_delta": count_delta,
    }


def _include_phase_difference_detail(diff: Dict[str, Any]) -> bool:
    state = str(diff.get("state", "")).strip()
    if state not in {"Yellow", "Red"}:
        return True

    label = str(diff.get("label", "")).strip()
    event_class = str(diff.get("event_class", "")).strip()
    return label.startswith(("Ovlp ", "Overlap ")) or event_class.startswith("Overlap ")


def _build_similarity_trends(
  results: List[ScenarioResult],
  *,
  clearance_attr: str = "clearance_irregularities",
  phase_attr: str = "phase_differences",
  operational_attr: str = "operational_differences",
  include_scenario_flags: bool = False,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, List[str]]]:
  scenario_flags: Dict[str, List[str]] = {}
  clearance_trend_map: Dict[tuple, Dict[str, Any]] = defaultdict(
    lambda: {
      "type": "",
      "pattern": "",
      "devices": set(),
      "count_a_values": [],
      "count_b_values": [],
    }
  )
  operational_trend_map: Dict[tuple, Dict[str, Any]] = defaultdict(
    lambda: {
      "type": "",
      "pattern": "",
      "devices": set(),
      "avg_duration_deltas": [],
      "count_deltas": [],
    }
  )

  for row in results:
    if row.test_type != TestType.SIMILARITY:
      continue

    is_thrown_out, _ = _resolve_similarity_thrown_out(row)
    if is_thrown_out:
      continue

    flags: List[str] = []

    for diff in _normalize_clearance_rows(getattr(row, clearance_attr, [])):
      flagged = _flag_clearance_irregularity(diff)
      if not flagged:
        continue
      trend_key = (flagged["type"], flagged["pattern"])
      if include_scenario_flags:
        flags.append(f"{flagged['type']}: {flagged['message']}")
      trend = clearance_trend_map[trend_key]
      trend["type"] = flagged["type"]
      trend["pattern"] = flagged["pattern"]
      trend["devices"].add(row.scenario_id)
      trend["count_a_values"].append(flagged["count_a"])
      trend["count_b_values"].append(flagged["count_b"])

    for diff in _normalize_difference_rows(getattr(row, phase_attr, [])):
      flagged = _flag_overlap_signal_occurrence_difference(diff)
      if not flagged:
        continue
      trend_key = (flagged["type"], flagged["pattern"])
      if include_scenario_flags:
        flags.append(f"{flagged['type']}: {flagged['message']}")
      trend = operational_trend_map[trend_key]
      trend["type"] = flagged["type"]
      trend["pattern"] = flagged["pattern"]
      trend["devices"].add(row.scenario_id)
      if flagged["avg_duration_delta"] is not None:
        trend["avg_duration_deltas"].append(flagged["avg_duration_delta"])
      trend["count_deltas"].append(flagged["count_delta"])

    for diff in _normalize_difference_rows(getattr(row, operational_attr, [])):
      flagged = _flag_operational_difference(diff)
      if not flagged:
        continue
      trend_key = (flagged["type"], flagged["pattern"])
      if include_scenario_flags:
        flags.append(f"{flagged['type']}: {flagged['message']}")
      trend = operational_trend_map[trend_key]
      trend["type"] = flagged["type"]
      trend["pattern"] = flagged["pattern"]
      trend["devices"].add(row.scenario_id)
      trend["avg_duration_deltas"].append(flagged["avg_duration_delta"])
      trend["count_deltas"].append(flagged["count_delta"])

    if include_scenario_flags:
      deduped_flags: List[str] = []
      for flag in flags:
        if flag not in deduped_flags:
          deduped_flags.append(flag)
      scenario_flags[row.scenario_id] = deduped_flags[:4]
      hidden_flag_count = max(0, len(deduped_flags) - len(scenario_flags[row.scenario_id]))
      if hidden_flag_count:
        scenario_flags[row.scenario_id].append(
          f"{hidden_flag_count} additional trend flag(s) are listed in the detailed tables below."
        )

  clearance_trends: List[Dict[str, Any]] = []
  trends: List[Dict[str, Any]] = []

  for trend in clearance_trend_map.values():
    scenarios = sorted(trend["devices"])
    if not scenarios:
      continue

    baseline_count = sum(trend["count_a_values"])
    new_count = sum(trend["count_b_values"])
    clearance_trends.append(
      {
        "type": trend["type"],
        "pattern": trend["pattern"],
        "device_count": len(scenarios),
        "baseline_count": baseline_count,
        "new_count": new_count,
        "count_delta": new_count - baseline_count,
        "devices": ", ".join(scenarios),
        "row_class": _trend_row_class(trend["type"], trend["pattern"]),
      }
    )

  for trend in operational_trend_map.values():
    scenarios = sorted(trend["devices"])
    if not scenarios:
      continue

    trends.append(
      {
        "type": trend["type"],
        "pattern": trend["pattern"],
        "device_count": len(scenarios),
        "avg_duration_delta": _average(trend["avg_duration_deltas"]) if trend["avg_duration_deltas"] else None,
        "total_count_delta": sum(trend["count_deltas"]),
        "devices": ", ".join(scenarios),
        "row_class": _trend_row_class(trend["type"], trend["pattern"]),
      }
    )

  clearance_trends.sort(
    key=lambda item: (
      abs(item["count_delta"]),
      item["baseline_count"] + item["new_count"],
      item["device_count"],
    ),
    reverse=True,
  )

  trends.sort(
    key=lambda item: (
      abs(item["avg_duration_delta"] or 0.0),
      abs(item["total_count_delta"]),
      item["device_count"],
    ),
    reverse=True,
  )

  return clearance_trends[:20], trends[:20], scenario_flags


def _build_integrity_summary(
  results: List[ScenarioResult],
) -> List[Dict[str, Any]]:
  def _clearance_type_label(diff: Dict[str, Any]) -> str:
    event_class = str(diff.get("event_class", "")).strip()
    label = str(diff.get("label", "")).strip()
    state = str(diff.get("state", "")).strip()
    if event_class in {"Yellow", "Red"} or label.startswith("Ph "):
      return f"Phase {state} Clearance".strip()
    return f"Overlap {state} Clearance".strip()

  def _operational_type_label(diff: Dict[str, Any]) -> str:
    event_class = str(diff.get("event_class", "")).strip()
    label = str(diff.get("label", "")).strip()
    state = str(diff.get("state", "")).strip()
    if event_class in {"Transition Longway", "Transition Shortway"}:
      return event_class
    if label.startswith(("Ovlp Ped", "Overlap Ped")):
      return "Overlap Ped Service"
    if event_class == "Ped Service" or label.startswith("Ped "):
      return "Ped Service"
    if event_class == "Preempt" or label.startswith("Preempt"):
      return "Preempt"
    return f"{label} {state}".strip()

  type_map: Dict[str, Dict[str, Any]] = defaultdict(
    lambda: {"baseline_count": 0, "new_count": 0, "devices": set()}
  )
  for row in results:
    if row.test_type != TestType.SIMILARITY:
      continue
    for diff in _normalize_clearance_rows(getattr(row, "invalid_clearance_irregularities", [])):
      label = _clearance_type_label(diff)
      type_map[label]["baseline_count"] += _as_int(diff.get("irregular_count_a"))
      type_map[label]["new_count"] += _as_int(diff.get("irregular_count_b"))
      type_map[label]["devices"].add(row.scenario_id)
    for diff in _normalize_difference_rows(getattr(row, "invalid_operational_differences", [])):
      label = _operational_type_label(diff)
      type_map[label]["baseline_count"] += _as_int(diff.get("count_a"))
      type_map[label]["new_count"] += _as_int(diff.get("count_b"))
      type_map[label]["devices"].add(row.scenario_id)
  output: List[Dict[str, Any]] = []
  for type_label, data in type_map.items():
    if data["baseline_count"] > 0 or data["new_count"] > 0:
      output.append({
        "type": type_label,
        "baseline_count": data["baseline_count"],
        "new_count": data["new_count"],
        "count_delta": data["new_count"] - data["baseline_count"],
        "devices": ", ".join(sorted(data["devices"])),
      })
  output.sort(key=lambda r: max(r["baseline_count"], r["new_count"]), reverse=True)
  return output


_REPORT_TEMPLATE = Template("""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ suite.suite_name }} &mdash; Firmware Validation Report</title>
<style>
:root {
  --pass: #1b8a2e;
  --pass-bg: #e6f4ea;
  --fail: #c5221f;
  --fail-bg: #fce8e6;
  --warn: #e8710a;
  --warn-bg: #fef7e0;
  --border: #dadce0;
  --bg: #f8f9fa;
  --card: #ffffff;
  --text: #1f1f1f;
  --muted: #5f6368;
  --link: #1a73e8;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); line-height: 1.5; font-size: 16px; }

/* Header */
.header { background: linear-gradient(135deg, #1a237e 0%, #283593 100%); color: white; padding: 32px 40px; }
.header h1 { font-size: 32px; font-weight: 700; margin-bottom: 4px; }
.header .subtitle { opacity: 0.9; font-size: 17px; }
.header .generated { opacity: 0.72; font-size: 14px; margin-top: 8px; }

/* Layout */
.container { max-width: 1600px; margin: 0 auto; padding: 24px 40px 64px; }

/* Cards */
.card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; margin-bottom: 20px; overflow: hidden; }
.card-header { padding: 16px 20px; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 12px; }
.card-header h2 { font-size: 22px; font-weight: 700; }
.card-body { padding: 20px; }

/* Summary tiles */
.summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
.tile { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; text-align: center; }
.tile .value { font-size: 40px; font-weight: 700; }
.tile .label { font-size: 15px; color: var(--muted); margin-top: 4px; }
.tile.pass .value { color: var(--pass); }
.tile.fail .value { color: var(--fail); }
.tile.neutral .value { color: var(--text); }

/* Tables */
table { width: 100%; border-collapse: collapse; font-size: 15px; }
th { background: var(--bg); padding: 12px 14px; text-align: left; font-weight: 700; border-bottom: 2px solid var(--border); white-space: nowrap; font-size: 15px; }
td { padding: 12px 14px; border-bottom: 1px solid var(--border); vertical-align: middle; }
tr:hover td { background: #f1f3f4; }

/* Badge */
.badge { display: inline-block; padding: 4px 11px; border-radius: 12px; font-size: 13px; font-weight: 700; letter-spacing: 0.3px; }
.badge.pass { background: var(--pass); color: white; }
.badge.fail { background: var(--fail); color: white; }
.badge.warn { background: var(--warn); color: white; }
.badge.info { background: #e8eaed; color: var(--muted); }

/* Scenario detail */
.scenario { margin-bottom: 32px; padding-bottom: 24px; border-bottom: 2px solid var(--border); }
.scenario:last-child { border-bottom: none; }
.scenario-header { display: flex; align-items: center; gap: 12px; margin-bottom: 12px; }
.scenario-header h3 { font-size: 24px; font-weight: 700; }
.meta-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 8px 24px; margin-bottom: 16px; font-size: 16px; }
.meta-grid .label { color: var(--muted); }
.meta-grid .val { font-weight: 600; }

/* Phase diff table */
.phase-table { width: 100%; border-collapse: collapse; font-size: 14px; font-family: 'Cascadia Code', 'Consolas', monospace; margin: 8px 0; }
.phase-table th { background: var(--bg); padding: 8px 10px; text-align: left; font-weight: 700; border-bottom: 2px solid var(--border); font-size: 13px; }
.phase-table td { padding: 7px 10px; border-bottom: 1px solid var(--border); }
.phase-table tr:hover td { background: #f1f3f4; }
.phase-table .num { text-align: right; font-variant-numeric: tabular-nums; }
.phase-table .pos { color: var(--pass); }
.phase-table .neg { color: var(--fail); }
.phase-table .warning-yellow { background-color: #fff4e5; font-weight: bold; }

/* Notes */
.notes { background: #f8f9fa; border-left: 3px solid var(--border); padding: 12px 16px; font-family: 'Cascadia Code', 'Consolas', monospace; font-size: 14px; white-space: pre-wrap; word-break: break-word; margin: 12px 0; border-radius: 0 6px 6px 0; max-height: 300px; overflow-y: auto; }
.notes.error { border-left-color: var(--fail); background: var(--fail-bg); }

/* Images */
.plot-gallery { display: flex; flex-direction: column; gap: 16px; margin-top: 12px; }
.plot-gallery img { max-width: 100%; border: 1px solid var(--border); border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
.plot-caption { font-size: 14px; color: var(--muted); margin-top: 2px; font-weight: 600; }

/* Footer */
.footer { text-align: center; padding: 24px; font-size: 14px; color: var(--muted); border-top: 1px solid var(--border); margin-top: 32px; }

/* Anchor offset for fixed-ish header */
[id] { scroll-margin-top: 20px; }
a { color: var(--link); text-decoration: none; }
a:hover { text-decoration: underline; }

/* Config section */
.config-table td:first-child { font-weight: 500; white-space: nowrap; width: 260px; }

/* Sparkline */
.sparkline-wrap {
  display: block;
  width: 100%;
  padding: 14px 16px 12px;
  background: #f8f9fa;
  border: 1px solid var(--border);
  border-radius: 8px;
  overflow-x: auto;
}
.sparkline-wrap svg {
  display: block;
  width: 100%;
  height: auto;
  min-height: 190px;
}
.chart-note { color: var(--muted); font-size: 14px; margin-top: 8px; }
.chart-note strong { color: var(--text); }
.section-copy { color: var(--muted); font-size: 15px; margin-bottom: 14px; max-width: 1050px; }
.summary-note-cell { white-space: normal; }
.trends-table tbody tr:nth-child(odd) td { background: #fbfcff; }
.trends-table .trend-chip { display: inline-block; padding: 4px 10px; border-radius: 999px; font-weight: 700; font-size: 13px; letter-spacing: 0.2px; }
.trends-table .trend-row td:first-child { border-left: 4px solid transparent; }
.trends-table .trend-row.trend-yellow td:first-child { border-left-color: #f9ab00; }
.trends-table .trend-row.trend-yellow .trend-chip { background: #fff4ce; color: #8a5a00; }
.trends-table .trend-row.trend-red td:first-child { border-left-color: #c5221f; }
.trends-table .trend-row.trend-red .trend-chip { background: #fde7e9; color: #8a1c1a; }
.trends-table .trend-row.trend-preempt td:first-child { border-left-color: #0b57d0; }
.trends-table .trend-row.trend-preempt .trend-chip { background: #e8f0fe; color: #0b57d0; }
.trends-table .trend-row.trend-ped td:first-child { border-left-color: #188038; }
.trends-table .trend-row.trend-ped .trend-chip { background: #e6f4ea; color: #188038; }
.trends-table .trend-row.trend-overlap-ped td:first-child { border-left-color: #137333; }
.trends-table .trend-row.trend-overlap-ped .trend-chip { background: #ddf2e3; color: #137333; }
.trends-table .trend-row.trend-transition td:first-child { border-left-color: #b06000; }
.trends-table .trend-row.trend-transition .trend-chip { background: #fef0c7; color: #9a6700; }
.trends-table .trend-row.trend-operational td:first-child { border-left-color: #5f6368; }
.trends-table .trend-row.trend-operational .trend-chip { background: #eceff1; color: #3c4043; }
.trends-table .trend-delta { font-weight: 700; border-radius: 6px; }
.trends-table .trend-delta.pos { color: var(--pass); background: rgba(27, 138, 46, 0.10); }
.trends-table .trend-delta.neg { color: var(--fail); background: rgba(197, 34, 31, 0.10); }
</style>
</head>
<body>

<div class="header">
  <h1>{{ suite.suite_name }}</h1>
  <div class="subtitle">Baseline <strong>{{ suite.baseline_version }}</strong> &rarr; New firmware <strong>{{ suite.firmware_version }}</strong></div>
  <div class="generated">Generated {{ generated_at }}</div>
</div>

<div class="container">

<div class="section-copy" style="max-width:none; margin:0 0 16px; padding:10px 14px; background:#f8f9fa; border-left:4px solid #80868b; border-radius:4px; font-size:13px;">
  <strong>Chart note:</strong> bars drawn with a diamond hatch and dimmed fill represent intervals flagged invalid (missing or unreliable raw data) rather than blank gaps. They are shown for context but are excluded from similarity scoring and difference statistics; comparison windows that overlap invalid data are not flagged as divergences.
</div>

<!-- ===== Summary tiles ===== -->
<div class="summary-grid">
  <div class="tile {{ 'pass' if total_count > 0 and total_pass == total_count else ('neutral' if total_count == 0 else 'fail') }}">
    <div class="value">{{ total_pass }}/{{ total_count }}</div>
    <div class="label">Scenarios Passed</div>
  </div>
  <div class="tile {{ avg_match_class }}">
    <div class="value">{{ avg_match_display }}</div>
    <div class="label">Avg Sequence Match</div>
  </div>
  <div class="tile {{ avg_timing_match_class }}">
    <div class="value">{{ avg_timing_match_display }}</div>
    <div class="label">Avg Timing Match</div>
  </div>
</div>

<!-- ===== Results table ===== -->
{% if summary_similarity_rows|length > 0 %}
<div class="card">
  <div class="card-header"><h2>Similarity Results</h2></div>
  <div class="card-body" style="padding:0;">
    <table>
      <thead>
        <tr><th>Scenario</th><th>Sequence Match</th><th>Timing Match<br><span style="font-size:12px;font-weight:500;color:var(--muted);">(events within 0.5s)</span></th><th>Status</th></tr>
      </thead>
      <tbody>
      {% for r in summary_similarity_rows %}
        <tr>
          <td><a href="#{{ r.scenario_id }}">{{ r.scenario_id }}</a></td>
          <td style="font-weight:600;{% if r.thrown_out %} color:var(--muted);{% elif r.match_percentage is not none and r.match_percentage >= 95 %} color:var(--pass);{% elif r.match_percentage is not none and r.match_percentage >= 80 %} color:var(--warn);{% elif r.match_percentage is not none %} color:var(--fail);{% endif %}">
            {{ '&mdash;' if r.thrown_out else ('%.1f%%'|format(r.match_percentage) if r.match_percentage is not none else '&mdash;') }}
          </td>
          <td style="font-weight:600;{% if r.thrown_out or r.timing_match_percentage is none %} color:var(--muted);{% elif r.timing_match_percentage >= 90 %} color:var(--pass);{% elif r.timing_match_percentage >= 80 %} color:var(--warn);{% else %} color:var(--fail);{% endif %}">
            {{ '&mdash;' if r.thrown_out else ('%.1f%%'|format(r.timing_match_percentage) if r.timing_match_percentage is not none else '&mdash;') }}
          </td>
          <td><span class="badge {{ 'warn' if r.thrown_out else ('pass' if r.passed else 'fail') }}">{{ 'THROWN OUT' if r.thrown_out else ('PASS' if r.passed else 'FAIL') }}</span></td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
</div>
{% endif %}

{% if conflict|length > 0 %}
<div class="card">
  <div class="card-header"><h2>Conflict Results</h2></div>
  <div class="card-body" style="padding:0;">
    <table>
      <thead>
        <tr><th>Scenario</th><th>Runs</th><th>New Conflicts</th><th>Status</th></tr>
      </thead>
      <tbody>
      {% for r in conflict %}
        <tr>
          <td><a href="#{{ r.scenario_id }}">{{ r.scenario_id }}</a></td>
          <td>{{ r.runs_completed }} / {{ r.total_runs }}</td>
          <td>{{ r.conflicts_found|length }}</td>
          <td><span class="badge {{ 'pass' if r.passed else 'fail' }}">{{ 'PASS' if r.passed else 'FAIL' }}</span></td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
</div>
{% endif %}

  {% if clearance_trends or device_trends or integrity_rows %}
  <div class="card">
    <div class="card-header"><h2>Device Trends</h2></div>
    <div class="card-body" style="padding:0;">
      {% if clearance_trends or device_trends %}
      <div style="padding:16px 20px 8px; font-weight:600;">Clearance Interval Checks (Valid Timeline Events)</div>
      {% if clearance_trends %}
      <div class="section-copy" style="padding:0 20px 12px; margin:0; max-width:none;">Total clearance events whose duration differs from that movement's own median by at least 0.1s. Overlap yellow/red rows are included here only when their median is at most 6.0s and at least 95% of samples are within 2.0s of that median.</div>
      <table class="trends-table">
        <thead>
          <tr><th>Type</th><th>{{ suite.baseline_version }}<br>Irregular Count</th><th>{{ suite.firmware_version }}<br>Irregular Count</th><th>&#916; Count</th><th>Devices</th></tr>
        </thead>
        <tbody>
        {% for trend in clearance_trends %}
          <tr class="trend-row {{ trend.row_class }}">
            <td><span class="trend-chip">{{ trend.pattern }}</span></td>
            <td class="num">{{ trend.baseline_count }}</td>
            <td class="num">{{ trend.new_count }}</td>
            <td class="num trend-delta {{ 'pos' if trend.count_delta > 0 else ('neg' if trend.count_delta < 0 else '') }}">{{ '%+d'|format(trend.count_delta) if trend.count_delta != 0 else '&mdash;' }}</td>
            <td>{{ trend.devices }}</td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
      {% endif %}

      {% if device_trends %}
      <div style="padding:16px 20px 8px; font-weight:600;">Signal / Operational Trends</div>
      <table class="trends-table">
        <thead>
          <tr><th>Type</th><th>Avg<br>Delta (s)</th><th>Total Count<br>Delta</th><th>Devices</th></tr>
        </thead>
        <tbody>
        {% for trend in device_trends %}
          <tr class="trend-row {{ trend.row_class }}">
            <td><span class="trend-chip">{{ trend.pattern }}</span></td>
            <td class="num trend-delta {{ 'pos' if trend.avg_duration_delta is not none and trend.avg_duration_delta > 0 else ('neg' if trend.avg_duration_delta is not none and trend.avg_duration_delta < 0 else '') }}">{{ '%+.1f'|format(trend.avg_duration_delta) if trend.avg_duration_delta is not none else '&mdash;' }}</td>
            <td class="num trend-delta {{ 'pos' if trend.total_count_delta > 0 else ('neg' if trend.total_count_delta < 0 else '') }}">{{ '%+d'|format(trend.total_count_delta) if trend.total_count_delta != 0 else '&mdash;' }}</td>
            <td>{{ trend.devices }}</td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
      {% endif %}
      {% endif %}

      {% if integrity_rows %}
      <div style="padding:16px 20px 8px; font-weight:600;">Data Integrity</div>
      <div class="section-copy" style="padding:0 20px 12px; margin:0; max-width:none;">Total count of timeline rows where ATSPM marked the event as invalid (<code>IsValid = False</code>), grouped by main event type instead of movement number. The detailed device breakdowns and flagged charts below use only valid timeline rows.</div>
      <table class="trends-table">
        <thead>
          <tr><th>Type</th><th>{{ suite.baseline_version }}<br>Invalid Count</th><th>{{ suite.firmware_version }}<br>Invalid Count</th><th>&#916; Count</th><th>Devices</th></tr>
        </thead>
        <tbody>
        {% for row in integrity_rows %}
          <tr>
            <td>{{ row.type }}</td>
            <td class="num">{{ row.baseline_count }}</td>
            <td class="num">{{ row.new_count }}</td>
            <td class="num trend-delta {{ 'pos' if row.count_delta > 0 else ('neg' if row.count_delta < 0 else '') }}">{{ '%+d'|format(row.count_delta) if row.count_delta != 0 else '&mdash;' }}</td>
            <td>{{ row.devices }}</td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
      {% endif %}
    </div>
  </div>
  {% endif %}

{% if combined_phase_call_timeline_svg %}
<div class="card">
  <div class="card-header"><h2>Combined Timeline</h2></div>
  <div class="card-body">
    <div class="section-copy">
      Black points and the connecting line show the average vehicular phase-call input similarity across all similarity devices. This is a system-wide sanity check for network or compute issues that may have affected every device at once, independent of firmware version.
    </div>
    <div class="sparkline-wrap">{{ combined_phase_call_timeline_svg }}</div>
  </div>
</div>
{% endif %}

<!-- ===== Detailed results ===== -->
<div class="card">
  <div class="card-header"><h2>Detailed Results</h2></div>
  <div class="card-body">
  {% for row in detail_rows %}
    <div class="scenario" id="{{ row.scenario_id }}">
      <div class="scenario-header">
        <h3>{{ row.scenario_id }}</h3>
        <span class="badge {{ 'warn' if row.thrown_out else ('pass' if row.passed else 'fail') }}">{{ 'THROWN OUT' if row.thrown_out else ('PASS' if row.passed else 'FAIL') }}</span>
        <span class="badge info">{{ row.test_type }}</span>
      </div>
      {% if row.notes_column %}
        <div style="font-weight:600; margin-bottom:12px; font-size:15px; border-left:4px solid #1a237e; padding-left:12px; color:#1a237e;">{{ row.notes_column }}</div>
      {% endif %}

      <div class="meta-grid">
        {% if row.thrown_out %}
        <div><span class="label">Sequence Match:</span> <span class="val" style="color:var(--warn)">Thrown out</span></div>
        <div><span class="label">Reason:</span> <span class="val">{{ row.thrown_out_reason or 'All scored chunks fell below the input similarity threshold' }}</span></div>
        {% elif row.match_percentage is not none %}
        <div><span class="label">Sequence Match:</span> <span class="val" style="color:{% if row.match_percentage >= 95 %}var(--pass){% elif row.match_percentage >= 80 %}var(--warn){% else %}var(--fail){% endif %}">{{ '%.1f%%'|format(row.match_percentage) }}</span></div>
        {% endif %}
        {% if row.test_type == 'similarity' and not row.thrown_out %}
        <div><span class="label">Timing Match:</span> <span class="val" style="color:{% if row.timing_match_percentage is none %}var(--muted){% elif row.timing_match_percentage >= 90 %}var(--pass){% elif row.timing_match_percentage >= 80 %}var(--warn){% else %}var(--fail){% endif %}">{{ '%.1f%%'|format(row.timing_match_percentage) if row.timing_match_percentage is not none else '&mdash;' }}</span></div>
        {% if row.timing_p95_error_seconds is not none %}
        <div><span class="label">Timing p95 error:</span> <span class="val">{{ '%.3fs'|format(row.timing_p95_error_seconds) }}</span></div>
        {% endif %}
        {% if row.timing_max_error_seconds is not none %}
        <div><span class="label">Timing max error:</span> <span class="val">{{ '%.3fs'|format(row.timing_max_error_seconds) }}</span></div>
        {% endif %}
        {% endif %}
        <div><span class="label">Runs:</span> <span class="val">{{ row.runs_completed }} / {{ row.total_runs }}</span></div>
        {% if row.test_type == 'similarity' and row.sparkline_svg %}
        <div><span class="label">Included chunks:</span> <span class="val">{{ row.included_chunk_count }}</span></div>
        <div><span class="label">Excluded chunks:</span> <span class="val">{{ row.excluded_chunk_count }}</span></div>
        {% endif %}
        {% if row.annotation %}<div><span class="label">Note:</span> <span class="val">{{ row.annotation }}</span></div>{% endif %}
      </div>

      {% if row.scenario_flags %}
        <div style="font-weight:600;margin:10px 0 6px;">Special Notes</div>
        <div class="notes" style="border-left-color: var(--warn); background: var(--warn-bg);">{{ row.scenario_flags|join('\n') }}</div>
      {% endif %}

      {% if row.sparkline_svg %}
        <div style="margin:12px 0;">
          <div style="font-weight:600;margin-bottom:6px;">Match Timeline</div>
          <div class="sparkline-wrap">{{ row.sparkline_svg }}</div>
          {% if row.test_type == 'similarity' %}
          <div class="chart-note"><strong>Match bars</strong> show the firmware comparison score by chunk. <strong>Black points/line</strong> show vehicular phase-call input similarity, which is treated as a simulation-reliability sanity check independent of firmware version. Semi-transparent bars were excluded from the device-level match average because input similarity fell below {{ phase_call_similarity_threshold }}%.</div>
          {% endif %}
        </div>
      {% endif %}

      {% if row.error %}
        <div class="notes error">{{ row.error }}</div>
      {% endif %}
      {% if row.conflicts_found %}
        <details open>
          <summary style="cursor:pointer;font-weight:600;margin-bottom:6px;">Conflicts ({{ row.conflicts_found|length }})</summary>
          <div class="notes error">{{ row.conflicts_found }}</div>
        </details>
      {% endif %}

      {% if row.test_type == 'similarity' %}
        <details{% if not row.passed %} open{% endif %}>
          <summary style="cursor:pointer;font-weight:600;margin-bottom:6px;">Phase / Overlap Differences</summary>
          {% if row.timeline_difference_analysis_available and row.clearance_irregularities %}
          <div style="font-weight:600;margin:10px 0 6px;">Clearance Irregularities</div>
          <div class="chart-note">Counts clearance events at least 0.1s away from that movement's median within each source run. Avg Dev is the signed average deviation of those irregular events from the median.</div>
          <table class="phase-table">
            <thead>
              <tr>
                <th>Movement</th>
                <th class="num">Median Orig (s)</th>
                <th class="num">Median New (s)</th>
                <th class="num">Irregular Orig</th>
                <th class="num">Irregular New</th>
                <th class="num">&Delta; Count</th>
                <th class="num">Avg Dev Orig (s)</th>
                <th class="num">Avg Dev New (s)</th>
              </tr>
            </thead>
            <tbody>
            {% for d in row.clearance_irregularities %}
              <tr>
                <td>{{ d.label }} {{ d.state }}</td>
                <td class="num">{{ '%.1f'|format(d.median_a) }}</td>
                <td class="num">{{ '%.1f'|format(d.median_b) }}</td>
                <td class="num">{{ d.irregular_count_a }}</td>
                <td class="num {{ 'pos' if d.irregular_count_b > d.irregular_count_a else ('neg' if d.irregular_count_b < d.irregular_count_a else '') }}">{{ d.irregular_count_b }}</td>
                <td class="num trend-delta {{ 'pos' if d.irregular_count_delta > 0 else ('neg' if d.irregular_count_delta < 0 else '') }}">{{ '%+d'|format(d.irregular_count_delta) if d.irregular_count_delta != 0 else '&mdash;' }}</td>
                <td class="num">{{ '%+.2f'|format(d.avg_deviation_a) if d.irregular_count_a else '&mdash;' }}</td>
                <td class="num">{{ '%+.2f'|format(d.avg_deviation_b) if d.irregular_count_b else '&mdash;' }}</td>
              </tr>
            {% endfor %}
            </tbody>
          </table>
          {% endif %}
          {% if row.timeline_difference_analysis_available and row.phase_differences %}
          <div style="font-weight:600;margin:10px 0 6px;">Other Signal Timing Differences{% if row.phase_differences %} (top {{ [row.phase_differences|length, 5]|min }} of {{ row.phase_differences|length }}){% endif %}</div>
          <table class="phase-table">
            <thead>
              <tr>
                <th>Phase / Overlap</th>
                <th>State</th>
                <th class="num">Count (Orig)</th>
                <th class="num">Count (New)</th>
                <th class="num">&Delta; Count</th>
                <th class="num">Avg Dur Orig (s)</th>
                <th class="num">Avg Dur New (s)</th>
                <th class="num">Total Dur Orig (s)</th>
                <th class="num">Total Dur New (s)</th>
                <th class="num">&Delta; Avg Dur (s)</th>
                <th class="num">&Delta; Total Dur (s)</th>
              </tr>
            </thead>
            <tbody>
            {% for d in row.phase_differences[:5] %}
              <tr>
                <td>{{ d.label }}</td>
                <td>{{ d.state }}</td>
                <td class="num{% if d.count_a == 0 %} warning-yellow{% endif %}">{{ d.count_a }}</td>
                <td class="num{% if d.count_b == 0 %} warning-yellow{% endif %}">{{ d.count_b }}</td>
                <td class="num {{ 'pos' if d.count_delta > 0 else ('neg' if d.count_delta < 0 else '') }}">{{ '%+d'|format(d.count_delta) if d.count_delta != 0 else '&mdash;' }}</td>
                <td class="num">{{ '%.1f'|format(d.duration_a) }}</td>
                <td class="num">{{ '%.1f'|format(d.duration_b) }}</td>
                <td class="num">{{ '%.1f'|format(d.total_duration_a) }}</td>
                <td class="num">{{ '%.1f'|format(d.total_duration_b) }}</td>
                <td class="num {{ 'pos' if d.duration_delta > 0 else ('neg' if d.duration_delta < 0 else '') }}">{{ '%+.1f'|format(d.duration_delta) }}</td>
                <td class="num {{ 'pos' if d.total_duration_delta > 0 else ('neg' if d.total_duration_delta < 0 else '') }}">{{ '%+.1f'|format(d.total_duration_delta) }}</td>
              </tr>
            {% endfor %}
            </tbody>
          </table>
          {% elif row.timeline_difference_analysis_available and not row.clearance_irregularities %}
          <div class="chart-note">No meaningful phase or overlap differences were found.</div>
          {% else %}
          <div class="chart-note">Phase / overlap breakdown unavailable because detailed timeline analysis was not available for this scenario.</div>
          {% endif %}
        </details>
      {% endif %}

      {% if row.test_type == 'similarity' %}
        <details{% if not row.passed %} open{% endif %}>
          <summary style="cursor:pointer;font-weight:600;margin-bottom:6px;">Transition / Preempt / Ped Service Differences{% if row.operational_differences %} (top {{ [row.operational_differences|length, 5]|min }} of {{ row.operational_differences|length }}){% endif %}</summary>
          {% if row.timeline_difference_analysis_available and row.operational_differences %}
          <table class="phase-table">
            <thead>
              <tr>
                <th>Category</th>
                <th>State</th>
                <th class="num">Count (Orig)</th>
                <th class="num">Count (New)</th>
                <th class="num">&Delta; Count</th>
                <th class="num">Avg Dur Orig (s)</th>
                <th class="num">Avg Dur New (s)</th>
                <th class="num">Total Dur Orig (s)</th>
                <th class="num">Total Dur New (s)</th>
                <th class="num">&Delta; Avg Dur (s)</th>
                <th class="num">&Delta; Total Dur (s)</th>
              </tr>
            </thead>
            <tbody>
            {% for d in row.operational_differences[:5] %}
              <tr>
                <td>{{ d.label }}</td>
                <td>{{ d.state }}</td>
                <td class="num{% if d.count_a == 0 %} warning-yellow{% endif %}">{{ d.count_a }}</td>
                <td class="num{% if d.count_b == 0 %} warning-yellow{% endif %}">{{ d.count_b }}</td>
                <td class="num {{ 'pos' if d.count_delta > 0 else ('neg' if d.count_delta < 0 else '') }}">{{ '%+d'|format(d.count_delta) if d.count_delta != 0 else '&mdash;' }}</td>
                <td class="num">{{ '%.1f'|format(d.duration_a) }}</td>
                <td class="num">{{ '%.1f'|format(d.duration_b) }}</td>
                <td class="num">{{ '%.1f'|format(d.total_duration_a) }}</td>
                <td class="num">{{ '%.1f'|format(d.total_duration_b) }}</td>
                <td class="num {{ 'pos' if d.duration_delta > 0 else ('neg' if d.duration_delta < 0 else '') }}">{{ '%+.1f'|format(d.duration_delta) }}</td>
                <td class="num {{ 'pos' if d.total_duration_delta > 0 else ('neg' if d.total_duration_delta < 0 else '') }}">{{ '%+.1f'|format(d.total_duration_delta) }}</td>
              </tr>
            {% endfor %}
            </tbody>
          </table>
          {% elif row.timeline_difference_analysis_available %}
          <div class="chart-note">No meaningful transition, preempt, or pedestrian-service differences were found.</div>
          {% else %}
          <div class="chart-note">Transition, preempt, and pedestrian-service breakdown unavailable because detailed timeline analysis was not available for this scenario.</div>
          {% endif %}
        </details>
      {% endif %}

      {% if row.images %}
        <div class="plot-gallery">
        {% for img in row.images %}
          <div>
            <div class="plot-caption">{{ img.caption }}</div>
            <img src="data:image/png;base64,{{ img.data }}" alt="{{ row.scenario_id }} {{ img.caption }}" loading="lazy" />
          </div>
        {% endfor %}
        </div>
      {% else %}
        {% if row.test_type == 'similarity' and row.passed %}
          <div style="color:var(--pass);font-size:14px;margin-top:8px;">&#10003; Sequences matched within thresholds &mdash; no divergence charts needed.</div>
        {% endif %}
      {% endif %}
    </div>
  {% endfor %}
  </div>
</div>

<!-- ===== Configuration ===== -->
<div class="card">
  <div class="card-header"><h2>Configuration</h2></div>
  <div class="card-body" style="padding:0;">
    <table class="config-table">
      <thead><tr><th>Setting</th><th>Value</th><th>Description</th></tr></thead>
      <tbody>
      <tr><td>Suite name</td><td>{{ suite.suite_name }}</td><td>Name of this validation test suite</td></tr>
      <tr><td>Firmware version</td><td>{{ suite.firmware_version }}</td><td>New firmware version being validated</td></tr>
      <tr><td>Baseline version</td><td>{{ suite.baseline_version }}</td><td>Reference baseline used for comparison</td></tr>
      <tr><td>Collection interval</td><td>{{ suite.collection_interval_minutes }} minutes</td><td>Duration of event log collection per scenario</td></tr>
      <tr><td>Post-replay settle</td><td>{{ suite.post_replay_settle_seconds }} seconds</td><td>Wait time after log replay before collecting events</td></tr>
      <tr><td>Analysis settle window</td><td>{{ suite.analysis_settle_minutes }} minutes</td><td>Initial replay period excluded from reported similarity analysis</td></tr>
      {% if suite.analysis_start_time %}
      <tr><td>Manual analysis start time</td><td>{{ suite.analysis_start_time }}</td><td>Clock time used to clip TOD scenarios before similarity analysis</td></tr>
      {% endif %}
      {% if suite.analysis_end_time %}
      <tr><td>Manual analysis end time</td><td>{{ suite.analysis_end_time }}</td><td>Clock time used to stop TOD scenario analysis, using the run end date for overnight windows</td></tr>
      {% endif %}
      {% if suite.comparison_thresholds %}
      <tr><td>Sequence match threshold</td><td>{{ suite.comparison_thresholds.match_threshold }}%</td><td>Minimum event-sequence match percentage to pass a scenario</td></tr>
      {% endif %}
      <tr><td>Timing match threshold</td><td>90.0% within 0.5s</td><td>Minimum percentage of aligned matching event groups whose timing error is within 0.5 seconds</td></tr>
      <tr><td>Phase-call similarity threshold</td><td>{{ phase_call_similarity_threshold }}%</td><td>Vehicular phase-call input similarity below this value excludes a chunk from the device-level match average while leaving the chunk bar visible as a reliability sanity check</td></tr>
      </tbody>
    </table>
  </div>
</div>

</div><!-- container -->

<div class="footer">
  Firmware Validation Report &mdash; signal_replay v{{ version }} &mdash; {{ generated_at }}
</div>
</body>
</html>
""")


def generate_report(
    results: List[ScenarioResult],
    suite: FirmwareTestSuite,
    output_path: str,
    annotations: Optional[Dict[str, str]] = None,
) -> str:
    """Generate a self-contained HTML firmware validation report.

    The report embeds all plot images as base64 so it is portable as a single file.
    """
    annotations = annotations or {}

    similarity = [r for r in results if r.test_type == TestType.SIMILARITY]
    conflict = [r for r in results if r.test_type == TestType.CONFLICT]

    summary_similarity_rows: List[Dict[str, Any]] = []
    scored_similarity: List[ScenarioResult] = []
    similarity_status: Dict[str, Tuple[bool, str]] = {}
    for row in similarity:
      is_thrown_out, thrown_out_reason = _resolve_similarity_thrown_out(row)
      similarity_status[row.scenario_id] = (is_thrown_out, thrown_out_reason)
      summary_similarity_rows.append(
        {
          "scenario_id": row.scenario_id,
          "notes": row.notes,
          "match_percentage": row.match_percentage,
          "timing_match_percentage": getattr(row, "timing_match_percentage", None),
          "num_divergences": row.num_divergences,
          "passed": row.passed,
          "thrown_out": is_thrown_out,
        }
      )
      if not is_thrown_out:
        scored_similarity.append(row)

    similarity_pass = sum(1 for r in scored_similarity if r.passed)
    similarity_fail = len(scored_similarity) - similarity_pass

    conflict_pass = sum(1 for r in conflict if r.passed)
    conflict_fail = len(conflict) - conflict_pass

    total_pass = similarity_pass + conflict_pass
    total_count = len(scored_similarity) + len(conflict)

    match_values = [r.match_percentage for r in scored_similarity if r.match_percentage is not None]
    avg_match = sum(match_values) / len(match_values) if match_values else None
    if avg_match is None:
      avg_match_display = "&mdash;"
      avg_match_class = "neutral"
    else:
      avg_match_display = f"{avg_match:.1f}%"
      avg_match_class = "pass" if avg_match >= 95 else ("neutral" if avg_match >= 80 else "fail")
    timing_match_values = [
      getattr(r, "timing_match_percentage", None)
      for r in scored_similarity
      if r.match_percentage is not None
      and r.match_percentage >= 95.0
      and getattr(r, "timing_match_percentage", None) is not None
    ]
    avg_timing_match = (
      sum(timing_match_values) / len(timing_match_values)
      if timing_match_values
      else None
    )
    if avg_timing_match is None:
      avg_timing_match_display = "&mdash;"
      avg_timing_match_class = "neutral"
    else:
      avg_timing_match_display = f"{avg_timing_match:.1f}%"
      avg_timing_match_class = "pass" if avg_timing_match >= 90 else ("neutral" if avg_timing_match >= 80 else "fail")
    phase_call_similarity_threshold = float(getattr(suite, "phase_call_similarity_threshold", getattr(suite, "detector_similarity_threshold", 90.0)))
    combined_phase_call_timeline_svg = _build_combined_phase_call_timeline(results, phase_call_similarity_threshold)

    # Keep input order for both summary table and detail section
    # (caller is expected to pre-sort by scenario name or suite order)
    sorted_similarity = list(similarity)
    clearance_trends, device_trends, scenario_flags = _build_similarity_trends(
      sorted_similarity,
      include_scenario_flags=True,
    )
    integrity_rows = _build_integrity_summary(sorted_similarity)

    detail_rows = []
    for row in results:
        encoded_images = []
        plot_captions = list(getattr(row, 'plot_captions', []))
        for index, image_path in enumerate(row.plot_paths):
            img_data = _image_to_base64(image_path)
            if img_data:
                caption = plot_captions[index] if index < len(plot_captions) and plot_captions[index] else Path(image_path).stem.replace("_", " ")
                encoded_images.append({"caption": caption, "data": img_data})

        is_thrown_out, thrown_out_reason = similarity_status.get(
            row.scenario_id,
            (getattr(row, "thrown_out", False), getattr(row, "thrown_out_reason", "")),
        )

        detail_rows.append(
            {
                "scenario_id": row.scenario_id,
                "test_type": row.test_type.value,
                "passed": row.passed,
                "match_percentage": row.match_percentage,
                "timing_match_percentage": getattr(row, "timing_match_percentage", None),
                "timing_p95_error_seconds": getattr(row, "timing_p95_error_seconds", None),
                "timing_max_error_seconds": getattr(row, "timing_max_error_seconds", None),
                "thrown_out": is_thrown_out,
                "thrown_out_reason": thrown_out_reason,
                "num_divergences": row.num_divergences,
                "runs_completed": row.runs_completed,
                "total_runs": row.total_runs,
                "notes": row.notes,
                "notes_column": getattr(row, 'notes_column', ''),
                "error": row.error,
                "conflicts_found": row.conflicts_found,
                "annotation": annotations.get(row.scenario_id, ""),
                "images": encoded_images,
                "phase_differences": [
                  diff
                  for diff in _normalize_difference_rows(getattr(row, 'phase_differences', []))
                  if _include_phase_difference_detail(diff)
                ],
                "clearance_irregularities": _normalize_clearance_rows(getattr(row, 'clearance_irregularities', [])),
                "operational_differences": _normalize_difference_rows(getattr(row, 'operational_differences', [])),
                "sparkline_svg": getattr(row, 'sparkline_svg', ''),
                "phase_call_chunk_scores": getattr(row, 'phase_call_chunk_scores', getattr(row, 'detector_chunk_scores', [])),
                "included_chunk_count": getattr(row, 'included_chunk_count', 0),
                "excluded_chunk_count": getattr(row, 'excluded_chunk_count', 0),
                "analysis_diagnostics": getattr(row, 'analysis_diagnostics', []),
                "scenario_flags": scenario_flags.get(row.scenario_id, []),
                "timeline_difference_analysis_available": getattr(row, 'timeline_difference_analysis_available', False),
                "temporal_shift_seconds": getattr(row, 'temporal_shift_seconds', 0.0),
            }
        )

    try:
        from . import __version__ as version
    except Exception:
        version = "?"

    html = _REPORT_TEMPLATE.render(
        suite=suite,
        similarity=similarity,
        conflict=conflict,
        similarity_pass=similarity_pass,
        similarity_fail=similarity_fail,
        similarity_scored_count=len(scored_similarity),
        conflict_pass=conflict_pass,
        conflict_fail=conflict_fail,
        total_pass=total_pass,
        total_count=total_count,
        avg_match_display=avg_match_display,
        avg_match_class=avg_match_class,
        avg_timing_match_display=avg_timing_match_display,
        avg_timing_match_class=avg_timing_match_class,
        phase_call_similarity_threshold=phase_call_similarity_threshold,
        combined_phase_call_timeline_svg=combined_phase_call_timeline_svg,
        summary_similarity_rows=summary_similarity_rows,
        clearance_trends=clearance_trends,
        device_trends=device_trends,
        integrity_rows=integrity_rows,
        detail_rows=detail_rows,
        version=version,
        generated_at=datetime.now().strftime("%Y-%m-%d"),
    )

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
    return str(output)
