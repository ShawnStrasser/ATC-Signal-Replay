from __future__ import annotations
import json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "paper/generated/results"

def main() -> None:
    required = ["dataset_summary.csv", "software_release_results.csv", "parameter_intervention_results.csv", "parameter_detection_summary.csv", "sensitivity_results.csv"]
    missing = [p for p in required if not (RESULTS / p).exists()]
    if missing:
        raise SystemExit("Missing generated outputs: " + ", ".join(missing))
    software = pd.read_csv(RESULTS / "software_release_results.csv")
    parameter = pd.read_csv(RESULTS / "parameter_intervention_results.csv")
    datasets = pd.read_csv(RESULTS / "dataset_summary.csv")
    checks = {
        "software_rows": len(software) == 25,
        "software_passes": int((software.status == "PASS").sum()) == 19,
        "software_candidate_failures": int((software.status != "PASS").sum()) == 6,
        "parameter_rows": len(parameter) == 25,
        "parameter_changed": int((parameter.expected_comparison_group == "changed").sum()) == 14,
        "parameter_unchanged": int((parameter.expected_comparison_group == "unchanged").sum()) == 11,
        "parameter_changed_flagged": int(parameter.loc[parameter.expected_comparison_group == "changed", "automatically_flagged"].sum()) == 11,
        "parameter_unchanged_flagged": int(parameter.loc[parameter.expected_comparison_group == "unchanged", "automatically_flagged"].sum()) == 0,
        "unchanged_sequence_mean": round(float(parameter.loc[parameter.expected_comparison_group == "unchanged", "sequence_match_percent"].mean()), 1) == 99.6,
        "unchanged_timing_mean": round(float(parameter.loc[parameter.expected_comparison_group == "unchanged", "timing_match_percent"].mean()), 1) == 97.7,
        "all_campaign_conflicts_zero": int(datasets["conflicts_count"].sum()) == 0,
    }
    print(json.dumps(checks, indent=2))
    if not all(checks.values()):
        raise SystemExit(1)

if __name__ == "__main__":
    main()
