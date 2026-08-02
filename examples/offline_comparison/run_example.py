from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
from signal_replay.comparison import compare_event_sequences
ROOT = Path(__file__).resolve().parent
def compare(name: str):
    reference = pd.read_csv(ROOT / "reference.csv")
    candidate = pd.read_csv(ROOT / name)
    return compare_event_sequences(reference, candidate, label_a="reference", label_b=name, print_summary=False)
def main() -> None:
    same = compare("candidate_same.csv")
    changed = compare("candidate_changed.csv")
    summary = {"same_match_percentage": float(same.match_percentage), "changed_match_percentage": float(changed.match_percentage), "changed_divergence_count": len(changed.divergence_windows)}
    (ROOT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
if __name__ == "__main__":
    main()