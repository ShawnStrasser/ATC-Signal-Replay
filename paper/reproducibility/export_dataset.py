from __future__ import annotations
import argparse
import hashlib
import json
import shutil
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]
DBS = {
    "outputs-2.15.1": ROOT / "firmware_validation/results/2.15.1/collected.db",
    "outputs-2.18.1": ROOT / "firmware_validation/results/2.18.1/collected.db",
    "outputs-2.18.1-parameter-modified": ROOT / "firmware_validation/results/2.18.1_trailing/collected.db",
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser(description="Export a candidate comparison-only reproducibility bundle; nothing is published automatically.")
    ap.add_argument("--output", type=Path, default=ROOT / "paper/reproducibility-candidate")
    ap.add_argument("--scenario", action="append", help="Optional scenario ID; repeat to restrict export.")
    args = ap.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    scenarios = args.scenario
    manifest = {"format": "comparison-only-v1", "source_commit": "1f25f36", "databases": {}}
    for name, db in DBS.items():
        target = out / name
        target.mkdir(exist_ok=True)
        con = duckdb.connect(str(db), read_only=True)
        for table in ["events", "input_events", "input_detector_events", "latency_offset_updates"]:
            if table not in {r[0] for r in con.execute("show tables").fetchall()}:
                continue
            where = ""
            params = []
            if scenarios:
                placeholders = ",".join("?" for _ in scenarios)
                where = f" WHERE device_id IN ({placeholders})"
                params = scenarios
            frame = con.execute(f'SELECT * FROM "{table}"{where}', params).fetchdf()
            frame.to_csv(target / f"{table}.csv", index=False)
        con.close()
        manifest["databases"][name] = {"source_sha256": sha256(db), "source_path": str(db), "scenario_count": len(scenarios) if scenarios else 25}
    shutil.copy2(ROOT / "paper/analysis/intervention_manifest.csv", out / "intervention_manifest.csv")
    shutil.copy2(ROOT / "paper/analysis/paper_config.toml", out / "comparison-parameters.toml")
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote candidate bundle at {out}; review and approve before publishing.")

if __name__ == "__main__":
    main()
