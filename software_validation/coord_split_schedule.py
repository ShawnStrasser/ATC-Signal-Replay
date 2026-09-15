#!/usr/bin/env python
"""Expand coordination pattern JSON into programmed split intervals.

Output columns: Phase, start_time, end_time

Reads every *.json in the coord_patterns folder beside this script and writes
a sibling *_coord_splits.csv for each one.  Run with no arguments.

Ring/barrier notes
------------------
Each ring runs its phases in sequence.  The 'a' and 'b' tokens mark barrier
crossings -- both rings must complete all pre-barrier phases before either can
advance past the barrier.  With programmed (fixed) splits the barrier groups
are balanced by design (e.g. P1+P2 split sum == P5+P6 split sum), so each
ring's sequential timing naturally synchronises at the barriers.

At each plan change the boundary is reconciled per ring: if the same phase is
active on both sides it keeps the old start and adopts the new plan's end time.
If the active phase changes the old split terminates at the boundary and the
new one begins there.
"""
from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any

SECONDS_PER_DAY = 24 * 60 * 60
EPSILON = 1e-9
COORD_PATTERNS_DIR = Path(__file__).resolve().parent / "coord_patterns"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hms(s: int) -> str:
    return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def _to_sec(t: str) -> int:
    parts = t.strip().split(":")
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + (int(parts[2]) if len(parts) > 2 else 0)


def _ring_phases(value: Any) -> list[str]:
    """Ordered phase labels from a ring string like '2,1,a,4,b', barriers stripped."""
    if not value:
        return []
    return [f"P{t.strip()}" for t in str(value).split(",") if t.strip().isdigit()]


def _parse_splits(raw: Any) -> dict[str, int]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, int] = {}
    for k, v in raw.items():
        if v is None:
            continue
        digits = "".join(c for c in str(k) if c.isdigit())
        if digits:
            out[f"P{int(digits)}"] = int(v)
    return out


def _seq_offsets(phases: list[str], splits: dict[str, int]) -> dict[str, int]:
    """Start-offset within the cycle for each phase, in ring order."""
    offsets: dict[str, int] = {}
    elapsed = 0
    for p in phases:
        offsets[p] = elapsed
        elapsed += splits[p]
    return offsets


def _ref_phase(value: Any) -> str | None:
    if not value:
        return None
    digits = "".join(c for c in str(value) if c.isdigit())
    return f"P{int(digits)}" if digits else None


def _anchor(
    offset: int,
    ref: str | None,
    rings: dict[str, list[str]],
    splits: dict[str, int],
) -> int:
    """Second-of-day where the notional cycle index 0 starts."""
    if ref is not None:
        for phases in rings.values():
            if ref in phases:
                return offset - _seq_offsets(phases, splits)[ref]
    return offset


def _phase_at(
    second: float,
    anchor: int,
    cycle: int,
    phases: list[str],
    splits: dict[str, int],
) -> tuple[str, int, int] | None:
    """Return (phase, phase_start, phase_end) active in this ring at `second`."""
    ci = math.floor((second - anchor) / cycle)
    cs = anchor + ci * cycle
    elapsed = 0
    for p in phases:
        dur = splits[p]
        if elapsed <= second - cs < elapsed + dur:
            return p, cs + elapsed, cs + elapsed + dur
        elapsed += dur
    return None


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

class _IV:
    """Mutable split interval -- internal use only."""
    __slots__ = ("phase", "ring", "start", "end")

    def __init__(self, phase: str, ring: str, start: int, end: int) -> None:
        self.phase = phase
        self.ring = ring
        self.start = start
        self.end = end


def build_programmed_splits(path: Path) -> list[dict[str, object]]:
    """Return list of {"Phase": ..., "start_time": ..., "end_time": ...} dicts."""
    schedule = json.loads(path.read_text(encoding="utf-8"))["traffic_signal_schedule"]

    # Parse each schedule entry into a window dict (None = free / uncoordinated)
    windows: list[dict | None] = []
    for i, entry in enumerate(schedule):
        start = _to_sec(entry["time"])
        end = _to_sec(schedule[i + 1]["time"]) if i + 1 < len(schedule) else SECONDS_PER_DAY
        if entry.get("cycle") is None:
            windows.append(None)
            continue
        splits = _parse_splits(entry.get("splits"))
        rings = {
            "ring_1": _ring_phases(entry.get("ring_1")),
            "ring_2": _ring_phases(entry.get("ring_2")),
        }
        ref = _ref_phase(entry.get("ref_point"))
        cycle = int(entry["cycle"])
        anch = _anchor(int(entry["offset"]), ref, rings, splits)
        windows.append({
            "start": start, "end": end,
            "cycle": cycle, "anchor": anch,
            "rings": rings, "splits": splits,
        })

    # Generate raw intervals for every coordinated window
    all_iv: list[_IV] = []
    for w in windows:
        if w is None:
            continue
        si = math.floor((w["start"] - w["anchor"]) / w["cycle"]) - 1
        ei = math.floor((w["end"] - w["anchor"]) / w["cycle"]) + 1
        for ci in range(si, ei + 1):
            cs = w["anchor"] + ci * w["cycle"]
            for ring_name, phases in w["rings"].items():
                if not phases:
                    continue
                offsets = _seq_offsets(phases, w["splits"])
                for p in phases:
                    s = max(cs + offsets[p], w["start"])
                    e = min(cs + offsets[p] + w["splits"][p], w["end"])
                    if s < e:
                        all_iv.append(_IV(p, ring_name, s, e))

    # Reconcile plan boundaries -- merge same-phase intervals that straddle a change
    removed: set[int] = set()
    for i in range(1, len(windows)):
        prev, curr = windows[i - 1], windows[i]
        if prev is None or curr is None:
            continue
        b = curr["start"]
        for ring_name in ("ring_1", "ring_2"):
            pp = prev["rings"].get(ring_name, [])
            cp = curr["rings"].get(ring_name, [])
            if not pp or not cp:
                continue
            p_act = _phase_at(b - EPSILON, prev["anchor"], prev["cycle"], pp, prev["splits"])
            c_act = _phase_at(float(b), curr["anchor"], curr["cycle"], cp, curr["splits"])
            if p_act is None or c_act is None or p_act[0] != c_act[0]:
                continue
            if p_act[2] <= b or c_act[2] <= b:
                continue
            old = next(
                (iv for iv in all_iv if id(iv) not in removed
                 and iv.ring == ring_name and iv.phase == p_act[0]
                 and iv.end == b and iv.start < b),
                None,
            )
            new = next(
                (iv for iv in all_iv if id(iv) not in removed
                 and iv.ring == ring_name and iv.phase == c_act[0]
                 and iv.start == b and iv.end > b),
                None,
            )
            if old and new:
                old.end = new.end
                removed.add(id(new))

    return sorted(
        ({"Phase": iv.phase, "start_time": _hms(iv.start), "end_time": _hms(iv.end)}
         for iv in all_iv if id(iv) not in removed and iv.start < iv.end),
        key=lambda r: (r["start_time"], r["Phase"]),
    )


# ---------------------------------------------------------------------------
# Batch conversion
# ---------------------------------------------------------------------------

def convert_all_pattern_files(
    coord_patterns_dir: Path = COORD_PATTERNS_DIR,
) -> list[tuple[Path, int]]:
    results: list[tuple[Path, int]] = []
    for p in sorted(coord_patterns_dir.glob("*.json")):
        rows = build_programmed_splits(p)
        out = p.with_name(f"{p.stem}_coord_splits.csv")
        with out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["Phase", "start_time", "end_time"])
            writer.writeheader()
            writer.writerows(rows)
        results.append((out, len(rows)))
    return results


def main() -> int:
    if not COORD_PATTERNS_DIR.exists():
        raise FileNotFoundError(f"coord_patterns folder not found: {COORD_PATTERNS_DIR}")
    results = convert_all_pattern_files()
    if not results:
        print(f"No JSON files found in {COORD_PATTERNS_DIR}")
        return 0
    for out_path, count in results:
        print(f"Wrote {count} rows to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
