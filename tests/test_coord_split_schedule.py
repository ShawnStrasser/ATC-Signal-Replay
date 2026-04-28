import csv
import importlib.util
import sys
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "firmware_validation" / "coord_split_schedule.py"
    spec = importlib.util.spec_from_file_location("coord_split_schedule_module", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _rows():
    module = _load_module()
    path = (
        Path(__file__).resolve().parents[1]
        / "firmware_validation" / "coord_patterns" / "test.json"
    )
    return module.build_programmed_splits(path)


def test_skips_free_time_and_clips_to_coord_start():
    rows = _rows()
    first_p2 = next(r for r in rows if r["Phase"] == "P2")
    assert first_p2["start_time"] == "06:00:00"
    assert first_p2["end_time"] == "06:00:32"


def test_truncates_when_active_phase_changes_at_pattern_change():
    rows = _rows()
    # P2 in ring_1 should be cut at the 08:45 boundary
    outgoing = next(r for r in rows if r["Phase"] == "P2" and r["start_time"] == "08:44:24")
    assert outgoing["end_time"] == "08:45:00"
    # P4 starts fresh from that boundary
    incoming = next(r for r in rows if r["Phase"] == "P4" and r["start_time"] == "08:45:00")
    assert incoming["end_time"] == "08:45:37"


def test_merges_same_phase_across_plan_boundary():
    rows = _rows()
    # P4 was active at 17:30 in both Coord Plan 3 and Coord Plan 2 -> merged
    merged = next(r for r in rows if r["Phase"] == "P4" and r["start_time"] == "17:29:52")
    assert merged["end_time"] == "17:30:37"
    # No duplicate P4 starting exactly at 17:30:00
    assert not any(r for r in rows if r["Phase"] == "P4" and r["start_time"] == "17:30:00")


def test_convert_all_pattern_files_writes_csv_beside_each_json(tmp_path):
    module = _load_module()
    coord_dir = tmp_path / "coord_patterns"
    coord_dir.mkdir()
    source = (
        Path(__file__).resolve().parents[1]
        / "firmware_validation" / "coord_patterns" / "test.json"
    )
    (coord_dir / "sample.json").write_text(source.read_text(encoding="utf-8"), encoding="utf-8")

    results = module.convert_all_pattern_files(coord_dir)

    assert len(results) == 1
    out_path, count = results[0]
    assert out_path == coord_dir / "sample_coord_splits.csv"
    assert out_path.exists()
    assert count > 0

    with out_path.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    assert set(rows[0].keys()) == {"Phase", "start_time", "end_time"}
    assert rows[0]["Phase"] == "P2"
    assert rows[0]["start_time"] == "06:00:00"
