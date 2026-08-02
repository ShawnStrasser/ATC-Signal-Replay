from __future__ import annotations
import json
from pathlib import Path
import runpy

def test_offline_comparison_example(tmp_path, monkeypatch):
    source = Path(__file__).parents[1] / "examples" / "offline_comparison"
    target = tmp_path / "offline_comparison"
    target.mkdir()
    for path in source.glob("*.csv"):
        (target / path.name).write_bytes(path.read_bytes())
    script_text = (source / "run_example.py").read_text(encoding="utf-8").replace("ROOT = Path(__file__).resolve().parent", f'ROOT = Path(r"{target}")')
    test_script = target / "run_example.py"
    test_script.write_text(script_text, encoding="utf-8")
    monkeypatch.chdir(target)
    runpy.run_path(str(test_script), run_name="__main__")
    summary = json.loads((target / "summary.json").read_text(encoding="utf-8"))
    assert summary["same_match_percentage"] == 100.0
    assert summary["changed_match_percentage"] < 100.0