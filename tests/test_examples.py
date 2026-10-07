"""
The examples/ scripts compile and the offline app-integration example runs.

examples/app_integration.py replaces every SNMP send with an in-process
stand-in and supplies output events from a simulated log, so it never
contacts a device.
"""

import py_compile
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


@pytest.mark.parametrize("path", sorted(EXAMPLES.rglob("*.py")), ids=lambda p: p.name)
def test_example_compiles(path, tmp_path):
    py_compile.compile(str(path), cfile=str(tmp_path / "out.pyc"), doraise=True)


def _run_example(*args):
    proc = subprocess.run(
        [sys.executable, str(EXAMPLES / "app_integration.py"), *args],
        capture_output=True,
        text=True,
        timeout=300,
    )
    output = proc.stdout + proc.stderr
    assert proc.returncode == 0, output
    return output


def test_app_integration_example_replicates_and_stores_results():
    output = _run_example()
    assert "replicated=True" in output, output
    assert "detectors reset: {'1234': True}" in output, output
    assert "A100: PASS" in output and "B200: FAIL" in output, output
    assert "replay_conflicts: 1 row(s)" in output, output
    assert "validation_scenario_results: 2 row(s)" in output, output
    # Library code prints nothing; only the example's own lines reach stdout.
    assert "aggregation runtime" not in output, output


def test_app_integration_example_cancel():
    output = _run_example("--cancel-after", "2")
    assert "stop_reason=cancelled" in output, output
    assert "cancelled=True" in output, output
    assert "detectors reset: {'1234': True}" in output, output
