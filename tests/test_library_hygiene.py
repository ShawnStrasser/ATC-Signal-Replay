"""Library hygiene: import side effects, logging, thread-safe plotting, packaging flags.

These guard the properties an application relies on when it embeds
signal_replay: importing the package is cheap and silent, all output goes
through the ``signal_replay`` logger, and plotting works from worker threads.
"""

import ast
import io
import logging
import subprocess
import sys
import textwrap
import threading
from pathlib import Path
from unittest.mock import patch

import pytest

import signal_replay as sr
from signal_replay import _logging as sr_logging

SRC_DIR = Path(sr.__file__).resolve().parent


def _run_python(code: str, cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=240,
    )


@pytest.fixture
def restore_package_logger():
    pkg_logger = logging.getLogger("signal_replay")
    handlers = list(pkg_logger.handlers)
    level = pkg_logger.level
    propagate = pkg_logger.propagate
    yield pkg_logger
    for handler in list(pkg_logger.handlers):
        if handler not in handlers:
            pkg_logger.removeHandler(handler)
            handler.close()
    pkg_logger.setLevel(level)
    pkg_logger.propagate = propagate


# ---------------------------------------------------------------------------
# Import side effects
# ---------------------------------------------------------------------------

def test_import_is_lazy_and_configures_no_logging(tmp_path):
    result = _run_python(
        """
        import logging, sys
        root_before = list(logging.getLogger().handlers)
        import signal_replay
        heavy = ["matplotlib.pyplot", "atspm", "ibis", "yaml", "psutil", "requests"]
        loaded = [name for name in heavy if name in sys.modules]
        assert not loaded, f"imported at package import time: {loaded}"
        handlers = logging.getLogger("signal_replay").handlers
        assert len(handlers) == 1 and isinstance(handlers[0], logging.NullHandler), handlers
        assert logging.getLogger().handlers == root_before
        print("OK")
        """,
        cwd=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "OK"


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def test_enable_console_logging_is_idempotent(restore_package_logger):
    pkg_logger = restore_package_logger
    stream = io.StringIO()

    first = sr.enable_console_logging(stream=stream)
    second = sr.enable_console_logging(stream=stream)

    tagged = [h for h in pkg_logger.handlers if getattr(h, "_signal_replay_console", False)]
    assert tagged == [second]
    assert first is not second
    assert pkg_logger.propagate is False

    logging.getLogger("signal_replay.replay").info("hello %s", "world")
    assert stream.getvalue().count("hello world") == 1

    sr.disable_console_logging()
    assert not [h for h in pkg_logger.handlers if getattr(h, "_signal_replay_console", False)]
    assert pkg_logger.propagate is True


def test_log_to_file_closes_handler(tmp_path, restore_package_logger):
    pkg_logger = restore_package_logger
    log_path = tmp_path / "sub" / "run.log"

    with sr.log_to_file(log_path) as handler:
        assert handler in pkg_logger.handlers
        logging.getLogger("signal_replay.batch_runner").info("inside the block")

    assert handler not in pkg_logger.handlers
    logging.getLogger("signal_replay.batch_runner").info("after the block")
    text = log_path.read_text(encoding="utf-8")
    assert "inside the block" in text
    assert "after the block" not in text
    log_path.unlink()  # fails on Windows if the handler were still open


def test_log_to_file_closes_handler_on_error(tmp_path, restore_package_logger):
    log_path = tmp_path / "run.log"
    with pytest.raises(RuntimeError):
        with sr.log_to_file(log_path):
            raise RuntimeError("boom")
    assert not [h for h in restore_package_logger.handlers if isinstance(h, logging.FileHandler)]
    log_path.unlink()


@pytest.fixture
def root_at_warning():
    root = logging.getLogger()
    level = root.level
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
    root.addHandler(handler)
    root.setLevel(logging.WARNING)
    yield stream
    root.removeHandler(handler)
    root.setLevel(level)


def test_log_to_file_does_not_leak_info_to_app_handlers(tmp_path, restore_package_logger, root_at_warning):
    pkg_logger = restore_package_logger
    pkg_logger.setLevel(logging.NOTSET)
    log = logging.getLogger("signal_replay.batch_runner")
    log.info("before")
    with sr.log_to_file(tmp_path / "run.log"):
        log.info("during run")
        log.warning("warned during run")
    log.info("after")
    app_output = root_at_warning.getvalue()
    assert "INFO" not in app_output
    assert "WARNING:signal_replay.batch_runner:warned during run" in app_output
    assert "before" not in app_output and "after" not in app_output
    text = (tmp_path / "run.log").read_text(encoding="utf-8")
    assert "during run" in text and "warned during run" in text
    assert pkg_logger.level == logging.NOTSET
    assert pkg_logger.propagate is True


def test_concurrent_log_to_file_scopes_stay_separate(tmp_path, restore_package_logger, root_at_warning):
    pkg_logger = restore_package_logger
    pkg_logger.setLevel(logging.NOTSET)
    log = logging.getLogger("signal_replay.orchestrator")
    a_entered, b_entered, a_exited = threading.Event(), threading.Event(), threading.Event()
    errors = []

    def job_a():
        try:
            with sr.log_to_file(tmp_path / "a.log"):
                a_entered.set()
                b_entered.wait(5)
                log.info("A-while-B-open")

                def worker():
                    log.info("A-worker-thread")

                thread = threading.Thread(target=sr_logging.run_in_log_context(worker))
                thread.start()
                thread.join()
            a_exited.set()
        except Exception as exc:  # pragma: no cover - reported below
            errors.append(exc)
            a_exited.set()

    def job_b():
        try:
            a_entered.wait(5)
            with sr.log_to_file(tmp_path / "b.log"):
                b_entered.set()
                a_exited.wait(5)
                log.info("B-after-A-exit")
        except Exception as exc:  # pragma: no cover - reported below
            errors.append(exc)

    threads = [threading.Thread(target=job_a), threading.Thread(target=job_b)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)
    assert not errors

    a_text = (tmp_path / "a.log").read_text(encoding="utf-8")
    b_text = (tmp_path / "b.log").read_text(encoding="utf-8")
    assert "A-while-B-open" in a_text and "A-worker-thread" in a_text
    assert "B-after-A-exit" not in a_text
    assert "B-after-A-exit" in b_text
    assert "A-while-B-open" not in b_text and "A-worker-thread" not in b_text
    assert pkg_logger.level == logging.NOTSET
    assert pkg_logger.propagate is True
    assert not [h for h in pkg_logger.handlers if isinstance(h, logging.FileHandler)]
    assert "INFO" not in root_at_warning.getvalue()


def test_debug_level_promotes_debug_flag_messages():
    assert sr_logging.debug_level(True) == logging.INFO
    assert sr_logging.debug_level(False) == logging.DEBUG


def _build_suite(tmp_path: Path) -> sr.SoftwareTestSuite:
    scenario = sr.TestScenario(
        scenario_id="S1",
        database_name="S1.bin",
        events_source="S1.parquet",
        test_type=sr.TestType.SIMILARITY,
    )
    batch = sr.TestBatch(batch_id="batch_1", assignments={"S1": "127.0.0.1:1025"})
    return sr.SoftwareTestSuite(
        suite_name="suite",
        software_version="new",
        baseline_version="old",
        scenarios=[scenario],
        batches=[batch],
        output_dir=str(tmp_path),
    )


def _fake_similarity_batch(self, batch, similarity_ids, db_loader_callback=None):
    logging.getLogger("signal_replay.orchestrator").info("fake replay for %s", batch.batch_id)
    return None


def test_batch_runner_construction_adds_no_handlers(tmp_path):
    managed = [logging.getLogger(), logging.getLogger("signal_replay"),
               logging.getLogger("signal_replay.batch_runner")]
    before = [list(lg.handlers) for lg in managed]

    sr.BatchRunner(_build_suite(tmp_path), debug=False)

    assert [list(lg.handlers) for lg in managed] == before
    assert not (tmp_path / "new" / "run.log").exists()


def test_batch_runner_run_log_is_scoped_and_closed(tmp_path):
    runner = sr.BatchRunner(_build_suite(tmp_path), debug=False)
    handlers_before = list(logging.getLogger("signal_replay").handlers)

    with patch.object(sr.BatchRunner, "_run_similarity_batch", _fake_similarity_batch):
        runner.run(db_loader_callback=lambda *_: True)

    assert list(logging.getLogger("signal_replay").handlers) == handlers_before
    text = runner.run_log_path.read_text(encoding="utf-8")
    assert "fake replay for batch_1" in text
    assert "Batch batch_1 complete" in text
    runner.run_log_path.unlink()  # proves the FileHandler was closed


def test_batch_runner_run_log_can_be_disabled(tmp_path):
    runner = sr.BatchRunner(_build_suite(tmp_path), debug=False, run_log=False)
    with patch.object(sr.BatchRunner, "_run_similarity_batch", _fake_similarity_batch):
        runner.run_batch_once(runner.suite.batches[0], db_loader_callback=lambda *_: True)
    assert not runner.run_log_path.exists()


def test_checkpoint_timestamps_are_timezone_aware(tmp_path):
    runner = sr.BatchRunner(_build_suite(tmp_path), debug=False, run_log=False)
    checkpoint = runner._default_checkpoint()
    assert checkpoint["started_at"].endswith("+00:00")


# ---------------------------------------------------------------------------
# Source checks: no print/input/traceback output from library code
# ---------------------------------------------------------------------------

# Functions whose explicit purpose is to print a summary the caller asked for.
_PRINT_ALLOWED = {"compare_event_sequences", "compare_and_visualize"}
_LOG_METHODS = {"debug", "info", "warning", "error", "exception", "critical", "log"}


def _iter_calls_with_function(tree):
    """Yield (call_node, enclosing_function_name) for every call in a module."""
    def visit(node, func_name):
        for child in ast.iter_child_nodes(node):
            name = func_name
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = child.name if func_name is None else func_name
            if isinstance(child, ast.Call):
                yield child, name
            yield from visit(child, name)
    yield from visit(tree, None)


def _string_constants(node):
    for sub in ast.walk(node):
        if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
            yield sub.value


@pytest.mark.parametrize("path", sorted(SRC_DIR.glob("*.py")), ids=lambda p: p.name)
def test_library_output_goes_through_logging(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    problems = []
    for call, func_name in _iter_calls_with_function(tree):
        func = call.func
        if isinstance(func, ast.Name) and func.id == "print":
            if func_name not in _PRINT_ALLOWED:
                problems.append(f"line {call.lineno}: print() in {func_name}")
            for value in _string_constants(call):
                if not value.isascii():
                    problems.append(f"line {call.lineno}: non-ASCII print text")
        if isinstance(func, ast.Attribute) and func.attr == "print_exc":
            problems.append(f"line {call.lineno}: traceback.print_exc()")
        if (
            isinstance(func, ast.Attribute)
            and func.attr in _LOG_METHODS
            and (
                (isinstance(func.value, ast.Name) and func.value.id == "logger")
                or (isinstance(func.value, ast.Attribute) and func.value.attr == "logger")
            )
        ):
            if call.args and isinstance(call.args[0], ast.JoinedStr):
                problems.append(f"line {call.lineno}: f-string passed to logger.{func.attr}")
            for value in _string_constants(call):
                if not value.isascii():
                    problems.append(f"line {call.lineno}: non-ASCII log text")
    assert not problems, problems


@pytest.mark.parametrize("path", sorted(SRC_DIR.glob("*.py")), ids=lambda p: p.name)
def test_no_pyplot_or_global_backend_in_library(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert all(alias.name != "matplotlib.pyplot" for alias in node.names), path.name
        if isinstance(node, ast.ImportFrom) and node.module == "matplotlib":
            assert all(alias.name != "pyplot" for alias in node.names), path.name
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "use" and getattr(node.func.value, "id", None) == "matplotlib":
                pytest.fail(f"{path.name}:{node.lineno} calls matplotlib.use()")


# ---------------------------------------------------------------------------
# Thread-safe plotting
# ---------------------------------------------------------------------------

def test_gantt_chart_renders_from_worker_threads(tmp_path):
    # Runs in a fresh interpreter with no backend forced (unlike conftest.py), so a
    # GUI backend being picked up would show as a warning or a Tcl crash at exit.
    result = _run_python(
        f"""
        import sys, threading, warnings
        from datetime import datetime, timedelta
        from pathlib import Path
        import pandas as pd
        from matplotlib.figure import Figure
        from signal_replay.comparison import create_comparison_gantt_matplotlib

        base = datetime(2026, 1, 1, 9, 0, 0)
        rows = [
            {{"StartTime": base + timedelta(seconds=40 * i),
              "EndTime": base + timedelta(seconds=40 * i + 25),
              "EventClass": "Green", "EventValue": 2 if i % 2 == 0 else 6}}
            for i in range(10)
        ]
        timeline = pd.DataFrame(rows)
        out_dir = Path(r"{tmp_path}")
        errors, figures = [], []

        def worker(idx):
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("error")
                    fig = create_comparison_gantt_matplotlib(
                        timeline_a=timeline, timeline_b=timeline.copy(),
                        label_a="A", label_b="B", title=f"T{{idx}}",
                        output_path=out_dir / f"chart_{{idx}}.png",
                        window_minutes=5.0, align_by_time_delta=False,
                    )
                figures.append(fig)
            except BaseException as exc:
                errors.append(repr(exc))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, errors
        assert all(isinstance(f, Figure) for f in figures), figures
        assert "matplotlib.pyplot" not in sys.modules
        print("OK")
        """,
        cwd=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "outside of the main thread" not in result.stderr
    assert result.stdout.strip() == "OK"
    for idx in range(2):
        png = tmp_path / f"chart_{idx}.png"
        assert png.exists() and png.stat().st_size > 1000


# ---------------------------------------------------------------------------
# Optional dependencies and pytest collection flags
# ---------------------------------------------------------------------------

def test_load_annotations_without_pyyaml_raises_clear_error(tmp_path, monkeypatch):
    path = tmp_path / "notes.yaml"
    path.write_text("S1: note\n", encoding="utf-8")
    monkeypatch.setitem(sys.modules, "yaml", None)
    with pytest.raises(ImportError, match="pip install"):
        sr.load_annotations(str(path))


def test_memory_log_without_psutil_is_a_noop(monkeypatch, caplog):
    from signal_replay.collector import _log_memory

    monkeypatch.setitem(sys.modules, "psutil", None)
    with caplog.at_level(logging.DEBUG, logger="signal_replay.collector"):
        _log_memory("label")
    assert not [r for r in caplog.records if "Memory RSS" in r.getMessage()]


@pytest.mark.parametrize("name", ["TestType", "TestScenario", "TestBatch", "SoftwareTestSuite"])
def test_exported_test_classes_are_not_collected_by_pytest(name):
    assert getattr(sr, name).__test__ is False


def test_test_type_enum_members_unchanged():
    assert [member.value for member in sr.TestType] == ["similarity", "conflict"]


def test_logging_helpers_are_public():
    for name in ("enable_console_logging", "disable_console_logging", "log_to_file"):
        assert name in sr.__all__
        assert callable(getattr(sr, name))


def test_generate_timeline_prints_nothing(capfd):
    """atspm prints timing lines unless told verbose=0; the package passes it."""
    from datetime import datetime, timedelta

    import pandas as pd

    rows = [
        {"TimeStamp": datetime(2026, 1, 1, 7) + timedelta(seconds=30 * i),
         "EventId": 1 if i % 2 == 0 else 8, "Parameter": 2, "DeviceId": "1"}
        for i in range(60)
    ]
    timeline = sr.generate_timeline(pd.DataFrame(rows), device_id="1")
    assert not timeline.empty
    captured = capfd.readouterr()
    assert captured.out == ""


def test_ci_runs_for_every_directory_the_offline_suite_uses():
    workflow = (Path(__file__).resolve().parents[1] / ".github" / "workflows" / "unit-tests.yml").read_text(
        encoding="utf-8"
    )
    for path in ("src/**", "tests/**", "software_validation/**", "examples/**", "pyproject.toml"):
        assert workflow.count(f"'{path}'") == 2, path  # push and pull_request
