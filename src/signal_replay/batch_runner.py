import dataclasses
import inspect
import json
import logging
import os
import sys
import threading
import warnings
from collections.abc import Mapping
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import duckdb

from ._logging import log_to_file
from .progress import ProgressCallback, ProgressEvent, ProgressReporter, Stage, StatusTracker
from .config import SignalConfig
from .orchestrator import ATCSimulation
from .results import new_run_uuid
from .test_suite import SoftwareTestSuite, ScenarioResult, TestType, TestScenario
from .workspace import (
    BATCH_DB_NAME,
    CHECKPOINT_NAME,
    PLOTS_DIR_NAME,
    RUN_LOG_NAME,
    resolve_work_dir,
    write_manifest,
)

logger = logging.getLogger(__name__)


class OperationCancelled(RuntimeError):
    """Raised when a batch or comparison is cancelled through its stop event.

    :meth:`BatchRunner.run_batch_once` and :func:`compare_software` raise it;
    :meth:`BatchRunner.run` returns ``{..., 'cancelled': True}`` instead.
    """


@dataclass(frozen=True)
class DbLoadRequest:
    """What :class:`BatchRunner` needs loaded on a controller before a scenario runs.

    Passed to a ``db_loader_callback`` that takes one argument. The
    callback must return True once ``database_name`` is loaded on
    ``target``; returning False (or raising) aborts the batch, which is
    then cleared. The callback runs on the thread that called
    :meth:`BatchRunner.run`.

    Attributes:
        batch_id: Batch being run.
        scenario_id: Scenario the database belongs to.
        database_name: Controller database to load.
        target: Assigned controller (``host``, ``host:port`` or
            ``host:udp_port:http_port``).
        index: 1-based position of this load within the batch.
        total: Number of loads in the batch (one per scenario).
        test_type: ``'similarity'`` or ``'conflict'``.
    """

    batch_id: str
    scenario_id: str
    database_name: str
    target: str
    index: int
    total: int
    test_type: Optional[str] = None


def console_db_loader(request: DbLoadRequest) -> bool:
    """Ask on the console for a controller database load and wait for Enter.

    The default loader for interactive use (see ``BatchRunner(interactive=...)``).
    Never call it from a GUI or service: it blocks on ``input()``.
    """
    if request.test_type == TestType.CONFLICT.value:
        logger.info(
            "Load conflict scenario %s: %s -> %s",
            request.scenario_id, request.database_name, request.target,
        )
        input(
            f"Load conflict database for {request.scenario_id} ({request.database_name}) "
            "and press Enter...\n"
        )
        return True
    message = "\n".join([
        f"Batch {request.batch_id}: load these databases before continuing "
        f"({request.index}/{request.total}):",
        f" - {request.scenario_id}: {request.database_name} -> {request.target}",
    ])
    logger.info("%s", message)
    input(f"\n{message}\n\nPress Enter when database loading is complete...\n")
    return True


def _stdin_is_interactive() -> bool:
    """True when stdin is a terminal a person can answer."""
    stdin = sys.stdin
    if stdin is None:
        return False
    try:
        return bool(stdin.isatty())
    except (AttributeError, ValueError, OSError):
        return False


def _takes_request(callback: Callable[..., Any]) -> bool:
    """True when ``callback`` takes one DbLoadRequest, False for the 0.x (name, target) form."""
    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return False
    try:
        signature.bind(object(), object())
        return False
    except TypeError:
        pass
    try:
        signature.bind(object())
        return True
    except TypeError:
        return False


def _call_db_loader(callback: Callable[..., Any], request: DbLoadRequest) -> bool:
    """Call a loader in either supported form and return its answer."""
    if _takes_request(callback):
        return bool(callback(request))
    return bool(callback(request.database_name, request.target))


def _parse_assignment(value: str) -> Tuple[str, Optional[int], Optional[int]]:
    """
    Parse assignment target into host + UDP/SNMP + HTTP ports.

    Supported formats:
      - host                    -> udp=None, http=None
      - host:port               -> localhost uses udp=http=port; remote uses udp=None, http=port
      - host:udp_port:http_port -> explicit ports for both protocols
    """
    parts = value.split(":")
    if len(parts) == 1:
        return value, None, None
    if len(parts) == 2:
        host, port_text = parts
        port = int(port_text)
        if host.lower() in ("localhost", "127.0.0.1"):
            return host, port, port
        return host, None, port
    if len(parts) == 3:
        host, udp_text, http_text = parts
        return host, int(udp_text), int(http_text)
    raise ValueError(
        f"Invalid assignment '{value}'. Use host, host:port, or host:udp_port:http_port."
    )

def _serialize_result(result: ScenarioResult) -> dict:
    return result.to_dict()


def _raise_if_simulation_cancelled(result: object, batch_label: str) -> None:
    """Abort the batch if the simulation reported that it was cancelled."""
    if isinstance(result, Mapping) and result.get("cancelled"):
        raise OperationCancelled(f"Cancelled during {batch_label}")


def _raise_if_simulation_collection_failed(result: object, batch_label: str) -> None:
    """Fail the batch if the simulation reported a fatal collection error."""
    if isinstance(result, Mapping) and result.get("collection_error"):
        raise RuntimeError(f"Data collection failed during {batch_label}")


class BatchRunner:
    """Run the replay batches of a :class:`SoftwareTestSuite` on one controller.

    Args:
        suite: The test suite to run.
        debug: Log extra diagnostic detail from the replay and collector.
        run_log: When True (default), copy signal_replay log records to
            ``<run_dir>/run.log`` while :meth:`run` or :meth:`run_batch_once`
            is executing. The file handler is closed when the call returns.
            Constructing a BatchRunner never adds logging handlers.
        stop_event: Optional ``threading.Event`` shared with the caller.
            Setting it (or calling :meth:`stop`) cancels the whole batch run:
            the active simulation stops and resets its detectors, the
            partly run batch is cleared and not marked completed, and no
            further scenarios or batches start. Every simulation this runner
            creates shares the event. Once set, it stays set; clear it with
            :meth:`reset_stop` (or use a new BatchRunner) to run again.
        event_source: Output-event source for every scenario (see
            :mod:`signal_replay.events`). None uses ``suite.event_source``,
            and when that is None too, the MAXTIME HTTP event log. The
            source receives a :class:`~signal_replay.CollectionTarget` whose
            ``extra`` holds ``scenario_id``, ``database_name`` and
            ``assignment`` plus the scenario's ``collection_extra``.
        on_progress: Optional progress callback. It receives BATCH and
            AWAITING_DB_LOAD events from the runner and every event of the
            simulations it runs, with ``batch_id`` (and ``scenario_id`` for
            conflict scenarios) filled in. Called on worker threads; see
            :mod:`signal_replay.progress`.
        interactive: How databases get loaded when :meth:`run` or
            :meth:`run_batch_once` is called without ``db_loader_callback``.
            True uses :func:`console_db_loader` (an ``input()`` prompt);
            False raises ValueError; None (default) prompts only when stdin
            is a terminal. The check happens before any data is touched.
        work_dir: Optional working folder owned by the caller, used instead
            of ``<suite.output_dir>/<software_version>``. Files get fixed
            names: ``collected.db``, ``checkpoint.json``, ``run.log``,
            ``plots/`` and ``manifest.json``. Nothing is derived from the
            current directory. After :meth:`close` (or leaving a ``with``
            block) no file stays open, so the folder can be deleted.

    BatchRunner is a context manager::

        with BatchRunner(suite, work_dir=folder, run_log=False) as runner:
            runner.run_batch_once(batch, db_loader_callback=loader)
        shutil.rmtree(folder)  # after copying what the app keeps
    """

    def __init__(
        self,
        suite: SoftwareTestSuite,
        debug: bool = False,
        run_log: bool = True,
        stop_event: Optional[threading.Event] = None,
        event_source: Any = None,
        on_progress: Optional[ProgressCallback] = None,
        interactive: Optional[bool] = None,
        work_dir: Optional[Union[str, os.PathLike]] = None,
    ):
        self.suite = suite
        self.interactive = interactive
        self._status = StatusTracker()
        self._progress = ProgressReporter(on_progress, log=logger, status=self._status)
        self._batch_lock = threading.Lock()
        self._batch_info: Dict[str, Any] = {}
        self._last_sim_status: Optional[Dict[str, Any]] = None
        self._batches_run: List[str] = []
        self.event_source = event_source if event_source is not None else getattr(suite, "event_source", None)
        self.debug = debug
        self.run_log = run_log
        #: Identifier of this runner (written to manifest.json with work_dir).
        self.run_uuid: str = new_run_uuid()
        self.work_dir: Optional[Path] = resolve_work_dir(work_dir) if work_dir is not None else None
        if self.work_dir is not None:
            self.run_dir = self.work_dir
        else:
            self.run_dir = Path(suite.output_dir) / suite.software_version
            self.run_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path = self.run_dir / CHECKPOINT_NAME
        self.run_log_path = self.run_dir / RUN_LOG_NAME
        self._simulation_run_uuids: List[str] = []
        self._closed = False
        self._active_simulation: Optional[ATCSimulation] = None
        self._stop_requested: threading.Event = (
            stop_event if stop_event is not None else threading.Event()
        )
        self.logger = logger

    @property
    def stop_event(self) -> threading.Event:
        """The event that cancels this runner (shared with its simulations)."""
        return self._stop_requested

    @property
    def db_path(self) -> Path:
        """The shared working database (``<run_dir>/collected.db``)."""
        return self._shared_db_path()

    # -- lifetime ----------------------------------------------------------

    def __enter__(self) -> "BatchRunner":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        """Release everything this runner holds and refresh ``manifest.json``.

        Log handlers are already closed when each run returns; this also
        stops an active simulation (if a run is still going on another
        thread) and writes the manifest when ``work_dir`` was given. Safe to
        call more than once.
        """
        if self._closed:
            return
        self._closed = True
        sim = self._active_simulation
        if sim is not None:
            sim.request_stop('closed')
        self._write_manifest()

    def _write_manifest(self) -> None:
        if self.work_dir is None:
            return
        try:
            write_manifest(
                self.work_dir,
                run_uuid=self.run_uuid,
                kind="batch",
                extra={
                    "suite_name": self.suite.suite_name,
                    "software_version": self.suite.software_version,
                    "db_path": str(self._shared_db_path()),
                    "simulation_run_uuids": list(self._simulation_run_uuids),
                    "state": self._status.state,
                },
            )
        except Exception:
            logger.warning("Could not write manifest.json in %s", self.work_dir, exc_info=True)

    # -- progress / status -------------------------------------------------

    def get_status(self) -> Dict[str, Any]:
        """Thread-safe, JSON-safe snapshot of the batch run.

        Contains the fields described in
        :class:`~signal_replay.progress.StatusTracker` (fed by the runner's
        events and by its simulations' events), plus:

        * ``batch``: ``{batch_id, index, total, scenario_id,
          completed_batches, cancelled_batches, errors}``
        * ``simulation``: ``get_status()`` of the simulation running now
          (or the last one), or None
        """
        status = self._status.snapshot()
        status["kind"] = "batch"
        with self._batch_lock:
            batch = json.loads(json.dumps(self._batch_info, default=str))
        status["batch"] = batch
        sim = self._active_simulation
        sim_status = None
        getter = getattr(sim, "get_status", None) if sim is not None else None
        if callable(getter):
            try:
                sim_status = getter()
            except Exception:
                logger.debug("Simulation status unavailable", exc_info=True)
        if sim_status is None:
            sim_status = self._last_sim_status
        status["simulation"] = sim_status
        return status

    def _set_batch_info(self, **fields: Any) -> None:
        with self._batch_lock:
            self._batch_info.update(fields)

    def _sim_progress(self, batch_id: str, scenario_id: Optional[str]) -> Callable[[ProgressEvent], None]:
        """Callback for a simulation: tags its events with the batch and forwards them."""
        def _forward(ev: ProgressEvent) -> None:
            if ev.batch_id is None or (ev.scenario_id is None and scenario_id is not None):
                ev = dataclasses.replace(
                    ev,
                    batch_id=ev.batch_id or batch_id,
                    scenario_id=ev.scenario_id or scenario_id,
                )
            self._progress.forward(ev)
        return _forward

    def _run_simulation(self, sim: ATCSimulation) -> Any:
        """Run ``sim`` as the active simulation and keep its final status."""
        self._active_simulation = sim
        run_uuid = getattr(sim, "run_uuid", None)
        if isinstance(run_uuid, str):
            self._simulation_run_uuids.append(run_uuid)
        try:
            return sim.run()
        finally:
            getter = getattr(sim, "get_status", None)
            if callable(getter):
                try:
                    self._last_sim_status = getter()
                except Exception:
                    logger.debug("Simulation status unavailable", exc_info=True)
            self._active_simulation = None

    def _resolve_db_loader(self, db_loader_callback: Optional[Callable[..., Any]]) -> Callable[..., Any]:
        """The loader to use: the callback, the console prompt, or ValueError."""
        if db_loader_callback is not None:
            if not callable(db_loader_callback):
                raise TypeError("db_loader_callback must be callable")
            return db_loader_callback
        interactive = self.interactive
        if interactive is None:
            interactive = _stdin_is_interactive()
        if interactive:
            return console_db_loader
        raise ValueError(
            "db_loader_callback is required when not running interactively: pass a callable "
            "taking a DbLoadRequest (or database_name, target) that returns True once the "
            "database is loaded, or BatchRunner(interactive=True) to prompt on the console"
        )

    def _request_db_load(
        self,
        batch,
        scenario_id: str,
        loader: Callable[..., Any],
        test_type: TestType,
    ) -> None:
        """Emit AWAITING_DB_LOAD, call the loader, and check for a stop before and after."""
        label = f"{test_type.value} scenario {scenario_id}"
        self._raise_if_stopped(label)
        scenario = self._get_scenario(scenario_id)
        order = list(batch.assignments.keys())
        target = batch.assignments[scenario_id]
        request = DbLoadRequest(
            batch_id=batch.batch_id,
            scenario_id=scenario_id,
            database_name=scenario.database_name,
            target=target,
            index=order.index(scenario_id) + 1,
            total=len(order),
            test_type=test_type.value,
        )
        self._progress.emit(
            Stage.AWAITING_DB_LOAD,
            f"Batch {batch.batch_id}: waiting for database {scenario.database_name} "
            f"({scenario_id}) to be loaded on {target} ({request.index}/{request.total})",
            batch_id=batch.batch_id,
            scenario_id=scenario_id,
            index=request.index,
            total=request.total,
            extra={"database_name": scenario.database_name, "target": target,
                   "test_type": test_type.value},
        )
        if not _call_db_loader(loader, request):
            raise RuntimeError(f"Database load callback failed for {scenario_id}")
        self._raise_if_stopped(label)

    def _raise_if_stopped(self, label: str) -> None:
        if self._stop_requested.is_set():
            raise OperationCancelled(f"Cancelled before {label}")

    def _run_log_scope(self):
        """Context that writes run.log for the duration of a run, if enabled."""
        if self.run_log:
            return log_to_file(self.run_log_path)
        return nullcontext()

    def _default_checkpoint(self) -> dict:
        return {
            "suite_name": self.suite.suite_name,
            "software_version": self.suite.software_version,
            "completed_batches": [],
            "batch_members": {},
            "scenario_db_map": {},
            "started_at": datetime.now(timezone.utc).isoformat(),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    def _load_checkpoint(self) -> dict:
        if not self.checkpoint_path.exists():
            return self._default_checkpoint()
        with open(self.checkpoint_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data

    def _save_checkpoint(self, checkpoint: dict) -> None:
        checkpoint["last_updated"] = datetime.now(timezone.utc).isoformat()
        with open(self.checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, indent=2)

    def _get_scenario(self, scenario_id: str) -> TestScenario:
        for scenario in self.suite.scenarios:
            if scenario.scenario_id == scenario_id:
                return scenario
        raise ValueError(f"Scenario '{scenario_id}' not found in suite")

    def _shared_db_path(self) -> Path:
        return self.run_dir / BATCH_DB_NAME

    def _adaptive_latency_lookback_min(self) -> Optional[float]:
        return (
            self.suite.replay_latency_offset_lookback_min
            if self.suite.replay_latency_offset_lookback_min is not None
            else self.suite.replay_latency_offset_update_min
        )

    def _clear_scenario_data(self, db_path: Path, scenario_ids: List[str]) -> None:
        """Delete persisted rows for the provided scenarios from a DuckDB file.

        Works on databases written by any version: a table without a
        ``device_id`` column (for example the 0.x ``simulation_runs``, which
        is keyed by run number only and is migrated when the simulation
        opens the file) is skipped. All deletes run in one transaction.
        """
        if not scenario_ids or not db_path.exists():
            return

        placeholders = ",".join(["?"] * len(scenario_ids))
        con = duckdb.connect(str(db_path))
        try:
            tables = {
                row[0].lower()
                for row in con.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                ).fetchall()
            }
            targets = []
            for table in (
                "events", "conflicts", "comparison_results", "input_detector_events",
                "simulation_runs", "latency_offset_updates", "latency_offset_samples",
            ):
                if table not in tables:
                    continue
                columns = {
                    str(row[1]).lower()
                    for row in con.execute(f"PRAGMA table_info('{table}')").fetchall()
                }
                if "device_id" in columns:
                    targets.append(table)
            con.execute("BEGIN TRANSACTION")
            try:
                for table in targets:
                    con.execute(
                        f"DELETE FROM {table} WHERE device_id IN ({placeholders})",
                        scenario_ids,
                    )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
        finally:
            con.close()

    def _signal_config(self, scenario: TestScenario, target: str) -> SignalConfig:
        """SignalConfig for one scenario on its assigned controller."""
        ip, udp_port, http_port = _parse_assignment(target)
        extra = {
            "scenario_id": scenario.scenario_id,
            "database_name": scenario.database_name,
            "assignment": target,
        }
        extra.update(dict(getattr(scenario, "collection_extra", None) or {}))
        signal_cfg = SignalConfig(
            device_id=scenario.scenario_id,
            ip=ip,
            udp_port=udp_port,
            http_port=http_port,
            incompatible_pairs=scenario.incompatible_pairs,
            cycle_length=scenario.cycle_length,
            cycle_offset=scenario.cycle_offset,
            tod_align=scenario.tod_align,
            replay_latency_offset_seconds=self.suite.replay_latency_offset_seconds,
            collection_extra=extra,
        )
        object.__setattr__(signal_cfg, "events", scenario.events_source)
        return signal_cfg

    def _collection_kwargs(self) -> dict:
        """Output-collection settings shared by every simulation of this runner."""
        return {
            "event_source": self.event_source,
            "final_collection_timeout_seconds": getattr(
                self.suite, "final_collection_timeout_seconds", 900.0
            ),
            "final_collection_poll_seconds": getattr(
                self.suite, "final_collection_poll_seconds", 20.0
            ),
        }

    def _run_similarity_batch(
        self,
        batch,
        similarity_ids: List[str],
        db_loader_callback: Optional[Callable[[str, str], bool]] = None,
    ) -> Optional[Path]:
        if not similarity_ids:
            return None

        label = f"similarity batch {batch.batch_id}"
        self._raise_if_stopped(label)
        loader = self._resolve_db_loader(db_loader_callback)
        for scenario_id in similarity_ids:
            self._request_db_load(batch, scenario_id, loader, TestType.SIMILARITY)
        self._raise_if_stopped(label)

        signals: List[SignalConfig] = []

        for scenario_id in similarity_ids:
            scenario = self._get_scenario(scenario_id)
            signals.append(self._signal_config(scenario, batch.assignments[scenario_id]))

        db_path = self._shared_db_path()
        self._clear_scenario_data(db_path, similarity_ids)

        sim = ATCSimulation(
            signals=signals,
            events=None,
            replays=1,
            stop_on_conflict=False,
            db_path=str(db_path),
            simulation_speed=1.0,
            collection_interval_minutes=self.suite.collection_interval_minutes,
            post_replay_settle_seconds=self.suite.post_replay_settle_seconds,
            snmp_timeout_seconds=self.suite.snmp_timeout_seconds,
            snmp_send_retries=self.suite.snmp_send_retries,
            snmp_retry_backoff_seconds=self.suite.snmp_retry_backoff_seconds,
            show_progress_logs=self.suite.show_progress_logs,
            progress_log_interval_seconds=self.suite.progress_log_interval_seconds,
            replay_latency_offset_lookback_min=self._adaptive_latency_lookback_min(),
            replay_latency_offset_min_samples=self.suite.replay_latency_offset_min_samples,
            comparison_thresholds=self.suite.comparison_thresholds,
            output_dir=str(self.run_dir / PLOTS_DIR_NAME),
            debug=self.debug,
            skip_comparison=True,
            stop_event=self._stop_requested,
            on_progress=self._sim_progress(batch.batch_id, None),
            **self._collection_kwargs(),
        )
        self._raise_if_stopped(label)
        self._set_batch_info(scenario_id=None)
        self._progress.emit(
            Stage.BATCH,
            f"Starting similarity batch {batch.batch_id} with {len(similarity_ids)} scenarios",
            batch_id=batch.batch_id,
            extra={"phase": "scenario_start", "test_type": "similarity",
                   "scenario_ids": list(similarity_ids)},
        )
        result = self._run_simulation(sim)
        _raise_if_simulation_cancelled(result, label)
        _raise_if_simulation_collection_failed(
            result,
            f"similarity batch {batch.batch_id}",
        )
        self._progress.emit(
            Stage.BATCH,
            f"Completed similarity batch {batch.batch_id}",
            batch_id=batch.batch_id,
            extra={"phase": "scenario_complete", "test_type": "similarity",
                   "scenario_ids": list(similarity_ids)},
        )
        return db_path

    def _run_conflict_scenario(
        self,
        batch,
        scenario_id: str,
        db_loader_callback: Optional[Callable[[str, str], bool]] = None,
    ) -> Path:
        scenario = self._get_scenario(scenario_id)
        target = batch.assignments[scenario_id]
        label = f"conflict scenario {scenario_id}"
        self._raise_if_stopped(label)
        loader = self._resolve_db_loader(db_loader_callback)
        self._request_db_load(batch, scenario_id, loader, TestType.CONFLICT)

        signal_cfg = self._signal_config(scenario, target)

        db_path = self._shared_db_path()
        self._clear_scenario_data(db_path, [scenario_id])
        sim = ATCSimulation(
            signals=[signal_cfg],
            events=None,
            replays=scenario.replays,
            stop_on_conflict=True,
            db_path=str(db_path),
            simulation_speed=1.0,
            collection_interval_minutes=self.suite.collection_interval_minutes,
            post_replay_settle_seconds=self.suite.post_replay_settle_seconds,
            snmp_timeout_seconds=self.suite.snmp_timeout_seconds,
            snmp_send_retries=self.suite.snmp_send_retries,
            snmp_retry_backoff_seconds=self.suite.snmp_retry_backoff_seconds,
            show_progress_logs=self.suite.show_progress_logs,
            progress_log_interval_seconds=self.suite.progress_log_interval_seconds,
            replay_latency_offset_lookback_min=self._adaptive_latency_lookback_min(),
            replay_latency_offset_min_samples=self.suite.replay_latency_offset_min_samples,
            comparison_thresholds=self.suite.comparison_thresholds,
            output_dir=str(self.run_dir / PLOTS_DIR_NAME),
            debug=self.debug,
            skip_comparison=True,
            stop_event=self._stop_requested,
            on_progress=self._sim_progress(batch.batch_id, scenario_id),
            **self._collection_kwargs(),
        )
        self._raise_if_stopped(label)
        self._set_batch_info(scenario_id=scenario_id)
        self._progress.emit(
            Stage.BATCH,
            f"Starting conflict scenario {scenario_id}",
            batch_id=batch.batch_id,
            scenario_id=scenario_id,
            extra={"phase": "scenario_start", "test_type": "conflict"},
        )
        result = self._run_simulation(sim)
        _raise_if_simulation_cancelled(result, label)
        _raise_if_simulation_collection_failed(
            result,
            f"conflict scenario {scenario_id}",
        )
        self._progress.emit(
            Stage.BATCH,
            f"Completed conflict scenario {scenario_id}",
            batch_id=batch.batch_id,
            scenario_id=scenario_id,
            extra={"phase": "scenario_complete", "test_type": "conflict",
                   "conflicts_found": len(result.get("conflicts") or []) if isinstance(result, Mapping) else None},
        )
        return db_path

    def stop(self) -> None:
        """Cancel the whole batch run. Thread-safe; returns immediately.

        The active simulation (if any) stops and resets its detectors, the
        partly run batch is cleared and not marked completed, and no further
        scenarios or batches start. :meth:`run` then returns with
        ``cancelled=True`` and :meth:`run_batch_once` raises
        :class:`OperationCancelled`.
        """
        self.logger.warning("Stop requested for batch run")
        self._stop_requested.set()
        self._status.set_state("stopping", stop_reason="user")
        sim = self._active_simulation
        if sim is not None:
            sim.request_stop()

    def reset_stop(self) -> None:
        """Clear a previous :meth:`stop` so this runner can run (or resume) again."""
        self._stop_requested.clear()

    def run_batch_once(
        self,
        batch,
        db_loader_callback: Optional[Callable[..., bool]] = None,
    ) -> Path:
        """Run a single batch without checkpoint-based batch tracking.

        Args:
            batch: The :class:`~signal_replay.TestBatch` to run.
            db_loader_callback: Called once per scenario before it runs,
                either as ``callback(request)`` with a :class:`DbLoadRequest`
                or, for 0.x callers, ``callback(database_name, target)``. It
                returns True once the database is loaded. Without it, see
                the ``interactive`` argument of :class:`BatchRunner`.

        Raises:
            ValueError: No ``db_loader_callback`` and not interactive (raised
                before any data is cleared).
            OperationCancelled: :meth:`stop` (or the shared stop event) cancelled
                the batch. Partial data for the batch has been cleared.
        """
        loader = self._resolve_db_loader(db_loader_callback)
        self._status.start(batch_id=batch.batch_id, index=1, total=1)
        self._set_batch_info(batch_id=batch.batch_id, index=1, total=1, scenario_id=None)
        try:
            with self._run_log_scope():
                path = self._run_batch_once(batch, loader)
        except OperationCancelled as exc:
            self._finish("cancelled", f"Batch {batch.batch_id} cancelled", reason=str(exc))
            raise
        except KeyboardInterrupt:
            self._finish("cancelled", f"Batch {batch.batch_id} cancelled", reason="keyboard_interrupt")
            raise
        except BaseException as exc:
            self._finish("failed", f"Batch {batch.batch_id} failed: {exc}", reason=str(exc))
            raise
        finally:
            self._write_manifest()
        self._finish("completed", f"Batch {batch.batch_id} complete")
        self._write_manifest()
        return path

    def _finish(self, state: str, message: str, reason: Optional[str] = None, **extra: Any) -> None:
        """Set the runner's final state and emit DONE / CANCELLED / ERROR."""
        extra["final"] = True
        if state == "cancelled":
            self._status.set_state("cancelled", stop_reason=reason or "user")
            self._progress.emit(Stage.CANCELLED, message, level=logging.WARNING, log=False, extra=extra)
        elif state == "failed":
            self._status.set_state("failed", error=reason)
            self._progress.emit(Stage.ERROR, message, level=logging.ERROR, log=False, extra=extra)
        else:
            self._status.set_state("completed")
            self._progress.emit(Stage.DONE, message, log=False, extra=extra)

    def _run_batch_once(
        self,
        batch,
        db_loader_callback: Optional[Callable[..., bool]],
    ) -> Path:
        scenario_ids = list(batch.assignments.keys())
        similarity_ids = [
            sid for sid in scenario_ids if self._get_scenario(sid).test_type == TestType.SIMILARITY
        ]
        conflict_ids = [
            sid for sid in scenario_ids if self._get_scenario(sid).test_type == TestType.CONFLICT
        ]

        try:
            self._run_similarity_batch(batch, similarity_ids, db_loader_callback)
            for sid in conflict_ids:
                self._run_conflict_scenario(batch, sid, db_loader_callback)
        except BaseException:
            # Failure, cancel or Ctrl+C: never leave a partial batch behind.
            self._clear_scenario_data(self._shared_db_path(), scenario_ids)
            raise

        return self._shared_db_path()

    def run(
        self,
        db_loader_callback: Optional[Callable[..., bool]] = None,
        batch_ids: Optional[List[str]] = None,
    ) -> dict:
        """Run replay batches.

        Parameters
        ----------
        db_loader_callback : callable, optional
            Called once per scenario before it runs, either as
            ``callback(request)`` with a :class:`DbLoadRequest` or, for 0.x
            callers, ``callback(database_name, target)``. It returns True
            once the database is loaded; False or an exception fails the
            batch. Without it, the ``interactive`` setting decides: console
            prompt, or ValueError before any data is touched.
        batch_ids : list[str], optional
            If provided, only run batches whose ``batch_id`` is in this list.
            Batches already completed (per checkpoint) are still skipped.

        Returns
        -------
        dict
            The checkpoint contents plus ``cancelled`` (bool). A cancelled
            batch is cleared, listed in ``cancelled_batches`` and not in
            ``completed_batches``, so a later run (after :meth:`reset_stop`, or
            with a new runner) runs it again. Ctrl+C is handled the same way
            and then re-raised.

        Raises
        ------
        ValueError
            No ``db_loader_callback`` and not interactive.
        """
        loader = self._resolve_db_loader(db_loader_callback)
        self._status.start()
        try:
            with self._run_log_scope():
                result = self._run(loader, batch_ids)
        except KeyboardInterrupt:
            self._finish("cancelled", "Batch run cancelled", reason="keyboard_interrupt")
            raise
        except BaseException as exc:
            self._finish("failed", f"Batch run failed: {exc}", reason=str(exc))
            raise
        finally:
            self._write_manifest()
        errors = result.get("batch_errors") or {}
        failed = [b for b in self._batches_run if errors.get(b)]
        if result["cancelled"]:
            self._finish("cancelled", "Batch run cancelled", cancelled_batches=result.get("cancelled_batches", []))
        elif failed:
            self._finish(
                "failed", f"Batch run finished with failed batch(es): {', '.join(failed)}",
                reason=f"failed batches: {', '.join(failed)}", failed_batches=failed,
            )
        else:
            self._finish(
                "completed", "Batch run complete",
                completed_batches=result.get("completed_batches", []),
            )
        self._write_manifest()
        return result

    def _run(
        self,
        db_loader_callback: Optional[Callable[..., bool]],
        batch_ids: Optional[List[str]],
    ) -> dict:
        checkpoint = self._load_checkpoint()
        self._batches_run: List[str] = []
        completed = set(checkpoint.get("completed_batches", []))
        batch_members = checkpoint.setdefault("batch_members", {})
        scenario_db_map = checkpoint.get("scenario_db_map", {})

        cancelled_batches = set(checkpoint.get("cancelled_batches", []))
        cancelled = False
        interrupted = False

        selected = [
            batch for batch in self.suite.batches
            if batch_ids is None or batch.batch_id in batch_ids
        ]
        self._set_batch_info(
            batch_id=None, index=None, total=len(selected), scenario_id=None,
            completed_batches=sorted(completed), cancelled_batches=sorted(cancelled_batches),
        )
        for batch_index, batch in enumerate(selected, start=1):
            scenario_ids = list(batch.assignments.keys())
            current_members = sorted(scenario_ids)
            if batch.batch_id in completed and batch_members.get(batch.batch_id) == current_members:
                self._progress.emit(
                    Stage.BATCH,
                    f"Skipping completed batch {batch.batch_id}",
                    batch_id=batch.batch_id, index=batch_index, total=len(selected),
                    extra={"phase": "batch_skipped"},
                )
                continue
            if self._stop_requested.is_set():
                cancelled = True
                self.logger.warning("Batch run cancelled before batch %s", batch.batch_id)
                break
            self._batches_run.append(batch.batch_id)
            self._set_batch_info(batch_id=batch.batch_id, index=batch_index, scenario_id=None)
            self._progress.emit(
                Stage.BATCH,
                f"Starting batch {batch.batch_id} ({batch_index}/{len(selected)})",
                batch_id=batch.batch_id, index=batch_index, total=len(selected),
                extra={"phase": "batch_start", "scenario_ids": scenario_ids},
            )

            similarity_ids = [
                sid for sid in scenario_ids if self._get_scenario(sid).test_type == TestType.SIMILARITY
            ]
            conflict_ids = [
                sid for sid in scenario_ids if self._get_scenario(sid).test_type == TestType.CONFLICT
            ]

            batch_errors = []
            batch_failed = False
            batch_cancelled = False

            try:
                similarity_db = self._run_similarity_batch(batch, similarity_ids, db_loader_callback)
                if similarity_db is not None:
                    for sid in similarity_ids:
                        scenario_db_map[sid] = str(similarity_db)
            except OperationCancelled as exc:
                self.logger.warning("%s", exc)
                batch_cancelled = True
            except KeyboardInterrupt:
                self.logger.warning("Keyboard interrupt during similarity batch %s", batch.batch_id)
                batch_cancelled = interrupted = True
            except Exception as exc:
                self.logger.error("Similarity batch %s failed: %s", batch.batch_id, exc)
                batch_errors.append(f"similarity: {exc}")
                batch_failed = True

            if not batch_failed and not batch_cancelled:
                for sid in conflict_ids:
                    try:
                        conflict_db = self._run_conflict_scenario(batch, sid, db_loader_callback)
                        scenario_db_map[sid] = str(conflict_db)
                    except OperationCancelled as exc:
                        self.logger.warning("%s", exc)
                        batch_cancelled = True
                        break
                    except KeyboardInterrupt:
                        self.logger.warning("Keyboard interrupt during conflict scenario %s", sid)
                        batch_cancelled = interrupted = True
                        break
                    except Exception as exc:
                        self.logger.error("Conflict scenario %s failed: %s", sid, exc)
                        batch_errors.append(f"{sid}: {exc}")
                        batch_failed = True
                        break

            if batch_failed or batch_cancelled:
                self._clear_scenario_data(self._shared_db_path(), similarity_ids)
                for sid in conflict_ids:
                    self._clear_scenario_data(self._shared_db_path(), [sid])
                for sid in scenario_ids:
                    scenario_db_map.pop(sid, None)
            else:
                completed.add(batch.batch_id)

            if batch_cancelled:
                cancelled = True
                cancelled_batches.add(batch.batch_id)
            else:
                cancelled_batches.discard(batch.batch_id)
            checkpoint["cancelled_batches"] = sorted(cancelled_batches)
            checkpoint["completed_batches"] = sorted(completed)
            batch_members[batch.batch_id] = current_members
            checkpoint["batch_members"] = batch_members
            checkpoint["scenario_db_map"] = scenario_db_map
            if batch_errors:
                errors_map = checkpoint.setdefault("batch_errors", {})
                errors_map[batch.batch_id] = batch_errors
            elif "batch_errors" in checkpoint:
                checkpoint["batch_errors"].pop(batch.batch_id, None)
            self._save_checkpoint(checkpoint)
            self._set_batch_info(
                completed_batches=sorted(completed), cancelled_batches=sorted(cancelled_batches),
                errors=dict(checkpoint.get("batch_errors") or {}),
            )

            batch_fields = {"batch_id": batch.batch_id, "index": batch_index, "total": len(selected)}
            if batch_cancelled:
                self._progress.emit(
                    Stage.BATCH,
                    f"Batch {batch.batch_id} cancelled; its partial data was cleared",
                    level=logging.WARNING, extra={"phase": "batch_cancelled"}, **batch_fields,
                )
                if interrupted:
                    raise KeyboardInterrupt
                break
            if batch_errors:
                self._progress.emit(
                    Stage.BATCH,
                    f"Batch {batch.batch_id} failed and was reset: {batch_errors}",
                    level=logging.WARNING,
                    extra={"phase": "batch_failed", "errors": list(batch_errors)}, **batch_fields,
                )
            else:
                self._progress.emit(
                    Stage.BATCH, f"Batch {batch.batch_id} complete",
                    extra={"phase": "batch_complete"}, **batch_fields,
                )

        result = dict(checkpoint)
        result["cancelled"] = cancelled
        return result


def _resolve_run_dir_sources(run_dir: Union[str, os.PathLike], suite: SoftwareTestSuite) -> Dict[str, Tuple[str, str]]:
    """``{scenario_id: ('db', path)}`` for a BatchRunner run directory.

    Uses ``checkpoint.json``'s ``scenario_db_map`` when present (written by
    :meth:`BatchRunner.run`), otherwise the ``run_batch_once`` layout
    ``<run_dir>/collected.db``. Scenarios without a database are left out.
    """
    folder = Path(run_dir)
    shared = folder / BATCH_DB_NAME
    mapping: Dict[str, str] = {}
    checkpoint_path = folder / CHECKPOINT_NAME
    if checkpoint_path.exists():
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)
        for scenario_id, db_path in (checkpoint.get("scenario_db_map") or {}).items():
            path = Path(db_path)
            if not path.exists() and (folder / path.name).exists():
                path = folder / path.name
            mapping[scenario_id] = str(path)
    sources: Dict[str, Tuple[str, str]] = {}
    for scenario in suite.scenarios:
        path_text = mapping.get(scenario.scenario_id)
        if path_text is None and shared.exists():
            path_text = str(shared)
        if path_text is not None and Path(path_text).exists():
            sources[scenario.scenario_id] = ("db", path_text)
    return sources


def compare_software(
    baseline_run_dir: str,
    new_run_dir: str,
    suite: SoftwareTestSuite,
    output_dir: Optional[str] = None,
    max_workers: Optional[int] = None,
    trim_edges_minutes: Optional[float] = None,
    stop_event: Optional[threading.Event] = None,
    on_progress: Optional[ProgressCallback] = None,
    settings: Any = None,
) -> List[ScenarioResult]:
    """Compare a baseline run directory with a new-software run directory.

    A thin wrapper over :func:`~signal_replay.compare_validation`: it finds
    each scenario's collected database in both run directories (from
    ``checkpoint.json``, or ``<run_dir>/collected.db`` as written by
    :meth:`BatchRunner.run_batch_once`) and compares them with the suite's
    analysis settings (settle minutes, analysis window, phase-call
    threshold).

    Args:
        output_dir: When given, plots go to ``<output_dir>/plots`` and the
            results to ``<output_dir>/comparison_results.json``.
        max_workers: Worker processes for the similarity comparisons. None
            uses up to one per CPU; 0 or 1 runs them in this process, one
            after another (the safe choice inside a GUI app or a frozen
            executable).
        trim_edges_minutes: Deprecated and ignored; the suite's settle and
            analysis-window settings are used instead.
        stop_event: Optional ``threading.Event``. When it is set, workers
            are terminated within about 0.25 s and
            :class:`OperationCancelled` is raised.
        on_progress: Optional progress callback: one COMPARE event per
            scenario (``scenario_id``, ``index``, ``total``,
            ``extra['passed']``), then DONE (or CANCELLED). Called on the
            calling thread.
        settings: Optional :class:`~signal_replay.ValidationSettings` (or
            dict) overriding the suite's analysis settings.
    """
    from .validation import ValidationSettings, compare_validation

    if trim_edges_minutes is not None:
        warnings.warn(
            "compare_software(trim_edges_minutes=...) is ignored; the suite's settle and "
            "analysis-window settings apply",
            DeprecationWarning,
            stacklevel=2,
        )
    cfg = settings if settings is not None else ValidationSettings.from_suite(suite)
    plots_dir = Path(output_dir) / PLOTS_DIR_NAME if output_dir else None
    results = compare_validation(
        _resolve_run_dir_sources(baseline_run_dir, suite),
        _resolve_run_dir_sources(new_run_dir, suite),
        suite.scenarios,
        cfg,
        plots_dir=plots_dir,
        max_workers=max_workers,
        on_progress=on_progress,
        stop_event=stop_event,
        baseline_label=str(suite.baseline_version),
        candidate_label=str(suite.software_version),
    )

    if output_dir:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        with open(out / "comparison_results.json", "w", encoding="utf-8") as f:
            json.dump([_serialize_result(r) for r in results], f, indent=2, allow_nan=False)
    return results
