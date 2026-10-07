# Changelog

## 1.0.0 (2026-10-06)

First stable release. From here on, renaming or removing anything in `signal_replay.__all__` bumps the major version; new features bump the minor version; fixes bump the patch version. `tests/test_public_api.py` guards the list.

This entry covers every change since the last release on PyPI, 0.1.0. The intermediate 0.2.0 (research version) and 0.3.0 (tagged on GitHub, never published) are folded in. Applications that embed the package should read the new [integration guide](https://github.com/ShawnStrasser/ATC-Signal-Replay/blob/main/docs/integration.md).

### Breaking changes

- **Validation naming.** The validation workflow tests controller software releases, and the names say so: the suite class is `SoftwareTestSuite` and the comparison function is `compare_software()` (they replace the 0.1.0 class and function of the same role), and the suite's version field, the `settings.json` key and the checkpoint key are `software_version`. The workspace folder is `software_validation/`, its runner is `software_validate.py`, `extract_data.py` takes `--software-version`, the default `SoftwareTestSuite.output_dir` is `./software_test_results`, and the HTML report is titled "Software Validation Report".
- **Removed `save_to_yaml()` and `load_from_yaml()`.** Build the suite in Python (see `software_validation/software_validate.py`).
- **No console output by default.** The package logs through `logging` under the `signal_replay` logger with only a `NullHandler`; library code never calls `print()` (the `print_summary` options of `compare_event_sequences` and `compare_and_visualize` excepted) and never configures handlers. Call `sr.enable_console_logging()` in scripts and notebooks. `BatchRunner` no longer adds console and file handlers when constructed; it copies records to `run.log` only while a run executes (`run_log=False` turns that off).
- **`ATCSimulation.run()` returns a `ReplicationResult`**, not a `dict`. It is a read-only mapping with every 0.x key, so `result['completed_runs']` and `result['conflicts']` (a list of dicts) still work; code that tested `isinstance(result, dict)` should test `collections.abc.Mapping` or use `dict(result)`.
- **`compare_software()` pass/fail** now follows the validation criteria (sequence match at least 95 percent and timing match at least 90 percent) instead of `ComparisonThresholds` and a zero-divergence rule, returns full `ScenarioResult`s in suite order, and no longer needs `checkpoint.json`. `trim_edges_minutes` is deprecated and ignored.
- **`BatchRunner` never blocks on `input()` unless interactive.** Without a `db_loader_callback`, `run()` and `run_batch_once()` use the console prompt only when stdin is a terminal or `interactive=True`, and otherwise raise `ValueError` before touching any data. The console prompt now asks once per scenario instead of once per batch.
- **Comparison internals left the package namespace.** `encode_categorical_sequence`, `compute_dtw`, `create_comparison_gantt_matplotlib`, `create_multi_divergence_plots`, `store_comparison_result`, `find_alignment_offset`, `calculate_timeline_offset` and `compute_timeline_offset` were importable from `signal_replay` without being in `__all__`. They still resolve with a `DeprecationWarning`; import them from `signal_replay.comparison`. `load_events` is now public.
- **`check_conflicts()`** returns three more columns: `Last_TimeStamp`, `Occurrences` and `Duration_Seconds`.
- **Working database schema version 2.** `simulation_runs` is keyed by `(device_id, run_number)` and records run timing; a `meta` table holds versions. Databases written by 0.x are upgraded in place the first time they are opened for writing. Resume counts a run as done only when every device of the simulation has status `completed` or `incomplete`.
- **Every replay resets the detectors it drove** (preempt first) when it ends, including after a stop or error, so a replay now ends with a few more SNMP sends.
- **Dependencies.** `openpyxl` is no longer installed (it is in the `validation` extra); `pyyaml` (`yaml` extra) and `psutil` (`diagnostics` extra) are optional; `numpy` and `duckdb` are declared; every dependency has a lower bound and the major-version caps `pysnmp<8`, `duckdb<2`, `pandas<3`, `dtaidistance<3`, `atspm<3`.
- **Plotting** uses `matplotlib.figure.Figure` instead of `pyplot`; plot functions still return `Figure` objects. The package never selects a matplotlib backend.
- `SimulationConfig.controller_type` is a free-text label; it no longer has to be `'MAXTIME'`.

### Cancellation and safe shutdown

- Ctrl+C stops a run within about a second on Windows (previously it waited for every replay to finish). The run is cleaned up, then `KeyboardInterrupt` is re-raised; the result is kept in `ATCSimulation.last_results`.
- `ATCSimulation(stop_event=...)` and `request_stop(reason=...)` cancel from another thread; `run()` returns with `cancelled`, `cancel_reason`, `cancelled_run` and `stop_reason` (`completed`, `conflict`, `cancelled`, `collection_error`, `all_signals_failed`). After a stop, queued SNMP sends are dropped and a send in progress is cancelled.
- `SignalReplay.run()` called where an event loop is already running (Jupyter, an async app) stops its replay thread and lets it reset the detectors when the call is interrupted, instead of leaving the replay running in the background.
- `compare_validation()` with worker processes no longer waits forever when a worker dies (out of memory, native crash, killed): the scenarios caught in the broken pool are re-run one at a time, and one that kills its worker again gets an error `ScenarioResult`.
- The closing detector reset runs within `detector_reset_timeout_seconds`; `result.detectors_reset` reports per device whether the controller confirmed it.
- A cancelled run is recorded as `cancelled`, skips the settle wait and comparison, and is run again on resume. A fatal collection error stops the replay at once and is recorded as `failed`.
- `BatchRunner.stop()` and `BatchRunner(stop_event=...)` cancel the whole batch run, clear the partial batch and do not mark it completed (`cancelled_batches`; `reset_stop()` to run again). `run_batch_once()`, `compare_software()` and `compare_validation()` raise the new `OperationCancelled`.
- New `SimulationConfig` fields: `stop_grace_seconds`, `detector_reset_timeout_seconds`, `cancel_final_poll_seconds`. The HTTP poll uses a 5 s connect timeout.

### Output events from any controller

- New module `signal_replay.events`: `event_source=` on `SimulationConfig`, `ATCSimulation`, `SoftwareTestSuite` and `BatchRunner` accepts a function `(target, since)` (sync or `async`), or an object with `fetch(target, since)`, that returns the controller's output events as a DataFrame or a list of dicts. The MAXTIME HTTP log stays the default (`MaxtimeHttpEventSource`; `fetch_output_data` is still exported). New exports: `CollectionTarget`, `FetchResult`, `EventSourceError`, `OutputEventSource`, `EventSource`, `MaxtimeHttpEventSource`, `normalize_output_events`, `OUTPUT_EVENT_COLUMNS`.
- Documented output-event schema (`TimeStamp` naive local time, `EventTypeID`, `Parameter`) with column aliases (including `DeviceId`/`TimeStamp`/`EventId`/`Parameter`), device filtering, timezone conversion (`source_timezone`), a per-signal `clock_offset_seconds` and `collection_extra` (passed to the source as `target.extra`; `source_device_id` maps ids).
- Final collection wait: after each replay the package polls until the source reports its events complete through the end of the replay (`FetchResult.complete_through`), up to `final_collection_timeout_seconds` (900 s), every `final_collection_poll_seconds` (20 s). A run whose events are still incomplete is recorded as `incomplete`, listed in `incomplete_runs`, and still checked for conflicts with what arrived.
- `collection_health` and `collection_health_by_run` report polls, failures, rows, first/last timestamps, last error and `complete_through` per device. Any exception from a source is a counted, non-fatal failed poll. A warning is logged once per run when event codes needed for conflict detection or adaptive latency never appear.
- `DataCollector` takes `CollectionTarget`s (the old tuple form still works) and has public `ingest()`, `detect_conflicts()` and `finalize_run()`. The first poll asks for events from 60 s before the run instead of the whole log.
- An `async` source runs on one event loop per source object that lives as long as the object, so a loop-bound client it caches (`httpx.AsyncClient`, an aiohttp session) keeps working across simulations and `BatchRunner` batches.
- For a source that reports `complete_through`, the final collection stores only rows up to the end of the replay plus the settle time, so a log file that runs past the run does not add the controller's free-running events (and any conflict in them) to it. The `since` hint never skips past the `complete_through` the source reported, so late rows in that gap are asked for again. `DataCollector.ingest()` takes `until=`.
- Naive source timestamps in a DST-observing `source_timezone` are no longer dropped (or failing the poll) in the autumn repeated hour or the spring gap: repeated times are resolved from the row order when possible, otherwise taken as daylight time, and a warning gives the count.
- Adaptive latency ends its matching window where the collected output events are complete (`update_once(data_complete_through=...)`), not at the PC clock, and logs a warning at the end of a run when it never adjusted a device's offset.

### Progress reporting and status

- New module `signal_replay.progress` with `Stage`, the frozen `ProgressEvent` dataclass (with `fraction` and `to_dict()`) and the `ProgressCallback` type. `on_progress=` on `ATCSimulation`, `SignalReplay`, `create_replays`, `DataCollector`, `BatchRunner`, `compare_software` and `compare_validation` receives setup, replay progress, collection polls, the final collection wait, conflicts, signal failures, comparison, plots, database-load waits and batch events, then one terminal `done`, `cancelled` or `error` event. Callbacks run on worker threads; an exception in a callback is logged and ignored.
- `ATCSimulation.get_status()` and `BatchRunner.get_status()` return a thread-safe, JSON-safe snapshot (`state`, stage, run and device progress, start-wait countdown, collection health, final collection wait, conflicts, elapsed time).
- New `DbLoadRequest` and `console_db_loader`. A `db_loader_callback` may take a `DbLoadRequest` or the 0.x `(database_name, target)` arguments.

### Results, storage and the validation comparison

- `ReplicationResult`: `run_uuid`, `stop_reason`, `replicated`, `first_conflict_run`, `runs_attempted`, `runs_completed`, `runs` (one `RunRecord` per device per run with replay and source start/end, `date_shift_seconds`, events sent and status), `conflicts` (`ConflictRecord` objects), `comparisons`, `collection_health`, `detectors_reset`, `conflict_store_errors`, `db_path`, `work_dir`.
- `ConflictRecord` gains `last_timestamp`, `occurrences`, `duration_seconds`, `source_equivalent_timestamp` (the moment in the original log), `stored`, `first_timestamp` and `pairs`. A conflict that cannot be written to the database is reported in `conflict_store_errors`.
- `to_dict()` / `from_dict()` on `ComparisonResult`, `DivergenceWindow`, `ScenarioResult`, `ConflictRecord`, `RunRecord` and `ReplicationResult`, always valid for `json.dumps(..., allow_nan=False)`. `DivergenceWindow` carries absolute start and end timestamps for both sides.
- New `results_to_frames(result)` and `FRAME_COLUMNS`: fixed-column, typed DataFrames (`runs`, `conflicts`, `comparison_scores`, `divergence_windows`, `chunk_scores`, `scenario_results`, `scenario_findings`, `plots`), each with `run_uuid`, for an application's own database. Empty frames keep their column types.
- New `compare_validation(baseline, candidate, scenarios, settings, ...)` and `ValidationSettings`: the comparison `software_validate.py` used to carry privately, now the package's single implementation. Inputs are explicit (collected DuckDB path, parquet/CSV log, DataFrame, per-scenario mapping or callable); events are loaded in the calling process with read-only connections. `load_collected_events()` and `load_coord_split_schedules()` are exported. `compare_software(max_workers=0)` and `compare_validation(max_workers=1)` (the default) run in the calling process.
- `work_dir=` on `ATCSimulation` (with `run_log=`) and `BatchRunner`: every file goes under an application-owned folder with fixed names plus `manifest.json` (`read_manifest()`). `BatchRunner` is a context manager with `close()`, and exposes `run_uuid` and `db_path`. `DatabaseManager` gains `read_only=`, `get_runs()`, `update_run_details()` and `get_meta()`/`set_meta()`. Every DuckDB connection is closed in `finally`.
- Resume runs every run up to the target that is not done for some device, including a failed run below the highest done run. A run that is done for some devices only is replayed on every signal but cleared, collected and recorded only for the devices that were not done, so stored results of the others are kept. `ReplicationResult` includes the conflicts already stored for runs done before the call (`replicated` and `first_conflict_run` cover them) and lists those runs in `prior_completed_runs`. New `DatabaseManager.get_done_runs_by_device()`.
- `DatabaseManager(read_only=True)` answers run queries on a database written by 0.x (it presents the old `simulation_runs` table in the per-device layout without migrating the file). `BatchRunner` can clear scenarios from such a database before it is migrated, in one transaction.

### Library hygiene and packaging

- New logging helpers `enable_console_logging()`, `disable_console_logging()` and `log_to_file()`. `atspm` timeline runs no longer print timing lines.
- `log_to_file()` (and `run.log` of `BatchRunner` and `ATCSimulation(work_dir=...)`) writes only the records of its own run, also when several runs execute at once in different threads, and no longer lets INFO records reach the application's own handlers while it is active.
- `reset_all_detectors(raise_on_error=True)` raises `RuntimeError` when the controller does not answer, so it can serve as a connectivity check (by default a failed reset is logged and the call returns).
- `import signal_replay` no longer imports `matplotlib.pyplot`, `atspm`, `requests`, `yaml` or `psutil`; they load when first needed. Plotting is safe from worker threads.
- `TestType`, `TestScenario`, `TestBatch` and `SoftwareTestSuite` set `__test__ = False`, so importing them into a test module does not trigger pytest collection warnings.
- `ATCSimulation.format_summary()` returns the end-of-run summary text.
- `datetime.utcnow()` and `asyncio.get_event_loop()` replaced by their non-deprecated forms.
- Package metadata: SPDX license, classifiers for Python 3.10 to 3.13, `py.typed`, a single version source (`signal_replay.__version__`), and extras `yaml`, `diagnostics`, `validation` and `dev`. Releases publish to PyPI from GitHub with trusted publishing after a tag and version check.
- Live-controller tests are marked `live`, deselected by default, never run in CI, and read their target from `TEST_CONTROLLER_IP` / `TEST_CONTROLLER_HTTP_PORT`.

### Other additions since 0.1.0

- Adaptive per-device replay-latency calibration from sparse detector events (`replay_latency_offset_lookback_min`), for time-of-day aligned replays.
- Comparison validity handling: intervals with missing or unreliable data are excluded rather than reported as behavioral differences.
- Support and documentation for comparing timing-parameter changes with the software held constant.
- Fatal controller-collection errors fail the validation batch instead of appearing as completed runs.
- Offline examples: `examples/offline_comparison/` and `examples/app_integration.py` (worker thread, status polling, cancel, a custom event source and results written to a separate DuckDB file, all without hardware).

### Documentation

- README rewritten for manual use (scripts and notebooks) around the two workflows, with inputs, outputs, a configuration reference, stopping a run, and live-test instructions; links are absolute so they work on PyPI.
- New `docs/integration.md` for applications that embed the package.
- `software_validation/README.md` documents every settings key, catalog column, command-line flag and output file.

### Migration from 0.1.0

1. Rename the validation suite class and comparison function to `SoftwareTestSuite` and `compare_software()`, and the version field and settings key to `software_version`.
2. Add `sr.enable_console_logging()` to scripts that relied on printed progress.
3. Replace `isinstance(result, dict)` checks on `ATCSimulation.run()` results with `Mapping` checks or attribute access.
4. Pass `db_loader_callback=` (or `interactive=True`) to `BatchRunner.run()` when stdin is not a terminal.
5. Install the `validation` extra if you run `software_validation/software_validate.py`.

Existing working databases are upgraded to schema version 2 the first time the package opens them for writing.

## 0.1.0

Initial public release: NTCIP detector replay, MAXTIME output collection, conflict detection, and DTW comparison.
