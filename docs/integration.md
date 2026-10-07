# Embedding signal_replay in an application

This guide is for engineers (or agents) building a desktop or web application on top of `signal_replay` 1.x: the user picks a signal and a time period, enters a test controller address, starts a replay, watches progress, may cancel, and the application stores the results in its own database. The [README](https://github.com/ShawnStrasser/ATC-Signal-Replay/blob/main/README.md) covers manual use from scripts and notebooks. A runnable, offline version of everything below is in [`examples/app_integration.py`](https://github.com/ShawnStrasser/ATC-Signal-Replay/blob/main/examples/app_integration.py).

The integration surface is three entry points plus their result types:

| Job | Entry point | Returns |
|---|---|---|
| Failure replication (replay one or more signals, watch for a conflict) | `ATCSimulation(...).run()` | `ReplicationResult` |
| Software validation, replay pass (one pass per software version) | `BatchRunner(suite, ...).run_batch_once(batch)` or `.run()` | path of the pass's `collected.db` / checkpoint dict |
| Software validation, comparison (baseline pass vs candidate pass) | `compare_validation(baseline, candidate, scenarios, settings)` | `list[ScenarioResult]` |

Everything in `signal_replay.__all__` is stable for the 1.x series (see `tests/test_public_api.py`).

## Ground rules

- **Run on a worker thread.** `run()`, `run_batch_once()` and `compare_validation()` block for minutes to days. Call them from a background `threading.Thread`, never from the UI or request thread. Build `ATCSimulation` on that worker thread too: its constructor creates the working database and stores the input events.
- **One working folder per run.** Pass `work_dir=` and let the package own that folder. Never point the package at the application's own DuckDB file (see [Files and locks](#files-and-locks)).
- **Nothing prints, nothing prompts.** The package logs under the `signal_replay` logger with only a `NullHandler`, never calls `print()` in library code, and never calls `input()` unless you ask for the console loader. Plotting uses matplotlib's object API (no `pyplot`), so it is safe on worker threads and never changes your backend.
- **Device ids are strings.** `SignalConfig.device_id` and every `device_id` in results are strings. An integer `DeviceId` in your input events or polled rows is matched by its string form (`1234` matches `'1234'`).
- **Timestamps are naive local time of the PC running the replay.** Replay times, conflict times and collected events are all on that clock.

## Failure replication on a worker thread

```python
import threading, queue
import signal_replay as sr

class ReplicationJob:
    def __init__(self, work_dir, device_id, input_events, ip, port, pairs, event_source):
        self.stop_event = threading.Event()          # Cancel button sets this
        self.progress = queue.Queue()                # UI drains this
        self.sim = None
        self.result = None
        self.error = None
        self._args = (work_dir, device_id, input_events, ip, port, pairs, event_source)
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        work_dir, device_id, input_events, ip, port, pairs, event_source = self._args
        try:
            self.sim = sr.ATCSimulation(
                signals=[sr.SignalConfig(
                    device_id=str(device_id), ip=ip, udp_port=port,
                    http_port=None,                  # events come from event_source
                    incompatible_pairs=pairs,        # e.g. [('O5', 'Ph4')]
                )],
                events=input_events,                 # DataFrame: DeviceId, TimeStamp, EventId, Parameter
                replays=25,
                stop_on_conflict=True,
                event_source=event_source,
                stop_event=self.stop_event,
                on_progress=self.progress.put,
                work_dir=work_dir,
            )
            self.result = self.sim.run()             # ReplicationResult
        except Exception as exc:
            self.error = exc

    def get_status(self):                            # for GET /status
        return self.sim.get_status() if self.sim else {"state": "starting"}

    def cancel(self):                                # for POST /cancel
        self.stop_event.set()
```

Input events are the source log for the period the user picked, as a DataFrame (or a CSV/Parquet path) with a device id column. Column names are matched without regard to case: `TimeStamp`/`timestamp`, `EventId`/`EventTypeID`/`event_id`, `Parameter`/`parameter`/`Detector`, `DeviceId`/`device_id`. Only detector events (81/82, 89/90, 102/104) are replayed; phase and overlap events in the same frame are kept as the comparison reference. Filter the frame to the period yourself (or use `SignalConfig.limit_minutes`).

Replay timing: the default replays immediately with the original relative timing. Use `SignalConfig(tod_align=True)` when the failure depends on a time-of-day plan; the replay then waits until the PC clock reaches the log's time of day (shown as a `seconds_until_start` countdown).

`ReplicationResult` answers the replication question: `replicated`, `first_conflict_run`, `stop_reason` (`completed`, `conflict`, `cancelled`, `collection_error`, `all_signals_failed`), `conflicts` (`ConflictRecord` with `timestamp`, `last_timestamp`, `occurrences`, `duration_seconds`, `conflict_details`, `pairs` and `source_equivalent_timestamp`, the matching moment in the original log), `runs` (one `RunRecord` per device per run), `collection_health`, `detectors_reset`, `run_uuid`. It is also a read-only mapping with the 0.x dict keys.

## Progress: `on_progress` and `get_status()`

Use either or both.

**Callback.** `on_progress=` on `ATCSimulation`, `BatchRunner`, `compare_validation` and `compare_software` receives frozen `sr.ProgressEvent` objects: `stage` (`sr.Stage`), ASCII `message`, logging `level`, and depending on the stage `run_number`/`total_runs`, `device_id`, `events_sent`/`events_total`, `seconds_until_start`, `batch_id`, `scenario_id`, `index`/`total`, `extra`. `event.fraction` is progress in [0, 1] where it applies; `event.to_dict()` is JSON-safe (for SSE or a websocket).

- A simulation emits `setup`, `store_input`, then per run `run_start`, `detector_reset`, `waiting`, `replay` (throttled to about one per second per device, always at 0 and at the end), `collect` (one per device per poll; `extra['rows']` is new rows), `final_collection`, `conflict`, `signal_failed`, `run_complete`; then `compare` and `plot`; finally exactly one of `done`, `cancelled` or `error`.
- `BatchRunner` forwards every simulation event with `batch_id` filled in and adds `batch` (start and end of batches and scenarios, kind in `extra['phase']`) and `awaiting_db_load`.
- `compare_validation` emits one `compare` per scenario (`index`, `total`, `extra['passed']`) and then `done` or `cancelled`.

Threading contract: callbacks run on the package's worker threads (replay threads, the collection thread, the thread that called `run()`), never on your UI thread. Return quickly; marshal to the UI with `queue.put(event)`, a Qt signal, or similar. An exception raised by a callback is logged with `logger.exception` and ignored, so it can never stop a replay. `compare_validation` calls it on the calling thread.

**Polling.** `sim.get_status()` and `runner.get_status()` are thread-safe, cheap, and return a JSON-safe dict, suitable for a `/status` endpoint polled every second or two:

| Key | Meaning |
|---|---|
| `state` | `idle`, `running`, `stopping`, `cancelled`, `completed`, `failed` |
| `stage`, `message`, `level`, `updated_at` | latest event |
| `run_number`, `total_runs` | position |
| `devices` | per device: `stage`, `events_sent`, `events_total`, `seconds_until_start` (live countdown), `message` |
| `seconds_until_start` | longest remaining start wait, or None |
| `collection` | per device health of the latest poll: `rows`, `total_rows`, `polls`, `failures`, `consecutive_failures`, `complete_through`, `last_success`, `last_error`, `degraded` |
| `final_collection` | while waiting for complete output events: `waiting`, `needed`, `devices` (`complete_through` per device), `seconds_left` |
| `conflicts_found`, `conflicts` | count and the latest 100 |
| `failed_signals`, `stop_reason`, `error` | |
| `started_at`, `finished_at`, `elapsed_seconds` | |

`runner.get_status()` adds `batch` (`batch_id`, `index`, `total`, `scenario_id`, `completed_batches`, `cancelled_batches`, `errors`) and `simulation` (the active simulation's status). Read the batch position from `status['batch']`; the top-level `index`/`total` can be overwritten by forwarded simulation events.

**Logs.** For a log pane, attach your own handler to `logging.getLogger("signal_replay")`. The logger is process-wide and records carry no job id, so use progress events for anything per-job. `BatchRunner(run_log=True)` (the default) also copies records to `<work_dir>/run.log` while a run executes; `ATCSimulation(run_log=True)` does the same. Pass `run_log=False` if you do not want the file.

## Cancelling

Cancel with the shared event or the object's method; both are thread-safe and return immediately:

```python
job.stop_event.set()        # or sim.request_stop() / runner.stop()
```

What happens:

1. Replays stop within about 0.25 s. Queued SNMP sends are dropped and a send in progress is abandoned.
2. Every detector and preempt group the replay drove is reset to 0 on the controller (preempt first), within `detector_reset_timeout_seconds` (5 s). `result.detectors_reset[device_id]` is True when the controller confirmed it; **False means check the controller for inputs left ON.**
3. One last collection poll (capped at `cancel_final_poll_seconds`, 3 s) keeps the events up to the stop. The settle wait, the final collection wait, conflict detection on the partial run and the comparison are skipped.
4. `run()` returns normally, usually within a second (at most about `stop_grace_seconds + detector_reset_timeout_seconds`, 8 + 5 s): `result.cancelled is True`, `stop_reason == 'cancelled'`, `cancel_reason`, `cancelled_run`. `get_status()['state']` becomes `cancelled` and a final `cancelled` progress event is sent.
5. The interrupted run is recorded per device as `cancelled` in the working database's `simulation_runs`, never `completed`, so a resume against the same `work_dir` runs it again. Earlier finished runs stay `completed`. A resume runs every run up to `replays` that is not done for some device (also one below the highest done run); a run that is done for some devices only is replayed on every signal but recorded only for the others, and conflicts already stored for done runs are included in the new result.

A cancel during the final collection wait (which can last up to `final_collection_timeout_seconds` for file-based sources) also returns within about 2 s.

`BatchRunner`: setting its `stop_event` (or `runner.stop()`) cancels the whole pass. The active simulation stops as above, the partly run batch is removed from `collected.db` and listed in `cancelled_batches` (never `completed_batches`), no further scenario starts, `run()` returns `{'cancelled': True, ...}` and `run_batch_once()` raises `sr.OperationCancelled`. The event stays set; call `runner.reset_stop()` (or use a new runner) before running again. `compare_validation(stop_event=...)` stops between scenarios (terminating worker processes, if any, within about 0.25 s) and raises `sr.OperationCancelled`.

Ctrl+C in a console app is handled the same way, then `KeyboardInterrupt` is re-raised (the result is in `sim.last_results`).

## Supplying output events (`event_source`)

The package needs the test controller's own high-resolution log (its output events) to detect conflicts, calibrate latency and compare runs. MAXTIME's HTTP event log is built in (`http_port=`). If your application already polls controllers, give the package a source function and it will never open its own HTTP connection:

```python
def poll_events(target: sr.CollectionTarget, since):
    # target.device_id ('1234'), target.ip, target.http_port, target.extra (read-only mapping)
    rows = my_poller.fetch(controller_for(target), since=since)   # list of dicts
    return rows        # [{'DeviceId': 1234, 'TimeStamp': dt, 'EventId': 1, 'Parameter': 2}, ...]

sim = sr.ATCSimulation(..., event_source=poll_events)
```

Contract:

- **Signature** `(target, since)`. `since` is a naive datetime hint: the package already holds every event before it, so returning more is harmless. On the first poll of a run it is 60 s before the run started; after that it trails the newest stored event by a small overlap, or the `complete_through` you last reported when that is earlier (so rows you return beyond `complete_through` do not make the package skip the gap before them).
- **Return** a DataFrame or a list of dicts. Columns are matched without regard to case: `TimeStamp` (`timestamp`, `time`, ...), `EventTypeID` (`EventId`, `event_id`, `eventcode`, ...), `Parameter` (`param`, ...). Extra columns are ignored. If a `DeviceId`/`device_id` column is present, rows for other devices are dropped; when your id differs from `target.device_id`, put it in `SignalConfig.collection_extra={'source_device_id': 1234}` (or `TestScenario.collection_extra`). Rows need not be sorted; re-delivered rows are de-duplicated. Event codes must be Indiana high-resolution codes.
- **Sync or async.** A plain function runs on a short-lived helper thread per call. An `async def` function (or an object with an async `fetch`) runs on a private event loop that the package owns, on its own thread. There is one loop per source object (the function, or the object with `fetch`), and it lives as long as that object, so every simulation and batch that is given the same source uses the same loop. It is **not** your application's loop, so do not reuse a loop-bound client (for example an `httpx.AsyncClient`) created on another loop. Create the client lazily inside the source on first use and cache it on the source object (it then stays valid across simulations), or wrap your fetch in a sync function that submits it to your own loop with `asyncio.run_coroutine_threadsafe(...).result()`.
- **Errors** are non-fatal: any exception counts as one failed poll in `collection_health` (raise `sr.EventSourceError` for expected ones such as "controller offline"); the next poll tries again. A single call that takes longer than 120 s is abandoned and counted as a failure.
- **Credentials** stay in your application. The package passes only `target` and `since`.
- **Options** can be attached with `sr.EventSource(fn, ordered=False, source_timezone=None, name=None)` or as attributes of a source object. `ordered=True` means rows arrive in append order and never late.

### File-based logs (McCain Omni) and `complete_through`

Some controllers publish their log as files that close every few minutes, so the last few minutes are not available until the current file closes. Return a `FetchResult` saying how far your data is complete:

```python
def poll_omni(target, since):
    rows, max_end_ts = omni_fetch_closed_files(target, since)   # your SFTP + parse_dat_files
    return sr.FetchResult(rows, complete_through=max_end_ts)

source = sr.EventSource(poll_omni, source_timezone="UTC")      # Omni .dat timestamps are naive UTC
```

After each replay (and `post_replay_settle_seconds`), the package keeps polling every `final_collection_poll_seconds` (20 s) until every device's `complete_through` reaches the end of the replay, then checks conflicts on the complete run. Rows your source returns for times after that target (the rest of the last file) are not stored for the run. If that has not happened within `final_collection_timeout_seconds` (900 s), the run is recorded as `incomplete` (listed in `result.incomplete_runs`, still checked for conflicts on what arrived, and not re-run on resume). Plain return values (no `FetchResult`) count as complete up to the moment the call started, which suits a live HTTP log. Size the timeout to your file period plus transfer time. Progress during the wait is reported as `final_collection` events and in `get_status()['final_collection']`.

### Timezones and clocks

The package works in naive PC-local time. Timezone-aware timestamps are converted to local time and made naive. Naive timestamps are taken as local time unless the source declares `source_timezone` (on `EventSource`, as an attribute of a source object, or per signal with `SignalConfig.source_timezone`, which wins). The same timezone applies to `since` (the package converts it before calling you) and to `complete_through`. If the controller clock is off from the PC clock, set `SignalConfig.clock_offset_seconds` (seconds added to the controller's timestamps).

A warning is logged once per run when event codes needed by an enabled feature never appear (conflict detection: phase 1/10, overlap 61/63/65, pedestrian 21/23, overlap-pedestrian 67/65 for the pairs you monitor; adaptive latency: 82), or when no rows arrive at all.

## Software validation

The validation question is "does the new controller software behave like the old one on the same field traffic?". Each pass replays the same scenarios, time-of-day aligned, to the controller running one software version; the comparison then lines the two passes up.

Recommended flow for an application with one test controller:

1. **Baseline pass** on the current software, for every scenario, into its own folder.
2. **Upgrade** the controller software.
3. **Candidate pass** on the new software, same scenarios, into a second folder.
4. **Compare** the two passes with `compare_validation`.

```python
suite = sr.SoftwareTestSuite(
    suite_name="Upgrade 2.15.1 -> 2.18.1",
    software_version="2.15.1",              # the version this pass runs on
    baseline_version="2.15.1",
    scenarios=[
        sr.TestScenario(
            scenario_id="1234", database_name="1234.bin",
            events_source=str(work_root / "inputs" / "1234.parquet"),   # write the picked period here
            test_type=sr.TestType.SIMILARITY, tod_align=True,
            collection_extra={"source_device_id": 1234},
        ),
    ],
    batches=[sr.TestBatch(batch_id="1234", assignments={"1234": "10.0.0.5:161:80"})],
    event_source=poll_events,
)

def load_database(req: sr.DbLoadRequest) -> bool:
    # Ask the user (through the UI) to load req.database_name on req.target, then
    # wait. Return False to abort. Check the stop event while waiting.
    while not ui_confirmed(req).wait(0.5):
        if runner.stop_event.is_set():
            return False
    return True

with sr.BatchRunner(suite, work_dir=work_root / "pass_2.15.1", stop_event=stop,
                    on_progress=progress.put, interactive=False, run_log=False) as runner:
    for batch in suite.batches:
        runner.run_batch_once(batch, db_loader_callback=load_database)
```

Then the candidate pass is the same with `software_version="2.18.1"` and `work_dir=work_root / "pass_2.18.1"`, and:

```python
results = sr.compare_validation(
    baseline=str(work_root / "pass_2.15.1" / "collected.db"),
    candidate=str(work_root / "pass_2.18.1" / "collected.db"),
    scenarios=suite.scenarios,
    settings=sr.ValidationSettings(settle_minutes=10, baseline_label="2.15.1",
                                   candidate_label="2.18.1"),
    plots_dir=work_root / "plots",          # or None for no plots
    stop_event=stop, on_progress=progress.put,
)
```

Notes:

- **Database loads.** The runner calls `db_loader_callback` once per scenario, on the thread that called `run_batch_once()`/`run()`, before replaying it, with an `sr.DbLoadRequest` (`batch_id`, `scenario_id`, `database_name`, `target`, `index`, `total`, `test_type`), preceded by an `awaiting_db_load` progress event. Return True once the controller is ready; False or an exception aborts and clears the batch. The package cannot interrupt a blocking callback, so wait in short steps and check `runner.stop_event` as above. With `interactive=False` and no callback, `run()`/`run_batch_once()` raise `ValueError` before touching any data; `sr.console_db_loader` (an `input()` prompt) is only for console use.
- **Scenarios and batches.** `TestScenario.scenario_id` is the package device id. `assignments` maps scenario id to `host`, `host:port` or `host:udp_port:http_port`. Similarity scenarios replay once, time-of-day aligned (a 23-hour log takes 23 hours of wall time). Conflict scenarios (`test_type=sr.TestType.CONFLICT`, `replays`, `incompatible_pairs`) run up to `replays` times and stop at the first conflict; they pass when the baseline reproduced the conflict and the candidate never did.
- **Resume.** `run_batch_once()` replays the batch it is given. `run()` keeps `checkpoint.json` in the work folder and skips batches already completed; a cancelled or failed batch is cleared and run again next time.
- **Inputs to `compare_validation`.** Each side can be a collected DuckDB path, a parquet/CSV log, a DataFrame, a per-scenario mapping `{scenario_id: source}`, or a callable `scenario_id -> DataFrame`. Events are read in the calling process with read-only connections. A scenario missing on either side gets `passed=False` and `error` set. Pass/fail for similarity scenarios: sequence match at least `sequence_match_threshold` (95) and timing match at least `timing_match_threshold` (90) percent, unless the scenario was thrown out because the replayed inputs did not match the source well enough (`thrown_out`, `phase_call_threshold`).
- **Workers.** `compare_validation(max_workers=1)` (the default) compares in the calling process, which is the right choice inside a GUI or a frozen executable. With `max_workers > 1` it uses a `multiprocessing` pool; a frozen Windows app must then call `multiprocessing.freeze_support()` first thing in its `if __name__ == "__main__":` block.

## Files and locks

`work_dir=` makes the package write only inside a folder you own, with fixed names:

| Object | Files in `work_dir` |
|---|---|
| `ATCSimulation` | `replay.duckdb`, `plots/` (when a comparison plot is drawn), `run.log` (with `run_log=True`), `manifest.json` |
| `BatchRunner` | `collected.db`, `checkpoint.json` (written by `run()`), `run.log` (unless `run_log=False`), `manifest.json` |

`sr.read_manifest(work_dir)` returns `run_uuid`, `package_version`, `schema_version` and the file list. Nothing is derived from the current directory.

DuckDB allows one read-write connection per file per process, and refuses a read-only connection from the same process while a read-write one is open. Therefore:

- **Never** pass your application's database as `db_path` or put it in `work_dir`. The package's database is private to one run (or one validation pass).
- Do not open the working database while a run is active. Follow the run with `on_progress` and `get_status()`.
- After `run()` returns (or after `BatchRunner.close()` / leaving its `with` block) no package file is open. Then read the working database with `read_only=True` (`sr.load_collected_events(db_path, scenario_id)`, `sr.DatabaseManager(path, read_only=True)` or `duckdb.connect(path, read_only=True)`), copy what you keep, and delete the folder.
- Run concurrent jobs with separate `work_dir`s.

## Storing results in your own DuckDB

All result types serialise with `to_dict()` (ISO-8601 timestamps; `inf`/NaN become None; always valid for `json.dumps(..., allow_nan=False)`) and come back with `from_dict()`: `ReplicationResult`, `RunRecord`, `ConflictRecord`, `ScenarioResult`, `ComparisonResult`, `DivergenceWindow`. Store the JSON if you want the whole object.

For tables, `sr.results_to_frames(result)` returns a dict of DataFrames with fixed columns (`sr.FRAME_COLUMNS`), every one with `run_uuid`: `runs`, `conflicts`, `comparison_scores`, `divergence_windows`, `chunk_scores`, `scenario_results`, `scenario_findings` (long format, item as JSON text in `payload`) and `plots`. Every frame is always returned, empty when it does not apply, and has real column types even when empty, so you can create tables from it:

```python
frames = sr.results_to_frames(result)          # ReplicationResult or list[ScenarioResult]
con = app_duckdb_connection()                  # your own connection / cursor
for name, df in frames.items():
    con.register("df", df)
    con.execute(f"CREATE TABLE IF NOT EXISTS replay_{name} AS SELECT * FROM df LIMIT 0")
    con.execute(f"INSERT INTO replay_{name} SELECT * FROM df")
    con.unregister("df")
```

Add your own job id or signal id columns as needed; `run_uuid` ties the rows of one call together. To keep the raw collected events as well, read them after the run with `sr.load_collected_events(db_path, scenario_id)` (columns `device_id`, `run_number`, `timestamp`, `event_id`, `parameter`).

Plot files listed in the `plots` frame live in the package's work folder; copy them before deleting it.

## Packaging notes

- Install: `pip install signal-replay` (the extras are only for the bundled scripts). `atspm` and `requests` are imported lazily, only when the timeline analysis or the built-in MAXTIME source runs; `import signal_replay` loads neither, nor `matplotlib.pyplot`.
- `atspm` depends on `ibis-framework`, whose deep test paths can exceed the Windows 260-character path limit when a venv lives in a deep folder. Keep the venv path short or enable Windows long-path support.
- PyInstaller: the SQL templates in `signal_replay/sql/*.sql` are package data; include them (for example `collect_data_files("signal_replay")`). Check the frozen build once against an emulator; libraries that load modules dynamically (such as `pysnmp`) may need their submodules collected too.
- Version pins that are known to work together: `duckdb` 1.1 to 1.x, `pandas` 2.x, `atspm` 2.5.x, Python 3.10 to 3.13.
