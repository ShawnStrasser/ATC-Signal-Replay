# Signal-Replay

Replay high-resolution traffic-signal event logs to an ATC controller over NTCIP, collect what the controller does, and compare the result against a baseline.

Use it to:

- **Replicate a field event** (a conflict flash, a preempt bug, a cycle fault) on a bench controller or emulator.
- **Validate a controller software release** by replaying the same field traces to the old and new versions and comparing every phase and overlap event.
- **Validate a configuration change** the same way, with the software held constant.

Input replay uses standard NTCIP 1202 detector, pedestrian, and preempt objects, so it works against any NTCIP 1202 controller or emulator. Reading the controller's output event log is built in for MAXTIME (its HTTP event-log endpoint); for other controllers, pass your own `event_source` function (see "Output events from other controllers" below).

This README covers using the package from scripts and notebooks. To embed it in a desktop or web application (worker threads, progress callbacks and status polling, cancel, app-supplied output events, storing results in your own database), read the [integration guide](https://github.com/ShawnStrasser/ATC-Signal-Replay/blob/main/docs/integration.md).

## Install

```bash
pip install signal-replay          # from PyPI
pip install -e .                   # from a clone of this repository
```

Requires Python 3.10 or newer. The package installs `duckdb`, `pandas`, `pysnmp`, `atspm`, `matplotlib`, and the other runtime dependencies. On Windows, keep the virtual environment in a short folder path (or enable long-path support): `atspm` pulls in `ibis-framework`, whose files have long paths. Optional extras:

```bash
pip install "signal-replay[yaml]"         # PyYAML, for report.load_annotations()
pip install "signal-replay[diagnostics]"  # psutil, memory usage in DEBUG logs
pip install "signal-replay[validation]"   # openpyxl, PyYAML, psutil for software_validation/software_validate.py
```

### Logging

The package reports progress through the standard `logging` module under the `signal_replay` logger and prints nothing on its own. Scripts and notebooks turn the console output on with one call:

```python
import signal_replay as sr
sr.enable_console_logging()                # INFO and above to stderr; call again to change level or stream
```

`sr.disable_console_logging()` turns it off again. To copy the records to a file for one block, use `with sr.log_to_file("run.log"): ...`. You can also attach your own handlers to `logging.getLogger("signal_replay")` with the standard `logging` API.

## Inputs and outputs

| | What | Format |
|---|---|---|
| **Input** | High-resolution event log for one or more intersections | CSV, Parquet, MAXTIME SQLite `.db`, or a pandas DataFrame. Columns `timestamp`, `event_id`, `parameter`, `device_id` (see "Event data format" below). |
| **Input** | Controller target per intersection | IP address, SNMP UDP port (161 for real controllers, per-instance for emulators), HTTP port for log collection |
| **Input** | Optional conflict pairs | Phase/overlap pairs that must never be active together, e.g. `('O5', 'Ph4')` |
| **Output** | DuckDB database | Collected controller events, detected conflicts, stored input events, run status and timing, comparison metrics (see "Storage contract" below) |
| **Output** | Python results | `ReplicationResult` from `ATCSimulation.run()`, `ScenarioResult` lists from `compare_validation()`; all JSON-safe via `to_dict()` and convertible to fixed-column DataFrames with `results_to_frames()` |
| **Output** | Plots and reports | Gantt comparison charts (`.png`) and a self-contained HTML validation report |

Only detector-type events are replayed: vehicle detector off/on (81/82), pedestrian detector off/on (89/90), and preempt on/off (102/104). Everything the controller emits in response (phase, overlap, and pedestrian state changes) is what gets collected and compared.

## Workflow 1: Replay a single study

Replay one intersection's log to one controller, monitor for a conflict, and compare the controller's output to the original log.

```python
import signal_replay as sr

sim = sr.ATCSimulation(
    signals=[
        sr.SignalConfig(
            device_id='2C039',                 # must match the device_id column in the events
            ip='192.0.2.10',                   # controller or emulator address
            udp_port=161,                      # SNMP port (required for 127.0.0.1)
            http_port=80,                      # MAXTIME log endpoint; None disables collection
            incompatible_pairs=[('O5', 'Ph4'), ('O5', 'Ph8')],
        )
    ],
    events='2C039_events.parquet',
    replays=40,                                # repeat the log up to 40 times
    stop_on_conflict=True,                     # stop as soon as a conflict is recorded
    db_path='./2C039_conflict.db',
)

results = sim.run()
```

What happens:

1. Events are loaded, filtered to the signal's `device_id`, and reduced to detector actuations. Missing on/off pairs are imputed.
2. All detector states on the controller are reset over SNMP.
3. Actuations are sent in real time as SNMP SET commands, one bitmask per 8-detector group.
4. A background collector polls the controller's event log every `collection_interval_minutes`, stores events in DuckDB, and checks `incompatible_pairs`.
5. After the last run, each run is compared to the input log with dynamic time warping (DTW) and the summary is logged.

`sim.run()` returns an `sr.ReplicationResult` (also kept as `sim.result`) that answers "did we reproduce the failure, when, and in which run":

- `replicated` (any conflict found), `first_conflict_run`, `stop_reason` (`completed`, `conflict`, `cancelled`, `collection_error` or `all_signals_failed`), `runs_attempted`, `runs_completed`, `run_uuid`.
- `runs`: one `sr.RunRecord` per device per run with `status` (`completed`, `incomplete`, `cancelled`, `failed`), `replay_start`/`replay_end`, `source_start`/`source_end`, `date_shift_seconds`, `events_sent`/`events_total` and `detectors_reset`.
- `conflicts`: `sr.ConflictRecord` objects with the first and last time the conflict state was seen (controller clock), `occurrences`, `duration_seconds`, the conflicting `pairs`, and `source_equivalent_timestamp`: the matching moment in the original log, from the replay's date shift or start offset.
- `comparisons` (`ComparisonResult` list), `collection_health` / `collection_health_by_run`, `detectors_reset`, `conflict_store_errors` (conflicts found but not written to the database), `completed_runs`, `incomplete_runs`, `failed_signals_by_run`, `cancelled`, `cancel_reason`, `cancelled_run`, `db_path`, `work_dir`.

`result.to_dict()` / `sr.ReplicationResult.from_dict()` round-trip through JSON (ISO-8601 timestamps; `inf`/NaN become `null`). For 0.x code the result is also a read-only mapping with the old keys: `result['completed_runs']`, `result['conflicts']` (a list of dicts), `result['stop_reason']` and so on. `sim.get_events()`, `sim.get_conflicts()`, and `sim.get_comparison_results()` return the stored data as DataFrames and objects.

### Stopping a run

Press **Ctrl+C** (or interrupt the notebook cell). The run stops within about a second: replays stop, queued SNMP sends are dropped, every detector and preempt group the replay drove is reset to 0 on the controller (preempt first), and `KeyboardInterrupt` is re-raised. The partial result is in `sim.last_results`.

- Every replay ends with that reset, whether it finished, was stopped or failed. `result.detectors_reset` says per device whether the controller acknowledged it; `False` means check the controller for inputs left ON.
- From another thread, call `sim.request_stop()` (or pass `stop_event=threading.Event()` to `ATCSimulation` and set it). `run()` then returns normally with `cancelled=True` and `stop_reason='cancelled'`.
- A cancelled run skips the settle wait and the comparison, keeps the events collected up to the stop, and is recorded as `cancelled` for each of its devices, not completed, so running the same simulation again against the same `db_path` repeats it. Earlier completed runs are kept.
- For a software-validation batch, `BatchRunner.stop()` (or Ctrl+C) clears the partly run batch so it is replayed next time; `run()` returns `cancelled=True` and `run_batch_once()` raises `sr.OperationCancelled`.

### Quick trial

Check that the controller answers before committing to a long run, then replay only the last few minutes of a log:

```python
import signal_replay as sr

sr.reset_all_detectors(('127.0.0.1', 9701), raise_on_error=True)   # raises RuntimeError if SNMP does not answer

sim = sr.ATCSimulation(
    signals=[
        sr.SignalConfig(
            device_id='13008',
            ip='127.0.0.1', udp_port=9701, http_port=1025,   # emulator SNMP and HTTP ports; real controllers use 161 and 80
            limit_minutes=10,                                # replay only the last 10 minutes of the log
        )
    ],
    events='logs/13008.parquet',
    replays=1,
    db_path='./trial.db',
)
sim.run()
```

The run finishes in about ten minutes and logs the collected event count, any conflicts, and the DTW match against the input (call `sr.enable_console_logging()` first to see it on the console).

### Replay timing modes

| Mode | Setting | Use when |
|---|---|---|
| **Compressed** (default) | `cycle_length=0`, `tod_align=False` | The log is replayed immediately, preserving relative timing. Good for replicating a specific pattern or bug. |
| **Time-of-day aligned** | `tod_align=True` | Each event is sent at the same wall-clock time of day as the original. Required for testing time-of-day plans, and used by the software validation workflow. Replay starts at the current clock time and skips events earlier in the day, so a 23-hour log started at midnight takes 23 hours. |
| **Cycle synchronized** | `cycle_length=120`, `cycle_offset=30` | Coordinated signals: replay starts at the configured offset within the cycle so multiple signals stay in step. Incompatible with `tod_align`. |

### Multiple signals

Pass several `SignalConfig` objects and one events source containing all of them. Events are split by `device_id` and each signal replays in its own thread.

```python
sim = sr.ATCSimulation(
    signals=[
        sr.SignalConfig(device_id='main_1st', ip='127.0.0.1', udp_port=1025, cycle_length=120, cycle_offset=0),
        sr.SignalConfig(device_id='main_2nd', ip='127.0.0.1', udp_port=1026, cycle_length=120, cycle_offset=30),
    ],
    events='all_signals.csv',
    replays=5,
    db_path='./coordination_test.db',
)
```

### Configuration reference

`SignalConfig` (one per intersection):

| Parameter | Default | Description |
|---|---|---|
| `device_id` | required | Identifier matching the `device_id` column of the events |
| `ip` | required | Controller IP address |
| `udp_port` | 161 | SNMP port. Required explicitly for `127.0.0.1` |
| `http_port` | `udp_port` for localhost, 80 otherwise | MAXTIME log endpoint port. `None` disables collection and conflict checking unless an `event_source` is given |
| `clock_offset_seconds` | 0.0 | Added to collected output timestamps to line the controller clock up with this PC's clock |
| `source_timezone` | `None` | Timezone of naive timestamps from the output-event source (for example `'UTC'`). `None` means local time |
| `collection_extra` | `None` | Mapping passed to your `event_source` as `target.extra` |
| `incompatible_pairs` | `None` | Phase/overlap pairs to monitor. `None` disables conflict checking |
| `tod_align` | `False` | Replay at original wall-clock time of day |
| `cycle_length` / `cycle_offset` | 0 / 0.0 | Coordinated start (seconds). `cycle_length=0` disables |
| `limit_minutes` / `buffer_minutes` | 0 / 0 | Replay only the last N minutes, with an optional lead-in |
| `replay_latency_offset_seconds` | 0.1853 | Sends are scheduled this much earlier to cancel measured SNMP latency. `0.0` disables |

`ATCSimulation` keyword arguments:

| Parameter | Default | Description |
|---|---|---|
| `signals`, `events` | required | Signals and the events source (path or DataFrame) |
| `replays` | 1 | Number of replay runs |
| `stop_on_conflict` | `True` | Stop before the next run once a conflict has been recorded |
| `db_path` | `./atc_replay.db` | DuckDB output file (`<work_dir>/replay.duckdb` when `work_dir` is set) |
| `work_dir` | `None` | Folder for every output file (database, plots, `run.log`, `manifest.json`) |
| `run_log` | `False` | With `work_dir`, also write this run's log records to `<work_dir>/run.log` |
| `simulation_speed` | 1.0 | Speed multiplier. Must be 1.0 with `tod_align` |
| `collection_interval_minutes` | 5.0 | How often the collector polls the controller |
| `post_replay_settle_seconds` | 10.0 | Wait before the final collection |
| `event_source` | `None` (MAXTIME HTTP) | Where output events come from; see "Output events from other controllers" below |
| `final_collection_timeout_seconds` | 900 | Longest wait after a replay for the source to report its events complete; after that the run is recorded as `incomplete` |
| `final_collection_poll_seconds` | 20 | Poll interval during that wait |
| `snmp_timeout_seconds`, `snmp_send_retries`, `snmp_retry_backoff_seconds` | 2.0, 0, 0.25 | SNMP send behaviour |
| `replay_latency_offset_lookback_min` | `None` | Enable adaptive per-device latency calibration using the last N minutes of sparse detector events. Requires `tod_align=True` |
| `comparison_thresholds` | defaults | `ComparisonThresholds(sequence_threshold=0.05, timing_threshold=0.02, match_threshold=95.0)` |
| `output_dir` | `None` | Where to write Gantt plots when a comparison exceeds thresholds |
| `skip_comparison` | `False` | Skip the post-replay DTW comparison |
| `show_progress_logs`, `debug` | `False` | Verbosity |

### Output events from other controllers

Conflict detection, latency calibration and the comparisons need the controller's output events with standard Indiana high-resolution event codes. For a controller other than MAXTIME, write a function that returns them and pass it as `event_source`:

```python
import pandas as pd
import signal_replay as sr

def my_events(target, since):
    # target.device_id, target.ip, target.extra; return events at or after `since`
    rows = read_controller_log(target.ip, since)        # your code
    return pd.DataFrame(rows, columns=['TimeStamp', 'EventTypeID', 'Parameter'])

sim = sr.ATCSimulation(signals=[...], events='events.parquet', event_source=my_events)
```

Return a DataFrame or a list of dicts (aliases such as `timestamp`, `EventId` and `param` are accepted); timestamps are the controller's local time unless you wrap the function as `sr.EventSource(my_events, source_timezone='UTC')`. An exception from the function counts as one failed poll and never stops the replay. If the log only becomes available in chunks (files that close every few minutes), return `sr.FetchResult(events, complete_through=...)` and the package waits after each replay, up to `final_collection_timeout_seconds`, until the whole run is available. `BatchRunner(suite, event_source=my_events)` uses the same function for every scenario.

### Query the results

```python
import duckdb
con = duckdb.connect('./2C039_conflict.db', read_only=True)   # after run() has returned
con.execute("SELECT run_number, timestamp, last_timestamp, occurrences, conflict_details FROM conflicts ORDER BY timestamp").df()
con.execute("SELECT * FROM events WHERE run_number = 1 ORDER BY timestamp").df()
con.close()
```

### Storage contract

The working database (`db_path`, or `<work_dir>/replay.duckdb`) is private to one run. Schema version 2 (`sr.SCHEMA_VERSION`, stored in `meta`):

| Table | Contents |
|---|---|
| `events` | Collected controller events: `device_id`, `run_number`, `timestamp`, `event_id`, `parameter` (primary key on all five) |
| `conflicts` | One row per conflict signature per device and run: `timestamp` (first seen), `conflict_details`, `last_timestamp`, `occurrences`, `duration_seconds`, `source_equivalent_timestamp`, `run_uuid` |
| `simulation_runs` | One row per device per run, key `(device_id, run_number)`: `status` (`running`, `completed`, `incomplete`, `cancelled`, `failed`), `started_at`, `completed_at`, `run_uuid`, `replay_start`, `replay_end`, `source_start`, `source_end`, `date_shift_seconds`, `events_sent`, `events_total` |
| `meta` | `schema_version`, `package_version`, `created_at`, latest `run_uuid` |
| `input_events` | Source phase/overlap events kept for comparison |
| `input_detector_events` | Source detector events actually replayed |
| `latency_offset_samples`, `latency_offset_updates` | Adaptive latency calibration history |
| `comparison_results` | DTW metrics per run pair, thresholds, plot path and `run_uuid` (standalone `ATCSimulation` with comparison only) |

A database written by 0.x is upgraded in place when it is opened: each old `simulation_runs` row is copied to every device that has events for that run.

DuckDB allows one writer per file, so open the database (read-only, as above) after `run()` has returned, not while a run is active.

### Working folder and exports

Pass `work_dir=` to `ATCSimulation` or `BatchRunner` to keep every file of a run in one folder with fixed names (`replay.duckdb` for a simulation, `collected.db` and `checkpoint.json` for a batch runner, `plots/`, `run.log`, and `manifest.json`, which `sr.read_manifest()` reads). No file stays open after `run()` returns, so the folder can be copied or deleted.

Every result object has `to_dict()` (JSON-safe) and `from_dict()`. `sr.results_to_frames(result)` turns a `ReplicationResult` or a list of `ScenarioResult` into fixed-column DataFrames (`runs`, `conflicts`, `comparison_scores`, `divergence_windows`, `chunk_scores`, `scenario_results`, `scenario_findings`, `plots`) for writing to Parquet, CSV or another database:

```python
for name, df in sr.results_to_frames(result).items():
    df.to_parquet(f'results_{name}.parquet', index=False)
```

## Workflow 2: Software validation (many intersections, A/B)

The [`software_validation/`](https://github.com/ShawnStrasser/ATC-Signal-Replay/blob/main/software_validation/README.md) folder is a ready-to-use workspace for validating a controller software release. It replays field logs from many intersections to a bank of controllers running the release under test, then compares the collected output to the same logs collected under the baseline release and produces one HTML report.

```
software_validation/
  settings.json                 # copy of settings.example.json, edited for your bench
  databases.xlsx                # catalog: one row per intersection to test
  logs/<TSSU>.parquet           # replay input logs, one per intersection
  databases/<TSSU>.bin          # controller configuration files you load onto the controllers
  conflict_monitor/conflict_pairs.json    # optional: {"<TSSU>": [["O5","Ph4"], ...]}
  results/<software_version>/   # everything the run produces
      collected.db              # DuckDB of collected controller events
      device_events/<TSSU>.csv  # baseline and new events side by side
      divergence_plots/*.png    # Gantt charts of the largest differences
      report.html               # the validation report
```

Run it from that folder:

```bash
py software_validate.py                  # replay the next pending batch, then analyse and report when all are collected
py software_validate.py --report-only    # skip replay: re-run analysis and rebuild the report
py software_validate.py --archive        # also export results/<version>/logs/*.parquet
```

The script picks up to one intersection per configured controller that has not yet been collected, prints which configuration file to load on which controller, waits for you to press Enter, checks SNMP and HTTP connectivity, and replays that batch time-of-day aligned. Run it again the next day for the next batch. Once every catalog row has data in `collected.db`, it compares each intersection against the baseline and writes `report.html`.

For a first trial, use one controller target and one catalog row whose log covers a short window later today (time-of-day alignment skips events already in the past). Give the row a `CycleLength` instead if you want the replay to start immediately rather than at the log's time of day.

Baseline resolution: if `results/<baseline_version>/collected.db` exists, it is the baseline. Otherwise the original field logs in `logs/` are used. To validate the next release, set `baseline_version` to the version you just finished, set `software_version` to the new label, load the new software on the controllers, and run again.

Pass criteria in the report:

- **Similarity scenario** passes when sequence match is at least 95 percent and timing match is at least 90 percent (within 0.5 s), and the input phase-call similarity check did not flag the run as unreliable.
- **Conflict scenario** passes when the baseline reproduces the configured conflict and the new software does not.

Settings keys, catalog columns, and file-naming rules are documented in [`software_validation/README.md`](https://github.com/ShawnStrasser/ATC-Signal-Replay/blob/main/software_validation/README.md).

### Use the validation pieces from Python

`software_validate.py` is a reference runner built only on the public package API. A script or notebook can call the same building blocks directly:

```python
import signal_replay as sr

suite = sr.SoftwareTestSuite(
    suite_name='Release 2.18.1 validation',
    software_version='2.18.1',
    baseline_version='2.15.1',
    output_dir='./results',
    scenarios=[
        sr.TestScenario(
            scenario_id='13008',
            database_name='databases/13008.bin',        # shown to the operator, not uploaded
            events_source='logs/13008.parquet',
            test_type=sr.TestType.SIMILARITY,
            tod_align=True,
        ),
        sr.TestScenario(
            scenario_id='2C039',
            database_name='databases/2C039.bin',
            events_source='logs/2C039.parquet',
            test_type=sr.TestType.CONFLICT,
            replays=25,
            incompatible_pairs=[('O5', 'Ph4'), ('O5', 'Ph8')],
        ),
    ],
    batches=[
        sr.TestBatch(batch_id='day1', assignments={
            '13008': '192.0.2.10:161:80',              # host:udp_port:http_port
            '2C039': '192.0.2.11:161:80',
        }),
    ],
)

# 1. Replay. The callback is called with an sr.DbLoadRequest (batch_id, scenario_id,
#    database_name, target, index, total, test_type) before each scenario; return
#    True once that database is loaded on that controller. (A 0.x callback taking
#    (database_name, target) still works.)
def load_database(request: sr.DbLoadRequest) -> bool:
    print(f'Load {request.database_name} on {request.target}')
    return True

runner = sr.BatchRunner(suite)
checkpoint = runner.run(db_loader_callback=load_database)

# 2. Compare against a previous run of the same suite on the baseline software.
results = sr.compare_software(
    baseline_run_dir='./results/2.15.1',
    new_run_dir='./results/2.18.1',
    suite=suite,
)
# ...or with explicit inputs: a collected DuckDB, a parquet/CSV log or a DataFrame,
# for all scenarios or per scenario ({scenario_id: source}).
results = sr.compare_validation(
    baseline='./results/2.15.1/collected.db',
    candidate='./results/2.18.1/collected.db',
    scenarios=suite.scenarios,
    settings=sr.ValidationSettings.from_suite(suite, group_tolerance=0.25),
    plots_dir='./results/2.18.1/plots',
)

# 3. Report.
sr.generate_report(results, suite, './results/2.18.1/report.html')
for r in results:
    print(r.scenario_id, r.test_type.value, 'PASS' if r.passed else 'FAIL', r.match_percentage)
```

`BatchRunner.run()` writes `checkpoint.json` and `collected.db` under `output_dir/<software_version>/` and skips batches already completed, so it can be resumed. Without a `db_loader_callback` it prompts on the console with `sr.console_db_loader` when stdin is a terminal (or with `BatchRunner(interactive=True)`), and otherwise raises `ValueError` before touching any data.

`sr.compare_validation()` is the one comparison implementation (the same one `software_validate.py` uses). It applies the settle time, the time-of-day analysis window and the phase-call threshold, and returns one `ScenarioResult` per scenario in the given order, with pass/fail, sequence and timing match, phase, clearance and operational differences, chunk scores, a sparkline, issue plots and the underlying `comparison`. Each side can be a collected DuckDB file, a parquet/CSV log, a DataFrame, or a per-scenario mapping of these. `max_workers=1` (the default) compares in the calling process; larger values use worker processes (guard your script with `if __name__ == "__main__":`). `compare_software()` is a thin wrapper that finds each scenario's database from `checkpoint.json` or `<run_dir>/collected.db` and uses the suite's analysis settings. `generate_report()` embeds all plots as base64 so the HTML file is portable.

## Compare logs without a controller

The comparison stage works on any two event logs:

```python
import signal_replay as sr

result = sr.compare_and_visualize(
    events_a='baseline_run.parquet',
    events_b='candidate_run.parquet',
    label_a='2.15.1', label_b='2.18.1',
    output_dir='./plots',              # Gantt chart written when a threshold is exceeded
    match_threshold=95.0,
)
print(result.match_percentage, len(result.divergence_windows))
```

`compare_event_sequences(events_a, events_b, ...)` does the same without plotting. `load_events(path)` reads CSV, Parquet, or a MAXTIME `.db` into a DataFrame. A runnable no-hardware example lives in [`examples/offline_comparison`](https://github.com/ShawnStrasser/ATC-Signal-Replay/blob/main/examples/offline_comparison/README.md).

How the comparison works: phase, overlap, and pedestrian state-change events are grouped by timestamp into sets, the two sequences are auto-aligned, and DTW with Jaccard distance finds the best alignment. Reported metrics are **Match %** (aligned groups with identical event sets), **Sequence DTW** and **Timing DTW** (lower is more similar), divergence windows (where one side has events the other lacks), and timing jitter statistics for matched groups. Intervals with missing or unreliable data on either side are excluded rather than reported as differences.

## Event data format

Column names are matched case-insensitively:

| Column | Accepted names | Notes |
|---|---|---|
| `timestamp` | `TimeStamp`, `time_stamp`, `time` | Event time |
| `event_id` | `EventId`, `EventTypeID`, `event_type_id` | Indiana hi-res event code |
| `parameter` | `Parameter`, `Detector`, `param` | Phase, overlap, or detector number |
| `device_id` | `DeviceId` | Required when the file contains more than one intersection |

Replayed detector events: 81/82 (vehicle off/on), 89/90 (pedestrian off/on), 102/104 (preempt on/off). Detectors numbered 65 and above are ignored. [`ASCControllerEventTypes.csv`](https://github.com/ShawnStrasser/ATC-Signal-Replay/blob/main/ASCControllerEventTypes.csv) lists the event codes.

## Compatibility

- **Replay**: any NTCIP 1202 v3 controller or emulator (vehicle, pedestrian, and preempt detector objects).
- **Collection**: built in for MAXTIME controllers via `http://<ip>:<port>/v1/asclog/xml/full`. Other controllers: supply an `event_source` that returns events with Indiana high-resolution codes. Conflict detection needs phase (1, 10), overlap (61, 63, 65), pedestrian (21, 23) and overlap-pedestrian (67) events for the pairs you monitor; adaptive latency needs detector-on (82). A warning is logged when a needed code never appears.
- **Latency compensation**: the default 185 ms offset came from the study in [`experiments/latency_scaling_runs/report.md`](https://github.com/ShawnStrasser/ATC-Signal-Replay/blob/main/experiments/latency_scaling_runs/report.md). Adaptive per-device calibration is available for time-of-day aligned replays.

## Development

```bash
pip install -e .[dev]
pytest                    # runs tests/ offline; tests marked "live" are deselected
```

### Running the live tests

Tests marked `live` send SNMP and HTTP to a controller, so they never run by default and never run on GitHub. To run them locally against your own bench controller or emulator, set the target in environment variables and select the marker:

```bash
# tests/test_live_integration.py: a controller you are allowed to drive
export TEST_CONTROLLER_IP=192.168.1.100        # or 192.168.1.100:161 for a non-default SNMP port
export TEST_CONTROLLER_HTTP_PORT=80            # MAXTIME event-log HTTP port (default 80)
pytest -m live tests/test_live_integration.py -v -s

# tests/test_live_preempt_56.py: preempt round-trip against emulators
export PREEMPT_TEST_HOST=127.0.0.1             # default 127.0.0.1
export PREEMPT_2B045_PORTS=9701,9702           # comma-separated emulator ports
export PREEMPT_13010_PORTS=9705,9706
pytest -m live tests/test_live_preempt_56.py -v
```

No device address is stored in the repository; the live integration tests skip when `TEST_CONTROLLER_IP` is unset or the device does not answer.

## Citation and license

See [`CITATION.cff`](https://github.com/ShawnStrasser/ATC-Signal-Replay/blob/main/CITATION.cff) for how to cite Signal-Replay. Released under the MIT License, see [LICENSE](https://github.com/ShawnStrasser/ATC-Signal-Replay/blob/main/LICENSE).
