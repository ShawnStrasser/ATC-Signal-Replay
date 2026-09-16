# Signal-Replay

Replay high-resolution traffic-signal event logs to an ATC controller over NTCIP, collect what the controller does, and compare the result against a baseline.

Use it to:

- **Replicate a field event** (a conflict flash, a preempt bug, a cycle fault) on a bench controller or emulator.
- **Validate a controller software release** by replaying the same field traces to the old and new versions and comparing every phase and overlap event.
- **Validate a configuration change** the same way, with the software held constant.

Input replay uses standard NTCIP 1202 detector, pedestrian, and preempt objects, so it works against any NTCIP controller or emulator. Output collection currently reads the MAXTIME HTTP event-log endpoint; other controller families need a collector adapter.

## Install

```bash
pip install signal-replay          # from PyPI
pip install -e .                   # from a clone of this repository
```

Requires Python 3.10 or newer. The package installs `duckdb`, `pandas`, `pysnmp`, `atspm`, `matplotlib`, and the other runtime dependencies.

## Inputs and outputs

| | What | Format |
|---|---|---|
| **Input** | High-resolution event log for one or more intersections | CSV, Parquet, MAXTIME SQLite `.db`, or a pandas DataFrame. Columns `timestamp`, `event_id`, `parameter`, `device_id` (see [Event data format](#event-data-format)). |
| **Input** | Controller target per intersection | IP address, SNMP UDP port (161 for real controllers, per-instance for emulators), HTTP port for log collection |
| **Input** | Optional conflict pairs | Phase/overlap pairs that must never be active together, e.g. `('O5', 'Ph4')` |
| **Output** | DuckDB database | Collected controller events, detected conflicts, stored input events, comparison metrics |
| **Output** | Python results | `dict` from `ATCSimulation.run()`, `ComparisonResult` objects, `ScenarioResult` lists |
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
5. After the last run, each run is compared to the input log with dynamic time warping (DTW) and the summary is printed.

`sim.run()` returns a dictionary with `completed_runs`, `conflicts`, `failed_signals_by_run`, `stopped_early`, `collection_error`, and `comparison_summary`. `sim.get_events()`, `sim.get_conflicts()`, and `sim.get_comparison_results()` return the same data as DataFrames and objects.

### Quick trial

Check that the controller answers before committing to a long run, then replay only the last few minutes of a log:

```python
import signal_replay as sr

sr.reset_all_detectors(('127.0.0.1', 9701))     # raises if SNMP does not answer

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

The run finishes in about ten minutes and prints the collected event count, any conflicts, and the DTW match against the input.

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
| `http_port` | `udp_port` for localhost, 80 otherwise | MAXTIME log endpoint port. `None` disables collection and conflict checking |
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
| `db_path` | `./atc_replay.db` | DuckDB output file |
| `simulation_speed` | 1.0 | Speed multiplier. Must be 1.0 with `tod_align` |
| `collection_interval_minutes` | 5.0 | How often the collector polls the controller |
| `post_replay_settle_seconds` | 10.0 | Wait before the final collection |
| `snmp_timeout_seconds`, `snmp_send_retries`, `snmp_retry_backoff_seconds` | 2.0, 0, 0.25 | SNMP send behaviour |
| `replay_latency_offset_lookback_min` | `None` | Enable adaptive per-device latency calibration using the last N minutes of sparse detector events. Requires `tod_align=True` |
| `comparison_thresholds` | defaults | `ComparisonThresholds(sequence_threshold=0.05, timing_threshold=0.02, match_threshold=95.0)` |
| `output_dir` | `None` | Where to write Gantt plots when a comparison exceeds thresholds |
| `skip_comparison` | `False` | Skip the post-replay DTW comparison |
| `show_progress_logs`, `debug` | `False` | Verbosity |

### Query the results

```python
import duckdb
con = duckdb.connect('./2C039_conflict.db')
con.execute("SELECT run_number, timestamp, conflict_details FROM conflicts ORDER BY timestamp").df()
con.execute("SELECT * FROM events WHERE run_number = 1 ORDER BY timestamp").df()
```

<details>
<summary>Database tables</summary>

| Table | Contents |
|---|---|
| `events` | Collected controller events: `device_id`, `run_number`, `timestamp`, `event_id`, `parameter` |
| `conflicts` | Detected incompatible-pair activations with `conflict_details` |
| `input_events` | Source phase/overlap events kept for comparison |
| `input_detector_events` | Source detector events actually replayed |
| `simulation_runs` | One row per run with status and timing |
| `latency_offset_samples`, `latency_offset_updates` | Adaptive latency calibration history |
| `comparison_results` | DTW metrics per run pair, thresholds, and plot paths (created on first comparison) |

</details>

## Workflow 2: Software validation (many intersections, A/B)

The [`software_validation/`](software_validation/README.md) folder is a ready-to-use workspace for validating a controller software release. It replays field logs from many intersections to a bank of controllers running the release under test, then compares the collected output to the same logs collected under the baseline release and produces one HTML report.

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

Settings keys, catalog columns, and file-naming rules are documented in [`software_validation/README.md`](software_validation/README.md).

### Use the validation pieces from your own application

`software_validate.py` is a reference runner built only on the public package API. An application can call the same building blocks directly:

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

# 1. Replay. The callback is called with (database_name, target) before each scenario;
#    return True once the configuration is loaded on that controller.
runner = sr.BatchRunner(suite)
checkpoint = runner.run(db_loader_callback=lambda db_name, target: True)

# 2. Compare against a previous run of the same suite on the baseline software.
results = sr.compare_software(
    baseline_run_dir='./results/2.15.1',
    new_run_dir='./results/2.18.1',
    suite=suite,
)

# 3. Report.
sr.generate_report(results, suite, './results/2.18.1/report.html')
for r in results:
    print(r.scenario_id, r.test_type.value, 'PASS' if r.passed else 'FAIL', r.match_percentage)
```

`BatchRunner.run()` writes `checkpoint.json` and `collected.db` under `output_dir/<software_version>/` and skips batches already completed, so it can be resumed. `compare_software()` reads the checkpoints of both run directories and returns one `ScenarioResult` per scenario. `generate_report()` embeds all plots as base64 so the HTML file is portable.

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

`compare_event_sequences(events_a, events_b, ...)` does the same without plotting. `load_events(path)` reads CSV, Parquet, or a MAXTIME `.db` into a DataFrame. A runnable no-hardware example lives in [`examples/offline_comparison`](examples/offline_comparison/README.md).

How the comparison works: phase, overlap, and pedestrian state-change events are grouped by timestamp into sets, the two sequences are auto-aligned, and DTW with Jaccard distance finds the best alignment. Reported metrics are **Match %** (aligned groups with identical event sets), **Sequence DTW** and **Timing DTW** (lower is more similar), divergence windows (where one side has events the other lacks), and timing jitter statistics for matched groups. Intervals with missing or unreliable data on either side are excluded rather than reported as differences.

## Event data format

Column names are matched case-insensitively:

| Column | Accepted names | Notes |
|---|---|---|
| `timestamp` | `TimeStamp`, `time_stamp`, `time` | Event time |
| `event_id` | `EventId`, `EventTypeID`, `event_type_id` | Indiana hi-res event code |
| `parameter` | `Parameter`, `Detector`, `param` | Phase, overlap, or detector number |
| `device_id` | `DeviceId` | Required when the file contains more than one intersection |

Replayed detector events: 81/82 (vehicle off/on), 89/90 (pedestrian off/on), 102/104 (preempt on/off). Detectors numbered 65 and above are ignored. [`ASCControllerEventTypes.csv`](ASCControllerEventTypes.csv) lists the event codes.

## Compatibility

- **Replay**: any NTCIP 1202 v3 controller or emulator (vehicle, pedestrian, and preempt detector objects).
- **Collection**: MAXTIME controllers via `http://<ip>:<port>/v1/asclog/xml/full`. Other controller families need a new collection method and event-code mapping.
- **Latency compensation**: the default 185 ms offset came from the study in [`experiments/latency_scaling_runs/report.md`](experiments/latency_scaling_runs/report.md). Adaptive per-device calibration is available for time-of-day aligned replays.

## Development

```bash
pip install -e .[dev]
pytest                    # runs tests/; live-controller tests skip when no device is reachable
```

## Citation and license

See [`CITATION.cff`](CITATION.cff) for how to cite Signal-Replay. Released under the MIT License, see [LICENSE](LICENSE).
