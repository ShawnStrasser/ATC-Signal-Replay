# Signal-Replay

Replay historical traffic signal events to test ATC controllers for **bug replication**, **software validation**, and **behavior comparison**.

Signal-Replay reads high-resolution event logs, replays detector actuations via NTCIP/SNMP, collects output events from controllers, monitors for phase conflicts, and uses Dynamic Time Warping (DTW) to compare controller behavior across runs. It can send calls to any controller but data collection currently only works with MAXTIME controllers.

## Features

- **Replay** hi-res detector events to any ATC controller via NTCIP SNMP
- **Detect conflicts** between incompatible phase/overlap pairs
- **Compare runs** using group-level DTW with Jaccard distance
- **Multi-signal** coordinated replay with parallel execution
- **Time-of-day alignment** mode (`tod_align`) for replaying events at real wall-clock times
- **Cycle synchronization** for coordinated signal replay with offsets
- **DuckDB storage** for SQL-based analysis of events, conflicts, and comparisons
- **Matplotlib Gantt charts** for visual comparison of phase timing
- **Batch runner & HTML reports** for firmware validation workflows (experimental)

## Installation

```bash
pip install signal-replay
```

## Quick Start: Conflict Detection

Replay events to a controller and monitor for phase conflicts:

```python
import signal_replay as sr

sim = sr.ATCSimulation(
    signals=[
        sr.SignalConfig(
            device_id='0',
            ip='192.0.2.10',
            incompatible_pairs=[('O5', 'Ph4'), ('O5', 'Ph8')],
        )
    ],
    events='2025-01-15_events.csv',  # Must have device_id column
    replays=40,
    stop_on_conflict=True,
    db_path='./conflict_test.duckdb',
)

results = sim.run()
```

The simulation will:
1. Load events and filter by `device_id`
2. Generate an activation feed (detector on/off SNMP commands)
3. Reset all detector states on the controller
4. Replay detector actuations in real-time via SNMP
5. Periodically collect output events from the controller via HTTP
6. Check for conflicts between incompatible phase/overlap pairs
7. Stop early if `stop_on_conflict=True` and a conflict is found
8. Run DTW comparison between input events and each replay run

**Output:**

```
Starting ATC simulation with 1 signals, 40 replays
Estimated duration per run: 0h 32m (computed in 0.3s)

--- Starting Run 1/40 ---
Sending events to 1 controllers...
[0] Complete — sent 4231 events
Run 1 completed

Conflict detected! Stopping simulation.

--- Running Comparison Analysis ---

============================================================
SIMULATION COMPLETE
============================================================

Completed Runs: 1
Conflicts Found: 2

Conflicts:
  [0] Run 1: O5 & Ph4; O5 & Ph8 at 2026-02-02 14:28:16.100000

Comparison Summary:

Device: 0
  input vs 1: Sequence DTW=0.0034, Timing DTW=0.0001, Match=56.0%
```

## Multi-Signal Coordinated Replay

Test coordinated signals using a **single event file** containing all device data.
Events are automatically filtered by `device_id` and distributed to each signal:

```python
import signal_replay as sr

sim = sr.ATCSimulation(
    signals=[
        sr.SignalConfig(
            device_id='main_1st',
            ip='127.0.0.1',
            udp_port=1025,          # Required for localhost
            cycle_length=120,
            cycle_offset=0,
        ),
        sr.SignalConfig(
            device_id='main_2nd',
            ip='127.0.0.1',
            udp_port=1026,
            cycle_length=120,
            cycle_offset=30,        # 30s offset from reference
        ),
    ],
    events='all_signals_events.csv',  # Must have 'device_id' column
    replays=5,
    db_path='./coordination_test.duckdb',
    debug=True,
)

results = sim.run()
```

When multiple signals share the same `cycle_length`, each signal waits for its `cycle_offset` position in the cycle before beginning replay. All signals run in parallel via ThreadPoolExecutor.

The centralized events file must contain a `device_id` column matching the `device_id` in each `SignalConfig`.

---

## Time-of-Day Alignment

Use `tod_align=True` to replay events at their real wall-clock times. Instead of compressing events relative to the start of the data, each event is sent at the same time-of-day as the original log:

```python
sr.SignalConfig(
    device_id='intersection_1',
    ip='192.0.2.10',
    tod_align=True,       # Replay at real wall-clock times
    # cycle_length must be 0 when tod_align is True
)
```

This is useful for testing time-of-day plans rather than specific patterns. If you wanted to test for a conflict that occured during a specefic pattern, you don't need this, you could just set the controller to run that pattern and configure the cycle length and offset parameters.

> **Note:** `tod_align=True` is incompatible with `cycle_length > 0` and requires `simulation_speed=1.0`.

---

## Querying Results with DuckDB

All events, conflicts, and comparisons are stored in DuckDB:

```python
import duckdb

con = duckdb.connect('./conflict_test.duckdb')

# Find all conflicts
conflicts = con.execute("""
    SELECT timestamp, conflict_details, run_number
    FROM conflicts
    ORDER BY timestamp
""").df()

print(conflicts)
```

| timestamp | conflict_details | run_number |
|-----------|------------------|------------|
| 2026-02-02 14:28:16.100 | O5 & Ph4; O5 & Ph8 | 1 |

```python
# Compare phase green times across runs
phase_greens = con.execute("""
    SELECT 
        run_number,
        parameter as phase,
        COUNT(*) as green_count,
        MIN(timestamp) as first_green,
        MAX(timestamp) as last_green
    FROM events
    WHERE event_id = 1  -- Phase On
    GROUP BY run_number, parameter
    ORDER BY run_number, parameter
""").df()

print(phase_greens)
```

| run_number | phase | green_count | first_green | last_green |
|------------|-------|-------------|-------------|------------|
| 1 | 1 | 42 | 2025-01-15 14:00:01 | 2025-01-15 14:32:15 |
| 1 | 2 | 38 | 2025-01-15 14:00:45 | 2025-01-15 14:31:52 |
| 2 | 1 | 42 | 2025-01-15 14:00:01 | 2025-01-15 14:32:14 |

---

## Dynamic Time Warping (DTW)

Signal-Replay uses DTW to compare event sequences between runs. DTW aligns two time series by finding the optimal "warping path" that minimizes the total distance between matched points, even when events are shifted in time.

**How it works:**

1. **Event filtering**: Only phase and overlap state-change events are compared (green on/off, yellow, red, overlap states, pedestrian walk/dont-walk, etc.). Detector actuations are excluded since they are the *input*, not the *output*.

2. **Timestamp grouping**: Events at the same timestamp are grouped into sets of `(event_id, parameter)` tuples. This makes comparison order-independent — events at the same timestamp may appear in different order between runs.

3. **Jaccard distance**: Distance between two timestamp groups is computed as Jaccard distance: 0 if the event sets are identical, otherwise the fraction of non-overlapping events. This means `{(1, 5), (1, 6)}` (Phase 5 and 6 Green) is treated as completely different from `{(1, 5), (7, 6)}` (Phase 5 Green and Phase 6 Yellow End).

4. **Auto-alignment**: Before DTW, the sequences are automatically aligned by trying different timestamp-group offsets and finding the one with the best Jaccard match over the first 6 minutes.

5. **DTW alignment**: The algorithm finds the best alignment between grouped sequences, allowing for insertions, deletions, and timing shifts.

6. **Divergence detection**: Two types of divergences are detected:
   - **Structural gaps**: Where one sequence advances while the other stalls (missing events)
   - **Value mismatches**: Consecutive timestamp groups with different event sets

7. **Timing analysis**: For matched groups, the time differences between runs are analyzed to measure timing jitter (std deviation, max, 95th percentile).

8. **Timing DTW**: Standard Euclidean DTW on normalized time deltas provides a backward-compatible timing distance metric.

**Metrics reported:**

| Metric | Meaning |
|--------|---------|
| Sequence DTW | Jaccard-based distance on timestamp groups (lower = more similar) |
| Timing DTW | Euclidean distance on normalized time deltas (lower = more similar) |
| Match % | Percentage of aligned timestamp groups with identical event sets |

**Example interpretation:**
- `Match=99%` → Nearly identical runs
- `Match=92%`, 2 divergences → Runs diverged in specific windows

### Manual Comparison

You can compare any two event DataFrames directly:

```python
import pandas as pd
import signal_replay as sr

# Load events from any source
events_a = pd.read_csv('simulation_1/run_3.csv')
events_b = pd.read_csv('simulation_2/run_7.csv')

# Compare them directly
result = sr.compare_event_sequences(
    events_a,
    events_b,
    label_a="Sim1 Run3",
    label_b="Sim2 Run7",
)
```

**Output:**

```
============================================================
DTW Comparison: Sim1 Run3 vs Sim2 Run7
============================================================
  Match Percentage:          97.2%
  Groups in A:               312
  Groups in B:               308
  Alignment offset:          2 groups
  Divergence Windows:        1
============================================================

Divergence Windows:
  1. ~15s gap in Replay at 12:34–12:49 (8 unmatched groups)

Timing Analysis (298 matched groups):
  Timing jitter std:  0.142s
  Max jitter:         0.831s
  95th percentile:    0.287s
```

Access detailed results programmatically:

```python
# Suppress automatic printing for scripted use
result = sr.compare_event_sequences(
    events_a, events_b,
    label_a="A", label_b="B",
    print_summary=False,
)

print(f"Match: {result.match_percentage:.1f}%")
print(f"Divergences: {len(result.divergence_windows)}")

# Examine divergence windows
for div in result.divergence_windows:
    print(f"  {div.description}")

# Timing stats
if result.timing_stats:
    print(f"Timing jitter std: {result.timing_stats['std_diff']:.3f}s")
```

---

## Comparison Visualization

Compare any two event logs and generate a Gantt chart showing signal phase timing side-by-side:

```python
import signal_replay as sr

result = sr.compare_and_visualize(
    events_a='input_events.csv',      # Path, DataFrame, or .db file
    events_b='output_run_0.csv',
    label_a='Input Events',
    label_b='Output Run 0',
    output_dir='./comparison_plots',
    output_name='my_comparison',       # Generates my_comparison.png
    
    # Optional thresholds
    match_threshold=95.0,      # Warn if match < 95%
    sequence_threshold=0.05,   # Warn if sequence DTW > 0.05
    timing_threshold=0.02,     # Warn if timing DTW > 0.02
)
```

**Output:**

```
============================================================
Comparison: Input Events vs Output Run 0
============================================================
  Match Percentage:  97.2%  (threshold: ≥95.0%)
    ✓ OK
  Timestamp groups in A: 312
  Timestamp groups in B: 308
  Alignment: trimmed 2 groups (0.0s from A, 3.2s from B)
  Divergences: 1
    1. ~15s gap in Replay at 12:34–12:49 (8 unmatched groups)
  Timing: jitter_std=0.142s, max=0.831s, p95=0.287s
============================================================
Generating timelines with atspm...
Creating Gantt chart with 1240 events (A) and 1235 events (B)...
Timeline alignment offset: -2.5s
```

Plots are only generated when thresholds are exceeded (or `force_plot=True`).

> **Note:** Gantt chart generation requires the `atspm` package for timeline reconstruction and `matplotlib` for rendering. Output format is `.png` (also supports `.pdf`, `.svg`, `.jpg`).

### Supported Input Formats

The `compare_and_visualize` and `load_events` functions accept multiple formats:

| Format | Example |
|--------|---------|
| CSV file | `'events.csv'` |
| Parquet file | `'events.parquet'` |
| SQLite database | `'results.db'` (reads `Event` table, MAXTIME format) |
| pandas DataFrame | `pd.DataFrame(...)` |

### Gantt Chart Features

The generated matplotlib chart includes:
- **Two-panel layout**: Original events on top, replay on bottom
- **Color-coded phases**: Green, Yellow, Red, and Overlap states
- **Divergence markers**: Red shaded regions indicate where sequences diverged
- **Time-aligned**: Cross-correlation is used to align the two timelines

### Threshold Interpretation

| Metric | Good Value | Meaning |
|--------|------------|---------|
| Match % | ≥95% | Percentage of timestamp groups that align with identical events |
| Sequence DTW | <0.05 | Lower = more similar event sequences |
| Timing DTW | <0.02 | Lower = more similar event timing |

### Database Storage

When running via `ATCSimulation`, comparison results are automatically stored in the database:

```sql
-- Find all comparisons with poor match percentage
SELECT device_id, run_a, run_b, match_percentage
FROM comparison_results
WHERE match_percentage < 90
ORDER BY match_percentage ASC;
```

---

## Configuration Reference

### ATCSimulation

The main entry point. Events are **always** provided at this level and automatically filtered by `device_id`.

Accepts either a `SimulationConfig` object (legacy) or keyword arguments (recommended):

```python
# Recommended API
sim = sr.ATCSimulation(
    signals=[...],                     # List of SignalConfig
    events='events.csv',               # REQUIRED: centralized events with device_id column
    replays=5,                         # Number of simulation runs
    stop_on_conflict=False,            # Stop on first conflict
    db_path='./test.duckdb',           # Database path
    simulation_speed=1.0,              # Speed multiplier (must be 1.0 with tod_align)
    collection_interval_minutes=5.0,   # How often to poll controller logs
    post_replay_settle_seconds=10.0,   # Wait after replay before final collection
    snmp_timeout_seconds=2.0,          # SNMP response timeout
    show_progress_logs=False,          # Print periodic "Sent x/y events" updates
    progress_log_interval_seconds=60.0,# Seconds between progress log lines
    comparison_thresholds=None,        # ComparisonThresholds object (or use defaults)
    output_dir=None,                   # Directory for comparison plots
    skip_comparison=False,             # Skip post-replay DTW comparison
    debug=False,
)

# Legacy API (still supported)
config = sr.SimulationConfig(
    signals=[...],
    events='events.csv',
    simulation_replays=5,
)
sim = sr.ATCSimulation(config)
```

### SignalConfig

Configuration for individual signals. Events are provided at the simulation level.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `device_id` | str | *required* | Unique identifier matching the events file |
| `ip` | str | *required* | Controller IP address |
| `udp_port` | int | 161 | SNMP port. **Required for localhost** (no default for 127.0.0.1) |
| `cycle_length` | int | 0 | Cycle length in seconds for coordination (0 = disabled) |
| `cycle_offset` | float | 0.0 | Offset in seconds within cycle for synchronized start |
| `tod_align` | bool | False | Replay events at real wall-clock time-of-day |
| `incompatible_pairs` | list | None | Phase/overlap pairs to monitor, e.g. `[('O5', 'Ph4')]`. `None` = no conflict checking |
| `http_port` | int/None | Auto | HTTP port for log collection. Auto = `udp_port` for localhost, `80` for remote. `None` disables collection |
| `limit_minutes` | float | 0.0 | Only replay the last N minutes of events (0 = all) |
| `buffer_minutes` | float | 0.0 | Include extra lead-in minutes before `limit_minutes` window |

### SimulationConfig (Legacy)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `signals` | list | *required* | List of `SignalConfig` |
| `events` | DataFrame/Path | *required* | Centralized events (filtered by `device_id`) |
| `simulation_replays` | int | 1 | Number of replay runs |
| `stop_on_conflict` | bool | False | Stop on first conflict detection |
| `db_path` | str | `./atc_replay.duckdb` | Database path |
| `controller_type` | str | `"MAXTIME"` | Controller type (only MAXTIME supported) |
| `simulation_speed` | float | 1.0 | Speed multiplier |
| `collection_interval_minutes` | float | 5.0 | Minutes between controller log polls |
| `post_replay_settle_seconds` | float | 10.0 | Seconds to wait after replay before final collection |
| `snmp_timeout_seconds` | float | 2.0 | SNMP response timeout |
| `show_progress_logs` | bool | False | Print periodic send progress |
| `progress_log_interval_seconds` | float | 60.0 | Seconds between progress log lines |

---

## Event Data Format

Input events require these columns (flexible naming — case-insensitive matching):

| Column | Alternatives | Description |
|--------|--------------|-------------|
| `timestamp` | `TimeStamp`, `time_stamp`, `time` | Event timestamp |
| `event_id` | `EventId`, `EventTypeID`, `event_type_id` | Event type code |
| `parameter` | `Parameter`, `Detector`, `param` | Phase/detector number |
| `device_id` | `DeviceId` | **Required** — maps events to signals |

Only detector actuation events are replayed (event IDs 81/82 = vehicle on/off, 89/90 = ped on/off, 102/104 = preempt on/off). Detectors with parameter ≥ 65 are filtered out.

For comparison, phase and overlap state-change events are used (phase green/yellow/red, overlap green/yellow/end, pedestrian walk/dont-walk, etc.).

---

## How Replay Works

1. **Load events**: Input events are loaded from CSV, Parquet, SQLite (.db), or DataFrame
2. **Filter detectors**: Only detector actuation events (81, 82, 89, 90, 102, 104) are kept
3. **Impute missing actuations**: Missing on/off pairs are interpolated to ensure correct state tracking (e.g., two consecutive "on" events get an "off" inserted between them)
4. **Generate activation feed**: Events are grouped by detector group (8 detectors per group), cumulative bitmask states are computed, and inter-event sleep times are calculated
5. **Reset detectors**: All detector states on the controller are set to 0 via SNMP
6. **Wait for cycle**: If `cycle_length > 0`, wait until the correct cycle offset position
7. **Send commands**: SNMP SET commands are sent to the controller in real-time sequence. Each command sets the bitmask state for a detector group.
8. **Collect output**: A background thread periodically polls the controller's HTTP event log endpoint and stores events in DuckDB
9. **Check conflicts**: Each collection also checks for incompatible phase/overlap pairs being active simultaneously

---

<details>
<summary><strong>Database Schema</strong></summary>

**`events`** — Output events collected from controller

| Column | Type | PK |
|--------|------|-----|
| device_id | VARCHAR | ✓ |
| run_number | INTEGER | ✓ |
| timestamp | TIMESTAMP | ✓ |
| event_id | INTEGER | ✓ |
| parameter | INTEGER | ✓ |

**`conflicts`** — Detected phase/overlap conflicts

| Column | Type |
|--------|------|
| device_id | VARCHAR |
| run_number | INTEGER |
| timestamp | TIMESTAMP |
| conflict_details | VARCHAR |

**`input_events`** — Source phase/overlap events stored for comparison

| Column | Type |
|--------|------|
| device_id | VARCHAR |
| timestamp | TIMESTAMP |
| event_id | INTEGER |
| parameter | INTEGER |

**`comparison_results`** — DTW comparison metrics (created on first comparison)

| Column | Type | Description |
|--------|------|-------------|
| device_id | VARCHAR | Signal identifier |
| run_a | VARCHAR | First run label (e.g., `'input'`) |
| run_b | VARCHAR | Second run label (e.g., `'1'`) |
| timestamp | TIMESTAMP | When comparison was performed |
| sequence_dtw_distance | DOUBLE | Raw sequence DTW distance |
| sequence_dtw_normalized | DOUBLE | Normalized sequence DTW distance |
| timing_dtw_distance | DOUBLE | Raw timing DTW distance |
| timing_dtw_normalized | DOUBLE | Normalized timing DTW distance |
| match_percentage | DOUBLE | Percentage of aligned timestamp groups that match |
| num_divergences | INTEGER | Number of divergence windows |
| sequence_threshold | DOUBLE | Threshold used |
| timing_threshold | DOUBLE | Threshold used |
| match_threshold | DOUBLE | Threshold used |
| exceeds_threshold | BOOLEAN | Whether any threshold was exceeded |
| threshold_reason | VARCHAR | Description of exceeded thresholds |
| plot_path | VARCHAR | Path to generated plot file |

</details>

---

## Compatibility

- **Sending actuations**: Any NTCIP 1202 v3 controller (uses standard detector actuation OIDs for vehicle, pedestrian, and preempt detectors)
- **Collecting output logs**: MAXTIME controllers via HTTP XML endpoint (`/v1/asclog/xml/full`). Other controller types can be added by implementing a new collection method.
- **Loading input logs**: CSV, Parquet, MAXTIME SQLite `.db` files

---

## Experimental: Firmware Validation

> **⚠️ Work in Progress** — The firmware validation workflow is functional but still under active development. APIs and configuration formats may change.

The firmware validation system extends Signal-Replay to automate **A/B testing of controller firmware versions**. It replays the same set of scenarios against a baseline firmware and a new firmware, then compares outputs to detect behavioral regressions.

### Concepts

- **Test Suite** (`FirmwareTestSuite`): Defines scenarios, batches, and firmware versions to compare
- **Scenario** (`TestScenario`): A single intersection/controller log to replay. Each scenario has a `test_type`:
  - `similarity` — Replay and compare output via DTW. Pass = behavior matches baseline within thresholds.
  - `conflict` — Replay repeatedly to trigger a known conflict. Pass = baseline reproduces conflict, new firmware does not.
- **Batch** (`TestBatch`): Groups scenarios assigned to physical controllers for a single replay session. Contains a mapping of `scenario_id` → controller `host:port`.
- **Batch Runner** (`BatchRunner`): Executes batches sequentially with checkpoint/resume support. Prompts (or calls a callback) for database loading between batches.
- **Comparison** (`compare_firmware`): Compares baseline vs new replay outputs across all scenarios using parallel workers.
- **Report** (`generate_report`): Generates a self-contained HTML report with pass/fail status, match percentages, divergence details, and embedded Gantt chart images.

### Test Suite YAML Format

```yaml
suite_name: Firmware Validation
firmware_version: "2.15.1"
baseline_version: "original_logs"
scenarios:
  - scenario_id: "03013"
    database_name: "/path/to/03013.bin"
    events_source: "/path/to/03013.parquet"
    test_type: similarity
    description: "Lots of rail preemption"
    tod_align: true
  - scenario_id: "2C039"
    database_name: "/path/to/2C039.bin"
    events_source: "/path/to/2C039.parquet"
    test_type: conflict
    replays: 40
    incompatible_pairs:
      - ["O5", "Ph4"]
      - ["O5", "Ph8"]
    description: "Known cycle fault conflict"
batches:
  - batch_id: day1
    assignments:
      "03013": "192.168.1.10:161:80"
      "2C039": "192.168.1.11:161:80"
```

### Running a Firmware Validation

```python
import signal_replay as sr

# Load the test suite
suite = sr.load_from_yaml('firmware_validation/test_suite.yaml')

# Run baseline (on original firmware)
runner = sr.BatchRunner(suite, debug=True)
baseline_checkpoint = runner.run()

# ... flash new firmware onto controllers ...

# Run new firmware
suite.firmware_version = "2.16.0"
runner_new = sr.BatchRunner(suite, debug=True)
new_checkpoint = runner_new.run()

# Compare baseline vs new
results = sr.compare_firmware(
    baseline_run_dir=str(runner.run_dir),
    new_run_dir=str(runner_new.run_dir),
    suite=suite,
    output_dir='./comparison_output',
)

# Generate HTML report
sr.generate_report(
    results=results,
    suite=suite,
    output_path='./report.html',
)
```

### BatchRunner Features

- **Checkpoint/resume**: Progress is saved to `checkpoint.json` after each batch. Re-running skips completed batches.
- **Database loading prompts**: Before each batch, the runner prompts the operator to load the correct controller database (or accepts a callback for automation).
- **Separate conflict handling**: Conflict scenarios run individually with `stop_on_conflict=True` and their own DuckDB files.
- **Logging**: Per-run log file at `<output_dir>/<firmware_version>/run.log`.

### HTML Report

`generate_report()` produces a self-contained HTML file with:
- Summary tiles (pass/fail counts, average match percentage)
- Sortable results tables for similarity and conflict tests
- Detailed per-scenario sections with match %, divergence info, and timing analysis
- Phase/overlap difference breakdowns
- Embedded Gantt chart images (base64-encoded, no external dependencies)
- Configuration summary

---

## License

MIT — see [LICENSE](LICENSE)
