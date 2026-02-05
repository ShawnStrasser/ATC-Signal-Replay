# Signal-Replay

Replay historical traffic signal events to test controllers for **bug replication**, **software validation**, and **behavior comparison**.

Signal-Replay reads high-resolution event logs, replays detector actuations via NTCIP/SNMP, monitors for phase conflicts, and uses Dynamic Time Warping to compare controller behavior across runs. All results are stored in DuckDB for analysis.

## Features

- **Replay** hi-res events to any ATC controller via NTCIP
- **Detect conflicts** between incompatible phases/overlaps  
- **Compare runs** using DTW to find behavioral differences
- **Multi-signal** coordinated replay with parallel execution
- **DuckDB storage** for SQL-based analysis

## Installation

```bash
pip install signal-replay
```

---

## Example: Conflict Detection

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
    db_path='./conflict_test.duckdb'
)

results = sim.run()
```

**Output:**

```
Starting ATC simulation with 1 signals, 40 replays
Estimated duration per run: 32:29

--- Starting Run 1/40 ---
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

---

## Example: Multi-Signal with Centralized Events

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
    debug=True
)

results = sim.run()
```

The centralized events file must contain a `device_id` column matching the `device_id` in each `SignalConfig`.

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

1. **Sequence encoding**: Each `(event_id, parameter)` pair is encoded as a categorical value
2. **Categorical DTW**: For sequence comparison, a binary distance metric is used (0 = exact match, 1 = different event). This ensures that `(event_id=1, param=5)` "Phase 5 Green" is treated as completely different from `(event_id=1, param=6)` "Phase 6 Green" — there's no concept of "similar" for categorical events
3. **Timing DTW**: For timing comparison, standard Euclidean distance is used since timing values are numerical
4. **DTW alignment**: The algorithm finds the best alignment between runs, allowing for insertions, deletions, and timing shifts
5. **Divergence detection**: Large jumps in the warping path indicate where sequences diverge
6. **Match percentage**: Ratio of exactly matching events along the aligned path

**Metrics reported:**

| Metric | Meaning |
|--------|---------|
| Sequence DTW | Distance based on event type differences (lower = more similar) |
| Timing DTW | Distance based on event timing differences (lower = more similar) |
| Match % | Percentage of events that match along the warping path |

**Example interpretation:**
- `Sequence DTW=0.001, Match=99%` → Nearly identical runs
- `Sequence DTW=0.08, Match=92%` → Runs diverged, possibly around a conflict or timing change

### Manual Comparison

You can compare any two event DataFrames directly, which is useful for:
- Comparing runs from different simulations
- Comparing events from different time periods
- Cross-comparing runs that aren't consecutive

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
    label_b="Sim2 Run7"
)

# Output:
# ============================================================
# DTW Comparison: Sim1 Run3 vs Sim2 Run7
# ============================================================
#   Sequence DTW (normalized): 0.0234
#   Timing DTW (normalized):   0.0156
#   Match Percentage:          97.2%
#   Divergence Windows:        2
#   Sequence A length:         1542
#   Sequence B length:         1538
# ============================================================
```

Access detailed results programmatically:

```python
# Suppress automatic printing for scripted use
result = sr.compare_event_sequences(
    events_a, events_b,
    label_a="A", label_b="B",
    print_summary=False
)

print(f"Match: {result.match_percentage:.1f}%")
print(f"Sequence similarity: {1 - result.sequence_dtw.normalized_distance:.2%}")

# Examine divergence windows
for div in result.divergence_windows:
    print(f"Divergence at {div.start_time_delta_a:.1f}s - {div.end_time_delta_a:.1f}s")
```

---

## Comparison Visualization

Compare any two event logs and generate an interactive Gantt chart showing signal phase timing side-by-side:

```python
import signal_replay as sr

result = sr.compare_and_visualize(
    events_a='input_events.csv',      # Path, DataFrame, or .db file
    events_b='output_run_0.csv',
    label_a='Input Events',
    label_b='Output Run 0',
    output_dir='./comparison_plots',
    output_name='my_comparison',
    
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
    ✓  PASS
  Events in A: 620
  Events in B: 615
  Divergences: 1
============================================================
✓  ALL THRESHOLDS MET
Saved comparison chart to: comparison_plots/my_comparison.html
```

### Supported Input Formats

The `compare_and_visualize` function accepts multiple input formats:

| Format | Example |
|--------|---------|
| CSV file | `'events.csv'` |
| Parquet file | `'events.parquet'` |
| SQLite database | `'results.db'` (reads `events` table) |
| DuckDB database | `'results.duckdb'` (reads `events` table) |
| pandas DataFrame | `pd.DataFrame(...)` |

### Gantt Chart Features

The generated HTML chart includes:
- **Two-panel layout**: Original events on top, replay on bottom
- **Color-coded phases**: Green, Yellow, Red, and Overlap states
- **Interactive hover**: Shows start time and duration for each state
- **Divergence markers**: Red vertical lines indicate where sequences diverged
- **Zoom/Pan**: Use Plotly's built-in tools to explore the timeline

### Threshold Interpretation

| Metric | Good Value | Meaning |
|--------|------------|---------|
| Match % | ≥95% | Percentage of events that align exactly |
| Sequence DTW | <0.05 | Lower = more similar event sequences |
| Timing DTW | <0.02 | Lower = more similar event timing |

**Understanding the relationship:**
- **Match %** = `1 - Sequence DTW` (approximately)
- A low Match % with low Sequence DTW indicates timing shifts but similar sequences
- High Sequence DTW with high Match % is rare but indicates the DTW found a good alignment despite many differences

### Database Storage

Comparison results are automatically stored in the simulation database for SQL analysis:

```sql
-- Find all comparisons with poor match percentage
SELECT device_id, run_a, run_b, match_percentage
FROM comparison_results
WHERE match_percentage < 90
ORDER BY match_percentage ASC;

-- Get divergence details
SELECT * FROM divergence_windows
WHERE device_id = '0' AND run_a = 'input';
```

---

## Configuration Reference

### ATCSimulation

The main entry point. Events are **always** provided at this level and automatically filtered by `device_id`.

Accepts either a `SimulationConfig` object (legacy) or keyword arguments (streamlined):

```python
# Streamlined API (recommended)
sim = sr.ATCSimulation(
    signals=[...],           # List of SignalConfig  
    events='events.csv',     # REQUIRED: centralized events with device_id column
    replays=5,               # Number of simulation runs
    stop_on_conflict=False,  # Stop on first conflict
    db_path='./test.duckdb', # Database path
    simulation_speed=1.0,    # Speed multiplier
    debug=False
)

# Legacy API (still supported)
config = sr.SimulationConfig(
    signals=[...],
    events='events.csv',     # REQUIRED
    simulation_replays=5
)
sim = sr.ATCSimulation(config)
```

### SignalConfig

Configuration for individual signals. Events are provided at the simulation level.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `device_id` | str | *required* | Unique identifier |
| `ip` | str | *required* | Controller IP |
| `udp_port` | int | 161 | SNMP port. **Required for localhost** |
| `cycle_length` | int | 0 | Cycle length for coordination (0 = disabled) |
| `cycle_offset` | float | 0.0 | Offset from cycle start (use with `cycle_length`) |
| `incompatible_pairs` | list | None | Phase pairs to monitor. Omit to disable conflict checking |
| `http_port` | int/None | Auto | HTTP port for log collection. `None` disables |
| `limit_minutes` | float | 0.0 | Only replay last N minutes |
| `buffer_minutes` | float | 0.0 | Lead-in minutes when using `limit_minutes` |

### SimulationConfig (Legacy)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `signals` | list | *required* | List of `SignalConfig` |
| `events` | DataFrame/Path | *required* | Centralized events (filtered by `device_id`) |
| `simulation_replays` | int | 1 | Number of replay runs |
| `stop_on_conflict` | bool | False | Stop on first conflict |
| `db_path` | str | `./atc_replay.duckdb` | Database path |
| `simulation_speed` | float | 1.0 | Speed multiplier |

---

## Event Data Format

Input events require these columns (flexible naming):

| Column | Alternatives | Description |
|--------|--------------|-------------|
| timestamp | `TimeStamp`, `time` | Event timestamp |
| event_id | `EventId`, `EventTypeID` | Event type code |
| parameter | `Parameter`, `Detector` | Phase/detector number |
| device_id | `DeviceId` | **Required for centralized events** |

---

<details>
<summary><strong>Database Schema</strong></summary>

**`events`** — Output events from controller

| Column | Type |
|--------|------|
| device_id | VARCHAR |
| run_number | INTEGER |
| timestamp | TIMESTAMP |
| event_id | INTEGER |
| parameter | INTEGER |

**`conflicts`** — Detected conflicts

| Column | Type |
|--------|------|
| device_id | VARCHAR |
| run_number | INTEGER |
| timestamp | TIMESTAMP |
| conflict_details | VARCHAR |

**`input_events`** — Source events for comparison

| Column | Type |
|--------|------|
| device_id | VARCHAR |
| timestamp | TIMESTAMP |
| event_id | INTEGER |
| parameter | INTEGER |

**`comparison_results`** — DTW comparison metrics

| Column | Type | Description |
|--------|------|-------------|
| device_id | VARCHAR | Signal identifier |
| run_a | VARCHAR | First run label (e.g., 'input') |
| run_b | VARCHAR | Second run label (e.g., '1') |
| sequence_dtw | DOUBLE | Normalized sequence DTW distance |
| timing_dtw | DOUBLE | Normalized timing DTW distance |
| match_percentage | DOUBLE | Percentage of aligned events that match |
| divergence_count | INTEGER | Number of divergence windows |
| computed_at | TIMESTAMP | When comparison was performed |

**`divergence_windows`** — Detected divergence periods

| Column | Type | Description |
|--------|------|-------------|
| device_id | VARCHAR | Signal identifier |
| run_a | VARCHAR | First run label |
| run_b | VARCHAR | Second run label |
| start_time_a | TIMESTAMP | Start of divergence in run A |
| end_time_a | TIMESTAMP | End of divergence in run A |
| start_time_b | TIMESTAMP | Start of divergence in run B |
| end_time_b | TIMESTAMP | End of divergence in run B |
| severity | DOUBLE | Relative divergence magnitude |

</details>

---

## Compatibility

- **Sending actuations**: Any NTCIP 1202 v3 controller
- **Reading logs**: MAXTIME controllers (others can be added)

## License

MIT — see [LICENSE](LICENSE)
