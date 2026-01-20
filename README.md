# Signal-Replay

Python package for replaying high-resolution event logs from ATC signal controllers back to test controllers using NTCIP.

For now, it is set up to work with the MAXTIME emulator, but can be adapted to any controller due to using NTCIP.

## Installation

```bash
# Install from source (in development mode)
pip install -e .

# Or install dependencies only
pip install -r requirements.txt
```

## How it Works

1. Read vehicle/pedestrian/preempt inputs from hi-res data and converts them into detector group state integers per NTCIP 1202 v3 section 5.3.11.3 (page 275)
2. State integers are then fed back to a test controller with SNMP
3. Read the new event log from the test controller
4. Check for phase/overlap conflicts (virtual conflict monitor)
5. Compare output events to input events using DTW (Dynamic Time Warping)
6. Repeat until a conflict is found or all replays complete

## Quick Start

```python
import signal_replay as sr

# Define signal configuration
signal = sr.SignalConfig(
    device_id='intersection_1',
    ip_port=('127.0.0.1', 1025),
    cycle_length=100,
    incompatible_pairs=[
        ('Ph1', 'Ph5'),
        ('Ph2', 'Ph6'),
        ('Ph3', 'Ph7'),
        ('Ph4', 'Ph8'),
    ],
    events='path/to/events.csv'  # or DataFrame, or Arrow Table
)

# Create simulation config
config = sr.SimulationConfig(
    signals=[signal],
    simulation_replays=3,
    stop_on_conflict=True,
    db_path='./simulation_results.duckdb',
    simulation_speed=1.0
)

# Run simulation
sim = sr.ATCSimulation(config, debug=True)
results = sim.run()

# Access results
print(f"Completed runs: {results['completed_runs']}")
print(f"Conflicts found: {len(results['conflicts'])}")
print(results['comparison_summary'])

# Query data
events_df = sim.get_events(device_id='intersection_1', run_number=1)
conflicts_df = sim.get_conflicts()
```

## Multi-Signal Configuration

```python
import signal_replay as sr
import pandas as pd

# Load event data (can be DataFrame, Arrow Table, or file path)
events_signal_1 = pd.read_csv('signal_1_events.csv')
events_signal_2 = 'signal_2_events.parquet'  # file path

# Define incompatible pairs for each signal
incompatible_pairs_1 = [
    ('Ph1', 'Ph5'), ('Ph2', 'Ph6'),
    ('O1', 'Ph5'), ('O1', 'Ph6'),
]

incompatible_pairs_2 = [
    ('Ph1', 'Ph5'), ('Ph2', 'Ph6'),
    ('Ph3', 'Ph7'), ('Ph4', 'Ph8'),
]

# Create signal configs
signals = [
    sr.SignalConfig(
        device_id='signal_1',
        ip_port=('192.168.1.10', 1025),
        cycle_length=120,
        incompatible_pairs=incompatible_pairs_1,
        events=events_signal_1
    ),
    sr.SignalConfig(
        device_id='signal_2',
        ip_port=('192.168.1.11', 1025),
        cycle_length=120,  # Must match across all signals
        incompatible_pairs=incompatible_pairs_2,
        events=events_signal_2
    ),
]

# Create and run simulation
config = sr.SimulationConfig(
    signals=signals,
    simulation_replays=5,
    stop_on_conflict=False,
    db_path='./multi_signal_sim.duckdb',
    simulation_speed=1.0,
    collection_interval_minutes=5.0
)

sim = sr.ATCSimulation(config, debug=True)
results = sim.run()
```

## Quick Run Helper

For simpler use cases, use the `quick_run` helper:

```python
import signal_replay as sr

results = sr.quick_run(
    signals=[
        {
            'device_id': 'signal_1',
            'ip_port': ('127.0.0.1', 1025),
            'cycle_length': 100,
            'incompatible_pairs': [('Ph1', 'Ph5')],
            'events': 'events.csv'
        }
    ],
    simulation_replays=3,
    stop_on_conflict=True,
    debug=True
)
```

## Features

### Conflict Detection
- Virtual conflict monitor checks for incompatible phase/overlap combinations
- Conflicts are logged to DuckDB database with timestamps and details
- Optional `stop_on_conflict` to halt simulation immediately

### Data Collection
- Automatic polling of controller event logs every 5 minutes (configurable)
- Events stored in DuckDB with `run_number` for tracking
- Deduplication via destructive left join on `(device_id, timestamp, event_id, parameter)`

### DTW Comparison
- Compares output events to input events using Dynamic Time Warping
- Separate DTW analysis for event sequences and timing
- Identifies divergence windows where runs differ
- Calculates match percentage for similarity scoring

### Coordinated Signals
- Enter the cycle length to wait until the right time in the next cycle to begin simulation
- Ensures coordinated signals stay in sync during replay

### Speed Control
- Set `simulation_speed` to run faster than real-time
- Useful for testing long event logs quickly

## Event Data Format

Input events should have these columns (names are flexible):
- `timestamp` / `TimeStamp` / `time`: Event timestamp
- `event_id` / `EventId` / `EventTypeID`: Event type code
- `parameter` / `Parameter` / `Detector`: Event parameter (detector number, phase, etc.)

Optionally:
- `device_id` / `DeviceId`: Device identifier (added automatically if not present)

## Database Schema

The simulation stores data in a DuckDB database:

### `events` table
| Column | Type | Description |
|--------|------|-------------|
| device_id | VARCHAR | Device identifier |
| run_number | INTEGER | Simulation run number |
| timestamp | TIMESTAMP | Event timestamp |
| event_id | INTEGER | Event type code |
| parameter | INTEGER | Event parameter |

### `conflicts` table
| Column | Type | Description |
|--------|------|-------------|
| device_id | VARCHAR | Device identifier |
| run_number | INTEGER | Simulation run number |
| timestamp | TIMESTAMP | Conflict timestamp |
| conflict_details | VARCHAR | Description of conflicting phases |

### `input_events` table
| Column | Type | Description |
|--------|------|-------------|
| device_id | VARCHAR | Device identifier |
| timestamp | TIMESTAMP | Event timestamp |
| event_id | INTEGER | Event type code |
| parameter | INTEGER | Event parameter |

## Legacy Usage

The original `ATCReplay` class is still available in [atc_replay.py](atc_replay.py) for backwards compatibility. See [Example.ipynb](Example.ipynb) for legacy usage.

## Project Structure

```
ATC-Signal-Replay/
├── src/
│   └── atc_signal_replay/
│       ├── __init__.py
│       ├── config.py          # Configuration classes
│       ├── ntcip.py           # SNMP communication
│       ├── replay.py          # Event replay logic
│       ├── collector.py       # Data collection & conflict detection
│       ├── comparison.py      # DTW comparison analysis
│       ├── orchestrator.py    # Main simulation orchestrator
│       └── sql/               # SQL templates
├── pyproject.toml
├── requirements.txt
├── Example.ipynb              # Legacy example
├── test_package.ipynb         # Package testing notebook
└── [project folders]          # Various testing projects
```

## Requirements

- Python 3.9+
- DuckDB
- pandas
- pysnmplib
- requests
- jinja2
- dtaidistance
- pyarrow (optional, for Arrow table support)

## License

MIT License - see [LICENSE](LICENSE) for details.
