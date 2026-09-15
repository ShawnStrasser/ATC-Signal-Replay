# Software validation workspace

Batch A/B validation of a controller software release, or of a configuration change with the software held constant, across many intersections. `software_validate.py` is the reference runner. It uses only the public `signal_replay` API, so an application can call the same pieces directly (see the main [README](../README.md#use-the-validation-pieces-from-your-own-application)).

## Layout

```
software_validation/
  software_validate.py        # the runner
  settings.example.json       # template; copy to settings.json
  settings.json               # your bench configuration (ignored by git)
  databases.xlsx              # catalog of intersections to test (ignored by git)
  logs/                       # replay input logs, immutable source data (ignored by git)
  databases/                  # controller configuration files for manual loading (ignored by git)
  conflict_monitor/           # optional conflict_pairs.json (ignored by git)
  coord_patterns/             # optional <TSSU>.json coordination patterns (ignored by git)
  results/<software_version>/ # outputs of one validation run (ignored by git)
```

Everything except the scripts and the settings template is agency data and stays out of the repository.

## Setup

1. Copy `settings.example.json` to `settings.json` and edit it:

   | Key | Meaning |
   |---|---|
   | `controller_targets` | List of controllers, `"host:port"` or `"host:udp_port:http_port"`. One intersection is replayed per controller per batch. |
   | `software_version` | Label for the release under test. Outputs go to `results/<software_version>/`. |
   | `baseline_version` | Label to compare against. Uses `results/<baseline_version>/collected.db` when it exists, otherwise the original logs in `logs/`. |
   | `replay_latency_offset_seconds` | Fixed SNMP latency compensation (default 0.1853). |
   | `replay_latency_offset_lookback_min` | Enables adaptive per-device latency calibration using this many minutes of recent detector events. |
   | `catalog_file`, `logs_dir`, `databases_dir`, `results_dir`, `conflict_pairs_file` | Paths relative to this folder. |
   | `comparison.sequence_threshold`, `comparison.timing_threshold`, `comparison.match_threshold` | DTW alert thresholds (defaults 0.05, 0.02, 95.0). |
   | `comparison.phase_call_similarity_threshold` | Minimum input-replay similarity (percent) before a window is treated as unreliable and excluded. |
   | `comparison.analysis_start_time`, `comparison.analysis_end_time` | Clock times (`HH:MM`) bounding the analysed window for time-of-day aligned scenarios. Leave empty to analyse everything after the settle window. |
   | `comparison.settle_minutes` | Minutes excluded at the start of each scenario (default 10) when no `analysis_start_time` is set. |
   | `comparison.group_tolerance` | Seconds within which consecutive events are grouped (0.25 absorbs timestamp-resolution differences). |
   | `comparison.max_divergence_plots`, `comparison.divergence_window_minutes` | How many Gantt charts to draw per scenario and how wide each is. |
   | `analysis_workers` | Processes used for the comparison stage. |

2. Fill in `databases.xlsx`. The first worksheet needs these columns (header names are case-insensitive):

   | Column | Required | Meaning |
   |---|---|---|
   | `TSSU` | yes | Intersection identifier. Used to find `logs/<TSSU>.*` and `databases/<TSSU>*`. Becomes the `scenario_id` and `device_id`. |
   | `Version` | yes | Configuration version label, informational. |
   | `Type` | yes | `Similarity` or `Conflict`. |
   | `Notes` | yes | Free text shown in the report. |
   | `CycleLength`, `Offset` | no | When `CycleLength` > 0 the scenario replays cycle-synchronized instead of time-of-day aligned. |
   | `Replace on Rerun` | no | `yes` forces re-collection of a scenario that already has data in `collected.db`. |

3. Put one replay log per intersection in `logs/`, named `<TSSU>.parquet`, `<TSSU>.csv`, or `<TSSU>.db` (the first match of `<TSSU>*` is used). The log must contain a `device_id` column equal to the TSSU.

4. Put the controller configuration files in `databases/`, named `<TSSU>.bin` or any name starting with the TSSU. They are never uploaded by the package. They are shown to the operator so the right file is loaded on the right controller.

5. Optional. `conflict_monitor/conflict_pairs.json` maps a TSSU to the phase/overlap pairs its conflict monitor prohibits, for example `{"12059": [["Ph1", "Ph2"], ["O1", "Ph4"]]}`. Conflict scenarios without an entry fail with a note saying no pairs were configured. `conflict_monitor_generation.ipynb` contains a prompt for extracting pairs from a conflict-monitor card image, and `../generate_conflicts.py` shows the same derivation in code.

6. Optional. `coord_patterns/<TSSU>.json` describes coordination patterns for cycle-synchronized scenarios. `coord_split_schedule.py` expands them into `<TSSU>_coord_splits.csv`, which the report uses to annotate programmed splits.

## Run

```bash
py software_validate.py                    # replay the next pending batch; analyse and report when all are collected
py software_validate.py --report-only      # skip replay, refresh device_events CSVs, rebuild report.html
py software_validate.py --report-only-fast # skip replay and the CSV refresh, rebuild report.html only
py software_validate.py --archive          # after the report, export results/<version>/logs/<TSSU>.parquet
py software_validate.py --settle-minutes 5 --top-n 3 --settings other.json --verbose
```

One invocation does the following:

1. Reads the catalog, settings, and conflict pairs, and builds the test suite.
2. Selects the pending scenarios: catalog rows with a log file whose TSSU is not yet in `results/<software_version>/collected.db` (or is flagged `Replace on Rerun`). Up to one scenario per controller target.
3. Prints the load table (which configuration file goes on which controller) and waits for Enter.
4. Checks SNMP and HTTP on every controller in the batch.
5. Replays the batch. Similarity scenarios run once, time-of-day aligned, with conflict checking on but not stopping the run. Conflict scenarios run up to 25 times and stop at the first conflict.
6. If more scenarios are pending, exits. Run the script again (typically the next day) for the next batch.
7. When every scenario has data, compares each one against the baseline, writes `device_events/`, `divergence_plots/`, and `report.html`.

## Outputs

`results/<software_version>/`

| File | Contents |
|---|---|
| `collected.db` | DuckDB with the `events` collected from the controllers, detected `conflicts`, and stored `input_events`. |
| `device_events/<TSSU>.csv` | Baseline and new events for one intersection, aligned onto the same timeline. `DeviceId` is `baseline` or `new`. |
| `divergence_plots/*.png` | Gantt charts of the largest divergences and flagged clearance or operational issues. |
| `report.html` | Self-contained report: pass/fail summary, per-scenario match and timing scores, divergence details, phase and overlap difference tables, embedded charts. |
| `run.log` | Replay log. |
| `logs/<TSSU>.parquet` | Only with `--archive`. Collected events exported per intersection, usable as the next baseline's source logs. |

## Pass criteria

- **Similarity**: sequence match at least 95 percent and timing match at least 90 percent (events within 0.5 s), unless the scenario was thrown out because the replayed detector inputs themselves did not match the source well enough (`phase_call_similarity_threshold`). Thrown-out scenarios are reported but not counted.
- **Conflict**: pairs are configured, the baseline reproduces the conflict, and the new software does not.

## Validating the next release

1. Set `baseline_version` to the label you just finished and `software_version` to the new label.
2. Load the new software on the controllers.
3. Run `py software_validate.py` until every batch is collected. The comparison uses `results/<baseline_version>/collected.db`, so both versions are compared as collected, under identical replay conditions.

## Other files

- `extract_data.py --software-version <label>`: exports per-intersection Parquet files from a run that was driven by `BatchRunner.run()` (it needs that runner's `checkpoint.json`). Runs made with `software_validate.py` should use `--archive` instead.
- `walkthrough.ipynb`: the original interactive notebook version of this workflow, kept for reference. The script supersedes it.
