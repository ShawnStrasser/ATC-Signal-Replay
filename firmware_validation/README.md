# Firmware Validation Workspace

This folder is the source-of-truth input area for firmware validation runs.

## Directory Layout

```
firmware_validation/
	databases.xlsx           # Catalog of test entries (required)
	databases/               # Controller databases you manually load onto controllers
	logs/                    # Replay input event logs used by signal_replay
	conflict_monitor/        # Conflict pair definitions (conflict_pairs.json)
	results/                 # Per-version replay output, DuckDB, CSVs, and reports
	walkthrough.ipynb        # Main notebook for running validations
	get_data.ipynb           # Notebook for pulling raw event logs from the database
```

Each replay run keeps `logs/` as the immutable source input folder. Collected
controller output is persisted in `results/<firmware_version>/collected.db`.
Analysis reads directly from that DuckDB file. If you explicitly want one
Parquet file per device under `results/<firmware_version>/logs/`, use the
archive/export path after the run.

For the normal behavior, first copy `settings.example.json` to `settings.json` and edit the paths, controller targets, and run labels. The live `settings.json` is intentionally ignored by Git because it contains run-specific values.\n\nUse `py firmware_validate.py --report-only` for the normal report-only flow.
That keeps the original report-only flow, including refreshing
`results/<firmware_version>/device_events/*.csv` before rebuilding the HTML.

For a faster report rebuild that skips the device CSV refresh, use
`py firmware_validate.py --report-only-fast`.

## `databases.xlsx` (Required Columns)

The first worksheet must include these columns:

- `TSSU` (required): Controller/site identifier used for file matching
- `Version` (required): Version label for the catalog row
- `Type` (required): `Similarity` or `Conflict`
- `Notes` (required): Freeform scenario notes

How it is used by `firmware_validation/walkthrough.ipynb`:

- Day 1 = first 12 rows where `Type = Similarity`
- Day 2 = next 12 rows where `Type = Similarity`
- Day 3 = first 4 rows where `Type = Conflict`

If there are fewer than 4 `Conflict` rows, set `manual_conflict_tssu` in the notebook.

## File Naming Conventions

### Logs (`firmware_validation/logs`)

For each TSSU used in testing, provide one replay input log file named with TSSU prefix.

Supported lookup order in notebook:

1. `<TSSU>.csv`
2. `<TSSU>.parquet`
3. `<TSSU>.db`
4. First file matching `<TSSU>*`

Examples:

- `12059.csv`
- `13010.parquet`
- `03007_pattern_4.db`

### Controller Databases (`firmware_validation/databases`)

These files are not uploaded automatically by this package; they are references for operator prompts and manual loading.

Recommended naming:

- `<TSSU>.bin` (preferred)
- or any file prefixed by `<TSSU>`

Examples:

- `12059.bin`
- `03007_OR22_at_25th_2-6-25`

## Operational Flow

1. Maintain `databases.xlsx` rows and `Type` labels.
2. Ensure matching replay log files exist under `logs/`.
3. Ensure matching database files exist under `databases/`.
4. Run firmware validation:
	 - Step 1 builds scenarios and batches from Excel.
	 - Step 2 replays source logs from `logs/` to the target firmware.
	 - Step 3 stores collected output in `results/<firmware_version>/collected.db`.
	 - Step 4 compares that collected output to `results/<baseline_version>/logs/` when available.
	 - Step 5 falls back to `logs/` only when the baseline version folder does not exist.
	 - Step 6 exports human-readable `device_events/*.csv` files and generates the HTML report.

## Conflict Monitoring Behavior

- Similarity batches run with `stop_on_conflict=False`:
	- conflicts are still checked and recorded,
	- run continues even if conflicts are found.
- Conflict batches run with `stop_on_conflict=True`:
	- run stops when conflict is detected.
- Conflict scenarios are grouped after all similarity batches, so they execute at the end of the run.
- Runtime conflict stopping happens on the final collection pass after each replayed run.
- Report generation recomputes conflict results from the saved output logs rather than reusing replay-time conflict table state.
