# Firmware Validation Workspace

This folder is the source-of-truth input area for firmware validation runs.

## Directory Layout

```
firmware_validation/
	databases.xlsx           # Catalog of test entries (required)
	databases/               # Controller databases you manually load onto controllers
	logs/                    # Replay input event logs used by signal_replay
	conflict_monitor/        # Conflict pair definitions (conflict_pairs.json)
	results/                 # Output from replay runs
	test_suite.yaml          # Auto-generated test suite config (do not edit by hand)
	walkthrough.ipynb        # Main notebook for running validations
	get_data.ipynb           # Notebook for pulling raw event logs from the database
```

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
4. Run `walkthrough.ipynb`:
	 - Step 1 builds scenarios and batches from Excel.
	 - Step 2 runs baseline firmware capture.
	 - Step 3 runs new firmware capture.
	 - Step 4 compares outputs.
	 - Step 5 generates HTML report.

## Conflict Monitoring Behavior

- Similarity batches run with `stop_on_conflict=False`:
	- conflicts are still checked and recorded,
	- run continues even if conflicts are found.
- Conflict batches run with `stop_on_conflict=True`:
	- run stops when conflict is detected.
- Conflict checks occur during periodic collection (`collection_interval_minutes`) and once more during final end-of-run collection.
