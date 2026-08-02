# 13008 reviewer reproduction example

This folder regenerates one HTML comparison report used by the paper. It is a
saved-output example: it does **not** replay a controller, contact a network
device, include firmware, or include a proprietary controller configuration.

`events.csv` contains the saved controller-output events for configuration
13008. `baseline` rows came from the MAXTIME 2.15.1 comparison run; `new` rows
came from the saved MAXTIME 2.18.1 replay run. The runner uses the repository's
existing comparison and HTML-report code to compare those two streams again.

## Run it

From the repository root:

```powershell
py -m pip install -e . duckdb
py paper/reproducibility/13008/reproduce_report.py
```

The command writes `reproduced-report.html` here and verifies a pass with 98.6%
sequence match and 96.6% timing match. Open that self-contained file in a
browser. No controller address, emulator, or private file is required.

`expected-report.html` is the already-verified report committed with this
example. `expected-results.json` records the thresholds and expected result.

## Reproduction boundary

This reproduces **comparison and report generation** only. Original controller
replay requires controller/emulator software, configuration databases, and
agency test infrastructure, none of which are included here.
