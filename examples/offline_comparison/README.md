# Offline comparison example

This small example demonstrates the comparison stage without a controller or emulator.

Run from the repository root:

```powershell
py examples/offline_comparison/run_example.py
```

The script compares a reference event sequence with an unchanged candidate and a candidate containing one additional event. It prints the match percentage and writes `summary.json` beside the example files.