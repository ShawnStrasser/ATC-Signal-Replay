# Paper build

Run from the repository root:

```powershell
py -3 paper/analysis/reproduce_results.py --config paper/analysis/paper_config.toml
py -3 paper/analysis/make_figures.py
typst compile paper/manuscript.typ paper/generated/trb-paper.pdf
py -3 paper/analysis/validate_outputs.py
```

The first command reads the three stored DuckDB databases, checks the 25 configuration IDs, extracts the archived comparison rows, joins the intervention manifest, and writes `paper/generated/results/`. It does not require a controller. The archived HTML reports are evidence for the comparison/reporting stage; they are not used as paper figures.

The current manuscript intentionally uses author placeholders on the title page and marks sensitivity reruns and detailed parameter verification for review. Do not submit until those items are resolved.
