# Paper build

Run from the repository root:

```powershell
py -3 paper/analysis/reproduce_results.py --config paper/analysis/paper_config.toml
py -3 paper/analysis/make_figures.py
typst compile paper/manuscript.typ paper/generated/trb-paper.pdf
py -3 paper/analysis/validate_outputs.py
```

The first command reads the three stored DuckDB databases, checks the 25 configuration IDs, extracts the archived comparison rows, joins the intervention manifest, and writes `paper/generated/results/`. It does not require a controller. The archived HTML reports are evidence for the comparison/reporting stage; they are not used as paper figures.

The manuscript title page contains the single author information supplied by Shawn Strasser. The current PDF is 11 pages and uses the selected timeline explanation for Figure 2. The remaining author actions are final factual and visual approval, publication of Signal-Replay 0.2.0 to PyPI, any required ODOT approval, and the Editorial Manager selections listed in `AUTHOR_REVIEW.md`.
