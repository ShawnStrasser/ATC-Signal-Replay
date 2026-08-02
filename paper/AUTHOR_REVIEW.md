# Author review handoff

## Current recommendation

- **Recommended title:** *Automated Behavioral Regression Testing of Traffic-Signal Controllers Using Field-Derived Event Replay*
- **Research question:** Can field-recorded controller inputs be replayed through controller emulators to automatically detect behavioral changes caused by either a software update or an operational timing-parameter change?
- **Main contribution:** An open-source, field-derived replay and high-resolution event-trace comparison workflow that screens both release changes and same-software timing interventions across 25 production configurations.
- **Strongest novelty claim:** The reviewed literature did not identify the combination of long-duration field-derived replay, automated high-resolution output-trace alignment, and repeated production-configuration testing for both software- and parameter-induced behavioral changes. This is a combination claim, not a first claim for NTCIP, emulation, high-resolution logging, automated testing, or dynamic time warping.
- **Recommended category:** Presentation and Publication if the paper is not published or under review elsewhere and the author wants possible Transportation Research Record consideration; otherwise Presentation-Only.

## Verified findings

- Twenty-five primary field logs contain 7,296,218 raw rows and approximately 575 source-hours. The replay databases each contain 25 configurations and 627,240 input events.
- The 2.15.1-to-2.18.1 report has 19/25 passes, 96.2% average sequence match, and 93.8% average timing match. The six candidate configurations are 12036, 2B045, 2B054, 2B085, 2B094, and 2B339. They are not called confirmed defects.
- The same-software 2.18.1-to-2.18.1-trailing report has 14/25 passes and 84.1% average sequence match. The intervention manifest groups 14 configurations with trailing-overlap/red-revert edits and 11 without. The comparison flagged 11/14 changed configurations and 0/11 unchanged configurations. Three changed configurations passed; they are exposure-sensitive cases, not automatically false negatives.
- The 2.18.1 database contains 707,458 adaptive-latency samples and 6,577 offset updates. The package changes in commit `5eb309b` propagate settings, fail batches on output-collection errors, and make timeline validity handling symmetric and tested.
- The comparison-stage result tables and figures are under `paper/generated/results/` and `paper/figures/`. The reproduction script cross-checks archived report rows against database counts and hashes; it does not start a controller.

## Author decisions required

1. Replace all bracketed title-page fields with author names, job titles, affiliations, e-mails, ORCIDs, and final acknowledgments.
2. Confirm the 14-versus-11 intervention grouping and the individual changed parameter names/overlap numbers. The manifest currently has `author_verified=false`; the exploratory conflict script and CSVs remain excluded.
3. Decide whether any of the six software-release candidates or three changed-and-passed intervention cases were manually checked, and add the classification and evidence.
4. Decide whether to rerun the one-factor sensitivity analysis (grouping tolerance 0.10/0.25/0.50 s; phase-call reliability 80/85/90%; timing tolerance 0.25/0.50/1.00 s). The current `sensitivity_results.csv` is explicitly `not_run_from_archived_reports`.
5. Confirm Presentation-Only versus Presentation and Publication, institutional approval, coauthor approval, and whether the paper is under review elsewhere.
6. Verify every reference against the original publication and check the final TRB deadline time in Editorial Manager. The current Editorial Manager page says the 2027 site is under development.
7. Review and approve the generative-AI disclosure and the proposed comparison-only data bundle.

## Submission blockers

- Author metadata and approvals are missing.
- Detailed intervention edits are not author-verified.
- Six software-release candidates remain unexplained and are not manually classified.
- Sensitivity reruns have not been performed.
- The current PDF is a complete draft, not submission-ready until the preceding decisions are resolved.

## High-value improvements

- Rerun sensitivity settings and report classification stability.
- Manually classify candidate windows and add one concise example of a confirmed behavioral difference.
- Repeat the output-collection adapter on a second controller family to test the manufacturer-neutral portability claim.
- Add a DOI or archival release after the author approves public code/data.

## Repository-release recommendation

- **Publish:** package source, tests, `CITATION.cff`, changelog, documentation, settings example, offline comparison example, paper source, figures, and the comparison scripts.
- **Candidate for approval:** `paper/reproducibility/export_dataset.py` can create a bundle with replay inputs, three output-event tables, comparison parameters, intervention manifest, and expected results. The bundle would reproduce comparison/reporting only.
- **Keep private until approved:** controller firmware, proprietary `.bin` configuration databases, live settings, and any infrastructure-specific deployment material. The local trailing-conflict script and CSVs should remain private until their assumptions are reviewed.
- **Data sensitivity:** no deidentification is currently recommended; the recorded identifiers and local host/port information were described as non-sensitive. The author should still complete an agency/vendor approval check before publication.
- **Commit/release strategy:** local `main` now contains `5eb309b` (`Complete adaptive validation and timeline reliability handling`) and `1f25f36` (`Document and test the reproducible behavioral validation workflow`). Do not push, tag, publish to PyPI, or create a GitHub release without author approval. If approved, push these commits and create `v0.2.0`; otherwise cite commit `1f25f36`.

## Build and validation

- Build command: `typst compile paper/manuscript.typ paper/generated/trb-paper.pdf` (Typst 0.15.1 was installed temporarily from the official release for this build; no binary or font was committed).
- PDF: `paper/generated/trb-paper.pdf`.
- Page count: 8 pages, including title page and structured abstract; below the 20-page limit.
- Validation: all maintained tests under `tests/` pass with `MPLBACKEND=Agg`: **134 passed, 42 skipped**. The unfiltered repository-wide pytest invocation stopped during collection after four archived `experiments/` errors (missing old database, removed helper import, and two binary `.txt` files); those are not maintained package tests and were not changed.
- Results commands:
  `py -3 paper/analysis/reproduce_results.py --config paper/analysis/paper_config.toml`\
  `py -3 paper/analysis/make_figures.py`\
  `typst compile paper/manuscript.typ paper/generated/trb-paper.pdf`\
  `py -3 paper/analysis/validate_outputs.py`
- The rendered pages were inspected as a contact sheet and contain one-column text, embedded paper-specific figures/tables, page numbers, and line-number margins. No figures from the large diagnostic HTML reports are used.
- Git status at handoff should be checked immediately; the three existing untracked trailing-conflict artifacts were intentionally preserved and excluded.
