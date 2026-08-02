# Author review handoff

## Current recommendation

- **Recommended title:** *Feasibility of Field-Derived High-Resolution Event Replay for Traffic-Signal Controller Testing*
- **Research question:** Can high-resolution event logs collected from live field controllers be converted to NTCIP calls, replayed to test controllers, and compared automatically to identify operational differences in the resulting output sequences?
- **Answer supported by the study:** Yes. Separate same-software campaigns produced 99.6% mean sequence match and 97.7% mean timing match in the 11 unchanged configurations, while the comparison still flagged deliberate parameter changes and release-related divergences.
- **Main contribution:** A field-derived regression method that converts approximately 23 hours of actual detector, pedestrian, and preemption activity into reusable controller tests and aligns complete output traces with Jaccard-cost dynamic time warping.
- **Strongest novelty claim:** Prior work automated scripted controller tests and controller-in-the-loop evaluation. The reviewed literature did not identify a study combining long-duration field-derived input replay, complete high-resolution output-trace alignment, and repeated testing across numerous production configurations.
- **Recommended submission option:** Presentation and Publication, provided the work is not published or under review elsewhere and the author wants possible Transportation Research Record consideration.
- **Title-page clarification:** The submission option/category is selected in Editorial Manager; it is not required on the manuscript title page and has been removed. ORCID is optional, so the absence of one is not a blocker.
- **Line-number clarification:** TRB explicitly requires line numbers and requires them to restart at 1 on every page. They are visually intrusive, but removing them risks desk rejection.

## Verified findings

- **Scope:** 25 field logs, 7,296,218 raw high-resolution records, and 574.998 source configuration-hours; approximately 23 hours per signal. Each replay database contains 25 configurations and 627,240 replay commands.
- **Direct repeatability result:** The 11 configurations with no applicable timing edit between two version 2.18.1 campaigns all passed. Mean sequence match was 99.6% (range 99.1--100.0%); mean timing match was 97.7% (range 90.6--99.7%). This is the paper's strongest feasibility evidence.
- **Known operational intervention:** Transaction histories identify 14 configurations with trailing-yellow, trailing-red, or associated red-revert edits and 11 without relevant edits. The method flagged 11/14 changed and 0/11 unchanged configurations. The three changed configurations that passed scored 99.3%, 99.9%, and 100.0%, consistent with insufficient exposure to the affected behavior in the recorded day.
- **Software release:** Version 2.15.1 versus 2.18.1 produced 19 passes and six review candidates (12036, 2B045, 2B054, 2B085, 2B094, and 2B339), with mean sequence match 96.2% and mean reported timing match 93.8%.
- **Manual interpretation supported by the presentation:** One divergence reflected correction of a known 2.15.1 rail-preemption exit bug. Another reflected changed transition-state reporting with no noticeable operational effect. The timing intervention removed short/variable overlap-yellow behavior without broad unintended changes in the reviewed example.
- **Verified comparison method:** Timestamp-grouped sets of `(event_id, parameter)` pairs; Jaccard local cost; standard monotone DTW recurrence; 45-minute windows advanced every 40 minutes; 60-second edge clip; 0.25-second grouping tolerance; 95% sequence threshold; 90% timing threshold with 0.50-second timing tolerance; 85% phase-call reliability filter.
- **Portability boundary:** Input replay uses NTCIP 1202 vehicle, pedestrian, and preempt objects. The evaluated output collector is MAXTIME-specific. Other controller families require an output adapter and event mapping; the manuscript does not claim zero-effort compatibility.

## Author decisions required

1. Confirm that the presentation's preemption-exit, short-overlap-yellow, and transition-state graphics may be included in the submitted paper, and provide the configuration IDs/timestamps if you want those stated in captions.
2. Confirm the interpretation of the 14 changed versus 11 unchanged configurations. The transaction evidence supports this grouping, but the presentation phrase “added to all overlaps” could be read as all 25 configurations.
3. Classify the remaining software-release candidates if possible: intended behavior, bug fix, operationally immaterial, replay artifact, or unexplained. The paper currently makes no unsupported defect claim.
4. Supply final acknowledgments, any required ODOT disclaimer, and confirmation of agency/vendor approval for submission and public release.
5. Choose Presentation-Only or Presentation and Publication in Editorial Manager.
6. Approve the exact generative-AI disclosure and decide whether to publish the comparison-only dataset.

## Submission blockers

- Final author review of the three case-study interpretations and figures.
- Final classification or explicit “unexplained” designation for the six release candidates.
- ODOT/institutional approval, acknowledgment/disclaimer language, and the final submission-option decision.
- A public code citation: push the current commits and create `v0.2.0`, or cite the exact final commit hash.

## High-value improvements

- Run the planned one-factor sensitivity analyses; the current archived reports do not contain them.
- Add configuration IDs and time ranges to the case-study captions.
- Confirm the intervention manifest's detailed overlap numbers and change transactions.
- Validate the input and output adapters on a second controller family in future work; this is not required to submit the present feasibility study.

## Repository-release recommendation

- **Publish:** package source, tests, README files, `CITATION.cff`, changelog, settings example, offline comparison example, paper source, paper figures, and analysis scripts.
- **Publish after approval:** a comparison-only bundle containing replay inputs, three collected output sets, parameters, intervention manifest, and expected results. This would reproduce the comparison/reporting stage without controllers or emulators.
- **Exclude:** controller firmware and proprietary `.bin` configurations because they are unnecessary for offline comparison. Leave the exploratory trailing-conflict script/CSVs private until their assumptions are validated.
- **Data handling:** no deidentification is needed based on the author's direction; identifiers, localhost addresses, ports, and timestamps may remain. Agency/vendor release approval is still recommended.
- **Release strategy:** keep the work on `main`, push only after review, then tag `v0.2.0`. Do not publish to PyPI, GitHub Releases, or a data archive automatically.

## Build and validation

- Build command: `typst compile paper/manuscript.typ paper/generated/trb-paper.pdf`
- PDF: `paper/generated/trb-paper.pdf`
- Page count: **13 pages**, including title and structured-abstract pages; below the 20-page maximum.
- Format: US Letter, one-inch body margins, Times New Roman 10 pt, single column, required page-restarting line numbers, and bottom-centered page numbers.
- Rendering check: all 13 pages were rendered to 850 x 1100 pixel images at 100 ppi. No page content touched or crossed the image bounds; the content bounds were consistent with the required margins and footer placement. Typst compiled without warnings after citation and equation corrections.
- Maintained package tests from the prior package update: **134 passed, 42 skipped**. Four archived `experiments/` collection errors remain outside the maintained suite.
- Result validation command: `py -3 paper/analysis/validate_outputs.py`

The draft is materially stronger and complete, but it should not be called submission-ready until the blockers above are resolved by the author.