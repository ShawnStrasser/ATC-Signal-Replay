# Author review handoff

## Current recommendation

- **Title:** *Feasibility of Field-Derived High-Resolution Event Replay for Traffic-Signal Controller Testing*
- **Research question:** Can high-resolution field events be replayed to test controllers and the resulting output sequences compared automatically for operational differences?
- **Answer:** Yes. Separate unchanged campaigns averaged 99.6% sequence match and 97.7% timing match, while the method retained software and timing divergences.
- **Contribution:** An open-source workflow that replaces much manual input toggling with full-day, parallel testing of production configurations; DTW alignment; clearance, pedestrian, preemption, and transition diagnostics; and a software-based virtual conflict monitor.
- **Novelty:** The reviewed literature did not identify prior work combining long-duration field-derived input replay with complete output-trace alignment across numerous production configurations.
- **Submission option:** Presentation and Publication, if the paper is not published or under review elsewhere.

## Verified findings

- 25 field logs contain 7,296,218 records and 574.998 configuration-hours, approximately 23 hours per signal. Each campaign contains 627,240 replay commands.
- All 11 unchanged configurations passed across separate version 2.18.1 campaigns: 99.6% mean sequence match (99.1--100.0%) and 97.7% mean timing match (90.6--99.7%).
- Eleven of 14 configurations with applicable trailing-overlap edits were flagged; none of the 11 unchanged configurations was flagged.
- Version 2.15.1 versus 2.18.1 produced 19 aggregate passes and six review cases. Configuration 12036 likely failed because peer-to-peer operation was not configured correctly in the test environment.
- Detailed diagnostics confirmed the version 2.18.1 rail-preemption exit fix at 13008, a faster short-way transition at 2B049, a possible state-reporting issue at 2B049, and consistent expected dynamic red-clear extension at 13008.
- The virtual conflict monitor evaluated 863 incompatible pairs across 21 configurations. The stored conflict tables contain zero conflicts in all three campaigns.
- The exact DTW method is recorded in the manuscript: timestamp-grouped event sets, Jaccard local cost, monotone DTW, 45-minute windows advanced every 40 minutes, 60-second edge clips, and separate timing and phase-call reliability checks.
- NTCIP 1202 provides the portable replay-input boundary. Other controller families still require an output adapter and event-code mapping.

## Author decisions required

1. Confirm that the 14 changed/11 unchanged intervention grouping is the intended description. Transaction histories support it, but “trailing settings added to all overlaps” could be read as applying to all 25 configurations.
2. If possible, classify the five release review cases remaining after the likely 12036 environment issue. They can be reported as unexplained if no stronger evidence is available.
3. Approve the case-study figures and the new `example_report.png` for publication.
4. Confirm any required ODOT disclaimer or institutional approval.
5. Choose the submission option and approve the generative-AI disclosure.
6. Decide whether to publish the comparison-only dataset.

## Remaining substantive gaps

- Five release-threshold review cases remain incompletely classified.
- The planned parameter-sensitivity reruns have not been completed. This is a useful robustness improvement, but the same-software repeatability and intervention results already provide direct validation.
- A final public software citation is needed: push the work and tag `v0.2.0`, or cite the exact final commit.

No additional figure is required. If one more strong figure is available, the most useful would be the 13008 phase 4 dynamic red-clear example showing that the report flagged an irregular interval and both versions handled the configured extension consistently.

## Repository release

Publish the source, tests, documentation, offline example, paper source/figures, and analysis scripts. After approval, publish a comparison-only bundle containing replay inputs, collected outputs, parameters, and expected results. Exclude controller firmware and proprietary `.bin` configurations. No deidentification is required based on the author's direction.

## Build and validation

- Build: `typst compile paper/manuscript.typ paper/generated/trb-paper.pdf`
- PDF: `paper/generated/trb-paper.pdf`
- Page count: **10 pages**.
- Abstract: approximately 253 words, below the 300-word limit.
- All 10 pages rendered at 850 x 1100 pixels with no detected clipping or page-boundary contact.
- Quantitative output validation passes, including the 25-row release/intervention counts and the 99.6%/97.7% repeatability means.
- Maintained package tests from the package update: **134 passed, 42 skipped**.

The manuscript is close to submission-ready. The main research gap is the unresolved disposition of five release review cases; the remaining work is primarily author approval and public-release preparation.