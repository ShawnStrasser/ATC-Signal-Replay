# Author review handoff

## Current recommendation

- **Title:** *Feasibility of Field-Derived High-Resolution Event Replay for Traffic-Signal Controller Testing*
- **Research question:** Can real high-resolution events experienced by live field controllers be replayed to test controllers and the resulting outputs compared automatically for operational differences?
- **Answer:** Yes. Independent unchanged campaigns averaged 99.6% sequence match and 97.7% timing match, demonstrating that the outputs were deterministic enough for automatic alignment. Known timing changes and a confirmed preemption bug fix remained visible.
- **Contribution:** An open-source workflow that substantially enhances manual testing with full-day, parallel testing of production configurations; DTW alignment; clearance, pedestrian, preemption, and transition checks; and a virtual conflict monitor.
- **Novelty:** The reviewed literature did not identify prior work combining replay of full-day events actually experienced by live field controllers with full-day operational-output event alignment across numerous production configurations.
- **Operational use:** ODOT now uses the workflow for controller-software acceptance.
- **Submission option:** Presentation and Publication, if the paper is not published or under review elsewhere.

## Verified findings

- The 25 field logs contain 7,296,218 records: approximately 23 hours per controller, or 575 hours summed across all controllers. Each campaign contains 627,240 replay commands.
- All 11 unchanged configurations passed across independent MAXTIME 2.18.1 campaigns: 99.6% mean sequence match (99.1--100.0%) and 97.7% mean timing match (90.6--99.7%).
- Eleven of 14 configurations with applicable overlap clearance-setting changes were flagged; none of the 11 unchanged configurations was flagged.
- MAXTIME 2.15.1 versus 2.18.1 produced 19 aggregate passes and six review cases. Configuration 12036 likely failed because peer-to-peer operation was not configured correctly in the test environment.
- At 13008, the comparison confirmed that MAXTIME 2.18.1 corrected a bug that could leave the custom ODOT Preempt 6 function active after its input ended.
- At 2B049, the aligned traces showed different short-way transition logic, but no operational difference was apparent. The trace also raised a possible transition-state reporting question.
- The clearance check detected a 3.0-second overlap yellow during preemption, below ODOT's 3.5-second review threshold and the 4.0-second median for that signal phase.
- The virtual conflict monitor evaluated 863 incompatible pairs across 21 configurations. The stored conflict tables contain zero conflicts in all three campaigns.
- Sequence DTW uses ordered event-group content, not timestamps, for its local cost. Timestamps define simultaneous groups and initial alignment; timing is scored separately after sequence alignment.
- The adaptive latency calibration periodically matches scheduled isolated detector-on inputs to controller-recorded detector-on events and updates each controller's replay offset to account for workstation/controller clock drift.
- NTCIP 1202 provides the controller-brand-neutral replay-input boundary. The current event-log collector reads MAXTIME logs and can be extended to other controller types.

## Author decisions required

1. If possible, classify the five release review cases remaining after the likely 12036 environment issue. They can be reported as unexplained if no stronger evidence is available.
2. Approve the case-study figures, captions, ODOT operational-use statement, and overlap/preemption interpretations.
3. Confirm any required ODOT disclaimer or institutional approval.
4. Choose the submission option and approve the generative-AI disclosure.
5. Decide whether to publish the comparison-only dataset and whether to tag software release `v0.2.0`.

The author has confirmed the 14 changed/11 unchanged grouping. The manifest records that confirmation separately from the detailed transaction-history interpretation, which has not been individually author-verified.

## Remaining substantive gaps

- Five release-threshold review cases remain incompletely classified.
- A final public software citation is needed: push and tag `v0.2.0`, or cite the exact final public commit.

No additional image is required. Figure 2 now uses the selected timeline explanation.

## TRB submission checklist actions

The official two-page `2027-TRB-Annual-Meeting-Paper-Submission-Checklist.pdf` was reviewed on 2026-08-01. The manuscript satisfies the checkable document requirements: complete paper, transportation relevance, title-page fields, separate structured abstract with the five required headings, PDF, US Letter, one-inch margins, Times New Roman at 10 pt or larger, single column and spacing, page and line numbering, embedded tables/figures, no appendix, and author-year citations.

The author must still:

- Enter the same single author and author order in Editorial Manager.
- Paste the identical structured abstract, including all five headings, into Editorial Manager.
- Select the submission option and topic/category in Editorial Manager.
- Complete final author and ODOT approval.

## Repository release

The public repository now includes the source, tests, documentation, offline synthetic example, and a verified 13008 comparison-stage example with saved output events and expected results. A broader multi-configuration bundle remains optional. Exclude controller firmware and proprietary `.bin` configurations. No deidentification is required based on the author's direction.

## Build and validation

- Build: `typst compile paper/manuscript.typ paper/generated/trb-paper.pdf`
- PDF: `paper/generated/trb-paper.pdf`
- Page count: **11 pages**.
- Structured abstract: **291 words**.
- PDF page size: 612 x 792 points (US Letter); no right-margin overflow was detected.
- Extracted manuscript, table, workflow, and alignment-figure text is Times New Roman at 10 pt or larger. Natural mathematical subscripts are smaller.
- The Experimental Design section and Table 2 begin together; the table does not split across pages.
- Quantitative output validation passes, including the 25-row release/intervention counts, 99.6%/97.7% repeatability means, and zero stored virtual conflicts.
- Maintained package tests from the package update: **134 passed, 42 skipped**.

Independent reviews of publication readiness, external sources, and prose found no remaining scientific or novelty blocker after the corrections documented above. The manuscript is technically ready for submission, pending the author's final visual/factual approval, submission selections, AI-disclosure approval, and any required ODOT approval. The five incompletely classified release cases remain disclosed as review cases and do not invalidate the feasibility finding.