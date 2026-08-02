# Paper outline

## Title page
Purpose: provide only the TRB-required title and author information. Content: *Feasibility of Field-Derived High-Resolution Event Replay for Traffic-Signal Controller Testing*; Shawn Strasser, P.E.; job title, ODOT affiliation, email, and total page count. Do not print a submission category. ORCID is optional and omitted.

## Structured abstract
Purpose: answer the feasibility question in the five required headings. Main result: unchanged same-software campaigns averaged 99.6% sequence and 97.7% timing match; known changes were flagged without flags in unchanged configurations. No figure/table. Missing: final author approval of wording.

## 1. Introduction
Purpose: explain why manual/scripted testing cannot reproduce a day of realistic field complexity. Main claim: high-resolution field logs can be treated as executable controller tests. Evidence: 25 approximately 23-hour traces and three replay campaigns. Define the two-part feasibility condition: repeatable alignment plus retained divergences.

## 2. Prior work and research gap
Purpose: directly distinguish the work from Ahmed/Li scripted testing, Tung NTCIP testing, Stevanovic logger/HILS evaluation, and Wang controller-in-the-loop work. Main claim: novelty lies in the combination of field-derived long-duration inputs and whole-output-trace regression, not any component. Table 1 summarizes the closest work. Missing: none beyond final reference proofreading.

## 3. Field-derived replay method
Purpose: make the experiment understandable without the repository. Describe 9 a.m.--8 a.m. capture, event filtering, repeated-state imputation, eight-bit detector grouping, preempt Boolean calls, NTCIP objects, emulator preparation, time-of-day scheduling, latency compensation, output collection, and portability boundary. Figure 1 workflow; Table 2 input mapping. Evidence: replay SQL/Python, NTCIP module, stored databases, presentation.

## 4. Dynamic time warping comparison
Purpose: explain and justify the actual math. Define timestamp event sets, Jaccard local distance, cumulative DTW recurrence, monotone path, 45-minute/40-minute rolling scoring, 60-second edge clip, sequence score, timing score, and phase-call reliability filter. Figure 2 alignment; Table 3 parameters. Evidence: `comparison.py` and archived report windows.

## 5. Experimental evaluation
Purpose: separate the release screen from the controlled same-software intervention. Table 4 defines version 2.15.1 versus 2.18.1 and 2.18.1 versus parameter-modified 2.18.1. Missing: author confirmation of the 14/11 interpretation.

## 6. Results
Order the evidence by strength:

1. Repeatability: all 11 unchanged configurations passed; 99.6% mean sequence and 97.7% mean timing match.
2. Parameter intervention: 11/14 applicable changes flagged and 0/11 unchanged flagged; Figure 3 and Table 5.
3. Short-overlap-yellow case study: Figure 4.
4. Software release: 19/25 pass; six review candidates; Table 6 and Figure 5.
5. Rail-preemption bug fix and transition-state case studies: Figures 6 and 7.

Missing: configuration IDs/timestamps for the three presentation figures and final classification of six release candidates.

## 7. Discussion
Purpose: explain why field replay is richer than manual testing, how incident traces can reproduce bugs and validate fixes, why a baseline is an executable oracle, why flags require engineering judgment, and how field realism trades off against controlled coverage. Explain that NTCIP makes the input boundary portable while output adapters remain product-specific.

## 8. Limitations
Purpose: state one product family/emulator, one agency/day, emulator-specific configuration accommodations, exposure limitations, engineering rather than optimized thresholds, unrun sensitivity analysis, incomplete candidate classification, and replay-versus-offline-reproduction boundary.

## 9. Conclusions
Purpose: answer yes, with numerical evidence. State that DTW aligned most controller output while preserving operational divergences. Emphasize regression screening, timing-change evaluation, and targeted bug replication—not automatic certification.

## Acknowledgments, availability, AI disclosure, references
Purpose: record agency context, open-source package and candidate data bundle, exact TRB AI disclosure, and 8 verified focused references. Missing: final acknowledgments/disclaimer, public release/commit citation, and author approval.