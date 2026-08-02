# Paper outline

## Title page
Purpose: identify the study and satisfy TRB metadata requirements. Main claim: the paper evaluates a manufacturer-neutral replay-and-comparison method. Evidence: repository version history and experiment records. Missing: author names, titles, affiliations, e-mails, ORCIDs, acknowledgments, final page count.

## Structured abstract
Purpose: state the transportation problem, method, findings, cautious novelty, and practical use in the required headings. Evidence: the two 25-configuration comparisons and stored report summaries. Figure/table: none. Missing: author confirmation of wording and submission category.

## 1. Introduction
Purpose: motivate regression risk when signal software or timing parameters change and define the research question. Claim: field-derived replay can test behavior under operationally realistic inputs. Evidence: 25 approximately 23-hour field logs and three replay campaigns. Cite Li et al., Idaho report, Tung, and controller-testing literature.

## 2. Related work
Purpose: distinguish scripted controller tests, HIL/SIL/emulator testing, high-resolution logging, and sequence alignment from this combined evaluation. Claim: the contribution is the combination and scale, not a first claim for any component. Evidence: literature comparison in EVIDENCE.md. Figure/table: none.

## 3. Field-event replay and comparison method
Purpose: define input selection, NTCIP objects, latency compensation, output collection, event grouping, validity filtering, sequence and timing scores, and qualification thresholds. Claim: the method turns a long operational trace into repeatable behavioral comparisons. Evidence: `src/signal_replay`, `firmware_validation/firmware_validate.py`, committed tests, and `paper_config.toml`. Figure 1 workflow; Figure 2 illustrative alignment.

## 4. Experimental design
Purpose: document 25 configurations, field-log duration, controller/emulator environment, two comparisons, and intervention manifest. Claim: the design separates release change from same-software timing intervention. Table 1 scope; Table 2 thresholds. Missing: author confirmation of detailed parameter transactions.

## 5. Software-release results
Purpose: report 2.15.1 versus 2.18.1 without treating failures as confirmed defects. Claim: 19/25 passed; six are candidate behavioral differences. Table 3 all configurations; Figure 3 scores. Evidence: archived report and extracted CSV.

## 6. Parameter-intervention results
Purpose: test a known positive control with 2.18.1 unchanged versus modified trailing-overlap parameters. Claim: 11/14 changed configurations were flagged, while 11/11 unchanged configurations passed; three changed configurations passed and are exposure-sensitive cases, not automatic false negatives. Table 4; Figure 4.

## 7. Discussion and practical use
Purpose: interpret screening value, portability, and review workflow. Claim: the method is manufacturer-neutral at the NTCIP input boundary but adapters remain necessary for output logs. Evidence: code interfaces and results. Discuss triage, not certification.

## 8. Limitations
Purpose: state emulator dependence, one agency/data source, one controller family adapter, archived-report reproduction rather than new replay, unrun sensitivity reruns, manual intervention verification, and unexplained software-release candidates. No appendix or supplement.

## 9. Conclusions
Purpose: answer the research question with the strongest defensible conclusion and identify next validation steps.

## Acknowledgments / Code and data availability / Generative-AI disclosure
Purpose: document support, public package commit, candidate comparison dataset, and TRB-required AI disclosure. Missing: author-approved language and approvals.

## References
Purpose: Chicago author-date sources only; verify each DOI/URL against the original publication.
