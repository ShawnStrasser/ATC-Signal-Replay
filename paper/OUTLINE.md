# Paper outline

## Title page and structured abstract
Use the verified single-author information. Do not print a submission category or ORCID. The structured abstract answers the feasibility question with the repeatability, intervention, release, and zero-conflict findings.

## 1. Introduction
State the practical problem immediately: staff cannot manually toggle a full day of interacting inputs across 25 production timing configurations. Present field operation as a reusable test script. Define feasibility as repeatable alignment plus retained operational divergences.

## 2. Related work
Use three short paragraphs only: scripted automated controller tests; controller/emulator and high-resolution logging studies; DTW and the remaining field-replay gap. Avoid general traffic-signal background.

## 3. Replay and test architecture
Describe the 23-hour field logs, event-to-NTCIP conversion, time-of-day replay, emulator accommodations, parallel batches, latency compensation, and product-adapter boundary. Figure 1 is the workflow; Table 1 maps vehicle, pedestrian, and preempt events.

## 4. Output alignment and automated checks
Give the Jaccard and DTW equations, 45-minute/40-minute windowing, sequence/timing thresholds, and phase-call reliability screen. Explain clearance, pedestrian, preemption, and transition diagnostics. Give the virtual conflict monitor prominent treatment: 863 incompatible pairs across 21 configurations and zero stored conflicts. Figure 2 explains alignment; Figure 3 is `example_report.png`.

## 5. Experimental design
Define the 2.15.1-to-2.18.1 release comparison and the same-software trailing-overlap intervention. Table 2 contains only the essential design information.

## 6. Results
Lead with the 11 unchanged configurations (99.6% sequence, 97.7% timing), then the 11/14 changed and 0/11 unchanged intervention detection table. Use the short-overlap-yellow case as the timing-change example. Do not use the former parameter-intervention or software-release aggregate charts.

Summarize the release as 19 aggregate passes and six review cases. Identify 12036 as likely peer-to-peer test-environment failure. Emphasize that passed configurations still produced useful diagnostics: 13008 rail-preemption bug fix, 2B049 faster transition and possible transition-state issue, and 13008 expected dynamic red-clear extension. Use the preempt and transition figures.

## 7. Discussion
Sell the operational significance without hype: comprehensive full-day inputs, concurrent production configurations, automated review of millions of events, incident replay, and focused staff review. Explain baseline approval and NTCIP adaptability.

## 8. Limitations and conclusions
State the one-platform emulator limitation, field-exposure limitation, likely 12036 environment issue, five incompletely classified review cases, engineering thresholds, and comparison-only reproducibility. Conclude that the method is feasible and materially expands acceptance-test coverage.

## Acknowledgments and availability
Thank Chris Primm for reviewing results and contributing testing ideas. Cite the open-source package after `v0.2.0` or a final commit is public. Include the required AI disclosure.