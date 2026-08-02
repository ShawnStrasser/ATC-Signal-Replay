#set page(
  paper: "us-letter",
  margin: 1in,
  numbering: "1",
  number-align: center,
)
#set text(font: "Times New Roman", size: 10pt, lang: "en")
#set par(justify: true, leading: 0.55em)
#set par.line(numbering: "1", numbering-scope: "page", number-margin: left, number-clearance: 4pt)
#set heading(numbering: none)
#show figure: set par.line(numbering: none)
#show table: set par.line(numbering: none)
#show math.equation: set par.line(numbering: none)
#set bibliography(style: "chicago-author-date")

#let title = [Automated Behavioral Regression Testing of Traffic-Signal Controllers Using Field-Derived Event Replay]

#align(center)[
  #v(0.45in)
  #text(size: 15pt, weight: "bold")[#title]
  #v(0.32in)
  #text(size: 11pt)[[AUTHOR NAME], [COAUTHOR NAME]] \
  [Job title], [Institution] \
  [email address] \
  ORCID: [optional]
  #v(0.28in)
  #text(size: 10pt)[Recommended category: traffic signal systems, operations, and control software]
  #v(0.35in)
  #align(left)[
    *Author review required.* Replace bracketed author, affiliation, contact, and acknowledgment text before submission.\
    Total manuscript pages: #context counter(page).display("1", both: true)
  ]
]
#pagebreak()

#align(center)[#text(size: 12pt, weight: "bold")[Structured Abstract]]
#v(0.18in)
*Objectives.* This study evaluates whether field-recorded controller inputs can be replayed through controller emulators to detect behavioral changes caused by a software release or an operational timing-parameter change. The transportation need is a repeatable regression screen that exercises realistic combinations of detection, pedestrian, and preemption activity.

*Methods.* Twenty-five production configurations were replayed from approximately 23-hour field event logs. The input stream used National Transportation Communications for Intelligent Transportation Systems Protocol (NTCIP) detector, pedestrian, and preemption objects. Outputs from a MAXTIME emulator were collected as high-resolution event traces, latency-compensated, grouped into signal-state intervals, and compared with sequence and timing scores. Two comparisons were evaluated: versions 2.15.1 versus 2.18.1, and version 2.18.1 versus the same software with trailing-overlap timing parameters changed.

*Findings.* The software-release comparison passed 19 of 25 configurations; six were candidate behavioral differences. In the known intervention, 11 of 14 changed configurations were flagged and all 11 unchanged configurations passed. Three changed configurations passed, showing that a replay detects an intervention only when the recorded operation exercises its affected behavior.

*Novelty.* Prior work has automated scripted controller tests and hardware-, software-, and emulator-in-the-loop experiments. This paper evaluates their combination with long-duration field-derived replay, automated high-resolution trace alignment, and repeated testing across production configurations for both software- and parameter-induced changes.

*Practical Applications.* Agencies can use the workflow as a release and timing-change screening step before field deployment. NTCIP input replay is portable in principle; an implementation still needs an output-log adapter and event-code mapping for each controller family.
#pagebreak()

= Introduction

Traffic-signal controller changes are often validated with short scripted checks, bench tests, or a small number of field observations. Those checks are valuable but may not exercise the combinations of detector calls, pedestrian service, preemption, coordination, and overlap behavior that occur during daily operation. A software release can therefore pass a functional checklist while changing a less common sequence. The same risk applies when the controller software is unchanged but timing parameters are edited.

This paper studies a complementary regression screen: replay a recorded operational event stream through a controller or emulator, collect the resulting high-resolution event trace, and compare it with a reference trace. The question is deliberately behavioral rather than vendor-specific:

> Can field-recorded controller inputs be replayed through controller emulators to automatically detect behavioral changes caused by either a software update or an operational timing-parameter change?

The implementation is an open-source Python package. Its input side uses NTCIP 1202-style detector, pedestrian, and preemption objects, so the replay concept can be adapted to compliant controllers. The evaluation used MAXTIME emulators and a MAXTIME HTTP event-log collector; those are adapters at the experiment boundary, not a claim that all NTCIP controllers work without integration. The paper contributes an auditable method and two controlled evaluations rather than a controller certification procedure.

= Related Work

Traffic-controller automated testing has a substantial prior history. Li et al. developed a controller automated-testing tool and script language for exercising controller functions (2008). The Idaho project similarly used an automated testing tool with XML scripts, a controller interface device, and NTCIP support for selected controller classes (Ahmed et al. 2010). TungÃ¢â‚¬â„¢s Florida projects extended automated NTCIP-based testing across multiple manufacturers and emphasized the benefit of a standard interface (Tung 2012, 2015). These studies establish the value and portability of automated functional testing, but their inputs are designed scripts rather than long operational traces.

Hardware-in-the-loop and software-in-the-loop environments provide another foundation. Stevanovic, Klanac, and Radivojevic evaluated six controllers, event codes, and high-resolution logging in a HILS setup (2017). Wang et al. described a virtual controller interface for HILS (2019). Those studies show how controller behavior and high-resolution event data can be evaluated in controlled environments. They do not evaluate repeated release and parameter regression across numerous production configurations using field-derived inputs.

Dynamic time warping (DTW) provides a general foundation for aligning sequences that are similar but not synchronized sample-for-sample (Sakoe and Chiba 1978). In this project, alignment is applied after converting event records to qualified signal-state intervals and grouping adjacent event records. The cautious contribution is therefore a combination claim: the reviewed literature did not identify an evaluation combining long-duration field-derived replay, automated high-resolution output-trace alignment, and repeated production-configuration testing for both software- and parameter-induced changes. NTCIP, emulation, high-resolution logging, automated testing, and DTW are not claimed as individually novel.

= Field-Event Replay and Comparison Method

== Inputs and replay

The source data comprise 25 Parquet event logs in the repositoryÃ¢â‚¬â„¢s validation log directory. Each file covers about 23 hours and contains a device identifier, timestamp, event identifier, and parameter. The complete source set contains 7,296,218 rows and approximately 575 source-hours. Replay selects detector, pedestrian, preemption, and related events required by the controller test configuration. The replay runner sends those events through the controller interface and records the resulting output events.

The package separates portable input semantics from controller-specific collection. NTCIP objects define the input vocabulary used by the replay. The evaluated MAXTIME adapter supplies output events through an HTTP event-log interface. A different controller family would require an output-collection adapter and a mapping from its event codes to the common comparison vocabulary. This boundary keeps the research question manufacturer-neutral without implying zero integration effort.

== Latency and output traces

Controller output timestamps are not assumed to have the same delay as source-log timestamps. The 2.18.1 runs used adaptive latency compensation: recent matched event pairs estimate an offset, and the runner updates the offset only after a minimum sample count. The stored 2.18.1 database contains 707,458 latency samples and 6,577 offset updates. The same compensation workflow is present in the parameter-intervention run. A fixed offset remains available for environments that cannot estimate latency online.

== Alignment and scoring

Events are restricted to the configured comparison identifiers and transformed into signal-state intervals. Adjacent compatible records are grouped within a 0.25-second tolerance. Invalid, missing, or unreliable intervals are marked in both timelines; a comparison window overlapping invalid data is excluded rather than reported as a difference. Edge-truncated intervals and diagnostic issues without five minutes of surrounding valid context are also excluded. These rules were added to the package with focused tests (commit `5eb309b`).

For each qualified rolling window, the comparison aligns the reference and candidate event sequences, then reports sequence match and timing match. Timing match is the fraction of matched events within 0.5 seconds. A configuration passes when sequence match is at least 95 percent and timing match is at least 90 percent, with phase-call reliability of at least 85 percent. A failed configuration is a screening candidate, not a confirmed defect: it requires review of the aligned trace, controller logs, and configuration history.

#figure(
  image("figures/workflow.svg", width: 100%),
  caption: [Workflow evaluated in this paper. Input replay is portable at the NTCIP object boundary; the current output collector is a MAXTIME adapter.],
) <fig-workflow>

#figure(
  image("figures/alignment-example.pdf", width: 82%),
  caption: [Illustrative alignment from the committed offline example. The synthetic candidate contains an additional included event; this figure explains grouping and comparison but is not a field result.],
) <fig-alignment>

= Experimental Design

The experiment uses three stored result databases. The first contains the reference 2.15.1 run, the second the 2.18.1 candidate run, and the third the 2.18.1 run after trailing-overlap timing parameters were changed. All three databases contain 25 configuration identifiers and completed simulation runs. The comparison/reporting reproduction reads these databases and the archived reports; it does not start a controller, so the paperÃ¢â‚¬â„¢s reproducible claim is limited to the comparison stage.

#table(
  columns: (1.5fr, 1fr, 1fr, 1fr),
  inset: 4pt,
  align: left,
  [*Item*], [*2.15.1*], [*2.18.1*], [*2.18.1 modified*],
  [Configurations], [25], [25], [25],
  [Output events], [7,574,994], [8,019,167], [7,993,042],
  [Input events], [627,240], [627,240], [627,240],
  [Controller/emulator hours], [about 585], [about 580], [about 577],
  [Replay source], [same 25 field logs], [same 25 field logs], [same 25 field logs],
) <tab-scope>

#table(
  columns: (2fr, 1.2fr), inset: 4pt, align: left,
  [*Comparison parameter*], [*Value*],
  [Grouping tolerance], [0.25 s],
  [Rolling window / step], [300 s / 60 s],
  [Sequence pass threshold], [95%],
  [Timing pass threshold], [90%],
  [Timing match tolerance], [0.50 s],
  [Phase-call reliability], [85%],
) <tab-thresholds>

The second experiment is a known intervention. Transaction histories identify 14 configurations with trailing-overlap or associated red-revert edits and 11 configurations without those edits. The manifest records the grouping and the fields observed in transaction histories. Detailed individual edits remain marked for author verification because the exploratory conflict script assumes an overlap-parent relationship that has not been validated as a general interpretation.

= Software-Release Results

The 2.15.1-to-2.18.1 report passed 19 of 25 configurations. The average sequence match was 96.2 percent and the average timing match was 93.8 percent. Six configurations failed at least one threshold: 12036, 2B045, 2B054, 2B085, 2B094, and 2B339. These are candidate behavioral differences, not confirmed software bugs. Their causes may include changed behavior, replay exposure, data validity, or remaining integration effects.

#figure(
  image("figures/software-release-summary.pdf", width: 100%),
  caption: [Sequence and timing scores for all 25 configurations in the 2.15.1-to-2.18.1 comparison. Dashed lines show the 95-percent sequence and 90-percent timing thresholds; candidate configurations require review.],
) <fig-software>

#table(
  columns: (1fr, .8fr, .8fr, .8fr), inset: 3pt, align: left,
  [*Configuration*], [*Sequence %*], [*Timing %*], [*Status*],
  [01066], [96.1], [91.9], [Pass], [03013], [99.0], [90.8], [Pass],
  [05018], [97.9], [98.2], [Pass], [08042], [98.6], [96.5], [Pass],
  [08404], [99.4], [96.5], [Pass], [08411], [98.6], [95.7], [Pass],
  [12035], [95.3], [90.7], [Pass], [12036], [75.4], [--], [Candidate],
  [12059], [99.1], [98.4], [Pass], [13008], [98.6], [96.6], [Pass],
  [13010], [98.9], [90.1], [Pass], [2B009], [97.9], [92.4], [Pass],
  [2B045], [87.7], [--], [Candidate], [2B049], [95.4], [92.6], [Pass],
  [2B052], [98.6], [92.0], [Pass], [2B054], [98.7], [89.0], [Candidate],
  [2B085], [95.9], [87.6], [Candidate], [2B094], [94.8], [--], [Candidate],
  [2B337], [99.9], [95.0], [Pass], [2B339], [84.5], [--], [Candidate],
  [2B349], [97.9], [93.7], [Pass], [2B530], [99.4], [95.6], [Pass],
  [2C009], [97.6], [92.1], [Pass], [2C042], [99.7], [96.1], [Pass],
  [2C043], [99.7], [98.6], [Pass],
) <tab-software>

= Parameter-Intervention Results

The same-software comparison is the methodÃ¢â‚¬â„¢s stronger validation. Of the 14 configurations with trailing-overlap or associated red-revert changes, 11 were automatically flagged and three passed. All 11 configurations without those edits passed. Thus the observed flag rate was 78.6 percent in the changed group and zero percent in the unchanged group. The three changed-and-passed cases demonstrate an important interpretation boundary: a recorded replay can detect a change only when the exercised behavior is sufficiently exposed in the trace.

#figure(
  image("figures/parameter-intervention-summary.pdf", width: 100%),
  caption: [Known-intervention results. Red points are changed configurations flagged by the comparison; amber points are changed configurations that passed; green points are unchanged configurations that passed.],
) <fig-parameter>

#table(
  columns: (1.4fr, .9fr, .9fr, 1fr), inset: 4pt, align: left,
  [*Group*], [*Configurations*], [*Flagged*], [*Passed*],
  [Changed parameters], [14], [11], [3],
  [Unchanged parameters], [11], [0], [11],
) <tab-intervention>

The intervention result supports a screening interpretation. It shows that the method responds to a deliberate operational change while software is held constant, and that it does not flag the unchanged group in this dataset. It does not estimate sensitivity for arbitrary timing changes, because the 14 changed configurations were selected from one transaction history and the three passed cases may not have exercised the affected overlap behavior.

= Discussion and Practical Use

The workflow is useful when a controller agency has a reference trace, a proposed software or configuration change, and access to a controller or emulator. A release engineer can first run the replay in batch, use the pass/fail summary to triage configurations, and then inspect aligned windows only for candidates. This is more informative than a raw event-count difference because the score is tied to qualified windows, timing tolerance, and validity rules.

The method is manufacturer-neutral in its research framing but not adapter-free. NTCIP replay objects provide a portable input boundary. A new controller family still needs an output collector, event-code mapping, timestamp/latency behavior, and validation of its event semantics. The MAXTIME implementation demonstrates feasibility, not universal plug-and-play compatibility.

The open-source package is part of the contribution. Commit `5eb309b` completes adaptive validation settings propagation, batch failure handling, and timeline reliability rules. Commit `1f25f36` documents the workflow, ignores the live settings file in favor of a tracked example, adds citation metadata, and provides a synthetic offline comparison test. The package is not presented as a certified testing product; maintenance and controller adapters remain project responsibilities.

= Limitations

First, the stored replay campaigns were executed with MAXTIME emulators and a MAXTIME event-log interface. Repeating the study on another controller family is a necessary external validation. Second, the field logs and configuration databases represent one agency context and one set of operational conditions. Third, the comparison script in this paper reproduces the archived comparison/reporting stage and cross-checks database counts; it does not rerun the controller campaigns. Fourth, the detailed intervention edits need author confirmation, and the exploratory conflict CSVs are excluded. Fifth, sensitivity reruns for grouping tolerance, phase-call reliability, and timing tolerance were not available in the archived reports and are explicitly marked `not_run` in the generated sensitivity table. Finally, the six software-release candidates have not been manually classified as defects, intended changes, or data artifacts.

= Conclusions

This evaluation supports a focused answer to the research question. Field-derived event replay can automatically screen controller behavior for both a software-release change and a same-software timing-parameter intervention. Across 25 configurations, the release comparison passed 19 and identified six candidates for review. In the deliberate intervention, 11 of 14 changed configurations were flagged while all 11 unchanged configurations passed. The result is a practical, reproducible comparison method and a credible basis for agency regression screening, not a claim of universal controller compatibility or automatic defect diagnosis.

The next high-value validation is to rerun the comparison on a second controller family through an adapter and to complete manual classification of the six release candidates and three changed-and-passed intervention cases. Publishing the comparison-only event bundle would allow independent reproduction of the alignment and reporting stage without distributing controller firmware or proprietary configuration databases.

= Acknowledgments

[AUTHOR: identify agency, project, and technical support acknowledgments; confirm any required institutional approval.]

= Code and Data Availability

The open-source package state used for this draft is on the repository `main` branch at commits `5eb309b` and `1f25f36`. The replay campaign still requires controller/emulator infrastructure. The repository contains an exporter for a candidate comparison-only bundle under `paper/reproducibility/`; it has not been generated or published. The author should review and approve the bundle before attaching it to GitHub, Zenodo, or another archive.

= Generative-AI Disclosure

OpenAI Codex was used to inspect repository files, draft analysis scripts, generate paper figures, and draft and format this manuscript. The authors reviewed the source materials and remain responsible for the accuracy, interpretation, citations, and final submission. The author must revise this statement if the actual submission uses additional generative-AI tools or materially different purposes.

#bibliography("references.bib")
