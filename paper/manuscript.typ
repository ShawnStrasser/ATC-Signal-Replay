#set page(paper: "us-letter", margin: 1in, numbering: "1", number-align: center)
#set text(font: "Times New Roman", size: 10pt, lang: "en")
#set par(justify: true, leading: 0.50em)
#set par.line(numbering: "1", numbering-scope: "page", number-margin: left, number-clearance: 4pt)
#set heading(numbering: none)
#show heading.where(level: 1): it => block(above: 10pt, below: 4pt)[#text(size: 12pt, weight: "bold")[#upper(it.body)]]
#show heading.where(level: 2): it => block(above: 7pt, below: 3pt)[#text(size: 10.5pt, weight: "bold")[#it.body]]
#show figure: set par.line(numbering: none)
#show table: set par.line(numbering: none)
#show math.equation: set par.line(numbering: none)
#set bibliography(style: "chicago-author-date")

#let title = [Feasibility of Field-Derived High-Resolution Event Replay for Traffic-Signal Controller Testing]

#align(center)[
  #v(0.55in)
  #text(size: 15pt, weight: "bold")[#title]
  #v(0.42in)
  #text(size: 11pt, weight: "bold")[Shawn Strasser, P.E., M.S.] \
  Traffic Signal Operations Engineer \
  Oregon Department of Transportation \
  ORCID: #link("https://orcid.org/0009-0004-5468-7669")[0009-0004-5468-7669] \
  shawn.strasser12\@gmail.com
  #v(0.45in)
  #text(size: 10pt)[Total manuscript pages: #context counter(page).final().first()]
]
#pagebreak()

#align(center)[#text(size: 12pt, weight: "bold")[Structured Abstract]]
#v(0.18in)
*Objectives.* This study evaluates whether recorded high-resolution field events from live traffic signal controllers can be replayed to test controllers and their output sequences automatically aligned and compared to identify operational differences.

*Methods.* An open-source Python package converts high-resolution logs to National Transportation Communications for Intelligent Transportation Systems Protocol (NTCIP) calls and replays them to test controllers. Output events are grouped into sets of near-simultaneous events and aligned using dynamic time warping with Jaccard distance between event sets. It also checks clearances and incompatible phases and overlaps. Approximately 23 hours of events from each of 25 production configurations were replayed to controller emulators. Separate runs tested repeatability, known timing changes, and practical use for software-version acceptance.

*Findings.* Controller outputs from separate unchanged runs aligned with 99.6 percent mean sequence match and 97.7 percent mean timing match, demonstrating that controller responses to field-derived replay were deterministic enough for automatic comparison. The method flagged 11 of 14 configurations with deliberate overlap clearance-setting changes and none of the 11 unchanged configurations. Applied to a software update, it identified differences within similar full-day operation, including correction of a rail-preemption exit bug. No incompatible signal phases or overlaps were detected among the 21 configurations with defined incompatible pairs.

*Novelty.* The study combines replay of full-day events experienced by live field controllers with automatic alignment of operational-output sequences across numerous production timing configurations.

*Practical Applications.* The open-source workflow expands acceptance testing beyond manual input toggling. Agencies can replay signal activity across many configurations, identify unexpected differences before field deployment, reproduce field failures, and test software or timing corrections against the same inputs. The author now uses the workflow for software-version acceptance testing at the Oregon Department of Transportation (ODOT).
#pagebreak()

= Introduction

Controller acceptance testing is commonly limited by staff time and by the inputs staff can reasonably operate by hand. Manually toggling detector, pedestrian, and preemption inputs remains useful for checking individual functions, but it cannot reproduce a full day of interacting calls, coordination transitions, and preemption activity across many production timing configurations.

This project uses field operation as the test sequence. Approximately 23 hours of high-resolution events actually experienced by each of 25 live field controllers are converted to National Transportation Communications for Intelligent Transportation Systems Protocol (NTCIP) calls and replayed to test controllers. The resulting new output events are compared with the original field-derived baseline. The central question is:

#quote(block: true)[Can high-resolution field events be replayed to test controllers, and can the resulting output sequences be aligned and compared automatically to identify operational differences?]

Feasibility requires both repeatability and sensitivity to change. Replaying the same inputs must produce controller outputs similar enough to align reliably, while actual changes in signal operation must remain visible. The comparison must also direct staff to important differences in signal phases, clearances, pedestrian service, preemption, coordination, and incompatible movements without requiring them to inspect millions of event records.

The evaluation covers 25 production configurations and three emulator campaigns using MAXTIME controller software: version 2.15.1, version 2.18.1, and version 2.18.1 after overlap trailing yellow and red clearance settings were changed to prevent occasional truncated yellow and red intervals. The primary contribution is evidence that field-derived controller outputs were repeatable enough for automated comparison and that the method successfully identified real operational differences. The specific software versions provide the case study; the finding is that this form of testing works.
= Related Work

Li et al. developed a controller automated-testing tool using its CIDScript language @li2008. A subsequent Idaho Transportation Department system used Extensible Markup Language (XML) scripts to activate inputs and verify predefined responses @ahmed2010. Tung first demonstrated automated testing on a National Electrical Manufacturers Association (NEMA) TS2 Type-1 controller @tung2012, then reported 20 NTCIP-based test programs evaluated on five compliant controller models @tung2015. These systems established the value of repeatable automated tests. The present method differs by replaying real events recorded as they occurred at live field controllers and using a prior operational-output trace as the expected response rather than requiring staff to author every input and assertion.

Controller-in-the-loop studies have connected controllers and emulators to traffic simulation through physical or virtual interfaces @wang2021. Stevanovic, Klanac, and Radivojevic evaluated high-resolution logging across six controller vendors @stevanovic2017. A recent Oregon Department of Transportation (ODOT) controller-in-the-loop study evaluated timing changes across 15 microsimulation models @hurwitz2026. Those studies support controlled controller environments and platform-specific output validation. The present study instead replays inputs recorded during live field operation and compares operational-output event traces across production configurations.

High-resolution event records provide tenth-second controller and detector histories @sturdevant2012. Dynamic time warping (DTW) provides a monotone alignment for sequences with local timing variation @sakoe1978. The literature reviewed for this study did not identify a prior evaluation combining full-day replay of events recorded from live field controllers, automatic operational-output event alignment, and repeated software and timing tests across numerous production configurations.

= Replay and Test Architecture

#figure(
  image("figures/workflow.svg", width: 100%),
  caption: [Field-derived testing workflow. Replay inputs use NTCIP 1202 objects. The current event-log collector reads MAXTIME logs and can be extended to other controller types.],
) <fig-workflow>

== Field event conversion

Each of the 25 source logs spans approximately 9:00 a.m. to 8:00 a.m. the next day. Together they contain 7,296,218 records: approximately 23 hours per controller, or 575 hours when summed across all controllers. Replay selects vehicle detector, pedestrian detector, and preemption changes. Table 1 shows the conversion implemented in the package @sturdevant2012.

#text(size: 10pt)[#table(
  columns: (1.25fr, 1fr, 1.1fr, 2.3fr), inset: 3pt, stroke: 0.5pt,
  [*Input*], [*Event codes*], [*Parameter*], [*NTCIP replay state*],
  [Vehicle detector], [81 off; 82 on], [Detector 1--64], [Update one bit in an eight-detector group and write the cumulative integer.],
  [Pedestrian detector], [89 off; 90 on], [Detector 1--64], [Update one bit in an eight-detector group and write the cumulative integer.],
  [Preemption], [104 off; 102 on], [Preempt number], [Write the corresponding Boolean preempt state.],
)]
#figure.caption([Table 1. Conversion from field events to replay inputs.])

Repeated on/off records are repaired by imputing the missing opposite transition. ODOT reserves detector numbers 65 and above for nonstandard uses, so those detector numbers are excluded from replay. Before replay, all calls are reset to zero. The package then sends Simple Network Management Protocol (SNMP) SET operations to NTCIP 1202 vehicle, pedestrian, and preempt objects at the recorded time of day @ntcip1202. Preserving time of day also exercises coordination and time-of-day plan changes.

The evaluated implementation used MAXTIME emulators loaded with production databases. Emulator accommodations removed detector delay/extension, disabled unused input/output modules, removed inverted preempt logic, used localhost peer addresses, and mapped overlap pedestrian calls to reserved detector inputs because MAXTIME was not accepting overlap pedestrian calls through NTCIP. Output events were collected through a MAXTIME Hypertext Transfer Protocol (HTTP) interface. Because replay inputs are sent through NTCIP 1202 objects, the input side is not tied to MAXTIME or another controller brand. The event-log collector currently reads MAXTIME logs; it can be extended to another controller family by mapping that controller's events to the common timestamp/event/parameter format.
== Parallel replay and latency control

Signals assigned to available emulator targets run concurrently. The 25 configurations were processed in batches over approximately one week without requiring staff to operate each input. Based on this deployment, future releases are expected to require only a few hours of staff preparation and review, although labor was not measured formally.

The workstation clock was found to drift relative to the test-controller clocks during the long replays. Without correction, the same input could gradually be recorded at a different controller time. The replay therefore schedules each command early by a per-controller latency offset. At periodic intervals, the package matches isolated detector-on inputs with the corresponding detector-on events recorded by the controller, compares the scheduled send time with the controller timestamp, and updates the offset to the measured median delay. The offset changes gradually during playback to avoid an abrupt timing jump. The MAXTIME version 2.18.1 release database contains 707,458 latency samples and 6,577 applied updates. Each campaign contains 627,240 replay commands and between 7.57 and 8.02 million controller-output events.

= Output Alignment and Automated Checks

== Discrete-event dynamic time warping

Events recorded at the same time, or within a 0.25-second grouping window, are represented as one set of (event identifier, parameter) pairs. This prevents the arbitrary database order of simultaneous events from affecting the result. The timestamps define the groups and support the initial time alignment, but they are not part of the DTW sequence cost. The comparison retains 33 specified operational-output event codes; input events and redundant output codes are excluded. Let $A_i$ and $B_j$ be the $i$th and $j$th event groups in the reference and candidate sequences. Their local sequence cost is Jaccard distance:

$ d(A_i, B_j) = 1 - frac(abs(A_i ∩ B_j), abs(A_i ∪ B_j)). $ <eq-jaccard>

Identical event groups cost zero. Missing or additional events increase the cost. The cumulative DTW cost is

$ D(i,j) = d(A_i, B_j) + min(D(i-1,j), D(i,j-1), D(i-1,j-1)), $ <eq-dtw>

with $D(0,0)=0$ and inaccessible borders set to infinity. Backtracking produces an order-preserving path based on event content. Horizontal and vertical path segments identify additional or missing event groups. After this sequence alignment is established, the timing comparison separately evaluates the controller timestamps of event groups that match exactly.

#figure(
  image("figures/alignment-example.pdf", width: 92%),
  caption: [Simplified event-sequence alignment. DTW aligns matching event groups by content and flags the additional Phase 6 call. After the sequence alignment is established, timestamp differences are evaluated separately.],
) <fig-alignment>

Sequence scores are calculated in 45-minute windows advanced every 40 minutes. Sixty seconds are clipped from each window edge to avoid penalizing a cycle split at a boundary. Sequence match is the percentage of aligned event-group pairs with zero Jaccard distance. Timing match is the percentage of exactly matched groups within 0.50 seconds after removing the remaining median clock offset from their timestamp differences. A configuration passes at 95 percent sequence match and 90 percent timing match. These are operational screening thresholds for staff review, not statistically calibrated decision boundaries.

Vehicle phase-call similarity provides a separate replay-reliability check. Windows below 85 percent are displayed but excluded from the configuration average. This helps distinguish controller differences from a test environment that did not reproduce the intended inputs.

== Operational checks and virtual conflict monitor

The automated review is broader than sequence scoring. It compares phase and overlap intervals, clearance durations, pedestrian service, preemption, and coordination-transition states. It highlights short or irregular yellow/red clearances and material changes in occurrence or duration.

A software-based virtual conflict monitor reconstructs active phase, overlap, pedestrian, and overlap-pedestrian states from every collected output event. Each state is checked against configuration-specific incompatible pairs. The study definitions contain 863 incompatible pairs across 21 configurations. No conflicts were stored in the MAXTIME 2.15.1, MAXTIME 2.18.1, or modified-overlap-setting campaigns. This automated check is a major increase in test coverage, but it complements rather than replaces a cabinet conflict monitor or formal safety certification.

#figure(
  image("figures/example_report.png", width: 92%),
  caption: [Example automated report. Aggregate sequence and timing results lead to focused views of clearance, pedestrian, preemption, transition, and conflict findings, allowing staff to review exceptions instead of the full event stream.],
) <fig-report>

#pagebreak()

= Experimental Design

Table 2 separates the two tests. The release comparison represents normal acceptance testing. The same-software comparison provides a repeatability group and a known operational intervention.

#text(size: 10pt)[#table(
  columns: (1.45fr, 1.2fr, 1.5fr, 1.1fr, 1.55fr), inset: 3pt, stroke: 0.5pt,
  [*Test*], [*Reference*], [*Candidate*], [*Configurations*], [*Question*],
  [Software release], [MAXTIME 2.15.1], [MAXTIME 2.18.1], [25], [Can full-day outputs be aligned and meaningful release differences surfaced?],
  [Overlap clearance settings], [MAXTIME 2.18.1], [Same software with applicable overlap trail settings], [14 changed; 11 unchanged], [Are unchanged replays repeatable, and are deliberate changes detected?],
)]
#figure.caption([Table 2. Evaluation design.])

Occasional truncated overlap yellow and red clearance intervals had been observed. To prevent them, trail-yellow settings were added to applicable overlaps, with trail-red and red-revert settings changed as needed. Transaction histories identified these edits in 14 configurations; 11 had no relevant edit. The test provides a known operational difference while holding the software version constant. A changed setting is detectable only if the recorded day exercises the affected overlap operation.

= Results

== Field replay produced repeatable controller outputs

All 11 unchanged configurations passed across separate MAXTIME 2.18.1 replay campaigns. Their mean sequence match was 99.6 percent (range 99.1--100.0 percent), and their mean timing match was 97.7 percent (range 90.6--99.7 percent). These were independent full-day controller executions, not a recorded output compared with itself. This is the principal feasibility result: the controller responses were deterministic enough for DTW to align and compare them automatically.

Eleven of 14 configurations with applicable overlap clearance-setting changes were flagged, while none of the 11 unchanged configurations was flagged. The three changed configurations that passed had sequence matches of 99.3, 99.9, and 100.0 percent; the recorded input may not have exercised the affected behavior, or the comparison may have been insensitive to the resulting change. Table 3 summarizes the result without implying exhaustive coverage of every possible timing change.

#table(
  columns: (2fr, 1fr, 1fr, 1fr), inset: 4pt, stroke: 0.5pt,
  [*Group*], [*Flagged*], [*Passed*], [*Total*],
  [Applicable overlap clearance edit], [11], [3], [14],
  [No relevant edit], [0], [11], [11],
)
#figure.caption([Table 3. Same-software repeatability and parameter-intervention results.])

The overlap-setting test was motivated by the type of event shown in @fig-overlap. The automated check detected a 3.0-second overlap yellow interval, below ODOT's 3.5-second minimum review threshold and the 4.0-second median yellow interval for that signal phase. It occurred with a preempt call; preemption was a common trigger for the truncated overlap-clearance problem.

#figure(
  image("figures/short-overlap-yellow.png", width: 100%),
  caption: [Truncated overlap yellow detected during preemption. The 3.0-second interval was below ODOT's 3.5-second minimum review threshold and the 4.0-second median yellow interval for this signal phase. This type of finding motivated the overlap trail-yellow and related clearance-setting changes.],
) <fig-overlap>
== Application to software-version acceptance

After repeatability was established, the method was applied to acceptance testing of a new software version. Configuration labels such as 12036 and 2B045 are ODOT internal tracking identifiers for individual signalized intersections. Nineteen of 25 configurations passed the aggregate thresholds and six were directed to staff review. Mean sequence match across all 25 configurations was 96.2 percent. Among the 21 configurations with timing scores, mean timing match was 93.8 percent. Four configurations fell below the sequence threshold; two exceeded the sequence threshold but fell below the timing threshold.

#text(size: 10pt)[#table(
  columns: (1.25fr, 0.8fr, 2.8fr), inset: 3pt, stroke: 0.5pt,
  [*Outcome*], [*Count*], [*Configurations / interpretation*],
  [Passed both thresholds], [19], [Operationally similar overall; detailed diagnostics still identified localized changes.],
  [Sequence review], [4], [12036, 2B045, 2B094, 2B339. The 12036 result likely reflects peer-to-peer operation not being configured correctly in the test environment.],
  [Timing review], [2], [2B054 and 2B085.],
)]
#figure.caption([Table 4. MAXTIME version 2.15.1 versus version 2.18.1 release-comparison outcomes.])

The detailed findings were more informative than the aggregate status:

- *Rail-preemption fix (13008).* Preempt 6 is a custom ODOT rail-preemption function that inhibits pedestrian service while preparing for the main rail preempt. In MAXTIME version 2.15.1, Preempt 6 sometimes remained active after its input ended. Version 2.18.1 exited the preempt correctly (@fig-preempt), confirming the known fix.
- *Transition-algorithm difference (2B049).* The aligned traces showed that the two versions used different short-way coordination-transition logic. The difference did not appear to change signal operation. The trace also raised a separate question: the controller appeared to be in step while still reporting transition (@fig-transition).
- *Expected clearance behavior (13008).* The report flagged an irregular phase 4 red-clearance interval. Review showed that dynamic red-clear extension was configured, and both versions handled it consistently. The flag demonstrated that an unexpected clearance change would have been visible.

Both examples passed the aggregate thresholds, showing why localized differences must be retained for review rather than reduced to a single pass/fail result.

#figure(
  image("figures/preempt-exit-bug-fix.png", width: 100%),
  caption: [Configuration 13008, Preempt 6. The custom ODOT function inhibits pedestrian service in preparation for the main rail preempt. MAXTIME version 2.15.1 failed to exit after the input ended; version 2.18.1 exited correctly.],
) <fig-preempt>

#figure(
  image("figures/transition-state-difference.png", width: 100%),
  caption: [Configuration 2B049, short-way transition. The aligned traces show a change in transition logic between MAXTIME versions. No operational difference was apparent, although the controller appeared to be in step while still reporting transition.],
) <fig-transition>

No new operation-impacting software defect was confirmed in MAXTIME version 2.18.1 from the reviewed results. The new version was otherwise operationally similar across the tested configurations, and the virtual conflict monitor recorded no incompatible simultaneous outputs among the 21 configurations with defined incompatible pairs.

= Discussion

The author now uses the method for controller-software acceptance at ODOT. Its value is added test coverage: approximately 23 hours of interacting inputs are replayed across multiple production configurations while the software checks millions of output events. The repeatability results show that ordinary run-to-run variation is low enough for deliberate timing changes and software differences to remain visible.

Field logs also capture combinations that are difficult to anticipate: simultaneous vehicle and pedestrian calls, time-of-day plan changes, coordination transitions, and preemption during actual traffic operation. A rare failure or flash event can be saved as a targeted replay, used to reproduce a reported problem, and then run again to test a vendor correction or timing workaround.

The accepted baseline is a reference trace, not an assumption that the old operation is always correct. The 13008 preemption difference was desirable because the candidate software fixed a baseline defect. Staff must classify each reported difference as expected, beneficial, harmful, caused by the test environment, or unexplained before accepting a new baseline.

The package separates NTCIP replay inputs from product-specific output collection. The input method is potentially portable to controllers supporting the required NTCIP objects, but another controller family would require an event-log collector and event-code mapping and has not yet been evaluated.
= Limitations

The current event-log collector supports MAXTIME only. Supporting another controller family requires a new event-log interface and event-code mapping. Replay capacity is limited by available computing and network resources, but this evaluation ran as many as 15 emulators simultaneously on a standard Windows desktop. The 23-hour traces cover only the conditions experienced by the 25 field controllers during the recorded day. An edge case occurring on another day or at another configuration will be missed unless that trace and configuration are added. Current time and computing constraints make testing every agency controller impractical, although additional complex or critical intersections can be included as needed.

= Conclusions

This study found that field-derived high-resolution event replay is feasible for expanded controller testing. Independent unchanged runs averaged 99.6 percent sequence match and 97.7 percent timing match, demonstrating that DTW could reliably align full-day operational-output event sequences. The comparison then flagged 11 of 14 configurations with deliberate overlap clearance-setting changes and none of the unchanged configurations. Applied to a software update, it surfaced real operational differences within otherwise similar full-day operation, including a confirmed rail-preemption exit bug fix. The virtual conflict monitor found no incompatible signal phases or overlaps among the 21 configurations with defined incompatible pairs.

The method expands test coverage beyond manually selected inputs by replaying actual field activity across production configurations and comparing signal phases, clearances, pedestrian service, preemption, coordination, and conflicts. It can expose unforeseen effects before field deployment and reproduce field failures for verification of proposed fixes.

The results establish the feasibility of the underlying field-replay and event-alignment approach.
= Acknowledgments

The author thanks Chris Primm, State Traffic Operations Engineer, Oregon Department of Transportation, for contributing ideas to the testing approach. The views expressed are those of the author and do not necessarily represent the views or policies of the Oregon Department of Transportation.

= Code and Data Availability

The study used Signal-Replay version 0.2.0, an open-source Python package available from #link("https://github.com/ShawnStrasser/ATC-Signal-Replay")[GitHub] and #link("https://pypi.org/project/signal-replay/")[PyPI]. The repository includes replay and comparison code, tests, documentation, and offline examples.

The repository includes a self-contained comparison-stage example for configuration 13008: saved MAXTIME 2.15.1 and 2.18.1 output events, comparison settings, expected results, and a reproduction script. Running the example regenerates the HTML report and verifies 98.6 percent sequence match and 96.6 percent timing match without controller hardware, firmware, or configuration databases. It reproduces comparison and report generation only; the original controller replay still requires controller/emulator software, configuration databases, and agency test infrastructure.
= Generative AI Disclosure



Generative artificial intelligence tools, including Claude Code and OpenAI Codex, were used to assist with software development and review, data-analysis workflows, figure preparation, manuscript review, and document formatting. The author reviewed and tested the software, verified the cited sources and reported results, and accepts responsibility for the research methods, findings, interpretations, and final manuscript.
#bibliography("references.bib", title: [References])