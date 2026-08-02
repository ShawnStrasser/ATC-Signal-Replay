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
  #text(size: 11pt, weight: "bold")[Shawn Strasser, P.E.] \
  Traffic Signal Operations Engineer \
  Oregon Department of Transportation \
  shawn.strasser12\@gmail.com
  #v(0.45in)
  #text(size: 10pt)[Total manuscript pages: #context counter(page).final().first()]
]
#pagebreak()

#align(center)[#text(size: 12pt, weight: "bold")[Structured Abstract]]
#v(0.18in)
*Objectives.* Manual controller testing cannot reproduce everything a signal controller experiences over a day, including interacting vehicle, pedestrian, coordination, and preemption activity. This study evaluates whether high-resolution events recorded at field controllers can be replayed to test controllers and whether the resulting output sequences can be aligned and compared automatically to identify operational differences.

*Methods.* An open-source Python package converts high-resolution logs to National Transportation Communications for Intelligent Transportation Systems Protocol (NTCIP) calls and replays them to test controllers. Output events are grouped by timestamp and aligned using dynamic time warping with Jaccard distance between event sets. It also checks clearance intervals and incompatible signal phases and overlaps. Approximately 23 hours of events from each of 25 production configurations were replayed to controller emulators. Separate runs tested repeatability, known timing changes, and practical use for software-version acceptance.

*Findings.* Controller outputs from separate unchanged runs aligned with 99.6 percent mean sequence match and 97.7 percent mean timing match, demonstrating that controller responses to field-derived replay were deterministic enough for automatic comparison. The method flagged operational differences in 11 of 14 configurations with deliberate overlap clearance-setting changes and none of the 11 unchanged configurations. Applied to a software update, it also identified real differences within otherwise similar full-day operation, including correction of a rail-preemption exit bug. No incompatible signal phases or overlaps were detected.

*Novelty.* The study combines replay of full-day event sequences actually experienced by live field controllers with automatic alignment and comparison of complete controller-output sequences across numerous production timing configurations.

*Practical Applications.* The open-source workflow substantially expands acceptance testing beyond manually toggling individual inputs. Agencies can replay realistic signal activity across many configurations, automatically identify unexpected differences before field deployment, reproduce field failures, and test software or timing corrections against the same inputs.
#pagebreak()

= Introduction

Controller acceptance testing is commonly limited by staff time and by the inputs staff can reasonably operate by hand. Manually toggling detector, pedestrian, and preemption inputs remains useful for checking individual functions, but it cannot reproduce a full day of interacting calls, coordination transitions, and preemption activity across many production timing configurations.

This project uses field operation as the test sequence. Approximately 23 hours of high-resolution events actually experienced by each of 25 live field controllers are converted to NTCIP calls and replayed to test controllers. The resulting new output events are compared with the original field-derived baseline. The central question is:

#quote(block: true)[Can high-resolution field events be replayed to test controllers, and can the resulting output sequences be aligned and compared automatically to identify operational differences?]

Feasibility requires both repeatability and sensitivity to change. Replaying the same inputs must produce controller outputs similar enough to align reliably, while actual changes in signal operation must remain visible. The comparison must also direct staff to important differences in signal phases, clearances, pedestrian service, preemption, coordination, and incompatible movements without requiring them to inspect millions of event records.

The evaluation covers 25 production configurations and three emulator campaigns using MAXTIME controller software: version 2.15.1, version 2.18.1, and version 2.18.1 after overlap trailing yellow and red clearance settings were changed to prevent occasional truncated yellow and red intervals. The primary contribution is evidence that field-derived controller outputs were repeatable enough for automated comparison and that the method successfully identified real operational differences. The specific software versions provide the case study; the finding is that this form of testing works.
= Related Work

Li et al. and the Idaho Transportation Department developed automated controller testing that used XML scripts to activate inputs and verify predefined responses @li2008 @ahmed2010. Tung extended NTCIP-based automated testing across controller devices @tung2012 @tung2015. These systems established the value of repeatable automated tests. The present method differs by replaying real events recorded as they occurred at live field controllers and using a prior complete output trace as the expected response rather than requiring staff to author every input and assertion.

Controller-in-the-loop studies have connected controllers and emulators to traffic simulation through physical or virtual interfaces @wang2021. Stevanovic, Klanac, and Radivojevic evaluated high-resolution logging across six controller vendors @stevanovic2017. Those studies support the use of controlled controller environments and show that output-event semantics require platform-specific validation. They did not evaluate field-log replay for software regression across production configurations.

High-resolution event records provide tenth-second controller and detector histories @sturdevant2012. DTW provides a monotone alignment for sequences with local timing variation @sakoe1978. The literature reviewed for this study did not identify a prior evaluation combining full-day replay of events recorded from live field controllers, automatic complete-output alignment, and repeated software and timing tests across numerous production configurations.

= Replay and Test Architecture

#figure(
  image("figures/workflow.svg", width: 100%),
  caption: [Field-derived testing workflow. Standard NTCIP objects form the replay-input boundary; controller output collection and event normalization require a product adapter.],
) <fig-workflow>

== Field event conversion

The 25 source logs span approximately 9:00 a.m. to 8:00 a.m. the next day and contain 7,296,218 records, or 574.998 configuration-hours. Replay selects vehicle detector, pedestrian detector, and preemption changes. Table 1 shows the conversion implemented in the package.

#text(size: 8.6pt)[#table(
  columns: (1.25fr, 1fr, 1.1fr, 2.3fr), inset: 3pt, stroke: 0.5pt,
  [*Input*], [*Event codes*], [*Parameter*], [*NTCIP replay state*],
  [Vehicle detector], [81 off; 82 on], [Detector 1--64], [Update one bit in an eight-detector group and write the cumulative integer.],
  [Pedestrian detector], [89 off; 90 on], [Detector 1--64], [Update one bit in an eight-detector group and write the cumulative integer.],
  [Preemption], [104 off; 102 on], [Preempt number], [Write the corresponding Boolean preempt state.],
)]
#figure.caption([Table 1. Conversion from field events to replay inputs.])

Repeated on/off records are repaired by imputing the missing opposite transition. Dummy detector numbers 65 and above are excluded. Before replay, all calls are reset to zero. The package then sends SNMP SET operations to NTCIP 1202 vehicle, pedestrian, and preempt objects at the recorded time of day @ntcip1202. Preserving time of day also exercises coordination and time-of-day plan changes.

The evaluated implementation used MAXTIME emulators loaded with production databases. Emulator accommodations removed detector delay/extension, disabled unused input/output modules, removed inverted preempt logic, used localhost peer addresses, and mapped overlap pedestrian calls to dummy detector inputs. Output events were collected through a MAXTIME HTTP interface. Because replay inputs are sent through NTCIP 1202 objects, the input side is not tied to MAXTIME or another controller brand. Applying the method to another controller family requires an output adapter that maps its events to the common timestamp/event/parameter format.
== Parallel replay and latency control

Signals assigned to available emulator targets run concurrently. The 25 configurations were processed in batches over approximately one week without requiring staff to operate each input. After the initial setup, future releases are expected to require only a few hours of staff preparation and review; this is operational experience, not a formal labor study.

Replay timing includes a configurable latency offset. During the MAXTIME version 2.18.1 campaigns, the runner also estimated latency from isolated detector-on events and updated the offset during collection. The stored release database contains 707,458 latency samples and 6,577 applied updates. The three campaign databases each contain 627,240 replay commands and between 7.57 and 8.02 million controller-output events.

= Output Alignment and Automated Checks

== Discrete-event dynamic time warping

Events at the same timestamp, or within 0.25 seconds, are grouped into sets of `(event identifier, parameter)` pairs. Let $A_i$ and $B_j$ be reference and candidate event sets. Their local cost is Jaccard distance:

$ d(A_i, B_j) = 1 - frac(abs(A_i ∩ B_j), abs(A_i ∪ B_j)). $ <eq-jaccard>

Identical event groups cost zero. Missing or additional events increase the cost. The cumulative DTW cost is

$ D(i,j) = d(A_i, B_j) + min(D(i-1,j), D(i,j-1), D(i-1,j-1)), $ <eq-dtw>

with $D(0,0)=0$ and inaccessible borders set to infinity. Backtracking produces an order-preserving path; horizontal and vertical path segments identify additions, omissions, or local shifts rather than hiding them.

#figure(
  image("figures/alignment-example.pdf", width: 82%),
  caption: [Simplified discrete-event alignment. DTW absorbs small timing shifts while missing and additional event groups retain nonzero costs.],
) <fig-alignment>

Sequence scores are calculated in 45-minute windows advanced every 40 minutes. Sixty seconds are clipped from each window edge to avoid penalizing a cycle split at a boundary. Sequence match is the percentage of aligned event-group pairs with zero Jaccard distance. Timing match is the percentage of exactly matched groups within 0.50 seconds after temporal alignment. A configuration passes at 95 percent sequence match and 90 percent timing match.

Vehicle phase-call similarity provides a separate replay-reliability check. Windows below 85 percent are displayed but excluded from the configuration average. This helps distinguish controller differences from a test environment that did not reproduce the intended inputs.

== Operational checks and virtual conflict monitor

The automated review is broader than sequence scoring. It compares phase and overlap intervals, clearance durations, pedestrian service, preemption, and coordination-transition states. It highlights short or irregular yellow/red clearances and material changes in occurrence or duration.

A software-based virtual conflict monitor reconstructs active phase, overlap, pedestrian, and overlap-pedestrian states from every collected output event. Each state is checked against configuration-specific incompatible pairs. The study definitions contain 863 incompatible pairs across 21 configurations. No conflicts were stored in the MAXTIME 2.15.1, MAXTIME 2.18.1, or modified-overlap-setting campaigns. This automated check is a major increase in test coverage, but it complements rather than replaces a cabinet conflict monitor or formal safety certification.

#figure(
  image("figures/example_report.png", width: 92%),
  caption: [Example automated report. Aggregate sequence and timing results lead to focused views of clearance, pedestrian, preemption, transition, and conflict findings, allowing staff to review exceptions instead of the full event stream.],
) <fig-report>

= Experimental Design

Table 2 separates the two tests. The release comparison represents normal acceptance testing. The same-software comparison provides a repeatability group and a known operational intervention.

#text(size: 8.6pt)[#table(
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

Eleven of 14 configurations with applicable overlap clearance-setting changes were flagged, while none of the 11 unchanged configurations was flagged. The three changed configurations that passed had sequence matches of 99.3, 99.9, and 100.0 percent; the recorded input apparently did not materially exercise the affected behavior. Table 3 summarizes the result without implying exhaustive coverage of every possible timing change.

#table(
  columns: (2fr, 1fr, 1fr, 1fr), inset: 4pt, stroke: 0.5pt,
  [*Group*], [*Flagged*], [*Passed*], [*Total*],
  [Applicable overlap clearance edit], [11], [3], [14],
  [No relevant edit], [0], [11], [11],
)
#figure.caption([Table 3. Same-software repeatability and parameter-intervention results.])

The intervention removed short and variable overlap-yellow intervals while otherwise retaining similar operation in the reviewed examples (@fig-overlap). This shows how the same field trace can evaluate a timing correction as well as a software release.

#figure(
  image("figures/short-overlap-yellow.png", width: 100%),
  caption: [Representative parameter test. Adding overlap trail-yellow and related clearance settings removed short, variable overlap-yellow intervals while the comparison checked the remainder of the replay for unintended changes.],
) <fig-overlap>
== Application to software-version acceptance

After repeatability was established, the method was applied to acceptance testing of a new software version. Nineteen of 25 configurations passed the aggregate thresholds and six were directed to staff review. Mean sequence match was 96.2 percent and mean timing match was 93.8 percent. Four configurations fell below the sequence threshold; two exceeded the sequence threshold but fell below the timing threshold. These counts describe how the review was organized; the important result is that the aligned traces exposed specific changes in signal operation.

#text(size: 8.6pt)[#table(
  columns: (1.25fr, 0.8fr, 2.8fr), inset: 3pt, stroke: 0.5pt,
  [*Outcome*], [*Count*], [*Configurations / interpretation*],
  [Passed both thresholds], [19], [Operationally similar overall; detailed diagnostics still identified localized changes.],
  [Sequence review], [4], [12036, 2B045, 2B094, 2B339. The 12036 result likely reflects peer-to-peer operation not being configured correctly in the test environment.],
  [Timing review], [2], [2B054 and 2B085.],
)]
#figure.caption([Table 4. MAXTIME version 2.15.1 versus version 2.18.1 release-comparison outcomes.])

The detailed findings were more informative than the aggregate status:

- *Rail-preemption fix (13008).* MAXTIME version 2.15.1 could leave Preempt 6 active with the agency's rail-preemption configuration. MAXTIME version 2.18.1 exited correctly (@fig-preempt). This confirmed a known bug fix.
- *Transition improvement (2B049).* MAXTIME version 2.18.1 completed a short-way coordination transition faster, as shown against the programmed split. The trace also raised a separate question: the controller appeared to be in step while still reporting transition (@fig-transition).
- *Expected clearance behavior (13008).* The report flagged an irregular phase 4 red-clearance interval. Review showed that dynamic red-clear extension was configured, and both versions handled it consistently. The flag demonstrated that an unexpected clearance change would have been visible.

The first two configurations passed the aggregate thresholds. This is important: the tool did not merely label controllers pass or fail. It retained localized differences that confirmed a bug fix and exposed an algorithm change within otherwise similar full-day operation.

#figure(
  image("figures/preempt-exit-bug-fix.png", width: 100%),
  caption: [Configuration 13008, Preempt 6. MAXTIME version 2.15.1 remained active under the rail-preemption configuration; version 2.18.1 exited correctly, confirming the known fix.],
) <fig-preempt>

#figure(
  image("figures/transition-state-difference.png", width: 100%),
  caption: [Configuration 2B049, short-way transition. MAXTIME version 2.18.1 returned to the programmed split faster. The aligned trace also showed the controller reporting transition after appearing to be in step.],
) <fig-transition>

No new operation-impacting software defect was confirmed in MAXTIME version 2.18.1 from the reviewed results. The new version was otherwise operationally similar across the tested configurations, and the virtual conflict monitor recorded no incompatible simultaneous outputs.

= Discussion

The method enhances controller acceptance testing; it does not simply automate the same manual test. Staff can still check individual functions deliberately, while field replay adds approximately 23 hours of interacting inputs across 25 actual timing configurations. Available emulators run concurrently, and the software checks millions of output events for sequence and timing changes, clearance irregularities, preemption and pedestrian differences, and incompatible signal phases and overlaps. Staff can concentrate on the exceptions instead of choosing and operating every input combination.

The high repeatability of the unchanged runs is what makes this practical. If repeated controller outputs could not be aligned, an unexpected software effect would be indistinguishable from ordinary run-to-run variation. Instead, the same inputs produced closely matching sequences, while deliberate timing changes and a software bug fix remained visible. The comparison therefore provides a broad safety net for unforeseen changes in every behavior exercised by the replay. It cannot detect a condition absent from the recorded day, so targeted manual tests and scripted requirements remain necessary.

Field logs also capture combinations that are difficult to anticipate: simultaneous vehicle and pedestrian calls, time-of-day plan changes, coordination transitions, and preemption during actual traffic operation. A rare failure or flash event can be saved as a targeted replay, used to reproduce a reported problem, and then run again to test a vendor correction or timing workaround.

The accepted baseline is a reference trace, not an assumption that the old operation is always correct. The 13008 preemption difference was desirable because the candidate software fixed a baseline defect. Staff must classify each reported difference as expected, beneficial, harmful, caused by the test environment, or unexplained before accepting a new baseline.

The package is open source and separates NTCIP inputs from product-specific outputs. The replay-input method is not tied to a controller brand: it can be applied to controllers that implement the required NTCIP objects. A new controller family still needs an output collector, event-code mapping, and validation of timestamp behavior. MAXTIME is the implementation evaluated here, not a limitation of the underlying replay method.
= Limitations

The campaigns used one agency's production configurations and one controller-family emulator. Emulator accommodations and incomplete peer-to-peer setup affected generalizability and likely explain the 12036 mismatch. Hardware and a second controller family should be evaluated in future work.

A 23-hour trace tests only behavior exercised during that day. Three parameter-modified configurations produced essentially unchanged output, illustrating the need to retain multiple field days and targeted incident traces. The thresholds are engineering review values rather than statistically optimized decision boundaries, and the planned one-factor sensitivity analysis has not yet been run.

Five release-threshold review cases remain incompletely classified after accounting for the likely 12036 environment issue. That does not invalidate the feasibility result, but final operational disposition would strengthen the acceptance-test case study. The virtual conflict monitor is also limited to the incompatible pairs defined for 21 configurations and is not a substitute for certified cabinet monitoring.

The controller replay requires emulator software, configuration databases, and agency infrastructure. The open-source repository and proposed event bundle can reproduce the comparison and reporting stage without that equipment, but not the original controller execution.

= Conclusions

This study found that field-derived high-resolution event replay is feasible for comprehensive controller testing. Independent unchanged runs averaged 99.6 percent sequence match and 97.7 percent timing match, demonstrating that DTW could reliably align complete controller-output sequences. The comparison then flagged 11 of 14 configurations with deliberate overlap clearance-setting changes and none of the unchanged configurations. Applied to a software update, it surfaced real operational differences within otherwise similar full-day operation, including a confirmed rail-preemption exit bug fix. The virtual conflict monitor found no incompatible signal phases or overlaps in the three campaigns.

The significance is the increase in test coverage. Instead of relying only on staff to select and toggle individual inputs, agencies can replay actual field activity across many production configurations and automatically compare the resulting signal phases, clearances, pedestrian service, preemption, coordination, and conflicts. This creates a practical opportunity to discover unforeseen software or timing effects in the test environment rather than after field deployment. The same process can reproduce field failures and verify proposed fixes against identical inputs.

The open-source package provides a reusable NTCIP-based input interface and a comparison method that other agencies can adapt. Controller-specific output collection and event mapping are still required, but the successful alignment and detection results establish that the underlying approach works.
= Acknowledgments

The author thanks Chris Primm, State Traffic Operations Engineer, Oregon Department of Transportation, for reviewing the results and contributing ideas for testing.

= Code and Data Availability

Signal-Replay is an open-source Python package available at `https://github.com/ShawnStrasser/ATC-Signal-Replay`. It includes replay and comparison code, tests, documentation, and an offline example. The final submission should cite release `v0.2.0` or the exact public commit. A comparison-only data bundle can include replay inputs, collected outputs, parameters, and expected results without controller firmware or proprietary configuration databases.

= Generative AI Disclosure

Generative artificial intelligence tools, including Claude Code and OpenAI Codex, were used to assist with software development and review, data-analysis workflows, figure preparation, manuscript review, and document formatting. The author reviewed and tested the software, verified the cited sources and reported results, and accepts responsibility for the research methods, findings, interpretations, and final manuscript.
#bibliography("references.bib", title: [References])