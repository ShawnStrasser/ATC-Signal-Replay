#set page(paper: "us-letter", margin: 1in, numbering: "1", number-align: center)
#set text(font: "Times New Roman", size: 10pt, lang: "en")
#set par(justify: true, leading: 0.52em)
#set par.line(numbering: "1", numbering-scope: "page", number-margin: left, number-clearance: 4pt)
#set heading(numbering: none)
#show heading.where(level: 1): it => block(above: 11pt, below: 5pt)[#text(size: 12pt, weight: "bold")[#upper(it.body)]]
#show heading.where(level: 2): it => block(above: 8pt, below: 3pt)[#text(size: 10.5pt, weight: "bold")[#it.body]]
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
*Objectives.* Traffic-signal controller changes are difficult to test against the breadth and timing of inputs that occur in service. This study asks whether high-resolution events recorded at live field controllers can be converted to controller calls, replayed at test controllers, and used to automatically identify operational differences in the resulting event sequences.

*Methods.* Approximately 23 hours of detector, pedestrian, and preemption events were collected for each of 25 production signal configurations. The events were converted to National Transportation Communications for Intelligent Transportation Systems Protocol calls and replayed at their recorded time of day to controller emulators. Output event traces were grouped by timestamp and aligned with a modified dynamic time warping procedure using Jaccard distance between discrete event sets. Feasibility was evaluated through a software-release comparison and a same-software timing-parameter intervention.

*Findings.* In 11 configurations unchanged between two same-software replay campaigns, mean sequence match was 99.6 percent and mean timing match was 97.7 percent, demonstrating that the field-derived replay produced highly repeatable controller outputs. Deliberate trailing-overlap changes were automatically flagged in 11 of 14 affected configurations, with no flags among the 11 unchanged configurations. Comparing software versions 2.15.1 and 2.18.1 produced 19 passes and six review candidates; manual review identified an expected preemption bug fix and transition-logic changes among the divergences.

*Novelty.* The study combines long-duration field-derived input replay with automated alignment of complete high-resolution output traces across numerous production configurations, extending prior scripted controller testing and controller-in-the-loop research.

*Practical Applications.* Agencies can use recorded operation as a reusable regression suite for software releases, timing changes, and targeted bug replication before field deployment.
#pagebreak()

= Introduction

Traffic-signal controller software is safety-critical operational software, yet the most difficult behaviors to test are often produced by ordinary field complexity: overlapping detector calls, pedestrian service, coordination transitions, and occasional preemption. A bench checklist can verify an isolated function, and microscopic simulation can generate traffic demand, but neither necessarily recreates the exact sequence and timing of controller inputs that previously occurred in service. Manual testing is also poorly suited to reproducing 23 hours of tenth-second events at dozens of configurations.

High-resolution controller logs provide another possibility. These logs record detector changes and controller states with subsecond timestamps and have primarily been used to calculate performance measures or diagnose past operation @sturdevant2012. The same records can instead be treated as executable test inputs. Detector, pedestrian, and preemption events can be converted into controller calls, delivered to a test controller at their original times, and reused whenever software or timing changes. The output is another high-resolution event trace rather than a small set of manually specified assertions.

This paper therefore asks a direct feasibility question:

#quote(block: true)[Can high-resolution event logs collected from live field controllers be replayed to test controllers, and can the resulting output sequences be aligned and compared automatically to identify operational differences?]

The feasibility condition has two parts. First, repeated presentation of the same recorded input must produce output that is deterministic enough to align despite network, logging, and scheduling jitter. Second, the comparison must preserve meaningful divergences rather than warping them away. A method that meets only the first condition would report similarity but miss change; one that meets only the second would overwhelm users with timing noise.

The study evaluates both conditions across 25 production signal configurations. About 23 hours of live inputs per configuration were replayed to create a software-version 2.15.1 baseline. The same field inputs were then replayed under version 2.18.1 and compared with the baseline. A second campaign held version 2.18.1 constant while trailing-overlap parameters were modified, providing a known operational intervention. Dynamic time warping (DTW) aligned groups of simultaneous discrete events, and separate sequence and timing measures supported automated screening.

The contribution is the evaluated workflow, not any one component. Automated controller testing, National Transportation Communications for Intelligent Transportation Systems Protocol (NTCIP) communication, controller emulation, high-resolution event logging, and DTW all predate this work. The narrower contribution is showing that long-duration, field-derived input histories can function as reusable regression tests and that complete controller-output traces are sufficiently repeatable for automated alignment across many production configurations.

= Prior Work and Research Gap

== Scripted controller testing

The closest prior work is the traffic-controller automated testing system developed by Li et al. and subsequently documented for the Idaho Transportation Department @li2008 @ahmed2010. That system replaced manual suitcase testing with controlled input activation and response verification. XML scripts specified which inputs to activate, when to activate them, and which responses to expect; variants supported controller interface devices, NEMA controller types, and NTCIP communication. The work established that automation improves repeatability and permits stored test definitions and results.

Tung's Florida Department of Transportation work further developed NTCIP-oriented automated testing across traffic-control devices @tung2012 @tung2015. These projects are important precedents: the present study does not claim to invent automated controller tests or NTCIP-based activation. The distinction is the source and scale of the test. Prior systems primarily executed authored functional scripts with predefined expected responses. Here, the input script is derived from an entire day of actual field operation, and the expected response is a previously collected, complete output trace. This changes the testing task from checking selected assertions to comparing emergent behavior under a broad operational sequence.

== Controller-in-the-loop and high-resolution event research

Controller-in-the-loop studies demonstrate that physical controllers, software controllers, and emulators can be connected to traffic simulations. Wang, Tian, and Yang used a virtual controller interface device and NTCIP to connect controllers to simulation, and compared hardware-, software-, and emulator-in-the-loop environments @wang2021. Their outcome measures were traffic-simulation measures such as queues, delay, and trajectories. The present study does not simulate vehicles or estimate network performance; it supplies controller inputs directly and examines controller event outputs as a software-regression trace.

Stevanovic, Klanac, and Radivojevic evaluated high-resolution logging and performance-measure consistency for six controller vendors in a hardware-in-the-loop laboratory @stevanovic2017. Their findings reinforce two cautions relevant here: event definitions and logging behavior require normalization, and results from one platform do not automatically transfer to another. Their objective was logger and performance-measure evaluation, not replay of recorded field inputs for release regression.

== Sequence alignment and gap

DTW was developed to align sequences that express similar patterns at different local rates @sakoe1978. Standard DTW defines a local cost between elements and uses dynamic programming to find a minimum-cost monotone path through the pairwise cost matrix. A horizontal or vertical step can absorb an insertion, deletion, or local shift, while the path still preserves event order. This makes DTW useful for controller traces: network and timestamp jitter should be alignable, whereas missing, additional, or different event groups should retain a positive cost.

The review found studies of scripted controller testing, NTCIP controller interfaces, controller-in-the-loop experiments, and high-resolution logger validation. It did not identify a prior evaluation combining (1) long-duration input sequences derived from live field logs, (2) repeated replay across numerous production configurations, and (3) automated alignment of complete output-event traces to detect software- and parameter-induced changes. This is a cautious combination claim, not a claim that any individual technology is new.

#text(size: 8.4pt)[#table(
  columns: (1.25fr, 1.35fr, 1.15fr, 1.5fr, 1.65fr), inset: 3pt, stroke: 0.5pt,
  [*Study*], [*Input source*], [*Environment*], [*Output evaluation*], [*Difference from this study*],
  [Li et al.; Ahmed et al.], [Authored XML/CIDScript tests], [Controller and interface device], [Predefined expected responses], [Field-derived 23-hour tests and whole output traces.],
  [Tung], [Authored NTCIP tests], [Multiple controller devices], [Functional and conformance results], [Behavioral regression under recorded operation.],
  [Stevanovic et al.], [Microsimulation activity], [Six-vendor hardware-in-the-loop], [Logger and measure consistency], [Field replay and release/configuration comparison.],
  [Wang et al.], [Microsimulation activity], [Hardware/software/emulator-in-loop], [Queue, delay, trajectories], [Direct controller-event sequence comparison.],
)]
#figure.caption([Table 1. Relationship to the closest prior controller-testing and controller-in-the-loop work.])
= Field-Derived Replay Method

#figure(
  image("figures/workflow.svg", width: 100%),
  caption: [End-to-end workflow. A field log is converted to standard NTCIP input states and replayed to a test controller. The current implementation collects manufacturer-specific output logs, normalizes events, aligns them to a stored baseline, and directs a reviewer to divergences.],
) <fig-workflow>

== Field event capture and input reconstruction

For each signal configuration, the source log spans approximately 9:00 a.m. to 8:00 a.m. the following day. Across 25 Parquet files, the stored source logs contain 7,296,218 high-resolution records and 574.998 configuration-hours. The replay uses the subset representing vehicle detector, pedestrian detector, and preemption state changes. In the evaluated event vocabulary, vehicle detector codes 81 and 82 represent off and on, pedestrian codes 89 and 90 represent off and on, and preemption codes 104 and 102 represent off and on. Parameters 65 and above are excluded as dummy detectors.

The software first corrects invalid repeated on/off records by imputing the missing opposite transition. For vehicle and pedestrian channels, detector numbers are packed in groups of eight. Each event changes one bit in an eight-bit state value, and the cumulative state is written to the corresponding controller object. Preemption channels are written as individual Boolean states. Table 2 connects the field record to the replay action.

#text(size: 8.6pt)[#table(
  columns: (1.25fr, 1fr, 1fr, 2.35fr), inset: 3pt, stroke: 0.5pt,
  [*Input class*], [*Event codes*], [*Field parameter*], [*Replay conversion*],
  [Vehicle detector], [81 off; 82 on], [Detector 1--64], [Update its bit in an eight-channel vehicle-detector group and send the cumulative integer state.],
  [Pedestrian detector], [89 off; 90 on], [Detector 1--64], [Update its bit in an eight-channel pedestrian-detector group and send the cumulative integer state.],
  [Preemption], [104 off; 102 on], [Preempt number], [Send 0 or 1 to the corresponding preempt input.],
)]
#figure.caption([Table 2. Conversion from high-resolution field events to controller input states.])

NTCIP 1202 defines data objects and SNMP-based access for actuated signal controllers @ntcip1202. The implementation sends SNMP SET operations to the vehicle-detector-call, pedestrian-detector-call, and preempt-call object groups. It resets all input groups before a replay, then schedules every cumulative state transition at the same time of day as the source record. Thus the input boundary is based on standardized controller objects rather than a proprietary hardware connector.

The method is manufacturer-neutral in concept but not plug-and-play across all products. The evaluated implementation used MAXTIME controller emulators, and output was collected through a MAXTIME HTTP event-log interface. Adapting the workflow to another NTCIP controller requires verifying its object implementation, writing an output-collection adapter, and mapping its output codes into the comparison vocabulary. The experiment establishes feasibility on one platform; it does not establish conformance or equivalent behavior for every NTCIP controller.

== Test-controller preparation and replay timing

Each emulator was loaded with the production database for the associated signal. Accommodations were needed because an emulator is not a complete field cabinet: detector delay and extension were removed, input/output modules above the first were disabled, inverted preempt logic was removed, peer addresses were set to localhost, and overlap pedestrian inputs were driven by dummy detector mappings. These changes let the software controller receive equivalent logical calls without field input hardware. They also limit generalization and must be documented whenever the method is repeated.

The replay preserves the original time of day so that time-of-day plans and coordination schedules are exercised. Network transmission and controller logging introduce latency, so raw timestamps are not assumed identical. A configurable offset advances input transmission relative to the desired logged time. In the 2.18.1 campaigns, the runner also estimated latency from matched, isolated detector-on events during replay and updated the offset after a minimum sample count. The stored 2.18.1 release database contains 707,458 latency samples and 6,577 applied updates. This correction addresses transport delay; it does not change the order or intended time spacing of source calls.

== Baseline and candidate traces

The initial 2.15.1 replay generated the baseline output trace for every production configuration. When version 2.18.1 became available, the same field input sequence was replayed to the same logical configurations and a new output trace was collected. A third campaign again used version 2.18.1, but introduced trailing-yellow, trailing-red, and associated red-revert timing changes where applicable. The three stored databases contain 627,240 replay commands each and approximately 7.6 to 8.0 million controller-output events.

Output comparison uses event identifiers representing phase and overlap colors, phase calls, pedestrian service, preemption, coordination transitions, and related state changes. Events occurring at the same timestamp—or within a 0.25-second grouping tolerance—form a set of `(event identifier, parameter)` pairs. Treating simultaneous records as a set avoids interpreting database row order as controller behavior.

= Dynamic Time Warping Comparison

== Local cost and warping path

Let $A = (A_1, ..., A_n)$ and $B = (B_1, ..., B_m)$ be the ordered reference and candidate event groups. Each $A_i$ or $B_j$ is a set of event-parameter pairs. The implementation uses Jaccard distance as the local mismatch cost:

$ d(A_i, B_j) = 1 - frac(abs(A_i ∩ B_j), abs(A_i ∪ B_j)). $ <eq-jaccard>

Identical groups have cost zero; partially overlapping groups have a cost between zero and one; groups with no common events have cost one. This definition distinguishes a complete group match from a group with missing or additional events while ignoring within-timestamp row order.

The cumulative cost matrix is calculated using the standard three-step DTW recurrence:

$ D(i,j) = d(A_i, B_j) + min(D(i-1,j), D(i,j-1), D(i-1,j-1)), $ <eq-dtw>

with $D(0,0)=0$ and inaccessible borders initialized to infinity. Backtracking from $D(n,m)$ gives a monotone warping path. Diagonal steps pair successive groups. Horizontal or vertical steps align one group against more than one group and therefore expose local additions, omissions, or shifts. DTW does not reorder events arbitrarily because both path indices are monotone.

#figure(
  image("figures/alignment-example.pdf", width: 84%),
  caption: [Simplified event-group alignment. DTW can absorb a small timestamp shift while a missing or additional group produces a nonzero Jaccard cost and a horizontal or vertical path segment. The figure is explanatory; reported field results use the full stored traces.],
) <fig-alignment>

== Windowed score, timing score, and reliability screening

A full-day pair can contain too many groups for an unconstrained quadratic cost matrix. The official sequence score is therefore calculated over 45-minute rolling windows advanced every 40 minutes. DTW sees the full window, but the first and last 60 seconds are excluded from scoring so a window boundary that splits a signal cycle does not create an artificial mismatch. For each scored path, the sequence match is the percentage of aligned group pairs with zero Jaccard distance. The configuration score is the mean of included window scores.

The replayed detector calls should also agree between campaigns. Vehicle phase-call similarity is calculated independently for each window as a simulation-reliability check. A window below 85 percent phase-call similarity remains visible in diagnostics but is excluded from the configuration average. This prevents unreliable controller input response from being reported as a release difference.

After sequence alignment, timing is assessed only for exactly matched event groups. For a matched path pair $(i,j)$, the absolute timing error is $abs(t(A_i)-t(B_j))$ after global temporal alignment. Timing match is the percentage of such pairs within 0.50 seconds. The screening thresholds used for the archived results are at least 95 percent sequence match and at least 90 percent timing match. A failure means that a configuration requires review; it is not automatically a software defect.

#text(size: 8.6pt)[#table(
  columns: (1.7fr, 1fr, 2.4fr), inset: 3pt, stroke: 0.5pt,
  [*Parameter*], [*Value*], [*Role*],
  [Event grouping tolerance], [0.25 s], [Combines records representing the same controller instant.],
  [DTW window / advance], [45 / 40 min], [Bounds computation while retaining five-minute overlap.],
  [Window edge clip], [60 s], [Avoids scoring a cycle split at a window boundary.],
  [Sequence threshold], [95%], [Minimum exactly matched aligned groups.],
  [Timing tolerance / threshold], [0.50 s / 90%], [Minimum share of exact matches within the allowed time error.],
  [Phase-call reliability threshold], [85%], [Excludes windows in which replay response is not sufficiently comparable.],
)]
#figure.caption([Table 3. Comparison settings used for the reported campaigns.])

A useful property of this separation is interpretability. Sequence mismatch identifies different controller events; timing mismatch identifies corresponding events occurring at different times. Raw event-count differences alone cannot make that distinction. Diagnostic plots then show the aligned state intervals surrounding each divergence for manual operational classification.

= Experimental Evaluation

== Experiment 1: software release

The first evaluation compared baseline software 2.15.1 with candidate software 2.18.1 across the same 25 production configurations and field-derived input sequences. This is the intended operational use: a new release is screened against a prior approved baseline before field deployment. Because software behavior may legitimately change, the outcome is not expected to be universal equality. Feasibility is supported if most long traces align and the exceptions direct the analyst to coherent operational differences.

== Experiment 2: same-software parameter intervention

The second evaluation held software at 2.18.1. Transaction histories identify trailing-overlap, trailing-red, or associated red-revert edits in 14 configurations and no relevant edits in 11. The identical input histories were replayed again. The unchanged group measures repeatability across separate replay campaigns. The changed group is a positive-control intervention: the method should flag configurations only when the recorded traffic sequence activates behavior affected by the edit.

#text(size: 8.5pt)[#table(
  columns: (1.45fr, 1.15fr, 1.25fr, 1.05fr, 1.3fr), inset: 3pt, stroke: 0.5pt,
  [*Campaign*], [*Reference*], [*Candidate*], [*Configurations*], [*Purpose*],
  [Software release], [2.15.1 baseline], [2.18.1], [25], [Release-to-release behavioral screen],
  [Parameter intervention], [2.18.1], [2.18.1 with trailing-overlap changes], [14 changed; 11 unchanged], [Repeatability and known-change detection],
)]
#figure.caption([Table 4. Experimental comparisons.])
= Results

== Repeatability establishes feasibility

The strongest evidence that field-event replay is deterministic enough for comparison comes from the 11 configurations that had no relevant parameter change between the two 2.18.1 campaigns. All 11 passed. Their sequence scores ranged from 99.1 to 100.0 percent, with a mean of 99.6 percent. Their timing scores ranged from 90.6 to 99.7 percent, with a mean of 97.7 percent. These are independent full-day replay campaigns, not a comparison of a file with itself.

This result answers the first part of the research question: after latency compensation and event grouping, nearly all event groups produced by unchanged software and configuration can be aligned exactly. The residual mismatch is small enough that operational divergences are not hidden by ordinary replay and logging variation.

== Detection of an operational timing intervention

Of the 14 configurations with applicable trailing-overlap or related changes, 11 fell below the comparison thresholds and were flagged. The sequence match among changed configurations averaged 71.9 percent and ranged from 37.8 to 100.0 percent. None of the 11 unchanged configurations was flagged. Table 5 presents the resulting detection matrix.

#table(
  columns: (1.7fr, 1fr, 1fr, 1fr), inset: 4pt, stroke: 0.5pt,
  [*Configuration group*], [*Flagged*], [*Passed*], [*Total*],
  [Applicable trailing-overlap change], [11], [3], [14],
  [No relevant change], [0], [11], [11],
)
#figure.caption([Table 5. Automated outcomes for the same-software parameter-intervention campaign.])

The three changed configurations that passed are not necessarily algorithmic false negatives. Their sequence matches were 100.0, 99.9, and 99.3 percent. A changed parameter can affect output only when the 23-hour input trace exercises the relevant overlap state and termination condition. The result therefore measures detection under the recorded field exposure, not sensitivity to every possible timing edit.

#figure(
  image("figures/parameter-intervention-summary.pdf", width: 100%),
  caption: [Parameter-intervention results by configuration. Eleven changed configurations were automatically flagged; three changed configurations passed because little or no affected behavior appeared in the replay; all 11 configurations without relevant edits passed.],
) <fig-parameter>

The diagnostic plot in Figure 4 shows the operational effect that motivated the intervention. Under the earlier settings, short and variable overlap-yellow intervals appeared in the output. Adding trailing-yellow and trailing-red behavior removed the short intervals while leaving other operation substantially similar. This is precisely the intended use of regression replay: confirm that a targeted timing change affects the problematic behavior without introducing broad, unrelated changes.

#figure(
  image("figures/short-overlap-yellow.png", width: 100%),
  caption: [Representative overlap-clearance diagnostic from the parameter study. The aligned event traces make short overlap-yellow intervals visible and show their removal after trailing-yellow and trailing-red settings were applied. Source: project diagnostic generated from the stored controller-output events.],
) <fig-overlap>

== Software-release screening

The 2.15.1-to-2.18.1 comparison passed 19 of 25 configurations. Mean sequence match across the configuration summaries was 96.2 percent, and mean reported timing match was 93.8 percent. Six configurations required review: 12036, 2B045, 2B054, 2B085, 2B094, and 2B339. Four failed the 95-percent sequence criterion only, and two exceeded the sequence criterion but fell below the 90-percent timing criterion.

#text(size: 8.1pt)[#table(
  columns: (0.85fr, 0.72fr, 0.72fr, 0.85fr, 0.85fr, 0.72fr, 0.72fr, 0.85fr), inset: 2.2pt, stroke: 0.45pt,
  [*ID*], [*Seq.*], [*Time*], [*Result*], [*ID*], [*Seq.*], [*Time*], [*Result*],
  [01066], [96.1], [91.9], [Pass], [03013], [99.0], [90.8], [Pass],
  [05018], [97.9], [98.2], [Pass], [08042], [98.6], [96.5], [Pass],
  [08404], [99.4], [96.5], [Pass], [08411], [98.6], [95.7], [Pass],
  [12035], [95.3], [90.7], [Pass], [12036], [75.4], [--], [Review],
  [12059], [99.1], [98.4], [Pass], [13008], [98.6], [96.6], [Pass],
  [13010], [98.9], [90.1], [Pass], [2B009], [97.9], [92.4], [Pass],
  [2B045], [87.7], [--], [Review], [2B049], [95.4], [92.6], [Pass],
  [2B052], [98.6], [92.0], [Pass], [2B054], [98.7], [89.0], [Review],
  [2B085], [95.9], [87.6], [Review], [2B094], [94.8], [--], [Review],
  [2B337], [99.9], [95.0], [Pass], [2B339], [84.5], [--], [Review],
  [2B349], [97.9], [93.7], [Pass], [2B530], [99.4], [95.6], [Pass],
  [2C009], [97.6], [92.1], [Pass], [2C042], [99.7], [96.1], [Pass],
  [2C043], [99.7], [98.6], [Pass], [], [], [], [],
)]
#figure.caption([Table 6. Version 2.15.1 versus 2.18.1 sequence and timing match percentages. Timing is not reported when sequence match is below 95 percent.])

#figure(
  image("figures/software-release-summary.pdf", width: 100%),
  caption: [Software-release comparison across 25 production configurations. Most output traces exceeded both thresholds, demonstrating broad alignment; six configurations were directed to manual review.],
) <fig-software>

A review candidate can be a defect, an intentional change, a bug fix, or an unexplained difference. Manual review of the release diagnostics found examples of each useful classification except a confirmed new operation-impacting defect. Most notably, version 2.15.1 exhibited a rail-preemption exit behavior that had previously been identified as a bug; version 2.18.1 no longer produced that sequence. The automated comparator correctly reported the traces as divergent. In this case, a failed regression threshold is positive evidence that the expected bug fix changed operation.

#figure(
  image("figures/preempt-exit-bug-fix.png", width: 100%),
  caption: [Representative aligned preemption-exit trace. The divergence corresponds to a previously identified version 2.15.1 rail-preemption exit defect that was corrected in version 2.18.1. The figure illustrates why automated failures require operational classification rather than being labeled defects automatically.],
) <fig-preempt>

The release also changed recorded transition-state behavior. In the representative example in Figure 7, the controller's internal transition state differed while the observed signal operation did not show a material effect. This is still valuable information: the method detected a software-state change that a reviewer could classify as non-operational rather than silently assuming release equivalence.

#figure(
  image("figures/transition-state-difference.png", width: 100%),
  caption: [Representative coordination-transition difference. The high-resolution states differ between releases, but project review found no noticeable operational consequence in the displayed interval.],
) <fig-transition>

Taken together, the software-release campaign supports the second part of the feasibility question. DTW aligned most of each 23-hour output trace while preserving localized changes associated with a bug fix and transition logic. The method reduced millions of event records to a short list of configurations and time intervals requiring engineering review.
= Discussion

== Why field-derived replay is useful

A field trace is not a synthetic demand profile. It contains the actual ordering and subsecond timing of calls experienced by a production controller, including simultaneous events and rare combinations that may be difficult to anticipate in a test plan. Replaying 23 hours across 25 configurations therefore exercises substantially more controller logic than a person can reproduce manually. It also preserves realistic correlation among inputs: pedestrian calls occur alongside vehicle calls, coordination transitions occur at scheduled times, and preemption interrupts the prevailing state.

The method complements rather than replaces authored tests. A scripted functional test is preferable when a requirement has a precise expected response or when safety logic must be exhaustively challenged. Field-derived replay is preferable as a broad regression screen and as a way to reproduce an observed incident. If a field controller flashes or behaves unexpectedly, the events preceding the occurrence can be extracted, replayed repeatedly, and shortened to isolate a triggering sequence. The same test can then verify a vendor software correction or an agency timing workaround.

A baseline trace also changes the role of expected output. An engineer need not specify millions of individual responses. The accepted controller run supplies an executable behavioral oracle, while DTW accommodates benign subsecond variability. This makes detailed regression practical, but it also means that baseline defects can be preserved. A baseline should therefore be an approved version, and detected improvements—as in the preemption fix—must be deliberately accepted into a new baseline.

== Interpretation of automated flags

The comparison is a triage system, not an automatic safety certification. A sequence failure indicates different event content; a timing failure indicates that matched states occurred at different times. Neither alone explains the operational cause. Review should consider the aligned interval, relevant timing parameters, controller release notes, and whether the field input exercised the intended condition.

The same-software experiment illustrates both sensitivity and exposure. It produced no flags among unchanged configurations and large divergences in 11 changed configurations, but three applicable changes produced virtually identical output. A regression suite built from field operation gains realism at the cost of controlled coverage. Agencies can address that limitation by accumulating traces from multiple days, retaining targeted incident traces, and supplementing the field corpus with authored calls for requirements not observed in the logs.

== Portability

NTCIP 1202 is an important portability boundary because the input states do not depend on a physical suitcase connector or a proprietary simulation API. Nevertheless, NTCIP conformance is not sufficient by itself. Products may implement optional objects differently, timestamp output differently, or expose high-resolution logs through different mechanisms. Before cross-vendor use, the replay adapter, output adapter, event mapping, and latency model must be validated against known calls.

The current output collector is manufacturer-specific, but the comparison is not. Once two controller traces are converted to the common `(event identifier, parameter, timestamp)` representation, the same grouping, DTW alignment, timing analysis, and reporting code can run offline. This separation makes a multi-vendor extension technically plausible and identifies exactly what integration work remains.

= Limitations

The evaluation used software emulators from one controller product family. Emulator behavior may differ from physical controller hardware, especially where cabinet inputs, hardware monitoring, or operating-system timing are involved. Configuration accommodations made for the emulator could also influence behavior. Replication on physical controllers and a second controller family is needed before claiming general cross-platform validity.

The field records represent one agency, 25 configurations, and one approximately 23-hour period per configuration. The traces are broad but not exhaustive. A behavior absent from the recorded day cannot be evaluated, which is the likely explanation for at least some changed configurations that passed the parameter intervention. Future work should quantify incremental coverage from additional days and targeted incident traces.

The reported thresholds were engineering screening settings, not statistically optimized decision boundaries. The present archive does not contain completed one-factor sensitivity reruns for grouping tolerance, phase-call reliability threshold, and timing tolerance. Those analyses remain a high-value pre-submission improvement. The six release review candidates also require a configuration-by-configuration final classification by the author; the manuscript currently reports only the manually reviewed bug-fix and transition examples supported by the project presentation and diagnostic figures.

The original controller replay requires emulator software, production configurations, and agency infrastructure. The stored databases allow the comparison and reporting stage to be reproduced without those systems, but the replay campaign itself is only procedurally reproducible. Finally, a prior baseline can contain unwanted behavior. Automated similarity cannot decide whether the reference behavior is correct.

= Conclusions

This study provides a positive feasibility result. Approximately 23 hours of field-derived vehicle, pedestrian, and preemption events for each of 25 production signal configurations were converted to NTCIP calls and replayed to test controllers. A Jaccard-cost DTW comparison aligned the resulting discrete event groups despite small timing variations and automatically retained missing, additional, and changed events as divergences.

The same-software comparison supplies the clearest evidence of determinism: all 11 unchanged configurations passed, with 99.6 percent mean sequence match and 97.7 percent mean timing match. The known timing intervention was flagged in 11 of 14 affected configurations and in none of the unchanged configurations. The version 2.15.1-to-2.18.1 campaign passed 19 configurations and directed six to review, including an expected preemption bug fix and a transition-state change.

The practical finding is not that replay makes controller testing automatic from end to end. It makes an otherwise unmanageable quantity of real operational testing repeatable and reviewable. Field histories can become a regression corpus for release screening, timing-change evaluation, and bug replication. Engineering judgment remains necessary to approve the baseline, interpret divergences, and determine whether a detected difference is beneficial, harmful, or immaterial.

= Acknowledgments

The author developed and evaluated this work as part of traffic-signal operations at the Oregon Department of Transportation. [AUTHOR REVIEW: add any colleagues, vendor personnel, funding, or required agency disclaimer before submission.]

= Code and Data Availability

Signal-Replay is an open-source Python package available at `https://github.com/ShawnStrasser/ATC-Signal-Replay`. The repository includes the replay and comparison implementation, tests, an offline synthetic example, analysis scripts, and instructions distinguishing controller-dependent replay from offline comparison. The exact public release or commit cited by the submitted paper must be added after the author pushes the current work. A candidate export script can package replay inputs, collected outputs, parameters, and expected summaries without controller firmware or proprietary configuration databases. No dataset has been published automatically.

= Generative-AI Disclosure

OpenAI Codex was used to inspect repository materials, assist with literature discovery, revise analysis and documentation code, generate data visualizations from stored results, and draft and format the manuscript. The author reviewed the source materials and is responsible for the methods, factual accuracy, interpretation, citations, and submitted text.

#bibliography("references.bib", title: [References])