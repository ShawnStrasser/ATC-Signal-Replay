# Evidence record

## Claim-to-evidence map

| Proposed paper claim | Primary repository evidence | Interpretation boundary |
|---|---|---|
| Twenty-five approximately 23-hour field traces were used | `firmware_validation/logs/*.parquet`; `paper/generated/results/run_metadata.json`; `dataset_summary.csv` | 7,296,218 raw records and 574.998 configuration-hours; only detector, pedestrian, and preempt events become replay calls. |
| Field events are converted to NTCIP calls | `src/signal_replay/replay.py:202-224`; `sql/generate_activation_feed.sql`; `src/signal_replay/ntcip.py:35-92` | Vehicle/pedestrian states are eight-bit grouped calls; preempts are Boolean. NTCIP input portability does not eliminate vendor integration. |
| Replay timing preserves field timing and compensates latency | `src/signal_replay/replay.py`; `src/signal_replay/latency.py`; stored latency tables | Version 2.18.1 contains 707,458 latency samples and 6,577 applied updates. |
| DTW aligns discrete event traces | `src/signal_replay/comparison.py:660-741`, `1416-1490`, `2152-2520` | Local cost is Jaccard distance between timestamp-grouped event sets; rolling windows are 45 minutes advanced every 40 minutes with 60-second edge clips. |
| Unchanged same-software results demonstrate repeatability | `parameter_intervention_results.csv` joined to `intervention_manifest.csv` | All 11 unchanged configurations passed; mean sequence 99.6% (99.1--100.0), mean timing 97.7% (90.6--99.7). This is the strongest feasibility claim. |
| Known parameter changes were detected | `parameter_detection_summary.csv`; archived `2.18.1_trailing/report.html` | 11/14 changed flagged and 0/11 unchanged flagged. Three changed passes may lack exposure and are not automatically algorithmic false negatives. |
| Version 2.15.1 versus 2.18.1 produced 19 passes and six candidates | archived `2.18.1/report.html`; `software_release_results.csv` | Candidates require engineering classification; an automated failure is not equivalent to a new defect. |
| Release divergences included a bug fix and non-operational transition difference | `paper/automated_maxtime_testing_final.pptx`, slides 6, 8, and 9; extracted figures | Practitioner classification from the project presentation. Add configuration IDs/times if available. |
| Trailing settings removed short/variable overlap yellow | presentation slides 7 and 10; `short-overlap-yellow.png`; parameter report | Representative manually reviewed operational result, not an aggregate safety finding. |
| Software is open source | Git history; `CITATION.cff`; `README.md`; `examples/offline_comparison/` | Current local `main` is ahead of public GitHub. Cite a pushed release or exact final commit. |

## Focused literature comparison

| Study | Input source | Test environment | Output evaluation | Version comparison | Main distinction |
|---|---|---|---|---|---|
| Li et al. (2008), DOI 10.1016/j.trc.2007.10.001 | Authored functional scripts | Controller automated-test tool and interface hardware | Predefined expected responses | Controller/firmware testing context, not full trace regression | Closest prior system; this study replaces manually authored long-form input/expected-output definitions with field-derived inputs and a baseline output trace. |
| Ahmed et al. (2010), FHWA-ID-10-180 | XML scripts specify activation, timing, and response | NEMA TS1/TS2 controllers, CID, NTCIP variants | Automated response verification and stored results | Specific firmware/device support discussed | Establishes automated suitcase replacement and NTCIP challenges; does not replay long field histories or align complete output traces. |
| Tung (2012; 2015) | Authored NTCIP/conformance tests | Multiple controller devices | Functional/conformance outcomes | Testing across device implementations | Strong precedent for NTCIP automation and portability; different test source and output comparison. |
| Stevanovic, Klanac, and Radivojevic (2017) | Microsimulation-generated activity | Six vendors in hardware-in-the-loop | Logger and performance-measure consistency | No field-derived release regression identified | Supports event normalization and platform limitations. |
| Wang, Tian, and Yang (2021) | Microsimulation-generated detector activity | Hardware-, software-, and emulator-in-the-loop via virtual CID | Queue, delay, trajectories | Environment comparison | Supports NTCIP controller interfaces and emulator limitations; not direct whole-event regression. |
| Sakoe and Chiba (1978) | Numeric sequences | General sequence alignment | Minimum-cost monotone path | Not applicable | Mathematical foundation for DTW recurrence. |
| Sturdevant et al. (2012) | High-resolution signal events | Controller event logger vocabulary | Event definitions at tenth-second resolution | Not applicable | Defines the operational event-data substrate; not a testing method. |

## Novelty conclusion

The evidence supports this cautious statement:

> Prior work automated scripted controller-function tests and evaluated controller hardware, software, emulators, and high-resolution logging. The reviewed literature did not identify a study that converted long-duration live field input histories into reusable controller tests and automatically aligned complete controller-output event traces across numerous production configurations for software and timing-parameter regression.

Do not claim novelty for automated controller testing, NTCIP testing, controller emulation, high-resolution logging, DTW, or firmware testing individually.

## Sources considered but not emphasized

- Broad automated traffic-signal performance-measure literature: useful context but not close to behavioral regression testing.
- General record/replay software-testing literature: conceptually related but adds little transportation-specific support within the page limit.
- Additional HILS/CID papers: excluded to avoid duplicating the more directly relevant Wang and Stevanovic sources.
- Exploratory trailing-overlap conflict CSVs: excluded because the parent-overlap assumption in the uncommitted script has not been validated.

## Reproduction boundary

The stored databases and export script can support independent reproduction of the comparison and reporting stage. Reproducing event replay requires controller/emulator software, configuration databases, and agency infrastructure. The paper must keep that distinction explicit.