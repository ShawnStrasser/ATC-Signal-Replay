# Evidence record

## Repository and experiment evidence

| Claim used in paper | Evidence | Audit note |
|---|---|---|
| The method compares behavioral traces, not just firmware labels | `src/signal_replay/comparison.py`, `firmware_validation/firmware_validate.py`, reports for `2.18.1` and `2.18.1_trailing` | Same-software parameter intervention is a separate experiment. |
| 25 production configurations were included | Three `collected.db` files each contain 25 distinct `device_id` values; `paper/generated/results/dataset_summary.csv` | `2B045_c` is excluded as a non-primary diagnostic file. |
| Field logs are long-duration operational traces | 25 Parquet files in `firmware_validation/logs`; source summary in `run_metadata.json` | Raw rows include events not selected for replay. |
| 2.15.1 versus 2.18.1 produced 19/25 passes | Archived `firmware_validation/results/2.18.1/report.html`; extracted `software_release_results.csv` | Six are candidate differences, not confirmed defects. |
| Same-software parameter intervention flagged 11/14 changed and 0/11 unchanged | `firmware_validation/results/2.18.1_trailing/report.html`; `parameter_intervention_results.csv`; `parameter_detection_summary.csv` | Three changed configurations passed; exposure is insufficient to call them false negatives. |
| Adaptive latency was used in 2.18.1 runs | `latency_offset_samples` and `latency_offset_updates` tables; 707,458 samples and 6,577 updates | Settings propagation is included in commit `5eb309b`. |
| Invalid and unreliable intervals are excluded | `src/signal_replay/comparison.py`, `report.py`, and focused tests in commit `5eb309b` | Existing reports include the validity note. |
| Package state is cited | commits `5eb309b` and `1f25f36`; `CITATION.cff`; `CHANGELOG.md` | Same `main` branch retained; no push/tag/release. |

## Literature comparison

| Source | Inputs and environment | Automation / comparison | Difference from this work |
|---|---|---|---|
| Li et al. (2008), [doi:10.1016/j.trc.2007.10.001](https://doi.org/10.1016/j.trc.2007.10.001) | Traffic-controller automated-testing tool and controller functions | Automated scripted testing | Does not evaluate long field-derived traces aligned against high-resolution output across 25 production configurations. |
| Ahmed et al. (2010), [FHWA-ID-10-180](https://rosap.ntl.bts.gov/view/dot/23916) | Idaho controller test tool, XML input scripts, NTCIP/TS2 context | Automated functional tests and reports | Scripted test cases rather than operational-log replay and trace alignment. Verify report identifier before submission. |
| Tung (2012), [FDOT report](https://rosap.ntl.bts.gov/view/dot/25053) | NTCIP-based controller test scripts across vendor implementations | Automated conformance/functional tests | Broad manufacturer coverage, but not long field-event replay with output sequence scoring. |
| Tung (2015), [FDOT Phase 2](https://rosap.ntl.bts.gov/view/dot/28634) | Expanded NTCIP controller tests | Automated tests across multiple manufacturers | Demonstrates portable NTCIP testing; this work adds event-trace behavioral regression screening. |
| Stevanovic et al. (2017), [FDOT report](https://rosap.ntl.bts.gov/view/dot/31807) | Six controllers, HIL/SIL, high-resolution event codes | HIL/SIL and high-resolution controller evaluation | Establishes high-resolution logging and emulator/HIL practice, not field-derived replay comparison at this scale. |
| Wang et al. (2019), [VCID](https://doi.org/10.1109/MITS.2019.2898968) | Virtual controller interface for HILS | Virtualized controller experimentation | Focuses on interface/HILS architecture rather than automated regression classification from recorded operations. |
| Sakoe and Chiba (1978), [doi:10.1109/TASSP.1978.1163055](https://doi.org/10.1109/TASSP.1978.1163055) | Dynamic time warping for time sequences | Foundational sequence alignment | Methodological foundation, not a traffic-signal application. |

The cautious novelty statement is: **Prior work has automated scripted controller-function tests and used hardware-, software-, and emulator-in-the-loop environments. The reviewed literature did not identify an evaluation that combined long-duration field-derived input replay, automated high-resolution output-trace alignment, and repeated testing across numerous production configurations to detect both software- and parameter-induced behavioral changes.** This is a combination claim, not a claim that NTCIP, emulation, high-resolution logging, automated testing, or dynamic time warping is individually new.

## Reproduction caveat

`reproduce_results.py` cross-checks archived report rows against stored DuckDB counts and hashes. It does not start a controller or rerun the 25 replays. The paper must call this a comparison/reporting-stage reproduction and identify new controller replay as future work unless the author reruns it.
