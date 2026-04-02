# SNMP Detector Event Latency — Scaling Study

**Date:** 2026-04-02  
**Scenarios:** 70 completed, 0 failed  
**Runtime:** 30 Minutes

---

## Background

This study measures the round-trip latency of SNMP-based detector event replay against
MAXTIME emulator devices. A single controller is addressed over UDP; detector ON/OFF events
are fired on a fixed schedule, and the controller's HTTP event log is polled to confirm receipt.
Latency is defined as the difference between the scheduled send time and the logged timestamp.

Two phases were run:

- **Single-port detector scaling** — one device active at a time, sweeping detector counts
  (1,8,16,32) across 10 ports.
  Port order is randomized within each detector-count block,
  so port identity and execution sequence are not aliased.
- **Device-count scaling** — 3× repeated, 8 detectors per device,
  stepping from 1 to 10 active devices.
  Each repetition uses a different random port selection and level ordering, giving genuine
  replication across the full device-count range.

Each scenario replayed ON/OFF bursts every 5 s (hold 1 s) for 20 s.

---

## Calibration recommendation

> **Use a single global latency offset of 185.3 ms. Do not condition it on detector count or device count.**

After subtracting 185.3 ms from each event:

- Median absolute residual: **24.8 ms**
- P95 absolute residual: **74.1 ms**
- Events within ±50 ms: **78%**
- Events within ±100 ms: **100%**

The rationale is in the statistical tests below — neither detector count nor device count
produced a statistically significant effect, so a single fixed offset is the right choice.

---

## Statistical tests

Permutation tests (20 000 shuffles) check whether latency varies with each factor.
**R²** = fraction of variance explained by the factor. **r** = Pearson correlation (−1 to +1).

| Factor | Effect size | Permutation p | Significant? |
| --- | --- | --- | --- |
| Detector count | R² = 0.076 | 0.760 | No |
| Device count | r = 0.309 | 0.098 | No |
| Overall burst load | r = 0.072 | 0.552 | No |
| Port / device instance† | R² = 0.604 | <0.001 | Yes† |

† Port order was randomized, so this reflects genuine per-port latency variation in the emulator, not an execution-order artifact.

---

## Offset model comparison

Comparing a single global offset against per-detector-count and per-device-count tables.
MAE and P95 are computed over matched event residuals.

| Offset model | Offset value | MAE (ms) | P95 error (ms) | Within ±100 ms |
| --- | --- | --- | --- | --- |
| Single global offset | 185.3 ms | 30.1 | 74.1 | 100% |
| Per-detector-count (4 values) | varies | 29.3 | 75.5 | 100% (+2.9%) |
| Per-device-count (10 values) | varies | 28.2 | 86.1 | 99% (+6.1%) |

Per-detector-count offsets improve MAE by only 2.9% — not worth the added complexity.
Per-device-count offsets show a larger nominal improvement (6.1%) but the relationship
is non-monotonic and the correlation is not statistically significant,
so it likely reflects noise rather than a real trend.

---

## Results by detector count

Median and P95 aggregated across all ports (10 runs per detector count).

| Detectors | Median (ms) | P95 (ms) | Runs |
| --- | --- | --- | --- |
| 1 | 193 | 209 | 10 |
| 8 | 185 | 223 | 10 |
| 16 | 198 | 209 | 10 |
| 32 | 180 | 198 | 10 |

---

## Charts

![Latency by detector count](plots/detector_scale.png)
![Latency vs device count](plots/device_scale.png)
![Load vs latency](plots/load_scatter.png)
![Residual distribution](plots/residuals.png)

