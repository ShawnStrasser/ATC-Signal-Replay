# Changelog

## Unreleased (0.2.0 research release)

- Added adaptive per-device replay-latency calibration from sparse detector events.
- Made fatal controller-collection errors fail the validation batch instead of appearing as completed runs.
- Improved comparison validity handling so missing or unreliable intervals are not reported as behavioral differences.
- Added support and documentation for comparing operational timing-parameter changes with unchanged software.
- Added an offline synthetic comparison example for reproducibility.