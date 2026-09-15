# Changelog

## 0.3.0 (2026-09-14)

The validation workflow tests controller **software** releases, not firmware, and the code now says so. This is a breaking rename.

- Renamed `FirmwareTestSuite` to `SoftwareTestSuite`, `compare_firmware()` to `compare_software()`, and the `firmware_version` field, settings key, and checkpoint key to `software_version`.
- Renamed the workspace folder `firmware_validation/` to `software_validation/` and the runner `firmware_validate.py` to `software_validate.py`. `extract_data.py` now takes `--software-version`.
- Default `SoftwareTestSuite.output_dir` is `./software_test_results`. The HTML report is titled "Software Validation Report".
- Declared `duckdb` as a direct dependency. It was imported by the package but only installed transitively.
- `pytest` now collects `tests/` by default.
- Rewrote the README around the two workflows (single-study replay and software validation), with the inputs, outputs, and the package API an application would call. Removed documentation for `load_from_yaml`, which does not exist.

Migration: replace the identifiers above in code and in `settings.json`. Existing `results/<version>/` folders keep working. Checkpoint files written by 0.2.0 contain a `firmware_version` key that is informational only.

## 0.2.0 (unreleased research version)

- Added adaptive per-device replay-latency calibration from sparse detector events.
- Made fatal controller-collection errors fail the validation batch instead of appearing as completed runs.
- Improved comparison validity handling so missing or unreliable intervals are not reported as behavioral differences.
- Added support and documentation for comparing operational timing-parameter changes with unchanged software.
- Added an offline synthetic comparison example for reproducibility.
