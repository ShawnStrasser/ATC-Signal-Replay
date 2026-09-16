# Changelog

## 1.0.0 (2026-09-16)

First stable release. From here on, renaming or removing anything importable from `signal_replay` bumps the major version; new features bump the minor version; fixes bump the patch version.

Changes since the last published release (0.1.0):

### Renamed: firmware to software

The validation workflow tests controller **software** releases, not firmware, and the code now says so.

- `FirmwareTestSuite` is now `SoftwareTestSuite`, `compare_firmware()` is now `compare_software()`, and the `firmware_version` field, settings key, and checkpoint key are now `software_version`.
- The workspace folder `firmware_validation/` is now `software_validation/` and the runner `firmware_validate.py` is now `software_validate.py`. `extract_data.py` takes `--software-version`.
- Default `SoftwareTestSuite.output_dir` is `./software_test_results`. The HTML report is titled "Software Validation Report".

### Added

- Adaptive per-device replay-latency calibration from sparse detector events (`replay_latency_offset_lookback_min`), for time-of-day aligned replays.
- Fatal controller-collection errors fail the validation batch instead of appearing as completed runs.
- Comparison validity handling: intervals with missing or unreliable data are excluded rather than reported as behavioral differences.
- Support and documentation for comparing timing-parameter changes with the software held constant.
- Offline synthetic comparison example under `examples/offline_comparison/`.
- `duckdb` declared as a direct dependency. It was imported by the package but only installed transitively.
- `pytest` collects `tests/` by default.

### Documentation

- README rewritten around the two workflows (single-study replay and software validation), with inputs, outputs, a configuration reference, and the package API an application would call.
- `software_validation/README.md` documents every settings key, catalog column, command-line flag, and output file.
- Removed documentation for `load_from_yaml`, which never existed.

### Migration from 0.2.x

Replace the identifiers above in code and in `settings.json`. Existing `results/<version>/` folders keep working. Checkpoint files written by 0.2.x contain a `firmware_version` key that is informational only.

## 0.1.0

Initial public release: NTCIP detector replay, MAXTIME output collection, conflict detection, and DTW comparison.
