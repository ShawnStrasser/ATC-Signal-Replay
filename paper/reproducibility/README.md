# Comparison-only reproducibility candidate

This directory contains the exporter for a possible comparison-stage dataset. It is intentionally not populated or published by the paper build.

The candidate bundle would contain replay input events, collected output events from the three stored runs, the comparison parameters, the intervention manifest, and expected summary results. It excludes controller firmware and proprietary `.bin` configuration databases because those are not needed to reproduce event alignment and reporting.

To create a local candidate bundle after reviewing storage and approval requirements:

```powershell
py -3 paper/reproducibility/export_dataset.py --output paper/reproducibility-candidate
```

The original controller/emulator replay remains infrastructure-dependent. The bundle reproduces the comparison/reporting stage only. The author should review the bundle before attaching it to GitHub, Zenodo, or another repository.
