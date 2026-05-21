# archive_legacy_analysis

Notebooks, scripts, and data artifacts that were part of this repo's original Phase 5–6 scope
(daily merge, calibration, spectral inspection). They are preserved here for reference but are
**not maintained**. All active Phase 5+ work has moved to `mobile-hydrocarbon-analysis`.

| File | Description |
|---|---|
| `merge_daily.py` | Project-specific daily merge (8 instruments → 1s grid, Utah local date grouping). Step 5 now belongs in `mobile-hydrocarbon-analysis`; copy this file there as a starting point. |
| `03_calibration.ipynb` | Cross-calibration of Ultra460, Ultra321, Pico017 CH4 vs Picarro |
| `03_calibration copy.ipynb` | Working copy from calibration session |
| `04_inspect.ipynb` | Interactive spectral inspection at periods of interest |
| `05_spectra.ipynb` | Spectra analysis |
| `apply_calibration.py` | Script to apply calibration coefficients from `calibration_coefs.json` |
| `calibration_coefs.json` | Calibration coefficients produced by `03_calibration.ipynb`. Canonical copy belongs in `mobile-hydrocarbon-analysis`. |
