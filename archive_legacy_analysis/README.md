# archive_legacy_analysis

Scripts and notebooks from the previous pipeline iteration. Preserved for reference — **not maintained, not run as part of the current pipeline.**

The current pipeline uses `pipeline/` and `src/` in the repo root.

## Old cleaning scripts (superseded by `src/standardize.py`)

| File | What it did |
|---|---|
| `clean.py` | Interactive CSV cleaner — configured header skip, column selection, timestamp format per instrument, then batch-processed files |
| `clean_sprinter.py` | WYO Sprinter CSV cleaner (SKIPROWS=3, packed 4-column UTC timestamp) |
| `clean_anem.py` | Toughbook Trisonica anemometer parser (SKIPROWS=6, labeled-value format) |
| `clean_gps.py` | Toughbook NMEA GPS parser (SKIPROWS=6, GPRMC/GPGGA/GPVTG sentences) |
| `apply_offsets.py` | Applied per-file lag offsets from JSON to `*_clean.csv` files → `recleaned/` |
| `add_spectra_headers.py` | Prepended correct column header to headerless Aeris Spectra/Spectralite files; derived column names from paired Raw file |

## Old analysis notebooks (superseded by `mobile-hydrocarbon-analysis`)

| File | Description |
|---|---|
| `03_calibration.ipynb` | Cross-calibration of Ultra460, Ultra321, Pico017 CH4 vs Picarro |
| `03_calibration copy.ipynb` | Working copy from calibration session |
| `04_inspect.ipynb` | Interactive inspection of gas data at periods of interest |
| `05_spectra.ipynb` | Spectral data inspection |
| `apply_calibration.py` | Applies calibration coefficients from `calibration_coefs.json` |
| `calibration_coefs.json` | Calibration coefficients from `03_calibration.ipynb`; canonical copy belongs in `mobile-hydrocarbon-analysis` |
| `merge_daily.py` | Project-specific daily merge (8 instrument streams → 1s grid, Utah local date); use as a starting point in `mobile-hydrocarbon-analysis` |
