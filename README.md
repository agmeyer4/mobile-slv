# mobile-slv

Analysis code for the Salt Lake Valley Winter Mobile Campaign 2026 (Jan 15 – Mar 10).
Two mobile labs (Wyoming Mobile Lab and Meyer Mobile Lab) measured CH4, C2H6, C3H8, and
other trace gases across the SLV. This repo handles timestamp correction, lag verification,
calibration, and spectra inspection.

## Setup

```bash
mamba env create -f environment.yml   # creates the 'mobile-slv' conda environment
conda activate mobile-slv
nbstripout --install                  # strip notebook outputs on git add (run once per clone)
```

## Data

Raw data lives outside this repo (read-only):
```
/uufs/chpc.utah.edu/common/home/lin-group24/agm/Mobile_SLV/Data/2026/raw/
```

Processed outputs are written to sibling directories (`ts_corrected/`, `cleaned/`, etc.)
under the same parent. See `Code/CLAUDE.md` for the full directory layout.

## Pipeline

| Step | Output dir | Tool |
|---|---|---|
| 1. Timestamp correction | `ts_corrected/` | `notebooks/01_timestamp_correction.ipynb` |
| 2. First clean | `cleaned/` | `mobilelab/preprocess/clean.py` |
| 3. Lag verification | — | `notebooks/02_verify_offsets.ipynb` |
| 4. Second clean (with lags) | `recleaned/` | `mobilelab/preprocess/clean.py` |
| 5. Daily merge | `merged/` | `mobilelab/preprocess/merge_daily.py` |
| 6. Analysis | — | `notebooks/03_calibration.ipynb`, `04_inspect.ipynb` |

## Notebooks

- **01_timestamp_correction** — Corrects the Aeris Ultra321 and Pico017 internal clocks
  using RPi/Toughbook logger files that contain both a correct UTC epoch and the instrument's
  wrong timestamp. Offsets vary by deployment period and are matched per-file. Results saved
  to `offsets/ts_correction_offsets.json`.

- **02_verify_offsets** — Cross-correlates cleaned instrument data against the Picarro
  reference to detect and fine-tune residual lags. Also verifies the Ultra460 timestamps.

- **03_calibration** — Cross-calibration using known-concentration periods.

- **04_inspect** — Interactive inspection of spectra at periods of interest.

## src

`src/timestamp_correction.py` — utilities for loading logger files, computing per-deployment
offsets, matching Aeris files to logger coverage windows, and applying corrections.
