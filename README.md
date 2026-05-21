# mobile-slv

## **⚠️ STATUS: FROZEN / ETL ONLY ⚠️**

This repository is frozen as the **Data Engineering / ETL pipeline** for the Salt Lake Valley Winter Mobile Campaign 2026 (Jan 15 – Mar 10). It handles raw data extraction, timestamp correction, cleaning, and lag verification — **nothing else**.

**The final ETL output is `recleaned/`.** Daily merging, calibration, and all scientific analysis have moved to [`slv-hydrocarbon-analysis`](https://github.com/agmeyer4/slv-hydrocarbon-analysis), which reads directly from `recleaned/`. Do not add analysis or merge code here.

---

## Setup

```bash
mamba env create -f environment.yml   # creates the 'mobile-slv' conda environment
conda activate mobile-slv
nbstripout --install                  # strip notebook outputs on git add (run once per clone)
```

## Data Lineage

All data lives outside this repo on CHPC. Raw files are read-only; each ETL step writes to its own directory. `recleaned/` is the handoff point for `slv-hydrocarbon-analysis`.

**All `TIMESTAMP` values throughout this pipeline are UTC.** They are stored as timezone-naive strings (ISO 8601, no offset suffix); there is no daylight saving ambiguity. Interpret every `TIMESTAMP` value as UTC.

For full instrument descriptions, deployment schedule, and file format details, see [`raw/README.md`](../lin-group24/agm/Mobile_SLV/Data/2026/raw/README.md) — that is the authoritative source for the raw data.

```
Raw (read-only)
/uufs/chpc.utah.edu/common/home/lin-group24/agm/Mobile_SLV/Data/2026/raw/
    ├── WYO_picarro/            ← trusted reference (~2s accuracy, used as UTC truth)
    ├── WYO_sprinter/           ← trusted GPS/met
    ├── WYO_aerisultra460/      ← usually correct timestamps, verified in Step 3
    ├── LANL_aerisultra321/     ← WRONG internal clock — corrected in Step 1
    ├── LANL_aerispico017/      ← WRONG internal clock — corrected in Step 1
    ├── LANL_rpi/               ← dual-timestamp logger used to correct Ultra321/Pico017 (WYO)
    ├── LANL_toughbook/         ← dual-timestamp logger used to correct Ultra321/Pico017 (MML)
    └── UOU_LGR/                ← trusted timestamps

Step 1 → ts_corrected/
/uufs/chpc.utah.edu/common/home/lin-group24/agm/Mobile_SLV/Data/2026/ts_corrected/
    Corrects the Aeris Ultra321 and Pico017 internal clocks using the co-located
    RPi/Toughbook logger files. Offsets are per-file (not global) and saved to
    offsets/ts_correction_offsets.json.

    Only Ultra321 and Pico017 files are actually rewritten here.
    WYO_picarro and WYO_sprinter are symlinked directly to raw/ (no correction needed).
    Ultra460, Toughbook, and LGR are copied as-is (trusted timestamps).

Step 2 → cleaned/
/uufs/chpc.utah.edu/common/home/lin-group24/agm/Mobile_SLV/Data/2026/cleaned/
    All streams standardized to uniform CSV format with a TIMESTAMP index.
    No lag offsets applied yet — this is the input for cross-correlation in Step 3.

Step 3 (no write) — cross-correlation results saved to offsets/*_lag.json

Step 4 → recleaned/   ← FINAL ETL OUTPUT
/uufs/chpc.utah.edu/common/home/lin-group24/agm/Mobile_SLV/Data/2026/recleaned/
    Per-file lag offsets applied to cleaned/ files. One directory per instrument stream.
    Picarro, Sprinter, and Toughbook copied unchanged (trusted timestamps).
    This is the handoff to slv-hydrocarbon-analysis for merging, calibration, and analysis.
```

---

## Pipeline

| Step | Output dir | Tool |
|---|---|---|
| 1. Aeris internal clock correction | `ts_corrected/` | `notebooks/01_timestamp_correction.ipynb` |
| 2. First clean | `cleaned/` | `mobilelab/preprocess/clean.py` |
| 3. Lag verification (cross-correlation) | — (offsets JSON) | `notebooks/02_verify_offsets.ipynb` |
| 4. Second clean with lag offsets | `recleaned/` | `mobilelab/preprocess/apply_offsets.py` |

**All four steps are complete.** `recleaned/` contains the final per-instrument CSVs for all deployment days (Jan 19–22, Feb 2–12, Mar 8, Mar 10).

---

## Notebooks

- **01_timestamp_correction** — Corrects the Aeris Ultra321 and Pico017 internal clocks
  using RPi/Toughbook logger files that contain both a correct UTC epoch and the instrument's
  wrong timestamp. Offsets vary by deployment period and are matched per-file. Results saved
  to `offsets/ts_correction_offsets.json`.

- **02_verify_offsets** — Cross-correlates cleaned instrument data against the Picarro
  reference to detect and fine-tune residual lags. Also verifies the Ultra460 timestamps.
  Results saved to `offsets/*_lag.json` and `offsets/*_rejected.json`.

Notebooks 03–05 have been moved to `archive_legacy_analysis/` and are superseded by
`slv-hydrocarbon-analysis`.

---

## src

ETL helper scripts in `src/`:

- `timestamp_correction.py` — Phase 1 offset engine (load logger files, compute offsets, apply corrections)
- `add_spectra_headers.py` — prepend correct header to headerless Aeris Spectra/Spectralite files
- `clean_sprinter.py` — clean WYO Sprinter CSVs
- `clean_gps.py` — parse Toughbook NMEA GPS files
- `clean_anem.py` — parse Toughbook Trisonica anemometer files

`merge_daily.py` has been moved to `archive_legacy_analysis/` — use it as a starting point in `slv-hydrocarbon-analysis`.

---

## offsets/

Version-controlled JSON files produced by the ETL pipeline:

| File | Phase | Description |
|---|---|---|
| `ts_correction_offsets.json` | 1 | Per-file clock offset (seconds) for Ultra321 and Pico017 |
| `ultra460_lag.json` / `_rejected.json` | 3 | Ultra460 residual lag vs Picarro |
| `ultra321_lag.json` / `_rejected.json` | 3 | Ultra321 residual lag vs Picarro / Toughbook |
| `pico017_lag.json` / `_rejected.json` | 3 | Pico017 residual lag |
| `lgr_lag.json` / `_rejected.json` | 3 | LGR residual lag |
| `ultra321_spectra_lag.json` / `_rejected.json` | 4 | Derived spectra keys for Ultra321 |
| `pico017_spectra_lag.json` / `_rejected.json` | 4 | Derived spectra keys for Pico017 |
| `ultra460_spectralite_lag.json` / `_rejected.json` | 4 | Derived spectra keys for Ultra460 |
