# mobile-slv

ETL pipeline for the **Salt Lake Valley Winter Mobile Campaign 2026** (Jan 15 – Mar 10). Handles raw data ingestion, UTC timestamp correction, standardization, and instrument time alignment. Analysis and calibration live in [`mobile-hydrocarbon-analysis`](https://github.com/agmeyer4/mobile-hydrocarbon-analysis).

---

## Setup

```bash
mamba env create -f environment.yml   # creates the 'mobile-slv' conda environment
conda activate mobile-slv
nbstripout --install                  # strip notebook outputs on git add (run once per clone)
```

---

## Data lineage

Raw files live on CHPC and are **read-only**. Each pipeline stage writes to its own output directory. All paths are defined in `config.py` — do not hardcode paths in scripts.

```
raw/  (read-only)
 └─► 01_utc_corrected/   Stage 01 — Aeris internal clock corrected
      └─► 02_standardized/   Stage 02 — uniform UTC TIMESTAMP index, clean column names
           └─► 03_instrument_aligned/   Stage 03 — cross-correlation lag offsets applied
```

**All `TIMESTAMP` values are UTC** with a `+00:00` offset suffix throughout the pipeline.

### Instrument sources

| Instrument | Raw location | Stage 01 source |
|---|---|---|
| Picarro G2401 | `raw/WYO_picarro/` | raw/ (trusted) |
| Sprinter met/GPS | `raw/WYO_sprinter/` | raw/ (trusted) |
| Aeris Ultra 460 | `raw/WYO_aerisultra460/` | raw/ (trusted) |
| Aeris Ultra 321 | `raw/LANL_aerisultra321/` | **01_utc_corrected/** (clock fixed) |
| Aeris Pico 017 | `raw/LANL_aerispico017/` | **01_utc_corrected/** (clock fixed) |
| UOU LGR | `raw/UOU_LGR/final/` | raw/ (trusted) |
| WYO PTR-TOF | `raw/WYO_PTR-TOF/` | — (no data yet) |

---

## Pipeline

| Stage | Script | Output | Status |
|---|---|---|---|
| 01 — UTC clock correction | `pipeline/01_utc_correction.ipynb` | `01_utc_corrected/` | ✅ Complete |
| 02 — Standardize | `pipeline/02_standardize.py` | `02_standardized/` | ⚠️ See note below |
| 03 — Instrument alignment | `pipeline/03_instrument_alignment.ipynb` | `03_instrument_aligned/` | 🔲 Not yet built |

**Stage 02 note:** Gas/met files are complete. Spectra files (1,034 columns, up to 676 MB each) require a format decision before running — see the open decision in `CLAUDE.md`.

To run Stage 02:
```bash
python pipeline/02_standardize.py
```

### Stage 02 output structure

```
02_standardized/
├── LANL_aerisultra321/
│   ├── Raw/           ← gas/met CSVs (TIMESTAMP index, UTC)
│   └── Spectra/       ← standardized spectra (TIMESTAMP index, named columns)
├── LANL_aerispico017/
│   ├── Raw/
│   └── Spectra/
├── WYO_aerisultra460/
│   ├── Raw/
│   └── Spectralite/
├── WYO_picarro/       ← flat (no subdirectory)
├── UOU_LGR/
├── WYO_sprinter/
└── run_manifest.json  ← git hash + per-instrument counts from last run
```

---

## src/

| Module | Used by | Purpose |
|---|---|---|
| `aeris_clock.py` | Stage 01 | Clock offset computation and application for Aeris instruments |
| `standardize.py` | Stage 02 | Per-instrument file readers; all column rename maps |

---

## offsets/

Legacy JSON files from the previous pipeline iteration. **Not consumed by the current pipeline.** Kept for reference; Stage 03 (when built) will write its own offsets to `03_instrument_aligned/`.

---

## archive_legacy_analysis/

Old pipeline scripts and analysis notebooks preserved for reference. Not maintained. See `archive_legacy_analysis/README.md` for contents.
