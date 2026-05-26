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

Raw files live on CHPC and are **read-only**. Each pipeline stage writes to its own output directory. All pipeline paths are defined in `paths.py` (repo root) — do not hardcode data paths in scripts. Instrument readers, column rename maps, and the task registry live in `src/readers.py`. Analysis-facing metadata lives in `config/instruments.yaml` and `config/deployments.yaml`.

```
raw/  (read-only)
 └─► 01_utc_corrected/      Stage 01 — Aeris internal clock corrected
      └─► 02_standardized/      Stage 02 — uniform UTC TIMESTAMP index, clean column names, Parquet
           └─► 03_instrument_aligned/  Stage 03 — cross-correlation lag offsets applied
                └─► 04_daily/          Stage 04 — per-instrument daily Parquet (planned)
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
| LANL GPS | `raw/LANL_toughbook/GPS/` | raw/ (toughbook epoch; lag inherited from LANL_Anem) |
| LANL Anem | `raw/LANL_toughbook/Anem/` | raw/ (toughbook epoch; H2O spike aligned in Stage 03) |
| WYO PTR-TOF | `raw/WYO_PTR-TOF/` | — (no data yet) |

---

## Pipeline

| Stage | Script | Output | Status |
|---|---|---|---|
| 01 — UTC clock correction | `pipeline/01_utc_correction.ipynb` | `01_utc_corrected/` | ⚠️ Needs re-run |
| 02 — Standardize | `pipeline/02_standardize.py` | `02_standardized/` | ⚠️ Needs re-run |
| 03 — Instrument alignment | `pipeline/03_instrument_alignment.ipynb` | `03_instrument_aligned/` | ⚠️ Needs interactive run |
| 04 — Daily merge | `pipeline/04_daily_merge.py` | `04_daily/` | 🔲 Not yet built |

To run Stage 02:
```bash
python pipeline/02_standardize.py
```

To run Stage 03: open `pipeline/03_instrument_alignment.ipynb` in JupyterLab with the `mobile-slv` kernel. Run cells top-to-bottom; step through the widget review sections (A→E), then run the Save and Apply cells. `lag_offsets.json` is written after every commit — no work is lost if the kernel dies mid-review.

### Stage 02 output structure

```
02_standardized/
├── {instrument}/
│   ├── Raw/             ← gas/met Parquet (UTC TIMESTAMP index, ts_status=utc_corrected or trusted)
│   ├── Raw/no_coverage/ ← uncorrected LANL files (ts_status=no_coverage, Mountain Time clock)
│   ├── Eng/             ← engineering + GPS Parquet
│   ├── Eng/no_coverage/
│   ├── Spectra/ or Spectralite/       ← spectra Parquet (1,034 cols — use column projection)
│   └── Spectra/no_coverage/
├── WYO_picarro/         ← flat (no subdirectory)
├── UOU_LGR/             ← flat
├── WYO_sprinter/        ← flat
├── LANL_GPS/            ← flat
├── LANL_Anem/           ← flat
└── run_manifest.json    ← git hash + per-instrument counts from last run
```

### Stage 03 output structure

```
03_instrument_aligned/
├── {instrument}/{subdir}/
│   ├── *.parquet            ← lag-shifted aligned files
│   ├── bad/*.parquet        ← files marked bad in widget (startup sessions, noisy); no lag applied
│   └── bad_timestamp/       ← Stage 02 no_coverage pass-through; Mountain Time clock (unreliable)
├── WYO_picarro/             ← trusted pass-through (lag = 0)
├── WYO_sprinter/            ← trusted pass-through (lag = 0)
├── LANL_GPS/                ← lag inherited from LANL_Anem by date match
├── lag_offsets.json         ← confirmed lags + rejected list; written during widget review
└── apply_manifest.json      ← apply stats written by Apply + pass-through cells
```

### Stage 03 sections

| Section | Instrument | Reference | Method |
|---|---|---|---|
| A | WYO_aerisultra460 | Picarro CH4 | auto cross-correlation |
| B | LANL_aerisultra321 (WYO dates) | Picarro CH4 | auto cross-correlation |
| C | LANL_aerispico017 (WYO dates) | Picarro CH4 | auto cross-correlation |
| D | LANL_aerispico017 (MML dates) + UOU_LGR | Ultra321 CH4 | auto cross-correlation |
| E | LANL_Anem | Ultra321 H2O_ppm | manual spike alignment (u/v/w vs H2O, z-scored) |
| — | LANL_GPS | — | inherits LANL_Anem lag by date match |

---

## src/

| Module | Used by | Purpose |
|---|---|---|
| `aeris_clock.py` | Stage 01 | Clock offset computation and application for Aeris instruments |
| `readers.py` | Stage 02 | Per-instrument file readers, rename maps, `INSTRUMENT_TASKS` registry |
| `align.py` | Stage 03 | Pure alignment utilities: `resample_series`, `cross_correlate`, `apply_lag_to_parquet`, `raw_stem` |

---

## archive_legacy_analysis/

Old pipeline scripts and analysis notebooks preserved for reference. Not maintained. See `archive_legacy_analysis/README.md` for contents.
