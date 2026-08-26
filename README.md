# mobile-slv

## STATUS: v2 pipeline — ETL + QA/QC (incl. calibration)

Supersedes [`v1.0-etl-freeze`](https://github.com/agmeyer4/mobile-slv/releases/tag/v1.0-etl-freeze),
which froze at `recleaned/` and explicitly excluded calibration. **v2 rebuilds the pipeline as
four numbered stages and brings calibration in as Stage 04.**

**Scope rule:** this repo holds *deterministic, reproducible-from-a-log* data preparation —
determine a correction from a known reference (logger clock, cross-correlation lag, tank
manifest), save it, apply it once. Open-ended scientific interpretation, exploratory
comparison, and spectra inspection belong in
[`mobile-hydrocarbon-analysis`](https://github.com/agmeyer4/mobile-hydrocarbon-analysis).
Calibration qualifies under that rule; it is not an exception to it. Before extending scope
further, apply the same test — deterministic-from-a-log, or open-ended?

**The final ETL output is `04_calibrated/`**, which downstream analysis reads directly. It is a
complete mirror of Stage 03's breadth (calibrated gas files plus untouched pass-through of
spectra, GPS, anemometer, sprinter, and LGR), so analysis needs only that one directory.

---

ETL and QA/QC pipeline for the **Salt Lake Valley Winter Mobile Campaign 2026** (Jan 15 – Mar 10).
Takes raw instrument files through clock correction, standardization, cross-instrument time
alignment, and gas calibration, producing an analysis-ready Parquet dataset with full
provenance back to the raw data and the exact commit that produced it.

---

## Setup

```bash
mamba env create -f environment.yml   # creates the 'mobile-slv' conda environment
conda activate mobile-slv
nbstripout --install                  # strip notebook outputs on git add (run once per clone)
```

`environment.lock.yml` holds exact resolved versions for reproducing a canonical run.
`environment.yml` is the human-edited statement of intent — regenerate the lock with
`conda env export -n mobile-slv --no-builds` after deliberately changing a dependency.

> **Notebook execution requires the `mobile-slv` environment's Python.** A system Python
> without `pyarrow` will report every Parquet file as unreadable.

---

## Running the full pipeline

Stages must run in order. Only Stage 02 is fully unattended.

```bash
# Stage 01 — Aeris clock correction (notebook, Run All)
jupyter nbconvert --execute --inplace pipeline/01_utc_correction.ipynb

# Stage 02 — Standardize (unattended, tens of minutes; re-parses ~14 GB of text)
python pipeline/02_standardize.py

# Stage 03a / 03b — alignment (interactive widgets; see below before running)
#   open in JupyterLab with the mobile-slv kernel

# Stage 04 — Calibration (notebook, Run All; method locks are in the config cell)
jupyter nbconvert --execute --inplace pipeline/04_calibration.ipynb

# Gate: verify the timestamp index of every output file
python scripts/check_timestamps.py --stage 02 03 04
```

`pipeline/03_survey.ipynb` is a **one-time** manual quality survey (341 files across 7
instruments) whose result lives in `03_instrument_aligned/quality_manifest.yaml`. It is not
part of a normal re-run — the manifest persists and stays valid across upstream changes.
Back it up before anything that could clear the Stage 03 directory; it is the only artifact
in the pipeline that cannot be regenerated without redoing the whole survey.

> ### Re-running Stage 03 without redoing the review
> The 03a/03b widgets hold their state in notebook globals, and `save_lag_offsets_*()`
> serialises whatever is in them. A fresh kernel running top-to-bottom with
> `RESUME_REVIEW = False` would therefore write an **empty** manifest over a real one and
> then apply 0 s to every file.
>
> **Set `RESUME_REVIEW = True`** (in the *Resume saved review state* cell) to seed the
> globals from the saved `lag_offsets_{wyo,mml}.json`. You can then re-run the Save and
> Apply cells to rebuild the aligned Parquet after an upstream change **without redoing any
> human review** — the lag numbers are unchanged; only `regen_git_hash` / `regen_run_utc`
> move.
>
> A genuine re-review (`RESUME_REVIEW = False`, stepping through every session) is only
> needed when the upstream *timeline itself* changed, since every lag was cross-correlated
> against the old one.

---

## Data lineage

Raw files live on CHPC and are **read-only**. Each stage writes to its own output directory.
All pipeline paths are defined in `paths.py` (repo root) — never hardcode data paths.

```
raw/  (read-only)
 └─► 01_utc_corrected/          Stage 01 — Aeris timestamps rebuilt from the logger host clock
      └─► 02_standardized/      Stage 02 — uniform UTC TIMESTAMP index, clean columns, Parquet
           └─► 03_instrument_aligned/   Stage 03 — cross-correlation / spike lag offsets applied
                └─► 04_calibrated/      Stage 04 — gas calibration applied (*_cal columns)
```

`04_calibrated/` is a **complete mirror** of Stage 03's breadth — calibrated gas files plus
straight pass-through of everything calibration doesn't touch (spectra, GPS, anemometer,
sprinter, LGR), so downstream analysis can read everything from one directory.

### Instrument sources

| Instrument | Raw location | Stage 01 source |
|---|---|---|
| Picarro G2401 | `raw/WYO_picarro/` | raw/ (trusted) |
| Sprinter met/GPS | `raw/WYO_sprinter/` | raw/ (trusted) |
| Aeris Ultra 460 | `raw/WYO_aerisultra460/` | raw/ (trusted) |
| Aeris Ultra 321 | `raw/LANL_aerisultra321/` | **01_utc_corrected/** (clock fixed) |
| Aeris Pico 017 | `raw/LANL_aerispico017/` | **01_utc_corrected/** (clock fixed) |
| UOU LGR | `raw/UOU_LGR/final/` | raw/ (trusted) |
| LANL GPS | `raw/LANL_toughbook/GPS/` | raw/ (toughbook epoch; GPS-corrected in Stage 03b) |
| LANL Anem | `raw/LANL_toughbook/Anem/` | raw/ (toughbook epoch; GPS-corrected in Stage 03b) |
| WYO PTR-TOF | `raw/WYO_PTR-TOF/` | — (no data; stub in pipeline) |

### Platform / date schedule

**Rule: everything before 2026-02-03 was MML (Toughbook only). RPi files for those dates are
accidental duplicates — ignore.**

| Date | Platform | Instruments | Logger |
|---|---|---|---|
| 2026-01-19 to 01-22 | MML | Ultra321, Pico017 | Toughbook |
| 2026-02-02 | MML | Ultra321 | Toughbook |
| 2026-02-03 | WYO | Ultra321, Ultra460, Picarro | RPi (no Pico017) |
| 2026-02-04 | MML | Ultra321, Pico017 | Toughbook |
| 2026-02-04 | WYO | Ultra460, Picarro | — (trusted) |
| 2026-02-05 to 02-12 | WYO | Ultra321, Pico017, Ultra460, Picarro | RPi |
| 2026-03-08 | MML | Ultra321, Pico017 | Toughbook |
| 2026-03-10 | MML | Ultra321, Pico017, LGR | Toughbook |

---

## Pipeline

| Stage | Entry point | Output | Mode |
|---|---|---|---|
| 01 — UTC clock correction | `pipeline/01_utc_correction.ipynb` | `01_utc_corrected/` | Run All |
| 02 — Standardize | `pipeline/02_standardize.py` | `02_standardized/` | unattended CLI |
| 03 — Quality survey | `pipeline/03_survey.ipynb` | `quality_manifest.yaml` | interactive, one-time |
| 03a — WYO alignment | `pipeline/03a_align_wyo.ipynb` | `03_instrument_aligned/` | interactive widget |
| 03b — MML alignment | `pipeline/03b_align_mml.ipynb` | `03_instrument_aligned/` | interactive widget |
| 04 — Calibration | `pipeline/04_calibration.ipynb` | `04_calibrated/` | Run All |
| 04 — Calibration QC | `pipeline/04_calibration_qc.ipynb` | *(none — read-only)* | Run All |

### Stage 01 — Aeris clock correction

Ultra321's timestamp counter ticks a fixed 1.024 s per sample while the unit actually samples
every ~0.992 s. It gains ~32 ms/sample and the firmware corrects with a hard jump back of
−2.000 s (or −3.000 s) roughly every 69 samples. **The backstep is the correction** — the rows
before it are up to 2 s too late.

Stage 01 fixes this at source by joining each row to the logger's host clock: the Toughbook/RPi
`.dat` files log both `Epoch_time` (host receipt) and the instrument's own `Time Stamp` for the
same rows, so `build_host_clock_map` pools every logger file into
`{instrument Time Stamp -> host Epoch_time}` and each Aeris row is matched by exact string.

Sorting by the instrument timestamp was investigated and **rejected**: file order is
acquisition order and is already correct, so sorting reorders rows away from truth to make a
wrong clock self-consistent, and leaves the error untouched.

Every corrected row carries a **`ts_source`** column:

| value | meaning |
|---|---|
| `logger_epoch` | matched to a real host-clock row — the accurate case |
| `median_offset` | no logger match; per-file median offset applied. **Still carries the ~2 s sawtooth.** |
| `instrument_clock` | no logger coverage at all for this file |
| `unpaired` | spectra row with no counterpart in the paired Raw file (label unknown; the timestamp itself is corrected) |

Files with no logger coverage go to a `no_coverage/` subdirectory rather than being silently
mixed in with corrected output.

### Stage 02 — Standardize

Reads every instrument through `src/readers.py`, applies rename maps, and writes Parquet with
a tz-aware UTC `TIMESTAMP` index. Guarantees `ts_source` is present on every file
(`instrument_clock` for instruments Stage 01 doesn't touch).

Spectra files are **headerless and positional** — `read_spectra` derives the spectral channel
count from total column width, so `ts_source` cannot be written into them at Stage 01. It is
recovered here by joining the paired Raw file on the timestamp string.

A `sort_index` remains, demoted to a **net** rather than the fix: after Stage 01 it can only
touch the residual `median_offset` rows and Ultra460.

### Stage 03 — Instrument alignment

`03_survey.ipynb` first (one-time) to mark each file good / uncertain / bad into
`quality_manifest.yaml`. Then:

**03a (WYO platform)** — auto cross-correlation against Picarro CH4.

| Section | Instrument | Reference | Method |
|---|---|---|---|
| A | WYO_aerisultra460 | Picarro CH4 | auto cross-correlation |
| B | LANL_aerisultra321 (WYO dates) | Picarro CH4 | auto cross-correlation |
| C | LANL_aerispico017 (WYO dates) | Picarro CH4 | auto cross-correlation |
| — | WYO_picarro, WYO_sprinter | — | trusted pass-through (lag = 0) |

**03b (MML platform)** — manual H2O spike alignment against the anemometer.

| Section | Instrument | Reference | Method |
|---|---|---|---|
| E1 | LANL_aerisultra321 (MML dates) | Anem u/v/w | manual H2O spike (`normalize=True`) |
| E2 | LANL_aerispico017 (MML dates) | Anem u/v/w | manual H2O spike (`normalize=True`) |
| E3 | UOU_LGR (Mar 10) | Anem u/v/w | manual H2O spike (`normalize=True`) |
| F | LANL_GPS | GPS satellite UTC | auto: median(toughbook_epoch − GPS_UTC) per date |
| — | LANL_Anem, LANL_GPS | — | GPS correction only (−gps_corr) |

**Total lag for MML gas:** `tube_lag − gps_corr`  **For Anem / GPS:** `−gps_corr`

> **Verifying a widget-driven review actually landed.** The apply step's `ok`/`bad`/`warn`
> counts are *not* sufficient — an un-reviewed session is applied as `tube_lag = 0.0` with a
> `[WARN no tube lag]` and still counts as `ok`. After a review, read
> `lag_offsets_{wyo,mml}.json` directly and confirm `tube_lags` has one entry per
> non-rejected session. An empty or short dict beside a high `warn` count is the signature of
> clicks that never persisted.

### Stage 04 — Calibration

Two notebooks, deliberately separated:

- **`04_calibration.ipynb`** — the applied path only. Fits, saves `calibration_coefs.json`,
  writes calibrated Parquet. Run All; no widgets.
- **`04_calibration_qc.ipynb`** — candidate comparison, drift QC, interference diagnostics.
  **Writes no files.** This is where methods are compared; it has no power to change output.

Two explicit locks live in the applied notebook's config cell, with inline `assert`s tying
them to the code path so an edited lock without a matching code change fails loudly.

**`CAL_METHOD_LOCKED`** — *how* each species is calibrated:

| species | method | why |
|---|---|---|
| `CH4` | `tank` | Both candidates viable. Tank wins at the certified ladder points, and this is a plume-detection campaign — accuracy at plume concentrations matters more than the cross-cal's tighter ambient agreement. Tank is also directly traceable to the certified standards. |
| `C3H8` | `tank` | Forced — only Ultra321 measures C3H8, so no reference partner exists. |
| `C2H6` | `reference` | Forced — the tank has one certified point (NOAA, 1.63 ppb), far below the ambient/plume range. Ultra460 stands in as reference. |

**`CAL_DROPPED`** — *which individual* `(gas, instrument)` corrections are withheld despite
being computable. A dropped correction is still **fit and plotted** in the notebook (so the
evidence for rejecting it stays on the page) but is excluded from `calibration_coefs.json` and
never applied, so that instrument simply has no calibrated column for that gas.

| dropped | reason |
|---|---|
| `('C2H6', 'Ultra321')` | R²(span) = 0.826 (vs Pico017's 0.998) and the fit residual correlates **+0.813 with C3H8** at the matched peaks — C3H8 leaking into the C2H6 retrieval, which no anchor choice fixes. Re-verified on the corrected timeline: the Stage 01 fix moved the interference correlation +0.947 → +0.813 but left R²(span) unchanged, so the defect is structural, not a timing artifact. **Ultra321 files carry no `C2H6_ppb_cal` column.** Its CH4 and C3H8 corrections are unaffected. |

CH4/C3H8 use a single multi-point OLS against the tank ladder (not piecewise — the residual
pattern that would motivate a low/high split appears identically in Picarro, so it reflects
dilution-manifold imprecision, not instrument nonlinearity). Canonical date is **Feb 12**, the
only event spanning the full 0–57 ppm range; Feb 3 and Feb 6 are computed as drift QC only.

Apply formula: `calibrated = (measured * scale_in - intercept) / slope`

---

## Output structure

### Stage 02

```
02_standardized/
├── {instrument}/
│   ├── Raw/                    ← gas/met Parquet (UTC index, ts_status, ts_source)
│   ├── Raw/no_coverage/        ← no logger coverage; Mountain Time clock, unreliable
│   ├── Eng/  Eng/no_coverage/  ← engineering + GPS
│   └── Spectra/ or Spectralite/ (+ no_coverage/)   ← 1,034 cols — use column projection
├── WYO_picarro/  UOU_LGR/  WYO_sprinter/  LANL_GPS/  LANL_Anem/   ← flat
├── routing_manifest.json       ← which raw_stem ran on which platform
└── run_manifest.json           ← git hash + per-instrument counts
```

### Stage 03

```
03_instrument_aligned/
├── {instrument}/{subdir}/
│   ├── *.parquet               ← lag-shifted aligned files
│   ├── bad/                    ← marked bad in the widget; no lag applied
│   └── bad_timestamp/          ← Stage 02 no_coverage pass-through
├── WYO_picarro/  WYO_sprinter/ ← trusted pass-through (lag = 0)
├── LANL_GPS/  LANL_Anem/       ← GPS correction only (−gps_corr, lag_ref=GPS_satellite_UTC)
├── quality_manifest.yaml       ← the one-time survey (NOT regenerable — back it up)
├── lag_offsets_wyo.json        ← 03a confirmed lags + rejected list
├── lag_offsets_mml.json        ← 03b tube lags + GPS corrections + rejected list
└── apply_manifest_wyo/mml.json ← apply stats
```

### Stage 04

```
04_calibrated/
├── {instrument}/{Raw,Eng}/     ← *_cal columns + cal_coefs_ref column
├── {spectra}/ {GPS} {Anem} {sprinter} {LGR}   ← pass-through, NO cal_coefs_ref
├── calibration_coefs.json      ← per-correction coefficients + method/confidence tags
├── apply_manifest.json         ← per-instrument file/row counts
└── passthrough_manifest.json   ← what was copied unchanged
```

The presence of a `cal_coefs_ref` column is how to tell a calibrated file from a pass-through.

---

## Conventions

- **All timestamps are UTC**, tz-aware with a `+00:00` offset, from Stage 02 onward.
- **`ts_status`** — `"utc_corrected"` / `"no_coverage"` / `"trusted"`, per file.
- **`ts_source`** — per **row** timestamp provenance (see Stage 01 table above).
- **All Stage 02+ output is Parquet.** Use `pd.read_parquet(path, columns=[...])`; spectra
  files have 1,034 columns, so column selection matters.
- **Never modify `raw/`** — read-only source of truth.
- **`paths.py` is the single source of truth for paths.** Import via `from paths import ...`.
- **`nbstripout` strips notebook outputs on `git add`** — executed HTML belongs with the data,
  not in git.

---

## Reproducibility

Every stage writes a manifest recording the commit that produced it, so any output directory
traces back to exact code and exact upstream inputs.

- **`git_hash` / `git_dirty`** — on every manifest, via `src/provenance.git_info()`.
- **`upstream`** — from Stage 02 onward each manifest embeds the upstream stage's own
  `{stage, git_hash, git_dirty, run_utc}` via `upstream_ref()`, so the full chain from raw to
  any output is traceable without matching timestamps by hand.
- **`regen_git_hash` / `regen_git_dirty`** — Stage 03's apply manifests carry these separately
  from `git_hash`/`git_dirty`, which record when the alignment was **decided** (the widget
  commit), not when the Parquet was last mechanically **rebuilt**.
- **`check_clean(REPO_ROOT, context=...)`** — called before each stage's write step; prints a
  loud non-fatal warning if the tree is dirty, so a non-reproducible run doesn't silently
  record `git_dirty: true`.
- **Human judgment persists outside the notebooks** — `quality_manifest.yaml` and
  `lag_offsets_*.json` survive kernel restarts and code changes. See *Re-running Stage 03*
  above.
- **Freezing a canonical run** — `jupyter nbconvert --execute --to html <notebook>
  --output-dir <its data stage dir>`. The executed HTML is the human-readable record of what
  ran; the manifest JSON plus that HTML together form the complete frozen record.

### Verification gate

```bash
python scripts/check_timestamps.py --stage 02 03 04    # add --verbose to list every offender
```

Checks every Parquet file's index for sort order, backsteps, duplicates, and tz-awareness, and
reports the `ts_source` breakdown. Exits non-zero if anything is unclean — safe to use in a
shell gate.

---

## Known issues and data-user caveats

These are properties of the delivered dataset. Read before treating any column as absolute.

- **~1.6% of Aeris rows still carry the ~2 s sawtooth.** Rows tagged `ts_source =
  "median_offset"` had no logger coverage, so only a per-file median offset could be applied.
  Filter on `ts_source` if timing accuracy matters. Rows in `no_coverage/` / `bad_timestamp/`
  directories are worse still — a Mountain Time clock.
- **The host clock is a serial *receipt* time**, and the Toughbook is not NTP-synced. Stage 01
  trades a systematic ±2 s ramp for roughly 0.4 s zero-mean noise. Absolute accuracy still
  rides on Stage 03's GPS correction.
- **`WYO_sprinter` has 72 duplicate timestamps** across 336,909 rows (0.02%), in 15 of 17
  files. The sprinter logs at ~10 Hz but its `UTC hhmmss` field has 0.1 s resolution, so
  collisions are expected. There are **zero exact-duplicate rows** — colliding rows carry
  genuinely different readings, so de-duplicating would discard real data. Left as-is
  deliberately. `merge_asof` tolerates duplicate keys, but `.loc[ts]` returns a frame instead
  of a row and `reindex` raises. `load_aligned_series` drops dupes (`keep='first'`) for
  in-repo use.
- **C2H6 is not traceable to a certified zero.** Pico017's baseline is anchored to match
  Ultra460's own ambient median, not the tank's absolute zero (the two genuinely disagree at
  baseline). This makes C2H6_cal cross-instrument-consistent but **not** an absolute
  measurement. The whole C2H6 calibration also rests on Ultra460's C2H6 being correct, which
  is assumed, not independently validated.
- **Ultra321 has no calibrated C2H6.** See `CAL_DROPPED` above.
- **MML-date calibration is an extrapolation.** All three tank events fell inside the WYO
  window (Feb 3–12); Feb-12 coefficients are applied to the January and March MML dates with
  no direct tank evidence there.
- **Method locks are a point-in-time decision.** Re-run `04_calibration_qc.ipynb` if new tank
  or ambient data should prompt revisiting one.

---

## Repository layout

### `src/`

| Module | Stage | Purpose |
|---|---|---|
| `aeris_clock.py` | 01 | `build_host_clock_map`, `correct_timestamps`, `apply_host_clock_to_raw/_spectra`. Scalar-offset fallbacks retained for coverage mapping. |
| `readers.py` | 02 | Per-instrument readers, rename maps, `INSTRUMENT_TASKS` registry. |
| `align.py` | 03 | `resample_series`, `cross_correlate`, `apply_lag_to_parquet`, `load_quality_manifest`, `load_aligned_series`, `resume_review`. |
| `calibration.py` | 04 | Generic fitting/apply/plotting — no campaign-specific logic. Entry points: `calibrate_and_check_tank`, `calibrate_and_check_reference`, `fit_reference_cal`. Comparison harness: `compare_candidate_coefs`, `assess_tank_coverage`. |
| `provenance.py` | all | `git_info`, `check_clean`, `upstream_ref`. |

### `scripts/`

| Script | Purpose |
|---|---|
| `check_timestamps.py` | Ingestion gate — timestamp index validation + `ts_source` breakdown for any stage. |
| `parquet_to_gpx.py` | Export a track to GPX for inspection in mapping tools. |

### `config/`

`instruments.yaml` and `deployments.yaml` are **descriptive metadata** for analysis consumers
(instrument types, gas channels, platform schedule, logger relationships). They are not parsed
by the pipeline itself — `paths.py` and `src/readers.py::INSTRUMENT_TASKS` drive execution, and
instrument keys match across all three.

Note that a `gases` entry lists what an instrument physically **measures**, which is not the
same as what has a calibrated `*_cal` column — see `CAL_DROPPED`.

### `archive_legacy_analysis/`

Pre-restructure pipeline scripts and analysis notebooks, kept for reference. **Not maintained;
do not run.** See its own README for contents.
