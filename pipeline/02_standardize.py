"""
Stage 02: Standardize files from all instruments.

INPUT:  per-instrument source directories defined in STAGE_02_SOURCES (config.py)
OUTPUT: STAGE_02_DIR/{instrument}/  — one CSV per input file, same filenames

What "standardized" means here:
  - Index is TIMESTAMP (UTC, tz-aware +00:00)
  - Column names are clean and consistent across runs (see RENAME maps in standardize.py)
  - Gas/met output: CSV. Spectra/Spectralite output: Parquet (fast, ~3–5x smaller than CSV).

Output directory structure:
  Aeris gas/met  →  {instrument}/Raw/         (mirrors the Raw/ subdirectory in the source)
  Non-Aeris      →  {instrument}/             (flat — no subdirectory)
  Aeris Spectra  →  {instrument}/Spectra/     (or Spectralite/ for Ultra460)

Spectra notes:
  - Ultra321 and Pico017 Spectra are sourced from 01_utc_corrected/ because their
    timestamps were corrected in Stage 01.
  - Ultra460 Spectralite is sourced directly from raw/ — its timestamps are trusted.
  - Spectra files arrive headerless; column names are derived from the paired Raw files.
  - Output columns: instrument params (Time Stamp → Tgas) | rd0 | rd1 | spec_0001 …

Instruments marked None (WYO_PTR-TOF, Extra_GPS) are logged as stubs and skipped.

A run_manifest.json recording the git hash and per-instrument file counts is written
to STAGE_02_DIR/ at the end of each run for reproducibility.

Usage:
    python pipeline/02_standardize.py
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Allow imports from the repo root (config.py, src/) regardless of working directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import STAGE_02_SOURCES, STAGE_02_DIR, REPO_ROOT
from src.standardize import (
    read_aeris_raw, read_picarro, read_lgr, read_sprinter, read_spectra,
    ULTRA321_RENAME, PICO017_RENAME, ULTRA460_RENAME,
)

# ── INSTRUMENT_CONFIG ─────────────────────────────────────────────────────────
# Drives Part 1 (gas/met files). One entry per instrument in STAGE_02_SOURCES.
#
# Keys:
#   reader     callable(path) → DataFrame | None
#   globs      glob patterns relative to the instrument's source directory
#   out_subdir subdirectory under STAGE_02_DIR/{instrument}/ for output
#              (omit for flat output directly under {instrument}/)
#
# Aeris instruments always write under Raw/ to mirror the source structure,
# which keeps gas files and Spectra files in parallel subdirectories.
# Non-Aeris instruments write flat because they have no Spectra counterpart.
#
# None means the instrument is planned but has no reader yet — it is logged
# as a stub and skipped without raising an error.

INSTRUMENT_CONFIG = {
    "LANL_aerisultra321": {
        "reader":     lambda p: read_aeris_raw(p, ULTRA321_RENAME),
        "globs":      ["Raw/*.txt"],
        "out_subdir": "Raw",
    },
    "LANL_aerispico017": {
        "reader":     lambda p: read_aeris_raw(p, PICO017_RENAME),
        "globs":      ["Raw/*.txt"],
        "out_subdir": "Raw",
    },
    "WYO_aerisultra460": {
        "reader":     lambda p: read_aeris_raw(p, ULTRA460_RENAME),
        "globs":      ["Raw/*.txt"],
        "out_subdir": "Raw",
    },
    "WYO_picarro": {
        "reader": read_picarro,
        "globs":  ["*.dat"],
    },
    "UOU_LGR": {
        "reader": read_lgr,
        "globs":  ["*.dat"],
    },
    "WYO_sprinter": {
        "reader": read_sprinter,
        "globs":  ["*.csv"],
    },
    "WYO_PTR-TOF": None,  # no data yet
    "Extra_GPS":   None,  # GPX parsing not implemented
}

# ── SPECTRA_CONFIG ────────────────────────────────────────────────────────────
# Drives Part 2 (Aeris Spectra / Spectralite files).
#
# Keys:
#   spectra_src  directory of headerless Spectra .txt files to process
#   raw_src      paired Raw directory — its header is read once to derive the
#                instrument-parameter column names (Time Stamp … Tgas) that
#                prefix every Spectra file for that instrument
#   out_subdir   subdirectory name under STAGE_02_DIR/{instrument}/

SPECTRA_CONFIG = {
    "LANL_aerisultra321": {
        "spectra_src": STAGE_02_SOURCES["LANL_aerisultra321"] / "Spectra",
        "raw_src":     STAGE_02_SOURCES["LANL_aerisultra321"] / "Raw",
        "out_subdir":  "Spectra",
    },
    "LANL_aerispico017": {
        "spectra_src": STAGE_02_SOURCES["LANL_aerispico017"] / "Spectra",
        "raw_src":     STAGE_02_SOURCES["LANL_aerispico017"] / "Raw",
        "out_subdir":  "Spectra",
    },
    "WYO_aerisultra460": {
        # Ultra460 Spectralite files come from raw/ directly — no Stage 01 correction needed
        "spectra_src": STAGE_02_SOURCES["WYO_aerisultra460"] / "Spectralite",
        "raw_src":     STAGE_02_SOURCES["WYO_aerisultra460"] / "Raw",
        "out_subdir":  "Spectralite",
    },
}

# ── Manifest helper ───────────────────────────────────────────────────────────

def _git_info():
    """Return (commit_hash, is_dirty) for the repo at REPO_ROOT."""
    try:
        h = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT), text=True
        ).strip()
        dirty = subprocess.call(["git", "diff", "--quiet"], cwd=str(REPO_ROOT)) != 0
        return h, dirty
    except Exception:
        return "unknown", False


# Accumulates per-instrument stats (ok / empty / errors) across both loops
# so they can all be written to run_manifest.json at the end.
manifest_stats = {}

# ═════════════════════════════════════════════════════════════════════════════
# Part 1 — Gas / met files
# Iterate over every instrument defined in STAGE_02_SOURCES, read each file
# with the instrument-specific reader, and write a clean CSV to STAGE_02_DIR.
# ═════════════════════════════════════════════════════════════════════════════

STAGE_02_DIR.mkdir(parents=True, exist_ok=True)

for instrument, src_dir in STAGE_02_SOURCES.items():
    cfg = INSTRUMENT_CONFIG.get(instrument)

    # Instruments with no reader are future placeholders — skip gracefully
    if cfg is None:
        print(f"\n[STUB]  {instrument} — not yet implemented, skipping")
        manifest_stats[instrument] = {"skipped": True}
        continue

    # Collect all matching input files across the configured glob patterns
    files = []
    for glob in cfg["globs"]:
        files.extend(sorted(src_dir.glob(glob)))

    if not files:
        print(f"\n[WARN]  {instrument} — no files found in {src_dir}")
        manifest_stats[instrument] = {"ok": 0, "empty": 0, "errors": 0, "warn": "no files"}
        continue

    # cfg.get("out_subdir", "") → empty string means write flat under {instrument}/
    out_dir = STAGE_02_DIR / instrument / cfg.get("out_subdir", "")
    out_dir.mkdir(parents=True, exist_ok=True)

    # label is used both for display and as the manifest key
    label = f"{instrument}/{cfg['out_subdir']}/" if "out_subdir" in cfg else f"{instrument}/"
    n = len(files)
    print(f"\n{'═' * 60}")
    print(f"  {label}  ({n} files)")
    print(f"{'═' * 60}")

    n_ok = n_empty = n_err = 0
    empty_files: list[str] = []
    error_files: list[dict] = []
    for i, path in enumerate(files):
        prog = f"({i+1}/{n})"
        try:
            df = cfg["reader"](path)
            if df is None or df.empty:
                print(f"  {prog}  EMPTY   {path.name}")
                n_empty += 1
                empty_files.append(path.name)
                continue
            out_path = out_dir / (path.stem + ".csv")
            df.to_csv(out_path)
            print(f"  {prog}  OK      {path.name}  [{len(df):,} rows]")
            n_ok += 1
        except Exception as e:
            print(f"  {prog}  ERROR   {path.name}  — {e}")
            n_err += 1
            error_files.append({"file": path.name, "error": str(e)})

    print(f"\n  Done — ok: {n_ok}", end="")
    if n_empty:
        print(f"  empty: {n_empty}", end="")
    if n_err:
        print(f"  errors: {n_err}", end="")
    print()
    entry: dict = {"ok": n_ok, "empty": n_empty, "errors": n_err}
    if empty_files:
        entry["empty_files"] = empty_files
    if error_files:
        entry["error_files"] = error_files
    manifest_stats[label.rstrip("/")] = entry

# ═════════════════════════════════════════════════════════════════════════════
# Part 2 — Spectra / Spectralite files
# Same pattern as Part 1, but using read_spectra() which:
#   1. Reads one Raw header to derive instrument column names
#   2. Reads the headerless Spectra file (timestamp col as str, rest as float)
#   3. Parses the Time Stamp column and sets a UTC TIMESTAMP index
# Output: .csv (renamed from .txt) with the same structure as gas/met files.
# ═════════════════════════════════════════════════════════════════════════════

for instrument, scfg in SPECTRA_CONFIG.items():
    spectra_src = scfg["spectra_src"]
    raw_src     = scfg["raw_src"]   # used by read_spectra to derive column names
    out_dir     = STAGE_02_DIR / instrument / scfg["out_subdir"]
    spec_label  = f"{instrument}/{scfg['out_subdir']}"

    files = sorted(spectra_src.glob("*.txt"))
    if not files:
        print(f"\n[WARN]  {spec_label} — no files found in {spectra_src}")
        manifest_stats[spec_label] = {"ok": 0, "empty": 0, "errors": 0, "warn": "no files"}
        continue

    out_dir.mkdir(parents=True, exist_ok=True)

    n = len(files)
    print(f"\n{'═' * 60}")
    print(f"  {spec_label}  ({n} files)")
    print(f"{'═' * 60}")

    n_ok = n_empty = n_err = 0
    empty_files = []
    error_files = []
    for i, path in enumerate(files):
        prog = f"({i+1}/{n})"
        try:
            df = read_spectra(path, raw_src)
            if df is None or df.empty:
                print(f"  {prog}  EMPTY   {path.name}")
                n_empty += 1
                empty_files.append(path.name)
                continue
            out_path = out_dir / (path.stem + ".parquet")
            df.to_parquet(out_path)
            print(f"  {prog}  OK      {path.name}  [{len(df):,} rows]")
            n_ok += 1
        except Exception as e:
            print(f"  {prog}  ERROR   {path.name}  — {e}")
            n_err += 1
            error_files.append({"file": path.name, "error": str(e)})

    print(f"\n  Done — ok: {n_ok}", end="")
    if n_empty:
        print(f"  empty: {n_empty}", end="")
    if n_err:
        print(f"  errors: {n_err}", end="")
    print()
    entry = {"ok": n_ok, "empty": n_empty, "errors": n_err}
    if empty_files:
        entry["empty_files"] = empty_files
    if error_files:
        entry["error_files"] = error_files
    manifest_stats[spec_label] = entry

# ── Write run manifest ────────────────────────────────────────────────────────
# Records the git commit and per-instrument counts so any output directory can
# be traced back to the exact code that produced it.

git_hash, git_dirty = _git_info()
manifest = {
    "stage":       "02_standardize",
    "run_utc":     datetime.now(timezone.utc).isoformat(),
    "git_hash":    git_hash,
    "git_dirty":   git_dirty,   # True if there were uncommitted changes at run time
    "instruments": manifest_stats,
}
manifest_path = STAGE_02_DIR / "run_manifest.json"
with open(manifest_path, "w") as fh:
    json.dump(manifest, fh, indent=2)

print(f"\n{'═' * 60}")
print(f"  Stage 02 complete  →  {STAGE_02_DIR}")
print(f"  Manifest           →  {manifest_path}")
print(f"{'═' * 60}")
