"""
Stage 02: Standardize files from all instruments.

INPUT:  per-instrument source directories defined in STAGE_02_SOURCES (config.py),
        including no_coverage/ subdirs from Stage 01.
OUTPUT: STAGE_02_DIR/{instrument}/{subdir}/{stem}.parquet

What "standardized" means:
  - Index is TIMESTAMP (UTC, tz-aware +00:00)
  - Science columns are renamed to clean cross-instrument names (CH4_ppm, etc.)
  - All other original columns are preserved unchanged
  - ts_status column added: "utc_corrected" | "no_coverage" | "trusted"
  - Format is always Parquet (column projection supported via pd.read_parquet columns=)

Output directory structure mirrors Stage 01 for LANL instruments:
  {instrument}/Raw/             corrected gas/met
  {instrument}/Raw/no_coverage/ uncorrected gas/met (instrument clock, ~MT)
  {instrument}/Eng/             corrected engineering + GPS
  {instrument}/Eng/no_coverage/ uncorrected engineering + GPS
  {instrument}/Spectra/         corrected spectra (parquet)
  {instrument}/Spectra/no_coverage/
  {instrument}/Spectralite/     WYO_aerisultra460 spectralite (parquet)

Non-Aeris instruments write flat under {instrument}/ (no subdirectory).

WYO_aerisultra460 has trusted timestamps; no no_coverage subdirs.
WYO_PTR-TOF and Extra_GPS are logged as stubs and skipped.

A run_manifest.json is written to STAGE_02_DIR/ at the end of each run.

Usage:
    python pipeline/02_standardize.py
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import STAGE_02_SOURCES, STAGE_02_DIR, STAGE_01_DIR, RAW_DIR, REPO_ROOT
from src.standardize import (
    read_aeris_raw, read_picarro, read_lgr, read_sprinter, read_spectra,
    ULTRA321_RENAME, ULTRA321_ENG_RENAME,
    PICO017_RENAME,  PICO017_ENG_RENAME,
    ULTRA460_RENAME, ULTRA460_ENG_RENAME,
)

# ── Source directory aliases ───────────────────────────────────────────────────
_U321 = STAGE_01_DIR / "LANL_aerisultra321"
_P017 = STAGE_01_DIR / "LANL_aerispico017"
_U460 = RAW_DIR / "WYO_aerisultra460"

# ── Task list ─────────────────────────────────────────────────────────────────
# Each task is one glob pattern → one output subdirectory.
#
# Keys:
#   instrument  output lives under STAGE_02_DIR/{instrument}/
#   src_dir     base directory for the glob
#   glob        glob pattern relative to src_dir
#   reader      callable(path) → DataFrame | None
#   out_subdir  subdirectory under {instrument}/  (empty string = flat)
#   ts_status   "utc_corrected" | "no_coverage" | "trusted"

TASKS = [
    # ── LANL_aerisultra321 (source: 01_utc_corrected) ────────────────────────
    {"instrument": "LANL_aerisultra321", "src_dir": _U321,
     "glob": "Raw/*.txt",              "reader": lambda p: read_aeris_raw(p, ULTRA321_RENAME),
     "out_subdir": "Raw",              "ts_status": "utc_corrected"},
    {"instrument": "LANL_aerisultra321", "src_dir": _U321,
     "glob": "Raw/no_coverage/*.txt",  "reader": lambda p: read_aeris_raw(p, ULTRA321_RENAME),
     "out_subdir": "Raw/no_coverage",  "ts_status": "no_coverage"},
    {"instrument": "LANL_aerisultra321", "src_dir": _U321,
     "glob": "Eng/*.txt",              "reader": lambda p: read_aeris_raw(p, ULTRA321_ENG_RENAME),
     "out_subdir": "Eng",              "ts_status": "utc_corrected"},
    {"instrument": "LANL_aerisultra321", "src_dir": _U321,
     "glob": "Eng/no_coverage/*.txt",  "reader": lambda p: read_aeris_raw(p, ULTRA321_ENG_RENAME),
     "out_subdir": "Eng/no_coverage",  "ts_status": "no_coverage"},
    {"instrument": "LANL_aerisultra321", "src_dir": _U321,
     "glob": "Spectra/*.txt",          "reader": lambda p: read_spectra(p, _U321 / "Raw"),
     "out_subdir": "Spectra",          "ts_status": "utc_corrected"},
    {"instrument": "LANL_aerisultra321", "src_dir": _U321,
     "glob": "Spectra/no_coverage/*.txt", "reader": lambda p: read_spectra(p, _U321 / "Raw"),
     "out_subdir": "Spectra/no_coverage", "ts_status": "no_coverage"},

    # ── LANL_aerispico017 (source: 01_utc_corrected) ─────────────────────────
    {"instrument": "LANL_aerispico017", "src_dir": _P017,
     "glob": "Raw/*.txt",              "reader": lambda p: read_aeris_raw(p, PICO017_RENAME),
     "out_subdir": "Raw",              "ts_status": "utc_corrected"},
    {"instrument": "LANL_aerispico017", "src_dir": _P017,
     "glob": "Raw/no_coverage/*.txt",  "reader": lambda p: read_aeris_raw(p, PICO017_RENAME),
     "out_subdir": "Raw/no_coverage",  "ts_status": "no_coverage"},
    {"instrument": "LANL_aerispico017", "src_dir": _P017,
     "glob": "Eng/*.txt",              "reader": lambda p: read_aeris_raw(p, PICO017_ENG_RENAME),
     "out_subdir": "Eng",              "ts_status": "utc_corrected"},
    {"instrument": "LANL_aerispico017", "src_dir": _P017,
     "glob": "Eng/no_coverage/*.txt",  "reader": lambda p: read_aeris_raw(p, PICO017_ENG_RENAME),
     "out_subdir": "Eng/no_coverage",  "ts_status": "no_coverage"},
    {"instrument": "LANL_aerispico017", "src_dir": _P017,
     "glob": "Spectra/*.txt",          "reader": lambda p: read_spectra(p, _P017 / "Raw"),
     "out_subdir": "Spectra",          "ts_status": "utc_corrected"},
    {"instrument": "LANL_aerispico017", "src_dir": _P017,
     "glob": "Spectra/no_coverage/*.txt", "reader": lambda p: read_spectra(p, _P017 / "Raw"),
     "out_subdir": "Spectra/no_coverage", "ts_status": "no_coverage"},

    # ── WYO_aerisultra460 (source: raw — trusted timestamps) ─────────────────
    {"instrument": "WYO_aerisultra460", "src_dir": _U460,
     "glob": "Raw/*.txt",              "reader": lambda p: read_aeris_raw(p, ULTRA460_RENAME),
     "out_subdir": "Raw",              "ts_status": "trusted"},
    {"instrument": "WYO_aerisultra460", "src_dir": _U460,
     "glob": "Eng/*.txt",              "reader": lambda p: read_aeris_raw(p, ULTRA460_ENG_RENAME),
     "out_subdir": "Eng",              "ts_status": "trusted"},
    {"instrument": "WYO_aerisultra460", "src_dir": _U460,
     "glob": "Spectralite/*.txt",      "reader": lambda p: read_spectra(p, _U460 / "Raw"),
     "out_subdir": "Spectralite",      "ts_status": "trusted"},

    # ── WYO_picarro (source: raw) ─────────────────────────────────────────────
    {"instrument": "WYO_picarro", "src_dir": STAGE_02_SOURCES["WYO_picarro"],
     "glob": "*.dat", "reader": read_picarro, "out_subdir": "", "ts_status": "trusted"},

    # ── UOU_LGR (source: raw/final) ───────────────────────────────────────────
    {"instrument": "UOU_LGR", "src_dir": STAGE_02_SOURCES["UOU_LGR"],
     "glob": "*.dat", "reader": read_lgr, "out_subdir": "", "ts_status": "trusted"},

    # ── WYO_sprinter (source: raw) ────────────────────────────────────────────
    {"instrument": "WYO_sprinter", "src_dir": STAGE_02_SOURCES["WYO_sprinter"],
     "glob": "*.csv", "reader": read_sprinter, "out_subdir": "", "ts_status": "trusted"},
]

STUBS = ["WYO_PTR-TOF", "Extra_GPS"]

# ── Manifest helper ───────────────────────────────────────────────────────────

def _git_info():
    try:
        h = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT), text=True
        ).strip()
        dirty = subprocess.call(["git", "diff", "--quiet"], cwd=str(REPO_ROOT)) != 0
        return h, dirty
    except Exception:
        return "unknown", False


# ── Main loop ─────────────────────────────────────────────────────────────────

STAGE_02_DIR.mkdir(parents=True, exist_ok=True)
manifest_stats: dict = {}

for task in TASKS:
    instrument = task["instrument"]
    src_dir    = task["src_dir"]
    glob_pat   = task["glob"]
    reader     = task["reader"]
    out_subdir = task["out_subdir"]
    ts_status  = task["ts_status"]

    label = f"{instrument}/{out_subdir}" if out_subdir else instrument

    files = sorted(src_dir.glob(glob_pat))
    if not files:
        print(f"\n[WARN]  {label} — no files found")
        manifest_stats[label] = {"ok": 0, "empty": 0, "errors": 0, "warn": "no files"}
        continue

    out_dir = STAGE_02_DIR / instrument / out_subdir if out_subdir else STAGE_02_DIR / instrument
    out_dir.mkdir(parents=True, exist_ok=True)

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
            df = reader(path)
            if df is None or df.empty:
                print(f"  {prog}  EMPTY   {path.name}")
                n_empty += 1
                empty_files.append(path.name)
                continue
            df["ts_status"] = ts_status
            out_path = out_dir / (path.stem + ".parquet")
            df.to_parquet(out_path)
            print(f"  {prog}  OK      {path.name}  [{len(df):,} rows]")
            n_ok += 1
        except Exception as e:
            print(f"  {prog}  ERROR   {path.name}  — {e}")
            n_err += 1
            error_files.append({"file": path.name, "error": str(e)})

    print(f"\n  Done — ok: {n_ok}", end="")
    if n_empty: print(f"  empty: {n_empty}", end="")
    if n_err:   print(f"  errors: {n_err}", end="")
    print()

    entry: dict = {"ok": n_ok, "empty": n_empty, "errors": n_err}
    if empty_files: entry["empty_files"] = empty_files
    if error_files: entry["error_files"] = error_files
    manifest_stats[label] = entry

for stub in STUBS:
    print(f"\n[STUB]  {stub} — not yet implemented, skipping")
    manifest_stats[stub] = {"skipped": True}

# ── Write run manifest ────────────────────────────────────────────────────────

git_hash, git_dirty = _git_info()
manifest = {
    "stage":       "02_standardize",
    "run_utc":     datetime.now(timezone.utc).isoformat(),
    "git_hash":    git_hash,
    "git_dirty":   git_dirty,
    "instruments": manifest_stats,
}
manifest_path = STAGE_02_DIR / "run_manifest.json"
with open(manifest_path, "w") as fh:
    json.dump(manifest, fh, indent=2)

print(f"\n{'═' * 60}")
print(f"  Stage 02 complete  →  {STAGE_02_DIR}")
print(f"  Manifest           →  {manifest_path}")
print(f"{'═' * 60}")
