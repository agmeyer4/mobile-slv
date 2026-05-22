"""
Stage 02: Standardize files from all instruments.

Reads source paths from STAGE_02_SOURCES (config.py), writes standardized CSV
files (UTC TIMESTAMP index, clean column names) to STAGE_02_DIR/{instrument}/.

Spectra files are NOT processed here — they remain in 01_utc_corrected/ for
inspection via add_spectra_headers.py when needed.

Instruments without a reader (WYO_PTR-TOF, Extra_GPS) are skipped with a notice.

Usage:
    python pipeline/02_standardize.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import STAGE_02_SOURCES, STAGE_02_DIR
from src.standardize import (
    read_aeris_raw, read_picarro, read_lgr, read_sprinter,
    ULTRA321_RENAME, PICO017_RENAME, ULTRA460_RENAME,
)

# ── Per-instrument reader + file-glob config ──────────────────────────────────
# reader: callable(path) -> DataFrame | None
# globs:  list of glob patterns relative to the instrument source directory

INSTRUMENT_CONFIG = {
    "LANL_aerisultra321": {
        "reader": lambda p: read_aeris_raw(p, ULTRA321_RENAME),
        "globs":  ["Raw/*.txt"],
    },
    "LANL_aerispico017": {
        "reader": lambda p: read_aeris_raw(p, PICO017_RENAME),
        "globs":  ["Raw/*.txt"],
    },
    "WYO_aerisultra460": {
        "reader": lambda p: read_aeris_raw(p, ULTRA460_RENAME),
        "globs":  ["Raw/*.txt"],
    },
    "WYO_picarro": {
        "reader": read_picarro,
        "globs":  ["*.dat"],
    },
    "UOU_LGR": {
        "reader": read_lgr,
        "globs":  ["**/*.txt"],
    },
    "WYO_sprinter": {
        "reader": read_sprinter,
        "globs":  ["*.csv"],
    },
    "WYO_PTR-TOF": None,  # no data yet
    "Extra_GPS":   None,  # GPX parsing not implemented
}

# ── Run ───────────────────────────────────────────────────────────────────────

STAGE_02_DIR.mkdir(parents=True, exist_ok=True)

for instrument, src_dir in STAGE_02_SOURCES.items():
    cfg = INSTRUMENT_CONFIG.get(instrument)

    if cfg is None:
        print(f"\n[STUB]  {instrument} — not yet implemented, skipping")
        continue

    files = []
    for glob in cfg["globs"]:
        files.extend(sorted(src_dir.glob(glob)))

    if not files:
        print(f"\n[WARN]  {instrument} — no files found in {src_dir}")
        continue

    out_dir = STAGE_02_DIR / instrument
    out_dir.mkdir(parents=True, exist_ok=True)

    n = len(files)
    print(f"\n{'═' * 60}")
    print(f"  {instrument}  ({n} files)  →  {out_dir.name}/")
    print(f"{'═' * 60}")

    n_ok = n_empty = n_err = 0
    for i, path in enumerate(files):
        prog = f"({i+1}/{n})"
        try:
            df = cfg["reader"](path)
            if df is None or df.empty:
                print(f"  {prog}  EMPTY   {path.name}")
                n_empty += 1
                continue
            out_path = out_dir / (path.stem + ".csv")
            df.to_csv(out_path)
            print(f"  {prog}  OK      {path.name}  [{len(df):,} rows]")
            n_ok += 1
        except Exception as e:
            print(f"  {prog}  ERROR   {path.name}  — {e}")
            n_err += 1

    print(f"\n  Done — ok: {n_ok}", end="")
    if n_empty:
        print(f"  empty: {n_empty}", end="")
    if n_err:
        print(f"  errors: {n_err}", end="")
    print()

print(f"\n{'═' * 60}")
print(f"  Stage 02 complete  →  {STAGE_02_DIR}")
print(f"{'═' * 60}")
