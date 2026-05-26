"""
Stage 02: Standardize files from all instruments.

INPUT:  per-instrument source directories defined in STAGE_02_SOURCES (paths.py),
        including no_coverage/ subdirs from Stage 01.
OUTPUT: STAGE_02_DIR/{instrument}/{subdir}/{stem}.parquet

What "standardized" means:
  - Index is TIMESTAMP (UTC, tz-aware +00:00)
  - Science columns are renamed to clean cross-instrument names (CH4_ppm, etc.)
  - All other original columns are preserved unchanged
  - ts_status column added: "utc_corrected" | "no_coverage" | "trusted"
  - Format is always Parquet (column projection supported via pd.read_parquet columns=)

Tasks are built from INSTRUMENT_TASKS in src/readers.py. Adding a new instrument:
  1. Add its entry to INSTRUMENT_TASKS in src/readers.py
  2. Add its source path to STAGE_02_SOURCES in paths.py
  Instruments in STAGE_02_SOURCES with no INSTRUMENT_TASKS entry are logged as stubs.

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

from paths import STAGE_02_SOURCES, STAGE_02_DIR, REPO_ROOT
from src.readers import INSTRUMENT_TASKS, make_spectra_reader

# ── Task list (built from registry) ───────────────────────────────────────────

def _resolve_reader(spec, src_dir):
    if "reader" in spec:
        return spec["reader"]
    return make_spectra_reader(src_dir / spec["spectra_raw_subdir"])

TASKS = [
    {"instrument": inst, "src_dir": src, "glob": s["glob"],
     "reader": _resolve_reader(s, src), "out_subdir": s["out_subdir"], "ts_status": s["ts_status"]}
    for inst, specs in INSTRUMENT_TASKS.items()
    for s in specs
    for src in [STAGE_02_SOURCES[inst]]
]

STUBS = [inst for inst in STAGE_02_SOURCES if inst not in INSTRUMENT_TASKS]

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
