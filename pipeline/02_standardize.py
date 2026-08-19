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
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from paths import STAGE_02_SOURCES, STAGE_02_DIR, STAGE_01_DIR, REPO_ROOT
from config import PLATFORM_BY_INST_DATE
from src.readers import INSTRUMENT_TASKS, make_spectra_reader
from src.align import raw_stem
from src.provenance import git_info, check_clean, upstream_ref

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

# ── Main loop ─────────────────────────────────────────────────────────────────

check_clean(REPO_ROOT, context='Stage 02')

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
            # OPEN ITEM (see CLAUDE.md "Known issues"): Aeris files are non-monotonic in
            # time — the instrument resyncs its clock, stepping the timestamp back ~1-2 s
            # every ~69 rows. Not introduced here; present identically in raw. Harmless
            # inside this repo (align.load_aligned_series sorts) but Stage 03/04 is the
            # delivered product and direct readers will trip on it. Fix is `df.sort_index()`
            # on the next line — verified to drop nothing (0 exact-duplicate timestamps).
            # Deliberately NOT applied yet: needs a full 01->04 rerun to propagate.
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

# ── Write routing manifest ────────────────────────────────────────────────────
# For instruments that appear on both platforms (Ultra321, Pico017), read the
# first UTC timestamp from each corrected Raw Parquet and look up the platform
# from PLATFORM_BY_INST_DATE.  Keyed by raw_stem so Raw/Eng/Spectra from the
# same session all share one routing entry.
# Using the actual UTC timestamp (not the filename date tag) avoids errors from
# the Mountain Time clock being ~7 hours behind UTC.

ROUTING_INSTRUMENTS = ['LANL_aerisultra321', 'LANL_aerispico017']

routing: dict = {}
print(f"\n{'═' * 60}")
print("  Building routing manifest")
print(f"{'═' * 60}")

for inst in ROUTING_INSTRUMENTS:
    raw_dir = STAGE_02_DIR / inst / 'Raw'
    if not raw_dir.exists():
        print(f"  [WARN]  {inst}/Raw not found — skipping")
        continue
    for f in sorted(raw_dir.glob('*.parquet')):
        try:
            idx = pd.read_parquet(f, columns=[]).index
            if idx.empty:
                print(f"  [WARN]  {f.name} is empty — skipping")
                continue
            ts = idx[0]
            platform = None
            for delta in [0, -1]:   # try UTC date, then UTC-1 (midnight rollover)
                candidate = (ts + pd.Timedelta(days=delta)).strftime('%y%m%d')
                platform = PLATFORM_BY_INST_DATE.get((inst, candidate))
                if platform:
                    break
            if platform:
                key = raw_stem(f)
                routing[key] = platform
                print(f"  {key:<55}  {platform}")
            else:
                print(f"  [WARN]  {f.name}: no schedule entry near {ts.date()} — unrouted")
        except Exception as e:
            print(f"  [WARN]  {f.name}: {e}")

routing_path = STAGE_02_DIR / "routing_manifest.json"
with open(routing_path, "w") as fh:
    json.dump(routing, fh, indent=2, sort_keys=True)

mml_n = sum(1 for v in routing.values() if v == 'MML')
wyo_n = sum(1 for v in routing.values() if v == 'WYO')
print(f"\n  Routing manifest: {len(routing)} entries  (MML={mml_n}, WYO={wyo_n})")

# ── Write run manifest ────────────────────────────────────────────────────────

git_hash, git_dirty = git_info(REPO_ROOT)
manifest = {
    "stage":       "02_standardize",
    "run_utc":     datetime.now(timezone.utc).isoformat(),
    "git_hash":    git_hash,
    "git_dirty":   git_dirty,
    "upstream":    upstream_ref(STAGE_01_DIR / "ts_offsets.json"),
    "instruments": manifest_stats,
}
manifest_path = STAGE_02_DIR / "run_manifest.json"
with open(manifest_path, "w") as fh:
    json.dump(manifest, fh, indent=2)

print(f"\n{'═' * 60}")
print(f"  Stage 02 complete      →  {STAGE_02_DIR}")
print(f"  Run manifest           →  {manifest_path}")
print(f"  Routing manifest       →  {routing_path}")
print(f"{'═' * 60}")
