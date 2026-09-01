#!/usr/bin/env python3
"""Check the timestamp index of every Parquet file under a pipeline stage directory.

Ingestion gate for Stages 02/03/04: verifies that each delivered file is sorted in
time, free of duplicate timestamps, and tz-aware UTC. Reads only the index (no data
columns), so a full campaign scan takes seconds even over multi-GB spectra files.

Background: the three Aeris units are non-monotonic in raw — Ultra321's counter ticks a
fixed 1.024 s per sample while the unit samples every ~0.992 s, so the firmware jumps the
timestamp back 2 s every ~69 rows. Stage 01 fixes this properly by joining each row to the
logger's host clock; Stage 02's sort is only a net for the rows that join could not cover.
This script confirms both halves landed: the ordering columns show the net worked, and the
ts_source breakdown shows how much of the data is genuinely logger-backed rather than
carrying the residual ~2 s sawtooth.

Usage:
    python scripts/check_timestamps.py                     # check Stage 03 (default)
    python scripts/check_timestamps.py --stage 02 04
    python scripts/check_timestamps.py --dir /path/to/dir
    python scripts/check_timestamps.py --stage 03 --verbose # list every offending file

Exit status is 0 if everything is clean, 1 otherwise — safe to use in a shell gate.
"""

import argparse
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from paths import STAGE_02_DIR, STAGE_03_DIR, STAGE_04_DIR

STAGE_DIRS = {"02": STAGE_02_DIR, "03": STAGE_03_DIR, "04": STAGE_04_DIR}


def group_key(root: Path, f: Path) -> str:
    """instrument/subdir label for one file, e.g. 'LANL_aerisultra321/Raw'."""
    rel = f.relative_to(root)
    return "/".join(rel.parts[:-1]) or "."


def scan(root: Path, verbose: bool = False) -> tuple[dict, list]:
    stats = defaultdict(lambda: {"files": 0, "rows": 0, "unsorted": 0,
                                 "backsteps": 0, "worst_s": 0.0, "dup_files": 0,
                                 "dups": 0, "naive": 0, "unreadable": 0,
                                 "src": Counter()})
    offenders = []
    for f in sorted(root.rglob("*.parquet")):
        s = stats[group_key(root, f)]
        s["files"] += 1
        try:
            idx = pd.read_parquet(f, columns=[]).index
        except Exception as e:
            s["unreadable"] += 1
            offenders.append((f, f"unreadable: {e}"))
            continue
        # ts_source is one small string column; cheap even beside multi-GB spectra
        try:
            s["src"].update(pd.read_parquet(f, columns=["ts_source"])["ts_source"])
        except Exception:
            s["src"].update({"(absent)": len(idx)})
        s["rows"] += len(idx)
        if not isinstance(idx, pd.DatetimeIndex):
            offenders.append((f, f"index is {type(idx).__name__}, not DatetimeIndex"))
            continue
        if idx.tz is None:
            s["naive"] += 1
            offenders.append((f, "timestamp index is tz-naive"))
        n_dup = int(idx.duplicated().sum())
        if n_dup:
            s["dup_files"] += 1
            s["dups"] += n_dup
            offenders.append((f, f"{n_dup:,} duplicate timestamps"))
        if len(idx) > 1 and not idx.is_monotonic_increasing:
            d = idx.to_series().diff().dt.total_seconds()
            n_back = int((d < 0).sum())
            worst = float(d.min())
            s["unsorted"] += 1
            s["backsteps"] += n_back
            s["worst_s"] = min(s["worst_s"], worst)
            offenders.append((f, f"{n_back:,} backwards steps, worst {worst:+.3f} s"))
    return stats, offenders


def report(label: str, root: Path, verbose: bool) -> bool:
    print(f"\n{'=' * 78}\n  {label}  —  {root}\n{'=' * 78}")
    if not root.exists():
        print("  directory does not exist — skipped")
        return True
    stats, offenders = scan(root, verbose)
    if not stats:
        print("  no Parquet files found")
        return True

    hdr = f"{'group':<38}{'files':>7}{'rows':>14}{'unsorted':>10}{'backsteps':>11}{'dups':>8}"
    print(hdr)
    print("-" * len(hdr))
    tot = defaultdict(int)
    src_totals = Counter()
    for k, s in sorted(stats.items()):
        print(f"{k:<38}{s['files']:>7}{s['rows']:>14,}{s['unsorted']:>10}"
              f"{s['backsteps']:>11,}{s['dups']:>8,}")
        for key in ("files", "rows", "unsorted", "backsteps", "dup_files", "dups",
                    "naive", "unreadable"):
            tot[key] += s[key]
        src_totals.update(s["src"])
    print("-" * len(hdr))
    print(f"{'TOTAL':<38}{tot['files']:>7}{tot['rows']:>14,}{tot['unsorted']:>10}"
          f"{tot['backsteps']:>11,}{tot['dups']:>8,}")

    if src_totals:
        total_rows = sum(src_totals.values())
        print("\n  ts_source breakdown:")
        for name, count in src_totals.most_common():
            print(f"    {name:<20}{count:>14,}{100 * count / total_rows:>8.1f}%")
        fallback = src_totals.get("median_offset", 0)
        if fallback:
            print(f"    -> {fallback:,} row(s) still carry the ~2 s Aeris sawtooth "
                  f"(no logger coverage)")

    clean = not (tot["unsorted"] or tot["dups"] or tot["naive"] or tot["unreadable"])
    if clean:
        print(f"\n  PASS — all {tot['files']} files sorted, unique, tz-aware UTC")
    else:
        print(f"\n  FAIL — {tot['unsorted']} unsorted, {tot['dup_files']} with duplicate "
              f"timestamps, {tot['naive']} tz-naive, {tot['unreadable']} unreadable")
        shown = offenders if verbose else offenders[:20]
        for f, why in shown:
            print(f"    {f.relative_to(root)}  —  {why}")
        if len(offenders) > len(shown):
            print(f"    ... and {len(offenders) - len(shown)} more (--verbose to list all)")
    return clean


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stage", nargs="+", choices=sorted(STAGE_DIRS), default=["03"],
                    help="pipeline stage(s) to check (default: 03)")
    ap.add_argument("--dir", type=Path, help="check this directory instead of a stage")
    ap.add_argument("--verbose", action="store_true", help="list every offending file")
    args = ap.parse_args()

    targets = ([("custom", args.dir)] if args.dir
               else [(f"Stage {s}", STAGE_DIRS[s]) for s in args.stage])
    ok = all([report(label, root, args.verbose) for label, root in targets])
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
