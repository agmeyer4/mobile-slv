"""
apply_offsets.py

Apply time-shift offsets to already-cleaned CSV files.
Copied from mobilelab/preprocess/apply_offsets.py (Harrison LeTourneau, U of Utah).


Usage:
    python apply_offsets.py <cleaned_dir/> <offsets.json> <output_dir/>
                            [--rejected <rejected.json>]

The offset JSON maps original filenames (*.txt) -> seconds.
Cleaned files are matched by converting _clean.csv -> .txt.
Files in the rejected JSON are skipped entirely.

Harrison LeTourneau, U of Utah, 2026
"""

import sys
import json
from pathlib import Path

import pandas as pd


def apply_offsets(cleaned_dir, offsets, rejected, out_dir):
    files = sorted(cleaned_dir.glob("*_clean.csv"))
    if not files:
        print(f"Error: no *_clean.csv files found in {cleaned_dir}")
        sys.exit(1)

    out_dir.mkdir(parents=True, exist_ok=True)

    n_written = n_skipped = n_missing = 0

    print(f"\n{'═' * 60}")
    print(f"  Input  : {cleaned_dir}")
    print(f"  Output : {out_dir}")
    print(f"  Files  : {len(files)}")
    print(f"{'═' * 60}")

    for f in files:
        key = f.name.replace("_clean.csv", ".txt")

        if key in rejected:
            print(f"  [SKIP]  {f.name}")
            n_skipped += 1
            continue

        if key not in offsets:
            print(f"  [WARN]  {f.name}  — no offset found, using 0s")
            shift_sec = 0.0
            n_missing += 1
        else:
            shift_sec = float(offsets[key])

        df = pd.read_csv(f, index_col="TIMESTAMP", parse_dates=True)

        if shift_sec != 0.0:
            df.index = df.index + pd.to_timedelta(shift_sec, unit="s")

        df.index = pd.to_datetime(df.index).strftime("%Y-%m-%d %H:%M:%S.%f")

        out_path = out_dir / f.name
        df.to_csv(out_path)
        print(f"  [OK]    {f.name}  {shift_sec:+.0f}s")
        n_written += 1

    print(f"\n  Done — written: {n_written}", end="")
    if n_skipped:
        print(f"  skipped (bad): {n_skipped}", end="")
    if n_missing:
        print(f"  no offset (0s): {n_missing}", end="")
    print()


def main():
    if len(sys.argv) < 4:
        print("Usage:")
        print("  python apply_offsets.py <cleaned_dir/> <offsets.json> <output_dir/>")
        print("                          [--rejected <rejected.json>]")
        sys.exit(1)

    args = sys.argv[1:]

    def pop_flag(flag):
        if flag not in args:
            return None
        i = args.index(flag)
        if i + 1 >= len(args):
            print(f"Error: {flag} requires an argument.")
            sys.exit(1)
        val = args[i + 1]
        args[:] = [a for j, a in enumerate(args) if j not in (i, i + 1)]
        return val

    rejected: set[str] = set()
    rejected_arg = pop_flag("--rejected")
    if rejected_arg:
        rpath = Path(rejected_arg)
        if not rpath.exists():
            print(f"Error: rejected file not found — {rpath}")
            sys.exit(1)
        with open(rpath) as fh:
            rejected = set(json.load(fh))
        print(f"Loaded {len(rejected)} rejected file(s) from {rpath.name}")

    cleaned_dir  = Path(args[0])
    offsets_path = Path(args[1])
    out_dir      = Path(args[2])

    if not cleaned_dir.exists():
        print(f"Error: cleaned dir not found — {cleaned_dir}")
        sys.exit(1)
    if not offsets_path.exists():
        print(f"Error: offsets JSON not found — {offsets_path}")
        sys.exit(1)

    with open(offsets_path) as fh:
        offsets = json.load(fh)
    print(f"Loaded offsets for {len(offsets)} file(s) from {offsets_path.name}")

    apply_offsets(cleaned_dir, offsets, rejected, out_dir)


if __name__ == "__main__":
    main()
