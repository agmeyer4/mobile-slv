"""
clean_anem.py

Parse raw Trisonica anemometer logger files (RPi / Toughbook) into a
standard TIMESTAMP-indexed CSV.

Each data line has the form:
    epoch_seconds,S  00.10 D  093 U -00.09 V  00.00 W  00.02 T  24.27 ...

The second field is a labeled-value string where each label precedes its
numeric value. Labels and their output column names:

    S  → Speed            D  → Dir
    U  → u                V  → v
    W  → w                T  → temp_c
    H  → hum_pct          DP → dew_point_c
    P  → pressure_hPa     AD → AD
    PI → pitch_deg        RO → roll_deg
    MD → mag_dir_deg      TD → true_dir_deg

Usage:
    python clean_anem.py <anem_dir/> <output_dir/>
"""

import re
import sys
from pathlib import Path

import pandas as pd

SKIPROWS = 6   # 4 header lines + blank + column-name line (not usable as CSV header)

_LABEL_MAP = {
    "S":  "Speed",
    "D":  "Dir",
    "U":  "u",
    "V":  "v",
    "W":  "w",
    "T":  "temp_c",
    "H":  "hum_pct",
    "DP": "dew_point_c",
    "P":  "pressure_hPa",
    "AD": "AD",
    "PI": "pitch_deg",
    "RO": "roll_deg",
    "MD": "mag_dir_deg",
    "TD": "true_dir_deg",
}

_LABEL_PATTERN = re.compile(r"([A-Z]+)\s+([-\d.]+)")


def _parse_labeled_string(s: str) -> dict:
    return {
        _LABEL_MAP[label]: float(value)
        for label, value in _LABEL_PATTERN.findall(s)
        if label in _LABEL_MAP
    }


def parse_anem_file(filepath: Path) -> pd.DataFrame:
    rows = []

    with open(filepath) as fh:
        for _ in range(SKIPROWS):
            fh.readline()
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = line.split(",", 1)
            if len(parts) < 2:
                continue
            try:
                epoch = float(parts[0])
            except ValueError:
                continue

            values = _parse_labeled_string(parts[1])
            if not values:
                continue

            values["epoch"] = epoch
            rows.append(values)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df.insert(0, "TIMESTAMP",
              pd.to_datetime(df["epoch"], unit="s", utc=True)
              .dt.tz_localize(None)
              .dt.strftime("%Y-%m-%d %H:%M:%S.%f"))
    df = df.drop(columns=["epoch"]).set_index("TIMESTAMP")
    return df


def main():
    if len(sys.argv) != 3:
        print("Usage: python clean_anem.py <anem_dir/> <output_dir/>")
        sys.exit(1)

    in_dir  = Path(sys.argv[1])
    out_dir = Path(sys.argv[2])

    if not in_dir.exists():
        print(f"Error: {in_dir} not found.")
        sys.exit(1)

    files = sorted(p for p in in_dir.iterdir()
                   if p.is_file() and not p.name.startswith("."))
    if not files:
        print(f"Error: no files found in {in_dir}")
        sys.exit(1)

    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'═'*60}")
    print(f"  Anemometer Cleaner")
    print(f"  Input  : {in_dir}")
    print(f"  Output : {out_dir}")
    print(f"  Files  : {len(files)}")
    print(f"{'═'*60}\n")

    n_written = 0
    for i, fp in enumerate(files, 1):
        try:
            df = parse_anem_file(fp)
            if df.empty:
                print(f"  [{i}/{len(files)}]  {fp.name}  →  [empty, skipped]")
                continue
            out_path = out_dir / (fp.stem + "_clean.csv")
            df.to_csv(out_path)
            print(f"  [{i}/{len(files)}]  {fp.name}  →  {len(df):,} rows  ✓")
            n_written += 1
        except Exception as e:
            print(f"  [{i}/{len(files)}]  {fp.name}  →  [error] {e}")

    print(f"\n  Done — {n_written}/{len(files)} file(s) written to {out_dir}\n")


if __name__ == "__main__":
    main()
