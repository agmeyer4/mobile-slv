"""
apply_calibration.py  (SLV 2026)

Apply calibration coefficients from calibration_coefs.json to all merged
daily files, writing new *_cal columns alongside the originals.

Formula:  calibrated = (measured * scale_in - intercept) / slope

Usage:
    python src/apply_calibration.py <out_dir/> [--coefs <path>]

    out_dir   where to write calibrated daily CSVs (created if missing)
    --coefs   path to calibration_coefs.json
              (default: offsets/calibration_coefs.json relative to this file)
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_MERGED = Path(
    "/uufs/chpc.utah.edu/common/home/lin-group24/agm/Mobile_SLV/Data/2026/merged"
)

_DEFAULT_COEFS = Path(__file__).parent.parent / "offsets" / "calibration_coefs.json"


def apply_corrections(df: pd.DataFrame, corrections: list) -> pd.DataFrame:
    for corr in corrections:
        col_in  = corr["col_in"]
        col_out = corr["col_out"]
        if col_in not in df.columns:
            continue
        corr_type = corr.get("type", "linear")
        if corr_type == "linear":
            scale = corr.get("scale_in", 1.0)
            df[col_out] = (df[col_in] * scale - corr["intercept"]) / corr["slope"]
        elif corr_type == "piecewise_linear":
            threshold = corr["threshold_ppm"]
            lo  = corr["low"]
            hi  = corr["high"]
            raw = df[col_in]
            below  = raw < threshold
            result = pd.Series(np.nan, index=df.index, dtype=float)
            result[below]  = (raw[below]  - lo["intercept"]) / lo["slope"]
            result[~below] = (raw[~below] - hi["intercept"]) / hi["slope"]
            df[col_out] = result
    return df


def run(out_dir: Path, coefs_path: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(coefs_path) as f:
        coefs = json.load(f)
    corrections = coefs["corrections"]

    meta = coefs.get("metadata", {})
    print(f"\n{'═' * 60}")
    print(f"  SLV 2026 — apply calibration coefficients")
    print(f"  Calibration date : {meta.get('calibration_date', '?')}")
    print(f"  C2H6 method      : {meta.get('c2h6_method', '?')}")
    print(f"  Formula          : {meta.get('formula', '?')}")
    print(f"  Coefficients     : {coefs_path}")
    print(f"  Input merged     : {_MERGED}")
    print(f"  Output           : {out_dir}")
    print(f"{'═' * 60}\n")

    print("  Corrections to apply:")
    for corr in corrections:
        corr_type = corr.get("type", "linear")
        if corr_type == "linear":
            sc = f"×{corr['scale_in']}" if corr.get("scale_in", 1.0) != 1.0 else ""
            print(f"    {corr['gas']:<6} {corr['instrument']:<12}  "
                  f"{corr['col_in']}{sc} → {corr['col_out']}  "
                  f"(slope={corr['slope']:.5f}, int={corr['intercept']:.4f}, R²={corr['r2']:.4f})")
        elif corr_type == "piecewise_linear":
            lo = corr["low"];  hi = corr["high"];  t = corr["threshold_ppm"]
            print(f"    {corr['gas']:<6} {corr['instrument']:<12}  "
                  f"{corr['col_in']} → {corr['col_out']}  "
                  f"(piecewise <{t} ppm: sl={lo['slope']:.5f}/int={lo['intercept']:.4f}; "
                  f"≥{t} ppm: sl={hi['slope']:.5f}/int={hi['intercept']:.4f})")
    print()

    files = sorted(_MERGED.glob("*.csv"))
    if not files:
        print(f"  ERROR: no CSV files found in {_MERGED}")
        sys.exit(1)

    n_written = 0
    for fpath in files:
        df = pd.read_csv(fpath)
        n_before = len(df.columns)
        df = apply_corrections(df, corrections)
        n_new = len(df.columns) - n_before
        out_path = out_dir / fpath.name
        df.to_csv(out_path, index=False)
        print(f"  {fpath.name}  {len(df):>7,} rows  +{n_new} cal cols  →  {out_path.name}")
        n_written += 1

    print(f"\n  Done — {n_written} files written to {out_dir}\n")


def main():
    args = sys.argv[1:]

    coefs_path = _DEFAULT_COEFS
    if "--coefs" in args:
        i = args.index("--coefs")
        if i + 1 >= len(args):
            print("Error: --coefs requires a path argument")
            sys.exit(1)
        coefs_path = Path(args[i + 1])
        args = [a for j, a in enumerate(args) if j not in (i, i + 1)]

    if len(args) != 1:
        print("Usage: python src/apply_calibration.py <out_dir/> [--coefs <path>]")
        sys.exit(1)

    run(Path(args[0]), coefs_path)


if __name__ == "__main__":
    main()
