"""
clean_sprinter.py

Clean WYO Sprinter CSV files into the standard TIMESTAMP-indexed CSV
format produced by mobilelab/preprocess/clean.py.

The Sprinter uses four UTC timestamp columns instead of a single datetime:
  UTC Year, UTC Month, UTC Day, UTC hhmmss (packed as HHMMSS.ss)

e.g.  UTC Year=2026  UTC Month=02  UTC Day=03  UTC hhmmss=163446.70
  →   2026-02-03 16:34:46.700000

Usage:
    python clean_sprinter.py <sprinter_dir/> <output_dir/>

Columns are configured interactively once (using the first file), then
the same selection is applied to all files. Output: one *_clean.csv per
input file with a TIMESTAMP index (UTC, tz-naive).
"""

import sys
from pathlib import Path

import pandas as pd

SKIPROWS = 3
OUT_FMT  = "%Y-%m-%d %H:%M:%S.%f"

# Column names that belong to the timestamp — excluded from data selection
_TS_COLS = {"pc", "utc hhmmss", "utc year", "utc month", "utc day"}


# ── Timestamp parsing ─────────────────────────────────────────────────────────

def _parse_timestamp(df: pd.DataFrame) -> pd.Series:
    """Build a UTC datetime Series from the four Sprinter timestamp columns."""
    hhmmss = pd.to_numeric(df["UTC hhmmss"], errors="coerce")
    year   = pd.to_numeric(df["UTC Year"],   errors="coerce")
    month  = pd.to_numeric(df["UTC Month"],  errors="coerce")
    day    = pd.to_numeric(df["UTC Day"],    errors="coerce")

    valid = year.notna() & month.notna() & day.notna() & hhmmss.notna()

    out = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    if not valid.any():
        return out

    h    = hhmmss[valid]
    hh   = (h // 10000).astype(int)
    mm   = ((h % 10000) // 100).astype(int)
    ss_f = h % 100
    ss   = ss_f.astype(int)
    us   = ((ss_f - ss) * 1_000_000).round().astype(int)

    out[valid] = pd.to_datetime(
        dict(
            year=year[valid].astype(int),
            month=month[valid].astype(int),
            day=day[valid].astype(int),
            hour=hh, minute=mm, second=ss, microsecond=us,
        ),
        errors="coerce",
    )
    return out


# ── Column selection (mirrors clean.py style) ─────────────────────────────────

def _data_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c.lower() not in _TS_COLS]


def _select_columns(df: pd.DataFrame) -> dict[str, str]:
    cols = _data_cols(df)

    print("\n  Available data columns:\n")
    for i, col in enumerate(cols, 1):
        sample = df[col].dropna().iloc[0] if df[col].dropna().shape[0] > 0 else "N/A"
        print(f"    [{i:>2}]  {col:<42}  (e.g. {sample})")

    print()
    print("  Enter column numbers to keep, or press Enter to keep ALL.")
    print("  To rename: follow number with :new_name  (e.g. 6:Latitude)")

    while True:
        raw = input("\n  Your selection: ").strip()

        if raw == "":
            return {c: c for c in cols}

        result: dict[str, str] = {}
        valid = True
        for token in raw.split():
            if ":" in token:
                num_part, new_name = token.split(":", 1)
                new_name = new_name.strip()
            else:
                num_part, new_name = token, None
            try:
                idx = int(num_part)
            except ValueError:
                print(f"  '{num_part}' is not a valid number. Try again.")
                valid = False
                break
            if not 1 <= idx <= len(cols):
                print(f"  {idx} out of range (1–{len(cols)}). Try again.")
                valid = False
                break
            orig = cols[idx - 1]
            result[orig] = new_name if new_name else orig

        if valid:
            print("\n  Columns to keep:")
            for orig, out in result.items():
                print(f"    {orig}" + (f"  →  {out}" if orig != out else ""))
            return result


# ── File reader (handles trailing extra field) ────────────────────────────────

def _read_sprinter(filepath: Path, **kwargs) -> pd.DataFrame:
    """
    Sprinter CSVs have one more field per data row than the header declares.
    Read header manually, then read data with header=None and trim to match.
    """
    with open(filepath) as fh:
        for _ in range(SKIPROWS):
            fh.readline()
        col_names = [c.strip() for c in fh.readline().strip().split(",")]

    df = pd.read_csv(
        filepath,
        skiprows=SKIPROWS + 1,
        header=None,
        engine="python",
        on_bad_lines="skip",
        na_values=["nan", "NaN", "NA"],
        **kwargs,
    )
    if df.empty:
        return pd.DataFrame(columns=col_names)
    df = df.iloc[:, : len(col_names)].copy()
    df.columns = col_names
    return df


# ── Per-file cleaning ─────────────────────────────────────────────────────────

def _clean_file(
    filepath: Path,
    col_map: dict[str, str],
    out_dir: Path,
) -> int:
    df = _read_sprinter(filepath)

    ts = _parse_timestamp(df)

    out = df[list(col_map.keys())].copy()
    out.rename(columns=col_map, inplace=True)
    for col in out.columns:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out.insert(0, "TIMESTAMP", ts.values)
    out.set_index("TIMESTAMP", inplace=True)
    out = out[~out.index.isna()]
    out = out[~out.index.duplicated(keep="first")]
    out.sort_index(inplace=True)
    out.index = pd.to_datetime(out.index).strftime(OUT_FMT)

    out_path = out_dir / (filepath.stem + "_clean.csv")
    out.to_csv(out_path)
    return len(out)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) != 3:
        print("Usage: python clean_sprinter.py <sprinter_dir/> <output_dir/>")
        sys.exit(1)

    in_dir  = Path(sys.argv[1])
    out_dir = Path(sys.argv[2])

    if not in_dir.exists():
        print(f"Error: {in_dir} not found.")
        sys.exit(1)

    files = sorted(p for p in in_dir.iterdir() if p.is_file() and not p.name.startswith("."))
    if not files:
        print(f"Error: no files found in {in_dir}")
        sys.exit(1)

    print(f"\n{'═'*60}")
    print(f"  Sprinter Cleaner")
    print(f"  Input  : {in_dir}")
    print(f"  Output : {out_dir}")
    print(f"  Files  : {len(files)}")
    print(f"{'═'*60}")

    # Configure column selection once using the first file
    first = files[0]
    print(f"\n  Configuring from '{first.name}'...\n")
    df0 = _read_sprinter(first, nrows=5)
    col_map = _select_columns(df0)

    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'═'*60}")
    print(f"  Processing {len(files)} file(s)  →  {out_dir}")
    print(f"{'═'*60}\n")

    n_written = 0
    for i, fp in enumerate(files, 1):
        try:
            n_rows = _clean_file(fp, col_map, out_dir)
            print(f"  [{i}/{len(files)}]  {fp.name}  →  {n_rows:,} rows  ✓")
            n_written += 1
        except Exception as e:
            print(f"  [{i}/{len(files)}]  {fp.name}  →  [error] {e}")

    print(f"\n  Done — {n_written}/{len(files)} file(s) written to {out_dir}\n")


if __name__ == "__main__":
    main()
