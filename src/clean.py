"""
clean.py

Interactive CSV cleaner for mobile lab instruments.
Copied from mobilelab/preprocess/clean.py (Harrison LeTourneau, U of Utah).


Single file:
    python clean.py <input_file.dat>

    Prompts for header skip, columns, timestamp format, and time shift.

Batch directory — two modes:

  Default (interactive):
    python clean.py <directory/>

    Configures header skip, columns, and timestamp format once using the
    first file, then processes all files, prompting for a per-file time
    shift at each step.

  JSON offset mode (-o):
    python clean.py <directory/> -o offsets.json [-r rejected.json]

    Same cleaning steps, but time shifts come from the JSON produced by
    sync_offsets.ipynb instead of manual prompts. Files absent from the
    JSON get a 0s shift. Files listed in the rejected JSON are skipped.

    offsets.json  — dict mapping original filename -> seconds  e.g.
                    {"Instrument_260203_172639.txt": 4.0, ...}
    rejected.json — list of filenames to skip entirely e.g.
                    ["Instrument_260205_181624.txt", ...]

Output: cleaned CSV(s) with a single TIMESTAMP index and chosen columns,
        formatted as YYYY-MM-DD HH:MM:SS.ffffff.

"""

import sys
import json
from pathlib import Path

import pandas as pd


# ── Helpers ───────────────────────────────────────────────────────────────────

def prompt(msg: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default is not None else ""
    while True:
        val = input(f"{msg}{suffix}: ").strip()
        if val == "" and default is not None:
            return default
        if val != "":
            return val
        print("  Please enter a value.")


def prompt_int(msg: str, default: int | None = None) -> int:
    while True:
        raw = prompt(msg, str(default) if default is not None else None)
        try:
            return int(raw)
        except ValueError:
            print(f"  '{raw}' is not a valid integer. Try again.")


def prompt_choice(msg: str, choices: list[str], default: str | None = None) -> str:
    choices_lower = [c.lower() for c in choices]
    while True:
        raw = prompt(msg, default).lower()
        if raw in choices_lower:
            return raw
        print(f"  Invalid choice. Options: {choices}")


def section(title: str):
    width = 60
    print("\n" + "─" * width)
    print(f"  {title}")
    print("─" * width)


# ── Step 1: Read with user-specified header skip ──────────────────────────────

def read_with_skip(filepath: Path, skiprows: int) -> pd.DataFrame:
    common = dict(engine="python", skiprows=skiprows, na_values=["nan", "NaN", "NA", ", "])
    df = pd.read_csv(filepath, sep=",", **common)
    if len(df.columns) == 1:
        df = pd.read_csv(filepath, sep=r"\s+", **common)
    df.columns = [c.strip() for c in df.columns]
    return df


# ── Step 2: Column selection ──────────────────────────────────────────────────

def select_columns(df: pd.DataFrame) -> dict[str, str]:
    """
    Returns an ordered dict of  { original_col_name: output_col_name }.
    The output name is either the original or a user-supplied rename.
    """
    ts_keywords = {"date", "time", "epoch", "julian", "frac", "timestamp"}
    data_cols = [
        c for c in df.columns
        if not any(kw in c.lower() for kw in ts_keywords)
    ]

    print("\n  Available data columns:\n")
    for i, col in enumerate(data_cols, 1):
        sample = df[col].dropna().iloc[0] if df[col].dropna().shape[0] > 0 else "N/A"
        print(f"    [{i:>2}]  {col:<35}  (e.g. {sample})")

    print()
    print("  Enter column numbers separated by spaces to keep them.")
    print("  To rename a column, follow its number with a colon and the new name.")
    print("  Examples:  1 2 3          (keep as-is)")
    print("             1 2:CO2_ppm 3  (rename column 2)")
    print("  Press Enter to keep ALL columns with original names.")

    while True:
        raw = input("\n  Your selection: ").strip()

        # Keep all with original names
        if raw == "":
            return {c: c for c in data_cols}

        result: dict[str, str] = {}
        valid = True

        for token in raw.split():
            if ":" in token:
                num_part, new_name = token.split(":", 1)
                new_name = new_name.strip()
            else:
                num_part = token
                new_name = None

            try:
                idx = int(num_part)
            except ValueError:
                print(f"  '{num_part}' is not a valid column number. Try again.")
                valid = False
                break

            if not (1 <= idx <= len(data_cols)):
                print(f"  Column number {idx} is out of range (1–{len(data_cols)}). Try again.")
                valid = False
                break

            orig = data_cols[idx - 1]
            result[orig] = new_name if new_name else orig

        if not valid:
            continue

        print("\n  Columns to keep:")
        for orig, out in result.items():
            if orig != out:
                print(f"    {orig}  →  {out}")
            else:
                print(f"    {orig}")
        return result


# ── Step 3: Timestamp building ────────────────────────────────────────────────

# Output format is always this — no exceptions.
TIMESTAMP_OUT_FMT = "%Y-%m-%d %H:%M:%S.%f"


class TimestampConfig:
    """Captures the user's timestamp choices so they can be replayed on other files."""
    def __init__(self, method: str, **kwargs):
        self.method = method   # "epoch" | "split" | "single"
        self.params = kwargs   # col, date_col, time_col, fmt, etc.

    def apply(self, df: pd.DataFrame) -> "pd.Series":
        if self.method == "epoch":
            col = self.params["col"]
            return pd.to_datetime(
                df[col].astype(float), unit="s", utc=True
            ).dt.tz_localize(None)

        elif self.method == "split":
            date_col = self.params["date_col"]
            time_col = self.params["time_col"]
            fmt      = self.params["fmt"]
            combined = (
                df[date_col].astype(str).str.strip()
                + " "
                + df[time_col].astype(str).str.strip()
            )
            return pd.to_datetime(combined, format=fmt, errors="coerce")

        elif self.method == "single":
            col = self.params["col"]
            fmt = self.params.get("fmt", None)
            return pd.to_datetime(
                df[col].astype(str).str.strip(), format=fmt, errors="coerce"
            )

        else:
            raise ValueError(f"Unknown timestamp method: {self.method}")


def build_timestamp(df: pd.DataFrame, label: str = "STEP 3 of 3 — Timestamp / Index") -> tuple["pd.Series", TimestampConfig]:
    """
    Interactively configure the timestamp.
    Returns (parsed Series, TimestampConfig) so the config can be
    replayed on other files without prompting again.
    """
    section(label)

    ts_keywords = ["date", "time", "epoch", "julian", "frac", "timestamp"]
    ts_cols = [
        c for c in df.columns
        if any(kw in c.lower() for kw in ts_keywords)
    ]
    print("\n  Detected timestamp-related columns:")
    for c in ts_cols:
        sample = df[c].iloc[0] if len(df) > 0 else "N/A"
        print(f"    {c:<35}  (e.g. {sample})")

    print()
    print("  How is the timestamp stored in this file?")
    print("    [1]  epoch    — one column of Unix seconds  (e.g. 1738620000.123)")
    print("    [2]  split    — separate DATE and TIME columns")
    print("    [3]  single   — one column that already contains the full datetime")
    method = prompt_choice("\n  Choice", ["1", "2", "3", "epoch", "split", "single"])

    if method in ("1", "epoch"):
        ts, cfg = _build_epoch(df, ts_cols)
    elif method in ("2", "split"):
        ts, cfg = _build_split(df, ts_cols)
    else:
        ts, cfg = _build_single(df, ts_cols)

    ts = pd.to_datetime(ts, errors="coerce")
    if hasattr(ts.dt, "tz") and ts.dt.tz is not None:
        ts = ts.dt.tz_localize(None)

    n_bad = ts.isna().sum()
    if n_bad:
        print(f"\n  [warn] {n_bad} rows could not be parsed and will be dropped.")

    good = ts.dropna()
    if len(good):
        print(f"\n  Preview — first parsed timestamp: {good.iloc[0]}")
        print(f"  Output format will be:  YYYY-MM-DD HH:MM:SS.ffffff")

    return ts, cfg


def _build_epoch(df: pd.DataFrame, ts_cols: list[str]) -> tuple["pd.Series", TimestampConfig]:
    epoch_candidates = [c for c in ts_cols if "epoch" in c.lower()]
    default_col = epoch_candidates[0] if epoch_candidates else (ts_cols[0] if ts_cols else None)

    col = prompt("\n  Epoch column name", default_col)
    if col not in df.columns:
        raise KeyError(f"Column '{col}' not found in file.")

    print(f"  Parsing '{col}' as Unix seconds (UTC) → tz-naive datetime.")
    cfg = TimestampConfig("epoch", col=col)
    return cfg.apply(df), cfg


def _build_split(df: pd.DataFrame, ts_cols: list[str]) -> tuple["pd.Series", TimestampConfig]:
    date_candidates = [c for c in ts_cols if "date" in c.lower()]
    time_candidates = [c for c in ts_cols if "time" in c.lower() and "epoch" not in c.lower()]

    date_col = prompt("\n  Date column name", date_candidates[0] if date_candidates else None)
    time_col = prompt("  Time column name", time_candidates[0] if time_candidates else None)

    for col in (date_col, time_col):
        if col not in df.columns:
            raise KeyError(f"Column '{col}' not found in file.")

    print(f"\n  Sample DATE values : {df[date_col].iloc[:3].tolist()}")
    print(f"  Sample TIME values : {df[time_col].iloc[:3].tolist()}")
    print()
    print("  Enter the strptime format for the combined DATE + TIME string.")
    print("  (They will be joined with a single space before parsing.)")
    print("  Examples:")
    print("    %Y-%m-%d %H:%M:%S       →  2026-02-10 21:00:02")
    print("    %Y-%m-%d %H:%M:%S.%f    →  2026-02-10 21:00:02.123456")
    print("    %m/%d/%Y %I:%M:%S %p    →  02/10/2026 09:00:02 PM")

    fmt = prompt("  Format string", "%Y-%m-%d %H:%M:%S")
    cfg = TimestampConfig("split", date_col=date_col, time_col=time_col, fmt=fmt)
    return cfg.apply(df), cfg


def _build_single(df: pd.DataFrame, ts_cols: list[str]) -> tuple["pd.Series", TimestampConfig]:
    dt_candidates = [
        c for c in ts_cols
        if any(kw in c.lower() for kw in ("datetime", "timestamp"))
    ]
    default_col = dt_candidates[0] if dt_candidates else (ts_cols[0] if ts_cols else None)

    col = prompt("\n  Datetime column name", default_col)
    if col not in df.columns:
        raise KeyError(f"Column '{col}' not found in file.")

    print(f"\n  Sample values: {df[col].iloc[:3].tolist()}")
    print()
    print("  Enter the strptime format, or press Enter to let pandas infer it.")
    print("  Examples:")
    print("    %Y-%m-%d %H:%M:%S.%f    →  2026-02-10 21:00:02.123456")
    print("    %Y/%m/%d %H:%M          →  2026/02/10 21:00")
    print("    %d-%b-%Y %H:%M:%S       →  10-Feb-2026 21:00:02")

    fmt_raw = input("  Format string [infer]: ").strip()
    fmt = fmt_raw if fmt_raw else None
    cfg = TimestampConfig("single", col=col, fmt=fmt)
    return cfg.apply(df), cfg


# ── Shared assembly ───────────────────────────────────────────────────────────

def prompt_timeshift() -> float:
    """Ask the user for a time shift in seconds. Returns 0.0 if skipped."""
    print("\n  Shift every timestamp by a fixed number of seconds.")
    print("  Positive = shift forward, negative = shift backward.")
    print("  Press Enter to skip (no shift).")
    raw = input("  Seconds to shift [0]: ").strip()
    if raw == "":
        return 0.0
    try:
        return float(raw)
    except ValueError:
        print(f"  Could not parse '{raw}' as a number — no shift applied.")
        return 0.0


def assemble(
    df: pd.DataFrame,
    col_map: dict[str, str],
    ts: "pd.Series",
    shift_sec: float,
) -> pd.DataFrame:
    """
    Build the final clean DataFrame from the raw df, column map,
    parsed timestamp series, and optional time shift.
    """
    df_clean = df[list(col_map.keys())].copy()
    df_clean.rename(columns=col_map, inplace=True)

    for col in df_clean.columns:
        df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")

    df_clean.insert(0, "TIMESTAMP", ts.values)
    df_clean.set_index("TIMESTAMP", inplace=True)
    df_clean.index.name = "TIMESTAMP"

    df_clean = df_clean[~df_clean.index.isna()]
    df_clean = df_clean[~df_clean.index.duplicated(keep="first")]
    df_clean.sort_index(inplace=True)

    if shift_sec != 0.0:
        df_clean.index = (
            pd.to_datetime(df_clean.index)
            + pd.to_timedelta(shift_sec, unit="s")
        )
        print(f"  Shifted by {shift_sec:+g} seconds.")

    df_clean.index = pd.to_datetime(df_clean.index).strftime("%Y-%m-%d %H:%M:%S.%f")
    return df_clean


def save(df_clean: pd.DataFrame, default_out: str) -> None:
    out_path = Path(prompt("  Output file path", default_out))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(out_path)
    print(f"  ✓  Saved → {out_path}")


# ── Single-file mode ──────────────────────────────────────────────────────────

def run_single(filepath: Path) -> None:
    print(f"\n{'═' * 60}")
    print(f"  Picarro Cleaner — Single File")
    print(f"  File: {filepath.name}")
    print(f"{'═' * 60}")

    # Step 1: Header skip
    section("STEP 1 of 4 — Header Lines")
    print("\n  First 10 lines of the file:\n")
    _peek(filepath)

    print("\n  How many lines should be skipped before the column-name row?")
    print("  (e.g. if row 0 is already the header, enter 0)")
    skiprows = prompt_int("  Lines to skip", default=0)

    try:
        df = read_with_skip(filepath, skiprows)
    except Exception as e:
        print(f"\n  Error reading file: {e}")
        sys.exit(1)

    print(f"\n  Read {len(df):,} rows, {len(df.columns)} columns.")
    print(f"  Column names detected: {df.columns.tolist()}")

    # Step 2: Column selection
    section("STEP 2 of 4 — Data Columns to Keep")
    col_map = select_columns(df)

    # Step 3: Timestamp
    ts, cfg = build_timestamp(df)

    # Step 4: Time shift
    section("STEP 4 of 4 — Time Shift")
    shift_sec = prompt_timeshift()

    # Assemble
    df_clean = assemble(df, col_map, ts, shift_sec)

    print(f"\n  ✓  {len(df_clean):,} rows × {len(df_clean.columns)} columns")
    print(f"     Time range: {df_clean.index[0]}  →  {df_clean.index[-1]}")

    # Output
    section("OUTPUT")
    save(df_clean, filepath.stem + "_clean.csv")
    print()


# ── Batch directory mode ──────────────────────────────────────────────────────

def run_batch(
    dirpath: Path,
    offsets: dict[str, float] | None = None,
    rejected: set[str] | None = None,
) -> None:
    # offsets=None  → interactive mode (prompt per file)
    # offsets={}    → JSON mode but empty JSON; files get 0s shift silently
    rejected = rejected or set()

    # Collect all files in the directory (non-recursive), sorted
    candidates = sorted(
        p for p in dirpath.iterdir()
        if p.is_file() and not p.name.startswith(".")
    )
    if not candidates:
        print(f"Error: no files found in {dirpath}")
        sys.exit(1)

    to_process = [p for p in candidates if p.name not in rejected]
    skipped    = [p for p in candidates if p.name in rejected]

    print(f"\n{'═' * 60}")
    print(f"  Cleaner — Batch Mode")
    print(f"  Directory : {dirpath}")
    print(f"  Files found  : {len(candidates)}")
    print(f"  To process   : {len(to_process)}")
    if skipped:
        print(f"  Rejected (bad): {len(skipped)}")
        for p in skipped:
            print(f"    [skip] {p.name}")
    print(f"{'═' * 60}")

    if not to_process:
        print("  Nothing to process after applying rejected list.")
        return

    # ── Configure once using the first non-rejected file ──────────────────────
    first = to_process[0]
    print(f"\n  Using '{first.name}' to configure shared settings.\n")

    section("STEP 1 of 3 — Header Lines  (applies to all files)")
    print("\n  First 10 lines of the file:\n")
    _peek(first)

    print("\n  How many lines should be skipped before the column-name row?")
    print("  (e.g. if row 0 is already the header, enter 0)")
    skiprows = prompt_int("  Lines to skip", default=0)

    try:
        df_first = read_with_skip(first, skiprows)
    except Exception as e:
        print(f"\n  Error reading file: {e}")
        sys.exit(1)

    print(f"\n  Read {len(df_first):,} rows, {len(df_first.columns)} columns.")
    print(f"  Column names detected: {df_first.columns.tolist()}")

    section("STEP 2 of 3 — Data Columns to Keep  (applies to all files)")
    col_map = select_columns(df_first)

    ts_first, ts_cfg = build_timestamp(df_first, label="STEP 3 of 3 — Timestamp Format  (applies to all files)")

    # Ask where to put the output files and what suffix to use
    print()
    default_out_dir = str(dirpath / "cleaned")
    out_dir = Path(prompt("\n  Output directory for all cleaned files", default_out_dir))
    out_dir.mkdir(parents=True, exist_ok=True)

    print("\n  Output filename suffix appended to each file stem.")
    print("  Examples:  _clean  →  myfile_clean.csv")
    print("             _sync   →  myfile_sync.csv")
    print("             (empty) →  myfile.csv")
    out_suffix = prompt("  Suffix", "_clean")

    # ── Per-file loop ─────────────────────────────────────────────────────────
    print(f"\n{'═' * 60}")
    print(f"  Processing {len(to_process)} file(s)  →  {out_dir}")
    print(f"{'═' * 60}")

    n_written = 0
    zero_all = False
    for i, filepath in enumerate(to_process, 1):
        print(f"\n  [{i}/{len(to_process)}]  {filepath.name}")

        try:
            df = read_with_skip(filepath, skiprows)
        except Exception as e:
            print(f"    [skip] Could not read file: {e}")
            continue

        try:
            ts = ts_cfg.apply(df)
            ts = pd.to_datetime(ts, errors="coerce")
            if hasattr(ts.dt, "tz") and ts.dt.tz is not None:
                ts = ts.dt.tz_localize(None)
        except Exception as e:
            print(f"    [skip] Timestamp error: {e}")
            continue

        # Use offsets JSON value if -o was given, otherwise prompt
        if offsets is not None:
            shift_sec = float(offsets.get(filepath.name, 0.0))
            print(f"    Time shift: {shift_sec:+g}s (from JSON)")
        elif zero_all:
            shift_sec = 0.0
            print(f"    Time shift: 0s (all remaining)")
        else:
            print(f"    Time shift in seconds [0]  (or 'a' to apply 0 to all remaining): ", end="", flush=True)
            raw = input().strip()
            if raw.lower() == "a":
                zero_all = True
                shift_sec = 0.0
                print(f"    Applying 0s to all remaining files.")
            else:
                try:
                    shift_sec = float(raw) if raw else 0.0
                except ValueError:
                    print(f"    Could not parse '{raw}' — no shift applied.")
                    shift_sec = 0.0

        try:
            df_clean = assemble(df, col_map, ts, shift_sec)
        except Exception as e:
            print(f"    [skip] Assembly error: {e}")
            continue

        out_path = out_dir / (filepath.stem + out_suffix + ".csv")
        df_clean.to_csv(out_path)
        print(f"    ✓  {len(df_clean):,} rows  →  {out_path.name}")
        n_written += 1

    print(f"\n  ✓  Batch complete. {n_written} file(s) written to {out_dir}")
    if skipped:
        print(f"  ✗  {len(skipped)} file(s) skipped (rejected):")
        for p in skipped:
            print(f"       {p.name}")
    print()



# ── Peek helper ───────────────────────────────────────────────────────────────

def _peek(filepath: Path) -> None:
    max_content = 72
    with open(filepath) as f:
        for i, line in enumerate(f):
            if i >= 10:
                break
            content = line.rstrip("\n")
            if len(content) > max_content:
                content = content[:max_content] + " ..."
            print(f"  {i:>3}  {content}")
    print()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python clean.py <file>               — single file, interactive")
        print("  python clean.py <dir/>               — batch, prompt for shift per file")
        print("  python clean.py <dir/> -o shifts.json [-r bad.json]")
        print("                                       — batch, shifts from JSON (no prompts)")
        sys.exit(1)

    args = sys.argv[1:]

    def _pop_flag(*flags):
        """Remove the first matching flag+value pair from args and return the value."""
        for flag in flags:
            if flag not in args:
                continue
            i = args.index(flag)
            if i + 1 >= len(args):
                print(f"Error: {flag} requires an argument.")
                sys.exit(1)
            val = args[i + 1]
            args[:] = [a for j, a in enumerate(args) if j not in (i, i + 1)]
            return val
        return None

    # -o / --offsets
    offsets: dict[str, float] | None = None
    offsets_arg = _pop_flag("-o")
    if offsets_arg:
        offsets_path = Path(offsets_arg)
        if not offsets_path.exists():
            print(f"Error: offsets file not found — {offsets_path}")
            sys.exit(1)
        with open(offsets_path) as fh:
            offsets = json.load(fh)
        print(f"Loaded offsets for {len(offsets)} file(s) from {offsets_path.name}")

    # -r / --rejected
    rejected: set[str] = set()
    rejected_arg = _pop_flag("-r")
    if rejected_arg:
        rejected_path = Path(rejected_arg)
        if not rejected_path.exists():
            print(f"Error: rejected file not found — {rejected_path}")
            sys.exit(1)
        with open(rejected_path) as fh:
            rejected = set(json.load(fh))
        print(f"Loaded {len(rejected)} rejected file(s) from {rejected_path.name}")

    if not args:
        print("Error: no target file or directory specified.")
        sys.exit(1)

    target = Path(args[0])
    if not target.exists():
        print(f"Error: path not found — {target}")
        sys.exit(1)

    if target.is_dir():
        run_batch(target, offsets=offsets, rejected=rejected)
    else:
        run_single(target)


if __name__ == "__main__":
    main()
