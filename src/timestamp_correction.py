"""
Aeris instrument timestamp correction utilities.

RPi and Toughbook logger .dat files contain both:
  Epoch_time  — correct UTC seconds since 1970 (logger's synced clock)
  Time Stamp  — instrument's internal clock (offset from UTC varies by file)

The offset = median(Epoch_time - epoch(Time Stamp)) is computed per logger file
and surveyed across all files before being applied to Aeris Raw and Spectra files.
"""

import numpy as np
import pandas as pd
from pathlib import Path

AERIS_TS_FORMAT = "%m/%d/%Y %H:%M:%S.%f"


def load_logger_file(path):
    """
    Load an RPi or Toughbook logger .dat file (4-line header + blank + CSV).

    Returns DataFrame with:
      Epoch_time      — correct UTC epoch (float seconds)
      Time Stamp      — instrument's wrong clock string
      offset_s        — per-row offset in seconds (Epoch_time - epoch(Time Stamp))
      + all original gas/diagnostic columns
    """
    df = pd.read_csv(path, skiprows=5)
    df.columns = df.columns.str.strip()
    epoch_col = next(c for c in df.columns if "Epoch" in c)
    df = df.rename(columns={epoch_col: "Epoch_time"})
    df["ts_instrument"] = pd.to_datetime(
        df["Time Stamp"].str.strip(), format=AERIS_TS_FORMAT, errors="coerce"
    )
    # Use total_seconds() rather than astype("int64")/1e9 — pandas 2.0+ may store
    # parsed datetimes as datetime64[us], making the int64 divide-by-1e9 give ms not s.
    _epoch = pd.Timestamp("1970-01-01 00:00:00")
    df["ts_instrument_epoch"] = (df["ts_instrument"] - _epoch).dt.total_seconds()
    df["offset_s"] = df["Epoch_time"] - df["ts_instrument_epoch"]
    return df


def compute_offset(df):
    """Return median offset in seconds from a loaded logger DataFrame."""
    return float(np.median(df["offset_s"].dropna()))


def _shift_ts_column(ts_series, offset_s):
    """
    Shift a series of 'MM/DD/YYYY HH:MM:SS.fff' strings by offset_s seconds.
    Returns shifted timestamps as strings in the same format.
    Rows with unparseable timestamps are left unchanged.
    """
    dt = pd.to_datetime(ts_series.str.strip(), format=AERIS_TS_FORMAT, errors="coerce")
    valid = dt.notna()
    shifted = dt[valid] + pd.Timedelta(seconds=offset_s)
    ms = (shifted.dt.microsecond // 1000).astype(str).str.zfill(3)
    result = ts_series.copy()
    result[valid] = shifted.dt.strftime("%m/%d/%Y %H:%M:%S.") + ms
    return result


def apply_offset_to_raw(in_path, offset_s, out_path):
    """
    Apply timestamp offset to an Aeris Raw .txt file (1-line header + CSV).
    Writes corrected file to out_path. All non-timestamp columns are unchanged.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(in_path)
    df.columns = df.columns.str.strip()
    df["Time Stamp"] = _shift_ts_column(df["Time Stamp"], offset_s)
    df.to_csv(out_path, index=False)


def apply_offset_to_spectra(in_path, offset_s, out_path):
    """
    Apply timestamp offset to an Aeris Spectra file (no header, timestamp in col 0).
    Writes corrected file to out_path. All spectral columns are unchanged.

    Note: spectra files can be large (hundreds of MB). This reads the full file
    into memory — ensure sufficient RAM before calling on the full batch.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(in_path, header=None, dtype={0: str}, on_bad_lines="skip")
    df[0] = _shift_ts_column(df[0], offset_s)
    df.to_csv(out_path, index=False, header=False)


def summarize_logger_files(logger_dir, file_glob="*.dat"):
    """
    Load all logger files in a directory, return a summary DataFrame with:
      filename, start_utc, end_utc, n_records, offset_median_s, offset_std_s
    Useful for verifying offset consistency across a campaign.
    """
    rows = []
    for path in sorted(Path(logger_dir).glob(file_glob)):
        try:
            df = load_logger_file(path)
            rows.append({
                "filename": path.name,
                "start_utc": pd.to_datetime(df["Epoch_time"].iloc[0], unit="s", utc=True),
                "end_utc": pd.to_datetime(df["Epoch_time"].iloc[-1], unit="s", utc=True),
                "n_records": len(df),
                "offset_median_s": round(compute_offset(df), 3),
                "offset_std_s": round(float(df["offset_s"].std()), 4),
            })
        except Exception as e:
            rows.append({"filename": path.name, "error": str(e)})
    return pd.DataFrame(rows)


def build_coverage_map(*summary_dfs, min_records=200):
    """
    Build a sorted list of logger coverage entries from one or more summarize_logger_files
    DataFrames (e.g. RPi/Ultra + Toughbook/Ultra pooled together).

    Each entry is a dict: {start_utc, end_utc, offset_s, logger_filename}.
    Files with fewer than min_records or missing offset/start_utc are excluded.
    Duplicate filenames (e.g. Jan 19-23 files appearing in both RPi and Toughbook
    directories) are deduplicated by filename — first occurrence wins.

    Returns a list sorted by start_utc.
    """
    seen = set()
    entries = []
    for df in summary_dfs:
        valid = df.dropna(subset=["offset_median_s", "start_utc", "end_utc"])
        if "n_records" in valid.columns:
            valid = valid[valid["n_records"] >= min_records]
        for _, row in valid.iterrows():
            fn = row["filename"]
            if fn in seen:
                continue
            seen.add(fn)
            entries.append({
                "start_utc":       row["start_utc"],
                "end_utc":         row["end_utc"],
                "offset_s":        row["offset_median_s"],
                "logger_filename": fn,
            })
    return sorted(entries, key=lambda x: x["start_utc"])


def find_offset_for_aeris_file(aeris_path, coverage_map, buf_hours=2):
    """
    Find the correct timestamp offset for one Aeris Raw file by matching it to the
    logger entry whose corrected UTC window contains this file's first timestamp.

    Strategy: read the first timestamp from the Aeris file, try adding each logger's
    offset_s, and return the first logger entry whose UTC window (expanded by buf_hours
    on each side) contains the corrected timestamp.

    Returns (offset_s, logger_filename) on success, or (None, reason_string) on failure.
    """
    try:
        first_row = pd.read_csv(aeris_path, nrows=1)
        first_row.columns = first_row.columns.str.strip()
        aeris_ts = pd.to_datetime(first_row["Time Stamp"].iloc[0].strip(), format=AERIS_TS_FORMAT)
    except Exception as e:
        return None, f"parse error: {e}"

    buf = pd.Timedelta(hours=buf_hours)
    for entry in coverage_map:
        corrected = (aeris_ts + pd.Timedelta(seconds=entry["offset_s"])).tz_localize("UTC")
        if (entry["start_utc"] - buf) <= corrected <= (entry["end_utc"] + buf):
            return entry["offset_s"], entry["logger_filename"]

    return None, "no logger coverage found"


def find_offset_for_spectra_file(spectra_path, coverage_map, buf_hours=2):
    """
    Same as find_offset_for_aeris_file but for header-less Spectra files
    (timestamp is the first column, no header row).
    """
    try:
        first_row = pd.read_csv(spectra_path, nrows=1, header=None, dtype={0: str})
        aeris_ts = pd.to_datetime(first_row[0].iloc[0].strip(), format=AERIS_TS_FORMAT)
    except Exception as e:
        return None, f"parse error: {e}"

    buf = pd.Timedelta(hours=buf_hours)
    for entry in coverage_map:
        corrected = (aeris_ts + pd.Timedelta(seconds=entry["offset_s"])).tz_localize("UTC")
        if (entry["start_utc"] - buf) <= corrected <= (entry["end_utc"] + buf):
            return entry["offset_s"], entry["logger_filename"]

    return None, "no logger coverage found"


def batch_assign_offsets(file_list, coverage_map, file_type="raw", buf_hours=2):
    """
    Assign an offset to every file in file_list using the coverage_map.
    file_type: 'raw' (has header) or 'spectra' (no header, timestamp in col 0).

    Returns a DataFrame with columns:
      filename, offset_s, logger_filename, status
    where status is 'ok' or an error/no-match reason.
    """
    finder = find_offset_for_aeris_file if file_type == "raw" else find_offset_for_spectra_file
    rows = []
    for path in file_list:
        offset_s, info = finder(path, coverage_map, buf_hours=buf_hours)
        rows.append({
            "filename":        Path(path).name,
            "offset_s":        offset_s,
            "logger_filename": info if offset_s is not None else None,
            "status":          "ok" if offset_s is not None else info,
        })
    return pd.DataFrame(rows)
