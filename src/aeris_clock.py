"""
Aeris instrument clock correction utilities.

RPi and Toughbook logger .dat files contain both:
  Epoch_time  — logger clock epoch (best available UTC reference; not NTP-synced during field deployments)
  Time Stamp  — Aeris instrument's internal clock (may be offset by seconds to hours)

PER-ROW correction (preferred, `build_host_clock_map` + `correct_timestamps`)
---------------------------------------------------------------------------
The logger recorded `Epoch_time` alongside the instrument's own `Time Stamp` for the
same rows, so joining on the timestamp string recovers a host clock for each row
individually. This matters because Ultra321's internal clock does not just sit at a
constant offset — its counter ticks a fixed 1.024 s per sample while the unit actually
samples every ~0.992 s. It gains ~32 ms/sample, and every ~69 samples the firmware
corrects the accumulated drift with a hard jump back of exactly -2.000 s (or -3.000 s).
The result is a ~2 s sawtooth error plus non-monotonic output. A single scalar offset
cannot remove a sawtooth; a per-row host timestamp removes it and is monotonic by
construction. Measured coverage: 97.5% of delivered Ultra321 rows, 99.6% on the worst
file, where it cuts 1,261 backward steps to 5.

Pico017 has the same 1.024 s tick but its counter matches its real sample rate, so it
barely drifts; Ultra460 (WYO, no logger) is not corrected by this stage at all.

SCALAR fallback (`compute_offset`, `find_offset_for_*`, `batch_assign_offsets`)
------------------------------------------------------------------------------
The original approach: correction = median(Epoch_time - epoch(Time Stamp)) per logger
file, applied to whole Aeris files. Still used for rows (and the two files) with no
logger coverage at all, and still what `summarize_logger_files` reports. Rows corrected
this way keep the ~2 s sawtooth and are labelled `ts_source='median_offset'` so the
limitation travels with the data.
"""

import numpy as np
import pandas as pd
from pathlib import Path

AERIS_TS_FORMAT = "%m/%d/%Y %H:%M:%S.%f"


def load_logger_file(path):
    """
    Load an RPi or Toughbook logger .dat file (4-line header + blank + CSV).

    Returns DataFrame with:
      Epoch_time      — logger clock epoch (float seconds; best available UTC reference)
      Time Stamp      — Aeris internal clock string (may be significantly offset)
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


_EPOCH = pd.Timestamp("1970-01-01")

# ts_source values written alongside every corrected timestamp
TS_SOURCE_LOGGER   = "logger_epoch"    # row matched a logger row; host clock used directly
TS_SOURCE_FALLBACK = "median_offset"   # no logger row; instrument clock + scalar offset
TS_SOURCE_BAD      = "unparseable"     # timestamp could not be parsed; left untouched


def build_host_clock_map(logger_dirs):
    """
    Build {instrument Time Stamp string -> host Epoch_time} from logger .dat files.

    Pools every .dat across all given directories (e.g. both the Toughbook and RPi
    directories for one instrument) into a single lookup, because a single Aeris file
    is routinely covered by several logger files — Ultra100321_260202_190739 spans
    three. Deduplicated with keep='first'; duplicates are vanishingly rare (4 in
    780,568 Ultra rows) and arise where logger files overlap.

    Matching is on the raw timestamp *string* rather than a parsed datetime, which is
    exact, avoids float/precision issues, and is unique within a file (verified: 0
    duplicate timestamps campaign-wide).

    Parameters
    ----------
    logger_dirs : iterable of path-like

    Returns
    -------
    dict[str, float]
    """
    parts = []
    for d in logger_dirs:
        d = Path(d)
        if not d.exists():
            continue
        for lf in sorted(d.glob("*.dat")):
            try:
                frame = pd.read_csv(lf, skiprows=5, usecols=[0, 1],
                                    names=["epoch", "ts"], header=0)
            except Exception:
                continue
            frame["ts"] = frame["ts"].astype(str).str.strip()
            frame["epoch"] = pd.to_numeric(frame["epoch"], errors="coerce")
            parts.append(frame)
    if not parts:
        return {}
    pooled = pd.concat(parts, ignore_index=True).dropna(subset=["epoch", "ts"])
    pooled = pooled.drop_duplicates("ts", keep="first")
    return dict(zip(pooled["ts"], pooled["epoch"]))


def correct_timestamps(ts_series, host_map, fallback_offset_s=None):
    """
    Correct a column of Aeris 'MM/DD/YYYY HH:MM:SS.fff' strings to UTC, per row.

    Rows found in host_map take the logger's host timestamp directly, which removes the
    instrument's ~2 s sawtooth and is monotonic by construction. Rows not found fall
    back to instrument-clock + a scalar offset, exactly as this stage always did.

    The fallback offset is the median of (host - instrument) over the rows of THIS file
    that did match, which is a better local estimate than a whole-logger-file median.
    It also self-checks: on Ultra100321_260202_190739 it lands at -84.510 s against
    ts_offsets.json's -84.612 s. Only when a file has zero matches is the caller's
    fallback_offset_s used.

    Deliberately does NOT interpolate the host clock across unmatched runs. Unmatched
    rows arrive in large contiguous blocks (3,350 leading / 1,131 interior / 246
    trailing on one file) that can contain genuine acquisition gaps >5 s, so
    interpolating would invent timestamps across real gaps.

    Parameters
    ----------
    ts_series : pd.Series of str
    host_map : dict[str, float]
    fallback_offset_s : float | None
        Used only when no row in this file matches. If None and nothing matches, the
        timestamps are returned unchanged with ts_source='median_offset'.

    Returns
    -------
    (corrected, ts_source, info) : (pd.Series[str], pd.Series[str], dict)
        info has keys: n, n_matched, match_pct, offset_s, offset_src.
    """
    raw = ts_series.astype(str).str.strip()
    parsed = pd.to_datetime(raw, format=AERIS_TS_FORMAT, errors="coerce")
    inst = (parsed - _EPOCH).dt.total_seconds()
    host = pd.to_numeric(raw.map(host_map), errors="coerce")
    matched = host.notna() & parsed.notna()

    if matched.any():
        offset_s = float(np.median(host[matched] - inst[matched]))
        offset_src = "file_matched_median"
    else:
        offset_s = fallback_offset_s
        offset_src = "caller_fallback" if fallback_offset_s is not None else "none"

    corrected_epoch = pd.Series(np.nan, index=raw.index, dtype=float)
    corrected_epoch[matched] = host[matched].astype(float)
    if offset_s is not None:
        unmatched = ~matched & parsed.notna()
        corrected_epoch[unmatched] = inst[unmatched] + offset_s

    ts_source = pd.Series(TS_SOURCE_FALLBACK, index=raw.index, dtype=object)
    ts_source[matched] = TS_SOURCE_LOGGER
    ts_source[parsed.isna()] = TS_SOURCE_BAD

    out = pd.to_datetime(corrected_epoch, unit="s", errors="coerce")
    ms = (out.dt.microsecond // 1000).astype("Int64").astype(str).str.zfill(3)
    corrected = out.dt.strftime("%m/%d/%Y %H:%M:%S.") + ms
    # rows we could not correct keep their original string unchanged
    corrected[out.isna()] = raw[out.isna()]

    info = {
        "n": int(len(raw)),
        "n_matched": int(matched.sum()),
        "match_pct": round(100.0 * float(matched.mean()), 2) if len(raw) else 0.0,
        "offset_s": round(offset_s, 4) if offset_s is not None else None,
        "offset_src": offset_src,
    }
    return corrected, ts_source, info


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


def apply_host_clock_to_raw(in_path, host_map, out_path, fallback_offset_s=None):
    """
    Correct an Aeris Raw or Eng .txt file (1-line header + CSV) per row.

    Adds a `ts_source` column recording how each row's timestamp was derived, so the
    ~2 s residual uncertainty on fallback rows travels with the data. `src/readers.py`
    preserves unrecognised columns, so it reaches the Stage 02 Parquet with no reader
    change. All other columns are unchanged.

    Returns the `info` dict from `correct_timestamps`.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(in_path)
    df.columns = df.columns.str.strip()
    corrected, ts_source, info = correct_timestamps(
        df["Time Stamp"], host_map, fallback_offset_s)
    df["Time Stamp"] = corrected
    df["ts_source"] = ts_source
    df.to_csv(out_path, index=False)
    return info


def apply_host_clock_to_spectra(in_path, host_map, out_path, fallback_offset_s=None):
    """
    Correct an Aeris Spectra file (no header, timestamp in col 0) per row.

    Deliberately does NOT add a ts_source column: spectra files are headerless and
    positional, and `readers.read_spectra` derives the spectral channel count from the
    total column width, so an extra column would silently mislabel every channel.
    Stage 02 recovers ts_source for spectra by joining on the timestamp against the
    paired Raw file — safe because Raw, Eng and Spectra share an identical timestamp
    sequence (verified 86,731/86,731 exact string match, same order).

    Note: spectra files can be large (hundreds of MB). This reads the full file
    into memory — ensure sufficient RAM before calling on the full batch.

    Returns the `info` dict from `correct_timestamps`.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(in_path, header=None, dtype={0: str}, on_bad_lines="skip")
    corrected, _ts_source, info = correct_timestamps(df[0], host_map, fallback_offset_s)
    df[0] = corrected
    df.to_csv(out_path, index=False, header=False)
    return info


def apply_offset_to_raw(in_path, offset_s, out_path):
    """
    Scalar-offset correction for an Aeris Raw .txt file (1-line header + CSV).

    Superseded by `apply_host_clock_to_raw` for instruments with logger coverage —
    a scalar cannot remove the sawtooth. Kept for files with no coverage at all.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(in_path)
    df.columns = df.columns.str.strip()
    df["Time Stamp"] = _shift_ts_column(df["Time Stamp"], offset_s)
    df["ts_source"] = TS_SOURCE_FALLBACK
    df.to_csv(out_path, index=False)


def apply_offset_to_spectra(in_path, offset_s, out_path):
    """
    Scalar-offset correction for an Aeris Spectra file (no header, timestamp in col 0).

    Superseded by `apply_host_clock_to_spectra`; see `apply_offset_to_raw`.

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
    Duplicate filenames (e.g. files appearing in both RPi and Toughbook directories)
    are deduplicated by filename — first occurrence wins.

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
    Find the timestamp offset for one Aeris Raw file by matching it to the logger
    entry whose corrected UTC window contains this file's first timestamp.

    Tries each coverage entry's offset_s; returns the first whose UTC window
    (expanded by buf_hours on each side) contains the corrected timestamp.

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
    Same as find_offset_for_aeris_file but for headerless Spectra files
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
