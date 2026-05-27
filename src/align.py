"""
Stage 03 alignment utilities.

Pure functions with no notebook or session-state dependencies.
Imported by pipeline/03a_align_wyo.ipynb and 03b_align_mml.ipynb; reusable by Stage 04.

Public API
----------
resample_series(s, freq_s=1)
    Resample a time-indexed Series to a regular grid and interpolate short gaps.

cross_correlate(ref, sig, max_lag_s=600, freq_s=1)
    Return the lag in seconds that best aligns sig to ref via full cross-correlation.
    Positive lag = sig timestamps are behind UTC by that many seconds.

apply_lag_to_parquet(src_path, lag_s, dst_path, ts_status=None)
    Read a Parquet file, shift its DatetimeIndex by lag_s seconds, write to dst_path.
    Optionally overwrites the ts_status column. Returns the number of rows written.

raw_stem(path)
    Strip Aeris file-type suffixes (Eng, spectra, spectralite) from a stem so that
    Raw, Eng, and Spectra files from the same session all share one lag-lookup key.

date_tag(path)
    Extract the YYMMDD date tag from an Aeris filename stem (second underscore field).
"""

import numpy as np
import pandas as pd
from pathlib import Path


def resample_series(s: pd.Series, freq_s: int = 1) -> pd.Series:
    s = s.resample(f"{freq_s}s").mean()
    return s.interpolate(method="time", limit=10)


def cross_correlate(
    ref: pd.Series,
    sig: pd.Series,
    max_lag_s: int = 600,
    freq_s: int = 1,
) -> float:
    """
    Return lag in seconds. Positive = sig timestamps are behind UTC by that many seconds.
    Correction at output: df.index += pd.Timedelta(seconds=lag).
    """
    start = max(ref.index[0], sig.index[0])
    end   = min(ref.index[-1], sig.index[-1])
    if start >= end:
        print("    [warn] No overlapping time window — defaulting to 0s")
        return 0.0
    combined = pd.DataFrame({"r": ref[start:end], "s": sig[start:end]}).dropna()
    if len(combined) < 10:
        print(f"    [warn] Only {len(combined)} overlapping point(s) — defaulting to 0s")
        return 0.0
    r_arr    = (combined["r"] - combined["r"].mean()).values
    s_arr    = (combined["s"] - combined["s"].mean()).values
    max_samp = max_lag_s // freq_s
    corr     = np.correlate(r_arr, s_arr, mode="full")
    lags     = np.arange(-(len(r_arr) - 1), len(r_arr))
    mask     = np.abs(lags) <= max_samp
    return float(lags[mask][np.argmax(corr[mask])] * freq_s)


def apply_lag_to_parquet(
    src_path,
    lag_s: float,
    dst_path,
    ts_status: str | None = None,
) -> int:
    """
    Read src_path, shift DatetimeIndex by lag_s seconds, write to dst_path.
    Creates parent directories as needed. Returns row count.
    If ts_status is given, overwrites the ts_status column before writing.
    """
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(src_path)
    if lag_s != 0.0:
        df.index = df.index + pd.Timedelta(seconds=lag_s)
    if ts_status is not None:
        df['ts_status'] = ts_status
    df.to_parquet(dst_path)
    return len(df)


def date_tag(path) -> str:
    """Extract the YYMMDD date tag from an Aeris filename (second underscore field).

    Examples
    --------
    'Ultra100321_260203_210000'    -> '260203'
    'Pico100017_260119_100000Eng'  -> '260119'
    """
    parts = Path(path).stem.split('_')
    return parts[1] if len(parts) >= 2 else ''


def raw_stem(path) -> str:
    """
    Strip Aeris file-type suffixes so Raw, Eng, and Spectra files from the same
    session share a single lag-lookup key.

    Examples
    --------
    'Ultra100321_260203_210000'      -> 'Ultra100321_260203_210000'
    'Ultra100321_260203_210000Eng'   -> 'Ultra100321_260203_210000'
    'Ultra100321_260203_210000spectra' -> 'Ultra100321_260203_210000'
    """
    s = Path(path).stem
    for suffix in ("Eng", "spectra", "spectralite"):
        if s.endswith(suffix):
            return s[: -len(suffix)]
    return s
