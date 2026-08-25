"""
Stage 03 alignment utilities.

Pure functions with no notebook or session-state dependencies.
Imported by pipeline/03_survey.ipynb, 03a_align_wyo.ipynb, and 03b_align_mml.ipynb;
reusable by Stage 04.

Public API
----------
resample_series(s, freq_s=1)
    Resample a time-indexed Series to a regular grid and interpolate short gaps.

cross_correlate(ref, sig, max_lag_s=600, freq_s=1)
    Return the lag in seconds that best aligns sig to ref via full cross-correlation.
    Positive lag = sig timestamps are behind UTC by that many seconds.

apply_lag_to_parquet(src_path, lag_s, dst_path, ts_status=None, lag_ref=None)
    Read a Parquet file, shift its DatetimeIndex by lag_s seconds, write to dst_path.
    Optionally overwrites the ts_status and lag_ref columns. Returns row count written.

raw_stem(path)
    Strip Aeris file-type suffixes (Eng, spectra, spectralite) from a stem so that
    Raw, Eng, and Spectra files from the same session all share one lag-lookup key.

date_tag(path)
    Extract the YYMMDD date tag from an Aeris filename stem (second underscore field).

load_quality_manifest(path)
    Load quality_manifest.yaml. Returns nested dict or empty dict if file absent.

file_quality(manifest, instrument, path)
    Return (status, reason, ref) for a file from the quality manifest.
    Falls back to ('uncertain', '', '') if not in manifest.

load_aligned_series(stage03_dir, instrument, subdir, col)
    Load all good aligned Parquet files for an instrument into a single Series.
    Returns None if no files found or the instrument dir doesn't exist yet.

resume_review(lag_offsets_path, lags_key, instrument)
    Reload a previously saved (confirmed, rejected) review state for one instrument
    from lag_offsets_{wyo,mml}.json, so a fresh kernel can re-run 03a/03b end to end
    without redoing — or silently discarding — the manual lag review.
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
    lag_ref: str | None = None,
) -> int:
    """
    Read src_path, shift DatetimeIndex by lag_s seconds, write to dst_path.
    Creates parent directories as needed. Returns row count.

    ts_status : overwrites the ts_status column if provided.
    lag_ref   : writes a lag_ref column recording which reference was used for
                alignment (e.g. 'WYO_picarro', 'LANL_Anem'). Useful for the
                analysis repo to know how well-grounded a file's timestamps are.
    """
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(src_path)
    if lag_s != 0.0:
        df.index = df.index + pd.Timedelta(seconds=lag_s)
    if ts_status is not None:
        df['ts_status'] = ts_status
    if lag_ref is not None:
        df['lag_ref'] = lag_ref
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
    'Ultra100321_260203_210000'        -> 'Ultra100321_260203_210000'
    'Ultra100321_260203_210000Eng'     -> 'Ultra100321_260203_210000'
    'Ultra100321_260203_210000spectra' -> 'Ultra100321_260203_210000'
    """
    s = Path(path).stem
    for suffix in ("Eng", "spectra", "spectralite"):
        if s.endswith(suffix):
            return s[: -len(suffix)]
    return s


# ── Quality manifest helpers ──────────────────────────────────────────────────

def load_quality_manifest(path) -> dict:
    """Load quality_manifest.yaml.

    Returns nested dict {instrument: {date_tag: {status, reason, ref}}}.
    Returns empty dict if the file does not exist.
    """
    import yaml
    path = Path(path)
    if not path.exists():
        return {}
    with open(path) as fh:
        data = yaml.safe_load(fh) or {}
    return data


def file_quality(manifest: dict, instrument: str, path) -> tuple:
    """Return (status, reason) for a file from the quality manifest.

    Looks up by instrument name and raw_stem (the session key shared by Raw, Eng,
    and Spectra files from the same session).  Falls back to ('uncertain', '')
    if the instrument or file is not in the manifest.

    status : 'good' | 'uncertain' | 'bad'
    reason : free-text note from the survey
    """
    stem      = raw_stem(path)
    inst_data = manifest.get(instrument, {})
    entry     = inst_data.get(stem, {})
    return (
        entry.get('status', 'uncertain'),
        entry.get('reason', ''),
    )


def reference_bad_dates(quality_manifest: dict, ref_instrument: str, ref_dir) -> set:
    """Return YYMMDD date tags where a reference instrument is marked bad in the manifest.

    Used to cascade pre-rejection to dependent instruments: if the reference was
    bad on a date, nothing that depends on it can be aligned for that date either.

    Determines dates from the first timestamp in each bad-marked parquet file rather
    than from the filename, so it works regardless of naming conventions.
    """
    bad_dates = set()
    ref_dir   = Path(ref_dir)
    for stem, entry in quality_manifest.get(ref_instrument, {}).items():
        if entry.get('status') != 'bad':
            continue
        f = ref_dir / f'{stem}.parquet'
        if not f.exists():
            continue
        try:
            idx = pd.read_parquet(f, columns=[]).index
            if len(idx) > 0:
                bad_dates.add(idx[0].strftime('%y%m%d'))
        except Exception:
            pass
    return bad_dates


def resume_review(lag_offsets_path, lags_key: str, instrument: str) -> tuple:
    """Reload a saved lag review for one instrument as (confirmed, rejected).

    The 03a/03b review widgets hold their state in notebook globals, and
    `save_lag_offsets_*()` serialises whatever is in those globals. That means a
    fresh kernel running the notebook top-to-bottom would write an EMPTY manifest
    over a real one and then apply zero lags to everything. Seeding the globals
    from the manifest on startup makes the notebooks safely re-runnable: the apply
    and pass-through cells can be re-executed after an upstream change (e.g. a
    Stage 02 rerun) without touching the human review.

    Parameters
    ----------
    lag_offsets_path : path-like
        STAGE_03_DIR/'lag_offsets_wyo.json' or .../'lag_offsets_mml.json'.
    lags_key : str
        Top-level key holding the per-instrument lag dicts — 'lags' in the WYO
        manifest, 'tube_lags' in the MML one.
    instrument : str
        e.g. 'WYO_aerisultra460'.

    Returns
    -------
    (confirmed, rejected) : (dict[str, float], set[str])
        Empty ({}, set()) if the manifest is missing or has no entry for the
        instrument — i.e. a first-time review starts from scratch as before.

    Note: saved 'lags' exclude rejected stems (save_lag_offsets_* filters them out),
    so a resumed session shows rejects as rejected but without their original slider
    value. That is the same information the apply step consumes, so nothing is lost.
    """
    import json
    lag_offsets_path = Path(lag_offsets_path)
    if not lag_offsets_path.exists():
        return {}, set()
    try:
        with open(lag_offsets_path) as fh:
            saved = json.load(fh)
    except Exception as e:
        print(f'  [WARN] could not read {lag_offsets_path.name}: {e} — starting empty')
        return {}, set()
    confirmed = dict(saved.get(lags_key, {}).get(instrument, {}))
    rejected  = set(saved.get('rejected', {}).get(instrument, []))
    print(f'  resumed {instrument}: {len(confirmed)} confirmed, {len(rejected)} rejected'
          f'  (from {lag_offsets_path.name})')
    return confirmed, rejected


def load_aligned_series(
    stage03_dir,
    instrument: str,
    subdir: str,
    col: str,
) -> 'pd.Series | None':
    """Load all good aligned Parquet files for an instrument into one time-indexed Series.

    Reads only files directly inside `{stage03_dir}/{instrument}/{subdir}/` — the bad/
    and bad_timestamp/ subdirectories are excluded because they are at deeper nesting.

    Returns None if the directory doesn't exist or no files are found.
    """
    inst_dir = Path(stage03_dir) / instrument
    if subdir:
        inst_dir = inst_dir / subdir
    if not inst_dir.exists():
        return None
    files = sorted(inst_dir.glob('*.parquet'))   # only direct children, not bad/ or bad_timestamp/
    if not files:
        return None
    parts = []
    for f in files:
        try:
            s = pd.read_parquet(f, columns=[col])[col].dropna()
            if len(s) > 0:
                parts.append(s)
        except Exception:
            pass
    if not parts:
        return None
    combined = pd.concat(parts).sort_index()
    return combined[~combined.index.duplicated(keep='first')]
