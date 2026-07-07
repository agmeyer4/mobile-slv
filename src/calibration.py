"""
Stage 04 calibration utilities.

Pure functions with no notebook or session-state dependencies, generic enough to be
reused for any gas-analyzer calibration exercise (not tied to this campaign's specific
instruments or dates). Imported by pipeline/04_calibration.ipynb, which supplies the
campaign-specific configuration (paths, instrument list, colors) and narration.

Public API
----------
Recommended entry points for a new campaign or a new species
    Pick "tank" or "reference", pass your data, get coefficients back with the standard
    diagnostic checks already run and plotted:

        coefs = cal.calibrate_and_check_tank(series_dict, tank, windows, 'CH4_ppm',
                                              colors, y_label='CH4 (ppm)')
        coef  = cal.calibrate_and_check_reference(ref_values, target_values, anchor='ols')

    calibrate_and_check_tank(series_dict, tank, windows, species_key, colors, y_label, title_prefix='')
        Tank-anchored: window_stats + fit_species per instrument, then the 1:1 scatter
        and the apply-back-to-own-windows sanity check. Returns {instrument: coef}.
    calibrate_and_check_reference(ref_values, target_values, anchor='ols', z_ref=None, z_tgt=None, ...)
        Reference-instrument: fit_reference_cal, then the matched-pairs scatter and
        (optionally) an interference-diagnostic plot. One target per call — loop over
        targets, since the pairing step upstream usually differs per target anyway.
    fit_reference_cal(ref_values, target_values, anchor='ols', z_ref=None, z_tgt=None)
        The fit alone, no plots — anchor='ols' for plain regression, anchor='pinned' to
        pin the baseline to a known (z_ref, z_tgt) point and fit only the gain.

    Need more control than these give (e.g. mobile-slv's own CH4 tank fit, which reuses
    one shared multi-instrument stats_df for error bars AND a multi-date drift check)?
    Drop to the lower-level functions below directly — same functions these wrap.

    Known limitation: parse_tank_details recognizes CH4/C3H8/C2H6 by name in its
    concentration-line regex; a campaign measuring different species needs to extend
    that branch (a smaller, separate task from the fitting-method generalization above).

Parsing
    parse_tank_details(filepath)
        Parse a tank/dilution calibration manifest into tank concentrations and
        per-date calibration windows.

Tank-anchored fitting
    linreg(x, y)
        OLS dropping NaN pairs. Returns (slope, intercept, r2, n).
    window_stats(series_dict, windows)
        Per-window mean/n for a dict of instrument -> time-indexed Series.
    fit_species(stats_df, tank, inst, species_key)
        Multi-point OLS: known tank concentration (x) vs instrument window mean (y).

Peak-alignment fitting
    find_peak_matches(ref_series, target_series, height, prominence, min_distance_s, window_s)
        For each local peak in ref_series (found per calendar day), take each target
        series' max within a window around the peak time. One row per matched event.

Ambient cross-calibration (continuous overlap, no tank needed)
    dates_for_platform(routing_manifest, platform, prefix=None)
        Sorted 'YYYYMMDD' dates where routing_manifest[key] == platform, optionally
        restricted to one instrument via a raw_stem prefix — the `dates` argument to
        restrict_series below often comes from here.
    restrict_series(series, dates, windows_by_date=None, pad_min=5)
        Restrict a series to specific dates, optionally excluding tank windows.
    pair_series_nearest(ref, target, tolerance_s=1)
        Pair two unsynced time series by nearest timestamp — for regressing one
        instrument's ambient reading against another's using every overlapping sample
        (huge n) instead of a handful of discrete tank points. Fit the pairs with the
        existing `linreg`.

Zero+span (baseline anchor + peak-alignment span)
    tank_window_stats(series, windows_by_date, tank_key)
        Pooled (mean, std, n) over all windows with a given tank_key — the std is the
        in-tank measurement noise (an error bar on a calibration point).
    tank_window_mean(series, windows_by_date, tank_key)
        Pooled mean of a series over all windows with a given tank_key (e.g. N2-zero).
    ambient_baseline_stats(series, q=0.5)
        (value, spread, n) of a series at a given quantile (default median) — an
        anchor for matching one instrument's baseline to another's, when there's no
        shared tank reading to anchor to instead. spread = half the IQR (robust to
        occasional plumes in the "ambient" population).
    fit_zero_span(ref_peaks, target_peaks, z_ref, z_tgt)
        Zero+span cross-calibration: anchor the baseline to a shared reference value
        (a tank reading, an ambient baseline, etc.), fit the gain on peaks. Returns
        apply-convention slope/intercept.
    fit_reference_cal(ref_values, target_values, anchor='ols', z_ref=None, z_tgt=None)
        Dispatches to linreg (anchor='ols') or fit_zero_span (anchor='pinned') — see
        "Recommended entry points" above; this is the lower-level fit-only piece.

Candidate comparison (for species where more than one method is viable)
    compare_candidate_coefs(candidates, stats_df, tank, species_key, inst)
        Apply every candidate coefficient (e.g. {'tank': ..., 'reference': ...}, either
        may be None) back to one instrument's own tank-window means and report
        RMS/max-abs residual per candidate, side by side — the common yardstick for
        deciding which method to lock in. None entries are skipped, so a species with
        only one viable candidate (C3H8: tank only; C2H6: reference only) calls this
        with the same shape as one with two (CH4).
    assess_tank_coverage(tank, species_key, ambient_series, quantiles=(0.5, 0.99, 1.0))
        Numeric version of "does the tank's certified range actually cover what we need
        to calibrate" — the tank's max certified point vs. requested quantiles of a real
        ambient/plume population. Returns ratios, not a verdict.

Applying calibration
    apply_linear(series, coef)
        Apply a {'slope', 'intercept', 'scale_in'} correction to a Series.
    apply_calibration_to_dir(src_dir, dst_dir, corrections_for_inst)
        Apply one or more corrections to every Parquet file in src_dir, writing
        calibrated copies (with *_cal columns + cal_coefs_ref) to dst_dir.
    copy_passthrough_dir(src_dir, dst_dir)
        Straight-copy every Parquet file in src_dir to dst_dir, unchanged.

Metadata
    git_info(repo_root)
        Return (commit_hash, is_dirty) for provenance tagging.

Plotting (each returns a plotly go.Figure; callers add .show() and any
campaign-specific annotations)
    plot_timeseries_panels(panels, colors, t0, t1, title, height=None)
        Stacked native-resolution timeseries panels over a chosen [t0, t1] window
        (no resampling or averaging).
    plot_timeseries_with_windows(series_dict, windows, colors, title, y_title)
        Timeseries with shaded, labeled calibration windows highlighted.
    plot_calibration_scatter(x_by_group, y_by_group, fits, colors, x_title, y_title, title, identity_line=True, yerr_by_group=None, xerr_by_group=None)
        Scatter + per-group fit line + optional dashed y=x identity line + optional error bars.
    plot_raw_vs_corrected(raw, corrected, title, y_title)
        Dual-trace before/after overlay for one series pair.
    plot_raw_corrected_vs_reference(reference, ref_label, pairs, colors, t0, t1, title, y_title)
        Per-target panel overlaying a reference series with each target's raw (dashed)
        and corrected (solid) trace over [t0, t1] — for checking a correction lands on
        a reference instrument.
    plot_peaks_highlighted(series_dict, peak_times, colors, title, y_title)
        Timeseries with detected-peak markers overlaid.
    plot_residual_diagnostic(residual, diagnostic, label, color, corr_threshold=0.3)
        Residual-vs-diagnostic-variable scatter. Returns (fig, corr) so the caller
        decides whether/how to flag it.
    plot_range_levels(levels, x_title, title)
        Log-scale dumbbell plot of named (label, value, color) levels — e.g. a tank's
        certified point against ambient/plume percentiles, for `assess_tank_coverage`.
"""

import re
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats as scipy_stats
from scipy.signal import find_peaks


# ── Parsing ──────────────────────────────────────────────────────────────────────

def parse_tank_details(filepath):
    """Parse a tank/dilution calibration manifest into tank concentrations and
    per-date calibration windows.

    Expected format: blocks of "<label>: <value> ppm/ppb <gas>, ..." concentration
    lines, followed by dated sections ("<Month> <day>, <year>") each containing
    "<HH:MM:SS> to <HH:MM:SS> - <label>" window lines.

    Parameters
    ----------
    filepath : path-like
        Path to the tank details manifest text file.

    Returns
    -------
    tank : dict
        {tank_key: {'CH4_ppm', 'C3H8_ppm', 'C2H6_ppb'}} (None where not certified).
        Always includes an implicit 'N2_zero' entry at 0 for all three gases.
    windows_by_date : dict
        {'YYYYMMDD': [{'tank_key', 'start', 'end'}, ...]}
    """
    MONTHS = {m: i + 1 for i, m in enumerate([
        'january', 'february', 'march', 'april', 'may', 'june',
        'july', 'august', 'september', 'october', 'november', 'december'])}

    conc_re = re.compile(r'^([\w\s]+):\s*(.*)')
    val_re  = re.compile(r'([\d.]+)\s*(ppm|ppb)\s+(\w+)', re.I)
    date_re = re.compile(r'(\w+)\s+(\d+),\s+(\d{4})')
    win_re  = re.compile(r'(\d{2}:\d{2}:\d{2})\s+to\s+(\d{2}:\d{2}:\d{2})\s*[-–]\s*(.+)')

    def norm_key(raw):
        s = raw.strip().lower()
        if s in ('n2 zero', 'n2_zero'):
            return 'N2_zero'
        if s in ('noaa', 'noaa tank'):
            return 'NOAA'
        m = re.match(r'dilution\s*(\d)', s)
        return f'Dilution{m.group(1)}' if m else raw.strip().replace(' ', '_')

    tank = {'N2_zero': {'CH4_ppm': 0.0, 'C3H8_ppm': 0.0, 'C2H6_ppb': 0.0}}
    windows_by_date = {}
    current_date = None

    for line in Path(filepath).read_text().splitlines():
        line = line.strip()
        if not line:
            continue

        dm = date_re.search(line)
        if dm:
            month = MONTHS[dm.group(1).lower()]
            current_date = f'{dm.group(3)}{month:02d}{int(dm.group(2)):02d}'
            windows_by_date[current_date] = []
            continue

        wm = win_re.match(line)
        if wm and current_date:
            t0, t1, raw_label = wm.groups()
            date_part = f'{current_date[:4]}-{current_date[4:6]}-{current_date[6:]}'
            windows_by_date[current_date].append({
                'tank_key': norm_key(raw_label),
                'start': f'{date_part} {t0}',
                'end':   f'{date_part} {t1}',
            })
            continue

        if current_date is None:
            cm = conc_re.match(line)
            if cm:
                entry = {'CH4_ppm': None, 'C3H8_ppm': None, 'C2H6_ppb': None}
                for val, unit, gas in val_re.findall(cm.group(2)):
                    val, gas = float(val), gas.upper()
                    if gas == 'CH4':
                        entry['CH4_ppm'] = val if unit.lower() == 'ppm' else val / 1000
                    elif gas == 'C3H8':
                        entry['C3H8_ppm'] = val if unit.lower() == 'ppm' else val / 1000
                    elif gas == 'C2H6':
                        entry['C2H6_ppb'] = val * 1000 if unit.lower() == 'ppm' else val
                tank[norm_key(cm.group(1))] = entry

    return tank, windows_by_date


# ── Tank-anchored fitting ────────────────────────────────────────────────────────

def linreg(x, y):
    """OLS dropping NaN pairs.

    Parameters
    ----------
    x, y : array-like

    Returns
    -------
    (slope, intercept, r2, n) : tuple of float, float, float, int
        First three are NaN (with n = count of finite pairs) if fewer than 2 usable
        points or x is constant.
    """
    x, y = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
    mask = np.isfinite(x) & np.isfinite(y)
    n = int(mask.sum())
    if n < 2 or x[mask].max() == x[mask].min():
        return np.nan, np.nan, np.nan, n
    sl, ic, r, *_ = scipy_stats.linregress(x[mask], y[mask])
    return sl, ic, r ** 2, n


def window_stats(series_dict, windows):
    """Per-window mean/n for a dict of instrument -> time-indexed Series.

    Parameters
    ----------
    series_dict : dict[str, pd.Series | None]
        Instrument name -> UTC-indexed Series. None entries are skipped.
    windows : list[dict]
        Each with 'tank_key' (or any label), 'start', 'end' (parseable timestamps).

    Returns
    -------
    pd.DataFrame
        One row per window, columns 'tank_key', '{inst}_mean', '{inst}_n' per instrument.
    """
    rows = []
    for w in windows:
        t0, t1 = pd.Timestamp(w['start'], tz='UTC'), pd.Timestamp(w['end'], tz='UTC')
        row = {'tank_key': w['tank_key']}
        for inst, s in series_dict.items():
            if s is None:
                continue
            sub = s[t0:t1]
            row[f'{inst}_mean'] = sub.mean() if len(sub) else np.nan
            row[f'{inst}_std']  = sub.std() if len(sub) else np.nan
            row[f'{inst}_n']    = len(sub)
        rows.append(row)
    return pd.DataFrame(rows)


def fit_species(stats_df, tank, inst, species_key):
    """Multi-point OLS: known tank concentration (x) vs instrument window mean (y).

    Parameters
    ----------
    stats_df : pd.DataFrame
        Output of window_stats, must have a '{inst}_mean' column.
    tank : dict
        Output of parse_tank_details (the `tank` return value).
    inst : str
    species_key : str
        e.g. 'CH4_ppm', 'C3H8_ppm'.

    Returns
    -------
    dict or None
        {'slope', 'intercept', 'r2', 'n'}, or None if fewer than 2 usable points.
    """
    col = f'{inst}_mean'
    if col not in stats_df.columns:
        return None
    x = stats_df['tank_key'].map(lambda k: tank.get(k, {}).get(species_key)).astype(float).values
    y = stats_df[col].astype(float).values
    sl, ic, r2, n = linreg(x, y)
    if n < 2:
        return None
    return {'slope': sl, 'intercept': ic, 'r2': r2, 'n': n}


# ── Peak-alignment fitting ───────────────────────────────────────────────────────

def find_peak_matches(ref_series, target_series, height, prominence, min_distance_s, window_s):
    """For each local peak in ref_series (found per calendar day), take each target
    series' max within +-window_s seconds of the peak time. One row per matched event.

    Parameters
    ----------
    ref_series : pd.Series
        Reference instrument's series (1 Hz assumed for distance/height in native units).
    target_series : dict[str, pd.Series | None]
        Other series (instruments and/or diagnostic channels) to sample at each peak.
    height, prominence : float
        Passed to scipy.signal.find_peaks.
    min_distance_s : int
        Minimum samples between peaks (passed as `distance`).
    window_s : int
        +-window (seconds) around each peak time to take each target's max.

    Returns
    -------
    pd.DataFrame
        Columns: 'date', 'peak_time', 'ref', plus one column per target_series key.
    """
    records = []
    for day, day_ref in ref_series.groupby(ref_series.index.date):
        if len(day_ref) < 20:
            continue
        peaks, _ = find_peaks(day_ref.values, height=height, prominence=prominence,
                               distance=min_distance_s)
        for p in peaks:
            t_peak = day_ref.index[p]
            row = {'date': str(day), 'peak_time': t_peak, 'ref': float(day_ref.iloc[p])}
            for name, s in target_series.items():
                if s is None:
                    row[name] = np.nan
                    continue
                win = s[t_peak - pd.Timedelta(seconds=window_s): t_peak + pd.Timedelta(seconds=window_s)]
                row[name] = float(win.max()) if len(win) else np.nan
            records.append(row)
    return pd.DataFrame(records)


# ── Ambient cross-calibration (continuous overlap, no tank needed) ───────────────

def dates_for_platform(routing_manifest, platform, prefix=None):
    """Sorted 'YYYYMMDD' dates where `routing_manifest[key] == platform`.

    Parameters
    ----------
    routing_manifest : dict
        raw_stem -> platform tag (e.g. Stage 02's routing_manifest.json, keyed by
        Aeris raw_stem like 'Ultra100321_260203_210000').
    platform : str
        Platform tag to match (e.g. 'WYO', 'MML').
    prefix : str, optional
        If given, only keys starting with this raw_stem prefix count — restricts to one
        instrument (e.g. 'Ultra100321') instead of every instrument on that platform.

    Returns
    -------
    list[str]
        'YYYYMMDD' date tags, parsed from each matching key via `src.align.date_tag`.
    """
    from src.align import date_tag
    return sorted({
        '20' + date_tag(k) for k, v in routing_manifest.items()
        if v == platform and (prefix is None or k.startswith(prefix))
    })


def restrict_series(series, dates, windows_by_date=None, pad_min=5):
    """Restrict a series to specific calendar dates, optionally excluding tank windows.

    Parameters
    ----------
    series : pd.Series or None
    dates : set or list of str
        'YYYYMMDD' date tags to keep.
    windows_by_date : dict or None
        If given (the output of parse_tank_details), every window across every date is
        excluded (padded by pad_min minutes each side) — keeps tank/dilution gas out of
        an otherwise-ambient population.
    pad_min : float
        Padding (minutes) applied to each excluded window.

    Returns
    -------
    pd.Series or None
    """
    if series is None:
        return None
    s = series[series.index.strftime('%Y%m%d').isin(dates)]
    if windows_by_date:
        mask = pd.Series(True, index=s.index)
        for wins in windows_by_date.values():
            for w in wins:
                t0 = pd.Timestamp(w['start'], tz='UTC') - pd.Timedelta(minutes=pad_min)
                t1 = pd.Timestamp(w['end'],   tz='UTC') + pd.Timedelta(minutes=pad_min)
                mask &= ~((s.index >= t0) & (s.index <= t1))
        s = s[mask]
    return s


def pair_series_nearest(ref, target, tolerance_s=1):
    """Pair two time-indexed Series by nearest timestamp, within a tolerance.

    For cross-calibrating two instruments that sample the same air but don't share a
    clock (different native rates, unsynced sample times) — e.g. regressing an Aeris
    unit's ambient CH4 against Picarro's, using every overlapping sample rather than a
    handful of discrete tank points.

    Parameters
    ----------
    ref, target : pd.Series
        Time-indexed (tz-aware) series to pair. NaNs dropped before pairing.
    tolerance_s : float
        Maximum time difference (seconds) allowed for a match; unmatched points dropped.

    Returns
    -------
    pd.DataFrame
        Columns 'ref', 'target', indexed by the matched timestamp (from `ref`).
    """
    r = ref.dropna().sort_index()
    t = target.dropna().sort_index()
    r_df = pd.DataFrame({'ref': r.values}, index=r.index).rename_axis('ts').reset_index()
    t_df = pd.DataFrame({'target': t.values}, index=t.index).rename_axis('ts').reset_index()
    merged = pd.merge_asof(r_df, t_df, on='ts', tolerance=pd.Timedelta(seconds=tolerance_s),
                           direction='nearest').dropna()
    return merged.set_index('ts')


# ── Zero + span calibration (tank zero anchor + peak-alignment span) ──────────────

def tank_window_stats(series, windows_by_date, tank_key):
    """Pooled (mean, std, n) of `series` over every window matching `tank_key`.

    Reads an instrument's response to a known tank (e.g. the N2-zero) by pooling all
    samples inside any window with that tank_key. The std is the in-tank measurement
    noise — useful as an error bar on a calibration point. Returns (nan, nan, 0) if the
    instrument has no data in any matching window.
    """
    parts = []
    for wins in windows_by_date.values():
        for w in wins:
            if w.get('tank_key') == tank_key:
                sub = series[pd.Timestamp(w['start'], tz='UTC'):pd.Timestamp(w['end'], tz='UTC')]
                if len(sub):
                    parts.append(sub)
    if not parts:
        return float('nan'), float('nan'), 0
    pooled = pd.concat(parts)
    return float(pooled.mean()), float(pooled.std()), int(len(pooled))


def tank_window_mean(series, windows_by_date, tank_key):
    """Pooled mean of `series` over windows matching `tank_key` (see tank_window_stats)."""
    return tank_window_stats(series, windows_by_date, tank_key)[0]


def ambient_baseline_stats(series, q=0.5):
    """(value, spread, n) of `series` at quantile `q` (default: median).

    An anchor for matching one instrument's baseline to another's when there's no
    shared tank/zero reference to anchor to instead — e.g. two instruments whose
    absolute baselines genuinely disagree, where the goal is cross-instrument
    agreement rather than an absolute-truth zero. `spread` is half the interquartile
    range, a measure of how variable the "ambient" population is (robust to the
    occasional plume, unlike a plain std).

    Parameters
    ----------
    series : pd.Series
        Should already be restricted to the population you want the baseline to
        represent (e.g. ambient-only, tank windows excluded).
    q : float
        Quantile to anchor on (0.5 = median).

    Returns
    -------
    (value, spread, n) : tuple of float, float, int
    """
    s = series.dropna()
    if len(s) == 0:
        return float('nan'), float('nan'), 0
    value = float(s.quantile(q))
    spread = float((s.quantile(0.75) - s.quantile(0.25)) / 2)
    return value, spread, int(len(s))


def fit_zero_span(ref_peaks, target_peaks, z_ref, z_tgt):
    """Zero + span cross-calibration mapping a target instrument onto a reference.

    Anchors the baseline exactly — ``corrected(z_tgt) == z_ref``, where z_tgt / z_ref
    are the target's and reference's readings at a shared anchor point (a tank zero,
    an ambient baseline percentile, anything the two series can both be evaluated
    at) — then fits the gain (span) on the plume peaks through the anchor-subtracted
    origin. Returned in the ``(measured*scale_in - intercept)/slope`` convention used by
    ``apply_linear``, so nothing special is needed at apply time.

    Parameters
    ----------
    ref_peaks, target_peaks : array-like
        Matched peak magnitudes (reference and target), same length, same units.
    z_ref, z_tgt : float
        Reference and target readings at the shared zero (same units as the peaks).

    Returns
    -------
    dict or None
        {'slope', 'intercept', 'gain', 'r2', 'n', 'z_ref', 'z_tgt'} with
        slope = 1/gain, intercept = z_tgt - z_ref/gain, and r2 the through-origin span
        fit. None if there are no usable peaks.
    """
    X = np.asarray(target_peaks, dtype=float) - z_tgt
    Y = np.asarray(ref_peaks, dtype=float) - z_ref
    m = np.isfinite(X) & np.isfinite(Y)
    X, Y = X[m], Y[m]
    if X.size < 1 or np.sum(X * X) == 0:
        return None
    gain = float(np.sum(X * Y) / np.sum(X * X))
    ss_tot = float(np.sum(Y ** 2))
    r2 = float(1 - np.sum((Y - gain * X) ** 2) / ss_tot) if ss_tot > 0 else np.nan
    return {'slope': 1.0 / gain, 'intercept': z_tgt - z_ref / gain,
            'gain': gain, 'r2': r2, 'n': int(m.sum()),
            'z_ref': float(z_ref), 'z_tgt': float(z_tgt)}


def fit_reference_cal(ref_values, target_values, anchor='ols', z_ref=None, z_tgt=None):
    """Fit a target instrument against already-matched reference values.

    The single entry point for "calibrate against a reference instrument" — the
    counterpart to tank-anchored fitting (`fit_species`). The caller does the pairing
    (`pair_series_nearest` for continuous ambient overlap, `find_peak_matches` for
    plume-peak matching, or anything else) since that step is usually also needed for
    diagnostics/plotting the caller wants to do anyway; this function only decides how
    to fit the matched values.

    Parameters
    ----------
    ref_values, target_values : array-like
        Matched reference/target values, same length, same units.
    anchor : {'ols', 'pinned'}
        'ols'    — plain regression (`linreg`); the offset floats free. Good when there
                   are many matched points spanning a wide range (e.g. continuous
                   ambient overlap against a reference instrument).
        'pinned' — force the fit through a known (z_ref, z_tgt) point (a tank zero via
                   `tank_window_stats`, or the reference's own ambient baseline via
                   `ambient_baseline_stats`), then fit the gain on `values`
                   (`fit_zero_span` underneath). Good with few matched points (e.g.
                   plume peaks) where the baseline needs to be pinned to something
                   trustworthy rather than left to the regression's own intercept.
    z_ref, z_tgt : float, required if anchor='pinned'
        Reference and target readings at the shared anchor point.

    Returns
    -------
    dict or None
        Same `apply_linear`-compatible convention as `fit_species`/`fit_zero_span`:
        {'slope', 'intercept', 'r2', 'n', ...}.
    """
    if anchor == 'ols':
        sl, ic, r2, n = linreg(ref_values, target_values)
        if n < 2 or not np.isfinite(sl):
            return None
        return {'slope': sl, 'intercept': ic, 'r2': r2, 'n': n}
    elif anchor == 'pinned':
        if z_ref is None or z_tgt is None:
            raise ValueError("anchor='pinned' requires z_ref and z_tgt")
        return fit_zero_span(ref_values, target_values, z_ref, z_tgt)
    else:
        raise ValueError(f"unknown anchor {anchor!r} (expected 'ols' or 'pinned')")


# ── Candidate comparison ─────────────────────────────────────────────────────────
#
# For a species where more than one calibration method is viable (e.g. CH4: tank-anchored
# AND reference-cross-cal), these two functions let a campaign compute every viable
# candidate and judge them on a common, numeric yardstick instead of eyeballing separate
# plots -- the "compute both, compare, lock one in" workflow this module is meant to
# support generically. Neither function embeds a pass/fail verdict: both return numbers
# for a human to read and a choice to be locked in explicitly by the caller (e.g. the
# notebook's own CAL_METHOD_LOCKED), matching this project's general preference for
# keeping judgment calls human-reviewed rather than silently automated.

def compare_candidate_coefs(candidates, stats_df, tank, species_key, inst):
    """Apply each candidate coefficient back to one instrument's tank-window means.

    The common yardstick every candidate method is judged against, regardless of how it
    was fit: this instrument's own tank-window means (from `window_stats`) versus the
    tank's certified concentrations (from `parse_tank_details`) -- the one dataset
    available for every species/instrument combination in this campaign, whether or not
    the candidate itself was fit on tank data (a reference-fit candidate still has an
    instrument response to compare against the tank's known concentrations, even though
    it wasn't trained on them).

    Parameters
    ----------
    candidates : dict[str, dict | None]
        candidate_name (e.g. 'tank', 'reference') -> coefficient dict (same shape
        returned by `fit_species` / `fit_reference_cal`), or None if that candidate
        wasn't fit / isn't viable for this instrument. None entries are skipped -- this
        is what lets a species with only one viable method (C3H8: only 'tank'; C2H6:
        only 'reference') reuse this same function as a species with two (CH4: both),
        with no special-casing.
    stats_df : pd.DataFrame
        Output of `window_stats`, must have an `f'{inst}_mean'` column.
    tank : dict
        Output of `parse_tank_details`.
    species_key : str
        e.g. 'CH4_ppm'.
    inst : str
        Which instrument's window means to score the candidates against.

    Returns
    -------
    pd.DataFrame
        Indexed by candidate name (only candidates present and non-None). Columns:
        'n_tank_points', 'rms_resid', 'max_abs_resid' (residual = corrected - true tank
        concentration, in species_key's units), plus the candidate's own 'r2' and 'n'
        (its fit diagnostics for context -- 'n' there is the candidate's OWN fit sample
        size, which may be much larger than 'n_tank_points', e.g. a reference-fit
        candidate trained on thousands of ambient pairs but evaluated here at only a
        handful of tank points). Empty DataFrame if no candidate is usable. No pass/fail
        column -- judging "adequate" is left to whoever reads the table.
    """
    col = f'{inst}_mean'
    if col not in stats_df.columns:
        return pd.DataFrame()
    xt = stats_df['tank_key'].map(lambda k: tank.get(k, {}).get(species_key)).astype(float)
    rows = []
    for name, coef in candidates.items():
        if coef is None:
            continue
        corrected = apply_linear(stats_df[col].astype(float), coef)
        resid = (corrected - xt).values
        resid = resid[np.isfinite(resid)]
        rows.append({
            'candidate': name,
            'n_tank_points': int(resid.size),
            'rms_resid': float(np.sqrt(np.mean(resid ** 2))) if resid.size else np.nan,
            'max_abs_resid': float(np.max(np.abs(resid))) if resid.size else np.nan,
            'r2': coef.get('r2', np.nan),
            'n': coef.get('n', np.nan),
        })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).set_index('candidate')


def assess_tank_coverage(tank, species_key, ambient_series, quantiles=(0.5, 0.99, 1.0)):
    """Compare the tank's certified concentration range against a real ambient/plume
    population, as numbers instead of an eyeballed plot.

    Generalizes the check that shows a species' certified tank point(s) may sit far
    below real plume levels (e.g. C2H6's single NOAA point, 1.63 ppb) into a reusable
    function: is there enough *certified range* to trust a tank fit for this species,
    given what it actually needs to cover? Returns numbers, not a verdict -- deciding
    "adequate" is a human judgment call left to whoever reads the table.

    Parameters
    ----------
    tank : dict
        Output of `parse_tank_details`.
    species_key : str
        e.g. 'C2H6_ppb', 'CH4_ppm'.
    ambient_series : pd.Series
        The population the calibration actually needs to span -- typically ambient +
        plume, with tank windows already excluded (e.g. via `restrict_series`).
        Quantile 1.0 (max) captures the largest plume peak.
    quantiles : tuple of float
        Which quantiles of `ambient_series` to report against the tank's max certified
        point.

    Returns
    -------
    pd.DataFrame
        One row. `tank_max_certified` column, one `q{q}_value` column per requested
        quantile, and one `tank_max_over_q{q}` ratio column per quantile (>1 means the
        tank's top certified point exceeds that population level; <1 flags a coverage
        gap -- deliberately not converted into a bool/verdict here).
    """
    certified = [v[species_key] for v in tank.values() if v.get(species_key) is not None]
    tank_max = float(max(certified)) if certified else float('nan')
    amb = ambient_series.dropna()
    row = {'tank_max_certified': tank_max}
    for q in quantiles:
        qval = float(amb.quantile(q)) if len(amb) else float('nan')
        row[f'q{q}_value'] = qval
        row[f'tank_max_over_q{q}'] = (tank_max / qval) if qval else float('nan')
    return pd.DataFrame([row])


# ── Applying calibration ─────────────────────────────────────────────────────────

def apply_linear(series, coef):
    """Apply a linear correction to a Series.

    calibrated = (measured * scale_in - intercept) / slope

    Parameters
    ----------
    series : pd.Series
    coef : dict
        Must have 'slope', 'intercept'; 'scale_in' defaults to 1.0.
    """
    scale = coef.get('scale_in', 1.0)
    return (series * scale - coef['intercept']) / coef['slope']


def apply_calibration_to_dir(src_dir, dst_dir, corrections_for_inst):
    """Apply one or more corrections to every Parquet file in src_dir.

    Reads each direct-child Parquet file (bad/ and bad_timestamp/ subdirectories are
    not descended into), adds a `{col_out}` column per matching correction whose
    `col_in` is present, tags the file with `cal_coefs_ref`, and writes to dst_dir.

    Parameters
    ----------
    src_dir : path-like
    dst_dir : path-like
    corrections_for_inst : dict[str, dict]
        gas -> correction dict, each with 'col_in', 'col_out', plus whatever
        apply_linear needs ('slope', 'intercept', 'scale_in').

    Returns
    -------
    (n_files, n_rows) : tuple of int, int
    """
    src_dir, dst_dir = Path(src_dir), Path(dst_dir)
    if not src_dir.exists():
        return 0, 0
    dst_dir.mkdir(parents=True, exist_ok=True)
    n_files = n_rows = 0
    for f in sorted(src_dir.glob('*.parquet')):
        df = pd.read_parquet(f)
        for gas, c in corrections_for_inst.items():
            if c['col_in'] not in df.columns:
                continue
            df[c['col_out']] = apply_linear(df[c['col_in']], c)
        df['cal_coefs_ref'] = 'calibration_coefs.json'
        df.to_parquet(dst_dir / f.name)
        n_files += 1
        n_rows  += len(df)
    return n_files, n_rows


def copy_passthrough_dir(src_dir, dst_dir):
    """Straight-copy every direct-child Parquet file in src_dir to dst_dir, unchanged.

    Returns
    -------
    n_files : int
    """
    src_dir, dst_dir = Path(src_dir), Path(dst_dir)
    if not src_dir.exists():
        return 0
    dst_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    for f in sorted(src_dir.glob('*.parquet')):
        shutil.copy2(f, dst_dir / f.name)
        n += 1
    return n


# ── Metadata ─────────────────────────────────────────────────────────────────────

def git_info(repo_root):
    """Return (commit_hash, is_dirty) for provenance tagging. ('unknown', False) on failure.

    Thin re-export of `src.provenance.git_info` — kept here so existing `cal.git_info(...)`
    call sites don't need to change; new code should import from `src.provenance` directly,
    which also has `check_clean` (dirty-tree warning) and `upstream_ref` (cross-stage
    provenance chaining).
    """
    from src.provenance import git_info as _git_info
    return _git_info(repo_root)


# ── Plotting ─────────────────────────────────────────────────────────────────────

def plot_timeseries_panels(panels, colors, t0, t1, title, height=None):
    """Stacked native-resolution timeseries panels over a chosen [t0, t1] window.

    Each series is sliced to [t0, t1] and plotted exactly as stored — no resampling,
    averaging, or interpolation. Intended for inspecting a specific period of raw (or
    calibrated) data at full resolution rather than a downsampled campaign overview.

    Parameters
    ----------
    panels : list[tuple]
        Each is (panel_title, y_title, series_dict), where series_dict maps a label ->
        pd.Series (None entries skipped). One stacked subplot row per panel, sharing the
        x-axis. A label keeps one legend entry across all panels (legendgroup).
    colors : dict[str, str]
        label -> hex color.
    t0, t1 : timestamp-like
        Inclusive window bounds (tz-aware, matching the series index).
    title : str
    height : int, optional
        Total figure height; defaults to ~240 px per panel.

    Returns
    -------
    go.Figure
    """
    n = len(panels)
    fig = make_subplots(rows=n, cols=1, shared_xaxes=True,
                        subplot_titles=[p[0] for p in panels])
    seen = set()
    for row, (_ptitle, ytitle, series_dict) in enumerate(panels, start=1):
        for label, s in series_dict.items():
            if s is None:
                continue
            sub = s[t0:t1]
            if len(sub) == 0:
                continue
            fig.add_trace(go.Scatter(
                x=sub.index, y=sub.values, mode='lines',
                line=dict(color=colors.get(label, 'gray'), width=1),
                name=label, legendgroup=label,
                showlegend=label not in seen,
            ), row=row, col=1)
            seen.add(label)
        fig.update_yaxes(title_text=ytitle, row=row, col=1)
    fig.update_xaxes(title_text='Time (UTC)', row=n, col=1)
    fig.update_layout(title=title, template='plotly_white',
                      height=height or (240 * n + 90), hovermode='x unified')
    return fig


def plot_timeseries_with_windows(series_dict, windows, colors, title, y_title):
    """Timeseries with shaded, labeled calibration windows highlighted.

    Parameters
    ----------
    series_dict : dict[str, pd.Series | None]
        Instrument name -> series, plotted over the union span of all windows
        (padded 10% on each side).
    windows : list[dict]
        Each with 'tank_key' (or any label), 'start', 'end'.
    colors : dict[str, str]
        Instrument name -> hex color.
    title, y_title : str

    Returns
    -------
    go.Figure
    """
    fig = go.Figure()

    starts = [pd.Timestamp(w['start'], tz='UTC') for w in windows]
    ends   = [pd.Timestamp(w['end'], tz='UTC') for w in windows]
    t0, t1 = min(starts), max(ends)
    pad = (t1 - t0) * 0.1
    view_start, view_end = t0 - pad, t1 + pad

    for inst, s in series_dict.items():
        if s is None:
            continue
        sub = s[view_start:view_end]
        if len(sub) == 0:
            continue
        fig.add_trace(go.Scatter(
            x=sub.index, y=sub.values, mode='lines',
            line=dict(color=colors.get(inst, 'gray'), width=1.5), name=inst,
        ))

    for w in windows:
        fig.add_vrect(
            x0=w['start'], x1=w['end'],
            fillcolor='rgba(128,128,128,0.15)', line_width=0,
            annotation_text=w['tank_key'], annotation_position='top left',
            annotation=dict(textangle=-90, font_size=10),
        )

    fig.update_layout(title=title, xaxis_title='Time (UTC)', yaxis_title=y_title,
                       template='plotly_white', height=460, hovermode='x unified')
    return fig


def plot_calibration_scatter(x_by_group, y_by_group, fits, colors, x_title, y_title,
                              title, identity_line=True, yerr_by_group=None, xerr_by_group=None):
    """Scatter + per-group fit line + optional dashed y=x identity line.

    Parameters
    ----------
    x_by_group, y_by_group : dict[str, array-like]
        Group name (e.g. instrument) -> x/y values. Both dicts share the same keys.
    fits : dict[str, dict | None]
        Group name -> {'slope', 'intercept', ...} used to draw each fit line.
    colors : dict[str, str]
        Group name -> hex color (falls back to 'gray').
    identity_line : bool
        If True, draw a dashed y=x reference line spanning the data range.
    yerr_by_group, xerr_by_group : dict[str, array-like] or None
        Optional per-group symmetric error-bar magnitudes (e.g. 1σ in-window noise),
        aligned to the same arrays as x/y. Drawn as error bars on the points.

    Notes
    -----
    Points and their fit line share a ``legendgroup`` per group, so clicking a group's
    legend entry toggles both together (points + fit line) — the fit line has no legend
    entry of its own, it just follows its points' visibility.

    Returns
    -------
    go.Figure
    """
    fig = go.Figure()
    all_x, all_y = [], []

    for group, x in x_by_group.items():
        y = y_by_group[group]
        x_arr, y_arr = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
        valid = np.isfinite(x_arr) & np.isfinite(y_arr)
        if not valid.any():
            continue
        all_x.append(x_arr[valid])
        all_y.append(y_arr[valid])
        err_y = err_x = None
        if yerr_by_group and group in yerr_by_group:
            err_y = dict(type='data', array=np.asarray(yerr_by_group[group], float)[valid],
                         visible=True, thickness=1, width=3,
                         color=colors.get(group, 'gray'))
        if xerr_by_group and group in xerr_by_group:
            err_x = dict(type='data', array=np.asarray(xerr_by_group[group], float)[valid],
                         visible=True, thickness=1, width=3,
                         color=colors.get(group, 'gray'))
        fig.add_trace(go.Scatter(
            x=x_arr[valid], y=y_arr[valid], mode='markers',
            marker=dict(color=colors.get(group, 'gray'), size=8, opacity=0.75),
            error_y=err_y, error_x=err_x,
            name=str(group), legendgroup=str(group),
        ))

    if all_x:
        cat_x, cat_y = np.concatenate(all_x), np.concatenate(all_y)
        span_max = max(cat_x.max(), cat_y.max()) * 1.05
        span_min = min(0.0, cat_x.min())
    else:
        span_min, span_max = 0.0, 1.0

    for group, c in fits.items():
        if c is None:
            continue
        xfit = np.array([span_min, span_max])
        fig.add_trace(go.Scatter(
            x=xfit, y=c['slope'] * xfit + c['intercept'], mode='lines',
            line=dict(color=colors.get(group, 'gray'), width=2),
            name=f'{group} fit', legendgroup=str(group), showlegend=False,
        ))

    if identity_line:
        fig.add_trace(go.Scatter(
            x=[span_min, span_max], y=[span_min, span_max], mode='lines',
            line=dict(color='rgba(0,0,0,0.45)', width=1.5, dash='dash'),
            name='1:1', showlegend=True,
        ))

    fig.update_layout(title=title, xaxis_title=x_title, yaxis_title=y_title,
                       template='plotly_white', height=480, hovermode='closest')
    return fig


def plot_raw_vs_corrected(raw, corrected, title, y_title):
    """Dual-trace before/after overlay for one series pair.

    Parameters
    ----------
    raw, corrected : pd.Series
        Same index (or overlapping); plotted over their shared span.
    title, y_title : str

    Returns
    -------
    go.Figure
    """
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=raw.index, y=raw.values, mode='lines',
                              line=dict(color='#8a8a8a', width=1.5), name='raw'))
    fig.add_trace(go.Scatter(x=corrected.index, y=corrected.values, mode='lines',
                              line=dict(color='#2b6cb0', width=1.5), name='corrected'))
    fig.update_layout(title=title, xaxis_title='Time (UTC)', yaxis_title=y_title,
                       template='plotly_white', height=420, hovermode='x unified')
    return fig


def plot_raw_corrected_vs_reference(reference, ref_label, pairs, colors, t0, t1, title, y_title):
    """Per-target panel overlaying a reference series with each target's raw + corrected trace.

    One stacked subplot row per target: the reference instrument (solid), the target's raw
    series (dashed, faded), and the target's corrected series (solid). All sliced to
    [t0, t1] at native resolution. Use to check that a correction actually brings a target
    onto the reference it was fit against.

    Parameters
    ----------
    reference : pd.Series
        The reference instrument's series (e.g. Ultra460 C2H6), shown in every panel.
    ref_label : str
        Legend/colors key for the reference.
    pairs : dict[str, tuple[pd.Series, pd.Series]]
        target_label -> (raw_series, corrected_series). One panel per entry.
    colors : dict[str, str]
        Maps ref_label and each target_label to a hex color.
    t0, t1 : timestamp-like
        Inclusive window bounds.
    title, y_title : str

    Returns
    -------
    go.Figure
    """
    rows = len(pairs)
    fig = make_subplots(rows=rows, cols=1, shared_xaxes=True,
                        subplot_titles=[f'{lbl} vs {ref_label}' for lbl in pairs])
    ref = reference[t0:t1]
    ref_color = colors.get(ref_label, '#888888')
    for r, (label, (raw, corrected)) in enumerate(pairs.items(), start=1):
        color = colors.get(label, 'gray')
        fig.add_trace(go.Scatter(x=ref.index, y=ref.values, mode='lines',
                                 line=dict(color=ref_color, width=1.5),
                                 name=ref_label, legendgroup=ref_label, showlegend=(r == 1)),
                      row=r, col=1)
        rw, cw = raw[t0:t1], corrected[t0:t1]
        fig.add_trace(go.Scatter(x=rw.index, y=rw.values, mode='lines',
                                 line=dict(color=color, width=1, dash='dot'), opacity=0.55,
                                 name=f'{label} raw', legendgroup=f'{label} raw'),
                      row=r, col=1)
        fig.add_trace(go.Scatter(x=cw.index, y=cw.values, mode='lines',
                                 line=dict(color=color, width=1.6),
                                 name=f'{label} corrected', legendgroup=f'{label} corrected'),
                      row=r, col=1)
        fig.update_yaxes(title_text=y_title, row=r, col=1)
    fig.update_xaxes(title_text='Time (UTC)', row=rows, col=1)
    fig.update_layout(title=title, template='plotly_white',
                      height=300 * rows + 90, hovermode='x unified')
    return fig


def plot_peaks_highlighted(series_dict, peak_times, colors, title, y_title):
    """Timeseries with detected-peak markers overlaid.

    Parameters
    ----------
    series_dict : dict[str, pd.Series | None]
    peak_times : array-like of Timestamp
        Peak locations (from the reference series) to mark on every trace.
    colors : dict[str, str]
    title, y_title : str

    Returns
    -------
    go.Figure
    """
    fig = go.Figure()
    peak_idx = pd.DatetimeIndex(peak_times)
    for inst, s in series_dict.items():
        if s is None or len(s) == 0:
            continue
        fig.add_trace(go.Scatter(
            x=s.index, y=s.values, mode='lines',
            line=dict(color=colors.get(inst, 'gray'), width=1.5), name=inst,
        ))
        near = s.reindex(peak_idx, method='nearest', tolerance=pd.Timedelta(seconds=10)).dropna()
        if len(near):
            fig.add_trace(go.Scatter(
                x=near.index, y=near.values, mode='markers',
                marker=dict(color=colors.get(inst, 'gray'), size=10, symbol='x',
                            line=dict(width=1, color='black')),
                name=f'{inst} peak', showlegend=False,
            ))
    fig.update_layout(title=title, xaxis_title='Time (UTC)', yaxis_title=y_title,
                       template='plotly_white', height=460, hovermode='x unified')
    return fig


def plot_residual_diagnostic(residual, diagnostic, label, color, corr_threshold=0.3):
    """Residual-vs-diagnostic-variable scatter, for spotting cross-channel interference.

    A real spectral cross-sensitivity would show residuals trending with the
    diagnostic variable's level, not just scattering around zero.

    Parameters
    ----------
    residual, diagnostic : pd.Series or array-like
        Same length/alignment; NaNs dropped pairwise.
    label : str
        Diagnostic variable name, used in the figure title/axis.
    color : str
        Hex color for the markers.
    corr_threshold : float
        |corr| above this is reported as flagged (caller decides how to act on it).

    Returns
    -------
    (fig, corr) : (go.Figure, float)
        corr is NaN if fewer than 6 valid pairs.
    """
    resid = pd.Series(np.asarray(residual, dtype=float))
    diag  = pd.Series(np.asarray(diagnostic, dtype=float))
    valid = resid.notna() & diag.notna()
    corr = float(np.corrcoef(diag[valid], resid[valid])[0, 1]) if valid.sum() > 5 else np.nan

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=diag[valid], y=resid[valid], mode='markers',
        marker=dict(color=color, size=7, opacity=0.7), name='residual',
    ))
    fig.add_hline(y=0, line=dict(color='rgba(0,0,0,0.45)', width=1, dash='dash'))
    flag = ' [POSSIBLE INTERFERENCE]' if np.isfinite(corr) and abs(corr) > corr_threshold else ''
    title = (f'Residual vs {label}  (corr={corr:+.3f}{flag})'
             if np.isfinite(corr) else f'Residual vs {label} (insufficient data)')
    fig.update_layout(title=title, xaxis_title=label, yaxis_title='fit residual',
                       template='plotly_white', height=380)
    return fig, corr


def plot_range_levels(levels, x_title, title):
    """Log-scale dumbbell plot comparing named concentration levels on one axis.

    Generic version of the "one tank point vs the range we must calibrate" figure --
    any list of (label, value, color) levels, not tied to a specific species/campaign.
    Pair with `assess_tank_coverage` for the numbers behind the picture.

    Parameters
    ----------
    levels : list[tuple[str, float, str]]
        (label, value, hex color) rows, one marker each, plotted top to bottom in the
        order given.
    x_title : str
        e.g. 'C2H6 (ppb, log scale)'.
    title : str

    Returns
    -------
    go.Figure
    """
    fig = go.Figure()
    for name, val, color in levels:
        fig.add_trace(go.Scatter(x=[val], y=[name], mode='markers+text',
                                 marker=dict(color=color, size=13),
                                 text=[f'  {val:,.2f}'], textposition='middle right',
                                 showlegend=False))
    fig.update_xaxes(type='log', title_text=x_title)
    fig.update_layout(title=title, template='plotly_white', height=340, margin=dict(l=230))
    return fig


# ── Calibration workflows (recommended entry points: fit + standard checks) ──────
#
# These two functions are the recommended starting point for a new campaign or a new
# species: pick "tank" or "reference", pass your data, get coefficients back with the
# standard diagnostic plots already shown. Both are thin orchestrators over the
# functions above — no new fitting math. Anything needing more control (shared
# multi-instrument stats, custom check sequencing) can drop to the lower-level
# functions directly, exactly as mobile-slv's own tank-CH4 fitting does.

def calibrate_and_check_tank(series_dict, tank, windows, species_key, colors, y_label,
                              title_prefix=''):
    """Tank-anchored calibration, fit + standard checks, for one or more instruments.

    1. `window_stats` + `fit_species` per instrument -> coefficients.
    2. 1:1 scatter (points = window means, ±1σ in-window-noise error bars, per-
       instrument fit line, dashed y=x) via `plot_calibration_scatter`.
    3. Sanity check: apply each fit back to its own window means and replot
       corrected-vs-truth (should collapse onto 1:1) via `plot_calibration_scatter`.

    Shows both plots and returns the coefficients. For anything needing the shared
    `stats_df` itself (e.g. re-fitting across several calibration dates for a drift
    check, or reusing the per-window std elsewhere), call `window_stats` + `fit_species`
    directly instead — same functions this wraps, just not hidden behind one call.

    Parameters
    ----------
    series_dict : dict[str, pd.Series]
        instrument -> full (or windowed) time series for this species.
    tank : dict
        Output of `parse_tank_details` (the `tank` return value).
    windows : list[dict]
        One calibration event's windows (e.g. `windows_by_date[date]`).
    species_key : str
        e.g. 'CH4_ppm' — must match a key in each tank entry.
    colors : dict[str, str]
        instrument -> hex color.
    y_label : str
        e.g. 'CH4 (ppm)'.
    title_prefix : str
        Prepended to both plot titles (e.g. 'Feb 12 ').

    Returns
    -------
    dict[str, dict | None]
        instrument -> coefficient dict (None if fewer than 2 usable tank points).
    """
    stats_df = window_stats(series_dict, windows)
    coefs = {inst: fit_species(stats_df, tank, inst, species_key) for inst in series_dict}

    xt = stats_df['tank_key'].map(lambda k: tank.get(k, {}).get(species_key)).astype(float)
    xg, yg, eg = {}, {}, {}
    for inst, c in coefs.items():
        col = f'{inst}_mean'
        if c is None or col not in stats_df.columns:
            continue
        xg[inst] = xt.values
        yg[inst] = stats_df[col].astype(float).values
        if f'{inst}_std' in stats_df.columns:
            eg[inst] = stats_df[f'{inst}_std'].astype(float).values
    fig_1to1 = plot_calibration_scatter(xg, yg, coefs, colors, x_title=f'Tank {y_label}',
            y_title=f'Instrument {y_label}',
            title=f'{title_prefix}1:1 view — points ±1σ in-window noise, per-instrument fit, 1:1 line',
            yerr_by_group=eg)
    fig_1to1.show()

    xg2, yg2 = {}, {}
    for inst, c in coefs.items():
        col = f'{inst}_mean'
        if c is None or col not in stats_df.columns:
            continue
        corrected = apply_linear(stats_df[col].astype(float), c)
        xg2[inst], yg2[inst] = xt.values, corrected.values
    fig_sanity = plot_calibration_scatter(xg2, yg2, {}, colors, x_title=f'Tank {y_label}',
            y_title=f'CORRECTED {y_label}',
            title=f'{title_prefix}sanity check — corrected readings vs tank (should lie on 1:1)')
    fig_sanity.show()

    return coefs


def calibrate_and_check_reference(ref_values, target_values, anchor='ols', z_ref=None,
                                   z_tgt=None, colors=None, x_label='', y_label='',
                                   title_prefix='', target_label='target', color=None,
                                   diagnostic_values=None, diagnostic_label=None,
                                   plot_sample_size=None, plot_seed=0):
    """Reference-instrument calibration, fit + standard checks, for one target.

    Call once per target instrument (the caller loops over targets — the pairing step,
    done beforehand via `pair_series_nearest` or `find_peak_matches`, usually differs
    per target: e.g. two targets covering different calendar-date ranges than the
    reference).

    1. `fit_reference_cal(anchor=...)` -> coefficient — **always fit on the full
       `ref_values`/`target_values`**, regardless of `plot_sample_size`.
    2. Matched-pairs scatter (fit line + dashed 1:1) via `plot_calibration_scatter` —
       subsampled for render weight if `plot_sample_size` is given (e.g. a continuous
       ambient-overlap fit with hundreds of thousands of points needs a lighter plot;
       the fit itself never sees the subsample).
    3. If `diagnostic_values` given (e.g. a second gas's matched values, for spotting
       cross-channel interference): `plot_residual_diagnostic`, same subsample — note the
       reported correlation is then computed on that subsample, not the full population.

    Shows the plot(s) and returns the coefficient. Deliberately does NOT include the
    native-resolution before/after timeseries check — that operates on full series
    plus a chosen time window, a different data shape than the matched-pairs arrays
    here. Call `apply_linear(series, coef)` + `plot_timeseries_panels` /
    `plot_raw_corrected_vs_reference` over your own window for that, same pattern
    mobile-slv's own notebook already uses.

    Parameters
    ----------
    ref_values, target_values : array-like
        Matched reference/target values (see `fit_reference_cal`).
    anchor : {'ols', 'pinned'}
        See `fit_reference_cal`.
    z_ref, z_tgt : float, required if anchor='pinned'
    colors : dict[str, str] or None
        Looked up by `target_label`; falls back to `color` or 'gray' if not found.
    x_label, y_label : str
        Axis titles (e.g. 'Ultra460 C2H6 peak (ppb)', 'Instrument C2H6 peak (ppb)').
    title_prefix : str
        Prepended to the scatter title.
    target_label : str
        Legend/color-lookup key for the target (e.g. the instrument name).
    color : str or None
        Direct color override if `colors` doesn't have `target_label`.
    diagnostic_values : array-like or None
        A second variable matched to the same pairs (e.g. another gas's concentration
        at each matched point) to check for cross-channel interference.
    diagnostic_label : str or None
        Required if `diagnostic_values` is given — used in the diagnostic plot's title.
    plot_sample_size : int or None
        If given and there are more than this many matched points, randomly subsample
        (fixed seed) to this many points for the plot(s) only — the fit always uses the
        full arrays. Use for huge continuous-overlap fits (hundreds of thousands of
        points) where plotting everything would be prohibitively heavy.
    plot_seed : int
        Seed for the subsample, for reproducible plots across re-runs.

    Returns
    -------
    dict or None
        Coefficient dict from `fit_reference_cal`.
    """
    coef = fit_reference_cal(ref_values, target_values, anchor=anchor, z_ref=z_ref, z_tgt=z_tgt)
    resolved_color = (colors or {}).get(target_label, color or 'gray')

    ref_arr = np.asarray(ref_values, dtype=float)
    tgt_arr = np.asarray(target_values, dtype=float)
    if plot_sample_size is not None and len(ref_arr) > plot_sample_size:
        idx = np.random.default_rng(plot_seed).choice(len(ref_arr), size=plot_sample_size, replace=False)
        plot_ref, plot_tgt = ref_arr[idx], tgt_arr[idx]
        diag_for_plot = np.asarray(diagnostic_values, dtype=float)[idx] if diagnostic_values is not None else None
    else:
        plot_ref, plot_tgt = ref_arr, tgt_arr
        diag_for_plot = diagnostic_values

    fig_fit = plot_calibration_scatter(
        {target_label: plot_ref}, {target_label: plot_tgt},
        {target_label: coef} if coef else {}, {target_label: resolved_color},
        x_title=x_label, y_title=y_label,
        title=f'{title_prefix}{target_label} vs reference — fit + 1:1 line')
    fig_fit.show()

    if diag_for_plot is not None and coef is not None:
        resid = plot_tgt - (coef['slope'] * plot_ref + coef['intercept'])
        fig_diag, _ = plot_residual_diagnostic(resid, diag_for_plot,
                label=diagnostic_label, color=resolved_color)
        fig_diag.update_layout(title=f'{target_label}: ' + fig_diag.layout.title.text)
        fig_diag.show()

    return coef
