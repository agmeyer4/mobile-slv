"""
Per-instrument file reader functions for Stage 02 (standardize).

Each reader returns a DataFrame with a tz-naive UTC TIMESTAMP index and clean
column names, or None for empty/unparseable files. These functions are pure —
no config dependency.

Spectra files are not processed here. Raw gas/met files only.
"""

import pandas as pd

AERIS_TS_FORMAT = "%m/%d/%Y %H:%M:%S.%f"

# ── Column rename maps ────────────────────────────────────────────────────────

ULTRA321_RENAME = {
    "P (mbars)":  "P_mbar",
    "Tgas(degC)": "Tgas_C",
    "CH4 (ppm)":  "CH4_ppm",
    "H2O (ppm)":  "H2O_ppm",
    "C2H6 (ppm)": "C2H6_ppm",
    "C3H8 (ppm)": "C3H8_ppm",
}

PICO017_RENAME = {
    "P (mbars)":  "P_mbar",
    "Tgas(degC)": "Tgas_C",
    "CH4 (ppm)":  "CH4_ppm",
    "H2O (ppm)":  "H2O_ppm",
    "C2H6 (ppb)": "C2H6_ppb",
    "R":          "R",
    "C2/C1":      "C2C1",
}

ULTRA460_RENAME = {
    "P (mbars)":  "P_mbar",
    "Tgas(degC)": "Tgas_C",
    "CH4 (ppm)":  "CH4_ppm",
    "H2O (ppm)":  "H2O_ppm",
    "C2H6 (ppb)": "C2H6_ppb",
    "R":          "R",
    "C2/C1":      "C2C1",
}

PICARRO_RENAME = {
    "CO_sync":      "CO_ppm",
    "CO2_dry_sync": "CO2_ppm",
    "CH4_dry_sync": "CH4_ppm",
    "H2O_sync":     "H2O_ppm",
}

LGR_RENAME = {
    "[CH4]_ppm": "CH4_ppm",
    "[H2O]_ppm": "H2O_ppm",
    "[CO2]_ppm": "CO2_ppm",
}

SPRINTER_RENAME = {
    "Latitude (DD.ddd +N)":     "lat_deg",
    "Longitude (DDD.ddd -W)":   "lon_deg",
    "Altitude (m)":             "altitude_m",
    "Air Temperature (C)":      "temp_C",
    "RH(%)":                    "RH_pct",
    "Dew Point (C)":            "dew_pt_C",
    "Wind Direction (Deg True)": "wind_dir_true",
    "Wind Speed (m/s)":         "wind_spd_ms",
    "Pressure (bar)":           "pressure_bar",
    "Heading(deg)":             "heading_deg",
    "GPSCorWindDirTrue (deg)":  "gps_wind_dir_true",
    "GPSCorWindSpeed (m/s)":    "gps_wind_spd_ms",
}


# ── Internal helpers ──────────────────────────────────────────────────────────

def _finalize(df, ts_series, rename):
    df = df.copy()
    df["TIMESTAMP"] = ts_series
    df = df.dropna(subset=["TIMESTAMP"]).set_index("TIMESTAMP")
    keep = {k: v for k, v in rename.items() if k in df.columns}
    if not keep:
        raise ValueError(f"No expected columns found. Got: {list(df.columns)[:10]}")
    return df[list(keep.keys())].rename(columns=keep)


def _sprinter_ts(df):
    """
    Parse Sprinter's split UTC timestamp columns to tz-naive UTC.
    UTC hhmmss is packed as HHMMSS.ss (e.g. 163446.70 = 16:34:46.70).
    """
    hhmmss = pd.to_numeric(df["UTC hhmmss"], errors="coerce")
    year   = pd.to_numeric(df["UTC Year"],   errors="coerce")
    month  = pd.to_numeric(df["UTC Month"],  errors="coerce")
    day    = pd.to_numeric(df["UTC Day"],    errors="coerce")
    valid  = year.notna() & month.notna() & day.notna() & hhmmss.notna()
    out    = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    if not valid.any():
        return out
    h    = hhmmss[valid]
    hh   = (h // 10000).astype(int)
    mm   = ((h % 10000) // 100).astype(int)
    ss_f = h % 100
    ss   = ss_f.astype(int)
    us   = ((ss_f - ss) * 1_000_000).round().astype(int)
    out[valid] = pd.to_datetime(
        dict(year=year[valid].astype(int), month=month[valid].astype(int),
             day=day[valid].astype(int), hour=hh, minute=mm, second=ss, microsecond=us),
        errors="coerce",
    )
    return out


# ── Public reader functions ───────────────────────────────────────────────────

def read_aeris_raw(path, rename: dict) -> pd.DataFrame:
    """Read an Aeris Raw .txt file (1-line header + CSV)."""
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    ts = pd.to_datetime(df["Time Stamp"].str.strip(), format=AERIS_TS_FORMAT, errors="coerce")
    return _finalize(df, ts, rename)


def read_picarro(path) -> pd.DataFrame:
    """Read a Picarro .dat file (space-delimited, 1-line header). Timestamps are already UTC."""
    df = pd.read_csv(path, sep=r"\s+", engine="python")
    ts = pd.to_datetime(
        df["DATE"].str.strip() + " " + df["TIME"].str.strip(),
        format="%Y-%m-%d %H:%M:%S.%f", errors="coerce",
    )
    return _finalize(df, ts, PICARRO_RENAME)


def read_lgr(path) -> pd.DataFrame | None:
    """
    Read an LGR .txt file. Line 1 is an instrument-info string; line 2 is the
    CSV header. Returns None for empty files (LGR writes zero-byte files on boot).
    """
    df = pd.read_csv(path, skiprows=1, on_bad_lines="skip")
    df.columns = df.columns.str.strip()
    if df.empty or "Time" not in df.columns:
        return None
    ts = pd.to_datetime(df["Time"].str.strip(), format=AERIS_TS_FORMAT, errors="coerce")
    result = _finalize(df, ts, LGR_RENAME)
    return result if not result.empty else None


def read_sprinter(path) -> pd.DataFrame | None:
    """
    Read a Sprinter CSV (3 junk rows, then header, then data).
    Data rows have one trailing extra field vs the header — trimmed on read.
    """
    _SKIP = 3
    with open(path) as fh:
        for _ in range(_SKIP):
            fh.readline()
        col_names = [c.strip() for c in fh.readline().strip().split(",")]
    df = pd.read_csv(path, skiprows=_SKIP + 1, header=None, engine="python",
                     on_bad_lines="skip", na_values=["nan", "NaN", "NA"])
    if df.empty:
        return None
    df = df.iloc[:, :len(col_names)].copy()
    df.columns = col_names
    result = _finalize(df, _sprinter_ts(df), SPRINTER_RENAME)
    return result if not result.empty else None
