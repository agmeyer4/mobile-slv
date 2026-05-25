"""
Per-instrument file reader functions for Stage 02 (standardize).

Each reader returns a DataFrame with a tz-aware UTC TIMESTAMP index and all original
columns preserved. A partial rename is applied: science columns that are compared
cross-instrument get clean names (e.g. CH4_ppm); everything else keeps its original
name. The ts_status column ("utc_corrected" | "no_coverage" | "trusted") is added
by the pipeline loop, not here.

Public API
----------
Readers:
  read_aeris_raw(path, rename)   — Aeris Raw and Eng .txt files (1-line header + CSV)
  read_picarro(path)             — Picarro .dat files
  read_lgr(path)                 — LGR final .dat files
  read_sprinter(path)            — Sprinter .csv files
  read_spectra(spectra_path, raw_dir) — headerless Aeris Spectra/Spectralite .txt files

Rename dicts (partial — apply known cross-instrument science names, keep all other cols):
  ULTRA321_RENAME      — Raw gas/met columns for Ultra321
  ULTRA321_ENG_RENAME  — Eng gas/met + wet columns for Ultra321
  PICO017_RENAME       — Raw gas/met columns for Pico017
  PICO017_ENG_RENAME   — Eng gas/met + wet columns for Pico017
  ULTRA460_RENAME      — Raw gas/met columns for Ultra460
  ULTRA460_ENG_RENAME  — Eng gas/met columns for Ultra460
  PICARRO_RENAME       — Picarro gas columns
  LGR_RENAME           — LGR raw gas columns
  SPRINTER_RENAME      — Sprinter GPS/met columns
"""

import pandas as pd
from pathlib import Path

AERIS_TS_FORMAT = "%m/%d/%Y %H:%M:%S.%f"

# Number of diagnostic columns (rd0, rd1) between instrument params and spectral channels
_N_SPECTRA_DIAGNOSTIC = 2

# ── Column rename maps ────────────────────────────────────────────────────────
# These are PARTIAL renames — only science columns that analysis compares
# cross-instrument are standardised. All other columns pass through with their
# original names. Duplicate column handling: pandas auto-mangles duplicates to
# col, col.1, col.2 on read; those mangled names are not in these maps and pass
# through unchanged.

ULTRA321_RENAME = {
    "P (mbars)":  "P_mbar",
    "Tgas(degC)": "Tgas_C",
    "CH4 (ppm)":  "CH4_ppm",
    "H2O (ppm)":  "H2O_ppm",
    "C2H6 (ppm)": "C2H6_ppm",
    "C3H8 (ppm)": "C3H8_ppm",
}

ULTRA321_ENG_RENAME = {
    **ULTRA321_RENAME,
    "CH4 (ppm)-Wet":  "CH4_ppm_wet",
    "C2H6 (ppm)-Wet": "C2H6_ppm_wet",
    "C3H8 (ppm)-Wet": "C3H8_ppm_wet",
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

PICO017_ENG_RENAME = {
    **PICO017_RENAME,
    "CH4 (ppm)-Wet":  "CH4_ppm_wet",
    "C2H6 (ppb)-Wet": "C2H6_ppb_wet",
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

ULTRA460_ENG_RENAME = {
    **ULTRA460_RENAME,
}

PICARRO_RENAME = {
    "CO_sync":      "CO_ppm",
    "CO2_dry_sync": "CO2_ppm",
    "CH4_dry_sync": "CH4_ppm",
    "H2O_sync":     "H2O_ppm",
}

LGR_RENAME = {
    "CH4d_ppm_raw": "CH4_ppm",
    "H2O_ppm":      "H2O_ppm",
    "CO2d_ppm_raw": "CO2_ppm",
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
    """
    Set the TIMESTAMP index, apply partial column renames, return all columns.
    Raises ValueError if none of the expected science columns are present
    (guards against misidentified files).
    """
    df = df.copy()
    df["TIMESTAMP"] = ts_series
    df = df.dropna(subset=["TIMESTAMP"]).set_index("TIMESTAMP")
    df.index = df.index.tz_localize("UTC")
    found = {k: v for k, v in rename.items() if k in df.columns}
    if not found:
        raise ValueError(f"No expected columns found. Got: {list(df.columns)[:10]}")
    return df.rename(columns=found)


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


def _instrument_cols_from_raw(raw_dir) -> list[str]:
    """
    Read instrument column names from the header of the first Raw .txt file in raw_dir.
    Returns columns from 'Time Stamp' through 'Tgas(degC)' — the prefix shared by
    all Aeris Spectra files for the same instrument.
    """
    raw_files = sorted(p for p in Path(raw_dir).iterdir() if p.suffix == ".txt" and p.is_file())
    if not raw_files:
        raise FileNotFoundError(f"No .txt files in {raw_dir}")
    with open(raw_files[0]) as fh:
        cols = [c.strip() for c in fh.readline().strip().split(",")]
    try:
        tgas_idx = next(i for i, c in enumerate(cols) if "tgas" in c.lower())
    except StopIteration:
        raise ValueError(f"No 'Tgas' column in {raw_files[0].name}")
    return cols[: tgas_idx + 1]


# ── Public reader functions ───────────────────────────────────────────────────

def read_aeris_raw(path, rename: dict) -> pd.DataFrame:
    """
    Read an Aeris Raw or Eng .txt file (1-line header + CSV).
    All columns are preserved; rename maps the science columns to clean names.
    Duplicate column names (e.g. two GPS Time columns in Ultra321 Eng) are
    auto-mangled by pandas to col, col.1, etc.
    """
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
    Read an LGR final .dat file (1-line header + CSV).
    Timestamp column is Time_UTC in ISO format. Cal columns are NA; raw values are used.
    """
    df = pd.read_csv(path, na_values=["NA"])
    df.columns = df.columns.str.strip()
    if df.empty or "Time_UTC" not in df.columns:
        return None
    ts = pd.to_datetime(df["Time_UTC"].str.strip(), errors="coerce")
    result = _finalize(df, ts, LGR_RENAME)
    return result if not result.empty else None


def read_spectra(spectra_path, raw_dir) -> pd.DataFrame | None:
    """
    Read a headerless Aeris Spectra/Spectralite .txt file.
    Column names are derived from paired Raw files in raw_dir. The 'Time Stamp'
    column is parsed to a UTC TIMESTAMP index identical to the gas/met readers.
    Returns None for 0-byte files (instrument wrote nothing).
    """
    if spectra_path.stat().st_size == 0:
        return None
    instrument_cols = _instrument_cols_from_raw(raw_dir)
    df = pd.read_csv(spectra_path, header=None, dtype={0: str}, on_bad_lines="skip")
    if df.empty:
        return None

    # Drop an existing header row if the file was previously headed
    if str(df.iloc[0, 0]).strip()[:1].isalpha():
        df = df.iloc[1:].reset_index(drop=True)
    if df.empty:
        return None

    n_total = df.shape[1]
    n_spec = n_total - len(instrument_cols) - _N_SPECTRA_DIAGNOSTIC
    if n_spec < 1:
        raise ValueError(f"Too few columns ({n_total}) for {spectra_path.name}")

    col_names = instrument_cols + ["rd0", "rd1"] + [f"spec_{i:04d}" for i in range(1, n_spec + 1)]
    df.columns = col_names
    # Consolidate fragmented memory blocks from mixed-dtype read to avoid PerformanceWarning
    df = df.copy()

    ts = pd.to_datetime(df["Time Stamp"].str.strip(), format=AERIS_TS_FORMAT, errors="coerce")
    valid = ts.notna()
    df = df[valid].copy()
    df.index = ts[valid].dt.tz_localize("UTC")
    df.index.name = "TIMESTAMP"
    df = df.drop(columns=["Time Stamp"])
    return df if not df.empty else None


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
