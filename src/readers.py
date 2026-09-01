"""
Per-instrument file readers for Stage 02 (standardize).

Each reader returns a DataFrame with a tz-aware UTC TIMESTAMP index and all original
columns preserved (or parsed to clean names). The ts_status column is added by the
pipeline loop, not here.

Public API
----------
Readers:
  read_aeris_raw(path, rename)        — Aeris Raw and Eng .txt files (1-line header + CSV)
  read_picarro(path)                  — Picarro .dat files
  read_lgr(path)                      — LGR final .dat files
  read_sprinter(path)                 — Sprinter .csv files; nulls lat/lon on no-fix rows
  read_spectra(spectra_path, raw_dir) — headerless Aeris Spectra/Spectralite .txt files;
                                        recovers ts_source from the paired Raw file
  read_gps(path)                      — toughbook GPS .dat files (NMEA-encoded);
                                        index=toughbook epoch; epoch + gps_receiver_utc columns for clock correction
  read_anem(path)                     — toughbook Anemometer .dat files (Trisonica-encoded)
  make_spectra_reader(raw_dir)        — factory: returns spectra reader with raw_dir baked in

Rename dicts (partial — standardise cross-instrument science columns, keep all others):
  ULTRA321_RENAME, ULTRA321_ENG_RENAME  (Eng same as Raw — wet variants pass through unchanged)
  PICO017_RENAME,  PICO017_ENG_RENAME   (Eng same as Raw — wet variants pass through unchanged)
  ULTRA460_RENAME, ULTRA460_ENG_RENAME
  PICARRO_RENAME, LGR_RENAME, SPRINTER_RENAME

Instrument task registry:
  INSTRUMENT_TASKS  — {instrument_name: [task_spec, ...]}
    Keys match config/instruments.yaml and paths.STAGE_02_SOURCES.
    task_spec keys: glob, out_subdir, ts_status, and either:
      reader             — direct callable(path) -> DataFrame | None
      spectra_raw_subdir — pipeline calls make_spectra_reader(src_dir / this)
"""

import re
import pandas as pd
from pathlib import Path

AERIS_TS_FORMAT = "%m/%d/%Y %H:%M:%S.%f"
_N_SPECTRA_DIAGNOSTIC = 2

# ── Column rename maps ────────────────────────────────────────────────────────

ULTRA321_RENAME = {
    "P (mbars)":  "P_mbar",
    "Tgas(degC)": "Tgas_C",
    "CH4 (ppm)":  "CH4_ppm",
    "H2O (ppm)":  "H2O_ppm",
    "C2H6 (ppm)": "C2H6_ppm",
    "C3H8 (ppm)": "C3H8_ppm",
}
ULTRA321_ENG_RENAME = {**ULTRA321_RENAME}

PICO017_RENAME = {
    "P (mbars)":  "P_mbar",
    "Tgas(degC)": "Tgas_C",
    "CH4 (ppm)":  "CH4_ppm",
    "H2O (ppm)":  "H2O_ppm",
    "C2H6 (ppb)": "C2H6_ppb",
    "C2/C1":      "C2C1",
}
PICO017_ENG_RENAME = {**PICO017_RENAME}

ULTRA460_RENAME = {
    "P (mbars)":  "P_mbar",
    "Tgas(degC)": "Tgas_C",
    "CH4 (ppm)":  "CH4_ppm",
    "H2O (ppm)":  "H2O_ppm",
    "C2H6 (ppb)": "C2H6_ppb",
    "C2/C1":      "C2C1",
}
ULTRA460_ENG_RENAME = {**ULTRA460_RENAME}

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
    "Latitude (DD.ddd +N)":      "lat_deg",
    "Longitude (DDD.ddd -W)":    "lon_deg",
    "Altitude (m)":              "altitude_m",
    "Air Temperature (C)":       "temp_C",
    "RH(%)":                     "RH_pct",
    "Dew Point (C)":             "dew_pt_C",
    "Wind Direction (Deg True)": "wind_dir_true",
    "Wind Speed (m/s)":          "wind_spd_ms",
    "Pressure (bar)":            "pressure_bar",
    "Heading(deg)":              "heading_deg",
    "GPSCorWindDirTrue (deg)":   "gps_wind_dir_true",
    "GPSCorWindSpeed (m/s)":     "gps_wind_spd_ms",
}


# ── Internal helpers ──────────────────────────────────────────────────────────

def _finalize(df, ts_series, rename):
    """Set TIMESTAMP index, apply partial column rename, return all columns."""
    df = df.copy()
    df["TIMESTAMP"] = ts_series
    df = df.dropna(subset=["TIMESTAMP"]).set_index("TIMESTAMP")
    df.index = df.index.tz_localize("UTC")
    found = {k: v for k, v in rename.items() if k in df.columns}
    if not found:
        raise ValueError(f"No expected columns found. Got: {list(df.columns)[:10]}")
    return df.rename(columns=found)


def _sprinter_ts(df):
    """Parse Sprinter's split UTC timestamp columns. UTC hhmmss packed as HHMMSS.ss."""
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
    """Read instrument column names from the first Raw .txt file in raw_dir."""
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


def _ts_source_from_paired_raw(spectra_path, raw_dir) -> dict | None:
    """
    Map {corrected Time Stamp string -> ts_source} from the Raw file paired with a
    Spectra file, or None if unavailable.

    Spectra files are headerless and positional — `read_spectra` derives the spectral
    channel count from the total column width — so Stage 01 cannot write a ts_source
    column into them without mislabelling every channel. It is recovered here instead.
    This is exact rather than approximate because Raw, Eng and Spectra share an
    identical timestamp sequence (verified 86,731/86,731 exact string match, same
    order) and corrected timestamps are unique within a file.

    Returns None for instruments that never pass through Stage 01 (e.g. Ultra460
    Spectralite), whose Raw files carry no ts_source column.
    """
    stem = Path(spectra_path).stem
    for suffix in ("spectra", "spectralite"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    raw_dir = Path(raw_dir)
    for candidate in (raw_dir / f"{stem}.txt", raw_dir / "no_coverage" / f"{stem}.txt"):
        if not candidate.exists():
            continue
        try:
            paired = pd.read_csv(candidate, usecols=["Time Stamp", "ts_source"], dtype=str)
        except (ValueError, KeyError):
            return None          # no ts_source column — not a Stage 01 instrument
        except Exception:
            return None
        return dict(zip(paired["Time Stamp"].str.strip(), paired["ts_source"]))
    return None


def _epoch_to_utc_index(epoch_series) -> pd.DatetimeIndex:
    return pd.to_datetime(epoch_series, unit="s", utc=True)


def _toughbook_data_lines(path):
    """
    Yield (epoch_float, rest_of_line) for every data line in a toughbook .dat file.
    Header and comment lines (not starting with a digit) are skipped.
    """
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or not line[0].isdigit():
                continue
            sep = line.find(",")
            if sep == -1:
                continue
            try:
                epoch = float(line[:sep])
            except ValueError:
                continue
            yield epoch, line[sep + 1:]


def _nmea_lat(val, hemi):
    if not val:
        return float("nan")
    v = float(val)
    d = int(v / 100)
    deg = d + (v - d * 100) / 60
    return -deg if hemi in ("S", "s") else deg


def _nmea_lon(val, hemi):
    if not val:
        return float("nan")
    v = float(val)
    d = int(v / 100)
    deg = d + (v - d * 100) / 60
    return -deg if hemi in ("W", "w") else deg


_ANEM_RE = re.compile(
    r"S\s+([\d.]+)\s+D\s+(\d+)\s+U\s+([-\d.]+)\s+V\s+([-\d.]+)\s+W\s+([-\d.]+)\s+"
    r"T\s+([-\d.]+)\s+H\s+([\d.]+)\s+DP\s+([-\d.]+)\s+P\s+([\d.]+)\s+"
    r"AD\s+([\d.]+)\s+PI\s+([-\d.]+)\s+RO\s+([-\d.]+)\s+MD\s+(\d+)"
)


# ── Public reader functions ───────────────────────────────────────────────────

def read_aeris_raw(path, rename: dict) -> pd.DataFrame:
    """Aeris Raw or Eng .txt file (1-line header + CSV). All columns preserved."""
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    ts = pd.to_datetime(df["Time Stamp"].str.strip(), format=AERIS_TS_FORMAT, errors="coerce")
    # Aeris appends a few startup rows from the next session to the still-open file
    # before creating a new one. Drop them only when the tail is ≤ 10 rows — a larger
    # tail suggests a real gap in the session that should be visible in the survey.
    _diffs = ts.dropna().diff()
    _big = _diffs[_diffs > pd.Timedelta("30min")]
    if not _big.empty:
        _cut = _big.index[0]
        if len(df) - _cut <= 10:
            df = df.loc[: _cut - 1]
            ts = ts.loc[: _cut - 1]
    return _finalize(df, ts, rename)


def read_picarro(path) -> pd.DataFrame:
    """Picarro .dat file (space-delimited, 1-line header). Timestamps already UTC."""
    df = pd.read_csv(path, sep=r"\s+", engine="python")
    ts = pd.to_datetime(
        df["DATE"].str.strip() + " " + df["TIME"].str.strip(),
        format="%Y-%m-%d %H:%M:%S.%f", errors="coerce",
    )
    return _finalize(df, ts, PICARRO_RENAME)


def read_lgr(path) -> pd.DataFrame | None:
    """LGR final .dat file. Cal columns are NA; raw values used."""
    df = pd.read_csv(path, na_values=["NA"])
    df.columns = df.columns.str.strip()
    if df.empty or "Time_UTC" not in df.columns:
        return None
    ts = pd.to_datetime(df["Time_UTC"].str.strip(), errors="coerce")
    result = _finalize(df, ts, LGR_RENAME)
    return result if not result.empty else None


def read_spectra(spectra_path, raw_dir) -> pd.DataFrame | None:
    """
    Headerless Aeris Spectra/Spectralite .txt file.
    Column names derived from paired Raw files in raw_dir.
    """
    if spectra_path.stat().st_size == 0:
        return None
    instrument_cols = _instrument_cols_from_raw(raw_dir)
    df = pd.read_csv(spectra_path, header=None, dtype={0: str}, on_bad_lines="skip")
    if df.empty:
        return None
    if str(df.iloc[0, 0]).strip()[:1].isalpha():
        df = df.iloc[1:].reset_index(drop=True)
    if df.empty:
        return None
    n_spec = df.shape[1] - len(instrument_cols) - _N_SPECTRA_DIAGNOSTIC
    if n_spec < 1:
        raise ValueError(f"Too few columns ({df.shape[1]}) for {spectra_path.name}")
    df.columns = instrument_cols + ["rd0", "rd1"] + [f"spec_{i:04d}" for i in range(1, n_spec + 1)]
    df = df.copy()
    ts_str = df["Time Stamp"].str.strip()
    ts = pd.to_datetime(ts_str, format=AERIS_TS_FORMAT, errors="coerce")
    valid = ts.notna()
    df = df[valid].copy()
    source_map = _ts_source_from_paired_raw(spectra_path, raw_dir)
    if source_map is not None:
        df["ts_source"] = ts_str[valid].map(source_map).fillna("unpaired")
    df.index = ts[valid].dt.tz_localize("UTC")
    df.index.name = "TIMESTAMP"
    df = df.drop(columns=["Time Stamp"])
    return df if not df.empty else None


def make_spectra_reader(raw_dir):
    """Return a reader closure for spectra files paired with raw_dir."""
    return lambda p: read_spectra(p, raw_dir)


def read_sprinter(path) -> pd.DataFrame | None:
    """
    Sprinter CSV (3 junk rows, then header, then data).

    Rows with no GPS fix (`GPS Quality == 0`, the NMEA invalid-fix code) are written by
    the logger with `lat_deg`/`lon_deg` of exactly 0.0 rather than a null, which is a real
    coordinate in the Gulf of Guinea — a spatial join would silently place those samples
    11,000 km away. They are masked to NaN here. The logger already nulls `altitude_m` on
    those rows, so this only makes latitude and longitude behave the way altitude already
    does. Met channels (`temp_C`, `RH_pct`, `pressure_bar`) stay untouched — they come off
    separate sensors and are still valid without a fix.
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
    if "GPS Quality" in result.columns:
        no_fix = pd.to_numeric(result["GPS Quality"], errors="coerce") == 0
        result.loc[no_fix, ["lat_deg", "lon_deg"]] = pd.NA
    return result if not result.empty else None


def read_gps(path) -> pd.DataFrame | None:
    """
    Toughbook GPS .dat file. Parses NMEA sentences into navigation and quality columns.

    Index:   TIMESTAMP — toughbook epoch as UTC DatetimeIndex (tz-aware).
    Columns (in order):
      epoch             — raw toughbook float epoch; subtract gps_receiver_utc to get clock correction
      gps_receiver_utc  — GPS satellite UTC time from GPRMC (tz-aware Timestamp; true UTC)
      lat_deg, lon_deg, altitude_m, geoid_sep_m
      speed_ms, course_true_deg, mag_course_deg
      fix_quality, n_sats, hdop          (from GPGGA; 10 Hz)
      fix_type, pdop, vdop               (from GPGSA;  1 Hz — merged within 1.5 s)
    """
    gprmc_rows: list[dict] = []
    gpgga_rows: list[dict] = []
    gpvtg_rows: list[dict] = []
    gpgsa_rows: list[dict] = []

    for epoch, sentence in _toughbook_data_lines(path):
        fields = sentence.split(",")
        msg = fields[0].lstrip("$")

        if msg == "GPRMC" and len(fields) >= 10 and fields[2] == "A":
            try:
                t_str, d_str = fields[1], fields[9]
                hh, mm = int(t_str[:2]), int(t_str[2:4])
                ss_f = float(t_str[4:])
                ss_i = int(ss_f)
                us = int((ss_f - ss_i) * 1_000_000)
                dd, mo, yy = int(d_str[:2]), int(d_str[2:4]), int(d_str[4:6]) + 2000
                gps_utc_s = pd.Timestamp(
                    year=yy, month=mo, day=dd,
                    hour=hh, minute=mm, second=ss_i, microsecond=us, tz="UTC",
                ).timestamp()
                gprmc_rows.append({
                    "epoch":            epoch,
                    "gps_utc_s":        gps_utc_s,
                    "lat_deg":          _nmea_lat(fields[3], fields[4]),
                    "lon_deg":          _nmea_lon(fields[5], fields[6]),
                    "speed_ms":         round(float(fields[7]) * 0.51444, 4) if fields[7] else float("nan"),
                    "course_true_deg":  float(fields[8]) if fields[8] else float("nan"),
                })
            except (ValueError, IndexError):
                continue

        elif msg == "GPGGA" and len(fields) >= 12:
            try:
                gpgga_rows.append({
                    "epoch":       epoch,
                    "fix_quality": float(fields[6]) if fields[6] else float("nan"),
                    "n_sats":      float(fields[7]) if fields[7] else float("nan"),
                    "hdop":        float(fields[8]) if fields[8] else float("nan"),
                    "altitude_m":  float(fields[9]) if fields[9] else float("nan"),
                    "geoid_sep_m": float(fields[11]) if fields[11] else float("nan"),
                })
            except (ValueError, IndexError):
                continue

        elif msg == "GPVTG" and len(fields) >= 4:
            try:
                gpvtg_rows.append({
                    "epoch":          epoch,
                    "mag_course_deg": float(fields[3]) if fields[3] else float("nan"),
                })
            except (ValueError, IndexError):
                continue

        elif msg == "GPGSA" and len(fields) >= 18:
            try:
                gpgsa_rows.append({
                    "epoch":    epoch,
                    "fix_type": float(fields[2]) if fields[2] else float("nan"),
                    "pdop":     float(fields[15]) if fields[15] else float("nan"),
                    "vdop":     float(fields[17].split("*")[0]) if fields[17] else float("nan"),
                })
            except (ValueError, IndexError):
                continue

    if not gprmc_rows:
        return None

    df = pd.DataFrame(gprmc_rows).set_index("epoch").sort_index()
    for extra_rows, tol in [(gpgga_rows, 0.5), (gpvtg_rows, 0.5), (gpgsa_rows, 1.5)]:
        if extra_rows:
            other = pd.DataFrame(extra_rows).set_index("epoch").sort_index()
            df = pd.merge_asof(df, other, left_index=True, right_index=True, tolerance=tol)

    df.insert(0, "epoch", df.index.values)
    df["gps_receiver_utc"] = pd.to_datetime(df["gps_utc_s"], unit="s", utc=True)
    df = df.drop(columns=["gps_utc_s"])
    df.index = _epoch_to_utc_index(df["epoch"])
    df.index.name = "TIMESTAMP"
    return df if not df.empty else None


def read_anem(path) -> pd.DataFrame | None:
    """
    Toughbook Trisonica anemometer .dat file. Parses the key=value encoded
    strings into clean columns. Indexed on toughbook epoch timestamp (UTC).
    """
    rows: list[dict] = []
    for epoch, encoded in _toughbook_data_lines(path):
        m = _ANEM_RE.search(encoded)
        if m is None:
            continue
        spd, dir_, u, v, w, temp, hum, dp, pres, ad, pitch, roll, md = m.groups()
        rows.append({
            "epoch":         epoch,
            "wind_spd_ms":   float(spd),
            "wind_dir_deg":  float(dir_),
            "u_ms":          float(u),
            "v_ms":          float(v),
            "w_ms":          float(w),
            "temp_C":        float(temp),
            "RH_pct":        float(hum),
            "dew_pt_C":      float(dp),
            "pressure_mbar": float(pres),
            "AD":            float(ad),
            "pitch_deg":     float(pitch),
            "roll_deg":      float(roll),
            "mag_dir_deg":   float(md),
        })
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df.index = _epoch_to_utc_index(df["epoch"])
    df.index.name = "TIMESTAMP"
    df = df.drop(columns=["epoch"])
    return df if not df.empty else None


# ── Instrument task registry ──────────────────────────────────────────────────
# Keys match instrument names in config/instruments.yaml and paths.STAGE_02_SOURCES.
# Instruments in STAGE_02_SOURCES with no entry here are treated as stubs by
# pipeline/02_standardize.py and logged as skipped in the run manifest.

def _t(glob, out_subdir="", ts_status="trusted", reader=None, spectra_raw_subdir=None):
    t = {"glob": glob, "out_subdir": out_subdir, "ts_status": ts_status}
    if reader is not None:
        t["reader"] = reader
    if spectra_raw_subdir is not None:
        t["spectra_raw_subdir"] = spectra_raw_subdir
    return t


_u321r = lambda p: read_aeris_raw(p, ULTRA321_RENAME)
_u321e = lambda p: read_aeris_raw(p, ULTRA321_ENG_RENAME)
_p017r = lambda p: read_aeris_raw(p, PICO017_RENAME)
_p017e = lambda p: read_aeris_raw(p, PICO017_ENG_RENAME)
_u460r = lambda p: read_aeris_raw(p, ULTRA460_RENAME)
_u460e = lambda p: read_aeris_raw(p, ULTRA460_ENG_RENAME)

INSTRUMENT_TASKS = {
    "LANL_aerisultra321": [
        _t("Raw/*.txt",                 reader=_u321r, out_subdir="Raw",                ts_status="utc_corrected"),
        _t("Raw/no_coverage/*.txt",     reader=_u321r, out_subdir="Raw/no_coverage",    ts_status="no_coverage"),
        _t("Eng/*.txt",                 reader=_u321e, out_subdir="Eng",                ts_status="utc_corrected"),
        _t("Eng/no_coverage/*.txt",     reader=_u321e, out_subdir="Eng/no_coverage",    ts_status="no_coverage"),
        _t("Spectra/*.txt",             spectra_raw_subdir="Raw", out_subdir="Spectra",                ts_status="utc_corrected"),
        _t("Spectra/no_coverage/*.txt", spectra_raw_subdir="Raw", out_subdir="Spectra/no_coverage",    ts_status="no_coverage"),
    ],
    "LANL_aerispico017": [
        _t("Raw/*.txt",                 reader=_p017r, out_subdir="Raw",                ts_status="utc_corrected"),
        _t("Raw/no_coverage/*.txt",     reader=_p017r, out_subdir="Raw/no_coverage",    ts_status="no_coverage"),
        _t("Eng/*.txt",                 reader=_p017e, out_subdir="Eng",                ts_status="utc_corrected"),
        _t("Eng/no_coverage/*.txt",     reader=_p017e, out_subdir="Eng/no_coverage",    ts_status="no_coverage"),
        _t("Spectra/*.txt",             spectra_raw_subdir="Raw", out_subdir="Spectra",                ts_status="utc_corrected"),
        _t("Spectra/no_coverage/*.txt", spectra_raw_subdir="Raw", out_subdir="Spectra/no_coverage",    ts_status="no_coverage"),
    ],
    "WYO_aerisultra460": [
        _t("Raw/*.txt",         reader=_u460r, out_subdir="Raw"),
        _t("Eng/*.txt",         reader=_u460e, out_subdir="Eng"),
        _t("Spectralite/*.txt", spectra_raw_subdir="Raw", out_subdir="Spectralite"),
    ],
    "WYO_picarro": [
        _t("*.dat", reader=read_picarro),
    ],
    "UOU_LGR": [
        _t("*.dat", reader=read_lgr),
    ],
    "WYO_sprinter": [
        _t("*.csv", reader=read_sprinter),
    ],
    "LANL_GPS": [
        _t("*.dat", reader=read_gps),
    ],
    "LANL_Anem": [
        _t("*.dat", reader=read_anem),
    ],
}
