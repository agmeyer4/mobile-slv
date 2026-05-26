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
  read_sprinter(path)                 — Sprinter .csv files
  read_spectra(spectra_path, raw_dir) — headerless Aeris Spectra/Spectralite .txt files
  read_gps(path)                      — toughbook GPS .dat files (NMEA-encoded)
  read_anem(path)                     — toughbook Anemometer .dat files (Trisonica-encoded)
  make_spectra_reader(raw_dir)        — factory: returns spectra reader with raw_dir baked in

Rename dicts (partial — standardise cross-instrument science columns, keep all others):
  ULTRA321_RENAME, ULTRA321_ENG_RENAME
  PICO017_RENAME,  PICO017_ENG_RENAME
  ULTRA460_RENAME, ULTRA460_ENG_RENAME
  PICARRO_RENAME, LGR_RENAME, SPRINTER_RENAME
  Source of truth: config/column_maps.yaml

Instrument task registry:
  INSTRUMENT_TASKS  — {instrument_name: [task_spec, ...]}
    Keys match config/instruments.yaml and config/paths.yaml stage_02_sources.
    task_spec keys: glob, out_subdir, ts_status, and either:
      reader             — direct callable(path) -> DataFrame | None
      spectra_raw_subdir — pipeline calls make_spectra_reader(src_dir / this)
"""

import re
import yaml
import pandas as pd
from pathlib import Path

AERIS_TS_FORMAT = "%m/%d/%Y %H:%M:%S.%f"
_N_SPECTRA_DIAGNOSTIC = 2

# ── Column rename maps (loaded from config/column_maps.yaml) ──────────────────

_CONFIG_DIR = Path(__file__).parent.parent / "config"

with open(_CONFIG_DIR / "column_maps.yaml") as _f:
    _COLUMN_MAPS = yaml.safe_load(_f)

ULTRA321_RENAME     = _COLUMN_MAPS["LANL_aerisultra321"]["Raw"]
ULTRA321_ENG_RENAME = _COLUMN_MAPS["LANL_aerisultra321"]["Eng"]
PICO017_RENAME      = _COLUMN_MAPS["LANL_aerispico017"]["Raw"]
PICO017_ENG_RENAME  = _COLUMN_MAPS["LANL_aerispico017"]["Eng"]
ULTRA460_RENAME     = _COLUMN_MAPS["WYO_aerisultra460"]["Raw"]
ULTRA460_ENG_RENAME = _COLUMN_MAPS["WYO_aerisultra460"]["Eng"]
PICARRO_RENAME      = _COLUMN_MAPS["WYO_picarro"]["Raw"]
LGR_RENAME          = _COLUMN_MAPS["UOU_LGR"]["Raw"]
SPRINTER_RENAME     = _COLUMN_MAPS["WYO_sprinter"]["Raw"]


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
    r"AD\s+([\d.]+)\s+PI\s+([-\d.]+)\s+RO\s+([-\d.]+)\s+MD\s+(\d+)\s+TD\s+(\d+)"
)


# ── Public reader functions ───────────────────────────────────────────────────

def read_aeris_raw(path, rename: dict) -> pd.DataFrame:
    """Aeris Raw or Eng .txt file (1-line header + CSV). All columns preserved."""
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    ts = pd.to_datetime(df["Time Stamp"].str.strip(), format=AERIS_TS_FORMAT, errors="coerce")
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
    ts = pd.to_datetime(df["Time Stamp"].str.strip(), format=AERIS_TS_FORMAT, errors="coerce")
    valid = ts.notna()
    df = df[valid].copy()
    df.index = ts[valid].dt.tz_localize("UTC")
    df.index.name = "TIMESTAMP"
    df = df.drop(columns=["Time Stamp"])
    return df if not df.empty else None


def make_spectra_reader(raw_dir):
    """Return a reader closure for spectra files paired with raw_dir."""
    return lambda p: read_spectra(p, raw_dir)


def read_sprinter(path) -> pd.DataFrame | None:
    """Sprinter CSV (3 junk rows, then header, then data)."""
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


def read_gps(path) -> pd.DataFrame | None:
    """
    Toughbook GPS .dat file. Parses GPRMC sentences (lat, lon, speed, course)
    and GPGGA (altitude). Indexed on toughbook epoch timestamp (UTC).
    """
    gprmc_rows: list[dict] = []
    gpgga_rows: list[dict] = []

    for epoch, sentence in _toughbook_data_lines(path):
        fields = sentence.split(",")
        msg = fields[0].lstrip("$")

        if msg == "GPRMC" and len(fields) >= 9 and fields[2] == "A":
            try:
                gprmc_rows.append({
                    "epoch":          epoch,
                    "lat_deg":        _nmea_lat(fields[3], fields[4]),
                    "lon_deg":        _nmea_lon(fields[5], fields[6]),
                    "speed_ms":       round(float(fields[7]) * 0.51444, 4) if fields[7] else float("nan"),
                    "course_true_deg": float(fields[8]) if fields[8] else float("nan"),
                })
            except (ValueError, IndexError):
                continue

        elif msg == "GPGGA" and len(fields) >= 10:
            try:
                gpgga_rows.append({
                    "epoch":      epoch,
                    "altitude_m": float(fields[9]) if fields[9] else float("nan"),
                })
            except (ValueError, IndexError):
                continue

    if not gprmc_rows:
        return None

    df_rmc = pd.DataFrame(gprmc_rows).set_index("epoch")
    if gpgga_rows:
        df_gga = pd.DataFrame(gpgga_rows).set_index("epoch")
        df = pd.merge_asof(
            df_rmc.sort_index(), df_gga.sort_index(),
            left_index=True, right_index=True, tolerance=0.5,
        )
    else:
        df = df_rmc

    df.index = _epoch_to_utc_index(df.index)
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
        spd, dir_, u, v, w, temp, hum, dp, pres, ad, pitch, roll, md, td = m.groups()
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
            "true_dir_deg":  float(td),
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
