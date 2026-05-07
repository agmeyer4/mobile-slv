"""
clean_gps.py

Parse raw NMEA GPS logger files (RPi / Toughbook) into a standard
TIMESTAMP-indexed CSV.

Each data line has the form:
    epoch_seconds,$NMEA_SENTENCE,...*checksum

Three sentence types are parsed:
    $GPRMC  →  lat, lon, gps_valid, speed_kts, course_true_deg
    $GPGGA  →  lat, lon, fix_quality, num_sats, hdop, altitude_m
    $GPVTG  →  course_true_deg, course_mag_deg, speed_kts

One row is written per parsed sentence. Fields not provided by a given
sentence type are NaN. When merge_daily.py resamples to 1s it will average
across the ~3 sentences per fix automatically.

Usage:
    python clean_gps.py <gps_dir/> <output_dir/>
"""

import re
import sys
from pathlib import Path

import pandas as pd

SKIPROWS = 6   # 4 header lines + blank + "This is the raw GPS data..." line


# ── NMEA helpers ──────────────────────────────────────────────────────────────

def _ddmm_to_decimal(ddmm: str, hem: str) -> float | None:
    """Convert NMEA DDMM.MMMMM + hemisphere to signed decimal degrees."""
    try:
        val = float(ddmm)
    except (ValueError, TypeError):
        return None
    deg  = int(val / 100)
    mins = val - deg * 100
    dec  = deg + mins / 60.0
    return -dec if hem in ("S", "W") else dec


def _f(fields: list[str], idx: int) -> str | None:
    """Safe field access — returns None if index out of range or field empty."""
    try:
        v = fields[idx].strip()
        return v if v else None
    except IndexError:
        return None


def _parse_gprmc(fields: list[str]) -> dict:
    return {
        "lat_deg":         _ddmm_to_decimal(_f(fields, 3), _f(fields, 4) or ""),
        "lon_deg":         _ddmm_to_decimal(_f(fields, 5), _f(fields, 6) or ""),
        "gps_valid":       1 if _f(fields, 2) == "A" else 0,
        "speed_kts":       float(_f(fields, 7)) if _f(fields, 7) else None,
        "course_true_deg": float(_f(fields, 8)) if _f(fields, 8) else None,
    }


def _parse_gpgga(fields: list[str]) -> dict:
    return {
        "lat_deg":      _ddmm_to_decimal(_f(fields, 2), _f(fields, 3) or ""),
        "lon_deg":      _ddmm_to_decimal(_f(fields, 4), _f(fields, 5) or ""),
        "fix_quality":  int(_f(fields, 6))   if _f(fields, 6)  else None,
        "num_sats":     int(_f(fields, 7))   if _f(fields, 7)  else None,
        "hdop":         float(_f(fields, 8)) if _f(fields, 8)  else None,
        "altitude_m":   float(_f(fields, 9)) if _f(fields, 9)  else None,
    }


def _parse_gpvtg(fields: list[str]) -> dict:
    return {
        "course_true_deg": float(_f(fields, 1)) if _f(fields, 1) else None,
        "course_mag_deg":  float(_f(fields, 3)) if _f(fields, 3) else None,
        "speed_kts":       float(_f(fields, 5)) if _f(fields, 5) else None,
    }


_PARSERS = {
    "$GPRMC": _parse_gprmc,
    "$GPGGA": _parse_gpgga,
    "$GPVTG": _parse_gpvtg,
}


# ── File parser ───────────────────────────────────────────────────────────────

def parse_gps_file(filepath: Path) -> pd.DataFrame:
    rows = []

    with open(filepath) as fh:
        for _ in range(SKIPROWS):
            fh.readline()
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = line.split(",", 1)
            if len(parts) < 2:
                continue
            try:
                epoch = float(parts[0])
            except ValueError:
                continue

            # Strip NMEA checksum (*XX) from end of sentence
            sentence = re.sub(r"\*[0-9A-Fa-f]{2}$", "", parts[1])
            fields = sentence.split(",")
            stype  = fields[0]

            if stype not in _PARSERS:
                continue

            try:
                row = _PARSERS[stype](fields)
            except Exception:
                continue

            row["epoch"] = epoch
            rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df.insert(0, "TIMESTAMP",
              pd.to_datetime(df["epoch"], unit="s", utc=True)
              .dt.tz_localize(None)
              .dt.strftime("%Y-%m-%d %H:%M:%S.%f"))
    df = df.drop(columns=["epoch"]).set_index("TIMESTAMP")
    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) != 3:
        print("Usage: python clean_gps.py <gps_dir/> <output_dir/>")
        sys.exit(1)

    in_dir  = Path(sys.argv[1])
    out_dir = Path(sys.argv[2])

    if not in_dir.exists():
        print(f"Error: {in_dir} not found.")
        sys.exit(1)

    files = sorted(p for p in in_dir.iterdir()
                   if p.is_file() and not p.name.startswith("."))
    if not files:
        print(f"Error: no files found in {in_dir}")
        sys.exit(1)

    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'═'*60}")
    print(f"  GPS Cleaner")
    print(f"  Input  : {in_dir}")
    print(f"  Output : {out_dir}")
    print(f"  Files  : {len(files)}")
    print(f"{'═'*60}\n")

    n_written = 0
    for i, fp in enumerate(files, 1):
        try:
            df = parse_gps_file(fp)
            if df.empty:
                print(f"  [{i}/{len(files)}]  {fp.name}  →  [empty, skipped]")
                continue
            out_path = out_dir / (fp.stem + "_clean.csv")
            df.to_csv(out_path)
            print(f"  [{i}/{len(files)}]  {fp.name}  →  {len(df):,} rows  ✓")
            n_written += 1
        except Exception as e:
            print(f"  [{i}/{len(files)}]  {fp.name}  →  [error] {e}")

    print(f"\n  Done — {n_written}/{len(files)} file(s) written to {out_dir}\n")


if __name__ == "__main__":
    main()
