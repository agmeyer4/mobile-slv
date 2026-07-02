#!/usr/bin/env python3
"""Convert one or more standardized GPS parquet files to a GPX track file."""

import argparse
import math
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd


def build_track_segment(df: pd.DataFrame, trk: ET.Element) -> None:
    trkseg = ET.SubElement(trk, "trkseg")

    if df.index.tz is None:
        times = df.index.tz_localize("UTC")
    else:
        times = df.index.tz_convert("UTC")

    for ts, row in zip(times, df.itertuples()):
        trkpt = ET.SubElement(
            trkseg,
            "trkpt",
            attrib={"lat": f"{row.lat_deg:.8f}", "lon": f"{row.lon_deg:.8f}"},
        )

        if not math.isnan(row.altitude_m):
            ET.SubElement(trkpt, "ele").text = f"{row.altitude_m:.2f}"

        ET.SubElement(trkpt, "time").text = ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        if hasattr(row, "speed_ms") and not math.isnan(row.speed_ms):
            ET.SubElement(trkpt, "speed").text = f"{row.speed_ms:.4f}"

        if hasattr(row, "course_true_deg") and not math.isnan(row.course_true_deg):
            ET.SubElement(trkpt, "course").text = f"{row.course_true_deg:.2f}"

        if hasattr(row, "n_sats") and not math.isnan(row.n_sats):
            ET.SubElement(trkpt, "sat").text = str(int(row.n_sats))

        if hasattr(row, "hdop") and not math.isnan(row.hdop):
            ET.SubElement(trkpt, "hdop").text = f"{row.hdop:.2f}"


def parquet_to_gpx(input_paths: list[Path], output_path: Path, decimate: int = 1) -> None:
    frames = []
    for p in input_paths:
        print(f"  Loading {p.name} ...")
        df = pd.read_parquet(p)
        df = df.dropna(subset=["lat_deg", "lon_deg"])
        frames.append(df)

    combined = pd.concat(frames).sort_index()

    if decimate > 1:
        combined = combined.iloc[::decimate]

    root = ET.Element(
        "gpx",
        attrib={
            "version": "1.1",
            "creator": "parquet_to_gpx.py",
            "xmlns": "http://www.topografix.com/GPX/1/1",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xsi:schemaLocation": (
                "http://www.topografix.com/GPX/1/1 "
                "http://www.topografix.com/GPX/1/1/gpx.xsd"
            ),
        },
    )

    trk = ET.SubElement(root, "trk")
    name = input_paths[0].stem if len(input_paths) == 1 else f"{len(input_paths)}_files_merged"
    ET.SubElement(trk, "name").text = name

    build_track_segment(combined, trk)

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        tree.write(f, encoding="unicode")

    print(f"Wrote {len(combined):,} track points to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert one or more GPS parquet files to a GPX track."
    )
    parser.add_argument("inputs", type=Path, nargs="+", help="Input .parquet file(s)")
    parser.add_argument("-o", "--output", type=Path, help="Output .gpx file")
    parser.add_argument(
        "-d",
        "--decimate",
        type=int,
        default=1,
        metavar="N",
        help="Keep every Nth point (default: 1 = keep all)",
    )
    args = parser.parse_args()

    if args.output:
        output = args.output
    elif len(args.inputs) == 1:
        output = args.inputs[0].with_suffix(".gpx")
    else:
        output = args.inputs[0].parent / "merged.gpx"

    parquet_to_gpx(args.inputs, output, decimate=args.decimate)


if __name__ == "__main__":
    main()
