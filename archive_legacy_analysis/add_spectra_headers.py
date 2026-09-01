"""
add_spectra_headers.py

Prepend a correct header row to headerless Aeris spectra / spectralite files.

Aeris spectra files share the same opening columns as their paired Raw files
(Time Stamp through Tgas), followed by two integer diagnostic columns (rd0,
rd1 — ring-down counts), followed by the spectral channel values.  Column
names for the instrument-parameter section are read directly from the Raw
directory so they stay consistent with the rest of the pipeline.

If a file already has a header (first field contains letters), it is stripped
and replaced — this handles re-running after an earlier incorrect invocation.

Usage:
    python add_spectra_headers.py <spectra_dir/> <raw_dir/> <output_dir/>

    spectra_dir  directory of spectra .txt files (headerless or already-headed)
    raw_dir      directory of paired Raw .txt files (provides instrument col names)
    output_dir   destination (same as spectra_dir = in-place)
"""

import sys
from pathlib import Path

N_DIAGNOSTIC = 2  # rd0, rd1 sit between instrument params and spectral channels


def _first_field_is_header(line: str) -> bool:
    """True if the first CSV field looks like a column name, not a timestamp."""
    first = line.split(",")[0].strip()
    # Timestamps start with a digit or slash (e.g. "01/19/2026" or "1768...")
    return bool(first) and first[0].isalpha()


def _instrument_cols_from_raw(raw_dir: Path) -> list[str]:
    """
    Read a Raw file header and return columns from 'Time Stamp' through
    'Tgas(degC)' — the subset that also appears at the start of spectra files.
    """
    raw_files = sorted(p for p in raw_dir.iterdir() if p.suffix == ".txt" and p.is_file())
    if not raw_files:
        raise FileNotFoundError(f"No .txt files found in {raw_dir}")

    with open(raw_files[0]) as fh:
        header_line = fh.readline().strip()

    cols = [c.strip() for c in header_line.split(",")]
    try:
        tgas_idx = next(i for i, c in enumerate(cols) if "tgas" in c.lower())
    except StopIteration:
        raise ValueError(f"No 'Tgas' column found in {raw_files[0].name}")

    return cols[: tgas_idx + 1]   # Time Stamp ... Tgas(degC)


def _build_header(instrument_cols: list[str], n_total: int) -> str:
    n_spec = n_total - len(instrument_cols) - N_DIAGNOSTIC
    if n_spec < 1:
        raise ValueError(
            f"Column count ({n_total}) is too small for {len(instrument_cols)} "
            f"instrument cols + {N_DIAGNOSTIC} diagnostics."
        )
    spec_names = [f"spec_{i:04d}" for i in range(1, n_spec + 1)]
    all_cols = instrument_cols + ["rd0", "rd1"] + spec_names
    return ",".join(all_cols) + "\n"


def _process_file(src: Path, instrument_cols: list[str], dst: Path) -> str:
    with open(src) as fh:
        lines = fh.readlines()

    if not lines:
        return "empty — skipped"

    if _first_field_is_header(lines[0]):
        data_lines = lines[1:]
        action = "replaced header"
    else:
        data_lines = lines
        action = "added header"

    if not data_lines:
        return "empty after header strip — skipped"

    n_total = data_lines[0].count(",") + 1
    header = _build_header(instrument_cols, n_total)

    dst.parent.mkdir(parents=True, exist_ok=True)
    with open(dst, "w") as fh:
        fh.write(header)
        fh.writelines(data_lines)

    return action


def main():
    if len(sys.argv) != 4:
        print("Usage: python add_spectra_headers.py <spectra_dir/> <raw_dir/> <output_dir/>")
        print()
        print("  spectra_dir  headerless (or already-headed) spectra files")
        print("  raw_dir      paired Raw files — used to read instrument col names")
        print("  output_dir   destination (same as spectra_dir for in-place)")
        sys.exit(1)

    spectra_dir = Path(sys.argv[1])
    raw_dir     = Path(sys.argv[2])
    out_dir     = Path(sys.argv[3])

    for path, label in [(spectra_dir, "spectra_dir"), (raw_dir, "raw_dir")]:
        if not path.exists() or not path.is_dir():
            print(f"Error: {label} not found — {path}")
            sys.exit(1)

    try:
        instrument_cols = _instrument_cols_from_raw(raw_dir)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error reading Raw dir: {e}")
        sys.exit(1)

    files = sorted(p for p in spectra_dir.iterdir() if p.is_file() and not p.name.startswith("."))
    if not files:
        print(f"Error: no files found in {spectra_dir}")
        sys.exit(1)

    in_place = spectra_dir.resolve() == out_dir.resolve()

    print(f"\n{'═'*60}")
    print(f"  add_spectra_headers")
    print(f"  Spectra dir  : {spectra_dir}")
    print(f"  Raw dir      : {raw_dir}")
    print(f"  Output       : {out_dir}  {'(in-place)' if in_place else ''}")
    print(f"  Files        : {len(files)}")
    print(f"  Instr cols   : {instrument_cols}")
    print(f"{'═'*60}\n")

    for src in files:
        try:
            if in_place:
                tmp = src.with_suffix(".tmp")
                action = _process_file(src, instrument_cols, tmp)
                if tmp.exists():
                    tmp.replace(src)
            else:
                action = _process_file(src, instrument_cols, out_dir / src.name)
            print(f"  [{action}]  {src.name}")
        except Exception as e:
            print(f"  [error: {e}]  {src.name}")

    print(f"\n  Done.\n")


if __name__ == "__main__":
    main()
