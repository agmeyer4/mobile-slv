from pathlib import Path

DATA_ROOT = Path('/uufs/chpc.utah.edu/common/home/lin-group24/agm/Mobile_SLV/Data/2026')
REPO_ROOT = Path(__file__).parent

# ── Raw input (never modified) ────────────────────────────────────────────────
RAW_DIR = DATA_ROOT / 'raw'

# ── Stage 01: UTC clock correction ───────────────────────────────────────────
STAGE_01_DIR         = DATA_ROOT / '01_utc_corrected'   # output directory
STAGE_01_INSTRUMENTS = {'LANL_aerisultra321', 'LANL_aerispico017'}   # corrected here; read from STAGE_01_DIR downstream
STAGE_01_LOGGERS     = {'LANL_rpi', 'LANL_toughbook'}               # input-only; not passed downstream

# ── Stage 02: standardize ─────────────────────────────────────────────────────
STAGE_02_DIR     = DATA_ROOT / '02_standardized'   # output directory
STAGE_02_SOURCES = {                               # input directory per instrument
    # from stage 01 (UTC-corrected)
    'LANL_aerisultra321': STAGE_01_DIR / 'LANL_aerisultra321',
    'LANL_aerispico017':  STAGE_01_DIR / 'LANL_aerispico017',
    # from raw (no UTC correction applied)
    'WYO_picarro':        RAW_DIR / 'WYO_picarro',
    'WYO_aerisultra460':  RAW_DIR / 'WYO_aerisultra460', 
    'UOU_LGR':            RAW_DIR / 'UOU_LGR',
    'WYO_PTR-TOF':        RAW_DIR / 'WYO_PTR-TOF',
    'WYO_sprinter':       RAW_DIR / 'WYO_sprinter',
    'Extra_GPS':          RAW_DIR / 'Extra_GPS',
}

# ── Stage 03: instrument alignment ───────────────────────────────────────────
STAGE_03_DIR = DATA_ROOT / '03_instrument_aligned'   # output directory; reads from STAGE_02_DIR
