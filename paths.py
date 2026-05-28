from pathlib import Path

DATA_ROOT = Path('/uufs/chpc.utah.edu/common/home/lin-group24/agm/Mobile_SLV/Data/2026')
REPO_ROOT = Path(__file__).parent

# ── Raw input (never modified) ────────────────────────────────────────────────
RAW_DIR = DATA_ROOT / 'raw'

# ── Stage directories ─────────────────────────────────────────────────────────
STAGE_01_DIR = DATA_ROOT / '01_utc_corrected'
STAGE_02_DIR = DATA_ROOT / '02_standardized'
STAGE_03_DIR = DATA_ROOT / '03_instrument_aligned'
STAGE_04_DIR = DATA_ROOT / '04_daily'

# ── Stage 03 quality manifest ─────────────────────────────────────────────────
QUALITY_MANIFEST_PATH = STAGE_03_DIR / 'quality_manifest.yaml'

# ── Stage 01 metadata ─────────────────────────────────────────────────────────
STAGE_01_INSTRUMENTS = {'LANL_aerisultra321', 'LANL_aerispico017'}
STAGE_01_LOGGERS     = {'LANL_rpi', 'LANL_toughbook'}

# ── Stage 02 source directories ───────────────────────────────────────────────
STAGE_02_SOURCES = {
    # UTC-corrected by Stage 01
    'LANL_aerisultra321': STAGE_01_DIR / 'LANL_aerisultra321',
    'LANL_aerispico017':  STAGE_01_DIR / 'LANL_aerispico017',
    # trusted timestamps — read directly from raw
    'WYO_picarro':        RAW_DIR / 'WYO_picarro',
    'WYO_aerisultra460':  RAW_DIR / 'WYO_aerisultra460',
    'UOU_LGR':            RAW_DIR / 'UOU_LGR' / 'final',
    'WYO_PTR-TOF':        RAW_DIR / 'WYO_PTR-TOF',
    'WYO_sprinter':       RAW_DIR / 'WYO_sprinter',
    'Extra_GPS':          RAW_DIR / 'Extra_GPS',
    # toughbook-logged — clock offset resolved in Stage 03
    'LANL_GPS':           RAW_DIR / 'LANL_toughbook' / 'GPS',
    'LANL_Anem':          RAW_DIR / 'LANL_toughbook' / 'Anem',
}
