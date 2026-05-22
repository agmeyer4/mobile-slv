from pathlib import Path

# Root of the processed data tree — change this if the data directory moves
DATA_ROOT = Path('/uufs/chpc.utah.edu/common/home/lin-group24/agm/Mobile_SLV/Data/2026')

# Pipeline stage directories
RAW_DIR                = DATA_ROOT / 'raw'
UTC_CORRECTED_DIR      = DATA_ROOT / '01_utc_corrected'
STANDARDIZED_DIR       = DATA_ROOT / '02_standardized'
INSTRUMENT_ALIGNED_DIR = DATA_ROOT / '03_instrument_aligned'

# Repo root
REPO_ROOT = Path(__file__).parent
