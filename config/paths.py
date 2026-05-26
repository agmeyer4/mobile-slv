"""
Loads config/paths.yaml and exposes Path constants for pipeline stages.
No configurable values live here — edit paths.yaml to change any path.
REPO_ROOT is the only exception: it is derived from __file__ at import time.
"""
import yaml
from pathlib import Path

_here = Path(__file__).parent

with open(_here / "paths.yaml") as _f:
    _cfg = yaml.safe_load(_f)

DATA_ROOT = Path(_cfg["data_root"])
REPO_ROOT = _here.parent

_d = _cfg["dirs"]
RAW_DIR      = DATA_ROOT / _d["raw"]
STAGE_01_DIR = DATA_ROOT / _d["stage_01"]
STAGE_02_DIR = DATA_ROOT / _d["stage_02"]
STAGE_03_DIR = DATA_ROOT / _d["stage_03"]
STAGE_04_DIR = DATA_ROOT / _d["stage_04"]

STAGE_01_INSTRUMENTS = set(_cfg["stage_01"]["instruments"])
STAGE_01_LOGGERS     = set(_cfg["stage_01"]["loggers"])

STAGE_02_SOURCES = {
    inst: DATA_ROOT / rel
    for inst, rel in _cfg["stage_02_sources"].items()
}
