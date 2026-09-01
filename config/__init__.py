import yaml
from pathlib import Path

_here = Path(__file__).parent

with open(_here / 'deployments.yaml') as fh:
    _dep = yaml.safe_load(fh)

with open(_here / 'instruments.yaml') as fh:
    INSTRUMENTS = yaml.safe_load(fh)

DEPLOYMENTS = _dep.get('schedule', [])
PLATFORMS   = _dep.get('platforms', {})
LOGGERS     = _dep.get('loggers', {})

# (instrument_key, 'YYMMDD') -> 'MML' | 'WYO'
# Derived from schedule entries in deployments.yaml.
# Used by pipeline/02_standardize.py to build routing_manifest.json.
PLATFORM_BY_INST_DATE = {}
for _entry in DEPLOYMENTS:
    _tag = str(_entry['date']).replace('-', '')[2:]
    for _inst in _entry.get('instruments', []):
        PLATFORM_BY_INST_DATE[(_inst, _tag)] = _entry['platform']
