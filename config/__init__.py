import yaml
from pathlib import Path

_here = Path(__file__).parent

with open(_here / 'deployments.yaml') as fh:
    _data = yaml.safe_load(fh)

DEPLOYMENTS = _data.get('schedule', [])
PLATFORMS   = _data.get('platforms', {})
LOGGERS     = _data.get('loggers', {})

# YYMMDD strings for dates where the MML platform deployed without WYO Picarro.
# Used by is_mml() in Stage 03 notebooks to route files to the correct alignment path.
MML_DATE_TAGS = {
    str(d['date']).replace('-', '')[2:]   # date obj / str '2026-01-19' -> '260119'
    for d in DEPLOYMENTS
    if d.get('platform') == 'MML'
}
