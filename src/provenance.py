"""
Cross-stage provenance utilities.

Every pipeline stage's manifest records the git commit and dirty-state of the code that
produced it. This module adds the piece that was missing: a reference to the upstream
stage's own manifest, so the full chain from raw data to any output is traceable to one
commit without cross-referencing timestamps by hand, plus a loud (non-fatal) warning
when a run isn't pinned to a clean commit.

Public API
----------
git_info(repo_root)
    Return (commit_hash, is_dirty). ('unknown', False) on failure (e.g. no git installed,
    or repo_root isn't inside a git repo).
check_clean(repo_root, context='')
    Print a loud warning if the working tree is dirty. Does not raise -- a pipeline run
    stays usable during development -- but makes a dirty run impossible to miss in the
    log, instead of silently recording git_dirty: true in a manifest nobody re-reads.
    Returns True if clean, False if dirty.
upstream_ref(manifest_path)
    Read another stage's manifest and pull out its {stage, git_hash, git_dirty, run_utc}
    as a small dict, for embedding in this stage's own manifest under an 'upstream' key.
    Returns {'error': ...} if the file is missing or unreadable, rather than raising --
    a missing upstream manifest shouldn't block a stage from running.
"""

import json
import subprocess
from pathlib import Path


def git_info(repo_root):
    """Return (commit_hash, is_dirty). ('unknown', False) on failure."""
    try:
        h = subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], cwd=str(repo_root), text=True).strip()
        dirty = subprocess.call(['git', 'diff', '--quiet'], cwd=str(repo_root)) != 0
        return h, dirty
    except Exception:
        return 'unknown', False


def check_clean(repo_root, context=''):
    """Print a loud (non-fatal) warning if the working tree is dirty.

    Parameters
    ----------
    repo_root : path-like
    context : str
        Optional label included in the warning (e.g. 'Stage 02').

    Returns
    -------
    bool
        True if the tree is clean, False if dirty.
    """
    _, dirty = git_info(repo_root)
    if dirty:
        label = f' ({context})' if context else ''
        print(f'*** WARNING: working tree is DIRTY{label} -- this run is not '
              f'reproducible from a single commit. Commit or stash first if this '
              f'run should be treated as canonical. ***')
    return not dirty


def upstream_ref(manifest_path):
    """Read another stage's manifest and extract its provenance fields.

    Parameters
    ----------
    manifest_path : path-like
        Path to the upstream stage's manifest JSON (expected to have at least
        'stage', 'git_hash', 'git_dirty', 'run_utc' keys — every stage's manifest
        already does).

    Returns
    -------
    dict
        {'manifest_path', 'stage', 'git_hash', 'git_dirty', 'run_utc'}, or
        {'manifest_path', 'error': ...} if the file is missing/unreadable.
    """
    manifest_path = Path(manifest_path)
    if not manifest_path.exists():
        return {'manifest_path': str(manifest_path), 'error': 'file not found'}
    try:
        with open(manifest_path) as fh:
            d = json.load(fh)
    except Exception as e:
        return {'manifest_path': str(manifest_path), 'error': f'could not read: {e}'}
    return {
        'manifest_path': str(manifest_path),
        'stage': d.get('stage'),
        'git_hash': d.get('git_hash'),
        'git_dirty': d.get('git_dirty'),
        'run_utc': d.get('run_utc'),
    }
