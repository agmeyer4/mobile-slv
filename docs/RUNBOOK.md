# Runbook — full re-run and release freeze

Taking the pipeline from a working branch to a tagged, reproducible release. Six phases, in
order. The order is not stylistic — each phase depends on the previous one's output.

> **The invariant.** Every manifest stamps `git_hash` from `HEAD` at run time, and a freeze
> tag exists to say *this commit produced this data*. So the run happens **after** the merge
> and **before** the tag. Run before merging and the manifests record a branch SHA that may
> not survive it; tag before running and the tag points at a commit that produced nothing.

A full re-run is not required for every change. Stage 04 alone is enough for a calibration
change; Stages 02–04 for a reader change. Re-run from Stage 01 when the timestamps themselves
change, or when you want a clean end-to-end release.

---

## 1. Prepare the branch

*Nothing uncommitted, or the run records `git_dirty: true`.*

Land every code **and doc** change first. Anything fixed after the run either misses the tag
or forces a re-run.

```bash
git status --short --branch    # must be empty
git push origin <branch>
```

`nbstripout` is a clean filter, so an executed notebook shows as unmodified. A clean
`git status` does **not** mean the working tree matches a fresh clone — and conversely,
re-executing a notebook won't make the tree dirty. Only source edits do.

---

## 2. Merge to main

*The run has to happen on main so manifests record a main SHA.*

Open the PR. Use a **merge commit** for a large restructure — a squash collapses the history
into one new SHA and orphans every commit the existing manifests reference.

```bash
git checkout main && git pull
git branch -d <branch> && git push origin --delete <branch>
git status --short --branch    # clean before running anything
```

---

## 3. Clear the slate

*Overwriting leaves stale files behind; moving guarantees a true re-run.*

Stage 03's apply writes rejected sessions into `bad/` but never removes a previously-good file
from the top level. If a session flips `ok → bad` in a fresh review, an overwrite leaves the
stale file sitting in the delivered directory next to the new `bad/` copy. Archiving the whole
tree removes that class of bug, and makes the Stage 04 file-count reconciliation and
`check_timestamps` real end-to-end checks instead of checks against partially-stale contents.

```bash
D=/uufs/chpc.utah.edu/common/home/lin-group24/agm/Mobile_SLV/Data/2026
A="$D/prev_$(date +%Y-%m-%d)"
mkdir -p "$A"

# instant — all stage dirs are on one filesystem, so this is metadata only.
# cp -a would copy ~36 GB (01_utc_corrected alone is 15 GB).
mv "$D/01_utc_corrected" "$D/02_standardized" \
   "$D/03_instrument_aligned" "$D/04_calibrated" "$A"/

# put back only what cannot be regenerated
mkdir -p "$D/03_instrument_aligned" "$D/04_calibrated"
cp -a "$A/03_instrument_aligned/quality_manifest.yaml" "$D/03_instrument_aligned/"
cp -a "$A/04_calibrated"/*.html                        "$D/04_calibrated/"
```

### Rescue before deleting

Two things in the stage directories are **not** regenerated:

- **`03_instrument_aligned/quality_manifest.yaml`** — the one-time manual file survey. It lives
  inside a stage output directory and is the only artifact the pipeline cannot rebuild.
- **Frozen run HTML in `04_calibrated/`** — the executed records of past releases, often the
  only copies.

The failure mode is silent. `align.load_quality_manifest` returns `{}` when the file is
missing, so 03a/03b run with **zero** pre-rejections and no bad-date cascade, and look like
they worked.

Everything else regenerates: `ts_offsets.json`, `ts_correction_manifest.json` (Stage 01);
`routing_manifest.json`, `run_manifest.json` (Stage 02); `lag_offsets_*.json`,
`apply_manifest_*.json` (Stage 03); `calibration_coefs.json`, `apply_manifest.json`,
`passthrough_manifest.json` (Stage 04).

`raw/` — including `raw/calibration/tank_details.txt` — is outside the blast radius.

---

## 4. Run the stages, in order

Verify each stage before starting the next, and *especially* before the manual one.

```bash
# Use the environment's Python. A bare `python` typically lacks pyarrow and reports
# every parquet file as "unreadable" — which looks like data corruption but isn't.
PY=~/software/pkg/miniconda3/envs/mobile-slv/bin/python

jupyter nbconvert --execute --inplace pipeline/01_utc_correction.ipynb
python pipeline/02_standardize.py
#   03a_align_wyo.ipynb, then 03b_align_mml.ipynb, interactively in JupyterLab
#   04_calibration_qc.ipynb — read the candidate comparison before locking anything
jupyter nbconvert --execute --inplace pipeline/04_calibration.ipynb

$PY scripts/check_timestamps.py --stage 02 03 04
```

`pipeline/03_survey.ipynb` is **not** part of a re-run. It is the one-time quality survey; its
manifest persists and stays valid across upstream changes.

### Stage 03 — the expensive one

`RESUME_REVIEW = False` starts a genuine re-review of every session. `= True` seeds the widget
globals from the saved `lag_offsets_*.json`, so you can rebuild the aligned Parquet **without
redoing any human review** — the lag numbers don't change, only `regen_git_hash` /
`regen_run_utc` move.

A full re-review is only *required* when the upstream timeline itself changed, since every lag
was cross-correlated against the old one. Verify Stages 01 and 02 first — catching an upstream
problem before the manual part is worth hours.

---

## What to check at each stage

| Stage | Check | Passing looks like |
|---|---|---|
| **01** | Manifest totals; backstep counts; `ts_source` on every corrected file | Row counts preserved per file, zero duplicates introduced, backsteps collapse toward zero |
| **02** | `check_timestamps --stage 02`; per-group `ts_source`; reconcile counts against 01; **`WYO_sprinter` no-fix masking** | 0 unsorted, 0 backsteps; Raw and Eng breakdowns identical per instrument; only known duplicate sources flagged; 15 rows with `GPS Quality == 0` have NaN `lat_deg`/`lon_deg` and **no exact-zero coordinate survives** |
| **03** | **Read `lag_offsets_*.json` directly.** One entry per non-rejected session | `warn: 0` in the apply manifest *and* a fully populated lag dict |
| **04** | Read a calibrated column back off disk against `(raw * scale_in - intercept) / slope`; confirm dropped corrections absent and caveated ones carry their `caveat` field | Max abs difference `0.0`; correction count matches the locks; no `MISMATCH` in Section H |
| **all** | `git_dirty` and the `upstream` chain in every manifest | `false` everywhere; each stage points at the run that actually fed it |

**Never accept a stage on its own printed summary.** Every failure worth catching so far has
looked fine in the summary and wrong in the underlying file — an un-reviewed session still
counts as `ok`, a missing survey still runs, a dirty tree still writes output.

---

## 5. Freeze

*The tag and the data-side record are two halves of one thing.*

First refresh any documented number the run may have moved. Reason strings and README
statistics quoting fit results go stale when the alignment changes. As of 2026-08-27 the
`CAL_DROPPED`/`CAL_CAVEATS` reason strings and the Section D box deliberately quote **no**
fit statistics for this reason — the live numbers live in `calibration_coefs.json`'s `r2`
fields and `04_calibration_qc.ipynb` §E. Check any that have crept back in.

```bash
# executed HTML next to the data it produced — one per notebook stage
jupyter nbconvert --to html --output <stage>_<date> \
  --output-dir <its data stage dir> pipeline/<notebook>.ipynb

git tag -a v<N>.0-etl-freeze -m "..."
git push origin v<N>.0-etl-freeze
```

The frozen record is **manifest JSON + executed HTML together**. The manifests alone don't show
what the run looked like; the HTML alone doesn't prove what produced it.

Exporting already-baked plotly outputs often yields a silently chart-free page (`UserWarning:
... application/vnd.plotly.v1+json ... not able to be represented`) when the notebook was
executed under VSCode/JupyterLab. Add a `text/html` key to any output carrying only the vendor
mimetype, built with `plotly.io.to_html(plotly.io.from_json(...), include_plotlyjs='cdn',
full_html=False)`, then grep the result for `plotly-graph-div` — don't trust a zero exit code.

---

## 6. Close out

- Update the **status banner** at the top of `README.md` to name the new tag and the final
  output directory.
- **Repoint downstream consumers.** `mobile-hydrocarbon-analysis` reads this pipeline's final
  output directory; its README cites that path. This is the one item living outside this repo
  and the easiest to forget — a freeze fails quietly when a consumer still points at the
  previous release.
- Drop archives that are superseded twice over, once the new run is verified.

---

## Standing gotchas

| Trap | What it looks like |
|---|---|
| Wrong Python | Every parquet file reports "unreadable" — a missing `pyarrow`, not corrupt data |
| Widget clicks not persisting | Apply counts look normal; the lag dict is empty and every session silently gets `0.0` |
| Fresh kernel, `RESUME_REVIEW = False` | An empty manifest is written over a real one, then zero lag applied to everything |
| Missing quality manifest | Returns `{}` with no error; all pre-rejections vanish |
| Notebook outputs live only in the working tree | Committed blob is tiny, working file is tens of MB — overwrite it and the run's plots are gone |
| Editing a large notebook with baked outputs | Read/Edit tooling chokes; patch `cell['source']` programmatically, then `nbformat.validate` and `compile()` every code cell |

---

Order is load-bearing: prepare → merge → clear → run → freeze → close out. Everything else is
verification.
