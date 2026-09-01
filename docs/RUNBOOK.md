# Runbook — running the pipeline, and freezing a release

Two things live here, because they are the same procedure at different scopes:

- **[Part I](#part-i--the-pipeline-raw--04_calibrated)** — how to get from `raw/` to
  `04_calibrated/`. The operating reference for any run, freeze or not.
- **[Part II](#part-ii--the-release-freeze)** — the extra steps that turn a run into a tagged,
  reproducible release.
- **[Part III](#part-iii--partial-re-runs-after-a-freeze)** — changing one stage after a freeze
  without redoing the whole thing.

A full re-run is not required for every change. Stage 04 alone is enough for a calibration
change; Stages 02–04 for a reader change; from Stage 01 when the timestamps themselves change,
or when you want a clean end-to-end release. Part III covers the partial cases.

---

# Part I — the pipeline: `raw` → `04_calibrated`

## Where things live

`paths.py` is the single source of truth for every path below; nothing hardcodes them.

```
DATA_ROOT = /uufs/chpc.utah.edu/common/home/lin-group24/agm/Mobile_SLV/Data/2026

  raw/                    never modified — the blast radius of every step below excludes it
    calibration/tank_details.txt    the tank/dilution manifest Stage 04 reads
  01_utc_corrected/       Stage 01 output   (~15 GB)
  02_standardized/        Stage 02 output   (~7.2 GB)
  03_instrument_aligned/  Stage 03 output   (~7.3 GB)
  04_calibrated/          Stage 04 output   (~6.3 GB)  ← the delivered product
```

Not every instrument enters at Stage 01. Only the two Aeris units with a logger
(`LANL_aerisultra321`, `LANL_aerispico017`) need clock correction; everything else is read
straight from `raw/` by Stage 02, and the toughbook-logged instruments (`LANL_GPS`,
`LANL_Anem`) have their clock offset resolved later, in Stage 03. `STAGE_02_SOURCES` in
`paths.py` spells out which is which.

## The stages

| Stage | File | What it does | Reads → writes | How it runs |
|---|---|---|---|---|
| **01** | `01_utc_correction.ipynb` | Rebuilds Aeris timestamps per row from the logger host clock, replacing the instrument's own drifting sample clock | `raw/` → `01_utc_corrected/` | headless |
| **02** | `02_standardize.py` | Per-instrument readers → uniform Parquet with a `TIMESTAMP` index and standardized column names; routes each session to a platform | `raw/` + `01_utc_corrected/` → `02_standardized/` | headless |
| **03 survey** | `03_survey.ipynb` | Human quality call (`good`/`uncertain`/`bad`) on every session, saved to `quality_manifest.yaml` | `02_standardized/` → `quality_manifest.yaml` | **interactive**, resumable |
| **03a** | `03a_align_wyo.ipynb` | Cross-correlation tube-lag alignment for the WYO platform | `02_standardized/` → `03_instrument_aligned/` | **interactive** widget |
| **03b** | `03b_align_mml.ipynb` | Same for the MML platform, plus the GPS clock-drift correction | `02_standardized/` → `03_instrument_aligned/` | **interactive** widget |
| **04 QC** | `04_calibration_qc.ipynb` | Compares calibration *candidates* — the evidence behind each method lock. Changes no data | `03_instrument_aligned/` → nothing | headless |
| **04** | `04_calibration.ipynb` | Applies the locked-in calibration; passes through everything it doesn't calibrate | `03_instrument_aligned/` → `04_calibrated/` | headless |

The split between **04 QC** and **04** is deliberate: QC is where candidates are *compared*,
Stage 04 is where one is *applied*. Stage 04 does not re-derive the choice at run time — it
reads `CAL_METHOD_LOCKED` from its own config cell, and asserts that the code path matches.
Change a lock and you must edit both.

## Running it

```bash
# Use the environment's Python. A bare `python` typically lacks pyarrow and reports
# every parquet file as "unreadable" — which looks like data corruption but isn't.
PY=~/software/pkg/miniconda3/envs/mobile-slv/bin/python

jupyter nbconvert --execute --inplace pipeline/01_utc_correction.ipynb
$PY pipeline/02_standardize.py
#   03_survey.ipynb   — interactively, if the quality calls are being reviewed
#   03a_align_wyo.ipynb, then 03b_align_mml.ipynb, interactively in JupyterLab
#   04_calibration_qc.ipynb — read the candidate comparison before locking anything
jupyter nbconvert --execute --inplace pipeline/04_calibration.ipynb

$PY scripts/check_timestamps.py --stage 02 03 04
```

Verify each stage before starting the next, and *especially* before the interactive ones —
catching an upstream problem before the manual part is worth hours.

### Stage 03 survey — resumable, and safe to re-run

`03_survey.ipynb` **loads** `quality_manifest.yaml` on start and writes back after each click,
so re-running it resumes rather than resets. Running its cells without clicking anything is
read-only: the summary cell prints every session with its `G` / `?` / `X` verdict and the
totals, which makes it the record of *what was looked at* as opposed to what was run.

Its verdicts are quality calls about a session, so they stay valid across upstream changes and
do not need redoing when Stage 01 or 02 is re-run.

> **The manifest must exist before you open it.** Delete it and the notebook starts from zero —
> that is a genuine re-review of every session, and it is the one artifact in the whole tree
> that cannot be regenerated by running something. See *Rescue before deleting* in Part II.

### Stage 03 alignment — the expensive one

`RESUME_REVIEW = False` starts a genuine re-review of every session. `= True` seeds the widget
globals from the saved `lag_offsets_*.json`, so you can rebuild the aligned Parquet **without
redoing any human review** — the lag numbers don't change, only `regen_git_hash` /
`regen_run_utc` move.

A full re-review is only *required* when the upstream timeline itself changed, since every lag
was cross-correlated against the old one.

## What to check at each stage

| Stage | Check | Passing looks like |
|---|---|---|
| **01** | Manifest totals; backstep counts; `ts_source` on every corrected file | Row counts preserved per file, zero duplicates introduced, backsteps collapse toward zero |
| **02** | `check_timestamps --stage 02`; per-group `ts_source`; reconcile counts against 01; **`WYO_sprinter` no-fix masking** | 0 unsorted, 0 backsteps; Raw and Eng breakdowns identical per instrument; only known duplicate sources flagged; 15 rows with `GPS Quality == 0` have NaN `lat_deg`/`lon_deg` and **no exact-zero coordinate survives** |
| **03 survey** | Summary cell totals against the file count | Every session carries a verdict; `-=0` unreviewed |
| **03** | **Read `lag_offsets_*.json` directly.** One entry per non-rejected session | `warn: 0` in the apply manifest *and* a fully populated lag dict |
| **04** | Read a calibrated column back off disk against `(raw * scale_in - intercept) / slope`; confirm dropped corrections absent and caveated ones carry their `caveat` field | Max abs difference `0.0`; correction count matches the locks; no `MISMATCH` in Section H |
| **all** | `git_dirty` and the `upstream` chain in every manifest | `false` everywhere; each stage points at the run that actually fed it |

**Never accept a stage on its own printed summary.** Every failure worth catching so far has
looked fine in the summary and wrong in the underlying file — an un-reviewed session still
counts as `ok`, a missing survey still runs, a dirty tree still writes output.

## The provenance chain

Every manifest stamps `git_hash` and `git_dirty` from `HEAD` at run time, plus an `upstream`
block naming the previous stage's manifest, *its* `git_hash`, and *its* `run_utc`
(`src/provenance.py`). That chain is what makes each stage directory self-identifying, and it
is what lets Part III re-run one stage without ambiguity about what produced the rest.

`check_clean()` warns loudly on a dirty tree but does **not** raise — a development run stays
possible, and the cost is that a dirty run records `git_dirty: true` in a manifest nobody
re-reads. Check it explicitly.

---

# Part II — the release freeze

Six phases, in order. The order is not stylistic — each depends on the previous one's output.

> **The invariant.** Every manifest stamps `git_hash` from `HEAD` at run time, and a freeze
> tag exists to say *this commit produced this data*. So the run happens **after** the merge
> and **before** the tag. Run before merging and the manifests record a branch SHA that may
> not survive it; tag before running and the tag points at a commit that produced nothing.

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

## 2. Merge to main

*The run has to happen on main so manifests record a main SHA.*

Open the PR. Use a **merge commit** for a large restructure — a squash collapses the history
into one new SHA and orphans every commit the existing manifests reference.

```bash
git checkout main && git pull
git branch -d <branch> && git push origin --delete <branch>
git status --short --branch    # clean before running anything
```

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
mkdir -p "$D/03_instrument_aligned" "$D/04_calibrated"

# ALWAYS restore: past freeze records. Not pipeline state, and often the only copies.
cp -a "$A/04_calibrated"/*.html "$D/04_calibrated/"

# Human review state — restore or not according to the mode chosen below.
# cp -a "$A/03_instrument_aligned/quality_manifest.yaml" "$D/03_instrument_aligned/"
# cp -a "$A/03_instrument_aligned/lag_offsets_wyo.json"  "$D/03_instrument_aligned/"
# cp -a "$A/03_instrument_aligned/lag_offsets_mml.json"  "$D/03_instrument_aligned/"
```

### Two modes — decide before running, they restore different things

**Mode A — full re-derivation.** Restore nothing but the HTML. Every byte downstream of
`raw/` is then recomputed, and every human call is made again, which is what makes a release
freeze an actual end-to-end test of the pipeline. Leave `RESUME_REVIEW = False` (the committed
default). **`03_survey.ipynb` must be completed FIRST** — see the ordering warning below.

**Mode B — reuse the human review.** Restore all three commented lines, and set
`RESUME_REVIEW = True` in `03a`/`03b`. Legitimate when the upstream *timeline* has not changed,
since the lags were cross-correlated against it and stay valid. Two traps:

- `resume_review()` reads `STAGE_03_DIR/lag_offsets_*.json` — the **live** directory, not the
  archive. Restore those files or it returns empty **silently**, having returned before its own
  "resumed N confirmed" print. A missing seed looks exactly like a successful one.
- `RESUME_REVIEW` is committed as `False`, so mode B needs a source edit. Commit it before the
  run or the manifests record `git_dirty: true` and the tag no longer reproduces the data.

> **Ordering, and the silent failure it prevents.** `load_quality_manifest` returns `{}` when
> the file is absent, so `03a`/`03b` will happily run with **zero** pre-rejections and produce
> plausible-looking output that quietly includes every session the survey had marked bad. In
> mode A the survey is therefore not optional and not reorderable: finish
> `03_survey.ipynb` and confirm its totals **before** opening either alignment notebook.

### Rescue before deleting

Three things in the stage directories are **not** rebuilt by running something:

- **`03_instrument_aligned/quality_manifest.yaml`** — the human quality survey (341 session
  verdicts). It lives inside a stage output directory but is really pipeline *input*: no stage
  computes it, and it is the same category of artifact as
  `raw/calibration/tank_details.txt`. Losing it means re-reviewing every session by hand.
- **`03_instrument_aligned/lag_offsets_wyo.json` / `lag_offsets_mml.json`** — the confirmed and
  rejected alignment lags, plus the MML file's `gps_corrections`. Unlike the survey these *are*
  derived from the pipeline (each lag is cross-correlated against the Stage 01/02 timeline), so
  re-deriving them genuinely re-tests Stages 01–03 — but only a human re-review regenerates
  them. **They are not "regenerated by running the notebook".**
- **Frozen run HTML in `04_calibrated/`** — the executed records of past releases, often the
  only copies.

The failure mode is silent. `align.load_quality_manifest` returns `{}` when the file is
missing, so 03a/03b run with **zero** pre-rejections and no bad-date cascade, and look like
they worked.

Everything else regenerates unattended: `ts_offsets.json`, `ts_correction_manifest.json`
(Stage 01); `routing_manifest.json`, `run_manifest.json` (Stage 02); `apply_manifest_*.json`
(Stage 03); `calibration_coefs.json`, `apply_manifest.json`, `passthrough_manifest.json`
(Stage 04).

`raw/` — including `raw/calibration/tank_details.txt` — is outside the blast radius.

## 4. Run the stages

Per [Part I](#running-it), in order, verifying each before the next.

For a freeze specifically, run **`03_survey.ipynb` and `04_calibration_qc.ipynb` as well**,
even though neither writes stage data. They are what make the frozen record show *what was
examined*, not merely what was executed: the survey's summary cell is the per-session verdict
list, and the QC notebook is the evidence behind every method lock. Export both to HTML in
phase 5 along with the stages that do write data.

## 5. Freeze

*The tag and the data-side record are two halves of one thing.*

First refresh any documented number the run may have moved. Reason strings and README
statistics quoting fit results go stale when the alignment changes. As of 2026-08-27 the
`CAL_DROPPED`/`CAL_CAVEATS` reason strings and the Section D box deliberately quote **no**
fit statistics for this reason — the live numbers live in `calibration_coefs.json`'s `r2`
fields and `04_calibration_qc.ipynb` §E. Check any that have crept back in.

```bash
# executed HTML next to the data it produced — one per notebook
jupyter nbconvert --to html --output <stage>_<date> \
  --output-dir <its data stage dir> pipeline/<notebook>.ipynb

git tag -a v<N>.0-etl-freeze -m "..."
git push origin v<N>.0-etl-freeze
```

A complete freeze exports **six**: `01_utc_correction`, `03_survey`, `03a_align_wyo`,
`03b_align_mml`, `04_calibration`, and `04_calibration_qc` — the survey and the QC notebook
alongside the four that write data. (`02_standardize.py` is a script, not a notebook; its
record is `run_manifest.json`.)

The frozen record is **manifest JSON + executed HTML together**. The manifests alone don't show
what the run looked like; the HTML alone doesn't prove what produced it.

Exporting already-baked plotly outputs often yields a silently chart-free page (`UserWarning:
... application/vnd.plotly.v1+json ... not able to be represented`) when the notebook was
executed under VSCode/JupyterLab. Add a `text/html` key to any output carrying only the vendor
mimetype, built with `plotly.io.to_html(plotly.io.from_json(...), include_plotlyjs='cdn',
full_html=False)`, then grep the result for `plotly-graph-div` — don't trust a zero exit code.

## 6. Close out

- Update the **status banner** at the top of `README.md` to name the new tag and the final
  output directory.
- **Repoint downstream consumers.** `mobile-hydrocarbon-analysis` reads this pipeline's final
  output directory; its README cites that path. This is the one item living outside this repo
  and the easiest to forget — a freeze fails quietly when a consumer still points at the
  previous release.
- Drop archives that are superseded twice over, once the new run is verified.

---

# Part III — partial re-runs after a freeze

A freeze is not a commitment to redo everything next time. Each stage reads only the stage
above it, and every manifest self-identifies, so one stage can be re-run against an untouched
upstream. The result is a data tree where different stages carry different `git_hash` values —
which is legible rather than confusing, because that is exactly what the `upstream` chain
records.

## Changing only the calibration

The common case: a method lock, a `CAL_CAVEATS` entry, or a coefficient decision changes.
**Stage 04 is terminal** — nothing in the pipeline reads `04_calibrated/` — so Stages 01–03
stay exactly as they are.

1. Edit `04_calibration.ipynb` (and `04_calibration_qc.ipynb` if the evidence changed).
   Commit — a re-run on a dirty tree records `git_dirty: true` and is not freezable.
2. **Archive the existing `04_calibrated/` first.** Stage 04 writes into the directory in
   place, so the previous release's delivered output — *and its HTML records* — are gone
   otherwise. This is the one destructive part of an otherwise cheap operation.
3. Re-run `04_calibration_qc.ipynb`, then `04_calibration.ipynb`. Minutes, not hours, and
   entirely headless.
4. Check per the Stage 04 row of the table in Part I.
5. Export the two Stage 04 HTMLs. **Do not re-export the others** — they describe runs that
   genuinely did not happen again, and replacing them would be a lie about provenance.
6. Tag the new commit (`v<N>.<M>-etl-freeze`) and note in the README banner which stages it
   covers.

Afterwards `04_calibrated/apply_manifest.json` records the new commit while its `upstream`
block still points at the same unchanged Stage 03 run. That mixed state is correct and
self-describing: any consumer can read which commit produced the calibration and which
produced the alignment underneath it.

**Why re-running Stage 04 in place is safe here but not in general.**
`cal.apply_calibration_to_dir` rebuilds every output file from its Stage 03 source, so
columns are rebuilt rather than accumulated — a correction you remove really does lose its
`*_cal` / `*_xcal` column. What it does *not* do is delete destination files that no longer have a
source. With Stage 03 unchanged the file set is identical and no orphan is possible; if Stage
03 *had* changed, an in-place re-run could leave stale files behind, which is the whole reason
Part II phase 3 archives the tree instead of overwriting it. Section H's file-count
reconciliation compares Stage 03 against Stage 04 and flags `MISMATCH` either way, so the
check exists — but archiving is what prevents the problem.

## Other partial re-runs

| Change | Re-run | Note |
|---|---|---|
| Calibration method, caveat, or coefficient | 04 QC + 04 | Above. Headless, minutes. |
| A reader in `src/readers.py` | 02 → 03 → 04 | Stage 03's human review is preserved with `RESUME_REVIEW = True` unless the *timeline* moved. |
| Timestamp handling in Stage 01 | 01 → 02 → 03 → 04 | Every lag was cross-correlated against the old timeline, so this one genuinely needs the full Stage 03 re-review. |
| A quality verdict only | 03 survey → 03a/03b → 04 | The survey resumes; only the changed session needs re-clicking. |

The rule underneath the table: re-run from the earliest stage whose **inputs or code** changed,
and every stage after it.

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
| Re-running Stage 04 in place | Overwrites the previous release's delivered output *and* its HTML records — archive first |

---

Order is load-bearing: prepare → merge → clear → run → freeze → close out. Everything else is
verification.
