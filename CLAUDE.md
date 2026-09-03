# mobile-slv — working notes

**Read `README.md` first.** It is the authoritative documentation: pipeline stages, run order,
output structure, conventions, reproducibility, known issues, and the `src/` module reference.
This file holds only the things that bite you while *working on* the repo, which don't belong
in user-facing docs.

For a full re-run or a release freeze, follow [`docs/RUNBOOK.md`](docs/RUNBOOK.md) rather than
improvising the order — the phase ordering is forced by how manifests and freeze tags work.

## Scope

This repo is **ETL + QA/QC, including calibration**. Open-ended scientific analysis belongs in
`mobile-hydrocarbon-analysis`. If a task is "explore/answer a science question," it is probably
in the wrong repo.

## Environment

Notebooks and scripts need the `mobile-slv` environment's Python. A bare `python` on PATH
typically lacks `pyarrow`, in which case every Parquet read fails with "unreadable" — which
looks like data corruption but is a missing dependency:

```
~/software/pkg/miniconda3/envs/mobile-slv/bin/python
```

The same applies to `jupyter`: a bare `jupyter` on PATH resolves to miniconda's **base** env,
which has no `nbconvert`, so a phase-5 HTML export fails there rather than in the pipeline. Use
`~/software/pkg/miniconda3/envs/mobile-slv/bin/jupyter-nbconvert` from a non-interactive shell.

## Editing notebooks

`nbstripout` is a **clean filter**, so git sees an executed notebook as unmodified. Two
consequences:

1. A notebook's committed blob can be tens of KB while its working-tree file is tens of MB of
   baked outputs. **Those outputs exist only in the working tree** — if you overwrite the file,
   they are gone. Check `git log`/blob size before rewriting, and snapshot to HTML first if the
   run matters.
2. `git status` being clean does *not* mean the working tree matches what you'd get from a
   fresh clone. Conversely, re-executing a notebook does not make the tree dirty — which is why
   a stage can legitimately report `git_dirty: false` with outputs present.
3. An executed notebook nevertheless shows ` M` in `git status` **forever**, because the file's
   mtime/size changed even though the filtered content is byte-identical to HEAD. `git diff` is
   empty and `git_dirty` (computed as `git diff --quiet`) stays false, so it is cosmetic.
   **`git update-index --refresh` does NOT clear it** — it compares the raw file without
   applying the clean filter, prints "needs update" and exits 1. What works: `git add
   pipeline/*.ipynb` (clears the flag, stages nothing, keeps the outputs) or `nbstripout
   pipeline/*.ipynb` (clears it and discards them).
4. **Never `git checkout` / `restore` / `stash` a notebook holding a run's outputs** — the
   smudge filter hands back the stripped version and the executed record is gone. To put back
   committed content without losing outputs, patch the file with a json/nbformat script instead.
   A Jupyter save silently *dropping* a trailing empty cell is enough to make the tree genuinely
   dirty and stamp `git_dirty: true` into the next stage's manifest; re-inserting that cell by
   script is the safe fix.

Editing large notebooks through Read/Edit tooling chokes once real plotly outputs are baked in.
Edit programmatically instead — load with `nbformat`/`json`, patch `cell['source']`, write back,
then `nbformat.validate` and `compile()` every code cell.

Exporting already-baked plotly outputs to HTML often silently produces a chart-free page
(`UserWarning: ... application/vnd.plotly.v1+json ... not able to be represented`) when the
notebook was executed under VSCode/JupyterLab. Fix: walk the outputs and add a `text/html` key
built with `plotly.io.to_html(plotly.io.from_json(...), include_plotlyjs='cdn',
full_html=False)` to any output that has the vendor mimetype but no `text/html`. Verify by
grepping the result for `plotly-graph-div` — don't trust a zero exit code.

## Stage 03 survey — the completeness check is not what it looks like

`03_survey.ipynb`'s summary cell iterates the **manifest**, not the file list, so a session that
was never clicked is simply absent and is never counted. **`-=0 unreviewed` therefore prints
clean no matter how many sessions are missing**, and `README`/`RUNBOOK`'s "every session carries
a verdict" check does not actually verify that clause. This has bitten once (one Ultra460 session
had no verdict after a full re-survey). Reconcile externally instead — glob
`SURVEY_INSTRUMENTS`' dirs (main + `no_coverage/`) and diff the stems against the manifest keys.
A missing entry is not inert: `align.file_quality` falls back to `('uncertain','')` and only
`'bad'` pre-rejects, so the session flows into alignment unjudged.

## Widget-driven stages (03a / 03b)

The review widgets keep state in notebook globals and `save_lag_offsets_*()` serialises
whatever is in them. Two failure modes, both of which have actually happened:

- A fresh kernel with `RESUME_REVIEW = False` writes an **empty** manifest over a real one, then
  applies 0 s to everything. Set `RESUME_REVIEW = True` to rebuild from saved decisions.
- Clicks that don't register leave a session un-reviewed, and the apply step still counts it
  `ok` (with `tube_lag = 0.0` and a `[WARN no tube lag]`). **The `ok`/`bad`/`warn` summary is not
  a sufficient check.** Read `lag_offsets_{wyo,mml}.json` and confirm `tube_lags` has one entry
  per non-rejected session.

## Verifying a stage

Don't rely on a notebook's own printed summary. `scripts/check_timestamps.py --stage NN` is the
independent gate, and for calibration it's worth reading a calibrated column back off disk and
checking it against `(raw * scale_in - intercept) / slope` directly.

For any change to a fit or calibration, render the actual continuous timeseries, not just
discrete-point residual tables — a fit can look fine at the fit points and be badly wrong
between them.

## Data

`raw/` is read-only. `quality_manifest.yaml` (Stage 03) is a one-time 341-file manual survey and
is **not regenerable** — back it up before anything that could clear the Stage 03 directory.
