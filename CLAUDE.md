# mobile-slv — working notes

**Read `README.md` first.** It is the authoritative documentation: pipeline stages, run order,
output structure, conventions, reproducibility, known issues, and the `src/` module reference.
This file holds only the things that bite you while *working on* the repo, which don't belong
in user-facing docs.

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

Editing large notebooks through Read/Edit tooling chokes once real plotly outputs are baked in.
Edit programmatically instead — load with `nbformat`/`json`, patch `cell['source']`, write back,
then `nbformat.validate` and `compile()` every code cell.

Exporting already-baked plotly outputs to HTML often silently produces a chart-free page
(`UserWarning: ... application/vnd.plotly.v1+json ... not able to be represented`) when the
notebook was executed under VSCode/JupyterLab. Fix: walk the outputs and add a `text/html` key
built with `plotly.io.to_html(plotly.io.from_json(...), include_plotlyjs='cdn',
full_html=False)` to any output that has the vendor mimetype but no `text/html`. Verify by
grepping the result for `plotly-graph-div` — don't trust a zero exit code.

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
