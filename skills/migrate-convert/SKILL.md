---
name: migrate-convert
description: >
  Convert ONE assessed migration object (Alteryx .yxmd first; SSIS later) to Databricks
  (PySpark / Lakeflow SDP / Databricks SQL), score the output deterministically, record
  unresolved items as TODOs, and write everything to the UC migration registry. The
  `assessed → converted` step of the lifecycle, run one object at a time in Genie Code.
  Use when the user says "convert", "migrate this workflow", or picks an assessed object.
---

# migrate-convert

Convert a single object that `migrate-assess` already registered. **One object per run**
(Genie Code 1-to-1). Batch/parallel via FMAPI is a later runtime — the conversion knowledge
in `references/` is identical either way.

## When to use
- An object is at `status='assessed'` in the registry and the user wants it converted.
- The user points at a specific `.yxmd` already assessed.

## Prerequisites
- Spine exists and the object is registered (run `migrate-assess` first).
- The source artifact is in the Volume (`objects.volume_path`).

## Conversion knowledge (progressive disclosure — read as needed)
Do NOT inline these into the prompt; open the reference when the workflow needs it:
- `references/alteryx-migration-index.md` — start here: workflow + decision trees.
- `references/alteryx-tool-mapping.md` — Alteryx tool → Spark/PySpark operator.
- `references/alteryx-formula-function-mapping.md` — formula/expression → Spark SQL.
- `references/alteryx-migration-pre-checks-decomposition.md` — the mandatory pre-checks and
  the "1 logical step = 1 operator" decomposition rules.
- `references/alteryx-output-validation-framework.md` — the 6 validation checks (used by
  `migrate-validate`, but preview it to know what the output must satisfy).
- `references/databricks-conversion-standards.md` — the Databricks table/pipeline standards this
  skill OWNS (table properties incl. reserved ones to avoid like `owner`; liquid clustering vs
  Z-order; Auto Loader schema-inference-first + directory paths; audit columns; PII; constraints).
  Follow these — they encode fixes learned from real runs. Do not inherit standards from other
  installed skills.

Target-specific guides — open the ONE matching the configured `target`:
- `references/target-notebook-job.md` — `notebook_job`: notebooks + a Lakeflow Job (ai-dev-kit MCP).
- `references/target-dbsql.md` — `dbsql`: Databricks SQL files + a SQL Warehouse Job.
- (`sdp` uses the SDP patterns in the standards + index references above.)

## Workflow

### 1. Load the object + config + open a run
- Read the object row (`volume_path`, `target_uc_fqn`, `layer`, `complexity`) from `objects`.
- Read the **target type** from config: `SELECT config_value FROM <catalog>.<schema>.config
  WHERE config_key='target'` → one of `sdp` | `notebook_job` | `dbsql` (default `sdp`).
  **Honor what the user asked for** — if they said "Lakeflow job" / "notebook", that's `notebook_job`,
  not `sdp`. Do not assume SDP.
- `CALL <catalog>.<schema>.start_run('<run_id>', '<object_id>', 'convert', 'genie_code');`
- Read the source artifact from the Volume.

### 2. Convert, following the reference rules
- Apply the **5-question visual-operator pre-check** and **decomposition rules** from the
  pre-checks reference before writing code.
- Map each tool via the tool-mapping reference; translate formulas via the formula reference.
- Apply the Databricks standards from `references/databricks-conversion-standards.md`.
- Assign medallion layer per the object's `layer` / the layer decision tree.
- Emit for the configured target:
  - `sdp` — Lakeflow Spark Declarative Pipeline (`@dp` Python or LDP SQL).
  - `notebook_job` — **one notebook per Alteryx tool/node**, plus a Lakeflow Job wiring them in
    the DAG order. Conventions (do not improvise):
    - **Folder:** a per-object CONTAINER folder, hierarchical by source then object:
      `<output_dir>/<source_type>/<object_slug>/` — e.g.
      `<output_dir>/alteryx/sample_sales_analytics_complex/`. Never dump notebooks loose in
      `output_dir`. Create the container folder first.
    - **Naming (inside the folder):** `<object_slug>__<NN>_<tool>` (keep the slug so a notebook
      is self-identifying even if moved), NN = ToolID, tool = plugin short name. E.g.
      `sample_sales_analytics_complex__04_Filter`, `__11_MultiRowFormula`.
    - **Job:** create it via the **ai-dev-kit MCP tools** (`manage_jobs`), one task per notebook,
      `task_key` = the notebook name, `depends_on` matching the Alteryx connections (the DAG).
    - **Traceability (MANDATORY):** after writing each notebook, `CALL add_artifact(object_id,
      'notebook', '<NN>_<tool>', '<path>', NULL, '<json>')`; after creating the job,
      `CALL add_artifact(object_id, 'job', NULL, NULL, '<job_id>', '<json>')`.
    - See `references/target-notebook-job.md`.
  - `dbsql` — one `.sql` file per output table in `output_dir` + a SQL Warehouse job; register each
    file and the job with `add_artifact` too.
- Where a construct has no clean equivalent (macros, R/Python tool, dynamic input, untranslated
  function), leave a clear `-- TODO:` marker in the output describing what needs manual work.

### 3. Write the output + register artifacts
- For `notebook_job`: create the container folder `<output_dir>/<source_type>/<object_slug>/`,
  write each notebook there using the naming convention above; create the Lakeflow Job via
  ai-dev-kit MCP `manage_jobs`.
- For `dbsql`: write `.sql` files to `<output_dir>/<source_type>/<object_slug>/`; create the
  SQL Warehouse job.
- For `sdp`: write the pipeline file; create/point the SDP pipeline.
- **Register every artifact** so the object is traceable to what it produced:
  `CALL <catalog>.<schema>.add_artifact('<object_id>', 'notebook'|'sql_file'|'job'|'pipeline',
  '<source_tool or NULL>', '<path or NULL>', '<job_id/pipeline_id or NULL>', '<json detail>');`

### 4. Score deterministically (NOT self-reported)
- Run `confidence.score(converted_code)` from `spine/lib/confidence.py`.
- This returns a 0..1 confidence + findings. **Do not invent a confidence number.**

### 5. Record state + TODOs + audit — ALWAYS via the UC procedures (never raw INSERT/UPDATE)
Use `CALL` against the registry procedures so the audit row is written with the state change,
atomically. Do NOT hand-write INSERT/UPDATE and do NOT invent your own run_id/audit_id.

```sql
-- one per deterministic finding
CALL <catalog>.<schema>.add_todo('<object_id>', '<category>', '<message>', '<severity>');
-- advance: converted if confidence high and no blocker; else needs_review
CALL <catalog>.<schema>.transition('<object_id>', 'converted', <confidence>, '<output_path>', NULL, 'converted to <target>', '<run_id>');
-- OR: CALL ...transition('<object_id>', 'needs_review', <confidence>, '<output_path>', NULL, '<why>', '<run_id>');
CALL <catalog>.<schema>.end_run('<run_id>', 'ok');   -- or 'partial' / 'failed'
```
Routing rule: `needs_review` if `confidence < threshold` (from `config`, default 0.8) OR any
finding is a blocker; otherwise `converted`.

### 6. Report
Tell the user: target emitted, confidence, open TODOs (ranked), and whether it's ready or needs
review. Offer to resolve the top TODO interactively (this is the human-in-the-loop repair — it
resolves TODOs and re-scores, still one object at a time).

## Output rules
- One object per run. Do not batch here.
- All state, TODOs, and audit go to the registry — never just printed.
- Confidence is always the deterministic score, never an LLM guess.
- Faithful conversion: prefer a visual/SQL operator over hand-written Python (see pre-checks).
