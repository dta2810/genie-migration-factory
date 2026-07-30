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
- **Tables land in the object's OWN schema, not the registry schema.** Create it first:
  `CREATE SCHEMA IF NOT EXISTS <output_catalog>.mig_<object_slug>` (output_catalog from config;
  object_slug = object_id with ':'→'_'). ALL tables the pipeline produces —
  intermediate `_stage_*` and final gold/detail — go in `<output_catalog>.mig_<object_slug>.<table>`.
  This isolates each migration's output (no cross-object collision, drop-schema cleans it up) and
  keeps it out of `migration_factory` (the framework registry). Intermediate `_stage_*` tables
  **persist** (real tables, inspectable/auditable), not temp views — register each in `artifacts`.
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
- **Register every artifact** so the object is traceable to what it produced — notebooks/sql files,
  the job/pipeline, AND the output tables (each `_stage_*` and final table):
  `CALL <catalog>.<schema>.add_artifact('<object_id>', 'notebook'|'sql_file'|'job'|'pipeline'|'table',
  '<source_tool or NULL>', '<path or table_fqn or NULL>', '<job_id/pipeline_id or NULL>', '<json detail>');`
  (For a table artifact use type='table' and put the full `mig_<slug>.<table>` fqn in the path arg.)

### 4. Score deterministically (NOT self-reported)
- Run `confidence.score(converted_code)` from `spine/lib/confidence.py`.
- This returns a 0..1 confidence + findings. **Do not invent a confidence number.**

### 5. Record state + TODOs + audit — ALWAYS via the UC procedures (never raw INSERT/UPDATE)
Use `CALL` against the registry procedures so the audit row is written with the state change,
atomically. Do NOT hand-write INSERT/UPDATE and do NOT invent your own run_id/audit_id.

```sql
-- one per deterministic finding
CALL <catalog>.<schema>.add_todo('<object_id>', '<category>', '<message>', '<severity>');
-- mark code generated. 'converted' means CODE GENERATED, NOT validated — it is an intermediate
-- state, never the happy end. Use needs_review instead if confidence<threshold or a blocker exists.
CALL <catalog>.<schema>.transition('<object_id>', 'converted', <confidence>, '<output_path>', NULL, 'code generated for <target>', '<run_id>');
CALL <catalog>.<schema>.end_run('<run_id>', 'ok');
```
`converted` routing: if `confidence < threshold` (config, default 0.8) OR any finding is a
blocker → go to `needs_review` and STOP (don't validate). Otherwise → `converted`, then validate.

### 5b. VALIDATE gate — run it before claiming success (do NOT skip)
**"converted" = code generated; "validated" = it actually ran and produced output.** A converted
object is NOT done. Validate per `validation_data_mode` (config; default `schema_only`):

- **`schema_only` (default):** static check + dry-run — build the table-dependency graph from the
  notebooks (`spark.read.table` vs `saveAsTable`) and confirm every read is written by an upstream
  task per the job's `depends_on`. This catches topological-order bugs (a task reading a table a
  later task writes) WITHOUT any data. See `references/validation.md`.
- **`synthetic`:** generate sample source data (see `references/synthetic-data-generation.md`),
  `add_artifact` + a standing TODO "validated with synthetic data — re-validate with real data",
  then run the job and check success.
- **`real`:** run the job on the real source data at config `source_data_dir`.

Then:
```sql
-- link the job run for traceability
CALL <catalog>.<schema>.link_run('<run_id>', '<job_run_id>');
-- success → validated ; failure → needs_review + the error as a blocker TODO
CALL <catalog>.<schema>.transition('<object_id>', 'validated', <confidence>, NULL, NULL, 'validated (<mode>): job run <job_run_id> succeeded', '<run_id>');
-- on failure instead:
-- CALL ...add_todo('<object_id>', 'validation', '<captured error text>', 'blocker');
-- CALL ...transition('<object_id>', 'needs_review', <confidence>, NULL, NULL, 'validation failed: <short reason>', '<run_id>');
```
NEVER declare validated without a successful run/dry-run. If data is missing and mode is
`schema_only`, the dry-run still validates structure; only `real`/`synthetic` execute data.

### 6. Report
Tell the user: target emitted, confidence, validation result (mode + pass/fail), open TODOs
(ranked), and next state. Offer to resolve the top TODO interactively (human-in-the-loop repair —
resolve TODOs, re-run the validate gate, still one object at a time).

## Output rules
- One object per run. Do not batch here.
- All state, TODOs, and audit go to the registry — never just printed.
- Confidence is always the deterministic score, never an LLM guess.
- **Never reach `validated` without a successful job run or dry-run** (per validation_data_mode).
- Faithful conversion: prefer a visual/SQL operator over hand-written Python (see pre-checks).
