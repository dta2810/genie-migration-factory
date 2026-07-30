---
name: migrate-assess
description: >
  Inventory a legacy data-platform artifact (Alteryx .yxmd first; SSIS later) and register
  each migration object into the UC migration registry with a complexity score and a forecast
  of items that will need manual review. First step of the migration lifecycle. Use when the
  user says "assess", "inventory", "scan this workflow/package", or points at a file in the
  migration Volume.
---

# migrate-assess

Register source objects into the migration registry and score them **before** any conversion.
This is the `discovered → assessed` step of the lifecycle. It does NOT convert.

## When to use
- The user uploaded a `.yxmd` (Alteryx) — or later a `.dtsx` (SSIS) — to the migration Volume.
- The user asks to inventory / assess / scope a migration.

## Prerequisites
- The spine exists (`migration_factory.<client>`: `objects`, `runs`, `audit`, `todos`, `config`
  tables + `raw` Volume). If not, run the DDL in `spine/ddl/` first.
- Know the target client schema (defaults from `config`).

## Workflow

### 1. Locate the artifact
Confirm the file is in the Volume (`/Volumes/migration_factory/<client>/raw/alteryx/<file>.yxmd`).
Read it. An Alteryx `.yxmd` is XML: `<AlteryxDocument>` with `<Nodes>` (tools) and `<Connections>`.

### 2. Parse the inventory (deterministic, no conversion)
Extract, per workflow:
- **Tools** — each `<Node>` has a `<GuiSettings Plugin="...">` naming the tool
  (e.g. `AlteryxBasePluginsGui.DbFileInput`, `.Filter`, `.Formula`, `.Join`, `.Summarize`,
  `.Union`, `.AppendFields`, `.Output`).
- **Connections** — the DAG between tools (source anchor → destination anchor).
- **Inputs/Outputs** — file/db sources and sinks (these become bronze sources / target tables).
- **Formulas / expressions** — inside Formula and Filter tools (these carry conversion risk).

### 3. Score complexity + forecast TODOs
Classify each workflow `low | medium | high`:
- **low** — linear input→transform→output, standard tools, simple expressions.
- **medium** — joins, unions, multiple outputs, moderate formulas.
- **high** — macros, iterative/batch macros, R/Python tools, dynamic input, spatial, unsupported
  tools, complex nested expressions.

Forecast items that will need manual review and record them as **todos** (not yet resolved):
- Macros / iterative macros → `manual_review`
- R Tool / Python Tool → `manual_review`
- Alteryx functions with no direct Spark equivalent → `untranslated_fn`
- Dynamic Input / Dynamic Rename → `manual_review`

### 4. Register into the spine — ALWAYS via the UC procedures (never raw INSERT/UPDATE)
The registry exposes procedures under `${catalog}.${schema}` that write state AND the audit
row together, so the audit trail stays complete by construction. Call them with `CALL`; do NOT
hand-write INSERT/UPDATE against `objects`/`audit`/`runs`/`todos`.

```sql
-- 1. register the object (idempotent: refreshes assessment fields on re-scan)
CALL <catalog>.<schema>.register_object(
  'alteryx:<name>', 'alteryx', 'workflow', '<volume_path>', NULL,
  '<catalog>.<schema>.<target>', 'gold', 'medium');
-- 2. open the assess run (you supply the run_id)
CALL <catalog>.<schema>.start_run('assess-<name>-<ts>', 'alteryx:<name>', 'assess', 'genie_code');
-- 3. one add_todo per forecast item
CALL <catalog>.<schema>.add_todo('alteryx:<name>', 'untranslated_fn', '<message>', 'warning');
-- 4. advance status (writes the discovered->assessed audit row automatically)
CALL <catalog>.<schema>.transition('alteryx:<name>', 'assessed', NULL, NULL, NULL, '<detail>', 'assess-<name>-<ts>');
-- 5. close the run
CALL <catalog>.<schema>.end_run('assess-<name>-<ts>', 'ok');
```

### 4b. Seed engagement config (first assess of the engagement)
The `config` table is the single source of engagement parameters — `migrate-convert` reads it,
so behavior is config-driven, not hard-coded. On the first assess (or whenever a value changes),
seed it. **Do not skip this** — an unseeded `target` is why conversions previously defaulted to SDP.
```sql
CALL <catalog>.<schema>.set_config('target', 'sdp');            -- sdp | notebook_job | dbsql
CALL <catalog>.<schema>.set_config('confidence_threshold', '0.8');  -- convert->needs_review cutoff
CALL <catalog>.<schema>.set_config('target_catalog', '<catalog>');
CALL <catalog>.<schema>.set_config('target_schema', '<schema>');
-- shared workspace folder where all generated notebooks/SQL land (NOT per-object; notebooks are
-- named <object_slug>__<NN>_<tool> so they stay distinct in the shared folder).
CALL <catalog>.<schema>.set_config('output_dir', '/Workspace/Users/<me>/migration-factory');
```
- **Pick `target` from what the user asked.** "Lakeflow job" / "notebooks" → `notebook_job`;
  "Databricks SQL" / "SQL warehouse" → `dbsql`; "SDP" / "declarative pipeline" / unspecified → `sdp`.
- Read the current target before assuming: `SELECT config_value FROM <catalog>.<schema>.config
  WHERE config_key='target';` — only set it if absent or the user changed their mind.

### 5. Report
Summarize to the user: N objects registered, complexity breakdown, top TODOs, and the suggested
next step (`migrate-convert` on the low-complexity objects first).

## Output rules
- Do not convert anything here — assessment only.
- Every registered object and TODO must be written to the registry, not just printed.
- Keep the assessment reproducible: same file → same inventory + score.

## References
- `references/` — Alteryx tool catalog and complexity heuristics (add as the catalog grows).
