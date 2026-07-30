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

### 4. Register into the spine
For each object, call the registry API (`spine/lib/registry.py`) via SQL/MCP:
- Insert/upsert a row in `objects` (`status='assessed'`, `complexity`, `volume_path`,
  proposed `target_uc_fqn`, `layer`).
- Insert `todos` rows for each forecast item, ranked by `severity`.
- Insert a `runs` row (`step='assess'`, `engine='genie_code'`, `outcome`).
- Insert an `audit` row for the `discovered → assessed` transition (actor, config_hash, detail).

### 5. Report
Summarize to the user: N objects registered, complexity breakdown, top TODOs, and the suggested
next step (`migrate-convert` on the low-complexity objects first).

## Output rules
- Do not convert anything here — assessment only.
- Every registered object and TODO must be written to the registry, not just printed.
- Keep the assessment reproducible: same file → same inventory + score.

## References
- `references/` — Alteryx tool catalog and complexity heuristics (add as the catalog grows).
