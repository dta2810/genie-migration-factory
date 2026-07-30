# Demo script — driving the migration from Genie Code

Exact prompts to type in **Genie Code** (in the FEVM workspace, with the migrate-* skills
installed). The skills do the work and write everything to the audited registry. This walks a
complex Alteryx workflow through the full lifecycle, deploying it as **notebooks in a config-set
folder** (target = `notebook_job`).

**Prereqs**
- Spine deployed: `migration_factory.<schema>` with the 5 tables, `raw` Volume, and the registry
  procedures (`databricks bundle run deploy_spine`).
- Skills installed to `/Users/<you>/.assistant/skills/` (migrate-assess, migrate-convert, migrate-triage).
- The complex sample uploaded to the Volume:
  `/Volumes/<catalog>/<schema>/raw/alteryx/sample_sales_analytics_complex.yxmd`

Replace `<catalog>`/`<schema>` with your engagement (e.g. `dt_serverless_stable_isqgt5_catalog` /
`migration_factory`).

---

## 0. Set the target to notebooks (config-driven)

Before converting, tell the engagement to emit notebooks (not the default SDP), and where to put
them (one SHARED folder — notebooks are named `<object>__<NN>_<tool>` so they stay distinct):

```
Set the migration target to notebook_job, and set output_dir to
/Workspace/Users/<me>/migration-factory
```

Genie should `CALL <catalog>.<schema>.set_config('target','notebook_job')` and
`set_config('output_dir', '...')`. Confirm with:

```
What's the current migration config?
```

## 1. Assess

```
Assess the Alteryx workflow at
/Volumes/<catalog>/<schema>/raw/alteryx/sample_sales_analytics_complex.yxmd
```

Expect: object registered → `assessed`, an inventory of ~12-16 tools + the branching DAG, a
complexity score, and **forecast TODOs** for the manual-review constructs (MultiRowFormula →
window function; DateTimeParse / REGEX → no clean Spark equivalent). All written to the registry
via `CALL` (not raw SQL).

## 2. Convert (as notebooks, into the config folder)

```
Convert it — follow the configured target
```

Expect: because `target=notebook_job`, Genie writes **one notebook per Alteryx tool** into the
shared `output_dir` (named `sample_sales_analytics_complex__<NN>_<tool>`), wires them into a
**Lakeflow Job** via the ai-dev-kit MCP tools (task per notebook, depends_on = the DAG) — NOT an
SDP pipeline. It registers each notebook + the job in the `artifacts` table, scores confidence
deterministically, records TODOs, and transitions to `converted` or `needs_review`.

## 3. Triage / status

```
Show me the migration status — what converted, what needs review, and why
```

Expect: the lifecycle funnel, the needs-review list with reasons, and the ranked open TODOs
(the MultiRowFormula/DateTimeParse items), read from the registry.

## 4. Work a TODO interactively (the human-in-the-loop repair)

```
For the running-total TODO, implement it as a Spark window function and resolve the TODO
```

## 5. Advance the lifecycle

```
Validate the pipeline against the source logic, then advance to validated
```
```
Advance the migration status to deployed
```

---

## What to verify (this run proves the hardening)

- **CALL, not raw SQL** — Genie uses `CALL <catalog>.<schema>.transition(...)` etc. (finding P1).
- **No leaked standards** — no `owner` TBLPROPERTY, no `CLUSTER BY AUTO` + zOrder combo, because
  the global Genie-Suite instructions were removed (findings P4/P5).
- **Notebooks, not SDP** — output honors `target=notebook_job` from config (finding P2/P3).
- **Audit trail complete** — every transition has an audit row:
  `SELECT action, from_status, to_status, actor FROM <catalog>.<schema>.audit ORDER BY event_ts`
- **Artifacts traced** — every notebook + the job registered:
  `SELECT artifact_type, source_tool, path, external_id FROM <catalog>.<schema>.artifacts
   WHERE object_id='alteryx:sample_sales_analytics_complex' ORDER BY created_at`
  (expect ~15 notebook rows — one per tool — plus one `job` row with the job_id.)
