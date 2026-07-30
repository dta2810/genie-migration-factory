# Genie Migration Factory — Architecture

> Status: design (Phase 0). Working language: English (files) / Spanish (conversation).

## 1. Reframe — what this framework IS

This is **not** a migration engine that competes with Lakebridge on conversion coverage.
Chasing "cover everything SSIS does" is an infinite race. Instead, borrowing the
**Model Factory** pattern, this is a **UC-native, audited migration control system**:

- **Objects are first-class, tracked entities.** Every source object (an SSIS package,
  a data flow, a task; an Alteryx workflow) and its intended **target UC object**
  (table / view / function) is a **row in a UC table**, pointing to a file in a **UC Volume**.
- **Migration is the lifecycle of those objects** — `discovered → assessed → converted →
  validated → deployed` — modeled as state transitions in UC tables.
- **Everything is audited.** Who, when, which engine, what confidence, which TODOs, which
  config — an append-only audit trail, exactly like the Model Factory's audit-insight column
  and config-driven multi-tenant pattern.

The **conversion engine is a pluggable step underneath the spine**, not the heart. If
Lakebridge improves, we swap the engine; the spine does not change.

**Definition of success for v1** is NOT "converted 95% of the SQL." It is:
*"a UC registry of N objects, each with state, confidence, and an audit trail — I know
exactly what migrated clean, what needs review, and why"* — with mechanical conversion
delegated to the engine. That is **migration governance**, which is what a bank/telco buys
and what neither Lakebridge nor Switch gives you.

## 2. The spine — UC Volume (artifacts) + UC tables (state & audit)

Source artifacts (the *bytes*) live in a **UC Volume**. Metadata, state, and audit (the
*facts about* those artifacts) live in **UC tables** that point to the Volume by path.

```
catalog: migration_factory
  schema: <client>                    ← multi-tenant: one schema + one config per engagement
    VOLUME raw/                         ← OBJECTS (files) live here
      ssis/*.dtsx  alteryx/*.yxmd       ← uploaded source
      staged/                           ← BladeBridge / intermediate conversion output
      output/                           ← generated SDP / DBSQL / notebooks
    TABLE objects                       ← one row per object: pointer to Volume + state
      (object_id, volume_path, source_type, object_kind, parent_id,
       target_uc_fqn, layer[bronze|silver|gold], complexity, status, confidence, updated_at)
    TABLE runs                          ← one row per executed step
      (run_id, object_id, step[assess|convert|validate|deploy], engine, started, ended, outcome)
    TABLE audit                         ← append-only: who/what/when/config_hash/diff/todos_json
    TABLE todos                         ← unresolved items, ranked (stored proc, incremental MERGE, PATINDEX…)
    TABLE config                        ← engagement parameters (target catalogs, prompt overrides, mode thresholds)
```

Logic: the Volume holds the raw `.dtsx` and generated outputs; the UC tables hold the
metadata, state, and audit trail keyed by `volume_path`. **Migration status is a SQL query;
the artifact is a Volume file.** Dashboardable in AI/BI. Audited by design, not a side log.

The object lifecycle (state machine on `objects.status`):

```
discovered → assessed → converted → validated → deployed
     └────────────── (repair) ←───────────────┘   every transition writes to `audit`
```

## 3. Two-plane execution — Genie Code (control) + engine (data)

Genie Code is turn-based, human-in-the-loop, context-window-bounded. It is a **poor batch
engine but an excellent orchestrator**. So:

| Plane | Role | Who |
|-------|------|-----|
| **Control plane** | conversational: assess, launch, monitor, **triage failures/TODOs**, decide waves, approve | **Genie Code + skills** |
| **Data plane** | heavy batch: convert N objects in parallel | **FMAPI** (`ai_query` / Switch Job) |

```
        ┌──────────── GENIE CODE (control plane, skills) ────────────┐
        │  assess → launch → monitor → triage → validate → bundle     │
        │  every action writes state + audit to the UC registry       │
        └──────┬──────────────────────────────────┬───────────────────┘
               │ launches (large estate)            │ interactive (few objects / demo)
               ▼                                    ▼
     FMAPI BATCH (data plane)              CASE-BY-CASE (context window OK)
     ai_query('databricks-claude-…',       Genie Code converts 1 object,
       :prompt || raw_code) over a table    one TODO, one stored proc
               │                                    │
               └──────────► writes results + confidence to `objects`/`audit` ◄──────────┘
                            Genie Code triages by confidence
```

- **Large estate (industrial):** Genie Code launches an FMAPI batch (`ai_query` over the
  staged table, or the Switch Job), then triages the results table by confidence; low-confidence
  rows go to interactive repair.
- **Single object / demo:** Genie Code converts it inline via the skill (no FMAPI) — the case
  where the context window is sufficient.
- **Threshold** (in `config`): ~1–5 objects → interactive; more → FMAPI batch + triage.

The SA never leaves Genie Code; only whether Genie Code *does* or *delegates* the conversion
changes. The "agentic repair loop" is not a separate engine — it is **Genie Code triaging the
TODOs / low-confidence rows the batch returns**.

## 4. The conversion engine (pluggable step, not the heart)

For SSIS we reuse Databricks' official tooling as-is; gaps become **audited TODOs**, not code
we write:

- **Parse `.dtsx` + control flow → Job tasks:** BladeBridge (override JSON
  `workflow_component_mapping`). Deterministic. Needs Java → runs on a cluster web terminal.
- **Convert T-SQL / patterns → SDP or DBSQL:** the **Switch prompts** (612/494/337 lines of
  tested mappings) — but decoupled from the Switch runtime. Two engines can run them:
  - **FMAPI batch** (`ai_query` with the prompt) — data plane, hundreds of files in parallel.
  - **Genie Code + skill** (same prompt as SKILL.md) — no FMAPI, interactive, human-in-the-loop.
- **Validate:** Lakebridge Reconcile.
- **Alteryx:** separate track, adapt the prompt-based `geniecodeskills-alteryxmigration` skills.

Key point: **the Switch prompt content is plain text, not tied to FMAPI.** The same content
is either a batch `ai_query` payload OR a Genie Code SKILL.md — engine choice is a config/scale
decision, not a rewrite.

## 5. In-workspace, no local CLI

- Genie Code (via ai-dev-kit MCP tools: `execute_sql`, `manage_jobs`, Volume read/write) is the
  control plane — reads/writes the Volume, queries the tables, launches/monitors Jobs. All server-side.
- FMAPI `ai_query` runs in-workspace by definition.
- BladeBridge + `run.sh` need Java + Bash → **cluster web terminal** (README-recommended host).
  Serverless-pure BladeBridge re-hosting is a later optimization.

## 6. Scope

**In scope (the value):**
- UC Volume + UC registry tables + append-only audit + object lifecycle state machine.
- Inventory → register each object as a row.
- Assessment / complexity / wave planning as **queries over the registry** (not guesses).
- Two-plane orchestration as **audited steps** that write to the registry.
- Config-driven multi-tenant (one client = one schema + one config).

**Out of scope (deliberately, for v1):**
- Emulating Lakebridge's full conversion coverage. Use its engine; gaps are **audited TODOs**.
- Perfecting hard constructs (stored procs, incremental MERGE) — **registered and ranked**, not
  force-solved in v1.
- Serverless-pure / BladeBridge re-hosting. Later.

## 7. Phase plan (v1: generic spine first, source later)

- **Phase 0 (current):** this design doc + provision FEVM.
- **Phase 1 — generic spine:** build the UC Volume + registry tables + audit + lifecycle state
  machine, **agnostic of source**, proven with **mock objects** (rows + fake files). This is the
  reusable core. Deliverable: `migration_factory` catalog schema + the audit/lifecycle skills.
- **Phase 2 — plug in SSIS:** land `.dtsx` in the Volume, register objects, wire BladeBridge +
  the Switch-prompt conversion (FMAPI batch + Genie Code interactive), write results/TODOs to
  the registry.
- **Phase 3 — governance surface:** AI/BI dashboard over the registry, triage skill, Reconcile
  validation, DAB emit.
- **Phase 4 — Alteryx track** on the same spine + package as Genie Code skill bundle + demo.

## 8. Model Factory lineage

Reuses the patterns from [[project_super_demo_model_factory]]:
- Config-driven multi-tenant (one config per engagement).
- Audit-insight column → here, the whole `audit` table + `todos`.
- Authoring→serving bridge → here, discovered→deployed lifecycle.
- Genie Code + skills as the operator surface.
