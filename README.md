# Genie Migration Factory

A **skills-based framework** to migrate legacy data-platform code (Alteryx, SSIS; more later)
to Databricks, driven by **Genie Code** and running **inside the workspace**.

The spine is a **UC-native, audited migration registry**: source objects and their target UC
objects are **rows in UC tables** pointing to files in a **UC Volume**, and migration is their
audited lifecycle (`discovered → assessed → converted → validated → deployed`). The conversion
engine is a **pluggable step underneath** — we don't build an engine or chase full coverage.

**Success = migration governance:** a registry of N objects with state, confidence, and an audit
trail — you know exactly what migrated clean, what needs review, and why.

## v1 approach — Genie Code, 1-to-1

Start with the simplest runtime: Genie Code converts **one object at a time**, conversationally,
writing to the registry. Parallel/batch via FMAPI `ai_query` is a later "gold" optimization that
swaps the runtime underneath the same spine and the same skills — no rewrite.

**Starting with Alteryx** (not SSIS) on purpose: Alteryx conversion is prompt-based, so Genie
Code reads the `.yxmd` XML directly — **no BladeBridge, no Java, no external orchestration.** The
simplest possible end-to-end slice to validate the spine. SSIS (BladeBridge + Java) plugs into
the same registry afterward.

## Layout

```
spine/            the audited core (source-agnostic)
  ddl/            catalog + schema + Volume + registry tables
  lib/            registry API (register, transition, audit, confidence scoring)
skills/           Genie Code surface (progressive disclosure)
  migrate-assess/   inventory + complexity + TODO forecast → registry
  migrate-convert/  convert one object using the conversion knowledge → registry + audit
  migrate-triage/   query the registry, surface what needs review and why
engines/
  genie_code/     v1 runtime: 1-to-1, the skill IS the runtime
  fmapi_batch/    gold/later: compile skill→ai_query prompt, parallel
samples/alteryx/  test .yxmd workflows
docs/ARCHITECTURE.md
```

## Registry (the spine)

`catalog: migration_factory` / `schema: <client>`:
- **Volume `raw/`** — the objects (files): `alteryx/*.yxmd`, `ssis/*.dtsx`, `staged/`, `output/`
- **`objects`** — one row per object: pointer to Volume + lifecycle state + confidence
- **`runs`** — one row per executed step
- **`audit`** — append-only: who/what/when/config/diff/todos
- **`todos`** — ranked unresolved items
- **`config`** — engagement parameters

Migration status is a SQL query; the artifact is a Volume file.

## Deploy

Linked to a FEVM workspace via Git. `databricks.yml` (DAB) provisions the spine
(catalog/schema/volume/tables); skills install to `/Workspace/Users/<you>/.assistant/skills/`.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full design.
