---
name: migrate-triage
description: >
  Query the UC migration registry to show the state of a migration: what migrated clean,
  what needs review and why, ranked open TODOs, and confidence distribution. The governance
  surface — read-only over the registry. Use when the user asks "status", "what needs review",
  "how's the migration going", "show blockers/TODOs", or wants a migration progress report.
---

# migrate-triage

The governance surface. Answers *"what migrated clean, what needs review, and why"* by
querying the registry. **Read-only** — it does not convert or change state.

## When to use
- The user wants migration status, progress, blockers, or a triage list.
- After a batch of `migrate-assess` / `migrate-convert` runs, to decide what to work next.

## Prerequisites
- The spine exists and has objects registered.

## Workflow

### 1. Overall status
- `Registry.summary()` → counts by status + average confidence per status.
- Report the lifecycle funnel: discovered → assessed → converted → validated → deployed,
  plus how many are `needs_review`.

### 2. What needs review (the triage list)
- `objects_by_status('needs_review')` — ordered by complexity desc, confidence asc.
- For each, show: object, complexity, confidence, and its open TODOs
  (`open_todos(object_id)`), so the user sees *why* it needs review.

### 3. Ranked open TODOs across the estate
- `open_todos()` — ranked blocker → warning → info.
- Group by `category` (stored_proc, incremental_merge, untranslated_fn, anti_pattern,
  manual_review) so the user sees the systemic gaps, not just per-object noise.

### 3b. Artifacts produced (traceability)
- For a converted/deployed object, show what it produced:
  `SELECT artifact_type, source_tool, path, external_id FROM <catalog>.<schema>.artifacts
   WHERE object_id = '<id>' ORDER BY created_at`
- This is the object → notebooks/job/pipeline trace: which notebooks were generated (one per
  Alteryx tool), and the `job`/`pipeline` external_id they were wired into.

### 4. Recommend next actions
- Point at the low-complexity `assessed` objects to convert next (quick wins).
- Point at the `needs_review` objects with only warning-level TODOs (fast to clear).
- Flag blocker TODOs that need a decision (e.g. incremental MERGE → implement outside the
  declarative pipeline; stored proc → manual migration).

## Output rules
- Read-only. Never change state or convert here.
- Everything comes from the registry (a SQL query), not from re-reading source files.
- Lead with the takeaway: how many clean, how many need review, top blocker.
