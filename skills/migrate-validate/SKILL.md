---
name: migrate-validate
description: >
  Validate a converted migration object by checking its generated Lakeflow Job actually runs
  (or passes a structural dry-run) before it can reach 'validated'. Catches topological-order
  bugs (a task reading a table a later task writes) and runtime failures, records the result +
  any error as a TODO in the registry. The converted→validated|needs_review gate.
---

# migrate-validate

**"converted" = code generated; "validated" = it actually ran (or passed a rigorous dry-run) and
produced output.** This skill is the gate. A converted object is NOT done until it passes here.

## When to use
- Right after `migrate-convert` produces a job (called as step 5b of convert), or
- On demand for any object in `converted` / `needs_review` you want to (re)validate.

## Modes (from config `validation_data_mode`; default `schema_only`)

### `schema_only` (default — no data, ~free, catches the 11/12 bug)
There is NO native Databricks Jobs dry-run. Instead do **static table-dependency analysis**
(implemented in `job_validator.py → schema_only_validate`):
1. Extract each task's table I/O from its notebook (`spark.read.table(...)` reads vs
   `saveAsTable(...)` writes).
2. Build the table dependency graph and compare against the job's `depends_on` DAG.
3. Flag any **topological inversion** — a task reads table X but the task that writes X runs
   later (or isn't an upstream dependency). This is exactly the notebook-11-reads-12's-output bug.
4. Also check: notebook paths exist, DAG has no cycles, every read is produced upstream.

Pass → the structure is sound. Fail → `needs_review` + a blocker TODO with the inversion.

### `synthetic` (generate data, then run)
Generate sample source data (see `../migrate-convert/references/synthetic-data-generation.md`),
run the job, check success. Result is AUDITED and carries a standing TODO "validated with
synthetic data — re-validate with real data before production." Never counts as real validation.

### `real` (run on real data)
Run the job on the real source data at config `source_data_dir`, check success.

## Running + capturing errors (job_validator.py)
```python
from job_validator import schema_only_validate, full_validation_run
# Phase 1 (always): free structural check
r = schema_only_validate(job_id, w)          # {'status':'PASS'|'FAIL', 'errors':[...]}
# Phase 2 (synthetic/real only): actually run + capture per-task errors
r = full_validation_run(job_id, w, timeout_minutes=30)  # uses run_now_and_wait; result_state, state_message
```
Error capture: `task.state.state_message` (short) or `w.jobs.get_run_output(run_id).error_trace`
(full) — store the real error text in the TODO.

## Record the result — via UC procedures (never raw SQL)
```sql
-- link the job run for traceability (data-quality audit join)
CALL <catalog>.<schema>.link_run('<run_id>', '<job_run_id or dryrun-id>');
-- PASS:
CALL <catalog>.<schema>.transition('<object_id>', 'validated', <confidence>, NULL, NULL,
  'validated (<mode>): <job run ok / structure sound>', '<run_id>');
-- FAIL:
CALL <catalog>.<schema>.add_todo('<object_id>', 'validation', '<captured error / inversion detail>', 'blocker');
CALL <catalog>.<schema>.transition('<object_id>', 'needs_review', <confidence>, NULL, NULL,
  'validation failed (<mode>): <short reason>', '<run_id>');
```

## Output rules
- NEVER declare `validated` without a PASS from schema_only (minimum) or a successful run.
- Every failure becomes a ranked TODO with the real error, so triage sees why.
- schema_only is the floor — always run it even in synthetic/real mode (cheap, catches structure).
