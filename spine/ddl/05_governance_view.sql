-- Genie Migration Factory — governance join view (P3)
--
-- Ties the spine's LIFECYCLE audit (this framework) to the DATA-QUALITY audit
-- (governance.pipeline_audit, written by the separate governance skill) via
-- runs.orchestrator_run_id = pipeline_audit.update_id.
--
-- Answers: "for migration object X, show its lifecycle AND the data-quality (row counts,
-- PII) of the pipeline it produced." Keeps the two tables separate (different grains,
-- authors, stakeholders) but queryable together.
--
-- NOTE: references ${gov_catalog}.governance.pipeline_audit. If that table doesn't exist
-- in this engagement, skip this file — the view is optional enrichment, not load-bearing.
CREATE OR REPLACE VIEW ${catalog}.${schema}.migration_execution_audit AS
SELECT
  o.object_id,
  o.source_type,
  o.status        AS lifecycle_status,
  o.confidence,
  r.step,
  r.engine,
  r.outcome       AS step_outcome,
  r.started_at,
  r.orchestrator_run_id,
  pa.table_name,
  pa.layer,
  pa.row_count,
  pa.contains_pii,
  pa.pii_columns,
  pa.run_state    AS data_run_state
FROM ${catalog}.${schema}.objects o
JOIN ${catalog}.${schema}.runs r      ON r.object_id = o.object_id
LEFT JOIN ${gov_catalog}.governance.pipeline_audit pa
       ON pa.update_id = r.orchestrator_run_id;
