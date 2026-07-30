-- Genie Migration Factory — registry procedures (UC-native audited API)
--
-- WHY: Genie Code writes SQL naturally. Rather than fight that with a Python wheel it
-- can't easily import, we expose the registry operations as UC PROCEDURES. Skills instruct
-- Genie to CALL these instead of hand-writing INSERT/UPDATE, so the audit trail stays
-- complete by construction (every state change writes an audit row inside the procedure)
-- and status transitions are validated in one governed place.
--
-- These mirror spine/lib/registry.py (which stays as the Python reference + unit-test target).
-- Parameterized by ${catalog}.${schema}. Procedures are multi-statement (INSERT + UPDATE + audit).
-- SQL SECURITY INVOKER: runs with the caller's privileges.

-- register_object: insert a new object (discovered) or refresh assessment fields if it
-- already exists; audits 'register' vs 'reregister' — never a phantom 'discovered' on re-scan.
CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.register_object(
  p_object_id     STRING,
  p_source_type   STRING,
  p_object_kind   STRING,
  p_volume_path   STRING,
  p_parent_id     STRING,
  p_target_uc_fqn STRING,
  p_layer         STRING,
  p_complexity    STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS BEGIN
  DECLARE v_exists BOOLEAN DEFAULT FALSE;
  SET v_exists = (SELECT count(*) > 0 FROM ${catalog}.${schema}.objects WHERE object_id = p_object_id);

  MERGE INTO ${catalog}.${schema}.objects t
  USING (SELECT p_object_id AS object_id) s
  ON t.object_id = s.object_id
  WHEN MATCHED THEN UPDATE SET
    t.source_type = p_source_type, t.object_kind = p_object_kind, t.parent_id = p_parent_id,
    t.volume_path = p_volume_path, t.target_uc_fqn = p_target_uc_fqn, t.layer = p_layer,
    t.complexity = p_complexity, t.updated_at = current_timestamp()
  WHEN NOT MATCHED THEN INSERT
    (object_id, source_type, object_kind, parent_id, volume_path, output_path,
     target_uc_fqn, layer, complexity, status, confidence, updated_at)
    VALUES (p_object_id, p_source_type, p_object_kind, p_parent_id, p_volume_path, NULL,
     p_target_uc_fqn, p_layer, p_complexity, 'discovered', NULL, current_timestamp());

  INSERT INTO ${catalog}.${schema}.audit SELECT
    uuid(), p_object_id, NULL, current_timestamp(), current_user(),
    CASE WHEN v_exists THEN 'reregister' ELSE 'register' END,
    NULL, CASE WHEN v_exists THEN CAST(NULL AS STRING) ELSE 'discovered' END,
    CAST(NULL AS STRING), CASE WHEN v_exists THEN 'assessment refreshed' ELSE 'object registered' END;
END;

-- transition: move an object to a new status and write the audit row. Raises if the
-- object doesn't exist (no phantom audit) or the status is unknown.
CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.transition(
  p_object_id     STRING,
  p_to_status     STRING,
  p_confidence    DOUBLE,
  p_output_path   STRING,
  p_target_uc_fqn STRING,
  p_detail        STRING,
  p_run_id        STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS BEGIN
  DECLARE v_from STRING;
  DECLARE v_count INT;
  DECLARE v_guard STRING;

  -- Guards use raise_error() as an EXPRESSION (bare RAISE_ERROR statement is unsupported here).
  IF p_to_status NOT IN ('discovered','assessed','converted','validated','deployed','needs_review') THEN
    SET v_guard = (SELECT raise_error('unknown status: ' || p_to_status));
  END IF;

  SET v_count = (SELECT count(*) FROM ${catalog}.${schema}.objects WHERE object_id = p_object_id);
  IF v_count = 0 THEN
    SET v_guard = (SELECT raise_error('cannot transition unknown object: ' || p_object_id));
  END IF;

  SET v_from = (SELECT status FROM ${catalog}.${schema}.objects WHERE object_id = p_object_id);

  UPDATE ${catalog}.${schema}.objects SET
    status = p_to_status,
    updated_at = current_timestamp(),
    confidence = COALESCE(p_confidence, confidence),
    output_path = COALESCE(p_output_path, output_path),
    target_uc_fqn = COALESCE(p_target_uc_fqn, target_uc_fqn)
  WHERE object_id = p_object_id;

  INSERT INTO ${catalog}.${schema}.audit SELECT
    uuid(), p_object_id, p_run_id, current_timestamp(), current_user(),
    'transition:' || p_to_status, v_from, p_to_status, CAST(NULL AS STRING), p_detail;
END;

-- start_run: open a run row, returning nothing (caller passes its own run_id so it can
-- thread it into transition() and end_run()).
CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.start_run(
  p_run_id    STRING,
  p_object_id STRING,
  p_step      STRING,
  p_engine    STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS BEGIN
  INSERT INTO ${catalog}.${schema}.runs VALUES (
    p_run_id, p_object_id, p_step, p_engine, current_timestamp(), NULL, NULL, NULL);
END;

-- link_run: attach the external pipeline update_id to a run, so the lifecycle audit
-- (this spine) can be joined to the data-quality audit (governance.pipeline_audit).
CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.link_run(
  p_run_id              STRING,
  p_orchestrator_run_id STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS BEGIN
  UPDATE ${catalog}.${schema}.runs SET orchestrator_run_id = p_orchestrator_run_id
  WHERE run_id = p_run_id;
END;

-- end_run: close a run with an outcome.
CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.end_run(
  p_run_id  STRING,
  p_outcome STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS BEGIN
  UPDATE ${catalog}.${schema}.runs SET ended_at = current_timestamp(), outcome = p_outcome
  WHERE run_id = p_run_id;
END;

-- add_todo: record a ranked unresolved item.
CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.add_todo(
  p_object_id STRING,
  p_category  STRING,
  p_message   STRING,
  p_severity  STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS BEGIN
  INSERT INTO ${catalog}.${schema}.todos SELECT
    uuid(), p_object_id, p_category, p_severity, p_message, FALSE, current_timestamp();
END;

-- resolve_todo: mark a todo resolved.
CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.resolve_todo(p_todo_id STRING)
LANGUAGE SQL
SQL SECURITY INVOKER
AS BEGIN
  UPDATE ${catalog}.${schema}.todos SET resolved = TRUE WHERE todo_id = p_todo_id;
END;

-- set_config: upsert an engagement config key.
CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.set_config(p_key STRING, p_value STRING)
LANGUAGE SQL
SQL SECURITY INVOKER
AS BEGIN
  MERGE INTO ${catalog}.${schema}.config t
  USING (SELECT p_key AS config_key) s
  ON t.config_key = s.config_key
  WHEN MATCHED THEN UPDATE SET config_value = p_value
  WHEN NOT MATCHED THEN INSERT (config_key, config_value) VALUES (p_key, p_value);
END;
