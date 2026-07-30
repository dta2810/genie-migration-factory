-- Genie Migration Factory — registry tables (the audited spine)
-- Source-agnostic. Artifacts live in the UC Volume; these tables hold state + audit.
-- Parameterized by ${catalog} and ${schema} (one schema per client engagement).

-- One row per migration object: a source artifact + its intended target UC object.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.objects (
  object_id      STRING    NOT NULL COMMENT 'stable id, e.g. <source_type>:<relative_path>[:<sub>]',
  source_type    STRING             COMMENT 'alteryx | ssis | ...',
  object_kind    STRING             COMMENT 'workflow | package | dataflow | task | tool',
  parent_id      STRING             COMMENT 'nesting: a tool belongs to a workflow',
  volume_path    STRING             COMMENT 'path in the raw/ Volume to the source artifact',
  output_path    STRING             COMMENT 'path in output/ once converted',
  target_uc_fqn  STRING             COMMENT 'intended target: catalog.schema.table|view|function',
  layer          STRING             COMMENT 'bronze | silver | gold',
  complexity     STRING             COMMENT 'low | medium | high (from assess)',
  status         STRING    NOT NULL COMMENT 'discovered | assessed | converted | validated | deployed | needs_review',
  confidence     DOUBLE             COMMENT 'deterministic 0..1 score of the conversion output',
  updated_at     TIMESTAMP NOT NULL,
  CONSTRAINT pk_objects PRIMARY KEY (object_id)
)
COMMENT 'Migration objects and their lifecycle state';

-- One row per executed step (assess/convert/validate/deploy) against an object.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.runs (
  run_id              STRING    NOT NULL,
  object_id           STRING    NOT NULL,
  step                STRING    NOT NULL COMMENT 'assess | convert | validate | deploy',
  engine              STRING             COMMENT 'genie_code | fmapi_batch',
  started_at          TIMESTAMP NOT NULL,
  ended_at            TIMESTAMP,
  outcome             STRING             COMMENT 'ok | partial | failed',
  orchestrator_run_id STRING             COMMENT 'external pipeline update_id, links to governance.pipeline_audit (data-quality audit)',
  CONSTRAINT pk_runs PRIMARY KEY (run_id)
)
COMMENT 'Executed migration steps';

-- Append-only audit trail: who/what/when/config/diff/todos for every state transition.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.audit (
  audit_id     STRING    NOT NULL,
  object_id    STRING,
  run_id       STRING,
  event_ts     TIMESTAMP NOT NULL,
  actor        STRING             COMMENT 'user or service principal',
  action       STRING             COMMENT 'state transition or event name',
  from_status  STRING,
  to_status    STRING,
  config_hash  STRING             COMMENT 'hash of the config used for this step',
  detail       STRING             COMMENT 'JSON: diff summary, notes, engine params',
  CONSTRAINT pk_audit PRIMARY KEY (audit_id)
)
COMMENT 'Append-only audit trail of all migration events';

-- Unresolved items surfaced during conversion, ranked for triage.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.todos (
  todo_id     STRING    NOT NULL,
  object_id   STRING    NOT NULL,
  category    STRING             COMMENT 'stored_proc | incremental_merge | untranslated_fn | anti_pattern | manual_review',
  severity    STRING             COMMENT 'blocker | warning | info',
  message     STRING,
  resolved    BOOLEAN            COMMENT 'set true once handled interactively',
  created_at  TIMESTAMP NOT NULL,
  CONSTRAINT pk_todos PRIMARY KEY (todo_id)
)
COMMENT 'Ranked unresolved conversion items for triage';

-- Artifacts produced by a conversion: one row per generated notebook / SQL file / job / pipeline.
-- Gives full traceability from a migration object to every asset it produced.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.artifacts (
  artifact_id   STRING    NOT NULL,
  object_id     STRING    NOT NULL COMMENT 'the migration object this artifact belongs to',
  artifact_type STRING             COMMENT 'notebook | sql_file | job | pipeline | table',
  source_tool   STRING             COMMENT 'the Alteryx tool/node this artifact was generated from (for notebook-per-tool)',
  path          STRING             COMMENT 'workspace path (notebook/sql) — NULL for job/pipeline',
  external_id   STRING             COMMENT 'job_id or pipeline_id — NULL for files',
  detail        STRING             COMMENT 'JSON: task_key, depends_on, layer, notes',
  created_at    TIMESTAMP NOT NULL,
  CONSTRAINT pk_artifacts PRIMARY KEY (artifact_id)
)
COMMENT 'Generated artifacts (notebooks, SQL files, jobs, pipelines) per migration object';

-- Engagement configuration (one row per client): target catalogs, thresholds, prompt overrides.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.config (
  config_key    STRING NOT NULL,
  config_value  STRING,
  CONSTRAINT pk_config PRIMARY KEY (config_key)
)
COMMENT 'Engagement-level configuration';
