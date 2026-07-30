-- Genie Migration Factory — catalog + schema (one schema per client engagement)
--
-- NOTE: catalog creation is intentionally NOT here. Many governed workspaces do not
-- grant CREATE CATALOG on the metastore, and engagements usually target an existing
-- catalog. Point ${catalog} at a catalog you already have USE/CREATE SCHEMA on. If you
-- do need a fresh catalog and have the privilege, create it once out-of-band:
--   CREATE CATALOG IF NOT EXISTS <name>;
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema}
  COMMENT 'Migration engagement: registry tables + raw Volume';
