-- Genie Migration Factory — catalog + schema (one schema per client engagement)
CREATE CATALOG IF NOT EXISTS ${catalog}
  COMMENT 'Genie Migration Factory — UC-native audited migration registry';

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema}
  COMMENT 'Migration engagement: registry tables + raw Volume';
