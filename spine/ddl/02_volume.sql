-- Genie Migration Factory — raw Volume (source artifacts + generated output live here)
-- Layout inside the Volume:
--   raw/alteryx/*.yxmd   raw/ssis/*.dtsx   ← uploaded source
--   staged/              ← intermediate conversion output (e.g. BladeBridge, for SSIS)
--   output/              ← generated SDP / DBSQL / notebooks
CREATE VOLUME IF NOT EXISTS ${catalog}.${schema}.raw
  COMMENT 'Source artifacts and generated migration output';
