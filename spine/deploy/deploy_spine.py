# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy the Migration Factory spine
# MAGIC Creates the catalog, schema, raw Volume, and registry tables by running the
# MAGIC DDL in `spine/ddl/`. Idempotent (`CREATE ... IF NOT EXISTS`). Parameterized by
# MAGIC widgets so one bundle can serve many client engagements.

# COMMAND ----------

dbutils.widgets.text("catalog", "migration_factory")
dbutils.widgets.text("schema", "sandbox")
dbutils.widgets.text("volume", "raw")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

print(f"Deploying spine to {catalog}.{schema} (volume: {volume})")

# COMMAND ----------

import os

# DDL files run in order; ${catalog}/${schema}/${volume} substituted here (not Spark params,
# since CREATE CATALOG/SCHEMA/VOLUME don't accept bound parameters).
ddl_dir = os.path.join(os.path.dirname(os.getcwd()), "ddl")
# When run from the bundle, the notebook sits at spine/deploy/, DDL at spine/ddl/.
# Resolve relative to this notebook's workspace path.
candidates = ["../ddl", "spine/ddl", "./ddl"]
ddl_path = next((c for c in candidates if os.path.isdir(c)), None)
if ddl_path is None:
    # Fall back: inline the DDL directory next to this notebook's parent.
    ddl_path = os.path.join(os.path.dirname(os.path.abspath("__file__")), "..", "ddl")

files = sorted(f for f in os.listdir(ddl_path) if f.endswith(".sql"))
print("DDL files:", files)

# COMMAND ----------

for fname in files:
    with open(os.path.join(ddl_path, fname)) as fh:
        raw = fh.read()
    sql_text = (
        raw.replace("${catalog}", catalog)
        .replace("${schema}", schema)
        .replace("${volume}", volume)
    )
    # A DDL file may contain multiple statements separated by ';'
    for stmt in [s.strip() for s in sql_text.split(";") if s.strip() and not s.strip().startswith("--")]:
        print(f"[{fname}] {stmt.splitlines()[0][:80]}...")
        spark.sql(stmt)

print("Spine deployed.")

# COMMAND ----------

# Verify
display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))
