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
# since CREATE SCHEMA/VOLUME don't accept bound parameters).
# When run from the bundle, the notebook sits at spine/deploy/, DDL at spine/ddl/.
candidates = ["../ddl", "spine/ddl", "./ddl"]
ddl_path = next((c for c in candidates if os.path.isdir(c)), None)
if ddl_path is None:
    raise FileNotFoundError(f"DDL directory not found; looked in {candidates} from {os.getcwd()}")

files = sorted(f for f in os.listdir(ddl_path) if f.endswith(".sql"))
print("DDL files:", files)

# COMMAND ----------

def split_statements(text):
    # Strip -- line comments first (a comment may contain ';'), then split on ';'.
    no_comments = "\n".join(
        ln for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("--")
    )
    return [s.strip() for s in no_comments.split(";") if s.strip()]


for fname in files:
    with open(os.path.join(ddl_path, fname)) as fh:
        raw = fh.read()
    sql_text = (
        raw.replace("${catalog}", catalog)
        .replace("${schema}", schema)
        .replace("${volume}", volume)
    )
    for stmt in split_statements(sql_text):
        print(f"[{fname}] {stmt.splitlines()[0][:80]}...")
        spark.sql(stmt)

print("Spine deployed.")

# COMMAND ----------

# Verify
display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))
