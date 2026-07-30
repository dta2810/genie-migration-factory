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


def split_procedures(text):
    # Procedures contain ';' inside BEGIN...END, so split on the closing 'END;' line only.
    no_comments = "\n".join(
        ln for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("--")
    )
    blocks, cur = [], []
    for ln in no_comments.splitlines():
        cur.append(ln)
        if ln.strip() == "END;":
            blocks.append("\n".join(cur).strip())
            cur = []
    tail = "\n".join(cur).strip()
    if tail:
        blocks.append(tail)
    return blocks


# gov_catalog defaults to the same catalog; the governance view is optional enrichment.
gov_catalog = catalog

for fname in files:
    with open(os.path.join(ddl_path, fname)) as fh:
        raw = fh.read()
    sql_text = (
        raw.replace("${catalog}", catalog)
        .replace("${schema}", schema)
        .replace("${volume}", volume)
        .replace("${gov_catalog}", gov_catalog)
    )
    # Procedure files (BEGIN...END) need END;-based splitting; plain DDL splits on ';'.
    is_proc_file = "CREATE OR REPLACE PROCEDURE" in sql_text
    stmts = split_procedures(sql_text) if is_proc_file else split_statements(sql_text)
    for stmt in stmts:
        print(f"[{fname}] {stmt.splitlines()[0][:80]}...")
        try:
            spark.sql(stmt)
        except Exception as e:
            # The governance view (05) references governance.pipeline_audit, which may not
            # exist in every engagement. It's optional enrichment — warn and continue.
            if "05_governance" in fname:
                print(f"  SKIPPED (optional governance view): {str(e)[:120]}")
            else:
                raise

print("Spine deployed.")

# COMMAND ----------

# Verify
display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))
