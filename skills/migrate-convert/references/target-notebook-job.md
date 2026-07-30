# Alteryx → Databricks Notebooks + Lakeflow Job

**Target:** Convert Alteryx workflows into **PySpark/SQL notebooks organized by medallion layer, orchestrated by a multi-task Lakeflow Job**

**Best for:** Complex procedural control flow, per-step imperative logic, non-declarative transform patterns, or when the Alteryx source has explicit sequencing / loops that don't map cleanly to Spark Declarative Pipeline (SDP) operators.

---

## When to Choose Notebook + Job Over SDP

| Scenario | Choose Notebook+Job | Choose SDP |
|----------|-------------------|-----------|
| **Control flow** | Explicit loops, branches, conditional skips, multi-branch logic | Declarative pipeline (no loops needed) |
| **Aggregation + join + custom aggregation** | Multiple sequential transforms, windowing, multi-step derivation | Single transform or declarative flow |
| **Imperative logic** | Pandas, Spark procedural style, UDFs, custom row iteration | Spark SQL set operations |
| **Debugging / logging** | Per-stage logging, row sampling, debug writes | Simple pipeline with implicit logging |
| **Streaming vs batch mix** | Per-stage choice (one notebook stream, one batch) | Consistent streaming or batch only |
| **External dependencies** | Call external APIs, multi-step orchestration, checkpoints | No external orchestration |

**Default rule:** SDP is preferred for medallion pipelines (simpler, serverless-native). Use notebooks when Alteryx logic is inherently procedural or requires explicit control.

---

## Conventions (authoritative — set by the skill, do not improvise)

- **Granularity: one notebook per Alteryx tool/node.** Each `<Node ToolID="NN">` becomes its own
  notebook. This maximizes traceability (one notebook ↔ one artifacts row ↔ one job task) and
  handles branching DAGs faithfully. The medallion three-stage pattern shown below is the *simple*
  case (linear flows); for real/branching workflows, decompose per tool.
- **Folder: one shared `output_dir`** (from config `output_dir`). No per-object subfolders.
- **Naming: `<object_slug>__<NN>_<Tool>`** — object_slug = object_id with `:`→`_`, NN = ToolID,
  Tool = plugin short name. E.g. `sample_sales_analytics_complex__11_MultiRowFormula`. This keeps
  every object's notebooks distinct within the shared folder.
- **Job: one task per notebook**, `task_key` = notebook name, `depends_on` = the Alteryx
  connections (the DAG). Create via ai-dev-kit MCP `manage_jobs`.
- **Traceability: register every notebook and the job** with `add_artifact` (see the skill).

## Architecture: Medallion Notebooks + Job (simple/linear case)

### Three-Stage Pipeline Pattern

```
Bronze Notebook (ingest)
  ↓ (depends_on)
Silver Notebook (transform + cleanse)
  ↓ (depends_on)
Gold Notebook (aggregate)
  ↓ (depends_on)
Output (save to delta or external)
```

Each notebook is **independent**, receives catalog/schema via job parameters, and writes its output as a materialized table.

### File Structure

```
notebooks/
  ├── bronze_ingest.py          # Read + audit + minimal transform
  ├── silver_transform.py        # Cleanse + derive + validate
  ├── gold_aggregate.py          # Aggregations + business logic
  └── job_config.yaml            # (Optional) DAB resource definition

migrations/
  └── customer_orders/
      ├── customer_orders_dag.md # Alteryx DAG decomposition notes
      └── job_spec.json          # (For manual inspection or REST API use)
```

---

## Notebook Structure: PySpark Example

### Bronze: Raw Ingest

**File:** `notebooks/bronze_ingest.py`

```python
# Databricks notebook source
# BRONZE INGEST: read source, apply audit columns

# Parameters (injected by Job)
dbutils.widgets.text("catalog", "samples")
dbutils.widgets.text("schema", "migration")
dbutils.widgets.text("source_path", "/mnt/data/orders/")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_path = dbutils.widgets.get("source_path")

# Import
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Read CSV with schema inference
df = spark.read \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .csv(source_path)

# Add audit columns
df_audited = df.withColumn(
  "audit_timestamp", F.current_timestamp()
).withColumn(
  "source_system", F.lit("csv_orders")
)

# Save to bronze table
table_name = f"{catalog}.{schema}.bronze_orders"
df_audited.write \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable(table_name)

print(f"✓ Bronze table written: {table_name}")
print(f"  Rows: {df_audited.count()}")
```

### Silver: Transform + Validate

**File:** `notebooks/silver_transform.py`

```python
# Databricks notebook source
# SILVER TRANSFORM: cleanse, derive, validate

dbutils.widgets.text("catalog", "samples")
dbutils.widgets.text("schema", "migration")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from pyspark.sql import functions as F
from datetime import datetime

# Read from bronze
df = spark.read.table(f"{catalog}.{schema}.bronze_orders")

# Transform
df_silver = df.select(
  F.col("order_id").cast("long").alias("order_id"),
  F.col("customer_id").cast("long").alias("customer_id"),
  F.col("order_date").cast("date").alias("order_date"),
  F.col("status").alias("status"),
  F.col("amount").cast("decimal(10,2)").alias("amount"),
  
  # Derived: year and month
  F.year(F.col("order_date")).alias("order_year"),
  F.month(F.col("order_date")).alias("order_month"),
  
  # Derived: flag invalid rows
  F.when(
    F.col("order_id").isNull() | (F.col("amount") < 0),
    "INVALID"
  ).otherwise("VALID").alias("data_quality_flag"),
  
  F.current_timestamp().alias("audit_timestamp"),
  F.lit("silver_transform").alias("source_system")
)

# Validation: reject rows with null customer_id
df_clean = df_silver.filter(F.col("customer_id").isNotNull())

print(f"Rows dropped (quality): {df_silver.count() - df_clean.count()}")

# Save
table_name = f"{catalog}.{schema}.silver_orders"
df_clean.write \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable(table_name)

print(f"✓ Silver table written: {table_name}")
print(f"  Rows: {df_clean.count()}")
```

### Gold: Aggregate

**File:** `notebooks/gold_aggregate.py`

```python
# Databricks notebook source
# GOLD AGGREGATE: business aggregations

dbutils.widgets.text("catalog", "samples")
dbutils.widgets.text("schema", "migration")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from pyspark.sql import functions as F

# Read from silver
df = spark.read.table(f"{catalog}.{schema}.silver_orders")

# Aggregate: revenue by order_year, order_month
df_gold = df.filter(
  F.col("data_quality_flag") == "VALID"
).groupBy(
  F.col("order_year"),
  F.col("order_month")
).agg(
  F.count(F.col("order_id")).alias("order_count"),
  F.sum(F.col("amount")).alias("total_revenue"),
  F.avg(F.col("amount")).alias("avg_order_value"),
  F.current_timestamp().alias("audit_timestamp"),
  F.lit("gold_aggregation").alias("source_system")
).orderBy("order_year", "order_month")

# Save
table_name = f"{catalog}.{schema}.gold_orders_daily"
df_gold.write \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable(table_name)

print(f"✓ Gold table written: {table_name}")
print(f"  Rows: {df_gold.count()}")
```

---

## Lakeflow Job Specification

### Create Job via ai-dev-kit MCP

**Recommended:** Use the **ai-dev-kit `manage_jobs` MCP tool** (action `create`) instead of hand-writing Job JSON. The MCP tool validates the spec and handles API errors.

**Shape of the job spec:**

```json
{
  "name": "customer_orders_migration",
  "tasks": [
    {
      "task_key": "bronze_ingest",
      "notebook_task": {
        "notebook_path": "/Users/user@databricks.com/notebooks/bronze_ingest",
        "base_parameters": {
          "catalog": "samples",
          "schema": "migration",
          "source_path": "/mnt/data/orders/"
        }
      },
      "compute_key": "job_compute",
      "timeout_seconds": 3600
    },
    {
      "task_key": "silver_transform",
      "notebook_task": {
        "notebook_path": "/Users/user@databricks.com/notebooks/silver_transform",
        "base_parameters": {
          "catalog": "samples",
          "schema": "migration"
        }
      },
      "depends_on": [
        {
          "task_key": "bronze_ingest"
        }
      ],
      "compute_key": "job_compute",
      "timeout_seconds": 3600
    },
    {
      "task_key": "gold_aggregate",
      "notebook_task": {
        "notebook_path": "/Users/user@databricks.com/notebooks/gold_aggregate",
        "base_parameters": {
          "catalog": "samples",
          "schema": "migration"
        }
      },
      "depends_on": [
        {
          "task_key": "silver_transform"
        }
      ],
      "compute_key": "job_compute",
      "timeout_seconds": 3600
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "job_compute",
      "new_cluster": {
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2,
        "aws_attributes": {
          "availability": "SPOT_WITH_FALLBACK"
        }
      }
    }
  ]
}
```

### Key Fields

| Field | Purpose | Example |
|-------|---------|---------|
| `task_key` | Unique task identifier (no spaces, alphanumeric+underscore) | `"bronze_ingest"` |
| `notebook_task.notebook_path` | Workspace path to the notebook | `"/Users/user@databricks.com/notebooks/bronze_ingest"` |
| `base_parameters` | Parameters passed to `dbutils.widgets.get()` | `{"catalog": "samples", "schema": "migration"}` |
| `depends_on[].task_key` | Array of upstream tasks | `[{"task_key": "bronze_ingest"}]` |
| `compute_key` | Reference to job cluster (below) | `"job_compute"` |
| `timeout_seconds` | Max runtime per task | `3600` |

### MCP Tool Usage

Using the ai-dev-kit `manage_jobs` tool to CREATE:

```python
# Pseudocode (actual MCP call)
response = manage_jobs(
  action="create",
  job_config=job_spec_dict  # The JSON spec above
)
job_id = response["job_id"]
print(f"Job created: {job_id}")
```

### Alternative: Databricks Asset Bundle (DAB)

For versioned, repeatable deployments, define the job in `databricks.yml`:

```yaml
resources:
  jobs:
    customer_orders_migration:
      name: customer_orders_migration
      tasks:
        - task_key: bronze_ingest
          notebook_task:
            notebook_path: ${workspace.root_path}/notebooks/bronze_ingest
            base_parameters:
              catalog: samples
              schema: migration
              source_path: /mnt/data/orders/
          job_cluster_key: job_compute
        - task_key: silver_transform
          notebook_task:
            notebook_path: ${workspace.root_path}/notebooks/silver_transform
            base_parameters:
              catalog: samples
              schema: migration
          depends_on:
            - task_key: bronze_ingest
          job_cluster_key: job_compute
        - task_key: gold_aggregate
          notebook_task:
            notebook_path: ${workspace.root_path}/notebooks/gold_aggregate
            base_parameters:
              catalog: samples
              schema: migration
          depends_on:
            - task_key: silver_transform
          job_cluster_key: job_compute
      job_clusters:
        - job_cluster_key: job_compute
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: i3.xlarge
            num_workers: 2

variables:
  workspace:
    root_path: /Workspace/Users/user@databricks.com
```

Deploy with:
```bash
databricks bundle deploy
```

---

## Parameterization & Configuration

### Via Job Parameters

Pass catalog/schema to all notebooks at job creation time:

```json
{
  "task_key": "bronze_ingest",
  "base_parameters": {
    "catalog": "samples",
    "schema": "migration",
    "source_path": "/mnt/data/orders/"
  }
}
```

Inside notebook:
```python
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = f"{catalog}.{schema}.bronze_orders"
```

### Via Notebook Widgets (Alternative)

Define widget defaults in notebook, override at job run:

```python
# In notebook
dbutils.widgets.text("catalog", "samples")
dbutils.widgets.text("schema", "migration")
```

Job spec passes different values:
```json
{
  "base_parameters": {
    "catalog": "prod_catalog",
    "schema": "orders_v2"
  }
}
```

### Environment-Specific Overrides

For multi-environment deployments (dev/staging/prod):

```yaml
# databricks.yml
variables:
  environment: ${env.DATABRICKS_ENV,dev}
  catalog: samples_${environment}
  schema: migration

resources:
  jobs:
    customer_orders_migration:
      tasks:
        - base_parameters:
            catalog: ${var.catalog}
            schema: ${var.schema}
```

Deploy to different environments:
```bash
DATABRICKS_ENV=prod databricks bundle deploy
```

---

## Task Dependencies & DAG Structure

### Linear DAG (Bronze → Silver → Gold)

```
task_key: "bronze_ingest"
  ↓
task_key: "silver_transform"
  depends_on: [{task_key: "bronze_ingest"}]
  ↓
task_key: "gold_aggregate"
  depends_on: [{task_key: "silver_transform"}]
```

### Parallel Tasks (Multiple branches from one table)

From Alteryx example: one source feeds two independent aggregations:

```json
{
  "task_key": "bronze_customers",
  ...
},
{
  "task_key": "gold_by_region",
  "depends_on": [{"task_key": "bronze_customers"}]
},
{
  "task_key": "gold_by_segment",
  "depends_on": [{"task_key": "bronze_customers"}]
}
```

Both gold tasks run in parallel after bronze completes.

### Handling Alteryx Multi-Output Tools

Alteryx **Filter** tool (T/F outputs) or **Formula** (multiple branches):

Create separate tasks for each branch's aggregation:

```
silver_transform (main)
  ├→ gold_completed_orders (filter status = "completed")
  └→ gold_pending_orders (filter status != "completed")
```

Job spec:
```json
{
  "task_key": "gold_completed_orders",
  "depends_on": [{"task_key": "silver_transform"}],
  "notebook_task": {
    "notebook_path": "/notebooks/gold_completed_orders"
  }
},
{
  "task_key": "gold_pending_orders",
  "depends_on": [{"task_key": "silver_transform"}],
  "notebook_task": {
    "notebook_path": "/notebooks/gold_pending_orders"
  }
}
```

Both run in parallel after silver completes.

---

## Example: Customer Orders Workflow

### Alteryx DAG

```
Input (orders.csv) ──→ Filter (completed=true) ──→ Join (customers) ──→ Formula (revenue_tier) ──→ Summarize (count, sum) ──→ Output
Input (customers.csv) ↗
```

### Notebook + Job Decomposition

**Bronze notebooks:**
- `bronze_orders.py` — Read orders.csv
- `bronze_customers.py` — Read customers.csv

**Silver notebook:**
- `silver_orders_cleaned.py` — Filter + join + derive (reads both bronze tables)

**Gold notebook:**
- `gold_orders_summary.py` — Aggregate by revenue_tier

**Job spec:**

```json
{
  "name": "customer_orders_workflow",
  "tasks": [
    {
      "task_key": "bronze_orders",
      "notebook_task": {
        "notebook_path": "/notebooks/bronze_orders",
        "base_parameters": {
          "source_path": "/mnt/data/orders.csv"
        }
      },
      "compute_key": "compute"
    },
    {
      "task_key": "bronze_customers",
      "notebook_task": {
        "notebook_path": "/notebooks/bronze_customers",
        "base_parameters": {
          "source_path": "/mnt/data/customers.csv"
        }
      },
      "compute_key": "compute"
    },
    {
      "task_key": "silver_orders",
      "notebook_task": {
        "notebook_path": "/notebooks/silver_orders_cleaned"
      },
      "depends_on": [
        {"task_key": "bronze_orders"},
        {"task_key": "bronze_customers"}
      ],
      "compute_key": "compute"
    },
    {
      "task_key": "gold_summary",
      "notebook_task": {
        "notebook_path": "/notebooks/gold_orders_summary"
      },
      "depends_on": [{"task_key": "silver_orders"}],
      "compute_key": "compute"
    }
  ],
  "job_clusters": [...]
}
```

### Silver Notebook (join + filter + derive)

```python
# silver_orders_cleaned.py
from pyspark.sql import functions as F

orders = spark.read.table("samples.migration.bronze_orders")
customers = spark.read.table("samples.migration.bronze_customers")

# Join
df_joined = orders.join(
  customers,
  orders.customer_id == customers.customer_id,
  "left"
)

# Filter: completed = true
df_filtered = df_joined.filter(
  F.col("status") == "completed"
)

# Derive: revenue_tier
df_silver = df_filtered.select(
  "*",
  F.when(
    F.col("amount") > 1000, "High"
  ).when(
    F.col("amount") > 500, "Medium"
  ).otherwise("Low").alias("revenue_tier"),
  F.current_timestamp().alias("audit_timestamp"),
  F.lit("silver_join_filter").alias("source_system")
)

df_silver.write.mode("overwrite").saveAsTable(
  "samples.migration.silver_orders"
)
```

### Gold Notebook (summarize)

```python
# gold_orders_summary.py
from pyspark.sql import functions as F

df = spark.read.table("samples.migration.silver_orders")

df_gold = df.groupBy("revenue_tier").agg(
  F.count(F.col("order_id")).alias("order_count"),
  F.sum(F.col("amount")).alias("total_revenue"),
  F.round(F.avg(F.col("amount")), 2).alias("avg_order_value")
).orderBy("revenue_tier")

df_gold = df_gold.withColumn(
  "audit_timestamp", F.current_timestamp()
).withColumn(
  "source_system", F.lit("gold_aggregation")
)

df_gold.write.mode("overwrite").saveAsTable(
  "samples.migration.gold_orders_summary"
)
```

---

## Best Practices

### 1. One Logical Step per Notebook

**✓ DO**: Separate input (bronze) from transform (silver) from aggregate (gold)
- Easier to debug and rerun
- Clearer lineage for data governance
- Parallelizable tasks

**✗ DON'T**: Combine all logic into a single notebook
- Hard to rerun partial workflows
- Debugging becomes painful

### 2. Idempotent Writes

**✓ DO**: Always use `.mode("overwrite")` for medallion tables
```python
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
```

**✗ DON'T**: Use `.mode("append")` — causes duplicate rows on retries

### 3. Explicit Audit Columns

Add `audit_timestamp` and `source_system` in EVERY notebook, last:

```python
df_final = df_final.withColumn(
  "audit_timestamp", F.current_timestamp()
).withColumn(
  "source_system", F.lit("source_description")
)
```

### 4. Use Parameterized Paths

**✓ DO**: Pass catalog/schema as job parameters
```python
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = f"{catalog}.{schema}.bronze_orders"
```

**✗ DON'T**: Hardcode paths
```python
table_name = "samples.migration.bronze_orders"  # ✗ inflexible
```

### 5. Logging & Validation

Add print statements for operational visibility:

```python
print(f"✓ Rows read: {df.count()}")
print(f"✓ Rows written to {table_name}: {df_final.count()}")
```

For validation failures, raise exceptions:

```python
row_count = df_final.count()
if row_count == 0:
  raise ValueError("No rows in output table — check source data")
```

### 6. Job Cluster Sizing

Choose compute based on data volume:

| Volume | Node | Workers | Spark Version |
|--------|------|---------|--------------|
| < 1GB | i3.xlarge | 1–2 | 15.4.x |
| 1–50GB | i3.xlarge | 2–4 | 15.4.x |
| 50GB–1TB | i3.2xlarge | 4–8 | 15.4.x |
| > 1TB | m5.4xlarge or i3.4xlarge | 8+ | 15.4.x |

Use `SPOT_WITH_FALLBACK` to reduce cost:

```json
{
  "aws_attributes": {
    "availability": "SPOT_WITH_FALLBACK"
  }
}
```

---

## Testing & Validation

### Local Run (Before Job Deployment)

Simulate the notebook in Databricks workspace:

1. Attach to a cluster
2. Run notebook manually
3. Verify output table:
   ```python
   spark.read.table("samples.migration.bronze_orders").display()
   ```

### Job Dry Run

Create job, then schedule a manual run:

```bash
# Via CLI
databricks jobs run-now --job-id <job_id>
```

Monitor run in Databricks UI → **Workflows** → **Runs**.

### Validation Checklist

After job succeeds:

- [ ] All three notebooks completed without error
- [ ] Row counts match expected (use `alteryx-output-validation-framework.md`)
- [ ] Schema is correct (check task run output logs)
- [ ] Audit columns are present
- [ ] Tables are in correct catalog/schema
- [ ] No data quality violations in silver layer

---

## Monitoring & Troubleshooting

### Job Failure Diagnosis

**Where to look:**

1. **Workflows UI** — Job status, task duration, error message
2. **Task logs** — Click task name → "View logs"
3. **Notebook output** — Scroll to bottom for print statements and exceptions

### Common Issues

| Error | Cause | Fix |
|-------|-------|-----|
| `FileNotFoundError: /mnt/data/orders.csv` | Source path is wrong or missing | Check `base_parameters.source_path` |
| `Table not found: bronze_orders` | Bronze notebook didn't complete | Check bronze task logs; verify table write |
| `Deadlock or timeout` | Too many workers competing on same table | Reduce num_workers or increase timeout_seconds |
| `Widget not set` | `dbutils.widgets.get()` called without parameter | Pass widget name in job `base_parameters` |

### Enable Debug Logging

Add to notebook:

```python
spark.sparkContext.setLogLevel("DEBUG")
```

Then rerun job and check logs for detailed Spark diagnostics.

---

## Migration Checklist: Notebook + Job

- [ ] Identified Alteryx DAG (linear or branched)
- [ ] Decomposed into bronze / silver / gold stages
- [ ] Created one notebook per stage
- [ ] Each notebook reads parameterized catalog/schema
- [ ] Each notebook adds audit columns (last two columns)
- [ ] Tested notebooks locally on dev cluster
- [ ] Created job spec JSON (or DAB resources)
- [ ] Wired depends_on to match Alteryx DAG
- [ ] Configured job clusters (size + spot settings)
- [ ] Deployed job via MCP `manage_jobs` or `databricks bundle`
- [ ] Manual job run succeeds (all tasks green)
- [ ] Output tables match Alteryx expected results (row count, schema, values)
- [ ] Validated audit columns present
- [ ] Configured alerts (job fails → notify on-call)
- [ ] Documented job schedule (if recurring)

---

## References

- **Databricks Jobs API**: https://docs.databricks.com/en-us/api/workspace/jobs
- **Spark SQL**: https://spark.apache.org/docs/latest/sql-ref.html
- **PySpark**: https://spark.apache.org/docs/latest/api/python/
- **ai-dev-kit MCP**: Refer to MCP tool docs for `manage_jobs` action
- **Databricks Asset Bundle**: https://docs.databricks.com/en-us/dev-tools/bundles
- **Alteryx DAG decomposition**: See `alteryx-migration-pre-checks-decomposition.md`
- **Validation**: See `alteryx-output-validation-framework.md`
