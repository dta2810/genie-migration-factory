# Alteryx → Databricks SQL Files + SQL Warehouse Job

**Target:** Convert Alteryx workflows into **Databricks SQL files orchestrated by a SQL Warehouse Job**

**Best for:** Pure set-based SQL transforms, no streaming, BI-style aggregations, simplest target for SQL-native teams, or when all transforms can be expressed as CREATE OR REPLACE TABLE / MERGE.

---

## When to Choose SQL Warehouse Over Notebooks / SDP

| Scenario | Choose SQL Warehouse | Choose Notebooks | Choose SDP |
|----------|---------------------|------------------|-----------|
| **Transforms** | Pure SQL (no UDFs, no Pandas) | Procedural, UDFs, custom logic | Declarative pipeline |
| **Compute** | BI teams familiar with SQL | Data eng, Spark developers | Data platform operators |
| **Performance** | Interactive queries, <10 min | Batch, flexible runtimes | Streaming or declarative |
| **Integration** | BI tools (Looker, Tableau), SQL IDE | Workflow orchestration | Pipeline orchestration |
| **Source data** | Well-structured tables | Any format (Parquet, CSV, JSON) | Any format + streaming |
| **Medallion layers** | Bronze/Silver/Gold via SQL only | Flexible staging | Streaming layers |
| **Simplicity** | ✓ Easiest for SQL ops | ✗ More overhead | ✓ Simpler than notebooks |

**Default rule:** SQL Warehouse is ideal for migrations where Alteryx workflows are composed of filters, joins, aggregations, and window functions — all natively SQL.

---

## Architecture: Medallion SQL Files + Job

### Three-Stage Pipeline Pattern

```
bronze_ingest.sql
  ↓ (task 1, no depends_on)
silver_transform.sql
  ↓ (task 2, depends_on task 1)
gold_aggregate.sql
  ↓ (task 3, depends_on task 2)
Optional: Output (export to S3, REST API, etc.)
```

Each `.sql` file is a **complete CREATE OR REPLACE TABLE** statement. The SQL Warehouse Job runs them in sequence via `sql_task`.

### File Structure

```
migrations/
  └── customer_orders/
      ├── bronze_ingest.sql          # CREATE TABLE ... AS SELECT (CTAS)
      ├── silver_transform.sql       # CREATE OR REPLACE TABLE (transform)
      ├── gold_aggregate.sql         # Aggregation CTAS
      ├── job_spec.json              # SQL Warehouse job definition
      └── README.md                  # DAG notes, table lineage
```

---

## SQL File Structure: Medallion Layers

### Bronze: Raw Ingest

**File:** `bronze_ingest.sql`

```sql
-- Bronze: Raw data from source
-- Purpose: Preserve source data with minimal transformation
-- Source: S3 CSV or UC Volume file

CREATE OR REPLACE TABLE samples.migration.bronze_orders (
  order_id           BIGINT           COMMENT 'Unique order identifier',
  customer_id        BIGINT           COMMENT 'References customer',
  order_date         DATE             COMMENT 'Order creation date',
  status             STRING           COMMENT 'Order status: pending, completed, cancelled',
  amount             DECIMAL(10, 2)   COMMENT 'Order total amount',
  audit_timestamp    TIMESTAMP        COMMENT 'Pipeline execution timestamp',
  source_system      STRING           COMMENT 'Source system identifier'
)
COMMENT "Raw order data ingested from CSV"
TBLPROPERTIES (
  "quality" = "bronze",
  "data_owner" = "ops",
  "domain" = "orders"
)
AS
SELECT
  CAST(order_id AS BIGINT) AS order_id,
  CAST(customer_id AS BIGINT) AS customer_id,
  CAST(order_date AS DATE) AS order_date,
  CAST(status AS STRING) AS status,
  CAST(amount AS DECIMAL(10, 2)) AS amount,
  CURRENT_TIMESTAMP() AS audit_timestamp,
  'csv_source' AS source_system
FROM
  read_files(
    path => '/Volumes/samples/migration/incoming/orders/',
    format => 'csv',
    header => true,
    inferSchema => true
  );
```

**Key features:**
- **Inline column descriptions** (required for SDP compliance, even in SQL)
- **CAST to explicit types** (safety from source schema drift)
- **Audit columns last** (`audit_timestamp`, `source_system`)
- **No WHERE clause** (preserve raw data)
- **TBLPROPERTIES** with `quality`, `data_owner`, `domain`

### Silver: Cleanse + Derive + Validate

**File:** `silver_transform.sql`

```sql
-- Silver: Cleaned and enriched order data
-- Purpose: Business-ready data with quality flags
-- Source: bronze_orders

CREATE OR REPLACE TABLE samples.migration.silver_orders (
  order_id              BIGINT           COMMENT 'Unique order identifier',
  customer_id           BIGINT           COMMENT 'References customer',
  order_date            DATE             COMMENT 'Order creation date',
  order_year            INT              COMMENT 'Derived: order year for aggregation',
  order_month           INT              COMMENT 'Derived: order month for aggregation',
  status                STRING           COMMENT 'Order status after validation',
  amount                DECIMAL(10, 2)   COMMENT 'Order amount (validated)',
  revenue_tier          STRING           COMMENT 'Derived: Low / Medium / High based on amount',
  data_quality_flag     STRING           COMMENT 'Row-level DQ: CLEAN or INVALID_<reason>',
  audit_timestamp       TIMESTAMP        COMMENT 'Pipeline execution timestamp',
  source_system         STRING           COMMENT 'Source table'
)
COMMENT "Cleaned orders with quality flags and derived dimensions - CONTAINS PII: customer_id"
TBLPROPERTIES (
  "quality" = "silver",
  "data_owner" = "data-engineering",
  "domain" = "orders",
  "delta.enableChangeDataFeed" = "true"
)
AS
SELECT
  order_id,
  customer_id,
  order_date,
  -- Derived: extract year and month
  YEAR(order_date) AS order_year,
  MONTH(order_date) AS order_month,
  status,
  amount,
  -- Derived: revenue tier classification
  CASE
    WHEN amount > 1000 THEN 'High'
    WHEN amount > 500 THEN 'Medium'
    ELSE 'Low'
  END AS revenue_tier,
  -- Data quality flag
  CASE
    WHEN order_id IS NULL THEN 'INVALID_NULL_ORDER_ID'
    WHEN customer_id IS NULL THEN 'INVALID_NULL_CUSTOMER_ID'
    WHEN amount < 0 THEN 'INVALID_NEGATIVE_AMOUNT'
    WHEN order_date > CURRENT_DATE() THEN 'INVALID_FUTURE_DATE'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  CURRENT_TIMESTAMP() AS audit_timestamp,
  'silver_transform' AS source_system
FROM
  samples.migration.bronze_orders
WHERE
  -- Only ingest from the past year (optional time window filter)
  order_date >= ADD_MONTHS(CURRENT_DATE(), -12);
```

**Key features:**
- **Transformations** as CASE WHEN / CAST / derived columns
- **Window filters** (WHERE) for logical constraints
- **Data quality flag** (CASE statement distinguishing error types)
- **Derivations** (year, month, revenue tier)
- **TBLPROPERTIES** with `delta.enableChangeDataFeed = true` (enable CDC for downstream)

### Gold: Aggregation

**File:** `gold_aggregate.sql`

```sql
-- Gold: Business aggregation for analytics
-- Purpose: Executive-level metrics
-- Source: silver_orders (clean rows only)

CREATE OR REPLACE TABLE samples.migration.gold_orders_daily (
  order_year            INT              COMMENT 'Year of orders',
  order_month           INT              COMMENT 'Month of orders',
  revenue_tier          STRING           COMMENT 'Revenue segment: Low / Medium / High',
  order_count           BIGINT           COMMENT 'Number of orders in segment',
  total_revenue         DECIMAL(12, 2)   COMMENT 'Sum of order amounts',
  avg_order_value       DECIMAL(10, 2)   COMMENT 'Average order amount',
  distinct_customers    BIGINT           COMMENT 'Unique customers in segment',
  audit_timestamp       TIMESTAMP        COMMENT 'Pipeline execution timestamp',
  source_system         STRING           COMMENT 'Source table'
)
COMMENT "Daily order aggregations by year, month, and revenue tier"
TBLPROPERTIES (
  "quality" = "gold",
  "data_owner" = "analytics",
  "domain" = "orders",
  "delta.enableChangeDataFeed" = "true"
)
AS
SELECT
  order_year,
  order_month,
  revenue_tier,
  COUNT(DISTINCT order_id) AS order_count,
  SUM(amount) AS total_revenue,
  ROUND(AVG(amount), 2) AS avg_order_value,
  COUNT(DISTINCT customer_id) AS distinct_customers,
  CURRENT_TIMESTAMP() AS audit_timestamp,
  'gold_aggregation' AS source_system
FROM
  samples.migration.silver_orders
WHERE
  data_quality_flag = 'CLEAN'
GROUP BY
  order_year,
  order_month,
  revenue_tier
ORDER BY
  order_year DESC,
  order_month DESC,
  revenue_tier;
```

**Key features:**
- **GROUP BY** on dimensions (order_year, order_month, revenue_tier)
- **Aggregation functions** (COUNT, SUM, AVG, COUNT DISTINCT)
- **WHERE clause** to filter clean rows only
- **ORDER BY** for deterministic output
- **No individual PII** in aggregated table

---

## SQL Warehouse Job Specification

### Job Definition (REST API / ai-dev-kit MCP)

**Shape of the job spec:**

```json
{
  "name": "customer_orders_migration_sql",
  "tasks": [
    {
      "task_key": "bronze_ingest",
      "sql_task": {
        "file": {
          "path": "/Workspace/migrations/customer_orders/bronze_ingest.sql"
        }
      },
      "warehouse_id": "<warehouse_id>",
      "timeout_seconds": 1800
    },
    {
      "task_key": "silver_transform",
      "sql_task": {
        "file": {
          "path": "/Workspace/migrations/customer_orders/silver_transform.sql"
        }
      },
      "depends_on": [
        {
          "task_key": "bronze_ingest"
        }
      ],
      "warehouse_id": "<warehouse_id>",
      "timeout_seconds": 1800
    },
    {
      "task_key": "gold_aggregate",
      "sql_task": {
        "file": {
          "path": "/Workspace/migrations/customer_orders/gold_aggregate.sql"
        }
      },
      "depends_on": [
        {
          "task_key": "silver_transform"
        }
      ],
      "warehouse_id": "<warehouse_id>",
      "timeout_seconds": 1800
    }
  ]
}
```

### Key Fields

| Field | Purpose | Example |
|-------|---------|---------|
| `task_key` | Unique task identifier | `"bronze_ingest"` |
| `sql_task.file.path` | Workspace path to .sql file | `"/Workspace/migrations/customer_orders/bronze_ingest.sql"` |
| `sql_task.query` | Inline SQL (alternative to `file`) | `"SELECT COUNT(*) FROM table"` |
| `warehouse_id` | SQL Warehouse ID (from UI or API) | `"1234abcd5678ef90"` |
| `depends_on[].task_key` | Upstream task(s) | `[{"task_key": "bronze_ingest"}]` |
| `timeout_seconds` | Max runtime per task | `1800` (30 min) |

### Create Job via ai-dev-kit MCP

**Recommended approach:** Use the ai-dev-kit `manage_jobs` MCP tool:

```python
# Pseudocode (actual MCP call)
response = manage_jobs(
  action="create",
  job_config=job_spec_dict  # The JSON spec above
)
job_id = response["job_id"]
print(f"SQL Warehouse job created: {job_id}")
```

### Alternative: Manual SQL Warehouse Query

For one-off runs, execute SQL files directly in SQL Editor:

1. Open SQL Editor in Databricks UI
2. Select SQL Warehouse
3. Copy + paste contents of `bronze_ingest.sql`
4. Run
5. Repeat for silver and gold files

(Not recommended for production; use job for repeatability.)

---

## Parameterization via SQL Variables

### Catalog/Schema Variables

Define at top of each .sql file:

```sql
-- Variables
DECLARE catalog STRING DEFAULT 'samples';
DECLARE schema STRING DEFAULT 'migration';
DECLARE source_path STRING DEFAULT '/Volumes/samples/migration/incoming/orders/';

-- Use in queries
CREATE OR REPLACE TABLE ${catalog}.${schema}.bronze_orders AS
SELECT ...
FROM read_files(path => ${source_path}, ...);
```

### Job-Level Parameter Injection

Pass variables via `sql_task.parameters`:

```json
{
  "task_key": "bronze_ingest",
  "sql_task": {
    "file": {"path": "/Workspace/migrations/customer_orders/bronze_ingest.sql"},
    "parameters": {
      "catalog": "prod_catalog",
      "schema": "orders_v2",
      "source_path": "/Volumes/prod_catalog/orders/incoming/"
    }
  },
  "warehouse_id": "<warehouse_id>"
}
```

Inside .sql file, reference parameters:

```sql
DECLARE catalog STRING DEFAULT 'samples';
DECLARE schema STRING DEFAULT 'migration';

-- Parameters override defaults if provided
-- Then use: ${catalog}.${schema}.table_name
```

---

## Task Dependencies & DAG Structure

### Linear: Bronze → Silver → Gold

```
task_key: "bronze_ingest"
  ↓
task_key: "silver_transform"
  depends_on: [{task_key: "bronze_ingest"}]
  ↓
task_key: "gold_aggregate"
  depends_on: [{task_key: "silver_transform"}]
```

**Job execution:** Tasks run sequentially. Job succeeds only if all tasks pass.

### Parallel Tasks from One Source

Alteryx example: One source feeds two independent aggregations.

**silver_orders** → **gold_by_region** AND **gold_by_segment** (parallel)

Job spec:
```json
{
  "task_key": "gold_by_region",
  "depends_on": [{"task_key": "silver_transform"}]
},
{
  "task_key": "gold_by_segment",
  "depends_on": [{"task_key": "silver_transform"}]
}
```

Both gold tasks start after silver completes; they run in parallel.

### Multiple Sources Joining

Alteryx: Two separate inputs (orders + customers) join in silver.

**bronze_orders** AND **bronze_customers** (parallel) → **silver_joined** → **gold_summary**

Job spec:
```json
{
  "task_key": "bronze_orders",
  "sql_task": {"file": {"path": ".../bronze_orders.sql"}},
  "warehouse_id": "..."
},
{
  "task_key": "bronze_customers",
  "sql_task": {"file": {"path": ".../bronze_customers.sql"}},
  "warehouse_id": "..."
},
{
  "task_key": "silver_joined",
  "depends_on": [
    {"task_key": "bronze_orders"},
    {"task_key": "bronze_customers"}
  ],
  "sql_task": {"file": {"path": ".../silver_joined.sql"}},
  "warehouse_id": "..."
},
{
  "task_key": "gold_summary",
  "depends_on": [{"task_key": "silver_joined"}],
  "sql_task": {"file": {"path": ".../gold_summary.sql"}},
  "warehouse_id": "..."
}
```

Both bronze tasks start immediately; gold_summary waits for silver.

---

## Example: Customer Orders Workflow

### Alteryx DAG

```
Input (orders.csv) ──→ Filter (status='completed') ──→ Join (customers) ──→ Formula (revenue_tier) ──→ Summarize (count, sum) ──→ Output

Input (customers.csv) ↗
```

### SQL Decomposition

**bronze_orders.sql:**

```sql
CREATE OR REPLACE TABLE samples.migration.bronze_orders
COMMENT "Raw orders from CSV"
TBLPROPERTIES ("quality" = "bronze", "data_owner" = "ops", "domain" = "orders")
AS
SELECT
  order_id, customer_id, order_date, status, amount,
  CURRENT_TIMESTAMP() AS audit_timestamp,
  'csv_orders' AS source_system
FROM read_files(
  path => '/Volumes/samples/migration/incoming/orders/',
  format => 'csv',
  header => true,
  inferSchema => true
);
```

**bronze_customers.sql:**

```sql
CREATE OR REPLACE TABLE samples.migration.bronze_customers
COMMENT "Raw customers from CSV"
TBLPROPERTIES ("quality" = "bronze", "data_owner" = "ops", "domain" = "customer")
AS
SELECT
  customer_id, customer_name, region,
  CURRENT_TIMESTAMP() AS audit_timestamp,
  'csv_customers' AS source_system
FROM read_files(
  path => '/Volumes/samples/migration/incoming/customers/',
  format => 'csv',
  header => true,
  inferSchema => true
);
```

**silver_orders_joined.sql:**

```sql
CREATE OR REPLACE TABLE samples.migration.silver_orders
COMMENT "Joined orders + customers with quality flags"
TBLPROPERTIES ("quality" = "silver", "data_owner" = "data-engineering", "domain" = "orders")
AS
SELECT
  o.order_id,
  o.customer_id,
  c.customer_name,
  c.region,
  o.order_date,
  o.status,
  o.amount,
  -- Filter: only completed orders
  CASE
    WHEN o.status = 'completed' THEN 'INCLUDE'
    ELSE 'EXCLUDED_NOT_COMPLETED'
  END AS filter_flag,
  -- Derive: revenue tier
  CASE
    WHEN o.amount > 1000 THEN 'High'
    WHEN o.amount > 500 THEN 'Medium'
    ELSE 'Low'
  END AS revenue_tier,
  -- Quality flag
  CASE
    WHEN o.order_id IS NULL OR c.customer_id IS NULL THEN 'INVALID_JOIN'
    WHEN o.amount < 0 THEN 'INVALID_NEGATIVE_AMOUNT'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  CURRENT_TIMESTAMP() AS audit_timestamp,
  'silver_join_transform' AS source_system
FROM samples.migration.bronze_orders o
LEFT JOIN samples.migration.bronze_customers c
  ON o.customer_id = c.customer_id;
```

**gold_summary.sql:**

```sql
CREATE OR REPLACE TABLE samples.migration.gold_orders_summary
COMMENT "Order aggregations by revenue tier and region"
TBLPROPERTIES ("quality" = "gold", "data_owner" = "analytics", "domain" = "orders")
AS
SELECT
  revenue_tier,
  region,
  COUNT(DISTINCT order_id) AS order_count,
  SUM(amount) AS total_revenue,
  ROUND(AVG(amount), 2) AS avg_order_value,
  COUNT(DISTINCT customer_id) AS unique_customers,
  CURRENT_TIMESTAMP() AS audit_timestamp,
  'gold_aggregation' AS source_system
FROM samples.migration.silver_orders
WHERE
  data_quality_flag = 'CLEAN'
  AND filter_flag = 'INCLUDE'
GROUP BY
  revenue_tier,
  region
ORDER BY
  revenue_tier,
  region;
```

### Job Spec (4 tasks)

```json
{
  "name": "customer_orders_sql_migration",
  "tasks": [
    {
      "task_key": "bronze_orders",
      "sql_task": {
        "file": {"path": "/Workspace/migrations/customer_orders/bronze_orders.sql"}
      },
      "warehouse_id": "...",
      "timeout_seconds": 1800
    },
    {
      "task_key": "bronze_customers",
      "sql_task": {
        "file": {"path": "/Workspace/migrations/customer_orders/bronze_customers.sql"}
      },
      "warehouse_id": "...",
      "timeout_seconds": 1800
    },
    {
      "task_key": "silver_joined",
      "sql_task": {
        "file": {"path": "/Workspace/migrations/customer_orders/silver_orders_joined.sql"}
      },
      "depends_on": [
        {"task_key": "bronze_orders"},
        {"task_key": "bronze_customers"}
      ],
      "warehouse_id": "...",
      "timeout_seconds": 1800
    },
    {
      "task_key": "gold_summary",
      "sql_task": {
        "file": {"path": "/Workspace/migrations/customer_orders/gold_summary.sql"}
      },
      "depends_on": [{"task_key": "silver_joined"}],
      "warehouse_id": "...",
      "timeout_seconds": 1800
    }
  ]
}
```

---

## SQL Patterns for Alteryx Tools

### Filter (Boolean output)

Alteryx **Filter** tool with T/F outputs → use WHERE in SQL:

```sql
-- Include rows (T output)
WHERE condition = true

-- Exclude rows (F output)
WHERE condition = false
```

Or create two separate tables:

```sql
-- gold_completed_orders.sql
CREATE OR REPLACE TABLE ... AS SELECT * FROM silver WHERE status = 'completed';

-- gold_pending_orders.sql
CREATE OR REPLACE TABLE ... AS SELECT * FROM silver WHERE status != 'completed';
```

### Formula (derived columns)

Alteryx **Formula** tool → use CASE WHEN / CAST / functions in SELECT:

```sql
SELECT
  col1,
  col2,
  -- Simple derivation
  UPPER(col3) AS col3_upper,
  
  -- Conditional derivation
  CASE
    WHEN amount > 1000 THEN 'High'
    ELSE 'Low'
  END AS tier,
  
  -- Type conversion
  CAST(col4 AS DECIMAL(10, 2)) AS amount_decimal,
  
  -- Date calculation
  DATEDIFF(CURRENT_DATE(), order_date) AS days_old
```

### Summarize (GROUP BY)

Alteryx **Summarize** tool → use GROUP BY + aggregation functions:

```sql
SELECT
  region,
  product_category,
  COUNT(*) AS record_count,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_amount,
  COUNT(DISTINCT customer_id) AS unique_customers
FROM table
GROUP BY region, product_category;
```

### Join (equi-join)

Alteryx **Join** tool (L/R/I outputs) → use SQL JOINs:

```sql
-- Left outer join (L output)
SELECT a.*, b.col FROM left_table a
LEFT JOIN right_table b ON a.key = b.key;

-- Right outer join (R output)
SELECT a.*, b.col FROM left_table a
RIGHT JOIN right_table b ON a.key = b.key;

-- Inner join (I output)
SELECT a.*, b.col FROM left_table a
INNER JOIN right_table b ON a.key = b.key;

-- Full outer join (for edge cases)
SELECT a.*, b.col FROM left_table a
FULL OUTER JOIN right_table b ON a.key = b.key;
```

### Multi-Row Formula (window functions)

Alteryx **Multi-Row Formula** → use window functions in SQL:

```sql
SELECT
  customer_id,
  order_date,
  amount,
  -- Running total
  SUM(amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_total,
  
  -- Rank within customer
  ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY order_date DESC
  ) AS rank_by_date,
  
  -- Lag (previous row)
  LAG(amount, 1, 0) OVER (
    PARTITION BY customer_id
    ORDER BY order_date
  ) AS prev_amount
FROM silver_orders;
```

### Union (Combine)

Alteryx **Union** / **Combine** tool → use UNION ALL (or UNION DISTINCT):

```sql
SELECT customer_id, amount, 'Online' AS channel
FROM online_orders
UNION ALL
SELECT customer_id, amount, 'Retail' AS channel
FROM retail_orders;
```

### Pivot / Cross Tab

Alteryx **Cross Tab** / **Transpose** → use PIVOT:

```sql
-- Pivot rows to columns
SELECT *
FROM (
  SELECT region, month, revenue FROM sales
)
PIVOT (
  SUM(revenue)
  FOR month IN ('Jan', 'Feb', 'Mar', 'Apr', ...)
);
```

---

## Best Practices

### 1. One Logical Output per .sql File

**✓ DO**: Separate files for each CREATE TABLE
```
bronze_ingest.sql
silver_transform.sql
gold_aggregate.sql
```

**✗ DON'T**: Multiple CREATE TABLE in one file
```
-- Don't do this:
CREATE TABLE ... ;
CREATE TABLE ... ;
CREATE TABLE ... ;
```

### 2. Explicit Column Types + Comments

**✓ DO**: Inline comments, explicit CAST:
```sql
CREATE TABLE t (
  order_id BIGINT COMMENT 'Unique order ID',
  amount DECIMAL(10, 2) COMMENT 'Order total'
)
```

**✗ DON'T**: Inferred types, no descriptions:
```sql
CREATE TABLE t AS SELECT order_id, amount FROM ...;
```

### 3. Audit Columns Last

**✓ DO**: `audit_timestamp`, `source_system` are LAST columns in every table:
```sql
SELECT col1, col2, col3,
  CURRENT_TIMESTAMP() AS audit_timestamp,
  'source' AS source_system
```

**✗ DON'T**: Put audit columns in the middle or omit them.

### 4. Quality Flags in Silver Layer

**✓ DO**: Add a `data_quality_flag` column (CASE WHEN):
```sql
CASE
  WHEN order_id IS NULL THEN 'INVALID_NULL_ORDER_ID'
  WHEN amount < 0 THEN 'INVALID_NEGATIVE'
  ELSE 'CLEAN'
END AS data_quality_flag
```

**✗ DON'T**: Skip validation or put it only in WHERE clause.

### 5. Idempotent CREATE OR REPLACE

**✓ DO**: Always use `CREATE OR REPLACE TABLE`:
```sql
CREATE OR REPLACE TABLE samples.migration.table_name AS SELECT ...;
```

**✗ DON'T**: Use INSERT or APPEND modes — causes duplicates on retries.

### 6. Use TBLPROPERTIES

**✓ DO**: Tag every table with `quality`, `data_owner`, `domain`:
```sql
TBLPROPERTIES (
  "quality" = "silver",
  "data_owner" = "data-engineering",
  "domain" = "orders"
)
```

**✗ DON'T**: Omit properties or use `owner` (reserved word).

### 7. No `owner` TBLPROPERTY

**✓ DO**: Use `data_owner`:
```sql
"data_owner" = "analytics-team"
```

**✗ DON'T**: Use `owner` — it is RESERVED and causes `UNSUPPORTED_FEATURE` error.

### 8. Column Organization

Organize columns in this order:
1. Identifiers (IDs, keys)
2. Dimensions (categories, attributes)
3. Measures (amounts, counts)
4. Derived columns
5. Quality flags
6. Audit columns (LAST)

```sql
SELECT
  -- Identifiers
  order_id, customer_id,
  
  -- Dimensions
  region, product_category,
  
  -- Measures
  amount, quantity,
  
  -- Derived
  revenue_tier,
  
  -- Quality
  data_quality_flag,
  
  -- Audit (LAST)
  audit_timestamp, source_system
```

### 9. ORDER BY for Determinism

**✓ DO**: Add ORDER BY to gold tables for consistent results:
```sql
ORDER BY order_year DESC, order_month DESC, revenue_tier;
```

**✗ DON'T**: Leave results unordered — non-deterministic behavior.

### 10. SQL Warehouse Sizing

Choose warehouse based on data volume and concurrency:

| Volume | Type | Size |
|--------|------|------|
| < 1GB | Dev/test | Extra small (1 cluster) |
| 1–50GB | Production, occasional | Small (2–4 clusters) |
| 50GB–1TB | Production, frequent | Medium (4–8 clusters) |
| > 1TB | Production, concurrent BI users | Large (8+ clusters) |

Use **Serverless SQL** (no warehouse management) if available in your region.

---

## Testing & Validation

### Local Test (SQL Editor)

1. Open SQL Editor in Databricks UI
2. Select SQL Warehouse
3. Paste .sql file content
4. Click **Run**
5. Verify output table:
   ```sql
   SELECT COUNT(*) FROM samples.migration.gold_orders_summary;
   ```

### Job Dry Run

1. Create job via API or UI
2. Click **Run now**
3. Monitor task progress in **Workflows** > **Runs**
4. Check output tables after all tasks succeed

### Validation Checklist

After job completes:

- [ ] All tasks passed (green status in job run)
- [ ] Row count matches expected (compare with Alteryx output)
- [ ] Schema is correct (columns, types, order)
- [ ] No null audit_timestamp or source_system
- [ ] data_quality_flag values are as expected (CLEAN / INVALID_*)
- [ ] Gold table is aggregated correctly (no duplicates, sum matches)
- [ ] TBLPROPERTIES applied correctly
- [ ] Warehouse executed within timeout_seconds

---

## Monitoring & Troubleshooting

### Job Failure

**Where to look:**

1. **Workflows UI** — Status, error message
2. **Task logs** — Click task name → "View logs" → scroll to error
3. **Query history** — SQL Warehouse "Query history" tab

### Common Issues

| Error | Cause | Fix |
|-------|-------|-----|
| `RESOURCE_ALREADY_EXISTS` on second run | Table exists; CREATE TABLE (not CREATE OR REPLACE) | Use `CREATE OR REPLACE TABLE` |
| `TABLE_NOT_FOUND: bronze_orders` | bronze_orders.sql didn't complete | Check bronze task logs; verify table creation |
| `WAREHOUSE_NOT_FOUND` | warehouse_id is invalid | Get warehouse ID from Warehouse UI |
| `SYNTAX_ERROR` in SQL file | Invalid SQL syntax | Run SQL file in editor manually; fix syntax |
| `Column <col> not found` | Column typo or missing from source | Check source table schema; verify column names |
| Job timeout | Task took > timeout_seconds | Increase timeout or optimize SQL query |

### Enable Query Profiling

To see query performance in SQL Warehouse:

1. Run query in SQL Editor
2. Check **Query profile** (chart icon)
3. Identify slow steps
4. Optimize: add indexes, rewrite predicates, parallelize

---

## SQL Warehouse Job Checklist

- [ ] Identified Alteryx DAG (inputs, transforms, outputs)
- [ ] Decomposed into bronze / silver / gold `.sql` files
- [ ] Each .sql file is a complete CREATE OR REPLACE TABLE
- [ ] All tables have inline column descriptions
- [ ] All tables have TBLPROPERTIES (quality, data_owner, domain)
- [ ] No `owner` in TBLPROPERTIES (use `data_owner` only)
- [ ] Audit columns (audit_timestamp, source_system) are LAST in every table
- [ ] Silver layer has data_quality_flag column
- [ ] Gold layer is aggregated (no individual PII)
- [ ] Tested all .sql files locally in SQL Editor
- [ ] Created job spec JSON with correct depends_on tasks
- [ ] Set warehouse_id (from SQL Warehouse UI)
- [ ] Set timeout_seconds per task (1800–3600)
- [ ] Deployed job via ai-dev-kit MCP or REST API
- [ ] Manual job run succeeds (all tasks green)
- [ ] Output row counts match expected (Alteryx reference)
- [ ] Configured job schedule (if recurring)
- [ ] Set up alerts for job failures

---

## References

- **Databricks SQL**: https://docs.databricks.com/en-us/sql/
- **SQL Warehouse Jobs API**: https://docs.databricks.com/en-us/api/workspace/jobs
- **CREATE TABLE AS SELECT (CTAS)**: https://docs.databricks.com/en-us/sql-reference/statements/create-table-as-select
- **Window Functions**: https://docs.databricks.com/en-us/sql/language-manual/sql-ref-window-functions
- **Databricks conversion standards**: See `databricks-conversion-standards.md`
- **Alteryx DAG decomposition**: See `alteryx-migration-pre-checks-decomposition.md`
- **Validation**: See `alteryx-output-validation-framework.md`
