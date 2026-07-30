# Alteryx Migration: Pre-Checks, Decomposition Rules & Architectural Guidance

**Source:** `geniecodeskills-alteryxmigration` SKILL.md documents  
**Last updated:** 2026-07-30  
**Context:** Mandatory steps before migration, operator selection rules, and logical decomposition patterns

---

## 1. Pre-Migration Mandatory Checklist

**MUST be completed before writing ANY migration code.** Do NOT proceed until all items are confirmed.

### 1.1 Expected Output File (CRITICAL)

**Requirement:** You must have a reference output file representing the correct results of the Alteryx workflow.

**Accepted formats:**
- CSV / Parquet / Delta / Excel / Avro / ORC
- Any tabular format
- Minimum: column names, row count, and 5-10 sample rows

**If the user has NOT provided an expected output file, STOP immediately and ask:**

```
I need an expected output file to validate the migrated code against the original 
Alteryx workflow results. Please provide one of the following:

1. A CSV/Parquet/Excel file with the expected output data
2. A path to an existing Delta table with expected results
3. A sample of the expected output (at minimum: column names, row count, and 5-10 sample rows)

Without this, I cannot guarantee the migration produces correct results.
```

**Action after obtaining the file:**
1. Load it and profile it: row count, column names, dtypes, sample rows, null counts
2. Store as validation baseline
3. Use for quality assurance after migration

### 1.2 Output Save Location (MANDATORY)

**Requirement:** Confirm where all migration artifacts and output data must be saved.

**If the user has NOT specified, ask explicitly:**

```
Where should I save the migration artifacts? I need:

1. **Notebook save path** — e.g., /Workspace/Users/you@company.com/migrations/workflow_name
2. **Output data location** — a Unity Catalog target: catalog.schema.table_name
   OR a Volume path: /Volumes/catalog/schema/volume/path
3. **Medallion tier** — Which layer does this output belong to? (Bronze / Silver / Gold)
```

**Ensure clarity on:**
- Catalog and schema names (must exist or will be created)
- Table naming convention (snake_case)
- Partition strategy (if any)
- Expected refresh frequency (one-time, daily, weekly, etc.)

### 1.3 Source Data Inventory

**Requirement:** Identify all input data sources from the Alteryx workflow.

**Audit the workflow for:**
- **File paths** (CSV, Excel, Parquet, JSON) — note full paths
- **Database connections** (SQL Server, Oracle, PostgreSQL, Snowflake, etc.) — note connection details
- **API endpoints** — capture URLs, authentication method
- **Alteryx Gallery data connections** — note source
- **Directory tools** — list folder paths
- **Dynamic inputs** — parameter-driven file paths

**For each source, map to Databricks equivalent:**
- File → UC Volume or external location
- Database → JDBC, UC Federation, or Lakehouse Federation
- API → Python ingestion operator
- Alteryx native → export to CSV/Parquet first

### 1.4 Tool & Configuration Inventory

**Before starting the conversion:**
1. Read the `.yxmd` XML file (or user description)
2. Identify every `<Node>` — extract `<Plugin>` attribute
3. Build a directed graph from `<Connection>` elements
4. For each node, extract:
   - Transformation logic (Filter expression, Formula, etc.)
   - Join keys, group-by fields
   - Output field names and types
5. Identify **branching**:
   - Filter T/F outputs
   - Join L/J/R outputs
   - Multi-way splits
6. Flag **special cases**:
   - Macros (`.yxmc` references)
   - Interface tools (Analytic App forms)
   - Reporting tools (Render, Email)
   - Spatial operations (Trade Area, Spatial Match)

### 1.5 Risk Assessment

**Flag high-risk patterns early:**
- Iterative macros → **MANUAL — requires Job loop**
- Spatial operations → **MANUAL — check Sedona availability**
- Custom Alteryx tools (`.yxi`) → **MANUAL — port to Python or UDO**
- `.yxdb` data sources → **MANUAL — export to CSV first**
- Analytic App interface tools → **MANUAL — replace with parameters or Databricks App**
- Dynamic input with complex macros → **Flag for review**

---

## 2. Mandatory Visual Operator Pre-Check

**Before writing ANY `sql` or `python` operator, answer ALL five questions in order.**

If you write `sql` or `python` without answering all five, the operator choice is wrong.

```
QUESTION 1: Can a Transform express this?
   ✓ CASE WHEN, CAST, COALESCE, TRIM, UPPER, SOUNDEX, 
     REGEXP_EXTRACT, SPLIT + ELEMENT_AT, DATEDIFF, 
     arithmetic, literals, passthrough(*)
   → YES → USE TRANSFORM. STOP.

QUESTION 2: Can a Filter express this?
   ✓ Boolean row condition (WHERE clauses)
   → YES → USE FILTER. STOP.

QUESTION 3: Can an Aggregate express this?
   ✓ GROUP BY + SUM/AVG/COUNT/MIN/MAX/MEDIAN/STDDEV/VARIANCE/PERCENTILE
   → YES → USE AGGREGATE. STOP.

QUESTION 4: Can a Join express this?
   ✓ Equi-join on key columns
   → YES → USE JOIN. STOP.

QUESTION 5: Can a Sort, Limit, Pivot, or Combine express this?
   ✓ ORDER BY, TOP N, reshape wide↔tall, UNION/INTERSECT/EXCEPT
   → YES → USE THE VISUAL OPERATOR. STOP.

IF ALL FIVE ARE NO:
   → Only then may you proceed to sql or python.
```

**Priority hierarchy:**
1. **Visual operators** (Transform, Filter, Aggregate, Join, Sort, Limit, Pivot, Combine) — ALWAYS preferred
2. **AI Functions** — ONLY when: creative/generative text output + low cardinality + no deterministic equivalent
3. **SQL operator** — ONLY for: window functions, COUNT DISTINCT, CTEs, subqueries, EXPLODE/SEQUENCE
4. **Python operator** — ABSOLUTE LAST RESORT: file I/O, ML code, external libraries

---

## 3. One Logical Step = One Operator Rule

**Default architecture (ALWAYS use this):**

Each distinct transformation purpose → its own operator node.

**NEVER consolidate multiple unrelated steps into one SQL or Python operator.**

### 3.1 Example: Fuzzy Match + Ranking

| Step | Alteryx Tool | Operator | Purpose |
|------|--------------|----------|---------|
| 1 | Formula: SOUNDEX(name) | `transform` | Derive phonetic match key |
| 2 | Formula: REGEXP_EXTRACT(email, ...) | `transform` (same cell) | Extract domain from email |
| 3 | SQL: DENSE_RANK() OVER (...) | `sql` | Assign group ID based on soundex + domain |
| 4 | SQL: LAG() OVER (...) | `sql` (separate from step 3) | Calculate lag for trend detection |
| 5 | Formula: DATEDIFF(txn_dt, prev_dt) | `transform` | Compute days since prior transaction |

**✅ CORRECT:** 5 separate operators (Transform → Transform → SQL → SQL → Transform)  
**❌ WRONG:** Consolidate all into one SQL CTE block without asking the user first

### 3.2 Consolidation Rule

Only consolidate SQL nodes when:
- Both perform **window functions** AND are logically dependent (e.g., step 2 depends on step 1's output)
- User explicitly approves

**Always ASK before consolidating:**
```
These two window functions could be combined into one SQL node for compactness, 
or kept as separate nodes for clarity. Which do you prefer?
```

**Default is always MORE operators.** Simplicity and traceability > compactness.

---

## 4. Decomposition Patterns for Common Alteryx Tools

### 4.1 Alteryx Filter Tool (T/F Outputs)

**Alteryx structure:**
- 1 Filter tool with a boolean condition
- 2 outputs: T (true/matching rows) and F (false/non-matching rows)

**Conversion to VDP:**
```yaml
# Step 1: Main flow — keep matching rows
- id: filter_condition_true
  template: filter
  name: filter_matching_rows
  config:
    condition: "status = 'active' AND region IN ('US', 'CA')"
  input:
    - node: upstream_source
      input_port: data
      output_port: data

# Step 2: Secondary flow — keep non-matching rows (inverse condition)
- id: filter_condition_false
  template: filter
  name: filter_non_matching_rows
  config:
    condition: "NOT (status = 'active' AND region IN ('US', 'CA'))"
  input:
    - node: upstream_source
      input_port: data
      output_port: data
```

**Note:** Run both in parallel if both downstream branches are needed; or use only the True branch if F output is not used.

### 4.2 Alteryx Join Tool (L/J/R Outputs)

**Alteryx structure:**
- 1 Join tool
- 3 outputs: L (left unmatched), J (joined/matched), R (right unmatched)

**Conversion to VDP (Option 1: SQL with UNION ALL):**
```yaml
- id: join_all_branches
  template: sql
  name: join_with_branches
  config:
    query: |
      -- Joined rows (J)
      SELECT l.*, r.* FROM left_table l
      INNER JOIN right_table r ON l.key = r.key
      
      UNION ALL
      
      -- Left unmatched (L)
      SELECT l.*, NULL FROM left_table l
      LEFT ANTI JOIN right_table r ON l.key = r.key
      
      UNION ALL
      
      -- Right unmatched (R)
      SELECT NULL, r.* FROM right_table r
      LEFT ANTI JOIN left_table l ON l.key = r.key
  input:
    - node: left_source
      input_port: data
      output_port: data
    - node: right_source
      input_port: data
      output_port: data
```

**Conversion to VDP (Option 2: Visual Join + separate SQL):**
```yaml
# Step 1: Visual join for matched rows
- id: join_matched
  template: join
  name: join_matched
  config:
    join_type: inner
    join_conditions: "left.key = right.key"
  input:
    - node: left_source
      input_port: left
      output_port: data
    - node: right_source
      input_port: right
      output_port: data

# Step 2: SQL for unmatched branches if needed (downstream)
- id: left_anti_join
  template: sql
  name: left_unmatched
  config:
    query: |
      SELECT l.* FROM left_source l
      LEFT ANTI JOIN right_source r ON l.key = r.key
  input:
    - node: left_source
      input_port: data
      output_port: data
    - node: right_source
      input_port: data
      output_port: data
```

### 4.3 Alteryx Summarize Tool (Fan-out Aggregation)

**Pattern:** One source → 5+ parallel Summarize chains (one per granularity like Period, YTD, Region)

**❌ Naive approach:** 5 separate `aggregate` operators + 5 joins + 5 sorts = 15 cells

**✅ Optimized approach:** Consolidate into ONE `sql` operator with UNION ALL per granularity

```yaml
- id: consolidated_aggregation
  template: sql
  name: summarize_all_granularities
  config:
    query: |
      -- Period granularity
      SELECT 'Period' AS Granularity, CAST(Period AS STRING) AS Granularity_Value,
             SUM(revenue) AS total_revenue,
             SUM(quantity) AS total_quantity,
             COUNT(*) AS row_count
      FROM source_data
      GROUP BY Period
      
      UNION ALL
      
      -- YTD granularity
      SELECT 'YTD' AS Granularity, 
             CONCAT('YTD_', CAST(YEAR(date) AS STRING)) AS Granularity_Value,
             SUM(revenue) AS total_revenue,
             SUM(quantity) AS total_quantity,
             COUNT(*) AS row_count
      FROM source_data
      GROUP BY YEAR(date)
      
      UNION ALL
      
      -- Region granularity
      SELECT 'Region' AS Granularity, Region AS Granularity_Value,
             SUM(revenue) AS total_revenue,
             SUM(quantity) AS total_quantity,
             COUNT(*) AS row_count
      FROM source_data
      GROUP BY Region
      
      UNION ALL
      
      -- Region-Period cross-tab
      SELECT 'Region-Period' AS Granularity, 
             CONCAT(Region, '_', CAST(Period AS STRING)) AS Granularity_Value,
             SUM(revenue) AS total_revenue,
             SUM(quantity) AS total_quantity,
             COUNT(*) AS row_count
      FROM source_data
      GROUP BY Region, Period
  input:
    - node: source_table
      input_port: data
      output_port: data
```

**Reduction:** 38 cells → 10 cells (60% fewer operators)

### 4.4 Alteryx Multi-Row Formula (Window Functions)

**Pattern:** Moving average, running total, LAG/LEAD, ranking

**Step 1 — Derive sort key (if needed):**
```yaml
- id: add_sort_key
  template: transform
  name: add_sort_key
  config:
    expressions:
      - "*"
      - "DATE_FORMAT(transaction_date, 'yyyy-MM-dd') AS sort_date"
  input:
    - node: upstream
      input_port: data
      output_port: data
```

**Step 2 — Apply window function:**
```yaml
- id: apply_window
  template: sql
  name: apply_window_function
  config:
    query: |
      SELECT *,
             LAG(amount) OVER (PARTITION BY customer_id ORDER BY sort_date) AS prev_amount,
             SUM(amount) OVER (PARTITION BY customer_id ORDER BY sort_date ROWS UNBOUNDED PRECEDING) AS running_total
      FROM add_sort_key
  input:
    - node: add_sort_key
      input_port: data
      output_port: transformed_data
```

**Step 3 — Derive metrics (if needed):**
```yaml
- id: calculate_delta
  template: transform
  name: calculate_delta
  config:
    expressions:
      - "*"
      - "CASE WHEN prev_amount IS NOT NULL THEN amount - prev_amount ELSE NULL END AS amount_delta"
  input:
    - node: apply_window
      input_port: data
      output_port: result
```

---

## 5. Medallion Layer Assignment Decision Tree

```
START: Examine Alteryx workflow stage

├─ Raw data ingestion (Input Data, Directory, Download)
│  └─ → BRONZE
│     Action: Add metadata (_ingested_at, _source_file), preserve raw structure
│
├─ Cleansing & deduplication (Filter, Data Cleansing, Unique, Join for lookups)
│  └─ → SILVER
│     Action: Apply business rules, enforce schema, standardize names
│
├─ Aggregation & summarization (Summarize, Cross Tab, Join + Agg combinations)
│  └─ → GOLD
│     Action: Calculate metrics, optimize for BI queries
│
├─ Reporting & visualization (Render, Email, Chart tools)
│  └─ → GOLD (output to Delta) + Lakeview dashboard
│     Action: Materialize as Delta; build visual dashboard
│
├─ Macros & Iteration (Batch Macro, Iterative Macro)
│  └─ → Decision point:
│     ├─ Standard/Batch → inline or extract to Python
│     └─ Iterative → Lakeflow Job with loop
│
└─ Predictive / Spatial (ML models, Trade Area, Spatial Match)
   └─ → Separate pipeline or **MANUAL review**
      Action: Log to MLflow; enable Sedona if needed
```

---

## 6. Visual Operator vs SQL Operator Lookup

**Use this table when deciding between visual operator and SQL:**

| Requirement | Operator | Example |
|---|---|---|
| **Simple columns selection + reorder** | `transform` | Select cols 1,3,5 in new order |
| **Add constant column** | `transform` | `5.0 AS trade_area_radius_km` |
| **Type conversion** | `transform` | `CAST(amount AS DOUBLE) AS amount_numeric` |
| **Simple CASE WHEN** | `transform` | `CASE WHEN region = 'X' THEN ... END` |
| **String functions** | `transform` | `UPPER(name), TRIM(address), SOUNDEX(...)` |
| **Date functions** | `transform` | `DATEDIFF(end_date, start_date)`, `TO_DATE(...)` |
| **Regex extract/replace** | `transform` | `REGEXP_EXTRACT(email, ...), REGEXP_REPLACE(...)` |
| **Simple arithmetic** | `transform` | `quantity * unit_price * (1 - discount)` |
| **Row filtering** | `filter` | `WHERE status = 'active' AND region IN (...)` |
| **GROUP BY + SUM/AVG/COUNT/MIN/MAX** | `aggregate` | Group by region, sum revenue, count orders |
| **GROUP BY with unsupported aggs** | `sql` | GROUP BY with COUNT(DISTINCT), FIRST_VALUE, collect_list |
| **Simple equi-join** | `join` | INNER/LEFT/RIGHT on matching keys |
| **Window functions** | `sql` | ROW_NUMBER(), RANK(), LAG/LEAD, SUM() OVER |
| **UNION / INTERSECT / EXCEPT** | `combine` | Merge two tables, find differences |
| **Pivot (Rows → Columns)** | `pivot` | Cross-tab transformation |
| **Unpivot (Columns → Rows)** | `pivot` (mode: unpivot) | `UNPIVOT` style reshape |
| **ORDER BY** | `sort` | Sort by multiple columns ASC/DESC |
| **TOP N / LIMIT** | `limit` | Keep first N rows |
| **Complex multi-source aggregation** | `sql` | Multiple CTEs with UNION ALL |
| **Subqueries / self-referencing** | `sql` | SELECT FROM (SELECT ...) |

**The 5-question pre-check prevents 90% of wrong operator choices.**

---

## 7. Anti-Patterns to Avoid

| ❌ Anti-Pattern | ✅ Correct Approach | Why |
|---|---|---|
| Python with `F.withColumn(*, F.case(...))` | Use `transform` with CASE WHEN | Visual is clearer; Python is last resort |
| SQL for `COALESCE(a, b, c)` | Use `transform` with COALESCE | Transform can handle it |
| SQL for `UPPER(col)` / `TRIM(col)` | Use `transform` with UPPER/TRIM | Transform is the right tool |
| AI Function on millions of rows for classification | `transform` CASE WHEN or join lookup table | Cost/latency explosion; deterministic preferred |
| Monolithic Python (groups + transforms + joins + CSV write) | Decompose: Aggregate → Transform → SQL (windows) → Transform → Python (CSV only) | Traceability, testability, reuse |
| Merging unrelated windows into one SQL without asking user | Ask user before consolidating; default is separate SQL nodes | User preference; transparency |
| `.cache()` on serverless compute | Use temp Delta table pattern with `materialize()` | `.cache()` not supported on serverless |
| `SELECT *` in final output before `combine` | Align schemas explicitly on both branches with `transform` | Heterogeneous unions fail |
| Skipping the 5-question pre-check | Answer all 5 before writing `sql` or `python` | Ensures right operator is chosen |

---

## 8. Materialization Strategy (Performance)

### 8.1 When to Materialize

**Always materialize a DataFrame if it is used in N > 1 downstream actions.**

| Pattern | Materialize? | Method |
|---|---|---|
| Output → used once | No | Pass through to output operator |
| Reused in 2+ joins | **YES** | Temp Delta table (serverless) or `.cache()` (classic) |
| Reused in join + aggregation | **YES** | Temp Delta table or cache |
| Branch point (feeds multiple paths) | **YES** | Temp Delta table or cache |
| After expensive transformation (join on 1B rows) | **YES** | Temp Delta table or cache |
| Reference table < 100MB (used in joins) | **NO** | Use broadcast instead: `F.broadcast(df)` |

### 8.2 Classic Cluster vs Serverless

**Classic cluster:**
```python
df.cache()  # or .persist()
# Use df multiple times
df.unpersist()  # optional cleanup
```

**Serverless compute (`.cache()` NOT supported):**
```python
def materialize(df, name):
    """Write DataFrame to temp Delta table and read back."""
    table = f"catalog.schema._tmp_{name}"
    df.write.mode("overwrite") \
        .option("delta.columnMapping.mode", "name") \
        .saveAsTable(table)
    _TEMP_TABLES.append(table)
    return spark.table(table)

# Usage
df_materialized = materialize(df_expensive, "expensive_branch")
result1 = df_materialized.join(lookup_df, ...)
result2 = df_materialized.groupBy(...).agg(...)

# Cleanup at end
for t in _TEMP_TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
```

---

## 9. Output Validation Framework (Mandatory)

See the companion document `alteryx-output-validation-framework.md` for the complete validation procedure.

**In brief:**
1. Load expected output (from Alteryx)
2. Run the migrated pipeline
3. Compare using the validation function:
   - Row count
   - Schema (columns, types)
   - Null counts per column
   - Numeric aggregations (sum, avg, min, max) with tolerance
   - Row-level diff (if key columns available)

**Pass criteria:**
- Row count: exact match
- Schema: all expected columns present + correct types
- Null counts: exact match
- Numeric aggregations: within ±0.000001 tolerance
- Row-level diff: zero unmatched rows

**Action on fail:**
- Investigate specific check failure
- Adjust transformation logic
- Re-run validation until PASS

---

## References

- **PySpark Window Functions:** [pyspark.sql.window](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
- **Spark SQL Window Functions:** [Apache Spark SQL - Window Functions](https://spark.apache.org/docs/latest/sql-ref-window-functions.html)
- **Lakeflow Designer Operators:** [Built-in Operators](https://learn.microsoft.com/en-us/azure/databricks/designer/built-in-operators)
- **VDP Decomposition Best Practices:** See section 3 (One Logical Step = One Operator)
- **Broadcast Hints:** `/*+ BROADCAST(df_name) */` for joins on large × small tables
