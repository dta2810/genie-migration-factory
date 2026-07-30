# Databricks Conversion Standards (migrate-convert)

Reference guide for Alteryx/SSIS-to-Databricks conversions. Owned by the `migrate-convert` skill. Based on enterprise standards extracted from genie-code-skills-demo data engineering skills, with corrections from real pipeline failures.

## Lessons Learned: Corrected Failures

These 4 issues were encountered in production conversions and are now corrected:

1. **`owner` is a RESERVED TBLPROPERTY** — Using `"owner"` in `TBLPROPERTIES` fails with `UNSUPPORTED_FEATURE` error. Use `"data_owner"` instead. Real ownership is expressed via Unity Catalog tags applied outside the pipeline (post-deployment).

2. **Liquid Clustering + Z-order are incompatible** — `CLUSTER BY AUTO` (liquid clustering) and `pipelines.autoOptimize.zOrderCols` cannot be set on the same table. Liquid clustering replaces Z-ordering entirely. For new tables, prefer `CLUSTER BY AUTO` (liquid clustering) — it is adaptive and requires no manual tuning.

3. **Explicit bronze schemas break Auto Loader inference** — When using Spark Declarative Pipeline (SDP) `read_files()` with Auto Loader for streaming CSV ingest, do NOT declare an explicit column schema. Let Auto Loader infer the schema. Explicit schemas conflict with Auto Loader's schema-inference mode and cause ingest failures.

4. **read_files() / STREAM requires DIRECTORY paths, not file paths** — `read_files()` and streaming contexts (e.g. Autoloader) require a directory path (e.g. `/mnt/data/incoming/`), not a specific file path (e.g. `/mnt/data/incoming/file.csv`). Globs and directories work; individual files cause schema-inference failures.

---

## Table Properties (TBLPROPERTIES)

### Core properties (all tables)

Every table MUST have at minimum:

```sql
TBLPROPERTIES (
  "quality" = "<bronze|silver|gold>",
  "data_owner" = "<team-or-domain>",
  "domain" = "<business-domain>"
)
```

| Property | Values | Purpose |
|----------|--------|---------|
| `quality` | `bronze`, `silver`, `gold` | Medallion layer classification |
| `data_owner` | team name or domain | Data steward/owning team. **NEVER use `owner` — it is reserved.** |
| `domain` | business domain (e.g. `customer`, `financial`, `ops`) | Business area for governance |

### Conditional properties

| Condition | Additional Properties |
|-----------|----------------------|
| Contains PII | `"contains_pii" = "true"`, `"pii_columns" = "col1,col2"` |
| Silver layer | `"delta.enableChangeDataFeed" = "true"` |
| Streaming table | `"delta.enableRowTracking" = "true"` |
| Subject to retention | `"retention_days" = "<number>"` |
| Gold aggregation | `"delta.enableChangeDataFeed" = "true"` (optional, for downstream change capture) |

### Reserved properties — DO NOT SET IN TBLPROPERTIES

- `owner` — RESERVED. Use `data_owner` instead.
- `pipelines.autoOptimize.zOrderCols` — see **Clustering** section below; incompatible with liquid clustering.

### Example

```sql
TBLPROPERTIES (
  "quality" = "silver",
  "data_owner" = "data-engineering",
  "domain" = "customer",
  "contains_pii" = "true",
  "pii_columns" = "email_hash,phone_masked",
  "delta.enableChangeDataFeed" = "true"
)
```

---

## Table Comments

Every table MUST have a `COMMENT` clause describing its purpose, source, and PII status.

| Layer | Pattern |
|-------|---------|
| Bronze | `COMMENT "Raw <entity> data ingested from <source>"` |
| Silver | `COMMENT "Cleaned and validated <entity> with derived metrics from bronze_<entity>"` |
| Gold | `COMMENT "Business aggregation: <metric> by <dimensions>"` |
| PII | Append ` - CONTAINS PII: <column_list>` to any table with personal data |

### Examples

```sql
-- Bronze
COMMENT "Raw customer records ingested from SAP ERP"

-- Silver with PII
COMMENT "Cleaned customer data with derived income tiers from bronze_customers - CONTAINS PII: email_hash, phone_masked"

-- Gold
COMMENT "Customer segment analytics: count and avg age by region, income tier, credit tier"
```

---

## Table Naming

All table names MUST use `lowercase_snake_case` with a layer prefix:

| Layer | Prefix | Example |
|-------|--------|---------|
| Bronze | `bronze_` | `bronze_transactions`, `bronze_customers` |
| Silver | `silver_` | `silver_customers`, `silver_transactions_daily` |
| Gold | `gold_` | `gold_daily_revenue`, `gold_customer_segments` |

Never use PascalCase, UPPERCASE, kebab-case, or camelCase. Never omit the layer prefix.

---

## Medallion Architecture

### Bronze Layer

- **Purpose**: Raw data, minimally transformed
- **Source**: Auto Loader (Autoloader), direct copy, CDC feed
- **Table type**: `STREAMING TABLE` (for file/CDC ingest) or `MATERIALIZED VIEW` (for batch)
- **Constraints**: None (preserve raw data); use `WHERE` only for filtering obvious corruptions
- **Audit columns**: `audit_timestamp`, `source_system` (last two columns)
- **Example**:
  ```sql
  CREATE OR REFRESH STREAMING TABLE bronze_transactions
  COMMENT "Raw transaction data from POS systems"
  TBLPROPERTIES ("quality" = "bronze", "data_owner" = "ops", "domain" = "transactions")
  AS SELECT
    *,
    current_timestamp() AS audit_timestamp,
    'pos_system' AS source_system
  FROM auto_loader_feed;
  ```

### Silver Layer

- **Purpose**: Cleaned, validated, business-ready data
- **Source**: Bronze tables (via `LIVE.bronze_*`)
- **Table type**: `MATERIALIZED VIEW`
- **Transformations**:
  - Validate and constrain data (e.g. `EXPECT customer_id IS NOT NULL`)
  - Derive business columns (e.g. income tier, age from DOB)
  - Mask or hash PII (e.g. email → SHA2 hash, phone → last 4 digits)
  - Add data quality flags
- **Data quality flag** (REQUIRED): Include a `data_quality_flag` column:
  ```sql
  CASE
    WHEN customer_id IS NULL THEN 'MISSING_CUSTOMER_ID'
    WHEN age < 0 THEN 'NEGATIVE_AGE'
    ELSE 'CLEAN'
  END AS data_quality_flag
  ```
- **Constraints**: Use `CONSTRAINT ... EXPECT` with `ON VIOLATION FAIL UPDATE` (critical) or `ON VIOLATION DROP ROW` (non-critical)
- **Audit columns**: `audit_timestamp`, `source_system` (last two columns)
- **Example**:
  ```sql
  CREATE OR REFRESH MATERIALIZED VIEW silver_customers (
    customer_id        COMMENT 'Unique customer ID from source',
    email_hash         COMMENT 'SHA-256 hash of email for matching without exposing PII',
    income_tier        COMMENT 'Derived: High Income / Upper Middle / Middle / Lower Middle',
    data_quality_flag  COMMENT 'Row-level DQ status: CLEAN or MISSING_CUSTOMER_ID',
    CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
  )
  COMMENT "Cleaned customer data from bronze_customers - CONTAINS PII: email_hash"
  TBLPROPERTIES (
    "quality" = "silver",
    "data_owner" = "data-engineering",
    "domain" = "customer",
    "contains_pii" = "true",
    "pii_columns" = "email_hash",
    "delta.enableChangeDataFeed" = "true"
  )
  AS SELECT
    customer_id,
    SHA2(LOWER(TRIM(email)), 256) AS email_hash,
    CASE
      WHEN annual_income >= 250000 THEN 'High Income'
      WHEN annual_income >= 100000 THEN 'Upper Middle'
      ELSE 'Middle'
    END AS income_tier,
    CASE
      WHEN customer_id IS NULL THEN 'MISSING_CUSTOMER_ID'
      ELSE 'CLEAN'
    END AS data_quality_flag,
    current_timestamp() AS audit_timestamp,
    'crm_system' AS source_system
  FROM LIVE.bronze_customers;
  ```

### Gold Layer

- **Purpose**: Aggregated, business-facing analytics
- **Source**: Silver tables (via `LIVE.silver_*`) or aggregations
- **Table type**: `MATERIALIZED VIEW`
- **Transformations**: Aggregations, grouping, final derivations
- **PII**: MUST NOT contain individual-level PII (only aggregates)
- **Constraints**: Generally none (data validated in Silver)
- **Audit columns**: `audit_timestamp`, `source_system` (last two columns)
- **Example**:
  ```sql
  CREATE OR REFRESH MATERIALIZED VIEW gold_customer_segments
  COMMENT "Customer segment analytics by region and income tier"
  TBLPROPERTIES (
    "quality" = "gold",
    "data_owner" = "analytics",
    "domain" = "customer",
    "delta.enableChangeDataFeed" = "true"
  )
  AS SELECT
    region,
    income_tier,
    COUNT(DISTINCT customer_id) AS customer_count,
    ROUND(AVG(age), 1) AS avg_age,
    current_timestamp() AS audit_timestamp,
    'gold_aggregation' AS source_system
  FROM LIVE.silver_customers
  WHERE data_quality_flag = 'CLEAN'
  GROUP BY region, income_tier;
  ```

---

## Column Descriptions (Inline Comments)

Column comments MUST be declared **INLINE in the CREATE statement's column list**, NOT via post-hoc `ALTER TABLE ... ALTER COLUMN ... COMMENT`. Spark Declarative Pipelines (SDP) do not support multi-column ALTER statements and will reject the pipeline update.

### Syntax

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_customers (
  customer_id        COMMENT 'Unique customer identifier from source system',
  email_hash         COMMENT 'SHA-256 hash of email for PII protection',
  income_tier        COMMENT 'Derived income bracket: High / Upper Middle / Middle / Lower Middle',
  data_quality_flag  COMMENT 'Row-level DQ status: CLEAN, MISSING_<field>, or NEGATIVE_<field>'
)
```

### When to describe columns

At minimum, add descriptions for:
- Primary keys and foreign keys
- Derived or calculated columns (explain the logic or formula)
- PII columns (note the masking method and risk level)
- Data quality flags
- Audit columns (`audit_timestamp`, `source_system`)
- Any column whose meaning is not obvious from its name

### Description patterns

| Column Type | Pattern |
|-------------|---------|
| Primary key | `'Unique <entity> identifier from <source>'` |
| Foreign key | `'References <parent_table>.<parent_column>'` |
| Derived numeric | `'Calculated as <formula>. Units: <unit>'` |
| Derived category | `'Derived <category>: <value1> / <value2> / ...'` |
| Hashed/masked PII | `'<Method> of <original_field> for PII protection'` (e.g., 'SHA-256 hash of email') |
| Audit timestamp | `'Pipeline execution timestamp'` |
| Source system | `'Upstream source system identifier'` |
| Data quality flag | `'Row-level data quality status: <values>'` |

---

## Clustering: Liquid Clustering vs Z-Ordering

**CRITICAL DECISION**: Choose ONE clustering strategy. Liquid clustering (CLUSTER BY AUTO) and Z-ordering (zOrderCols) are mutually exclusive.

### Liquid Clustering (Recommended for new tables)

**When**: Default choice for new medallion tables. Adaptive, low-maintenance, and designed for Unity Catalog.

**How**:
```sql
CREATE OR REFRESH STREAMING TABLE bronze_transactions
COMMENT "..."
TBLPROPERTIES ("quality" = "bronze", ...)
CLUSTER BY AUTO
AS SELECT ...;
```

**Characteristics**:
- Adaptive: Databricks automatically selects clustering columns based on query patterns
- No manual tuning required
- Supported in Unity Catalog (required for SDP)
- Best for tables with evolving query patterns or unknown access patterns
- Works with Photon and serverless compute

**Why use it**: Simpler to maintain, no need to predetermine clustering columns, automatic optimization.

### Z-Ordering (Legacy optimization, avoid for SDP)

**When**: Only for external (non-UC) Delta tables or when you have strongly predictable, static query patterns.

**How** (non-SDP context only):
```python
spark.sql("""
ALTER TABLE bronze_transactions
SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '32')
""")
spark.sql("OPTIMIZE bronze_transactions ZORDER BY (date_column, region_column)")
```

**Characteristics**:
- Requires manual column selection and tuning
- Optimization runs asynchronously
- Effective for known, stable access patterns
- NOT supported inside SDP pipeline definitions

**Why avoid for SDP**: Manual tuning + incompatible with SDP syntax + liquid clustering is superior for UC.

### Migration Rule

For Alteryx/SSIS conversions:
- **Default**: Use `CLUSTER BY AUTO` for all new streaming and materialized view tables in SDP pipelines.
- **Never set both**: Do not include `CLUSTER BY AUTO` and `pipelines.autoOptimize.zOrderCols` on the same table — they conflict and cause pipeline failures.
- **Existing Delta tables**: If migrating existing Delta tables with Z-order optimization, keep Z-order settings in external tables; apply liquid clustering to new UC-based tables.

---

## PII Management

### Identify PII Columns

Flag columns matching these patterns as PII:

| Column Pattern | PII Type | Risk |
|----------------|----------|------|
| `email`, `email_address` | EMAIL | HIGH |
| `phone`, `mobile`, `telephone` | PHONE | HIGH |
| `first_name`, `last_name`, `full_name` | NAME | MEDIUM |
| `date_of_birth`, `dob` | DOB | MEDIUM |
| `address`, `street`, `postal_code` | ADDRESS | MEDIUM |
| `ssn`, `national_id`, `tax_id` | SSN/NATIONAL_ID | CRITICAL |
| `account_number`, `iban`, `sort_code` | ACCOUNT | HIGH |
| `credit_score`, `income`, `salary` | FINANCIAL | HIGH |
| `card_number`, `cvv` | PAYMENT | CRITICAL |

### PII Handling by Layer

#### Bronze — Pass through with markers

Pass PII through at Bronze but label it for visibility:

```sql
CREATE OR REFRESH STREAMING TABLE bronze_customers
COMMENT "Raw customer data - CONTAINS PII: email, phone, first_name, last_name"
TBLPROPERTIES (
  "quality" = "bronze",
  "data_owner" = "ops",
  "domain" = "customer",
  "contains_pii" = "true",
  "pii_columns" = "email,phone,first_name,last_name"
)
AS SELECT
  customer_id,
  first_name,     -- [PII: NAME - MEDIUM]
  last_name,      -- [PII: NAME - MEDIUM]
  email,          -- [PII: EMAIL - HIGH]
  phone,          -- [PII: PHONE - HIGH]
  address,        -- [PII: ADDRESS - MEDIUM]
  current_timestamp() AS audit_timestamp,
  'crm_system' AS source_system
FROM ...;
```

#### Silver — Apply masking and derivation

Replace raw PII with derived or masked equivalents:

```sql
-- Email hash (for matching without exposing raw email)
SHA2(LOWER(TRIM(email)), 256) AS email_hash,

-- Phone masked (show last 4 digits only)
CONCAT('***-***-', RIGHT(REGEXP_REPLACE(phone, '[^0-9]', ''), 4)) AS phone_masked,

-- Income tier (instead of exact income)
CASE
  WHEN annual_income >= 250000 THEN 'High Income'
  WHEN annual_income >= 100000 THEN 'Upper Middle'
  ELSE 'Lower Middle'
END AS income_tier,

-- Age (instead of DOB)
FLOOR(DATEDIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE)) / 365) AS age
```

Silver table example:
```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_customers (
  customer_id    COMMENT 'Unique customer ID',
  email_hash     COMMENT 'SHA-256 hash of email for matching without PII exposure',
  phone_masked   COMMENT 'Last 4 digits of phone, masked for PII protection',
  age            COMMENT 'Derived age in years from date_of_birth',
  income_tier    COMMENT 'Derived income bracket: High / Upper Middle / Lower Middle'
)
COMMENT "Cleaned customer data from bronze_customers - CONTAINS PII: email_hash, phone_masked"
TBLPROPERTIES (
  "quality" = "silver",
  "data_owner" = "data-engineering",
  "domain" = "customer",
  "contains_pii" = "true",
  "pii_columns" = "email_hash,phone_masked",
  "delta.enableChangeDataFeed" = "true"
)
AS SELECT
  customer_id,
  SHA2(LOWER(TRIM(email)), 256) AS email_hash,
  CONCAT('***-***-', RIGHT(REGEXP_REPLACE(phone, '[^0-9]', ''), 4)) AS phone_masked,
  FLOOR(DATEDIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE)) / 365) AS age,
  CASE
    WHEN annual_income >= 250000 THEN 'High Income'
    WHEN annual_income >= 100000 THEN 'Upper Middle'
    ELSE 'Lower Middle'
  END AS income_tier,
  current_timestamp() AS audit_timestamp,
  'crm_system' AS source_system
FROM LIVE.bronze_customers;
```

#### Gold — Aggregated only (NO individual PII)

Gold tables MUST NOT contain individual-level PII:

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_customer_segments
COMMENT "Customer segment analytics by region and income tier"
TBLPROPERTIES ("quality" = "gold", "data_owner" = "analytics", "domain" = "customer")
AS SELECT
  region,
  income_tier,
  COUNT(DISTINCT customer_id) AS customer_count,
  ROUND(AVG(age), 1) AS avg_age,
  current_timestamp() AS audit_timestamp,
  'gold_aggregation' AS source_system
FROM LIVE.silver_customers
WHERE data_quality_flag = 'CLEAN'
GROUP BY region, income_tier;
```

### Unity Catalog Column Masking (Post-deployment)

Unity Catalog masking functions are applied AFTER the pipeline update succeeds, NOT inside the `.sql`:

```sql
-- Run AFTER the pipeline succeeds (governance step, not in SDP)
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('pii_full_access') THEN email
  WHEN is_member('pii_partial_access') THEN CONCAT(SUBSTR(email, 1, 3), '***@', SPLIT_PART(email, '@', 2))
  ELSE '***@***'
END;

ALTER TABLE silver_customers ALTER COLUMN email SET MASK mask_email;
```

---

## Auto Loader Ingest Rules

When ingesting files (CSV, Parquet, JSON, etc.) into Bronze via Spark Declarative Pipeline (SDP):

### Schema Inference (Recommended)

Let Auto Loader infer the schema. This is the default and most robust approach:

```sql
CREATE OR REFRESH STREAMING TABLE bronze_transactions
COMMENT "Raw transaction data from CSV files"
TBLPROPERTIES ("quality" = "bronze", "data_owner" = "ops", "domain" = "transactions")
AS SELECT * FROM cloud_files(
  '/mnt/data/incoming/transactions/',
  'csv',
  map('cloudFiles.inferColumnTypes', 'true', 'cloudFiles.schemaLocation', '/mnt/checkpoint/transactions')
);
```

**Key points**:
- `cloudFiles.inferColumnTypes = 'true'` — infer types from data
- `cloudFiles.schemaLocation` — Auto Loader checkpoint (state)
- Schema is discovered from the first batch and cached
- Changes to schema in source files will be detected on subsequent runs

### Directory paths (NOT file paths)

Auto Loader expects a **directory path**, not individual files:

| Correct | Incorrect |
|---------|-----------|
| `/mnt/data/incoming/` | `/mnt/data/incoming/file.csv` |
| `/mnt/data/incoming/*.csv` (glob) | `/mnt/data/incoming/2024-01-01/transactions.csv` |
| `s3://bucket/path/` | `s3://bucket/path/file.parquet` |

**Why**: Auto Loader continuously monitors the directory for new files. Specifying a single file disables streaming behavior and can conflict with schema inference.

### DO NOT declare explicit schemas in SDP

❌ **WRONG** — explicit schema conflicts with Auto Loader inference:
```sql
CREATE OR REFRESH STREAMING TABLE bronze_transactions (
  transaction_id INT,
  amount DECIMAL(10, 2),
  timestamp STRING
)
AS SELECT * FROM cloud_files(...);
```

✅ **CORRECT** — let Auto Loader infer:
```sql
CREATE OR REFRESH STREAMING TABLE bronze_transactions
AS SELECT * FROM cloud_files(...);
```

---

## Audit Columns (All Tables)

Every table MUST end with exactly two audit columns (in this order):

```sql
current_timestamp() AS audit_timestamp,
'<source_description>' AS source_system
```

These columns are LAST in the column list and SELECT output.

| Column | Type | Pattern | Purpose |
|--------|------|---------|---------|
| `audit_timestamp` | TIMESTAMP | `current_timestamp()` | When the pipeline ran (UTC) |
| `source_system` | STRING | System name or path identifier | Where the row came from |

### Examples

```sql
-- Bronze: raw data path
SELECT *, current_timestamp() AS audit_timestamp, 'csv_autoloader' AS source_system FROM ...

-- Silver: source table
SELECT *, current_timestamp() AS audit_timestamp, 'bronze_customers' AS source_system FROM LIVE.bronze_customers

-- Gold: derivation step
SELECT *, current_timestamp() AS audit_timestamp, 'silver_aggregation' AS source_system FROM LIVE.silver_customers
```

---

## Data Quality Constraints

### Bronze Layer

- No `EXPECT` constraints (preserve raw data)
- Use `WHERE` only for obvious filtering (e.g., remove null IDs if it's guaranteed data corruption)

### Silver Layer

Use `CONSTRAINT ... EXPECT` for validation:

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_customers (
  customer_id    COMMENT '...',
  email_hash     COMMENT '...',
  CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_email EXPECT (email_hash IS NOT NULL) ON VIOLATION DROP ROW
)
```

| Violation Mode | Use Case |
|----------------|----------|
| `ON VIOLATION FAIL UPDATE` | Critical field — stop the pipeline if invalid |
| `ON VIOLATION DROP ROW` | Non-critical field — skip rows that fail, continue |

**Important**: `EXPECT` constraints are evaluated against the OUTPUT columns (the final `SELECT`), not source columns. If you hash, mask, or drop a column, reference the derived column, not the source:

```sql
-- CORRECT: reference email_hash (output)
CONSTRAINT valid_email EXPECT (email_hash IS NOT NULL)

-- WRONG: reference email (source, may be dropped/masked)
CONSTRAINT valid_email EXPECT (email IS NOT NULL)
```

### Gold Layer

Generally no constraints (data is validated in Silver).

---

## Reserved Words & Anti-Patterns

| Term | Issue | Solution |
|------|-------|----------|
| `owner` in TBLPROPERTIES | Reserved; causes `UNSUPPORTED_FEATURE` error | Use `data_owner` |
| `CLUSTER BY AUTO` + `zOrderCols` together | Mutually exclusive; pipeline fails | Choose one: liquid clustering (preferred) or Z-order (legacy only) |
| Explicit schema + `read_files()` | Breaks Auto Loader inference | Remove explicit schema, let Auto Loader infer |
| File path to `read_files()` | Path must be directory | Use `/path/` or `/path/*.csv`, not `/path/file.csv` |
| `ALTER TABLE ... ALTER COLUMN ... COMMENT` in SDP | Not supported in pipelines | Declare comments inline in CREATE column list |
| `ALTER TABLE ... SET MASK` in SDP | Not supported in pipelines | Apply masks as post-deployment governance step |
| `ALTER TABLE ... SET TAGS` in SDP | Not supported in pipelines | Apply tags as post-deployment governance step |

---

## SQL Style Guide

| Element | Standard |
|---------|----------|
| SQL keywords | UPPERCASE (`SELECT`, `FROM`, `WHERE`, `AS`, `CASE`, etc.) |
| Table and column names | `lowercase_snake_case` |
| Aliases | short, lowercase (`t`, `a`, `d`) |
| Functions | UPPERCASE (`ROUND()`, `CAST()`, `SHA2()`, `DATEDIFF()`) |

### Column Organization

Always organize columns in this order:

1. Identifiers (primary keys, foreign keys, surrogate keys)
2. Dimensions (categories, hierarchies, hierarchical attributes)
3. Measures (quantities, amounts, counts)
4. Derived/calculated fields
5. Data quality flags
6. Audit columns LAST (`audit_timestamp`, `source_system`)

### Example

```sql
SELECT
  -- Identifiers
  customer_id,
  transaction_id,
  
  -- Dimensions
  region,
  product_category,
  
  -- Measures
  amount,
  quantity,
  
  -- Derived
  income_tier,
  age,
  
  -- Data quality
  data_quality_flag,
  
  -- Audit (LAST)
  current_timestamp() AS audit_timestamp,
  'source_system' AS source_system
FROM ...;
```

---

## Spark Declarative Pipeline (SDP) Basics

When generating migration SQL for SDP:

### Table Types

| Type | Use | Syntax |
|------|-----|--------|
| STREAMING TABLE | File ingest (Auto Loader), CDC, real-time | `CREATE OR REFRESH STREAMING TABLE` + `CLUSTER BY AUTO` |
| MATERIALIZED VIEW | Batch from Delta tables, aggregations | `CREATE OR REFRESH MATERIALIZED VIEW` |

### References within a pipeline

Reference other tables in the same pipeline using `LIVE.`:

```sql
-- Within the pipeline
FROM LIVE.bronze_customers

-- Outside the pipeline (fully qualified)
FROM catalog.schema.silver_customers
```

### Column inline declarations (required)

Declare column comments and constraints inline in the CREATE statement:

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_customers (
  customer_id    COMMENT 'Unique ID',
  email_hash     COMMENT 'SHA-256 hash',
  CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
```

### No post-hoc ALTER

Never use `ALTER TABLE` inside SDP pipelines for comments, masks, or tags. These are applied after the pipeline succeeds.

---

## Checklist for Every Table

Before committing conversion SQL, verify:

- [ ] Table name is `lowercase_snake_case` with layer prefix (`bronze_`, `silver_`, `gold_`)
- [ ] `COMMENT` clause present (includes source and PII note if applicable)
- [ ] `TBLPROPERTIES` includes `quality`, `data_owner` (NOT `owner`), `domain`
- [ ] If PII: `contains_pii = "true"` and `pii_columns` list in TBLPROPERTIES
- [ ] If PII: COMMENT includes `CONTAINS PII: <columns>`
- [ ] Column descriptions declared INLINE in CREATE column list
- [ ] No `ALTER TABLE ... COMMENT` or `ALTER TABLE ... SET TAGS` (governance step, post-pipeline)
- [ ] Constraints (`EXPECT`) reference OUTPUT columns only (not masked/dropped source columns)
- [ ] Last two columns are `audit_timestamp` and `source_system`
- [ ] Clustering: either `CLUSTER BY AUTO` (for streaming) OR no clustering (SDP will optimize)
- [ ] Never set both `CLUSTER BY AUTO` and `zOrderCols` (incompatible)
- [ ] For Auto Loader ingest: no explicit schema, directory path only
- [ ] Silver layer includes `data_quality_flag` column
- [ ] Gold layer has NO individual PII (aggregates only)
- [ ] SQL keywords UPPERCASE, names lowercase_snake_case

---

## References

- **Source**: genie-code-skills-demo data engineering skills (table-governance.md, sdp-basics.md, pii-management.md, audit-trail.md, pipeline-config.md)
- **Standard**: Databricks Unity Catalog + Spark Declarative Pipeline (SDP)
- **Scope**: Alteryx/SSIS-to-Databricks migration conversions
- **Maintained by**: migrate-convert skill
