# Alteryx Tool → Databricks/Spark Reference Mapping

**Source:** `geniecodeskills-alteryxmigration` repository SKILL.md documents  
**Last updated:** 2026-07-30  
**Scope:** Alteryx Designer tools → PySpark (Databricks) equivalents + Lakeflow Designer (VDP) operators

---

## 1. Tool Mapping Reference — Input/Output & Preparation

### Input / Output Tools

| Alteryx Tool | PySpark Equivalent | Lakeflow Designer Operator | Notes |
|---|---|---|---|
| Input Data (CSV) | `spark.read.csv(path, header=True, inferSchema=True)` | `source` (file_source, format: csv) | Use `read_files()` in SQL |
| Input Data (Excel) | `spark.read.format("com.crealytics.spark.excel").load()` or pandas then convert | `python` | Install `spark-excel` package; pandas + createDataFrame for simpler case |
| Input Data (DB / ODBC / OLEDB) | `spark.read.jdbc(url, table, properties)` | `source` (table_source) or `python` (JDBC) | Use Lakehouse Federation for persistent connections |
| Input Data (Parquet) | `spark.read.parquet(path)` | `source` (file_source, format: parquet) | Native Spark support |
| Input Data (JSON) | `spark.read.json(path)` | `source` (file_source, format: json) | Multiline option for pretty JSON |
| Input Data (Avro) | `spark.read.format("avro").load(path)` | `source` (file_source, format: avro) | |
| Input Data (ORC) | `spark.read.format("orc").load(path)` | `source` (file_source, format: orc) | |
| Output Data (CSV) | `df.write.csv(path, header=True)` | `python` (to UC Volume) | Prefer Delta: `.write.saveAsTable()` |
| Output Data (Delta/Table) | `df.write.mode("overwrite").saveAsTable("catalog.schema.table")` | `output` | Canonical materialized output |
| Output Data (DB) | `df.write.jdbc(url, table, mode, properties)` | `python` (JDBC) or UC Federation | Or write to Delta table |
| Browse | `display(df)` or `df.show()` | *omit* | Preview only — no VDP analog |
| Directory | `dbutils.fs.ls(path)` or `spark.read.format("binaryFile").load(path)` | `python` | File listing from Volumes |
| Text Input | `spark.createDataFrame(rows, schema)` | `python` | Inline data generation |
| Date/Time Now | `F.current_timestamp()` / `F.current_date()` | `transform` | SQL expressions: `current_timestamp()` / `current_date()` |

### Preparation Tools — Selection, Filtering, Formulas

| Alteryx Tool | PySpark Equivalent | Lakeflow Designer Operator | Notes |
|---|---|---|---|
| Select | `df.select("col1", "col2").withColumnRenamed("old", "new")` | `transform` | Also: `.drop()` to remove columns; handles retype in `transform` with CAST |
| Filter | `df.filter(F.col("x") > 10)` | `filter` | Chain multiple with `&` / `\|`; Alteryx T/F outputs become two parallel `filter` operators with inverse conditions |
| Formula | `df.withColumn("new_col", <expression>)` | `transform` | Use `F.when().otherwise()` for conditionals; Transform handles CASE WHEN, COALESCE, etc. |
| Multi-Field Formula | Loop over columns: `for c in cols: df = df.withColumn(c, expr)` | `transform` | Use `reduce()` for functional style |
| Multi-Row Formula | Window functions: `F.lag()`, `F.lead()`, `F.sum().over()` | `sql` | Define `Window.partitionBy().orderBy()` |
| Sort | `df.orderBy(F.col("x").desc())` | `sort` | One or more `column ASC\|DESC` |
| Sample / First N | `df.sample(fraction=0.1)` or `df.limit(100)` | `limit` or `sql` | First N → `limit`; sampling with seed → SQL `WHERE rand() < 0.1` |
| Unique | `df.dropDuplicates(["key_col"])` | `sql` | ROW_NUMBER() OVER (PARTITION BY key ...) WHERE rn = 1 |
| Data Cleansing (whitespace / case / nulls) | `df.na.fill(0)`, `df.na.drop()`, `F.trim()`, `F.lower()` | `transform` | TRIM, LOWER/UPPER, REPLACE, NULLIF |
| Data Cleansing (PII redaction) | Custom regex chains or UDF | `ai_function` | `ai_mask(text, ARRAY('EMAIL','PHONE','SSN',...))` |
| Auto Field | Explicit `.cast()` per column | `transform` | Cast columns; let Spark infer or pick narrowest type |
| Imputation (null fill with constant/col) | `.fillna({"col": value})` or `COALESCE(col, fallback)` | `transform` | `COALESCE(col, fallback_col) AS col` — use Transform when filling from another column or constant |
| Imputation (fill with aggregate) | `.fillna()` with window function | `sql` | `COALESCE(col, AVG(col) OVER ())` — use SQL only when fill value requires window/aggregate |
| DateTime | `F.to_date()`, `F.to_timestamp()`, `F.date_add()`, `F.datediff()` | `transform` | to_date, to_timestamp, date_format, unix_timestamp |
| Record ID | `F.monotonically_increasing_id()` | `sql` | Not guaranteed sequential; use ROW_NUMBER() for guaranteed sequence |
| Running Total | `F.sum("col").over(Window.orderBy("date"))` | `sql` | `SUM(col) OVER (PARTITION BY ... ORDER BY ...)` |
| Tile | `F.ntile(n).over(Window.orderBy("col"))` | `sql` | `NTILE(n) OVER (...)` |
| Random % Sample | `df.sample(fraction=p, seed=s)` | `sql` | `WHERE rand() < 0.1` (with seed if reproducibility needed) |
| Select Records | `df.limit(n)` or range filter | `sql` | Range-based: `WHERE rn BETWEEN a AND b` |
| Generate Rows | `spark.range(n)` with `.withColumn()` | `python` or `sql` | Row generation with parameters |

---

## 2. Join & Combine Tools

| Alteryx Tool | PySpark Equivalent | Lakeflow Designer Operator | Notes |
|---|---|---|---|
| Join | `df1.join(df2, on="key", how="inner")` | `join` | Left/Right unmatched: use `how="left_anti"` separately; Designer Join supports Full/Inner/Left/Right only. Alteryx's three outputs (L/J/R) → Left + Right anti joins or one SQL with UNION ALL branches |
| Join Multiple | Chain `.join()` calls | `join` (chained) or `sql` | Use broadcast for small tables; one `sql` with multi-table FROM for complex cases |
| Append Fields (cross join) | `df1.crossJoin(df2)` | `sql` | Designer Join has no cross-join — emit `SELECT * FROM left CROSS JOIN right` |
| Find Replace (small ≤10 mappings) | `df.join(lookup_df, on="key").withColumn(...)` or `F.when()` | `transform` | Use CASE WHEN: `CASE WHEN col = 'old' THEN 'new' ... ELSE col END AS col` |
| Find Replace (large lookup table) | Left join to lookup, COALESCE replacement | `join` + `transform` | Left join to lookup table, COALESCE replacement, drop lookup cols |
| Fuzzy Match (SOUNDEX / string distance) | `F.soundex()`, `F.levenshtein(a, b)` | `transform` + `sql` | **Transform**: `SOUNDEX(name) AS soundex`. **SQL**: `DENSE_RANK() OVER (ORDER BY ...) AS group_id` |
| Fuzzy Match (semantic similarity) | Custom Python or UDF | `ai_function` | `ai_similarity(left_text, right_text)` then threshold — only for truly semantic on low-cardinality |
| Make Group | Connected-components logic | `sql` | Flag **REVIEW** — rare pattern |
| Union | `df1.unionByName(df2, allowMissingColumns=True)` | `combine` | operator: UNION, quantifier: ALL (UNION ALL) or DISTINCT |
| Set difference / EXCEPT | Anti-join or EXCEPT SQL | `combine` | operator: EXCEPT (or MINUS) |
| Intersect | N/A (Alteryx has no native equivalent) | `combine` | operator: INTERSECT |

---

## 3. Transform & Aggregation Tools

| Alteryx Tool | PySpark Equivalent | Lakeflow Designer Operator | Notes |
|---|---|---|---|
| Summarize (SUM/AVG/COUNT/MIN/MAX/MEDIAN/STDDEV/VARIANCE/PERCENTILE) | `df.groupBy("col").agg(F.sum(), F.avg(), F.count(), ...)` | `aggregate` | **ALWAYS use Aggregate for simple GROUP BY** — map directly to supported aggregations |
| Summarize with COUNT(DISTINCT) | `df.groupBy("col").agg(F.countDistinct("col2"))` | `sql` | The Aggregate operator COUNT does NOT deduplicate — use `COUNT(DISTINCT col)` in SQL |
| Summarize (FIRST / LAST / Concat) | `.agg(F.first(), F.last(), F.concat_ws())` | `sql` | Designer's Aggregate does NOT expose first/last/collect_list — use FIRST_VALUE, LAST_VALUE, concat_ws |
| Count Records | `df.count()` | `aggregate` | Single COUNT(*) aggregation, no group_bys |
| Cross Tab (Pivot) | `df.groupBy("row").pivot("col").agg(F.sum("val"))` | `pivot` | **Rows → Columns** mode; pick pivot column + value/aggregation |
| Transpose (Unpivot) | `stack()` in `selectExpr` | `pivot` | **Columns → Rows** mode; `df.selectExpr("id", "stack(3, 'a', a, 'b', b, 'c', c) as (key, value)")` |
| Frequency Table | `df.groupBy("col").count().withColumn("pct", F.col("count") / df.count())` | `aggregate` | GROUP BY col, COUNT(*) |
| Weighted Average | `F.sum(F.col("val") * F.col("weight")) / F.sum("weight")` | `sql` | `SUM(value*weight) / SUM(weight)` per group |
| Pearson Correlation | `df.stat.corr("col1", "col2")` | `python` | For matrix: `Correlation.corr(df, "features")` (MLlib) |
| Field Summary | Describe-style query | `sql` | `describe`-style: count/mean/stddev/min/max per column |
| Arrange | Reshape — typically `melt` then `pivot` | `python` | Flag **REVIEW** |

---

## 4. Parse & String Tools

| Alteryx Tool | PySpark Equivalent | Lakeflow Designer Operator | Notes |
|---|---|---|---|
| RegEx (Extract) | `F.regexp_extract(col, pattern, group)` | `transform` | `REGEXP_EXTRACT(col, pattern, group) AS alias` — Java regex syntax |
| RegEx (Replace) | `F.regexp_replace(col, pattern, repl)` | `transform` | `REGEXP_REPLACE(col, pattern, repl) AS alias` |
| RegEx (Tokenize / split multi-row) | `F.explode(F.split(col, delim))` | `sql` | `EXPLODE(SPLIT(regexp_extract_all(...)))` |
| Text To Columns (fixed N columns) | `F.split("col", "delim")[0]`, `[1]`, etc. | `transform` | `ELEMENT_AT(SPLIT(col, ','), 1) AS part1`, `ELEMENT_AT(SPLIT(col, ','), 2) AS part2` — use Transform for fixed column count |
| Text To Columns (explode into rows) | `F.explode(F.split(col, ","))` | `sql` | `EXPLODE(SPLIT(col, ',')) AS part` — SQL required for row expansion |
| XML Parse | `spark.read.format("xml")` or xpath functions | `python` | Install `spark-xml` package; or `pyspark.sql.functions.xpath_*` |
| JSON Parse | `F.from_json()`, `F.get_json_object()` | `transform` | Define schema with StructType; Transform handles simple `from_json` |
| Free-text → fields (no fixed schema) | LLM call / custom UDF | `ai_function` | `ai_extract(text, ARRAY('field_a','field_b',...))` returns a struct |

---

## 5. Spatial & Macro Tools

| Alteryx Tool | PySpark Equivalent | Lakeflow Designer Operator | Notes |
|---|---|---|---|
| Spatial Match | `geopandas` or `sedona` (GeoSpark) | `python` (Sedona) | Not native Spark — install Sedona separately; `ST_Intersects` / `ST_Contains` join |
| Trade Area | Haversine UDF or `sedona` buffer | `python` (Sedona) | `ST_Buffer(geom, distance)` or drive-time → flag manual |
| Buffer / Distance / Find Nearest | Sedona functions | `python` (Sedona) | `ST_Buffer`, `ST_Distance`, `ST_Distance` + ROW_NUMBER |
| Iterative Macro | `while` loop with convergence check | **MANUAL** | Avoid collect() in loops; use Lakeflow Job `For Each` task |
| Batch Macro | Parameterized function or loop | `python` | Iterate with Spark; or inline as sub-DAG in VDP |
| Standard Macro (`.yxmc`) | Inline or extract to Python operator | inline or `python` | Option 1: inline macro nodes; Option 2: extract to single `python` operator |
| Dynamic Input | Parameterize paths: `spark.read.load(path_variable)` | `python` (env_config) | Parameterized table references using Python operators |

---

## 6. Advanced & Reporting Tools (Limited/Manual Support)

| Alteryx Tool | PySpark Equivalent | Lakeflow Designer Operator | Notes |
|---|---|---|---|
| Python Tool | Direct port of Python code | `python` | Rewrite `Alteryx.read()` → `inputs["data"][i]` |
| R Tool | Re-implement in PySpark | `python` | Flag **REVIEW** — R has no UDO path; re-implement first |
| Linear / Logistic Regression | `pyspark.ml.regression` / `classification` | `python` | Log to MLflow |
| Decision Tree / Forest / Boosted | `pyspark.ml.classification` / `xgboost-spark` | `python` | Log to MLflow |
| Score | Load MLflow model; `.transform(df)` | `python` | Load model URI; apply to rows |
| Sentiment Analysis | Custom regex or `pyspark.ml` | `ai_function` | `ai_analyze_sentiment(text)` — no model training needed |
| Text Classification | Regex or ML model | `ai_function` | `ai_classify(text, ARRAY('cls_a','cls_b',...))` |
| Render / Email / Reporting | N/A (output Delta + Lakeview) | **MANUAL** | Output Delta; build Lakeview (AI/BI) dashboard |
| Chart / Map / Layout | N/A (output Delta + Lakeview) | **MANUAL** | One Lakeview widget per Alteryx report element |
| Interface (Text Box, Drop Down, etc.) | N/A (Lakeflow Job parameters) | **MANUAL** | Replace with Lakeflow Job parameters or Databricks App |
| Run Command | Lakeflow Job task | **MANUAL** | Replace with Lakeflow Job task or notebook |

---

## 7. Medallion Layer Assignment Guide

### Bronze Layer (Raw Ingestion)
**Purpose:** Land raw data as-is from source systems. Minimal transformation.

**Alteryx tools that map to Bronze:**
- Input Data tool → `spark.read` / Auto Loader / `read_files()`
- Connect In-DB tool → JDBC/ODBC reads
- Download tool → API ingestion scripts
- Directory tool → File listing from Volumes

**Best practices:**
- Preserve original column names and types
- Add ingestion metadata: `_ingested_at`, `_source_file`, `_batch_id`
- Store as Delta tables in a `bronze` schema
- Use `COPY INTO` or Auto Loader for incremental file ingestion
- Never apply business logic at this layer

### Silver Layer (Cleansed & Conformed)
**Purpose:** Clean, deduplicate, validate, and conform data. This is where most Alteryx transformation logic lives.

**Alteryx tools that map to Silver:**
- Data Cleansing tool → `.dropDuplicates()`, null handling, type casting
- Filter tool → `.filter()` / `.where()`
- Formula tool → `.withColumn()` with expressions
- Multi-Row Formula → Window functions
- Select tool → `.select()`, `.withColumnRenamed()`
- Sort tool → `.orderBy()`
- Sample tool → `.limit()` / `.sample()`
- Unique tool → `.dropDuplicates()`
- Join tool (early join-based cleansing) → `.join()`
- Find Replace tool → `F.regexp_replace()`, `F.when().otherwise()`
- Auto Field tool → Schema enforcement / explicit casting
- DateTime, RegEx, Imputation tools → Transformations on per-row basis

**Best practices:**
- Apply data quality checks (null rates, value ranges, referential integrity)
- Enforce schema with explicit column types
- Remove exact duplicates
- Standardize column names (snake_case, lowercase)
- Store as Delta tables in a `silver` schema
- Partition by date or high-cardinality business key when appropriate

### Gold Layer (Business-Level Aggregates)
**Purpose:** Business-ready datasets optimized for analytics and reporting.

**Alteryx tools that map to Gold:**
- Summarize tool → `.groupBy().agg()`
- Cross Tab tool → Pivot operations
- Join tool (late dimension joins) → `.join()`
- Union tool → `.unionByName()`
- Append Fields tool → `.crossJoin()` (cross-product) — **use sparingly**
- Transpose tool → Unpivot with `stack()`
- Weighted Average → Custom aggregations with `.agg()`
- Running Total → Window functions with `F.sum().over()`
- Pearson Correlation → `df.stat.corr()`
- Frequency tool → `.groupBy().count()`
- Reporting tools → Results feed dashboards or BI tools

**Best practices:**
- Align with business glossary / metric definitions
- Optimize for query patterns (Z-ORDER, liquid clustering)
- Store as Delta tables in a `gold` schema
- Document metric calculations as column comments
- These tables serve dashboards, ML features, and ad-hoc analysis

---

## 8. Tools with NO Clean Equivalent (Manual Review Required)

These tools have limited or no direct Databricks/Spark equivalent and require manual review or alternative architecture:

| Tool | Reason | Workaround |
|---|---|---|
| `.yxdb` files (Alteryx proprietary binary) | Proprietary format; Python `yxdb` library fails on "e2 Database" format | Ask user to export to CSV/Parquet; extract from expected output file if available; use original source files |
| Iterative Macro | No native loop construct in Spark DAG | Use Lakeflow Job with `For Each` task or while-loop pattern in `python` |
| Analytic App (Interface tools) | No UI form builder in VDP / PySpark | Replace with Lakeflow Job parameters or Databricks App wrapper on top of pipeline |
| Render / Reporting tools | No native PDF/email rendering in Spark | Output Delta table; build Lakeview (AI/BI) dashboard; set up Job email notifications |
| Spatial tools (without Sedona) | Requires Sedona library | Enable Sedona on cluster; if unavailable, flag **MANUAL**; check `ST_*` availability |
| R Tool | R not available in PySpark runtime | Re-implement logic in Python/PySpark; flag **REVIEW** |
| Custom Tools (`.yxi`) | Packaged extensions specific to Alteryx | Check for Python implementation; consider UDO if reusable; otherwise flag **MANUAL** |
| Map Input | Designer-only visualization | Replace with a Volume-hosted file source |
| Dynamic Input with Macro | Complex recursive/conditional file reads | Evaluate whether dynamic dispatch is needed; use parameterized Python operator with env_config pattern |
| Make Group (connected-components clustering) | Rare graph pattern | Rare in typical workflows; flag for user review; may require graph library like GraphFrames |

---

## 9. File Format Support Matrix

| Category | Format | PySpark Read | Lakeflow Designer Source | Notes |
|---|---|---|---|---|
| **Tabular** | CSV / TSV | `spark.read.csv(...)` | `source` (file_source) | `header`, `delimiter`, `inferSchema` |
| | Fixed-width | `spark.read.text` + `substring` | `python` | Manual column slicing |
| | Excel `.xlsx` / `.xlsm` / `.xlsb` | `pandas.read_excel` → `createDataFrame` or `spark-excel` | `python` | Install `spark-excel` or use pandas + xlrd |
| | Excel `.xls` (legacy) | `pandas.read_excel` | `python` | Same as above |
| **Semi-structured** | JSON | `spark.read.json(...)` | `source` (file_source, format: json) | Multiline option for pretty JSON |
| | XML | `spark.read.format("xml")` | `python` | Install `spark-xml` |
| | Avro | `spark.read.format("avro")` | `source` (file_source, format: avro) | |
| | ORC | `spark.read.format("orc")` | `source` (file_source, format: orc) | |
| | Parquet | `spark.read.parquet(...)` | `source` (file_source, format: parquet) | Native Spark |
| | Delta | `spark.read.table("catalog.schema.table")` | `source` (table_source) | Prefer table_source over Delta path |
| **Stat packages** | SAS `.sas7bdat` | `pandas` + `pyreadstat` or `spark-sas7bdat` | `python` | For small files, pandas is simpler |
| | SPSS `.sav` | `pandas` + `pyreadstat` | `python` | |
| | R `.rds` | `pyreadr.read_r` | `python` | |
| **Alteryx native** | `.yxdb` | **Not recommended** | **MANUAL** | Proprietary binary; export to CSV/Parquet instead |
| | `.yxmd` | N/A | N/A | The workflow itself — input to migration |
| | `.yxmc` | N/A | N/A | Macro definition — inline or extract to Python |
| **Geospatial** | Shapefile `.shp` | `geopandas.read_file` or Sedona | `python` (Sedona) | Requires Sedona or GeoPandas |
| | GeoJSON | `geopandas.read_file` or Sedona | `python` (Sedona) | |
| | KML / KMZ | `geopandas.read_file` (KML via unzip) | `python` | Unzip → read KML |
| | MapInfo TAB | Convert externally first | **MANUAL** | Convert to Shapefile/GeoJSON first |
| **Documents** | PDF (tabular) | `tabula-py` or Databricks AI Functions | `python` | Table extraction |
| | HTML | `pandas.read_html` | `python` | |
| **Compressed** | `.gz` / `.bz2` | Spark reads transparently | `source` | Preserve extension |
| | `.zip` | Unzip first to Volume | `python` | |
| | `.7z` | `py7zr`; unzip first | `python` | |
| **Cloud / DB** | S3 / ADLS / GCS | UC External Volume mounted | `source` (file_source) | |
| | UC Tables / Foreign catalog | `spark.read.table(...)` | `source` (table_source) | Preferred over JDBC |
| | Snowflake / Redshift / SQL Server / Oracle / Postgres | JDBC or UC Federation | `python` (JDBC) or `source` (table_source with foreign catalog) | Prefer Lakehouse Federation |

---

## References & Related

- **PySpark Functions:** `pyspark.sql.functions` module for all F.* operations (trim, soundex, regexp_extract, etc.)
- **Spark SQL:** [Apache Spark SQL Functions](https://spark.apache.org/docs/latest/sql-ref-functions.html)
- **Lakeflow Designer:** [Built-in Operators](https://learn.microsoft.com/en-us/azure/databricks/designer/built-in-operators)
- **Databricks AI Functions:** `ai_analyze_sentiment`, `ai_classify`, `ai_extract`, `ai_mask`, `ai_fix_grammar`, `ai_gen`, `ai_summarize`, `ai_translate`, `ai_similarity`
