# Alteryx → Databricks Migration Knowledge Base — Index

**Repository:** `geniecodeskills-alteryxmigration`  
**Last updated:** 2026-07-30  
**Purpose:** Reusable mapping knowledge extracted from existing SKILL.md documents for Alteryx workflow conversion skill development

---

## Document Map

| Document | Purpose | Scope |
|---|---|---|
| **alteryx-tool-mapping.md** | Comprehensive Alteryx tool → Spark/Lakeflow operator reference | 9 sections: Input/Output, Preparation, Join, Transform, Parse, Spatial, File formats, Medallion tiers, No-equivalent tools |
| **alteryx-formula-function-mapping.md** | Alteryx formula functions → Spark SQL / PySpark equivalents | 11 sections: Conditionals, Nulls, Strings, Numerics, Dates, Aggregates, Type casts, Advanced, Operator precedence, Patterns, Gotchas |
| **alteryx-migration-pre-checks-decomposition.md** | Pre-migration setup & operator selection rules | 9 sections: Pre-checks, Visual operator pre-check, Decomposition patterns, Layer assignment, Visual vs SQL lookup, Anti-patterns, Materialization strategy, Validation framework |
| **alteryx-output-validation-framework.md** | Mandatory validation steps after migration | 9 sections: Setup, Row count, Schema, Type compatibility, Nulls, Numeric aggregations, Row-level diff, Report template, Post-validation actions |
| **This index** | Navigation & quick reference | Overview of all documents |

---

## Quick Reference: Migration Workflow

### Phase 1: Pre-Migration Discovery
**Duration:** 1-2 hours  
**Docs:** `alteryx-migration-pre-checks-decomposition.md` § 1-2

1. ✓ Obtain expected output file (required)
2. ✓ Confirm output save location (catalog, schema, table)
3. ✓ Inventory all input data sources
4. ✓ Risk assessment (identify MANUAL items early)

### Phase 2: Tool Mapping & Design
**Duration:** 2-4 hours  
**Docs:** `alteryx-tool-mapping.md` § 1-6, `alteryx-migration-pre-checks-decomposition.md` § 3-5

1. ✓ Parse Alteryx `.yxmd` file (build DAG)
2. ✓ For each tool, answer 5-question pre-check → select operator
3. ✓ Map to medallion tier (Bronze/Silver/Gold)
4. ✓ Decompose complex operations (one logical step = one operator)
5. ✓ Design data flow in Lakeflow Designer or PySpark notebook

### Phase 3: Implementation
**Duration:** 4-8 hours (depends on complexity)  
**Docs:** `alteryx-formula-function-mapping.md`, `alteryx-tool-mapping.md`

1. ✓ Write Bronze ingestion layer
2. ✓ Write Silver transformation layer (use formula mapping reference)
3. ✓ Write Gold aggregation layer (if applicable)
4. ✓ Translate each Alteryx formula to Spark SQL / PySpark

### Phase 4: Validation (MANDATORY)
**Duration:** 1-2 hours  
**Docs:** `alteryx-output-validation-framework.md`

1. ✓ Load expected output
2. ✓ Run 6 validation checks:
   - Row count
   - Schema
   - Data type compatibility
   - Null counts
   - Numeric aggregations
   - Row-level diff
3. ✓ Compare results → PASS or iterate
4. ✓ Generate validation report

### Phase 5: Optimization (Post-Validation)
**Duration:** 1-2 hours  
**Docs:** `alteryx-migration-pre-checks-decomposition.md` § 8

1. ✓ Identify reused DataFrames → materialize
2. ✓ Consolidate parallel aggregations
3. ✓ Remove debug `.count()` calls
4. ✓ Measure performance improvement

---

## Mandatory Pre-Check: 5 Visual Operator Questions

**Answer ALL FIVE before writing any `sql` or `python` operator.**

```
1. Can a Transform express this?
   → CASE WHEN, CAST, COALESCE, TRIM, UPPER, SOUNDEX, REGEXP_EXTRACT, 
     SPLIT + ELEMENT_AT, DATEDIFF, arithmetic, literals
   → YES → USE TRANSFORM. STOP.

2. Can a Filter express this?
   → Boolean row condition (WHERE clauses)
   → YES → USE FILTER. STOP.

3. Can an Aggregate express this?
   → GROUP BY + SUM/AVG/COUNT/MIN/MAX/MEDIAN/STDDEV/VARIANCE/PERCENTILE
   → YES → USE AGGREGATE. STOP.

4. Can a Join express this?
   → Equi-join on key columns
   → YES → USE JOIN. STOP.

5. Can a Sort, Limit, Pivot, or Combine express this?
   → ORDER BY, TOP N, reshape, UNION/INTERSECT/EXCEPT
   → YES → USE VISUAL OPERATOR. STOP.

IF ALL FIVE ARE NO:
   → Only then may you proceed to sql or python.
```

**Skipping this pre-check is the #1 cause of wrong operator selection.**

---

## Commonly Used Tool Mappings (Quick Lookup)

| Alteryx Tool | Operator | Notes |
|---|---|---|
| Input Data (CSV) | `source` (VDP) / `spark.read.csv()` (PySpark) | Use UC Volumes for paths |
| Filter | `filter` (VDP) / `.filter()` (PySpark) | T/F outputs → two parallel filters with inverse conditions |
| Formula | `transform` (VDP) / `.withColumn()` (PySpark) | Use `alteryx-formula-function-mapping.md` for function translations |
| Join | `join` (VDP) / `.join()` (PySpark) | L/J/R outputs → multiple joins or SQL UNION ALL |
| Summarize | `aggregate` (VDP) / `.groupBy().agg()` (PySpark) | COUNT(DISTINCT) requires `sql` operator |
| Multi-Row Formula | `sql` (VDP) / Window functions (PySpark) | Use `F.lag()`, `F.lead()`, `F.row_number().over()` |
| Cross Tab | `pivot` (VDP) / `.pivot()` (PySpark) | Rows → Columns transformation |
| Transpose | `pivot` (VDP, mode: unpivot) / `stack()` (PySpark) | Columns → Rows transformation |
| Output Data | `output` (VDP) / `.write.saveAsTable()` (PySpark) | Always materialize to Delta |
| Render / Reporting | **MANUAL** | Output Delta + Lakeview dashboard |
| Spatial / Macro | **MANUAL** | Flag for review; may require Sedona, Lakeflow Job, UDO |

---

## Troubleshooting Guide

### Row Count Mismatch

| Symptom | Likely Cause | Reference |
|---|---|---|
| Migrated output has MORE rows | Missing filter condition, or duplicate join | Tool mapping § 2.1; Decomposition § 4.2 |
| Migrated output has FEWER rows | Over-filtering, or INNER join instead of LEFT | Tool mapping § 2.2; Pre-checks § 4.2 |
| Slightly fewer rows (< 1% diff) | Data regeneration, time window change | Validation framework § 7 |

**Action:** Re-run row-count validation; investigate specific filter/join logic.

### Schema Mismatch

| Symptom | Likely Cause | Reference |
|---|---|---|
| Missing columns | Column not derived in Formula tool, or renamed wrongly | Tool mapping § 1; Formula mapping § 2 |
| Extra columns | Temporary columns not dropped in Select | Tool mapping § 1 |
| Wrong data type | Explicit cast missing; Alteryx auto-casted; Spark didn't | Formula mapping § 4 |

**Action:** Add Transform operator to rename/drop/cast as needed.

### Numeric Values Differ

| Difference | Likely Cause | Reference |
|---|---|---|
| < 0.1% | Normal Alteryx vs Spark precision variation | Validation framework § 7 |
| 0.1% – 2% | Data regenerated (different random seed) | Validation framework § 7 |
| > 2% | Formula error, wrong aggregation, or rounding | Validation framework § 2-3; Troubleshooting |

**Action:** Check formula matches Alteryx; verify aggregation GROUP BY fields; ensure rounding is applied consistently.

### Null Count Mismatch

| Symptom | Likely Cause | Reference |
|---|---|---|
| More NULLs in migrated | Filter removing rows, or over-aggressive nullification | Pre-checks § 4.2; Validation § 4 |
| Fewer NULLs in migrated | Missing null-fill logic (COALESCE, FILLNA) | Formula mapping § 2; Tool mapping § 1 (Imputation) |

**Action:** Check Alteryx filter conditions and null-fill logic; add Transform or Imputation operator.

---

## Key Decision Trees

### Operator Selection (Mandatory Pre-Check)
**Path:** `alteryx-migration-pre-checks-decomposition.md` § 2

**Decision:** For each Alteryx tool, run 5-question pre-check → choose operator

### Medallion Tier Assignment
**Path:** `alteryx-tool-mapping.md` § 7, `alteryx-migration-pre-checks-decomposition.md` § 5

**Decision:** Bronze (raw) → Silver (cleansed) → Gold (aggregated)

### Decomposition Strategy
**Path:** `alteryx-migration-pre-checks-decomposition.md` § 3-4

**Decision:** One logical step = one operator; only consolidate if user approves

### Materialization Decision
**Path:** `alteryx-migration-pre-checks-decomposition.md` § 8

**Decision:** If DataFrame used in N > 1 downstream actions → materialize

### Validation Strategy
**Path:** `alteryx-output-validation-framework.md`

**Decision:** 6 checks → all pass = READY; any fail = iterate

---

## Formula Translation Examples

### Conditional: IF / THEN / ELSE

**Alteryx:**
```
IF region = 'APAC' THEN revenue * 1.15
ELSEIF region = 'LATAM' THEN revenue * 1.10
ELSE revenue
ENDIF
```

**Spark SQL:**
```sql
CASE WHEN region = 'APAC' THEN revenue * 1.15
     WHEN region = 'LATAM' THEN revenue * 1.10
     ELSE revenue
END AS adjusted_revenue
```

**PySpark:**
```python
F.when(F.col("region") == "APAC", F.col("revenue") * 1.15) \
 .when(F.col("region") == "LATAM", F.col("revenue") * 1.10) \
 .otherwise(F.col("revenue"))
```

**Doc reference:** `alteryx-formula-function-mapping.md` § 1

### String: REGEX_Extract

**Alteryx:**
```
REGEX_Extract(email, "^[^@]+@([^\.]+)", 1)
```

**Spark SQL:**
```sql
REGEXP_EXTRACT(email, '^[^@]+@([^\.]+)', 1) AS domain_prefix
```

**PySpark:**
```python
F.regexp_extract(F.col("email"), r"^[^@]+@([^\.]+)", 1)
```

**Doc reference:** `alteryx-formula-function-mapping.md` § 3

### Window: Running Total

**Alteryx Multi-Row Formula:**
```
SUM([value]) running over (PARTITION BY region ORDER BY date)
```

**Spark SQL:**
```sql
SUM(value) OVER (PARTITION BY region ORDER BY date ROWS UNBOUNDED PRECEDING) AS running_total
```

**PySpark:**
```python
from pyspark.sql import Window
w = Window.partitionBy("region").orderBy("date").rangeBetween(Window.unboundedPreceding, 0)
df.withColumn("running_total", F.sum("value").over(w))
```

**Doc reference:** `alteryx-migration-pre-checks-decomposition.md` § 4.4; `alteryx-formula-function-mapping.md` § 6

---

## File Format Support

**Preferred formats (direct source operator):**
- CSV / TSV / Parquet / Avro / ORC / JSON
- UC Tables (table_source)

**Supported via Python operator:**
- Excel (pandas + createDataFrame)
- XML (spark-xml)
- SAS / SPSS / R (pandas + custom readers)
- Geospatial (Sedona)

**Not supported (manual export required):**
- `.yxdb` (Alteryx proprietary) → export to CSV
- `.yxmc` (macros) → inline or extract to Python
- `.yxi` (custom tools) → flag **MANUAL**

**Reference:** `alteryx-tool-mapping.md` § 9

---

## Anti-Patterns to Avoid

| ❌ Anti-Pattern | ✅ Correct Approach | Reference |
|---|---|---|
| Python with `F.withColumn(col, F.case(...))` | Use Transform with CASE WHEN | Pre-checks § 7 |
| SQL for UPPER/TRIM/COALESCE | Use Transform | Pre-checks § 6; Tool mapping § 1 |
| SQL for simple SOUNDEX/REGEXP_EXTRACT | Use Transform | Pre-checks § 6 |
| AI Function on millions of rows for standardization | Transform CASE WHEN or Join lookup table | Pre-checks § 7 |
| Monolithic Python for groups + transforms + joins + CSV | Decompose into separate operators | Pre-checks § 7 |
| `.cache()` on serverless compute | Use temp Delta table pattern | Pre-checks § 8.2 |
| Merging multiple window functions into one SQL without asking | Keep separate; ask user if consolidation preferred | Pre-checks § 3.2 |
| Skipping the 5-question pre-check | Answer all 5 before writing sql/python | Pre-checks § 2 |

---

## Mandatory Steps (NEVER Skip)

1. **Obtain expected output file** — required before migration begins
2. **Run 5-question pre-check** — before every sql/python operator
3. **One logical step = one operator** — decompose complex operations
4. **Run full validation** — row count, schema, nulls, aggregations, row diff
5. **Performance optimization** — identify reused DataFrames; materialize
6. **Post-validation cleanup** — remove debug nodes; document findings

---

## Support & Gotchas

### Alteryx ↔ Spark Differences

| Aspect | Alteryx | Spark | Mitigation |
|---|---|---|---|
| **Sort order** | Deterministic | Non-deterministic without explicit ORDER BY | Always add `.orderBy()` before `.limit()` |
| **Null vs empty string** | Treated differently | Usually treated as NULL or distinguished per function | Be explicit about empty string handling |
| **Date format strings** | `%Y-%m-%d` (strftime) | `yyyy-MM-dd` (SimpleDateFormat) | Convert format strings during migration |
| **Fixed-point precision** | FixedDecimal(19,6) | IEEE 754 double (64-bit) | Tolerance ~0.1% on numeric values |
| **Division by zero** | Returns error or NULL | Returns NaN or Infinity | Use CASE WHEN to guard |

### Known Limitations

| Issue | Symptom | Workaround |
|---|---|---|
| Workspace `file:` paths | FAILED_READ_FILE with @ in path | Use UC Volumes |
| `.yxdb` files | yxdb Python lib fails on "e2 Database" format | Export to CSV/Parquet first |
| Iterative macros | No native fixed-point loop | Use Lakeflow Job For Each task |
| `.cache()` on serverless | Not supported | Use temp Delta table pattern |
| Config stripping in Designer | Python/SQL configs reset on template change | Never change template type; recreate operator |

**Reference:** `alteryx-migration-pre-checks-decomposition.md` § 5; `alteryx-output-validation-framework.md` § 7

---

## Getting Started

### For a Single Alteryx Workflow Migration

1. Read `alteryx-migration-pre-checks-decomposition.md` § 1 (Pre-checks)
2. Run mandatory pre-checks (expected output, save location, source inventory)
3. For each Alteryx tool:
   - Look up in `alteryx-tool-mapping.md`
   - Run 5-question pre-check from `alteryx-migration-pre-checks-decomposition.md` § 2
   - If formula needed, reference `alteryx-formula-function-mapping.md`
   - Design operator(s) following decomposition rules
4. Build Bronze → Silver → Gold layers
5. Run validation checklist from `alteryx-output-validation-framework.md`
6. Optimize performance per `alteryx-migration-pre-checks-decomposition.md` § 8

### For Building an Alteryx Migration Skill

1. Use all 5 reference documents as foundation knowledge
2. Encode the 5-question pre-check as a decision gate (§ 2 of pre-checks doc)
3. Implement tool mapping as lookup table (§ 1-6 of tool-mapping doc)
4. Implement formula function translation (formula-function-mapping doc)
5. Automate decomposition patterns (pre-checks § 3-4)
6. Automate validation checks (output-validation-framework doc)

---

## Document Statistics

| Document | Sections | Tables | Code Examples | Pages (approx) |
|---|---|---|---|---|
| alteryx-tool-mapping.md | 9 | 27 | 15 | 20 |
| alteryx-formula-function-mapping.md | 11 | 35 | 25 | 22 |
| alteryx-migration-pre-checks-decomposition.md | 9 | 18 | 30 | 25 |
| alteryx-output-validation-framework.md | 9 | 12 | 20 | 18 |
| **Total** | **38** | **92** | **90** | **85** |

---

## Version History

| Version | Date | Changes |
|---|---|---|
| 1.0 | 2026-07-30 | Initial extraction from geniecodeskills-alteryxmigration SKILL.md; 5 reference documents |

---

## Contact & Questions

For questions or corrections to these mappings:
1. Refer to the original source: `/tmp/geniecodeskills-alteryxmigration/Skills/alteryxToPythonSpark/SKILL.md` and `alteryxToLakeflowDesigner/SKILL.md`
2. Test migration against expected output using validation framework
3. Flag any missing mappings or ambiguities for the skill author

---

## Copyright & Attribution

**Source:** `isaac-r-sa/geniecodeskills-alteryxmigration` repository  
**Extracted:** 2026-07-30  
**Purpose:** Internal reference for Databricks field engineering migration skill development  
**Status:** Faithful extraction — all mappings are verbatim from source SKILL.md documents
