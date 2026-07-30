# Alteryx Migration: Output Validation Framework

**Source:** `geniecodeskills-alteryxmigration` SKILL.md documents  
**Last updated:** 2026-07-30  
**Context:** Mandatory validation steps after migration; comparison of PySpark output against expected Alteryx baseline

---

## Overview

**This step is MANDATORY.** Every Alteryx migration must end with output validation comparing the migrated code's results against the expected Alteryx output file.

**Validation is NOT optional.** Correct results with poor performance is an incomplete migration; incorrect results with any performance is a failed migration.

---

## Pre-Migration Setup: Expected Output File

### Required Before Migration Begins

**STOP if the user cannot provide an expected output file.** Ask explicitly:

```
To validate the converted pipeline produces correct results, I need an expected 
output file — typically the CSV/Excel that the Alteryx workflow originally produced.

Could you provide one of the following?
1. A CSV/Parquet/Excel file with the expected output data
2. A path to an existing Delta table with expected results
3. A sample of the expected output (at minimum: column names, row count, and 5-10 sample rows)

Without this, I cannot guarantee the migration produces correct results.
```

### Expected Output Baseline Profile

Once obtained, immediately profile the expected output:

```python
import pyspark.sql.functions as F

# Load expected output (example: CSV)
df_expected = spark.read.csv(
    "/Volumes/catalog/schema/expected_output.csv",
    header=True,
    inferSchema=True
)

# Profile
print(f"Row count: {df_expected.count()}")
print(f"Columns: {df_expected.columns}")
print(f"Schema:\n{df_expected.printSchema()}")
print(f"\nSample rows:")
df_expected.show(5, truncate=False)
print(f"\nNull counts per column:")
df_expected.select([F.count(F.when(F.col(c).isNull(), 1)).alias(c) 
                    for c in df_expected.columns]).show(truncate=False)
```

**Store this profile for later comparison.**

---

## 1. Row Count Validation

### What to Check

Total row count of migrated output vs expected output.

### Validation Code

```python
expected_count = df_expected.count()
migrated_count = df_migrated.count()

row_count_match = expected_count == migrated_count
row_count_diff = migrated_count - expected_count

print(f"Expected rows: {expected_count}")
print(f"Migrated rows: {migrated_count}")
print(f"Difference: {row_count_diff} ({row_count_diff / expected_count * 100:.2f}%)")
print(f"Status: {'✓ PASS' if row_count_match else '✗ FAIL'}")
```

### Pass Criteria

- **Exact match** — preferred
- **< 1% difference** — acceptable if data was regenerated or time-window changed
- **> 1% difference** — investigate

### Action on Fail

- Check Alteryx filter conditions (too many rows → missing filter)
- Check Alteryx join type (too few rows → wrong join type, e.g., should be LEFT not INNER)
- Check for duplicate rows (too many → missing UNIQUE / DROPDUPLICATES)
- Verify source data is identical

---

## 2. Schema Validation

### What to Check

1. Column names match
2. Column data types are compatible
3. Column order (if important for downstream tools)

### Validation Code

```python
expected_cols = set(df_expected.columns)
migrated_cols = set(df_migrated.columns)

missing_in_migrated = expected_cols - migrated_cols
extra_in_migrated = migrated_cols - expected_cols
schema_match = expected_cols == migrated_cols

print("Expected columns:", sorted(expected_cols))
print("Migrated columns:", sorted(migrated_cols))
print(f"\nMissing: {missing_in_migrated if missing_in_migrated else 'None'}")
print(f"Extra: {extra_in_migrated if extra_in_migrated else 'None'}")
print(f"Status: {'✓ PASS' if schema_match else '✗ FAIL'}")

# Data type comparison
if not schema_match:
    print("\nData types (common columns):")
    common_cols = expected_cols & migrated_cols
    exp_types = {f.name: str(f.dataType) for f in df_expected.schema.fields if f.name in common_cols}
    mig_types = {f.name: str(f.dataType) for f in df_migrated.schema.fields if f.name in common_cols}
    
    for col in sorted(common_cols):
        exp_t = exp_types[col]
        mig_t = mig_types[col]
        match_icon = "✓" if exp_t == mig_t else "✗"
        print(f"  {match_icon} {col}: expected {exp_t} vs migrated {mig_t}")
```

### Pass Criteria

- All expected columns present
- No unexpected extra columns (unless intentionally added)
- Common columns have compatible types:
  - StringType ↔ VarcharType: compatible
  - IntegerType ↔ LongType: compatible (but require implicit cast)
  - DoubleType ↔ FloatType: compatible (but precision loss possible)
  - DecimalType ↔ DoubleType: compatible (but precision loss)

### Action on Fail

- **Missing columns:** Check Alteryx Select/Formula tools — formula likely deriving wrong name
- **Extra columns:** Check whether temporary/intermediate columns were not dropped
- **Type mismatch:**
  - IntegerType → expected LongType: add `.cast("long")` in Transform
  - StringType → expected DoubleType: check Alteryx type inference; add explicit cast

---

## 3. Data Type Compatibility Check

### What to Check

For common columns, verify data types are compatible and precision/range is preserved.

### Validation Code

```python
def check_type_compatibility(expected_type, migrated_type):
    """Return True if types are compatible for comparison."""
    compatible_pairs = [
        ("StringType", "StringType"),
        ("StringType", "VarcharType"),
        ("IntegerType", "IntegerType"),
        ("IntegerType", "LongType"),
        ("LongType", "LongType"),
        ("DoubleType", "DoubleType"),
        ("DoubleType", "FloatType"),
        ("FloatType", "FloatType"),
        ("BooleanType", "BooleanType"),
        ("DateType", "DateType"),
        ("TimestampType", "TimestampType"),
        ("DecimalType", "DecimalType"),
    ]
    return (expected_type, migrated_type) in compatible_pairs or expected_type == migrated_type

common_cols = set(df_expected.columns) & set(df_migrated.columns)
exp_types = {f.name: str(f.dataType) for f in df_expected.schema.fields if f.name in common_cols}
mig_types = {f.name: str(f.dataType) for f in df_migrated.schema.fields if f.name in common_cols}

type_mismatches = {}
for col in common_cols:
    exp_t = exp_types[col]
    mig_t = mig_types[col]
    if not check_type_compatibility(exp_t, mig_t):
        type_mismatches[col] = {"expected": exp_t, "migrated": mig_t}

print(f"Type compatibility: {'✓ PASS' if not type_mismatches else '✗ FAIL'}")
if type_mismatches:
    for col, mismatch in type_mismatches.items():
        print(f"  ✗ {col}: {mismatch['expected']} → {mismatch['migrated']}")
```

### Pass Criteria

- All common columns have compatible types
- No precision loss (e.g., Decimal → Double is acceptable with ~0.1% numeric diff)
- Boolean/Date/Timestamp types match exactly

### Action on Fail

- Add explicit `.cast()` in Transform operator to enforce expected type
- Use `TRY_CAST()` to handle dirty data gracefully

---

## 4. Null Count Validation

### What to Check

For each column, verify the number of NULL values matches.

### Validation Code

```python
null_comparison = {}
common_cols = set(df_expected.columns) & set(df_migrated.columns)

for col in sorted(common_cols):
    exp_nulls = df_expected.filter(F.col(col).isNull()).count()
    mig_nulls = df_migrated.filter(F.col(col).isNull()).count()
    
    if exp_nulls != mig_nulls:
        null_comparison[col] = {
            "expected_nulls": exp_nulls,
            "migrated_nulls": mig_nulls,
            "diff": mig_nulls - exp_nulls
        }

null_match = len(null_comparison) == 0
print(f"Null counts: {'✓ PASS' if null_match else '✗ FAIL'}")

if null_comparison:
    print("\nNull count discrepancies:")
    for col, stats in null_comparison.items():
        print(f"  ✗ {col}: expected {stats['expected_nulls']}, migrated {stats['migrated_nulls']} (diff: {stats['diff']})")
```

### Pass Criteria

- Exact match for all columns

### Action on Fail

- **More NULLs in migrated:** Check Filter condition (filtering out rows incorrectly) or Join type (should be LEFT, not INNER)
- **Fewer NULLs in migrated:** Check Imputation / Null-fill logic in Alteryx (e.g., Data Cleansing → fillna)
- **Different column is NULL:** Check column rename or derivation formula

---

## 5. Numeric Aggregation Validation

### What to Check

For numeric columns, compare sum, average, min, max to detect calculation errors.

### Validation Code

```python
def numeric_aggregation_comparison(df_expected, df_migrated, tolerance=1e-6):
    """Compare SUM/AVG/MIN/MAX for all numeric columns."""
    numeric_cols = [f.name for f in df_expected.schema.fields
                    if str(f.dataType) in ("DoubleType", "FloatType", "IntegerType",
                                           "LongType", "DecimalType(38,18)", "ShortType")
                    and f.name in set(df_migrated.columns)]
    
    agg_comparison = {}
    
    for col in numeric_cols:
        exp_stats = df_expected.select(
            F.sum(col).alias("sum"),
            F.avg(col).alias("avg"),
            F.min(col).alias("min"),
            F.max(col).alias("max")
        ).collect()[0]
        
        mig_stats = df_migrated.select(
            F.sum(col).alias("sum"),
            F.avg(col).alias("avg"),
            F.min(col).alias("min"),
            F.max(col).alias("max")
        ).collect()[0]
        
        diffs = {}
        for stat in ["sum", "avg", "min", "max"]:
            e = exp_stats[stat]
            m = mig_stats[stat]
            
            if e is None and m is None:
                continue  # Both NULL — OK
            elif e is None or m is None:
                diffs[stat] = {"expected": e, "migrated": m, "pct_diff": None}
            else:
                e_val = float(e)
                m_val = float(m)
                abs_diff = abs(e_val - m_val)
                pct_diff = (abs_diff / abs(e_val) * 100) if e_val != 0 else None
                
                if abs_diff > tolerance:
                    diffs[stat] = {
                        "expected": e_val,
                        "migrated": m_val,
                        "abs_diff": abs_diff,
                        "pct_diff": pct_diff
                    }
        
        if diffs:
            agg_comparison[col] = diffs
    
    return agg_comparison

agg_diff = numeric_aggregation_comparison(df_expected, df_migrated)
agg_match = len(agg_diff) == 0
print(f"Numeric aggregations: {'✓ PASS' if agg_match else '✗ FAIL'}")

if agg_diff:
    print("\nNumeric discrepancies:")
    for col, stats in agg_diff.items():
        print(f"  ✗ {col}:")
        for stat, vals in stats.items():
            pct = f" ({vals['pct_diff']:.2f}%)" if vals['pct_diff'] is not None else ""
            print(f"      {stat}: expected {vals['expected']}, migrated {vals['migrated']}{pct}")
```

### Pass Criteria

**Numeric tolerance:**
- **< 0.1% difference:** Normal (Alteryx intermediate rounding vs Spark continuous precision)
- **0.1% – 2% difference:** Acceptable if source data was regenerated (different random seed)
- **> 2% difference:** **Investigate** — likely a logic error in Filter, Formula, or Aggregation

### Action on Fail

- Check Alteryx formula precision (FixedDecimal(19,6) vs IEEE 754 double)
- Verify filter conditions are identical
- Check aggregation GROUP BY fields match Alteryx
- Verify join keys don't include extra matches
- Compare rounding behavior (Alteryx may round intermediate steps; Spark does not)

---

## 6. Row-Level Diff Validation (If Key Columns Available)

### What to Check

For a given set of key columns, find rows present in one output but not the other.

### Validation Code

```python
def row_level_diff(df_expected, df_migrated, key_columns):
    """Find rows in only expected, only migrated, or different values."""
    if not key_columns or not all(c in set(df_expected.columns) & set(df_migrated.columns) for c in key_columns):
        print("Key columns not available for row-level comparison.")
        return None
    
    # Rows only in expected
    only_in_expected = df_expected.join(df_migrated, on=key_columns, how="left_anti")
    exp_only_count = only_in_expected.count()
    
    # Rows only in migrated
    only_in_migrated = df_migrated.join(df_expected, on=key_columns, how="left_anti")
    mig_only_count = only_in_migrated.count()
    
    print(f"Rows only in expected: {exp_only_count}")
    print(f"Rows only in migrated: {mig_only_count}")
    
    if exp_only_count > 0:
        print("\nSample rows only in expected:")
        only_in_expected.show(5, truncate=False)
    
    if mig_only_count > 0:
        print("\nSample rows only in migrated:")
        only_in_migrated.show(5, truncate=False)
    
    match = exp_only_count == 0 and mig_only_count == 0
    print(f"\nStatus: {'✓ PASS' if match else '✗ FAIL'}")
    
    return {
        "rows_only_in_expected": exp_only_count,
        "rows_only_in_migrated": mig_only_count,
        "match": match
    }

# Usage
key_cols = ["customer_id", "transaction_date"]  # Adjust per your data
row_diff = row_level_diff(df_expected, df_migrated, key_cols)
```

### Pass Criteria

- `rows_only_in_expected = 0` AND `rows_only_in_migrated = 0`

### Action on Fail

- **Rows only in expected:** Check Alteryx filter conditions — migration may be too aggressive
- **Rows only in migrated:** Check Alteryx filter logic — migration may not be filtering correctly
- **Investigate sample rows:** Examine the differing key values to understand the pattern

---

## 7. Numeric Precision Tolerance Guidance

Alteryx uses `FixedDecimal` types (typically 19.6 — 19 digits, 6 decimal places). Spark uses IEEE 754 double-precision.

### Expected Differences

| Difference | Cause | Action |
|---|---|---|
| **< 0.1%** | Normal Alteryx intermediate rounding vs Spark continuous precision | Accept — normal variation |
| **0.1% – 2%** | Source data regenerated (different random seed) or time window changed | Verify structure: row counts, granularity types match pattern |
| **> 2%** | Logic error: formula, filter, join, or aggregation incorrect | **INVESTIGATE** — debug the specific calculation |

### Structural Match Criteria (When Values Differ Due to Data Regeneration)

Use these to confirm the migration is correct even if numeric values differ:

- ✓ Same number of time periods processed
- ✓ Same granularity types/labels present
- ✓ Same column names and data types
- ✓ Row counts per granularity follow the same pattern (e.g., N periods, M regions → N×M rows for cross-tab)
- ✓ Aggregate structure matches (e.g., one row per customer if grouped by customer)

---

## 8. Complete Validation Report Template

```python
def generate_validation_report(df_expected, df_migrated, key_columns=None):
    """Generate a comprehensive validation report."""
    results = {}
    
    # 1. Row count
    expected_count = df_expected.count()
    migrated_count = df_migrated.count()
    results["row_count"] = {
        "expected": expected_count,
        "migrated": migrated_count,
        "match": expected_count == migrated_count
    }
    
    # 2. Schema
    expected_cols = set(df_expected.columns)
    migrated_cols = set(df_migrated.columns)
    results["schema"] = {
        "missing_in_migrated": expected_cols - migrated_cols,
        "extra_in_migrated": migrated_cols - expected_cols,
        "match": expected_cols == migrated_cols
    }
    
    # 3. Null counts
    common_cols = expected_cols & migrated_cols
    null_comparison = {}
    for col in sorted(common_cols):
        exp_nulls = df_expected.filter(F.col(col).isNull()).count()
        mig_nulls = df_migrated.filter(F.col(col).isNull()).count()
        if exp_nulls != mig_nulls:
            null_comparison[col] = {"expected_nulls": exp_nulls, "migrated_nulls": mig_nulls}
    results["null_counts"] = {
        "discrepancies": null_comparison,
        "match": len(null_comparison) == 0
    }
    
    # 4. Numeric aggregations
    numeric_cols = [f.name for f in df_expected.schema.fields
                    if str(f.dataType) in ("DoubleType", "FloatType", "IntegerType", "LongType")
                    and f.name in common_cols]
    agg_comparison = {}
    for col in numeric_cols:
        exp_stats = df_expected.select(
            F.sum(col).alias("sum"), F.avg(col).alias("avg"),
            F.min(col).alias("min"), F.max(col).alias("max")
        ).collect()[0]
        mig_stats = df_migrated.select(
            F.sum(col).alias("sum"), F.avg(col).alias("avg"),
            F.min(col).alias("min"), F.max(col).alias("max")
        ).collect()[0]
        for stat in ["sum", "avg", "min", "max"]:
            e, m = exp_stats[stat], mig_stats[stat]
            if e is not None and m is not None and abs(float(e) - float(m)) > 1e-6:
                agg_comparison[col] = {stat: {"expected": float(e), "migrated": float(m)}}
    results["numeric_aggregations"] = {
        "discrepancies": agg_comparison,
        "match": len(agg_comparison) == 0
    }
    
    # 5. Row-level diff (if key columns provided)
    if key_columns and all(c in common_cols for c in key_columns):
        only_exp = df_expected.join(df_migrated, on=key_columns, how="left_anti").count()
        only_mig = df_migrated.join(df_expected, on=key_columns, how="left_anti").count()
        results["row_diff"] = {
            "rows_only_in_expected": only_exp,
            "rows_only_in_migrated": only_mig,
            "match": only_exp == 0 and only_mig == 0
        }
    
    # Summary
    all_passed = all(v.get("match", True) for v in results.values())
    results["overall"] = "PASS" if all_passed else "FAIL"
    
    return results

# Usage
results = generate_validation_report(df_expected, df_migrated, key_columns=["id"])

# Print summary
print(f"\n{'='*60}")
print(f"VALIDATION REPORT")
print(f"{'='*60}")
print(f"Overall: {results['overall']}")
for check, detail in results.items():
    if check != "overall":
        status = "PASS" if detail.get("match", True) else "FAIL"
        print(f"  {check}: {status}")
        if not detail.get("match", True):
            for k, v in detail.items():
                if k != "match":
                    print(f"    {k}: {v}")
```

---

## 9. Post-Validation Actions

### If All Checks Pass ✓

1. **Remove validation nodes** from the pipeline (they are temporary debugging tools)
2. **Document findings** in the migration README
3. **Mark migration as COMPLETE**
4. **Schedule for production deployment**

### If Some Checks Fail ✗

1. **Identify failing check** (row count, schema, nulls, aggregations, row diff)
2. **Investigate root cause** using the "Action on Fail" guidance in each section
3. **Fix the migrated transformation** (adjust formula, filter, join, aggregation)
4. **Re-run validation** until all checks pass
5. **Iterate until PASS**

### Common Fixes

| Failing Check | Likely Cause | Fix |
|---|---|---|
| Row count too high | Missing filter or duplicate join | Add filter operator or check join type |
| Row count too low | Over-filtering or inner join instead of left | Verify filter conditions; change join to LEFT |
| Schema mismatch | Wrong column name in formula | Rename column in Transform operator |
| Extra nulls | Filter removing rows; missing COALESCE | Check filter condition; add null-fill logic |
| Fewer nulls | Implicit null handling in Alteryx | Add null-fill Transform or Imputation |
| Numeric discrepancy > 2% | Formula error, rounding difference, or precision | Verify formula matches Alteryx; check casting |
| Row-level diff | Mismatched join keys or filter logic | Debug specific differing rows; fix join or filter |

---

## References

- **Spark DataFrame Validation:** [DataFrame.filter()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html), [DataFrame.select()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html)
- **Alteryx Data Types:** [Alteryx data types and casting](https://help.alteryx.com/2023.2/designer/data-types)
- **Spark SQL Data Types:** [Spark SQL Data Types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
- **IEEE 754 Floating-Point Precision:** [IEEE 754 - Double precision (64-bit)](https://en.wikipedia.org/wiki/IEEE_754)
