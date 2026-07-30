# Alteryx Formula Functions → Spark SQL / PySpark Reference

**Source:** `geniecodeskills-alteryxmigration` SKILL.md documents  
**Last updated:** 2026-07-30  
**Context:** Formula tool expressions and Multi-Row formulas in Alteryx → Spark SQL / PySpark equivalents

---

## Overview

Alteryx Formula tool expressions map to:
1. **Spark SQL functions** — preferred for Transform operator (Lakeflow Designer) and PySpark `.withColumn()` expressions
2. **PySpark functions** — `F.*` from `pyspark.sql.functions` for Databricks notebooks

The Transform operator in Lakeflow Designer compiles SQL expressions directly; PySpark notebooks use function calls and expressions.

---

## 1. Conditional & Control Flow

### IF / THEN / ELSE

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `IF <condition> THEN <expr> ELSE <expr> ENDIF` | `CASE WHEN <condition> THEN <expr> ELSE <expr> END` | `F.when(F.col(...), <expr>).otherwise(<expr>)` |
| Multi-branch `IF...ELSEIF...ELSE` | `CASE WHEN ... WHEN ... ELSE ... END` | Chained `.when(...).when(...).otherwise(...)` |
| `IF IsNull(col) THEN ... ELSE ... ENDIF` | `CASE WHEN col IS NULL THEN ... ELSE ... END` | `F.when(F.col(...).isNull(), ...).otherwise(...)` |

**Example:**
```sql
-- Spark SQL
CASE WHEN region = 'APAC' THEN revenue * 1.1
     WHEN region = 'EMEA' THEN revenue * 1.05
     ELSE revenue
END

-- PySpark
F.when(F.col("region") == "APAC", F.col("revenue") * 1.1) \
 .when(F.col("region") == "EMEA", F.col("revenue") * 1.05) \
 .otherwise(F.col("revenue"))
```

---

## 2. Null Handling

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `IsNull(col)` | `col IS NULL` | `F.col(...).isNull()` |
| `IsNotNull(col)` | `col IS NOT NULL` | `F.col(...).isNotNull()` |
| `Coalesce(col1, col2, fallback)` | `COALESCE(col1, col2, fallback)` | `F.coalesce(F.col("col1"), F.col("col2"), F.lit(fallback))` |
| `NVL(col, default)` | `COALESCE(col, default)` | `F.coalesce(F.col("col"), F.lit(default))` |
| `IfNull(col, default)` | `COALESCE(col, default)` | `F.coalesce(F.col("col"), F.lit(default))` |

**Example:**
```sql
-- Spark SQL
COALESCE(unit_price, list_price, 0.0) AS price_final

-- PySpark
F.coalesce(F.col("unit_price"), F.col("list_price"), F.lit(0.0)).alias("price_final")
```

---

## 3. String Functions

### Case Conversion

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `Upper(col)` | `UPPER(col)` | `F.upper(F.col("col"))` |
| `Lower(col)` | `LOWER(col)` | `F.lower(F.col("col"))` |
| `Proper(col)` / `InitCap` | `INITCAP(col)` | `F.initcap(F.col("col"))` |

### String Trimming & Cleaning

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `Trim(col)` | `TRIM(col)` | `F.trim(F.col("col"))` |
| `LTrim(col)` | `LTRIM(col)` | `F.ltrim(F.col("col"))` |
| `RTrim(col)` | `RTRIM(col)` | `F.rtrim(F.col("col"))` |
| `Replace(col, old, new)` | `REPLACE(col, old, new)` | `F.regexp_replace(F.col("col"), old, new)` |
| `Substitute(col, old, new)` | `REPLACE(col, old, new)` | Same as Replace |

### String Length & Substring

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `Length(col)` | `LENGTH(col)` | `F.length(F.col("col"))` |
| `Left(col, n)` | `SUBSTRING(col, 1, n)` or `LEFT(col, n)` | `F.substring(F.col("col"), 1, n)` |
| `Right(col, n)` | `SUBSTRING(col, LENGTH(col) - n + 1)` or `RIGHT(col, n)` | `F.substring(F.col("col"), F.length(F.col("col")) - n + 1)` |
| `Substring(col, start, len)` | `SUBSTRING(col, start, len)` | `F.substring(F.col("col"), start, len)` |
| `Mid(col, start, len)` | `SUBSTRING(col, start, len)` | Same as Substring |

### String Concatenation

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `col1 + col2` (concat) | `CONCAT(col1, col2)` or `col1 \|\| col2` | `F.concat(F.col("col1"), F.col("col2"))` |
| `Concat(col1, col2, ..., delim)` | `CONCAT_WS(delim, col1, col2, ...)` | `F.concat_ws(delim, F.col("col1"), F.col("col2"))` |

### String Splitting & Indexing

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `Find(substring, col)` | `INSTR(col, substring)` | `F.instr(F.col("col"), substring)` |
| `Contains(col, substring)` | `col LIKE '%substring%'` or `INSTR(col, substring) > 0` | `F.col("col").contains("substring")` |
| `StartsWith(col, prefix)` | `col LIKE 'prefix%'` or `SUBSTR(col, 1, len) = prefix` | `F.col("col").startsWith("prefix")` |
| `EndsWith(col, suffix)` | `col LIKE '%suffix'` or `SUBSTR(col, -len) = suffix` | `F.col("col").endsWith("suffix")` |
| `Split(col, delim)[n]` (nth element) | `SPLIT(col, delim)[n]` — **note:** 0-indexed | `F.split(F.col("col"), delim)[n]` or `ELEMENT_AT(SPLIT(col, delim), n+1)` |

**Example:**
```sql
-- Extract domain from email (Spark SQL)
REGEXP_EXTRACT(email, '@(.+)$', 1) AS domain
-- OR
ELEMENT_AT(SPLIT(email, '@'), 2) AS domain

-- PySpark
F.regexp_extract(F.col("email"), "@(.+)$", 1).alias("domain")
# OR
F.element_at(F.split(F.col("email"), "@"), 2).alias("domain")
```

### Regular Expressions

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `RegEx_Match(col, pattern)` | `col REGEXP pattern` or `REGEXP_LIKE(col, pattern)` | Not direct; use `filter(F.col(...).rlike(pattern))` or extract |
| `RegEx_Extract(col, pattern, group)` | `REGEXP_EXTRACT(col, pattern, group)` | `F.regexp_extract(F.col("col"), pattern, group)` |
| `RegEx_Replace(col, pattern, repl)` | `REGEXP_REPLACE(col, pattern, repl)` | `F.regexp_replace(F.col("col"), pattern, repl)` |
| `RegEx_CountAll(col, pattern)` | `LENGTH(col) - LENGTH(REGEXP_REPLACE(col, pattern, ''))` | Manual regex count — see note below |

**Note on regex:** Alteryx uses VB.NET regex; Spark uses Java regex. Patterns must be converted (e.g., `\.` for literal dot is same, but lookahead `(?=...)` syntax is supported).

### Phonetic Functions

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `Soundex(col)` | `SOUNDEX(col)` | `F.soundex(F.col("col"))` |
| `SoundexMatch(col1, col2)` | `SOUNDEX(col1) = SOUNDEX(col2)` | `F.soundex(F.col("col1")) == F.soundex(F.col("col2"))` |
| `Levenshtein(col1, col2)` | `LEVENSHTEIN(col1, col2)` | `F.levenshtein(F.col("col1"), F.col("col2"))` |

---

## 4. Numeric Functions

### Basic Arithmetic

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `ABS(col)` | `ABS(col)` | `F.abs(F.col("col"))` |
| `ROUND(col, decimals)` | `ROUND(col, decimals)` | `F.round(F.col("col"), decimals)` |
| `CEIL(col)` | `CEIL(col)` | `F.ceil(F.col("col"))` |
| `FLOOR(col)` | `FLOOR(col)` | `F.floor(F.col("col"))` |
| `SQRT(col)` | `SQRT(col)` | `F.sqrt(F.col("col"))` |
| `POWER(col, exp)` | `POWER(col, exp)` | `F.pow(F.col("col"), exp)` |
| `LOG(col)` | `LOG(col)` | `F.log(F.col("col"))` |
| `LOG10(col)` | `LOG10(col)` | `F.log10(F.col("col"))` |
| `EXP(col)` | `EXP(col)` | `F.exp(F.col("col"))` |

### Min / Max

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `Max(col1, col2, ...)` | `GREATEST(col1, col2, ...)` | `F.greatest(F.col("col1"), F.col("col2"))` |
| `Min(col1, col2, ...)` | `LEAST(col1, col2, ...)` | `F.least(F.col("col1"), F.col("col2"))` |

### Type Casting

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `Int(col)` | `CAST(col AS INT)` | `F.col("col").cast("int")` |
| `Double(col)` | `CAST(col AS DOUBLE)` | `F.col("col").cast("double")` |
| `String(col)` | `CAST(col AS STRING)` | `F.col("col").cast("string")` |
| `Bool(col)` | `CAST(col AS BOOLEAN)` | `F.col("col").cast("boolean")` |
| `TryInt(col)` | `TRY_CAST(col AS INT)` | `F.try_cast(F.col("col"), "int")` |
| `TryDouble(col)` | `TRY_CAST(col AS DOUBLE)` | `F.try_cast(F.col("col"), "double")` |

---

## 5. Date & Time Functions

### Date Construction & Extraction

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `DateTimeNow()` | `CURRENT_TIMESTAMP()` | `F.current_timestamp()` |
| `DateNow()` | `CURRENT_DATE()` | `F.current_date()` |
| `CreateDate(year, month, day)` | `MAKE_DATE(year, month, day)` | `F.make_date(F.lit(year), F.lit(month), F.lit(day))` |
| `Year(date_col)` | `YEAR(date_col)` | `F.year(F.col("date_col"))` |
| `Month(date_col)` | `MONTH(date_col)` | `F.month(F.col("date_col"))` |
| `Day(date_col)` | `DAY(date_col)` | `F.dayofmonth(F.col("date_col"))` |
| `DayOfWeek(date_col)` | `DAYOFWEEK(date_col)` (1=Sunday) | `F.dayofweek(F.col("date_col"))` |
| `DayOfYear(date_col)` | `DAYOFYEAR(date_col)` | `F.dayofyear(F.col("date_col"))` |
| `Quarter(date_col)` | `QUARTER(date_col)` | `F.quarter(F.col("date_col"))` |
| `Hour(timestamp_col)` | `HOUR(timestamp_col)` | `F.hour(F.col("timestamp_col"))` |
| `Minute(timestamp_col)` | `MINUTE(timestamp_col)` | `F.minute(F.col("timestamp_col"))` |
| `Second(timestamp_col)` | `SECOND(timestamp_col)` | `F.second(F.col("timestamp_col"))` |

### Date Arithmetic

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `DateAdd(date_col, num_days)` | `DATE_ADD(date_col, num_days)` | `F.date_add(F.col("date_col"), num_days)` |
| `DateDiff(end_date, start_date)` | `DATEDIFF(end_date, start_date)` or `DAYS(end_date) - DAYS(start_date)` | `F.datediff(F.col("end_date"), F.col("start_date"))` |
| `DateTimeAdd(ts, seconds)` | Not direct; use DATE_ADD or TIMESTAMP arithmetic | Add seconds: `F.col("ts") + F.expr("INTERVAL x SECOND")` |
| `DateTimeDiff(end_ts, start_ts)` | `(UNIX_TIMESTAMP(end_ts) - UNIX_TIMESTAMP(start_ts))` (in seconds) | `F.unix_timestamp(F.col("end_ts")) - F.unix_timestamp(F.col("start_ts"))` |
| `MonthsBetween(end_date, start_date)` | `MONTHS_BETWEEN(end_date, start_date)` | `F.months_between(F.col("end_date"), F.col("start_date"))` |

### Date Parsing & Formatting

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `DateTimeParse(str, format)` | `TO_TIMESTAMP(str, format)` | `F.to_timestamp(F.col("str"), format)` |
| `DateParse(str, format)` | `TO_DATE(str, format)` | `F.to_date(F.col("str"), format)` |
| `ToString(date_col, format)` | `DATE_FORMAT(date_col, format)` | `F.date_format(F.col("date_col"), format)` |
| `ToDate(timestamp_col)` | `CAST(timestamp_col AS DATE)` | `F.col("timestamp_col").cast("date")` |
| `ToTimestamp(date_col)` | `CAST(date_col AS TIMESTAMP)` | `F.col("date_col").cast("timestamp")` |

**Common date formats:**
- `"yyyy-MM-dd"` — ISO date
- `"MM/dd/yyyy"` — US date
- `"dd/MM/yyyy"` — EU date
- `"yyyy-MM-dd HH:mm:ss"` — Timestamp

**Example:**
```sql
-- Parse Alteryx formula to Spark SQL
DateTimeParse(date_string, "%Y-%m-%d %H:%M:%S")
→ TO_TIMESTAMP(date_string, "yyyy-MM-dd HH:mm:ss")

-- PySpark
F.to_timestamp(F.col("date_string"), "yyyy-MM-dd HH:mm:ss")
```

---

## 6. Aggregate Functions (Multi-Row Formula Context)

These appear in Multi-Row Formula or window functions. Use in conjunction with `.over(Window.partitionBy(...).orderBy(...))`

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `SUM(col)` | `SUM(col) OVER (...)` | `F.sum(F.col("col")).over(window)` |
| `AVG(col)` | `AVG(col) OVER (...)` | `F.avg(F.col("col")).over(window)` |
| `COUNT(col)` | `COUNT(col) OVER (...)` | `F.count(F.col("col")).over(window)` |
| `MIN(col)` | `MIN(col) OVER (...)` | `F.min(F.col("col")).over(window)` |
| `MAX(col)` | `MAX(col) OVER (...)` | `F.max(F.col("col")).over(window)` |
| `FIRST(col)` | `FIRST_VALUE(col) OVER (...)` | `F.first(F.col("col")).over(window)` |
| `LAST(col)` | `LAST_VALUE(col) OVER (...)` | `F.last(F.col("col")).over(window)` |
| `LAG(col, offset, default)` | `LAG(col, offset, default) OVER (...)` | `F.lag(F.col("col"), offset).over(window)` |
| `LEAD(col, offset, default)` | `LEAD(col, offset, default) OVER (...)` | `F.lead(F.col("col"), offset).over(window)` |
| `ROW_NUMBER()` | `ROW_NUMBER() OVER (ORDER BY ...)` | `F.row_number().over(window)` |
| `RANK()` | `RANK() OVER (ORDER BY ...)` | `F.rank().over(window)` |
| `DENSE_RANK()` | `DENSE_RANK() OVER (ORDER BY ...)` | `F.dense_rank().over(window)` |
| `NTILE(n)` | `NTILE(n) OVER (ORDER BY ...)` | `F.ntile(n).over(window)` |

**PySpark Window Example:**
```python
from pyspark.sql import Window

w = Window.partitionBy("region").orderBy("date")
df_windowed = df.withColumn(
    "prev_value", F.lag("value").over(w)
).withColumn(
    "running_sum", F.sum("value").over(w)
)
```

---

## 7. Type Conversion & Literals

| Alteryx | Spark SQL | PySpark |
|---|---|---|
| `5` (integer literal) | `5` or `5 AS col_name` | `F.lit(5)` |
| `5.0` (float literal) | `5.0 AS col_name` | `F.lit(5.0)` |
| `'string'` (string literal) | `'string' AS col_name` | `F.lit("string")` |
| `TRUE` / `FALSE` (boolean) | `true` / `false` | `F.lit(True)` / `F.lit(False)` |
| Null value | `NULL` | `None` (Python) or `F.lit(None)` |

---

## 8. Advanced / Rare Functions

| Alteryx | Spark SQL | PySpark | Notes |
|---|---|---|---|
| `MD5(col)` | `MD5(col)` | `F.md5(F.col("col"))` | Hash function — rarely used in transforms |
| `SHA1(col)` | `SHA1(col)` | `F.sha1(F.col("col"))` | Hash function |
| `SHA2(col, bits)` | `SHA2(col, bits)` | `F.sha2(F.col("col"), bits)` | Hash function (bits: 256, 512) |
| `CRC32(col)` | `CRC32(col)` | Not directly; use UDF | Checksum |
| `RandomInt(min, max)` | `FLOOR(RAND() * (max - min + 1)) + min` | `F.rand() * (max - min + 1) + min` | Not deterministic — use for testing |
| `Bin(num)` | `BIN(num)` | Not standard; use Python `bin()` in UDF | Binary representation |
| `Hex(num)` | `HEX(num)` | Not standard; use Python `hex()` in UDF | Hex representation |

---

## 9. Operator Precedence & Parentheses

Both Alteryx and Spark follow standard operator precedence:
1. **Parentheses** `()`
2. **Exponentiation** `^` or `POWER()`
3. **Multiplication, Division** `*`, `/`
4. **Addition, Subtraction** `+`, `-`
5. **Comparison** `=`, `!=`, `<`, `>`, `<=`, `>=`
6. **NOT** `NOT`
7. **AND** `AND`
8. **OR** `OR`

**Always use parentheses for clarity** when combining conditions:
```sql
-- Clear
CASE WHEN (region = 'US' AND revenue > 10000) 
           OR (region = 'EU' AND revenue > 5000) THEN 'High'
```

---

## 10. Common Migration Patterns

### Pattern 1: Conditional Revenue Adjustment
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
 .otherwise(F.col("revenue")) \
 .alias("adjusted_revenue")
```

### Pattern 2: Running Total with Window
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

### Pattern 3: Rank Within Group
**Alteryx Multi-Row Formula:**
```
RANK() over (PARTITION BY category ORDER BY sales DESC)
```

**Spark SQL:**
```sql
DENSE_RANK() OVER (PARTITION BY category ORDER BY sales DESC) AS rank
```

**PySpark:**
```python
w = Window.partitionBy("category").orderBy(F.col("sales").desc())
df.withColumn("rank", F.dense_rank().over(w))
```

### Pattern 4: String Parsing & Extraction
**Alteryx:**
```
REGEX_Match(email, "^[^@]+@([^\.]+)") → capture domain prefix
```

**Spark SQL:**
```sql
REGEXP_EXTRACT(email, '^[^@]+@([^\.]+)', 1) AS domain_prefix
```

**PySpark:**
```python
F.regexp_extract(F.col("email"), r"^[^@]+@([^\.]+)", 1).alias("domain_prefix")
```

### Pattern 5: Date Arithmetic
**Alteryx:**
```
DateAdd(order_date, 30)  → 30 days later
DateDiff(delivery_date, order_date)  → number of days
```

**Spark SQL:**
```sql
DATE_ADD(order_date, 30) AS due_date,
DATEDIFF(delivery_date, order_date) AS delivery_days
```

**PySpark:**
```python
df.withColumn("due_date", F.date_add(F.col("order_date"), 30)) \
  .withColumn("delivery_days", F.datediff(F.col("delivery_date"), F.col("order_date")))
```

---

## 11. Known Differences & Gotchas

| Issue | Alteryx | Spark / PySpark | Mitigation |
|---|---|---|---|
| **Regex flavor** | VB.NET regex | Java regex | Patterns usually compatible; test on sample data |
| **Division by zero** | Returns error or NULL | Returns `NaN` or `Infinity` (depends on data type) | Wrap with `CASE WHEN divisor != 0` |
| **String + Number** | Auto-coerces | Fails — explicit `CAST()` or `CONCAT_WS()` required | Always cast explicitly |
| **NULL propagation** | Most functions return NULL if any input is NULL (3-valued logic) | Same | Use `COALESCE()` or `CASE` for safe handling |
| **Floating-point precision** | Alteryx: FixedDecimal(19,6) | Spark: IEEE 754 double | Round to same precision for comparison; tolerance ~0.1% |
| **Date format strings** | Alteryx: `%Y-%m-%d` (Python strftime) | Spark: `yyyy-MM-dd` (SimpleDateFormat) | Rewrite date format strings during conversion |
| **Empty string vs NULL** | Alteryx treats differently | Spark: usually treated as NULL or distinguished per function | Be explicit about empty string handling |
| **LIMIT without ORDER BY** | Non-deterministic | Non-deterministic | Always add `.orderBy()` before `.limit()` if order matters |

---

## References

- **Alteryx Formula Language:** [Alteryx Designer documentation](https://help.alteryx.com/2023.2/designer/alteryx-designer-reference-guide) (Formula tool reference)
- **Spark SQL Functions:** [Apache Spark SQL Language Manual](https://spark.apache.org/docs/latest/sql-ref-functions.html)
- **PySpark Functions:** [pyspark.sql.functions API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.functions.html)
- **Databricks Date/Time Functions:** [Date and time functions](https://docs.databricks.com/sql/language-manual/functions/functions-by-category.html#date-and-time-functions)
- **Window Functions:** [PySpark Window Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
