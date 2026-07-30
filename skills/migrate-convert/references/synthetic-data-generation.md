# Synthetic Data Generation for Alteryx Validation

**Purpose:** When source CSV files are unavailable during migration validation, generate realistic synthetic data from the Alteryx workflow schema, enable converted pipelines to run end-to-end, and verify correctness without blocking on missing assets.

**Scope:** Extract input table schemas from `.yxmd` DbFileInput nodes → infer types and relationships → generate faithful dimension/fact data with referential integrity → write CSVs to UC Volumes for direct pipeline ingest.

**Status:** Reference design — code sketch, not production-ready; use for framework validation workflows.

---

## 1. Schema Inference from .yxmd

Alteryx workflows declare schema in two places: **explicit** (in DbFileInput `<MetaInfo>` blocks) and **inferred** (from downstream tool expressions and connections). For validation-mode data generation, prioritize explicit schema first; infer missing types from references.

### 1.1 Reading Explicit Schemas

DbFileInput nodes embed **complete schema metadata** in `<MetaInfo>` blocks:

```xml
<Node ToolID="1">
  <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput">
    ...
  </GuiSettings>
  <Properties>
    <Configuration>
      <File path="\\data\transactions.csv" />
      ...
    </Configuration>
    <MetaInfo connection="Output">
      <RecordInfo>
        <Field name="transaction_id" type="Int32" />
        <Field name="store_id" type="Int32" />
        <Field name="product_id" type="Int32" />
        <Field name="date" type="String" />
        <Field name="qty" type="Int32" />
        <Field name="unit_price" type="Double" />
      </RecordInfo>
    </MetaInfo>
  </Properties>
</Node>
```

**Extraction algorithm:**

1. Parse `.yxmd` as XML.
2. Find all `<Node>` elements with `Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput"` (input nodes).
3. For each input, extract:
   - **File path** from `<File path="...">` (may be relative or Windows network path; extract the filename).
   - **Fields** from `<RecordInfo><Field>` elements (name, type, size if present).
4. Map Alteryx types to Python/Spark types:
   - `Int32`, `Int16`, `Int64` → `int`
   - `Double`, `Float` → `float`
   - `String`, `WString` → `str` (default length 50)
   - `Bool` → `bool`
   - `Date`, `DateTime` → `str` (ISO 8601 format initially; you'll parse to date in Transform).

### 1.2 Inferring Column Semantics from Downstream References

When a field's type is ambiguous or missing, scan **downstream tool nodes** for clues:

| Tool | Expression Pattern | Inference |
|------|-------------------|-----------|
| **Filter** | `[date] >= DateTimeParse("2024-01-01", "%Y-%m-%d")` | `date` is a string parseable as date; retain date range (2024-01-01 to 2024-12-31 for generation). |
| **Filter** | `[qty] > 0` | `qty` is numeric, positive. |
| **Formula** | `[qty] * [unit_price]` | Both are numeric; multiplication produces numeric output. |
| **Formula** | `DateTimeYear([date])`, `DateTimeMonth([date])` | `date` is a date/datetime string; generation should yield valid dates in range. |
| **Join** | `ON [transactions.product_id] = [products.product_id]` | Both are keys; must match semantically (both IDs, same range or population). |
| **Summarize** | `SUM([net_sales])` | `net_sales` is numeric; can be aggregated. |

**Practical extraction:**

```python
import xml.etree.ElementTree as ET
import re

def extract_schema_and_hints(yxmd_path):
    """Parse .yxmd and return {table_name: [fields], joins, filters}"""
    tree = ET.parse(yxmd_path)
    root = tree.getroot()
    
    schema = {}
    joins = []
    filters = []
    
    # Find all DbFileInput nodes
    for node in root.findall('.//Node'):
        gui = node.find('.//GuiSettings')
        if gui is None or 'DbFileInput' not in gui.get('Plugin', ''):
            continue
        
        # Extract filename
        file_elem = node.find('.//File')
        if file_elem is not None:
            file_path = file_elem.get('path', '').split('\\')[-1]  # Extract just filename
            table_name = file_path.replace('.csv', '')
        else:
            continue
        
        # Extract fields
        fields = []
        for field in node.findall('.//Field'):
            fields.append({
                'name': field.get('name'),
                'type': field.get('type'),
                'size': field.get('size'),
            })
        schema[table_name] = fields
    
    # Scan downstream nodes for hints (Join, Filter, Formula)
    for node in root.findall('.//Node'):
        gui = node.find('.//GuiSettings')
        if gui is None:
            continue
        
        plugin = gui.get('Plugin', '')
        
        # Extract join keys
        if 'Join' in plugin:
            join_infos = node.findall('.//JoinInfo')
            if len(join_infos) >= 2:
                left_field = join_infos[0].find('.//Field')
                right_field = join_infos[1].find('.//Field')
                if left_field is not None and right_field is not None:
                    joins.append({
                        'left_field': left_field.get('field'),
                        'right_field': right_field.get('field'),
                    })
        
        # Extract filter constraints
        if 'Filter' in plugin:
            expr_elem = node.find('.//Expression')
            if expr_elem is not None:
                expr = expr_elem.text or ''
                # Heuristic: date ranges, qty > 0, etc.
                if 'DateTimeParse' in expr:
                    dates = re.findall(r'DateTimeParse\("(\d{4}-\d{2}-\d{2})"', expr)
                    filters.append({'type': 'date_range', 'values': dates})
                if '[qty] > 0' in expr:
                    filters.append({'type': 'qty_positive'})
    
    return schema, joins, filters
```

---

## 2. Referential Integrity — Key Population Strategy

**Core principle:** Generate dimensions first, then facts. Draw foreign keys from the dimension key sets.

For the sample workflow:
- **Dimensions:** `products` (product_id), `stores` (store_id)
- **Facts:** `transactions` (product_id FK, store_id FK)

### 2.1 Algorithm

1. **Scan joins** to identify which tables are "left" (fact) and which are "right" (dimension).
   - Left Outer Join with `transactions` on the left → `transactions` is fact, `products`/`stores` are dimensions.
   - Multiple left joins → one fact, N dimensions.

2. **Generate dimensions independently:**
   - `products`: 20 rows, product_id ∈ [1, 20], names like "Product_A", "Product_B", etc., categories from a fixed list (["Electronics", "Apparel", "Home", "Food"], cycle).
   - `stores`: 5 rows, store_id ∈ [1, 5], names like "Store_NY", "Store_LA", regions from ["North", "South", "East", "West", "Central"].

3. **Generate facts referencing dimensions:**
   - `transactions`: 200 rows.
   - For each row: pick random product_id from [1, 20], random store_id from [1, 5], random date in the filter range (2024-01-01 to 2024-12-31), qty ∈ [1, 100] (positive, satisfies filter qty > 0).

**Why this works:**
- Every join `transactions.product_id = products.product_id` matches.
- Every join `transactions.store_id = stores.store_id` matches.
- Filters like `qty > 0` pass because all generated qty ≥ 1.
- Date filters pass because generated dates are within [2024-01-01, 2024-12-31].
- Aggregations and window functions see realistic fact distributions (multiple products per store, multiple transactions per product).

### 2.2 Implementation Sketch

```python
import random
from datetime import datetime, timedelta

def generate_dimensions(schema, joins):
    """Generate dimension table data."""
    data = {}
    
    # Identify key fields from joins (right side of left outer joins)
    dimension_keys = set()
    for j in joins:
        dimension_keys.add(j['right_field'])
    
    for table_name, fields in schema.items():
        if table_name in ['products', 'stores']:  # Known dimensions for sample
            rows = []
            key_field = None
            
            # Find the primary key (id field)
            for f in fields:
                if 'id' in f['name'] and f['type'] in ['Int32', 'Int64']:
                    key_field = f['name']
                    break
            
            if key_field is None:
                continue
            
            # Generate dimension rows
            n_rows = 20 if table_name == 'products' else 5
            for i in range(1, n_rows + 1):
                row = {}
                for f in fields:
                    if f['name'] == key_field:
                        row[f['name']] = i
                    elif 'name' in f['name']:
                        row[f['name']] = f"{table_name.rstrip('s')}_{i}"
                    elif 'category' in f['name']:
                        row[f['name']] = random.choice(['Electronics', 'Apparel', 'Home', 'Food'])
                    elif 'region' in f['name']:
                        row[f['name']] = random.choice(['North', 'South', 'East', 'West', 'Central'])
                    elif f['type'] in ['Int32', 'Int64']:
                        row[f['name']] = random.randint(100, 999)
                    elif f['type'] == 'Double':
                        row[f['name']] = round(random.uniform(10, 100), 2)
                    else:
                        row[f['name']] = None
                rows.append(row)
            data[table_name] = rows
    
    return data

def generate_facts(schema, dimension_data, joins, date_range=None):
    """Generate fact table data with FK references."""
    if date_range is None:
        date_range = ['2024-01-01', '2024-12-31']
    
    start_date = datetime.strptime(date_range[0], '%Y-%m-%d')
    end_date = datetime.strptime(date_range[1], '%Y-%m-%d')
    
    data = {}
    
    for table_name, fields in schema.items():
        if table_name in ['transactions']:  # Known fact table for sample
            rows = []
            n_rows = 200
            
            # Build FK lookup maps
            fk_maps = {}
            for j in joins:
                if 'transactions' in j.get('left_field', ''):
                    # Extract dimension table and key
                    fk_maps[j['left_field']] = [
                        dim[j['right_field']] 
                        for dim in dimension_data.get(j['right_field'].split('_')[0] + 's', [])
                    ]
            
            for i in range(1, n_rows + 1):
                row = {}
                for f in fields:
                    if 'id' in f['name'] and 'product_id' not in f['name'] and 'store_id' not in f['name']:
                        # Primary key
                        row[f['name']] = i
                    elif f['name'] == 'product_id':
                        # FK: pick from products
                        row[f['name']] = random.randint(1, 20)
                    elif f['name'] == 'store_id':
                        # FK: pick from stores
                        row[f['name']] = random.randint(1, 5)
                    elif f['name'] == 'date':
                        # Generate date in range
                        days_delta = (end_date - start_date).days
                        random_date = start_date + timedelta(days=random.randint(0, days_delta))
                        row[f['name']] = random_date.strftime('%Y-%m-%d')
                    elif f['name'] == 'qty':
                        # Positive quantity (satisfies qty > 0 filter)
                        row[f['name']] = random.randint(1, 100)
                    elif f['type'] == 'Double':
                        # Unit price, cost price, etc.
                        row[f['name']] = round(random.uniform(10, 500), 2)
                    elif f['type'] in ['Int32', 'Int64']:
                        row[f['name']] = random.randint(1, 999)
                    else:
                        row[f['name']] = None
                rows.append(row)
            data[table_name] = rows
    
    return data
```

---

## 3. Realistic Values — Semantic Constraints

**Principle:** Generate data that satisfies known Alteryx filter/formula constraints **without hardcoding them**. Infer from the workflow instead.

### 3.1 Constraint Types

| Constraint | Source | Example | Generation Rule |
|-----------|--------|---------|-----------------|
| **Date range** | Filter: `[date] >= "2024-01-01" AND [date] <= "2024-12-31"` | Transactions between 2024 Q1–Q4 | Sample uniform random from the range; extract bounds via regex on Filter expressions. |
| **Positive numeric** | Filter: `[qty] > 0` | Quantity must be positive | Generate `random.randint(1, max_qty)` instead of allowing 0 or negatives. |
| **Numeric bounds** | Formula: `[extended_price] - [discount_amount]` (revenue computation) | Extended price = qty × unit_price; discount = 10%; net = extended − discount. | Calculate downstream formulas during generation; ensure arithmetic is valid (no NaN/Inf). |
| **String enumeration** | Summarize GROUP BY: `[category]` | Categories inferred from product master | Hardcode a small set for each string field; vary per row. |
| **Foreign key match** | Join: `[transactions.product_id] = [products.product_id]` | Every transaction references a known product | Draw FK values from the dimension's key set post-generation. |
| **Null constraints** | Filter: `[customer_id] IS NOT NULL` or explicit null-check | Dropped rows with null customer_id | Generate no NULLs in key fields; sparse NULLs (≤5%) in optional fields. |

### 3.2 Extraction from Filter Expressions

```python
import re

def extract_constraints(filter_expressions):
    """Parse Alteryx Filter expressions to infer generation rules."""
    constraints = {
        'date_ranges': [],
        'positive_fields': [],
        'bounds': {},
    }
    
    for expr in filter_expressions:
        # Date ranges: DateTimeParse("YYYY-MM-DD", ...)
        dates = re.findall(r'DateTimeParse\("(\d{4}-\d{2}-\d{2})"', expr)
        if len(dates) >= 2:
            constraints['date_ranges'].append({
                'start': dates[0],
                'end': dates[1],
            })
        elif len(dates) == 1:
            constraints['date_ranges'].append({
                'start': dates[0],
                'end': dates[0],  # Single date boundary
            })
        
        # Positive numeric: [field] > 0
        pos_fields = re.findall(r'\[([a-zA-Z_]+)\]\s*>\s*0', expr)
        constraints['positive_fields'].extend(pos_fields)
        
        # Bounds: [field] > N or [field] < N
        bounds_patterns = re.findall(r'\[([a-zA-Z_]+)\]\s*([<>=]+)\s*(\d+)', expr)
        for field, op, val in bounds_patterns:
            if field not in constraints['bounds']:
                constraints['bounds'][field] = []
            constraints['bounds'][field].append({'op': op, 'val': int(val)})
    
    return constraints
```

### 3.3 Integration into Generation

```python
def generate_fact_row(fields, constraints, dimension_fks):
    """Generate one fact row respecting constraints."""
    row = {}
    
    for f in fields:
        fname = f['name']
        ftype = f['type']
        
        # Apply constraints
        if fname in constraints['positive_fields'] and ftype in ['Int32', 'Int64']:
            row[fname] = random.randint(1, 100)
        elif fname in [d['field'] for d in dimension_fks]:
            # FK: pick from dimension
            row[fname] = random.choice(dimension_fks[[d['field'] for d in dimension_fks].index(fname)]['values'])
        elif fname == 'date' and constraints['date_ranges']:
            # Date field: use inferred range
            dr = constraints['date_ranges'][0]
            start = datetime.strptime(dr['start'], '%Y-%m-%d')
            end = datetime.strptime(dr['end'], '%Y-%m-%d')
            delta = (end - start).days
            rd = start + timedelta(days=random.randint(0, delta))
            row[fname] = rd.strftime('%Y-%m-%d')
        elif ftype == 'Double':
            row[fname] = round(random.uniform(1, 1000), 2)
        elif ftype in ['Int32', 'Int64']:
            row[fname] = random.randint(0, 999)
        else:
            row[fname] = None
    
    return row
```

---

## 4. Volume & Metadata — Audit Trail

### 4.1 Directory Structure

Generated CSVs are written to a **UC Volume** under a **synthetic data marker directory**:

```
catalog: migration_factory
  schema: <client>
    VOLUME raw/
      alteryx/
        <object_slug>/
          synthetic/                    ← Marker: this is generated, not real
            _metadata.json              ← Manifest (audit trail)
            transactions.csv
            products.csv
            stores.csv
```

The `synthetic/` subdirectory signals that this data was generated for validation only.

### 4.2 Metadata File

Record **generation provenance, schema, and constraints** for traceability:

```json
{
  "generated_at": "2026-07-30T15:45:23Z",
  "source_yxmd": "sample_sales_analytics_complex.yxmd",
  "source_object_id": "alteryx_sample_sales_analytics_complex_v1",
  "generation_mode": "synthetic_validation",
  "tables": {
    "transactions": {
      "rows": 200,
      "fields": [
        {"name": "transaction_id", "type": "Int32", "role": "key"},
        {"name": "product_id", "type": "Int32", "role": "fk", "references": "products.product_id"},
        {"name": "store_id", "type": "Int32", "role": "fk", "references": "stores.store_id"},
        {"name": "date", "type": "String", "role": "fact", "constraint": "2024-01-01 <= date <= 2024-12-31"},
        {"name": "qty", "type": "Int32", "role": "fact", "constraint": "qty > 0"},
        {"name": "unit_price", "type": "Double", "role": "fact"}
      ]
    },
    "products": {
      "rows": 20,
      "fields": [
        {"name": "product_id", "type": "Int32", "role": "key"},
        {"name": "product_name", "type": "String", "role": "dim"},
        {"name": "category", "type": "String", "role": "dim", "values": ["Electronics", "Apparel", "Home", "Food"]}
      ]
    },
    "stores": {
      "rows": 5,
      "fields": [
        {"name": "store_id", "type": "Int32", "role": "key"},
        {"name": "store_name", "type": "String", "role": "dim"},
        {"name": "region", "type": "String", "role": "dim", "values": ["North", "South", "East", "West", "Central"]}
      ]
    }
  },
  "constraints": {
    "joins": [
      {"left": "transactions.product_id", "right": "products.product_id"},
      {"left": "transactions.store_id", "right": "stores.store_id"}
    ],
    "filters": [
      "date >= 2024-01-01 AND date <= 2024-12-31",
      "qty > 0"
    ]
  },
  "status": "SYNTHETIC - NOT REAL DATA",
  "notes": "Validation dataset. Must re-validate with real data before productionization."
}
```

### 4.3 Writing to Volume

```python
import json
import csv
from datetime import datetime

def write_synthetic_tables_to_volume(
    schema, 
    dimension_data, 
    fact_data, 
    volume_path, 
    source_yxmd_name,
    constraints
):
    """Write synthetic CSVs to UC Volume with metadata."""
    
    # Create synthetic/ subdirectory
    synthetic_dir = f"{volume_path}/synthetic"
    dbutils.fs.mkdirs(synthetic_dir)
    
    # Write each table as CSV
    all_data = {**dimension_data, **fact_data}
    
    for table_name, rows in all_data.items():
        if not rows:
            continue
        
        # Prepare CSV output
        csv_path = f"{synthetic_dir}/{table_name}.csv"
        fieldnames = list(rows[0].keys())
        
        # Write to temp location, then move to Volume
        temp_csv = f"/tmp/{table_name}.csv"
        with open(temp_csv, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        # Copy to Volume
        dbutils.fs.cp(f"file://{temp_csv}", csv_path)
    
    # Write metadata
    metadata = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "source_yxmd": source_yxmd_name,
        "generation_mode": "synthetic_validation",
        "tables": {
            table_name: {
                "rows": len(rows),
                "fields": [
                    {
                        "name": fname,
                        "type": schema.get(table_name, [{}])[0].get("type", "String")
                    }
                    for fname in rows[0].keys()
                ]
            }
            for table_name, rows in all_data.items()
        },
        "constraints": constraints,
        "status": "SYNTHETIC - NOT REAL DATA",
        "notes": "Validation dataset. Must re-validate with real data before productionization."
    }
    
    metadata_path = f"{synthetic_dir}/_metadata.json"
    metadata_json = json.dumps(metadata, indent=2)
    dbutils.fs.put(metadata_path, metadata_json, overwrite=True)
    
    print(f"✓ Synthetic data written to {synthetic_dir}")
    print(f"  Tables: {', '.join(all_data.keys())}")
    print(f"  Metadata: {metadata_path}")
```

---

## 5. Reuse vs. Build — Tool Selection

### 5.1 Option A: Reuse `databricks-data-generation` Skill

**Pros:**
- Mature, tested on multiple ML workloads.
- Integrates Polars + Mimesis (realistic names, emails, addresses).
- Handles cardinality, distributions, seeding.

**Cons:**
- Designed for **unbounded synthetic ML datasets** (millions of rows, multi-table relationships via seed).
- Overkill for migration validation (small, fixed-size, FK-only).
- Configuration surface (Mimesis providers, cardinality maps) adds friction for a one-off per workflow.
- Heavyweight import (Polars, Mimesis) for a simple generator.

**When to use:** If your migration estate is large (100+ workflows) and you want consistency + robustness → factor out to a shared skill.

### 5.2 Option B: Lightweight Self-Contained Generator

**Pros:**
- Minimal dependencies (Python stdlib + `pandas` or raw CSV).
- Clear, inspection-friendly code per workflow.
- Easy to tweak constraints inline during migration.
- Fast: no Polars/Mimesis startup overhead.

**Cons:**
- One-off per workflow (copy-paste the generator).
- Less expressive (no Mimesis-style realistic names).
- Requires manual constraint extraction from .yxmd per workflow.

**Recommendation:** Use **Option B** for Phase 1 (migration framework launch + sample workflows). Migrate to Option A if:
- Migration estate grows >50 workflows.
- You need consistent faker data across multiple customers.
- Dimension cardinality becomes complex (e.g., city→country hierarchies).

---

## 6. Reference Implementation

Self-contained generator for the sample `sample_sales_analytics_complex` workflow. Copy into your conversion notebook or helper module.

### 6.1 Full Script

```python
"""
Synthetic Data Generator for Alteryx Workflow Validation
Target: sample_sales_analytics_complex.yxmd
Generated: 2026-07-30
"""

import csv
import json
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any

class SyntheticDataGenerator:
    """Generate referentially-sound synthetic dimension + fact tables for Alteryx workflow validation."""
    
    def __init__(self, seed: int = 42):
        random.seed(seed)
    
    def generate_products(self, n_products: int = 20) -> List[Dict]:
        """Generate product master dimension."""
        categories = ["Electronics", "Apparel", "Home", "Food"]
        rows = []
        for i in range(1, n_products + 1):
            rows.append({
                "product_id": i,
                "product_name": f"Product_{chr(65 + (i - 1) % 26)}{(i - 1) // 26}",
                "category": categories[(i - 1) % len(categories)],
                "supplier_id": random.randint(1000, 1999),
                "cost_price": round(random.uniform(5, 200), 2),
            })
        return rows
    
    def generate_stores(self, n_stores: int = 5) -> List[Dict]:
        """Generate store master dimension."""
        regions = ["North", "South", "East", "West", "Central"]
        store_types = ["Flagship", "Regional", "Outlet"]
        rows = []
        for i in range(1, n_stores + 1):
            rows.append({
                "store_id": i,
                "store_name": f"Store_{chr(65 + (i - 1) % 26)}{(i - 1) // 26}",
                "region": regions[(i - 1) % len(regions)],
                "manager_id": 1000 + i,
                "store_type": store_types[(i - 1) % len(store_types)],
            })
        return rows
    
    def generate_transactions(
        self, 
        n_transactions: int = 200, 
        product_ids: List[int] = None,
        store_ids: List[int] = None,
        date_start: str = "2024-01-01",
        date_end: str = "2024-12-31"
    ) -> List[Dict]:
        """Generate transaction fact table with referential integrity."""
        
        if product_ids is None:
            product_ids = list(range(1, 21))
        if store_ids is None:
            store_ids = list(range(1, 6))
        
        start_date = datetime.strptime(date_start, "%Y-%m-%d")
        end_date = datetime.strptime(date_end, "%Y-%m-%d")
        date_delta = (end_date - start_date).days
        
        rows = []
        for i in range(1, n_transactions + 1):
            # Random date in range
            random_date = start_date + timedelta(days=random.randint(0, date_delta))
            
            qty = random.randint(1, 100)  # Positive quantity (satisfies qty > 0 filter)
            unit_price = round(random.uniform(10, 500), 2)
            extended_price = qty * unit_price
            discount_amount = extended_price * 0.10
            net_sales = extended_price - discount_amount
            
            rows.append({
                "transaction_id": i,
                "store_id": random.choice(store_ids),
                "product_id": random.choice(product_ids),
                "date": random_date.strftime("%Y-%m-%d"),
                "qty": qty,
                "unit_price": unit_price,
            })
        
        return rows
    
    def generate_all(self) -> Dict[str, List[Dict]]:
        """Generate all tables."""
        products = self.generate_products()
        stores = self.generate_stores()
        product_ids = [p["product_id"] for p in products]
        store_ids = [s["store_id"] for s in stores]
        transactions = self.generate_transactions(
            product_ids=product_ids,
            store_ids=store_ids,
        )
        
        return {
            "products": products,
            "stores": stores,
            "transactions": transactions,
        }
    
    def write_to_csvs(self, output_dir: str):
        """Write tables to CSVs and metadata."""
        data = self.generate_all()
        
        # Ensure output directory exists
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        # Write CSVs
        for table_name, rows in data.items():
            if not rows:
                continue
            csv_path = f"{output_dir}/{table_name}.csv"
            fieldnames = list(rows[0].keys())
            
            with open(csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            
            print(f"  ✓ {csv_path} ({len(rows)} rows)")
        
        # Write metadata
        metadata = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source_workflow": "sample_sales_analytics_complex.yxmd",
            "generation_mode": "synthetic_validation",
            "tables": {
                table_name: {"rows": len(rows), "fields": list(rows[0].keys())}
                for table_name, rows in data.items()
            },
            "constraints": {
                "date_range": ["2024-01-01", "2024-12-31"],
                "qty_positive": True,
                "referential_integrity": [
                    "transactions.product_id → products.product_id",
                    "transactions.store_id → stores.store_id"
                ]
            },
            "status": "SYNTHETIC - NOT REAL DATA",
            "notes": "Validation dataset. Verify against real data before production use."
        }
        
        metadata_path = f"{output_dir}/_metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
        
        print(f"  ✓ {metadata_path}")
        print(f"✓ Synthetic data generation complete.")


# Usage in Databricks notebook or helper module
if __name__ == "__main__":
    gen = SyntheticDataGenerator(seed=42)
    gen.write_to_csvs("/tmp/synthetic_data")
    
    # For Databricks Volume:
    # gen.write_to_csvs("/Volumes/migration_factory/<client>/raw/alteryx/sample_sales_analytics_complex/synthetic")
```

### 6.2 Integration into Conversion Notebook

```python
# In your conversion notebook (e.g., `notebooks/bronze_ingest.py`)

from synthetic_data_gen import SyntheticDataGenerator

# Parameters (from job or hardcoded)
use_synthetic = True  # Toggle for validation mode
volume_base = "/Volumes/migration_factory/client_name/raw/alteryx"
workflow_name = "sample_sales_analytics_complex"

if use_synthetic:
    # Generate synthetic data
    gen = SyntheticDataGenerator(seed=42)
    synthetic_dir = f"{volume_base}/{workflow_name}/synthetic"
    gen.write_to_csvs(synthetic_dir)
    data_source = synthetic_dir
    print(f"✓ Using synthetic data from {synthetic_dir}")
    print(f"  NOTE: This is validation data only. Re-run with real data before production.")
else:
    # Use real data path
    data_source = f"/mnt/data"

# Read transaction CSV
df_transactions = spark.read.csv(f"{data_source}/transactions.csv", header=True, inferSchema=True)
df_products = spark.read.csv(f"{data_source}/products.csv", header=True, inferSchema=True)
df_stores = spark.read.csv(f"{data_source}/stores.csv", header=True, inferSchema=True)

# ... rest of pipeline
```

---

## 7. Validation Checklist

After generating synthetic data, verify before running the workflow:

- [ ] **Schema match:** Generated CSVs have same columns (name, type) as declared in `.yxmd` DbFileInput.
- [ ] **Referential integrity:** Sample join on `transactions.product_id = products.product_id` returns no nulls.
- [ ] **Filter satisfaction:** Count rows that pass the Filter (qty > 0, date in range); expect >0.
- [ ] **Aggregation plausibility:** Summarize query groups by product/store; expect multiple rows per group (not all singletons).
- [ ] **Metadata present:** `_metadata.json` exists in the synthetic/ directory with schema + constraints.
- [ ] **Row counts:** Dimensions 5–20 rows, facts ~200 rows (small but exercises logic).
- [ ] **Date format:** Dates are ISO 8601 (YYYY-MM-DD) and parseable by Databricks.

---

## 8. Gotchas & Limitations

| Issue | Reason | Mitigation |
|-------|--------|-----------|
| **Null handling** | Synthetic data has no nulls; real data may have sparse nulls. Filters like `IS NOT NULL` silently pass on synthetic. | Document in metadata; add a small % of nulls post-generation if needed. |
| **Numeric precision** | Generator uses `random.uniform()` → may not match real distribution. Double/Float rounding differs. | For validation, "close enough" is acceptable; flag in metadata for sign-off. |
| **String enumeration** | Hardcoded categories → may miss real domain values. | Extract categories from Filter GROUP BY or Summarize if available. |
| **Cardinality skew** | Synthetic balanced (uniform random); real data may be skewed (e.g., 80% from one store). | Document; re-validate against real data before production deployment. |
| **Time-dependent logic** | Synthetic spans full year; real data may be quarterly. Formulas like `DATEDIFF(...)` produce different ranges. | Extract date bounds from Filters; avoid hard-coding "2024-01-01 to 2024-12-31" for all workflows. |

---

## 9. Appendix: Alteryx Type Mapping

| Alteryx Type | Python/Pandas Type | Spark Type | CSV Representation |
|---|---|---|---|
| `Int32` | `int` | `IntegerType` | `123` |
| `Int64` | `int` | `LongType` | `123` |
| `Double` | `float` | `DoubleType` | `123.45` |
| `String` | `str` | `StringType` | `"text"` |
| `Bool` | `bool` | `BooleanType` | `true` / `false` |
| `Date` | `datetime.date` or `str` (ISO 8601) | `DateType` | `"2024-01-15"` |
| `DateTime` | `datetime.datetime` or `str` (ISO 8601) | `TimestampType` | `"2024-01-15T10:30:00"` |
| `Byte` | `bytes` | `BinaryType` | (not typical in CSV) |
| `FixedDecimal` | `decimal.Decimal` | `DecimalType(p, s)` | `"123.45"` |

---

**Status:** Reference design, ready for Phase 1 validation workflows. Evolve to `databricks-data-generation` skill reuse once migration estate scales.
