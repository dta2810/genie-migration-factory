"""End-to-end demo: assess -> convert -> triage against the live FEVM registry.

Mirrors what Genie Code does when following the skills, but scripted so we can show the
full loop deterministically. Reads the sample .yxmd from the UC Volume, populates the
registry, and prints the audit trail + triage view.

Usage:
    python demo/run_demo.py --profile fe-vm-dt-serverless-stable-isqgt5 \
        --warehouse 2f3030c18eff5d1a \
        --catalog dt_serverless_stable_isqgt5_catalog --schema migration_factory
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import time
import xml.etree.ElementTree as ET

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spine", "lib"))
import registry as reg_mod  # noqa: E402
import confidence as conf_mod  # noqa: E402

TOOL_LABEL = {
    "DbFileInput": "Input", "DbFileOutput": "Output", "AlteryxSelect": "Select",
    "Filter": "Filter", "Formula": "Formula", "Join": "Join", "Summarize": "Summarize",
    "Union": "Union",
}


def make_sql(w, warehouse_id):
    def sql(query: str):
        r = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=query, wait_timeout="30s"
        )
        while r.status and r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(1)
            r = w.statement_execution.get_statement(r.statement_id)
        if r.status and r.status.state == StatementState.FAILED:
            raise RuntimeError(f"SQL failed: {r.status.error.message}\n{query[:200]}")
        if not r.result or not r.manifest or not r.manifest.schema:
            return []
        cols = [c.name for c in r.manifest.schema.columns]
        return [dict(zip(cols, row)) for row in (r.result.data_array or [])]
    return sql


def parse_yxmd(xml_text: str):
    """Deterministic inventory: tools + a naive complexity read (what migrate-assess does)."""
    root = ET.fromstring(xml_text)
    tools = []
    for node in root.iter("Node"):
        gs = node.find("GuiSettings")
        plugin = gs.get("Plugin", "") if gs is not None else ""
        short = plugin.split(".")[-1] if plugin else "Unknown"
        ann = node.find(".//Annotation/Name")
        name = ann.text if ann is not None else short
        config_text = ET.tostring(node, encoding="unicode")
        tools.append({"tool": short, "label": TOOL_LABEL.get(short, short),
                      "name": name, "config": config_text})
    return tools


def assess_complexity(tools):
    kinds = {t["tool"] for t in tools}
    if {"Join", "Union", "Summarize"} & kinds and len(tools) > 6:
        return "medium"
    if kinds & {"Macro", "RTool", "PythonTool", "DynamicInput"}:
        return "high"
    return "low"


def forecast_todos(tools):
    """What migrate-assess flags for manual review before converting."""
    todos = []
    for t in tools:
        if "DateTimeParse" in t["config"]:
            todos.append(("untranslated_fn", "warning",
                          f"{t['name']}: DateTimeParse has no clean Spark equivalent (use to_date/to_timestamp)"))
        if t["tool"] in ("Macro", "RTool", "PythonTool", "DynamicInput"):
            todos.append(("manual_review", "blocker", f"{t['name']}: {t['tool']} needs manual migration"))
    return todos


def convert(tools) -> str:
    """A faithful-enough conversion to demonstrate the loop + scoring.

    (In Genie Code this is the skill doing the real tool-by-tool mapping. Here we emit a
    representative SDP SQL target that intentionally carries the DateTimeParse TODO.)
    """
    return """-- Lakeflow SDP: customer_order_summary (converted from Alteryx)
CREATE OR REFRESH MATERIALIZED VIEW customer_order_summary AS
SELECT c.customer_id, c.customer_name,
       -- TODO: DateTimeParse([order_date],"%Y-%m-%d") -> to_date(order_date, 'yyyy-MM-dd'); verify tz
       to_date(o.order_date) AS order_date,
       count(*) AS order_count, sum(o.amount) AS total_amount
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.status = 'Completed'
GROUP BY c.customer_id, c.customer_name, to_date(o.order_date)
"""


def banner(step):
    print(f"\n{'='*66}\n  {step}\n{'='*66}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--volume", default="raw")
    args = ap.parse_args()

    w = WorkspaceClient(profile=args.profile)
    sql = make_sql(w, args.warehouse)
    r = reg_mod.Registry(args.catalog, args.schema, sql=sql, actor="demo",
                        config={"client": "demo", "target": "sdp"})

    vol = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}"
    src = f"{vol}/alteryx/sample_customer_orders.yxmd"
    oid = "alteryx:sample_customer_orders"

    # ---- ASSESS -------------------------------------------------------------
    banner("migrate-assess  (read .yxmd from Volume -> register + score)")
    xml_text = "".join(l for l in w.files.download(src).contents.read().decode())
    tools = parse_yxmd(xml_text)
    print(f"Read {src}")
    print(f"Tools found ({len(tools)}): " + ", ".join(f"{t['label']}" for t in tools))
    complexity = assess_complexity(tools)
    print(f"Complexity: {complexity}")

    r.register_object(oid, source_type="alteryx", object_kind="workflow",
                      volume_path=src, target_uc_fqn=f"{args.catalog}.{args.schema}.customer_order_summary",
                      layer="gold", complexity=complexity)
    run_a = r.start_run(oid, "assess")
    todos = forecast_todos(tools)
    for cat, sev, msg in todos:
        r.add_todo(oid, cat, msg, sev)
    r.transition(oid, "assessed", detail=f"{len(tools)} tools, complexity={complexity}", run_id=run_a)
    r.end_run(run_a, "ok")
    print(f"Registered object '{oid}' -> assessed. Forecast TODOs: {len(todos)}")

    # ---- CONVERT ------------------------------------------------------------
    banner("migrate-convert  (convert -> write output -> deterministic score)")
    run_c = r.start_run(oid, "convert")
    code = convert(tools)
    out_path = f"{vol}/output/{oid}.sql"
    w.files.upload(out_path, io.BytesIO(code.encode()), overwrite=True)
    print(f"Wrote target SDP SQL -> {out_path}")
    score = conf_mod.score(code)
    print(f"Deterministic confidence: {score.confidence}  (findings: {len(score.findings)})")
    for f in score.findings:
        print(f"   - [{f['severity']}] {f['category']}: {f['message']}")
        r.add_todo(oid, f["category"], f["message"], f["severity"])
    to_status = "needs_review" if (score.confidence < 0.8 or
                                   any(f["severity"] == "blocker" for f in score.findings)) else "converted"
    r.transition(oid, to_status, confidence=score.confidence, output_path=out_path,
                 detail="converted to SDP SQL", run_id=run_c)
    r.end_run(run_c, "partial" if to_status == "needs_review" else "ok")
    print(f"Object -> {to_status}")

    # ---- TRIAGE -------------------------------------------------------------
    banner("migrate-triage  (governance view over the registry)")
    print("Lifecycle funnel:")
    for row in r.summary():
        print(f"   {row['status']:<14} n={row['n']}  avg_conf={row['avg_conf']}")
    print("\nOpen TODOs (ranked):")
    for t in r.open_todos(oid):
        print(f"   [{t['severity']:<7}] {t['category']:<16} {t['message']}")
    print("\nAudit trail (append-only, the product):")
    audit = sql(f"SELECT action, from_status, to_status, actor, detail "
                f"FROM {args.catalog}.{args.schema}.audit WHERE object_id = '{oid}' ORDER BY event_ts")
    for a in audit:
        arrow = f"{a['from_status']}->{a['to_status']}" if a['to_status'] else "-"
        print(f"   {a['action']:<22} {arrow:<26} by {a['actor']}  ({a['detail']})")

    print("\nDemo complete. This is exactly what Genie Code drives via the skills.")


if __name__ == "__main__":
    main()
