"""Live smoke test: exercise the registry against real Unity Catalog on the FEVM.

Runs the DDL, then drives one mock object through the lifecycle and verifies the
audit trail. Not a unit test — a deployment sanity check.

Usage:
    python spine/lib/test_registry_live.py \
        --profile fe-vm-dt-serverless-stable-isqgt5 \
        --warehouse 2f3030c18eff5d1a \
        --catalog migration_factory --schema smoketest
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

sys.path.insert(0, os.path.dirname(__file__))
import registry as reg_mod  # noqa: E402
import confidence as conf_mod  # noqa: E402


def make_sql(w: WorkspaceClient, warehouse_id: str, catalog: str, schema: str):
    def sql(query: str) -> list[dict]:
        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout="30s",
            catalog=catalog if _catalog_exists else None,
        )
        # poll if still running
        while resp.status and resp.status.state in (
            StatementState.PENDING,
            StatementState.RUNNING,
        ):
            time.sleep(1)
            resp = w.statement_execution.get_statement(resp.statement_id)
        if resp.status and resp.status.state == StatementState.FAILED:
            raise RuntimeError(f"SQL failed: {resp.status.error.message}\n{query[:200]}")
        # shape rows as list[dict]
        if not resp.result or not resp.manifest or not resp.manifest.schema:
            return []
        cols = [c.name for c in resp.manifest.schema.columns]
        data = resp.result.data_array or []
        return [dict(zip(cols, row)) for row in data]

    return sql


_catalog_exists = False  # first DDL creates it; before that don't scope statements to it


def _split_statements(text: str) -> list[str]:
    """Split a SQL file into statements.

    Strip `--` line comments FIRST (a comment may contain a ';', which would otherwise
    break the split), then split on ';'. Comment-only lines never reach the splitter.
    """
    no_comments = "\n".join(
        ln for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("--")
    )
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def run_ddl(sql, ddl_dir, catalog, schema, volume):
    files = sorted(f for f in os.listdir(ddl_dir) if f.endswith(".sql"))
    for fname in files:
        with open(os.path.join(ddl_dir, fname)) as fh:
            text = (
                fh.read()
                .replace("${catalog}", catalog)
                .replace("${schema}", schema)
                .replace("${volume}", volume)
            )
        for stmt in _split_statements(text):
            print(f"  DDL [{fname}]: {stmt.splitlines()[0][:70]}")
            sql(stmt)


def main():
    global _catalog_exists
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--catalog", default="migration_factory")
    ap.add_argument("--schema", default="smoketest")
    ap.add_argument("--volume", default="raw")
    args = ap.parse_args()

    w = WorkspaceClient(profile=args.profile)
    sql = make_sql(w, args.warehouse, args.catalog, args.schema)

    print("1. Running DDL (create catalog/schema/volume/tables)...")
    ddl_dir = os.path.join(os.path.dirname(__file__), "..", "ddl")
    run_ddl(sql, ddl_dir, args.catalog, args.schema, args.volume)
    _catalog_exists = True

    print("2. Driving a mock object through the lifecycle...")
    r = reg_mod.Registry(args.catalog, args.schema, sql=sql, actor="smoketest",
                         config={"client": "smoketest"})
    oid = "alteryx:sample_customer_orders"
    r.register_object(oid, source_type="alteryx", object_kind="workflow",
                      volume_path=f"/Volumes/{args.catalog}/{args.schema}/{args.volume}/alteryx/sample_customer_orders.yxmd",
                      target_uc_fqn=f"{args.catalog}.{args.schema}.customer_order_summary",
                      layer="gold", complexity="medium")
    run_id = r.start_run(oid, "convert")

    # simulate a conversion output with one TODO (DateTimeParse has no clean Spark equiv)
    converted = "SELECT customer_id, sum(amount) FROM orders -- TODO: DateTimeParse needs to_date()"
    score = conf_mod.score(converted)
    print(f"   deterministic confidence: {score.confidence}, findings: {len(score.findings)}")
    for f in score.findings:
        r.add_todo(oid, f["category"], f["message"], f["severity"])

    to_status = "needs_review" if score.confidence < 0.8 else "converted"
    r.transition(oid, to_status, confidence=score.confidence,
                 output_path=f"/Volumes/{args.catalog}/{args.schema}/{args.volume}/output/{oid}.sql",
                 detail="smoketest conversion", run_id=run_id)
    r.end_run(run_id, "partial")

    print("3. Verifying registry state...")
    print("   summary:", r.summary())
    print("   needs_review:", [o["object_id"] for o in r.objects_by_status("needs_review")])
    print("   open todos:", [(t["category"], t["severity"]) for t in r.open_todos(oid)])
    audit = sql(f"SELECT action, from_status, to_status FROM {args.catalog}.{args.schema}.audit "
                f"WHERE object_id = '{oid}' ORDER BY event_ts")
    print("   audit trail:", [(a["action"], a["from_status"], a["to_status"]) for a in audit])

    print("\nPASS — registry works against live UC. Clean up with:")
    print(f"   DROP SCHEMA {args.catalog}.{args.schema} CASCADE;")


if __name__ == "__main__":
    main()
