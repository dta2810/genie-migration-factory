"""Genie Migration Factory — registry API (the audited spine).

Source-agnostic API the Genie Code skills call to write migration state + audit into
Unity Catalog. Artifacts live in the UC Volume; these functions manage the rows in
`objects`, `runs`, `audit`, `todos`, and `config`.

Design notes:
- Runs in-workspace. SQL executes via an injected `sql(query: str) -> list[dict]` callable
  so the same code works from a notebook (spark.sql), a Databricks SQL connection, or the
  ai-dev-kit MCP `execute_sql` tool. No hard dependency on a specific runtime.
- Every state transition writes an append-only `audit` row. Status changes go through
  `transition()`, never a bare UPDATE, so the audit trail is complete by construction.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterable

# Lifecycle states (see docs/ARCHITECTURE.md). Order matters for validation.
LIFECYCLE = ["discovered", "assessed", "converted", "validated", "deployed"]
NEEDS_REVIEW = "needs_review"  # off-path state reachable from any step

SqlFn = Callable[[str], list[dict]]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _sql_str(value) -> str:
    """Render a Python value as a SQL literal (single-quoted, escaped) or NULL."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def config_hash(config: dict) -> str:
    """Stable hash of an engagement config, recorded on every audited step."""
    blob = json.dumps(config or {}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


@dataclass
class Registry:
    """Handle onto one engagement's registry (`{catalog}.{schema}`)."""

    catalog: str
    schema: str
    sql: SqlFn
    actor: str = "genie_code"
    config: dict = field(default_factory=dict)

    def _t(self, table: str) -> str:
        return f"{self.catalog}.{self.schema}.{table}"

    # -- objects ---------------------------------------------------------------

    def register_object(
        self,
        object_id: str,
        *,
        source_type: str,
        object_kind: str,
        volume_path: str,
        parent_id: str | None = None,
        target_uc_fqn: str | None = None,
        layer: str | None = None,
        complexity: str | None = None,
    ) -> str:
        """Register an object, or refresh its assessment fields if already present.

        Re-scanning the same artifact is safe and idempotent:
        - New object  -> inserted at status='discovered', audited as a register event.
        - Existing one -> assessment fields (source_type, object_kind, parent_id,
          volume_path, target_uc_fqn, layer, complexity) are refreshed WITHOUT resetting
          its lifecycle status/confidence, and audited as a re-register event. This keeps
          `migrate-assess` reproducible without lying about the object going back to
          'discovered'.
        """
        exists = bool(
            self.sql(
                f"SELECT 1 FROM {self._t('objects')} WHERE object_id = {_sql_str(object_id)}"
            )
        )

        cols = {
            "object_id": object_id,
            "source_type": source_type,
            "object_kind": object_kind,
            "parent_id": parent_id,
            "volume_path": volume_path,
            "output_path": None,
            "target_uc_fqn": target_uc_fqn,
            "layer": layer,
            "complexity": complexity,
            "status": "discovered",
            "confidence": None,
            "updated_at": _now(),
        }
        collist = ", ".join(cols)
        vallist = ", ".join(
            _sql_str(v) if k != "updated_at" else f"TIMESTAMP{_sql_str(v)}"
            for k, v in cols.items()
        )
        # Refresh only the assessment fields on match; never reset status/confidence.
        matched_updates = ", ".join(
            [
                f"t.source_type = {_sql_str(source_type)}",
                f"t.object_kind = {_sql_str(object_kind)}",
                f"t.parent_id = {_sql_str(parent_id)}",
                f"t.volume_path = {_sql_str(volume_path)}",
                f"t.target_uc_fqn = {_sql_str(target_uc_fqn)}",
                f"t.layer = {_sql_str(layer)}",
                f"t.complexity = {_sql_str(complexity)}",
                f"t.updated_at = TIMESTAMP{_sql_str(_now())}",
            ]
        )
        self.sql(
            f"""
            MERGE INTO {self._t('objects')} t
            USING (SELECT {_sql_str(object_id)} AS object_id) s
            ON t.object_id = s.object_id
            WHEN MATCHED THEN UPDATE SET {matched_updates}
            WHEN NOT MATCHED THEN INSERT ({collist}) VALUES ({vallist})
            """
        )
        if exists:
            self._audit(object_id, None, "reregister", None, None, "assessment refreshed")
        else:
            self._audit(object_id, None, "register", None, "discovered", "object registered")
        return object_id

    def transition(
        self,
        object_id: str,
        to_status: str,
        *,
        confidence: float | None = None,
        output_path: str | None = None,
        target_uc_fqn: str | None = None,
        detail: str | None = None,
        run_id: str | None = None,
    ) -> None:
        """Move an object to a new status and write an append-only audit row.

        The ONLY sanctioned way to change status — guarantees the audit trail.
        """
        if to_status not in LIFECYCLE and to_status != NEEDS_REVIEW:
            raise ValueError(f"unknown status: {to_status}")

        current = self.sql(
            f"SELECT status FROM {self._t('objects')} WHERE object_id = {_sql_str(object_id)}"
        )
        if not current:
            # Never audit a transition for an object that doesn't exist — that would
            # write a phantom audit row. Register the object first.
            raise ValueError(f"cannot transition unknown object: {object_id}")
        from_status = current[0]["status"]

        sets = [f"status = {_sql_str(to_status)}", f"updated_at = TIMESTAMP{_sql_str(_now())}"]
        if confidence is not None:
            sets.append(f"confidence = {confidence}")
        if output_path is not None:
            sets.append(f"output_path = {_sql_str(output_path)}")
        if target_uc_fqn is not None:
            sets.append(f"target_uc_fqn = {_sql_str(target_uc_fqn)}")
        self.sql(
            f"UPDATE {self._t('objects')} SET {', '.join(sets)} "
            f"WHERE object_id = {_sql_str(object_id)}"
        )
        self._audit(object_id, run_id, f"transition:{to_status}", from_status, to_status, detail)

    # -- runs ------------------------------------------------------------------

    def start_run(self, object_id: str, step: str, engine: str = "genie_code") -> str:
        run_id = _id("run")
        self.sql(
            f"INSERT INTO {self._t('runs')} (run_id, object_id, step, engine, started_at, ended_at, outcome) "
            f"VALUES ({_sql_str(run_id)}, {_sql_str(object_id)}, {_sql_str(step)}, "
            f"{_sql_str(engine)}, TIMESTAMP{_sql_str(_now())}, NULL, NULL)"
        )
        return run_id

    def end_run(self, run_id: str, outcome: str) -> None:
        self.sql(
            f"UPDATE {self._t('runs')} SET ended_at = TIMESTAMP{_sql_str(_now())}, "
            f"outcome = {_sql_str(outcome)} WHERE run_id = {_sql_str(run_id)}"
        )

    # -- todos -----------------------------------------------------------------

    def add_todo(
        self, object_id: str, category: str, message: str, severity: str = "warning"
    ) -> str:
        todo_id = _id("todo")
        self.sql(
            f"INSERT INTO {self._t('todos')} "
            f"(todo_id, object_id, category, severity, message, resolved, created_at) VALUES ("
            f"{_sql_str(todo_id)}, {_sql_str(object_id)}, {_sql_str(category)}, "
            f"{_sql_str(severity)}, {_sql_str(message)}, FALSE, TIMESTAMP{_sql_str(_now())})"
        )
        return todo_id

    def resolve_todo(self, todo_id: str) -> None:
        self.sql(
            f"UPDATE {self._t('todos')} SET resolved = TRUE WHERE todo_id = {_sql_str(todo_id)}"
        )

    def open_todos(self, object_id: str | None = None) -> list[dict]:
        where = f"WHERE resolved = FALSE" + (
            f" AND object_id = {_sql_str(object_id)}" if object_id else ""
        )
        return self.sql(
            f"SELECT * FROM {self._t('todos')} {where} "
            f"ORDER BY CASE severity WHEN 'blocker' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END"
        )

    # -- queries (triage surface) ---------------------------------------------

    def objects_by_status(self, status: str) -> list[dict]:
        return self.sql(
            f"SELECT * FROM {self._t('objects')} WHERE status = {_sql_str(status)} "
            f"ORDER BY complexity DESC, confidence ASC"
        )

    def summary(self) -> list[dict]:
        return self.sql(
            f"SELECT status, count(*) AS n, round(avg(confidence), 3) AS avg_conf "
            f"FROM {self._t('objects')} GROUP BY status ORDER BY status"
        )

    # -- audit (internal) ------------------------------------------------------

    def _audit(self, object_id, run_id, action, from_status, to_status, detail) -> None:
        self.sql(
            f"INSERT INTO {self._t('audit')} "
            f"(audit_id, object_id, run_id, event_ts, actor, action, from_status, to_status, "
            f"config_hash, detail) VALUES ("
            f"{_sql_str(_id('aud'))}, {_sql_str(object_id)}, {_sql_str(run_id)}, "
            f"TIMESTAMP{_sql_str(_now())}, {_sql_str(self.actor)}, {_sql_str(action)}, "
            f"{_sql_str(from_status)}, {_sql_str(to_status)}, "
            f"{_sql_str(config_hash(self.config))}, {_sql_str(detail)})"
        )
